//! Supervises the Strands worker child process and speaks its line-delimited
//! JSON protocol. Rust keeps database credentials, workspace authority and
//! every tool implementation; the worker only orchestrates the model.
use crate::{
    config::Config,
    error::{Error, Result},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{process::Stdio, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, Command},
    sync::mpsc,
    task::JoinHandle,
};

pub const PROTOCOL_VERSION: u64 = 1;
const MAX_LINE_BYTES: usize = 1_048_576;
const READY_TIMEOUT: Duration = Duration::from_secs(30);
const SHUTDOWN_GRACE: Duration = Duration::from_secs(3);

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Inbound<'a> {
    Run {
        run_id: &'a str,
        kind: &'a str,
        system_prompt: &'a str,
        message: &'a str,
        history: Vec<HistoryTurn>,
        tools: &'a [Value],
        limits: RunLimits,
    },
    ToolResult {
        run_id: &'a str,
        call_id: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        result: Option<Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
    Cancel {
        run_id: &'a str,
    },
    Shutdown,
}
#[derive(Debug, Serialize)]
pub struct HistoryTurn {
    pub role: &'static str,
    pub text: String,
}
#[derive(Debug, Clone, Copy, Serialize)]
pub struct RunLimits {
    pub turns: u32,
    pub output_tokens: u32,
    pub max_output_chars: u32,
    pub timeout_ms: u64,
    pub tool_timeout_ms: u64,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Outbound {
    Ready {
        protocol: u64,
        #[serde(default)]
        sdk: String,
    },
    TextDelta {
        run_id: String,
        text: String,
    },
    ToolCall {
        run_id: String,
        call_id: String,
        name: String,
        #[serde(default)]
        input: Value,
    },
    Completed {
        run_id: String,
        answer: String,
        #[serde(default)]
        stop_reason: String,
        #[serde(default)]
        model_calls: u32,
    },
    Failed {
        run_id: String,
        #[serde(default)]
        code: String,
        #[serde(default)]
        error: String,
    },
    Cancelled {
        run_id: String,
    },
}

pub struct Worker {
    child: Child,
    stdin: ChildStdin,
    messages: mpsc::Receiver<Outbound>,
    reader: JoinHandle<()>,
    logger: JoinHandle<()>,
    /// Run the worker is currently executing, until its terminal message arrives.
    active_run: Option<String>,
    pub sdk_version: String,
}

impl Worker {
    /// Start the worker and wait for its ready message. Model credentials are
    /// passed only through the child's private environment.
    pub async fn spawn(config: &Config) -> Result<Worker> {
        if !config.worker_script.is_file() {
            return Err(Error::Worker(format!(
                "Worker script {} is missing; run `npm ci && npm run build` in the worker folder",
                config.worker_script.display()
            )));
        }
        let mut command = Command::new(&config.node_bin);
        command
            .arg(&config.worker_script)
            .env_clear()
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        for key in [
            "PATH",
            "HOME",
            "LANG",
            "LC_ALL",
            "TMPDIR",
            "SSL_CERT_FILE",
            "NODE_EXTRA_CA_CERTS",
        ] {
            if let Ok(value) = std::env::var(key) {
                command.env(key, value);
            }
        }
        command.env("NODE_NO_WARNINGS", "1");
        if let Some(url) = &config.model_base_url {
            command.env("MODEL_BASE_URL", url);
        }
        if let Some(model) = &config.model_name {
            command.env("MODEL_NAME", model);
        }
        command.env("MODEL_API_KEY", &config.model_api_key);
        command.env(
            "MODEL_REQUEST_OPTIONS",
            config.model_request_options.to_string(),
        );
        command.env(
            "MODEL_TIMEOUT_SECONDS",
            config.model_timeout.as_secs().to_string(),
        );
        let mut child = command
            .spawn()
            .map_err(|e| Error::Worker(format!("Cannot start Node worker: {e}")))?;
        let stdin = child.stdin.take().expect("piped stdin");
        let stdout = child.stdout.take().expect("piped stdout");
        let stderr = child.stderr.take().expect("piped stderr");
        let (sender, messages) = mpsc::channel(256);
        let reader = tokio::spawn(async move {
            let mut lines = BufReader::new(stdout);
            let mut line = String::new();
            loop {
                line.clear();
                match lines.read_line(&mut line).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {}
                }
                if line.len() > MAX_LINE_BYTES {
                    tracing::error!("Worker sent an oversized message; closing the channel");
                    break;
                }
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                match serde_json::from_str::<Outbound>(trimmed) {
                    Ok(message) => {
                        if sender.send(message).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => tracing::warn!("Worker sent an unrecognized message"),
                }
            }
        });
        let logger = tokio::spawn(async move {
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let line: String = line.chars().take(4000).collect();
                if !line.trim().is_empty() {
                    tracing::warn!(target: "backhaus_ai_backend::worker_stderr", "{line}");
                }
            }
        });
        let mut worker = Worker {
            child,
            stdin,
            messages,
            reader,
            logger,
            active_run: None,
            sdk_version: String::new(),
        };
        match tokio::time::timeout(READY_TIMEOUT, worker.next()).await {
            Ok(Some(Outbound::Ready { protocol, sdk })) if protocol == PROTOCOL_VERSION => {
                worker.sdk_version = sdk;
                Ok(worker)
            }
            Ok(Some(Outbound::Ready { protocol, .. })) => {
                worker.kill().await;
                Err(Error::Worker(format!(
                    "Worker protocol {protocol} does not match {PROTOCOL_VERSION}"
                )))
            }
            Ok(_) => {
                worker.kill().await;
                Err(Error::Worker(
                    "Worker exited before reporting ready; check its build and Node version".into(),
                ))
            }
            Err(_) => {
                worker.kill().await;
                Err(Error::Worker("Worker did not report ready in time".into()))
            }
        }
    }

    pub async fn send(&mut self, message: &Inbound<'_>) -> Result<()> {
        if let Inbound::Run { run_id, .. } = message {
            self.active_run = Some((*run_id).to_owned());
        }
        let mut line = serde_json::to_vec(message).map_err(|_| Error::Worker("Encode".into()))?;
        line.push(b'\n');
        self.stdin
            .write_all(&line)
            .await
            .map_err(|_| Error::Worker("Worker stdin closed".into()))?;
        self.stdin
            .flush()
            .await
            .map_err(|_| Error::Worker("Worker stdin closed".into()))
    }

    /// Next message, or `None` once the worker has exited or broken the protocol.
    pub async fn next(&mut self) -> Option<Outbound> {
        let message = self.messages.recv().await;
        if let Some(
            Outbound::Completed { run_id, .. }
            | Outbound::Failed { run_id, .. }
            | Outbound::Cancelled { run_id },
        ) = &message
            && self.active_run.as_deref() == Some(run_id.as_str())
        {
            self.active_run = None;
        }
        message
    }

    pub fn pid(&self) -> Option<u32> {
        self.child.id()
    }

    pub fn is_alive(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None)) && !self.reader.is_finished()
    }

    /// Cancel an in-flight run and wait for its terminal message so that no
    /// stale output can be mistaken for a later run. Failure means the caller
    /// must replace this worker. A run that already finished needs nothing.
    pub async fn abort_run(&mut self, run_id: &str) -> Result<()> {
        if self.active_run.as_deref() != Some(run_id) {
            return Ok(());
        }
        self.send(&Inbound::Cancel { run_id }).await?;
        let drained = tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(message) = self.next().await {
                match message {
                    Outbound::Completed { run_id: id, .. }
                    | Outbound::Failed { run_id: id, .. }
                    | Outbound::Cancelled { run_id: id } => {
                        if id == run_id {
                            return true;
                        }
                    }
                    Outbound::ToolCall {
                        run_id: id,
                        call_id,
                        ..
                    } if id == run_id => {
                        // Never execute tools for an abandoned run.
                        let _ = self
                            .send(&Inbound::ToolResult {
                                run_id,
                                call_id: &call_id,
                                result: None,
                                error: Some("Run cancelled".into()),
                            })
                            .await;
                    }
                    _ => {}
                }
            }
            false
        })
        .await;
        match drained {
            Ok(true) => Ok(()),
            _ => Err(Error::Worker(
                "Worker did not acknowledge cancellation".into(),
            )),
        }
    }

    pub async fn shutdown(mut self) {
        let _ = self.send(&Inbound::Shutdown).await;
        let _ = self.stdin.shutdown().await;
        if tokio::time::timeout(SHUTDOWN_GRACE, self.child.wait())
            .await
            .is_err()
        {
            tracing::warn!("Worker did not exit after shutdown; killing it");
            let _ = self.child.kill().await;
        }
        self.reader.abort();
        self.logger.abort();
    }

    async fn kill(&mut self) {
        let _ = self.child.kill().await;
        self.reader.abort();
        self.logger.abort();
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        // kill_on_drop covers the child; stop the pump tasks too.
        self.reader.abort();
        self.logger.abort();
    }
}
