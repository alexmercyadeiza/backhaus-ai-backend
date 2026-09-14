use anyhow::{Context, Result, ensure};
use std::{env, net::SocketAddr, path::PathBuf, time::Duration};

#[derive(Clone)]
pub struct Config {
    pub database_url: String,
    pub bind: SocketAddr,
    pub api_key: String,
    pub cors_origin: String,
    pub workspace_id: String,
    pub db_max_connections: u32,
    pub model_base_url: Option<String>,
    pub model_name: Option<String>,
    pub model_api_key: String,
    pub model_request_options: serde_json::Value,
    pub model_timeout: Duration,
    pub worker_poll: Duration,
    pub typst_bin: String,
    pub node_bin: String,
    pub worker_script: PathBuf,
}
impl Config {
    pub fn from_env() -> Result<Self> {
        dotenvy::dotenv().ok();
        let get = |key: &str, default: &str| env::var(key).unwrap_or_else(|_| default.into());
        let optional = |key: &str| env::var(key).ok().filter(|s| !s.trim().is_empty());
        let api_key = env::var("BACKEND_API_KEY").context("Set BACKEND_API_KEY")?;
        ensure!(
            api_key.len() >= 32 && !api_key.starts_with("replace-"),
            "BACKEND_API_KEY must be a random secret of at least 32 characters"
        );
        let config = Self {
            database_url: env::var("DATABASE_URL").context("Set DATABASE_URL")?,
            bind: get("BIND_ADDRESS", "127.0.0.1:8080").parse()?,
            api_key,
            cors_origin: get("CORS_ORIGIN", "http://localhost:5173"),
            workspace_id: get("WORKSPACE_ID", "org_default"),
            db_max_connections: get("DB_MAX_CONNECTIONS", "4").parse()?,
            model_base_url: optional("MODEL_BASE_URL"),
            model_name: optional("MODEL_NAME"),
            model_api_key: get("MODEL_API_KEY", ""),
            model_request_options: serde_json::from_str(&get("MODEL_REQUEST_OPTIONS", "{}"))
                .context("MODEL_REQUEST_OPTIONS must be a JSON object")?,
            model_timeout: Duration::from_secs(get("MODEL_TIMEOUT_SECONDS", "120").parse()?),
            worker_poll: Duration::from_secs(get("WORKER_POLL_SECONDS", "2").parse()?),
            typst_bin: get("TYPST_BIN", "typst"),
            node_bin: get("NODE_BIN", "node"),
            worker_script: PathBuf::from(get("WORKER_SCRIPT", "worker/dist/worker.js")),
        };
        ensure!(
            config
                .model_request_options
                .as_object()
                .is_some_and(|options| options
                    .keys()
                    .all(|key| matches!(key.as_str(), "reasoning" | "provider"))),
            "MODEL_REQUEST_OPTIONS only accepts reasoning and provider options"
        );
        ensure!(
            (1..=16).contains(&config.db_max_connections),
            "DB_MAX_CONNECTIONS must be 1..16"
        );
        ensure!(
            (10..=600).contains(&config.model_timeout.as_secs()),
            "MODEL_TIMEOUT_SECONDS must be 10..600"
        );
        ensure!(
            (1..=60).contains(&config.worker_poll.as_secs()),
            "WORKER_POLL_SECONDS must be 1..60"
        );
        if let Some(url) = &config.model_base_url {
            ensure!(
                url.starts_with("https://") || url.starts_with("http://"),
                "MODEL_BASE_URL must be an HTTP(S) URL"
            );
        }
        config
            .cors_origin
            .parse::<axum::http::HeaderValue>()
            .context("Invalid CORS_ORIGIN")?;
        Ok(config)
    }
}
