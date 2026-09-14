use crate::{
    agent::{self, Worked},
    api::{self, AppState},
    config::Config,
    scoped_agents,
    worker::Worker,
};
use anyhow::Result;
use sqlx::PgPool;
use std::{sync::Arc, time::Duration};
use tokio::{sync::watch, task::JoinSet};
use uuid::Uuid;

pub async fn wait_for_stop(mut stop: watch::Receiver<bool>) {
    while !*stop.borrow_and_update() {
        if stop.changed().await.is_err() {
            break;
        }
    }
}

pub async fn run(pool: PgPool, config: Arc<Config>, mode: &str) -> Result<()> {
    // Keep a dedicated session lock for the runtime's lifetime. Reset takes the
    // exclusive counterpart. Detaching ensures Drop closes (not pools) the session.
    let mut runtime_lock = pool.acquire().await?.detach();
    sqlx::query("SELECT pg_advisory_lock_shared(hashtextextended($1, 0))")
        .bind(format!("demo-runtime:{}", config.workspace_id))
        .execute(&mut runtime_lock)
        .await?;
    let (stop, receiver) = watch::channel(false);
    let mut tasks: JoinSet<Result<()>> = JoinSet::new();
    // Bind before starting any workers. A duplicate start fails without leaving workers behind.
    if mode != "worker" {
        let listener = tokio::net::TcpListener::bind(config.bind).await?;
        let app = api::router(AppState {
            pool: pool.clone(),
            config: config.clone(),
        });
        let stopped = receiver.clone();
        tracing::info!(address=%config.bind, "API listening");
        tasks.spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(wait_for_stop(stopped))
                .await?;
            Ok(())
        });
    }
    if mode != "serve" {
        let db = pool.clone();
        let cfg = config.clone();
        let stopped = receiver.clone();
        tasks.spawn(async move { chat_worker(db, cfg, stopped).await });
        let db = pool.clone();
        let cfg = config.clone();
        let stopped = receiver.clone();
        tasks.spawn(async move { monitors(db, cfg, stopped).await });
    }
    tracing::info!(mode, "Backend ready; press Ctrl+C to stop");
    let failure = tokio::select! {
        _=shutdown_signal()=>None,
        completed=tasks.join_next()=>Some(completed),
    };
    tracing::info!("Stopping backend services");
    let _ = stop.send(true);
    let drained = tokio::time::timeout(Duration::from_secs(12), async {
        while let Some(result) = tasks.join_next().await {
            if !matches!(result, Ok(Ok(()))) {
                tracing::warn!("A service exited during shutdown");
            }
        }
    })
    .await;
    if drained.is_err() {
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    }
    if let Some(completed) = failure {
        match completed {
            Some(Ok(Err(error))) => return Err(error),
            Some(Err(error)) => return Err(error.into()),
            _ => anyhow::bail!("A backend service exited unexpectedly"),
        }
    }
    tracing::info!("All backend services stopped");
    Ok(())
}

async fn chat_worker(pool: PgPool, config: Arc<Config>, stop: watch::Receiver<bool>) -> Result<()> {
    if config.model_name.is_none() || config.model_base_url.is_none() {
        tracing::info!(
            "Chat worker waiting for model configuration; data monitors remain available"
        );
        wait_for_stop(stop).await;
        return Ok(());
    }
    let mut backoff = Duration::from_secs(1);
    while !*stop.borrow() {
        let mut worker = match Worker::spawn(&config).await {
            Ok(worker) => {
                tracing::info!(sdk=%worker.sdk_version, "Strands worker ready");
                backoff = Duration::from_secs(1);
                worker
            }
            Err(error) => {
                tracing::error!(%error, "Strands worker failed to start; retrying after {:?}", backoff);
                tokio::select! {
                    _=wait_for_stop(stop.clone())=>break,
                    _=tokio::time::sleep(backoff)=>{}
                }
                backoff = (backoff * 2).min(Duration::from_secs(30));
                continue;
            }
        };
        while !*stop.borrow() {
            match agent::work_one_until(&pool, config.clone(), stop.clone(), &mut worker).await {
                Ok(Worked::Done) => continue,
                Ok(Worked::Idle) => {}
                Ok(Worked::WorkerLost) => {
                    tracing::warn!("Strands worker lost; restarting it");
                    break;
                }
                Err(error) => {
                    tracing::warn!(%error, "Agent worker operation failed; retrying after poll interval");
                }
            }
            tokio::select! {
                _=wait_for_stop(stop.clone())=>break,
                _=tokio::time::sleep(config.worker_poll)=>{}
            }
        }
        worker.shutdown().await;
        if !*stop.borrow() {
            tokio::select! {
                _=wait_for_stop(stop.clone())=>break,
                _=tokio::time::sleep(backoff)=>{}
            }
        }
    }
    Ok(())
}

async fn monitors(pool: PgPool, config: Arc<Config>, stop: watch::Receiver<bool>) -> Result<()> {
    let instance = Uuid::new_v4();
    let review = config.model_name.is_some() && config.model_base_url.is_some();
    tracing::info!(
        strands_review = review,
        "Sales and Inventory monitors ready"
    );
    while !*stop.borrow() {
        let cycle = async {
            scoped_agents::heartbeat(&pool, &config.workspace_id, instance).await?;
            // At most one pending check per role per cycle; changes are coalesced.
            for _ in 0..2 {
                if !scoped_agents::check_one(&pool, &config.workspace_id, review).await? {
                    break;
                }
            }
            Ok::<_, crate::error::Error>(())
        };
        tokio::select! {
            _=wait_for_stop(stop.clone())=>break,
            result=cycle=>if result.is_err() { tracing::warn!("Agent check failed; checkpoint retained for retry"); }
        }
        tokio::select! {
            _=wait_for_stop(stop.clone())=>break,
            _=tokio::time::sleep(Duration::from_secs(2))=>{}
        }
    }
    scoped_agents::disconnect(&pool, instance).await?;
    Ok(())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("SIGTERM handler");
        tokio::select! { _=tokio::signal::ctrl_c()=>{}, _=term.recv()=>{} }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
