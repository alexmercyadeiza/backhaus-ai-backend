//! Supervision of the worker process: completion, crash recovery, cancellation,
//! stale messages, unresponsive workers, startup failures and shutdown.
mod common;
use backhaus_ai_backend::{
    agent::{self, Worked},
    jobs,
    worker::Worker,
};
use common::*;
use serde_json::{Value, json};
use sqlx::PgPool;
use std::{sync::Arc, time::Duration};
use uuid::Uuid;

async fn enqueue(pool: &PgPool, workspace: &str, key: &str) -> Uuid {
    let c = conversation(pool, workspace).await;
    let queued = jobs::enqueue(pool, workspace, c, key, json!({"message":"Count stock"}))
        .await
        .unwrap();
    queued["run_id"].as_str().unwrap().parse().unwrap()
}
async fn events(pool: &PgPool, run: Uuid) -> Vec<(String, Value)> {
    sqlx::query_as::<_, (String, Value)>(
        "SELECT event_type,payload FROM agent_events WHERE run_id=$1 ORDER BY id",
    )
    .bind(run)
    .fetch_all(pool)
    .await
    .unwrap()
}

#[tokio::test]
#[ignore = "Requires dedicated PostgreSQL test database"]
async fn worker_completes_a_run_through_a_validated_tool() {
    let (pool, base) = setup().await;
    let cfg = Arc::new(fake_worker_config(&base, "tool_then_answer"));
    let mut worker = Worker::spawn(&cfg).await.unwrap();
    assert!(worker.is_alive());
    let run = enqueue(&pool, &cfg.workspace_id, "complete").await;
    assert_eq!(
        agent::work_one(&pool, cfg.clone(), &mut worker)
            .await
            .unwrap(),
        Worked::Done
    );
    let state = jobs::get(&pool, &cfg.workspace_id, run).await.unwrap();
    assert_eq!(state["status"], "completed", "{state}");
    assert_eq!(state["result"]["answer"], "Inventory read: 50 items.");
    assert_eq!(state["result"]["agent_sdk"], "strands-typescript");
    let kinds: Vec<String> = events(&pool, run).await.into_iter().map(|e| e.0).collect();
    assert!(kinds.contains(&"tool_started".into()) && kinds.contains(&"tool_completed".into()));
    assert!(kinds.contains(&"text_delta".into()));
    assert_eq!(
        agent::work_one(&pool, cfg.clone(), &mut worker)
            .await
            .unwrap(),
        Worked::Idle
    );
    let pid = worker.pid().unwrap();
    worker.shutdown().await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let alive = std::process::Command::new("kill")
        .args(["-0", &pid.to_string()])
        .status()
        .unwrap()
        .success();
    assert!(!alive, "worker process must exit on shutdown");
}

#[tokio::test]
#[ignore = "Requires dedicated PostgreSQL test database"]
async fn worker_crash_mid_run_requeues_without_stale_output_and_recovers_with_a_new_worker() {
    let (pool, base) = setup().await;
    let crashing = Arc::new(fake_worker_config(&base, "crash_after_tool_call"));
    let mut worker = Worker::spawn(&crashing).await.unwrap();
    let run = enqueue(&pool, &crashing.workspace_id, "crash").await;
    assert_eq!(
        agent::work_one(&pool, crashing.clone(), &mut worker)
            .await
            .unwrap(),
        Worked::WorkerLost
    );
    let state = jobs::get(&pool, &crashing.workspace_id, run).await.unwrap();
    assert_eq!(state["status"], "queued", "{state}");
    assert_eq!(state["error_code"], "worker_unavailable");
    assert_eq!(state["attempt"], 1);
    assert!(state["result"].is_null());
    assert_eq!(
        agent::work_one(&pool, crashing.clone(), &mut worker)
            .await
            .unwrap(),
        Worked::WorkerLost,
        "a dead worker is never given more work"
    );
    sqlx::query("UPDATE agent_runs SET available_at=now() WHERE id=$1")
        .bind(run)
        .execute(&pool)
        .await
        .unwrap();
    let healthy = Arc::new(fake_worker_config(&base, "tool_then_answer"));
    let mut replacement = Worker::spawn(&healthy).await.unwrap();
    assert_eq!(
        agent::work_one(&pool, healthy.clone(), &mut replacement)
            .await
            .unwrap(),
        Worked::Done
    );
    let state = jobs::get(&pool, &healthy.workspace_id, run).await.unwrap();
    assert_eq!(state["status"], "completed");
    assert_eq!(state["attempt"], 2);
    replacement.shutdown().await;
}

#[tokio::test]
#[ignore = "Requires dedicated PostgreSQL test database"]
async fn cancel_during_a_run_stops_the_worker_and_records_no_result() {
    let (pool, base) = setup().await;
    let cfg = Arc::new(fake_worker_config(&base, "hang"));
    let mut worker = Worker::spawn(&cfg).await.unwrap();
    let run = enqueue(&pool, &cfg.workspace_id, "cancel").await;
    let canceller = {
        let pool = pool.clone();
        let workspace = cfg.workspace_id.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(1500)).await;
            jobs::control(&pool, &workspace, run, "cancel")
                .await
                .unwrap();
        })
    };
    let started = std::time::Instant::now();
    assert_eq!(
        agent::work_one(&pool, cfg.clone(), &mut worker)
            .await
            .unwrap(),
        Worked::Done
    );
    assert!(started.elapsed() < Duration::from_secs(8));
    canceller.await.unwrap();
    let state = jobs::get(&pool, &cfg.workspace_id, run).await.unwrap();
    assert_eq!(state["status"], "cancelled");
    assert!(state["result"].is_null());
    // The same worker keeps serving after a cooperative cancel.
    assert!(worker.is_alive());
    let next = enqueue(&pool, &cfg.workspace_id, "after-cancel").await;
    let hang_again = tokio::time::timeout(
        Duration::from_secs(3),
        agent::work_one(&pool, cfg.clone(), &mut worker),
    )
    .await;
    assert!(
        hang_again.is_err(),
        "the hanging double keeps running until cancelled"
    );
    jobs::control(&pool, &cfg.workspace_id, next, "cancel")
        .await
        .unwrap();
    worker.shutdown().await;
}

#[tokio::test]
#[ignore = "Requires dedicated PostgreSQL test database"]
async fn messages_for_other_runs_and_unknown_tools_are_rejected() {
    let (pool, base) = setup().await;
    let cfg = Arc::new(fake_worker_config(&base, "stale"));
    let mut worker = Worker::spawn(&cfg).await.unwrap();
    let run = enqueue(&pool, &cfg.workspace_id, "stale").await;
    assert_eq!(
        agent::work_one(&pool, cfg.clone(), &mut worker)
            .await
            .unwrap(),
        Worked::Done
    );
    let state = jobs::get(&pool, &cfg.workspace_id, run).await.unwrap();
    assert_eq!(state["status"], "completed", "{state}");
    let answer = state["result"]["answer"].as_str().unwrap();
    assert!(
        answer.starts_with("Finished after rejected tool: Unknown tool drop_database"),
        "{answer}"
    );
    let tools: Vec<String> = events(&pool, run)
        .await
        .into_iter()
        .filter(|e| e.0 == "tool_started")
        .map(|e| e.1["tool"].as_str().unwrap().to_owned())
        .collect();
    assert!(
        tools.is_empty(),
        "no tool ran for a stale or unknown request: {tools:?}"
    );
    worker.shutdown().await;
}

#[tokio::test]
#[ignore = "Requires dedicated PostgreSQL test database"]
async fn unresponsive_worker_times_out_and_is_replaced() {
    let (pool, base) = setup().await;
    let mut cfg = fake_worker_config(&base, "hang_ignore_cancel");
    cfg.model_timeout = Duration::from_secs(1);
    let cfg = Arc::new(cfg);
    let mut worker = Worker::spawn(&cfg).await.unwrap();
    let run = enqueue(&pool, &cfg.workspace_id, "timeout").await;
    assert_eq!(
        agent::work_one(&pool, cfg.clone(), &mut worker)
            .await
            .unwrap(),
        Worked::WorkerLost
    );
    let state = jobs::get(&pool, &cfg.workspace_id, run).await.unwrap();
    assert_eq!(state["status"], "queued");
    assert_eq!(state["error_code"], "model_timeout");
    let pid = worker.pid().unwrap();
    drop(worker);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let alive = std::process::Command::new("kill")
        .args(["-0", &pid.to_string()])
        .status()
        .unwrap()
        .success();
    assert!(!alive, "an abandoned worker must not linger");
}

#[tokio::test]
#[ignore = "Requires dedicated PostgreSQL test database"]
async fn startup_failures_are_explicit() {
    let (_pool, base) = setup().await;
    let missing = {
        let mut cfg = fake_worker_config(&base, "tool_then_answer");
        cfg.worker_script = std::path::PathBuf::from("/nonexistent/worker.js");
        cfg
    };
    let error = Worker::spawn(&missing).await.err().unwrap().to_string();
    assert!(error.contains("npm ci"), "{error}");
    let error = Worker::spawn(&fake_worker_config(&base, "no_ready"))
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("before reporting ready"), "{error}");
    let error = Worker::spawn(&fake_worker_config(&base, "bad_ready"))
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("protocol"), "{error}");
    let busy = Arc::new(fake_worker_config(&base, "busy"));
    let mut worker = Worker::spawn(&busy).await.unwrap();
    let run = enqueue(&_pool, &busy.workspace_id, "busy").await;
    assert_eq!(
        agent::work_one(&_pool, busy.clone(), &mut worker)
            .await
            .unwrap(),
        Worked::WorkerLost
    );
    assert_eq!(
        jobs::get(&_pool, &busy.workspace_id, run).await.unwrap()["error_code"],
        "worker_unavailable"
    );
    worker.shutdown().await;
}

#[tokio::test]
#[ignore = "Requires dedicated PostgreSQL test database"]
async fn interactive_chat_preempts_background_review_without_spending_an_attempt() {
    let (pool, base) = setup().await;
    let cfg = Arc::new(fake_worker_config(&base, "hang"));
    let w = cfg.workspace_id.clone();
    let mut tx = pool.begin().await.unwrap();
    let revision: i64 = sqlx::query_scalar(
        "SELECT checked_revision FROM scoped_agents WHERE workspace_id=$1 AND role='inventory'",
    )
    .bind(&w)
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    let review = jobs::enqueue_system(
        &mut tx,
        &w,
        "inventory_review",
        "preempt-note",
        json!({"revision":revision}),
    )
    .await
    .unwrap()
    .unwrap();
    tx.commit().await.unwrap();
    let mut worker = Worker::spawn(&cfg).await.unwrap();
    let p = pool.clone();
    let w2 = w.clone();
    let enqueue_chat = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        enqueue(&p, &w2, "interactive").await
    });
    assert_eq!(
        tokio::time::timeout(
            Duration::from_secs(4),
            agent::work_one(&pool, cfg, &mut worker)
        )
        .await
        .unwrap()
        .unwrap(),
        Worked::Done
    );
    let chat = enqueue_chat.await.unwrap();
    let state = jobs::get(&pool, &w, review).await.unwrap();
    assert_eq!(state["status"], "queued");
    assert_eq!(state["attempt"], 0);
    assert!(state["result"].is_null());
    assert_eq!(jobs::claim(&pool, &w).await.unwrap().unwrap().id, chat);
    worker.shutdown().await;
}
