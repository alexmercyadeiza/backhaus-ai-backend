mod common;
use backhaus_ai_backend::{
    agent,
    jobs::{self, Lane},
    outreach, research, scoped_agents, settings,
    worker::Worker,
};
use serde_json::json;
use std::{sync::Arc, time::Duration};
use uuid::Uuid;

async fn configure(pool: &sqlx::PgPool, cfg: &mut backhaus_ai_backend::config::Config) {
    cfg.model_base_url = Some("https://openrouter.ai/api/v1".into());
    cfg.model_api_key = "unused-test-key".into();
    settings::save(
        pool,
        &cfg.workspace_id,
        cfg,
        settings::SettingsInput {
            business_name: "Demo".into(),
            city: "Abuja".into(),
            country: "Nigeria".into(),
            reply_to: String::new(),
            auto_reply: true,
            version: 0,
        },
    )
    .await
    .unwrap();
}
async fn chat(pool: &sqlx::PgPool, w: &str, key: &str) -> Uuid {
    let c = common::conversation(pool, w).await;
    jobs::enqueue(
        pool,
        w,
        c,
        key,
        json!({"message":"Find vendors for medium roast coffee beans"}),
    )
    .await
    .unwrap()["run_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap()
}
#[tokio::test]
#[ignore = "Requires dedicated PostgreSQL test database"]
async fn chat_dispatch_uses_real_protocol_and_preserves_freeform_outreach() {
    let (pool, base) = common::setup().await;
    let mut cfg = common::fake_worker_config(&base, "chat_dispatch");
    configure(&pool, &mut cfg).await;
    let cfg = Arc::new(cfg);
    let w = &cfg.workspace_id;
    let chat_id = chat(&pool, w, "dispatch").await;
    let mut worker = Worker::spawn(&cfg).await.unwrap();
    let (_keep, stop) = tokio::sync::watch::channel(false);
    assert_eq!(
        agent::work_one_in_lane(&pool, cfg.clone(), stop, &mut worker, Lane::Chat)
            .await
            .unwrap(),
        agent::Worked::Done
    );
    worker.shutdown().await;
    let parent = jobs::get(&pool, w, chat_id).await.unwrap();
    assert_eq!(parent["status"], "completed", "{parent}");
    let research = research::latest(&pool, w).await.unwrap();
    assert_eq!(research["status"], "queued");
    assert_eq!(research["input"]["items"], json!([]));
    assert!(
        research["input"]["request"]
            .as_str()
            .unwrap()
            .contains("Coffee beans")
    );
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM inventory_items WHERE workspace_id=$1")
            .bind(w)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count, 50, "Research does not insert made-up inventory");
    let event: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent_events WHERE run_id=$1 AND event_type='research_dispatched')").bind(chat_id).fetch_one(&pool).await.unwrap();
    assert!(event);
    assert!(
        jobs::claim_in_lane(&pool, w, Lane::Chat)
            .await
            .unwrap()
            .is_none()
    );
    let supplier = common::vendor(&pool, w, "Coffee supplier", false).await;
    sqlx::query("UPDATE vendors SET source='web_research',phone='+2348001112222',evidence=$3 WHERE workspace_id=$1 AND id=$2")
        .bind(w).bind(supplier).bind(json!([{"run_id":research["id"]}])).execute(&pool).await.unwrap();
    let approved = outreach::approve_candidate(&pool, w, &cfg, supplier)
        .await
        .unwrap();
    assert_eq!(approved["status"], "whatsapp_ready"); // No network send.
    let t: Uuid = approved["thread_id"].as_str().unwrap().parse().unwrap();
    let d = outreach::detail(&pool, w, t).await.unwrap();
    let body = d["thread"]["initial_body"].as_str().unwrap();
    assert!(body.contains("Coffee beans, medium roast"));
    assert!(!body.contains("to replenish"));
    assert!(!body.contains("Item 00"));
    assert_eq!(
        outreach::approve_candidate(&pool, w, &cfg, supplier)
            .await
            .unwrap()["thread_id"],
        approved["thread_id"]
    );
}
#[tokio::test]
#[ignore = "Requires dedicated PostgreSQL test database"]
async fn procurement_pause_is_independent_and_dispatch_retries_reuse_one_job() {
    let (pool, mut cfg) = common::setup().await;
    configure(&pool, &mut cfg).await;
    let w = &cfg.workspace_id;
    let list = scoped_agents::list(&pool, w).await.unwrap();
    assert_eq!(list["agents"].as_array().unwrap().len(), 3);
    assert_eq!(list["agents"][2]["id"], "procurement");
    assert_eq!(list["agents"][0]["enabled"], false);
    chat(&pool, w, "retry").await;
    let run = jobs::claim_in_lane(&pool, w, Lane::Chat)
        .await
        .unwrap()
        .unwrap();
    let brief = || research::ChatResearchRequest {
        request: "Compostable takeaway boxes".into(),
    };
    let first = research::dispatch_from_chat(&pool, &cfg, &run, brief())
        .await
        .unwrap();
    let again = research::dispatch_from_chat(&pool, &cfg, &run, brief())
        .await
        .unwrap();
    assert_eq!(first["run_id"], again["run_id"]);
    assert_eq!(again["reused"], true);
    let bg = jobs::claim_in_lane(&pool, w, Lane::Background)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(bg.kind, "vendor_research");
    let paused = scoped_agents::control(&pool, w, "procurement", "pause")
        .await
        .unwrap();
    assert_eq!(paused["agents"][1]["enabled"], true);
    assert!(!jobs::active(&pool, &bg).await.unwrap());
    assert!(jobs::active(&pool, &run).await.unwrap());
    assert_eq!(
        jobs::get(&pool, w, bg.id).await.unwrap()["status"],
        "paused"
    );
    assert!(
        research::dispatch_from_chat(&pool, &cfg, &run, brief())
            .await
            .is_err()
    );
    assert!(
        jobs::claim_in_lane(&pool, w, Lane::Background)
            .await
            .unwrap()
            .is_none()
    );
    scoped_agents::control(&pool, w, "procurement", "resume")
        .await
        .unwrap();
    assert_eq!(
        jobs::claim_in_lane(&pool, w, Lane::Background)
            .await
            .unwrap()
            .unwrap()
            .id,
        bg.id
    );
    let mut foreign = run.clone();
    foreign.workspace = "foreign".into();
    assert!(
        research::dispatch_from_chat(&pool, &cfg, &foreign, brief())
            .await
            .is_err()
    );
}
#[tokio::test]
#[ignore = "Requires dedicated PostgreSQL test database"]
async fn chat_finishes_while_a_separate_worker_is_busy_researching() {
    let (pool, base) = common::setup().await;
    let w = &base.workspace_id;
    let mut tx = pool.begin().await.unwrap();
    let bg_id = jobs::enqueue_system(
        &mut tx,
        w,
        "vendor_research",
        "slow",
        json!({"request":"Coffee beans","items":[],"city":"Abuja","country":"Nigeria"}),
    )
    .await
    .unwrap()
    .unwrap();
    tx.commit().await.unwrap();
    let bg_cfg = Arc::new(common::fake_worker_config(&base, "hang"));
    let mut bg_worker = Worker::spawn(&bg_cfg).await.unwrap();
    let (stop, receiver) = tokio::sync::watch::channel(false);
    let db = pool.clone();
    let bg_task = tokio::spawn(async move {
        let result =
            agent::work_one_in_lane(&db, bg_cfg, receiver, &mut bg_worker, Lane::Background).await;
        bg_worker.shutdown().await;
        result
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if jobs::get(&pool, w, bg_id).await.unwrap()["status"] == "running" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let id = chat(&pool, w, "while-researching").await;
    let chat_cfg = Arc::new(common::fake_worker_config(&base, "tool_then_answer"));
    let mut chat_worker = Worker::spawn(&chat_cfg).await.unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        agent::work_one_in_lane(
            &pool,
            chat_cfg,
            stop.subscribe(),
            &mut chat_worker,
            Lane::Chat,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(result, agent::Worked::Done);
    assert_eq!(
        jobs::get(&pool, w, id).await.unwrap()["status"],
        "completed"
    );
    assert_eq!(
        jobs::get(&pool, w, bg_id).await.unwrap()["status"],
        "running"
    );
    stop.send(true).unwrap();
    chat_worker.shutdown().await;
    bg_task.await.unwrap().unwrap();
}

#[tokio::test]
#[ignore = "Requires PostgreSQL and BACKHAUS_LIVE_CHAT_DISPATCH=1; one live chat, no supplier outreach"]
async fn live_chat_dispatches_procurement_for_an_item_outside_inventory() {
    if std::env::var("BACKHAUS_LIVE_CHAT_DISPATCH").as_deref() != Ok("1") {
        return;
    }
    let (pool, mut cfg) = common::setup().await;
    configure(&pool, &mut cfg).await;
    let live = backhaus_ai_backend::config::Config::from_env().unwrap();
    cfg.model_name = live.model_name;
    cfg.model_base_url = live.model_base_url;
    cfg.model_api_key = live.model_api_key;
    cfg.model_request_options = live.model_request_options;
    cfg.model_timeout = Duration::from_secs(90);
    let cfg = Arc::new(cfg);
    let w = &cfg.workspace_id;
    let id = chat(&pool, w, "live-dispatch").await;
    let mut worker = Worker::spawn(&cfg).await.unwrap();
    let (_keep, stop) = tokio::sync::watch::channel(false);
    let result = agent::work_one_in_lane(&pool, cfg.clone(), stop, &mut worker, Lane::Chat).await;
    worker.shutdown().await;
    assert_eq!(result.unwrap(), agent::Worked::Done);
    let state = jobs::get(&pool, w, id).await.unwrap();
    assert_eq!(state["status"], "completed", "{state}");
    let queued = research::latest(&pool, w).await.unwrap();
    assert_eq!(
        queued["status"], "queued",
        "Chat must actually dispatch the job"
    );
    assert!(
        queued["input"]["request"]
            .as_str()
            .unwrap()
            .to_lowercase()
            .contains("coffee")
    );
    // This test exercises the real model's routing decision, but never starts the
    // background research or sends supplier messages.
    jobs::control(
        &pool,
        w,
        queued["id"].as_str().unwrap().parse().unwrap(),
        "cancel",
    )
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "Requires dedicated PostgreSQL test database"]
async fn explicit_retry_preserves_free_text_request_and_is_scoped_and_idempotent() {
    let (pool, mut cfg) = common::setup().await;
    configure(&pool, &mut cfg).await;
    let w = &cfg.workspace_id;
    let input =
        json!({"request":"Lamb meat, halal cuts","items":[],"city":"Abuja","country":"Nigeria"});
    let mut tx = pool.begin().await.unwrap();
    let source = jobs::enqueue_system(&mut tx, w, "vendor_research", "retry-source", input.clone())
        .await
        .unwrap()
        .unwrap();
    tx.commit().await.unwrap();
    assert!(
        research::retry(&pool, w, &cfg, source).await.is_err(),
        "Do not retry a queued search"
    );
    sqlx::query("UPDATE agent_runs SET status='completed',result=$2 WHERE id=$1")
        .bind(source)
        .bind(json!({"saved_count":0,"searches_used":0}))
        .execute(&pool)
        .await
        .unwrap();
    let (a, b) = tokio::join!(
        research::retry(&pool, w, &cfg, source),
        research::retry(&pool, w, &cfg, source)
    );
    let a = a.unwrap();
    let b = b.unwrap();
    assert_eq!(a["run_id"], b["run_id"]);
    let id: Uuid = a["run_id"].as_str().unwrap().parse().unwrap();
    let new = jobs::get(&pool, w, id).await.unwrap();
    assert_eq!(new["input"]["request"], input["request"]);
    assert_eq!(new["input"]["items"], json!([]));
    assert_eq!(new["input"]["city"], "Abuja");
    assert_eq!(new["input"]["retry_of"], source.to_string());
    assert_eq!(new["status"], "queued");
    assert!(
        research::retry(&pool, "other-workspace", &cfg, source)
            .await
            .is_err()
    );
    scoped_agents::control(&pool, w, "procurement", "pause")
        .await
        .unwrap();
    assert!(research::retry(&pool, w, &cfg, source).await.is_err());
}

#[tokio::test]
#[ignore = "Requires dedicated PostgreSQL test database"]
async fn discarding_research_cancels_work_and_hides_activity_without_deleting_suppliers() {
    let (pool, cfg) = common::setup().await;
    let w = &cfg.workspace_id;
    let supplier = common::vendor(&pool, w, "Saved supplier", false).await;
    let mut tx = pool.begin().await.unwrap();
    let id = jobs::enqueue_system(
        &mut tx,
        w,
        "vendor_research",
        "discard-me",
        json!({"request":"Lamb meat"}),
    )
    .await
    .unwrap()
    .unwrap();
    tx.commit().await.unwrap();
    let run = jobs::claim_in_lane(&pool, w, Lane::Background)
        .await
        .unwrap()
        .unwrap();
    assert!(jobs::active(&pool, &run).await.unwrap());
    assert!(
        research::discard(&pool, "other-workspace", id)
            .await
            .is_err()
    );
    assert_eq!(
        research::discard(&pool, w, id).await.unwrap()["discarded"],
        true
    );
    assert_eq!(
        research::discard(&pool, w, id).await.unwrap()["reused"],
        true
    );
    assert!(!jobs::active(&pool, &run).await.unwrap());
    assert_eq!(
        jobs::get(&pool, w, id).await.unwrap()["status"],
        "cancelled"
    );
    assert!(research::latest(&pool, w).await.unwrap().is_null());
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM vendors WHERE workspace_id=$1 AND id=$2)")
            .bind(w)
            .bind(supplier)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(exists);
    let mut tx = pool.begin().await.unwrap();
    let next = jobs::enqueue_system(
        &mut tx,
        w,
        "vendor_research",
        "something-else",
        json!({"request":"Coffee beans"}),
    )
    .await
    .unwrap()
    .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        research::latest(&pool, w).await.unwrap()["id"],
        next.to_string()
    );
    sqlx::query("UPDATE agent_runs SET status='completed',result=$2 WHERE id=$1")
        .bind(next)
        .bind(json!({"saved_count":1}))
        .execute(&pool)
        .await
        .unwrap();
    research::discard(&pool, w, next).await.unwrap();
    assert_eq!(
        jobs::get(&pool, w, next).await.unwrap()["status"],
        "completed",
        "Completed history is preserved"
    );
    assert!(
        research::latest(&pool, w).await.unwrap().is_null(),
        "Dismissal must not resurface an older search"
    );
}
