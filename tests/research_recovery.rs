mod common;
use backhaus_ai_backend::{agent, jobs, research, worker::Worker};
use serde_json::{Value, json};
use std::sync::Arc;
use uuid::Uuid;

async fn fixture() -> (
    sqlx::PgPool,
    backhaus_ai_backend::config::Config,
    jobs::Run,
    Vec<Uuid>,
) {
    let (pool, cfg) = common::setup().await;
    let mut tx = pool.begin().await.unwrap();
    jobs::enqueue_system(
        &mut tx,
        &cfg.workspace_id,
        "vendor_research",
        "research-recovery",
        json!({"city":"Abuja","country":"Nigeria","items":[]}),
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let run = jobs::claim(&pool, &cfg.workspace_id)
        .await
        .unwrap()
        .unwrap();
    let mut searches = Vec::new();
    for index in 0..5 {
        let id = Uuid::new_v4();
        let query = format!(
            "cached query {index} near Abuja, Nigeria. Required purchase quantities: . Find suppliers for restaurant-sized purchases, not container/export-only minimums. Include public supplier contacts and independent customer reviews or Google reviews where available."
        );
        let source = json!({"url":"https://supplier.example/contact","title":"Public supplier evidence","content":format!("Supplier {index} supplies fresh produce. Email sales{index}@supplier.example. {}","₦".repeat(1500))});
        sqlx::query("INSERT INTO vendor_searches(id,workspace_id,run_id,query,sources) VALUES($1,$2,$3,$4,$5)").bind(id).bind(&cfg.workspace_id).bind(run.id).bind(query).bind(json!([source])).execute(&pool).await.unwrap();
        searches.push(id);
    }
    for (index, search_id) in searches.iter().take(3).enumerate() {
        let candidate = || research::Candidate {
            category: None,
            search_id: *search_id,
            source_index: 0,
            name: format!("Supplier {index}"),
            email: Some(format!("sales{index}@supplier.example")),
            phone: None,
            evidence_quote: format!("Supplier {index} supplies fresh produce."),
        };
        let saved = research::save(&pool, &run, candidate()).await.unwrap();
        let repeated = research::save(&pool, &run, candidate()).await.unwrap();
        assert_eq!(saved["id"], repeated["id"], "retry must reuse the supplier");
        if index < 2 {
            research::vet(
                &pool,
                &run,
                research::Vetting {
                    review_source_indices: vec![],
                    vendor_id: saved["id"].as_str().unwrap().parse().unwrap(),
                    summary: "No independent reviews found in this search.".into(),
                    reviews_found: false,
                    search_id: *search_id,
                    source_indices: vec![],
                },
            )
            .await
            .unwrap();
        }
    }
    (pool, cfg, run, searches)
}

#[tokio::test]
#[ignore = "Requires dedicated PostgreSQL test database"]
async fn exhausted_budget_recovers_sources_without_network_and_enforces_scope() {
    let (pool, cfg, run, searches) = fixture().await;
    let checkpoint = research::checkpoint(&pool, &run).await.unwrap();
    assert_eq!(checkpoint["searches_used"], 5);
    assert_eq!(checkpoint["searches_remaining"], 0);
    assert_eq!(checkpoint["saved_suppliers"].as_array().unwrap().len(), 3);
    assert!(
        checkpoint.to_string().len() < 10_000,
        "bounded checkpoint must not embed full searches"
    );
    let budget = research::search(
        &pool,
        &cfg,
        &run,
        research::SearchInput {
            query: "another query".into(),
        },
    )
    .await
    .unwrap();
    assert_eq!(budget["status"], "budget_exhausted");
    assert_eq!(
        budget["checkpoint"]["saved_suppliers"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    // Config has no live search provider: succeeding proves the cached path uses no network.
    let cached = research::search(
        &pool,
        &cfg,
        &run,
        research::SearchInput {
            query: "cached query 0".into(),
        },
    )
    .await
    .unwrap();
    assert_eq!(cached["cached"], true);
    assert_eq!(cached["search_id"], searches[0].to_string());
    let source = || research::SourceLookup {
        search_id: searches[0],
        source_index: 0,
    };
    assert!(
        research::source(&pool, &run, source()).await.unwrap()["source"]["content"]
            .as_str()
            .unwrap()
            .len()
            > 4000
    );
    let mut foreign = run.clone();
    foreign.workspace = "another-workspace".into();
    assert!(research::source(&pool, &foreign, source()).await.is_err());
    assert_eq!(
        research::checkpoint(&pool, &foreign).await.unwrap()["searches_used"],
        0
    );
    foreign = run.clone();
    foreign.id = Uuid::new_v4();
    assert!(research::source(&pool, &foreign, source()).await.is_err());
    let outcome = research::outcome(&pool, &run).await.unwrap();
    assert_eq!(outcome["saved_count"], 3);
    assert_eq!(outcome["assessed_count"], 2);
    assert_eq!(outcome["outcome"], "partial");
    assert!(
        outcome["answer"]
            .as_str()
            .unwrap()
            .contains("Saved 3 supplier candidates")
    );
}

#[tokio::test]
#[ignore = "Requires dedicated PostgreSQL test database"]
async fn worker_retry_receives_checkpoint_and_cannot_report_zero_saved_suppliers() {
    let (pool, cfg, run, _) = fixture().await;
    jobs::finish(&pool, &run, None, Some("model_or_tool_failed"))
        .await
        .unwrap();
    sqlx::query("UPDATE agent_runs SET available_at=now() WHERE id=$1")
        .bind(run.id)
        .execute(&pool)
        .await
        .unwrap();
    let cfg = Arc::new(common::fake_worker_config(&cfg, "research_resume"));
    let mut worker = Worker::spawn(&cfg).await.unwrap();
    let result = agent::work_one(&pool, cfg.clone(), &mut worker).await;
    worker.shutdown().await;
    assert_eq!(result.unwrap(), agent::Worked::Done);
    let persisted = jobs::get(&pool, &cfg.workspace_id, run.id).await.unwrap();
    assert_eq!(persisted["status"], "completed", "{persisted}");
    assert_eq!(persisted["attempt"], 2);
    assert_eq!(persisted["result"]["saved_count"], 3);
    assert_eq!(persisted["result"]["summary_source"], "database");
    assert!(
        persisted["result"]["answer"]
            .as_str()
            .unwrap()
            .starts_with("Saved 3 supplier candidates")
    );
    assert_eq!(
        persisted["result"]["model_answer"],
        "I saved zero suppliers and ran no searches."
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM vendor_searches WHERE run_id=$1")
        .bind(run.id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 5);
    let events: Vec<Value> = sqlx::query_scalar(
        "SELECT payload FROM agent_events WHERE run_id=$1 AND event_type='completed'",
    )
    .bind(run.id)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(events[0]["result"]["saved_count"], 3);
}

#[tokio::test]
#[ignore = "Requires BACKHAUS_LIVE_RECOVERY=1; uses hosted inference, no new web searches"]
async fn live_strands_resumes_exhausted_research_from_checkpoint() {
    if std::env::var("BACKHAUS_LIVE_RECOVERY").as_deref() != Ok("1") {
        return;
    }
    let (pool, mut cfg, run, _) = fixture().await;
    let live = backhaus_ai_backend::config::Config::from_env().unwrap();
    cfg.model_name = live.model_name;
    cfg.model_api_key = live.model_api_key;
    cfg.model_base_url = live.model_base_url;
    cfg.model_request_options = live.model_request_options;
    jobs::finish(&pool, &run, None, Some("model_or_tool_failed"))
        .await
        .unwrap();
    sqlx::query("UPDATE agent_runs SET available_at=now() WHERE id=$1")
        .bind(run.id)
        .execute(&pool)
        .await
        .unwrap();
    let cfg = Arc::new(cfg);
    let mut worker = Worker::spawn(&cfg).await.unwrap();
    let result = agent::work_one(&pool, cfg.clone(), &mut worker).await;
    worker.shutdown().await;
    assert_eq!(result.unwrap(), agent::Worked::Done);
    let persisted = jobs::get(&pool, &cfg.workspace_id, run.id).await.unwrap();
    assert_eq!(persisted["status"], "completed", "{persisted}");
    assert_eq!(persisted["result"]["saved_count"], 3);
    assert_eq!(persisted["result"]["searches_used"], 5);
    assert_eq!(persisted["result"]["searches_remaining"], 0);
    assert_eq!(persisted["result"]["agent_sdk"], "strands-typescript");
    println!(
        "Live recovery completed: attempt={}, searches={}, suppliers={}, model calls={}",
        persisted["attempt"],
        persisted["result"]["searches_used"],
        persisted["result"]["saved_count"],
        persisted["result"]["model_calls"]
    );
}

#[tokio::test]
#[ignore = "Requires BACKHAUS_LIVE_LAMB_SEARCH=1; one live search, no supplier outreach"]
async fn live_lamb_search_saves_evidence_with_production_database_timeouts() {
    if std::env::var("BACKHAUS_LIVE_LAMB_SEARCH").as_deref() != Ok("1") {
        return;
    }
    let (base_pool, mut cfg) = common::setup().await;
    let live = backhaus_ai_backend::config::Config::from_env().unwrap();
    cfg.model_name = live.model_name;
    cfg.model_base_url = live.model_base_url;
    cfg.model_api_key = live.model_api_key;
    cfg.model_request_options = live.model_request_options;
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .after_connect(|c, _| {
            Box::pin(async move {
                sqlx::query("SET statement_timeout='15s'")
                    .execute(&mut *c)
                    .await?;
                sqlx::query("SET idle_in_transaction_session_timeout='30s'")
                    .execute(c)
                    .await?;
                Ok(())
            })
        })
        .connect(&cfg.database_url)
        .await
        .unwrap();
    let mut tx = pool.begin().await.unwrap();
    jobs::enqueue_system(&mut tx,&cfg.workspace_id,"vendor_research","live-lamb-search",json!({"request":"Lamb meat for a restaurant; no fixed quantity or cuts yet","items":[],"city":"Abuja","country":"Nigeria"})).await.unwrap();
    tx.commit().await.unwrap();
    let run = jobs::claim(&pool, &cfg.workspace_id)
        .await
        .unwrap()
        .unwrap();
    let result = research::search(
        &pool,
        &cfg,
        &run,
        research::SearchInput {
            query: "Abuja Nigeria lamb meat suppliers restaurant butcher contact".into(),
        },
    )
    .await
    .unwrap();
    assert!(!result["sources"].as_array().unwrap().is_empty());
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM vendor_searches WHERE run_id=$1")
        .bind(run.id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
    jobs::control(&pool, &cfg.workspace_id, run.id, "cancel")
        .await
        .unwrap();
    pool.close().await;
    base_pool.close().await;
}
