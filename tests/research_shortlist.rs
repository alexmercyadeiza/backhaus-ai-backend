mod common;
use backhaus_ai_backend::{jobs, research};
use serde_json::json;
use uuid::Uuid;

#[tokio::test]
#[ignore = "Requires dedicated PostgreSQL test database"]
async fn category_caps_more_and_evidence_gated_recommendations() {
    let (pool, mut config) = common::setup().await;
    config.model_base_url = Some("https://openrouter.ai/api/v1".into());
    config.model_name = Some("test-model".into());
    config.model_api_key = "test-key".into();
    let w = &config.workspace_id;
    let mut tx = pool.begin().await.unwrap();
    jobs::enqueue_system(&mut tx,w,"vendor_research","shortlist-test",json!({"request":"Lamb meat and coffee beans","items":[],"city":"Abuja","country":"Nigeria"})).await.unwrap();
    tx.commit().await.unwrap();
    let run = jobs::claim(&pool, w).await.unwrap().unwrap();
    let plan = || research::CategoryPlan {
        categories: vec!["Meat".into(), "Coffee".into()],
    };
    assert_eq!(
        research::plan(&pool, &run, plan()).await.unwrap()["search_limit"],
        10
    );
    assert!(
        research::plan(
            &pool,
            &run,
            research::CategoryPlan {
                categories: vec!["Something else".into()]
            }
        )
        .await
        .is_err()
    );
    let search = Uuid::new_v4();
    let content = "Supplier 0 supplies lamb. Supplier 1 supplies lamb. Supplier 2 supplies lamb. Supplier 3 supplies coffee. Supplier 4 supplies lamb. Email sales@supplier.example.";
    let sources = json!([{"url":"https://supplier.example/products","title":"Supply catalogue","content":content},{"url":"https://reviews.example/supplier","title":"Customer reviews","content":"Customers report reliable restaurant deliveries and fresh lamb from Supplier 0. Delivery fees must be confirmed."}]);
    sqlx::query("INSERT INTO vendor_searches(id,workspace_id,run_id,query,sources) VALUES($1,$2,$3,'test evidence',$4)").bind(search).bind(w).bind(run.id).bind(&sources).execute(&pool).await.unwrap();
    let candidate = |index, category: &str, search_id| research::Candidate {
        category: Some(category.into()),
        search_id,
        source_index: 0,
        name: format!("Supplier {index}"),
        email: Some("sales@supplier.example".into()),
        phone: None,
        evidence_quote: format!(
            "Supplier {index} supplies {}.",
            if index == 3 { "coffee" } else { "lamb" }
        ),
    };
    let mut ids = Vec::new();
    for i in 0..3 {
        ids.push(
            research::save(&pool, &run, candidate(i, "Meat", search))
                .await
                .unwrap()["id"]
                .as_str()
                .unwrap()
                .parse::<Uuid>()
                .unwrap(),
        );
    }
    assert!(
        research::save(&pool, &run, candidate(4, "Meat", search))
            .await
            .is_err(),
        "Fourth vendor in a category must be rejected"
    );
    assert!(
        research::save(&pool, &run, candidate(3, "Coffee", search))
            .await
            .is_ok(),
        "Another category has its own allowance"
    );
    assert_eq!(
        research::save(&pool, &run, candidate(0, "Meat", search))
            .await
            .unwrap()["id"],
        ids[0].to_string(),
        "Repeated saves reuse the existing slot"
    );
    let pick = || {
        research::Recommendation{category:"Meat".into(),vendor_id:ids[0],reason:"Best product fit with sourced positive restaurant delivery reviews; alternatives lack reviews. Confirm delivery fees before ordering.".into()}
    };
    assert!(
        research::recommend(&pool, &run, pick()).await.is_err(),
        "Unvetted supplier cannot be recommended"
    );
    for id in &ids {
        research::vet(
            &pool,
            &run,
            research::Vetting {
                vendor_id: *id,
                summary: "No independent reviews found in this search.".into(),
                reviews_found: false,
                search_id: search,
                source_indices: vec![0],
                review_source_indices: vec![],
            },
        )
        .await
        .unwrap();
    }
    assert!(
        research::recommend(&pool, &run, pick()).await.is_err(),
        "Contact details alone do not justify recommendation"
    );
    research::vet(&pool,&run,research::Vetting{vendor_id:ids[0],summary:"Positive independent customer reports about lamb and restaurant deliveries; confirm delivery fees.".into(),reviews_found:true,search_id:search,source_indices:vec![0,1],review_source_indices:vec![1]}).await.unwrap();
    research::recommend(&pool, &run, pick()).await.unwrap();
    research::recommend(&pool, &run, pick()).await.unwrap();
    let state = research::checkpoint(&pool, &run).await.unwrap();
    assert_eq!(state["recommendations"].as_array().unwrap().len(), 1);
    assert_eq!(state["searches_remaining"], 9);
    let detail: serde_json::Value =
        sqlx::query_scalar("SELECT vetting FROM vendors WHERE workspace_id=$1 AND id=$2")
            .bind(w)
            .bind(ids[0])
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        detail["review_sources"][0]["url"],
        "https://reviews.example/supplier"
    );
    assert!(
        research::more(
            &pool,
            w,
            &config,
            run.id,
            research::MoreRequest {
                category: Some("Meat".into())
            }
        )
        .await
        .is_err(),
        "Cannot expand a running search"
    );
    sqlx::query(
        "UPDATE agent_runs SET status='completed',lease_token=NULL,lease_until=NULL WHERE id=$1",
    )
    .bind(run.id)
    .execute(&pool)
    .await
    .unwrap();
    let (a, b) = tokio::join!(
        research::more(
            &pool,
            w,
            &config,
            run.id,
            research::MoreRequest {
                category: Some("Meat".into())
            }
        ),
        research::more(
            &pool,
            w,
            &config,
            run.id,
            research::MoreRequest {
                category: Some("Meat".into())
            }
        )
    );
    let more = a.unwrap();
    assert_eq!(more["run_id"], b.unwrap()["run_id"]);
    assert!(
        research::more(
            &pool,
            "other-workspace",
            &config,
            run.id,
            research::MoreRequest { category: None }
        )
        .await
        .is_err()
    );
    assert!(
        research::more(
            &pool,
            w,
            &config,
            run.id,
            research::MoreRequest {
                category: Some("Unrequested".into())
            }
        )
        .await
        .is_err()
    );
    let next = jobs::claim(&pool, w).await.unwrap().unwrap();
    assert_eq!(next.id.to_string(), more["run_id"]);
    assert_eq!(next.input["categories"], json!(["Meat"]));
    assert_eq!(
        next.input["available_categories"],
        json!(["Meat", "Coffee"])
    );
    assert_eq!(next.input["request"], run.input["request"]);
    assert_eq!(
        next.input["recommendations"][0]["vendor_id"],
        ids[0].to_string()
    );
    let search2 = Uuid::new_v4();
    sqlx::query("INSERT INTO vendor_searches(id,workspace_id,run_id,query,sources) VALUES($1,$2,$3,'more evidence',$4)").bind(search2).bind(w).bind(next.id).bind(&sources).execute(&pool).await.unwrap();
    assert!(
        research::save(&pool, &next, candidate(0, "Meat", search2))
            .await
            .is_err(),
        "Expansion must not recycle known suppliers"
    );
    assert!(
        research::save(&pool, &next, candidate(4, "Meat", search2))
            .await
            .is_ok()
    );
    let remaining: i64 =
        sqlx::query_scalar("SELECT count(*) FROM vendors WHERE workspace_id=$1 AND id=ANY($2)")
            .bind(w)
            .bind(&ids)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(remaining, 3, "Previous candidates remain intact");
    pool.close().await;
}

#[tokio::test]
#[ignore = "Explicit opt-in live checkpoint recovery, never sends outreach"]
async fn live_resume_shortlist_checkpoint() {
    let Ok(id) = std::env::var("BACKHAUS_LIVE_RESUME_RUN") else {
        return;
    };
    let id: Uuid = id.parse().unwrap();
    let url = std::env::var("TEST_DATABASE_URL").unwrap();
    assert!(url.ends_with("/backhaus_ai_test"));
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .unwrap();
    let workspace: String = sqlx::query_scalar(
        "SELECT workspace_id FROM agent_runs WHERE id=$1 AND kind='vendor_research'",
    )
    .bind(id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(workspace.starts_with("test-"));
    let mut cfg = common::config(&workspace, &url);
    let live = backhaus_ai_backend::config::Config::from_env().unwrap();
    cfg.model_name = live.model_name;
    cfg.model_base_url = live.model_base_url;
    cfg.model_api_key = live.model_api_key;
    cfg.model_request_options = live.model_request_options;
    cfg.model_timeout = std::time::Duration::from_secs(360);
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .try_init();
    let mut worker = backhaus_ai_backend::worker::Worker::spawn(&cfg)
        .await
        .unwrap();
    let (_sender, stop) = tokio::sync::watch::channel(false);
    backhaus_ai_backend::agent::work_one_until(&pool, std::sync::Arc::new(cfg), stop, &mut worker)
        .await
        .unwrap();
    worker.shutdown().await;
    let state = jobs::get(&pool, &workspace, id).await.unwrap();
    println!(
        "Recovered status: {}, result: {}",
        state["status"], state["result"]
    );
    assert_eq!(state["status"], "completed");
    assert_eq!(state["result"]["saved_count"], 3);
    assert!(state["result"]["searches_used"].as_i64().unwrap() <= 5);
    assert_eq!(state["result"]["assessed_count"], 3);
    pool.close().await;
}
