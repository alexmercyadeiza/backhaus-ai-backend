use axum::{
    Json,
    body::Body,
    http::{Request, StatusCode},
};
mod common;
use backhaus_ai_backend::{
    agent::Worked,
    api::{self, AppState},
    config::Config,
    data::{self, DateRange, InventoryQuery},
    import, jobs,
    reports::{self, ReportData, ReportFormat, ReportKind, ReportRequest},
    worker::Worker,
};
use common::*;
use http_body_util::BodyExt;
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tower::ServiceExt;
use uuid::Uuid;

#[test]
fn business_days_and_invalid_dates() {
    assert_eq!(
        import::business_day("2026-08-01T05:59:00Z")
            .unwrap()
            .to_string(),
        "2026-07-31"
    );
    assert_eq!(
        import::business_day("2026-08-01T06:00:00Z")
            .unwrap()
            .to_string(),
        "2026-08-01"
    );
    assert!(import::business_day("garbage").is_err());
    assert!(
        DateRange {
            from: "2026-08-31".parse().unwrap(),
            to: "2026-07-01".parse().unwrap()
        }
        .validate()
        .is_err()
    );
}
#[test]
fn reports_escape_untrusted_source_text() {
    let data = ReportData {
        title: "Inventory".into(),
        note: "Snapshot".into(),
        headers: vec!["Item".into()],
        rows: vec![
            vec!["=HYPERLINK(\"bad\")".into()],
            vec!["A & B <supplier>".into()],
        ],
    };
    let csv = String::from_utf8(reports::render_csv(&data).unwrap()).unwrap();
    assert!(csv.contains("'=HYPERLINK"));
    let word = reports::render_docx(&data).unwrap();
    assert_eq!(&word[..2], b"PK");
}

#[tokio::test]
#[ignore = "Requires dedicated PostgreSQL test database"]
async fn import_decimal_totals_voids_scope_and_reimport() {
    let (pool, cfg) = setup().await;
    let value = data::sales(&pool, &cfg.workspace_id, &range())
        .await
        .unwrap();
    assert_eq!(value["ticket_count"], 3);
    assert_eq!(
        value["gross_line_sales"]
            .as_str()
            .unwrap()
            .parse::<rust_decimal::Decimal>()
            .unwrap(),
        rust_decimal::Decimal::new(355, 1)
    );
    assert_eq!(value["days"][0]["tickets"], 2); // 02:00 belongs to July 31.
    assert_eq!(
        data::sales(&pool, "other-workspace", &range())
            .await
            .unwrap()["ticket_count"],
        0
    );
    let inventory = data::inventory(
        &pool,
        &cfg.workspace_id,
        &InventoryQuery {
            below_par: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(inventory["items"].as_array().unwrap().len(), 25);
    let injected = data::inventory(
        &pool,
        &cfg.workspace_id,
        &InventoryQuery {
            search: Some("' OR 1=1 --".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(injected["items"].as_array().unwrap().is_empty());
    assert_eq!(
        import::snapshot(&pool, &cfg.workspace_id, &fixture(&cfg.workspace_id))
            .await
            .unwrap()["status"],
        "already_imported"
    );
    let mut different: Value = serde_json::from_slice(&fixture(&cfg.workspace_id)).unwrap();
    different["inventory"][0]["name"] = json!("Changed");
    assert!(
        import::snapshot(
            &pool,
            &cfg.workspace_id,
            &serde_json::to_vec(&different).unwrap()
        )
        .await
        .is_err()
    );
    let broken_workspace = format!("test-{}", Uuid::new_v4());
    let mut broken: Value = serde_json::from_slice(&fixture(&broken_workspace)).unwrap();
    broken["tickets"][0]["orders"][0]["ticketId"] = json!(999);
    assert!(
        import::snapshot(
            &pool,
            &broken_workspace,
            &serde_json::to_vec(&broken).unwrap()
        )
        .await
        .is_err()
    );
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM inventory_items WHERE workspace_id=$1")
            .bind(broken_workspace)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(count, 0); // Failed import rolled back the entire snapshot.
}

#[tokio::test]
#[ignore = "Requires dedicated PostgreSQL test database"]
async fn jobs_are_idempotent_paused_fenced_and_recovered() {
    let (pool, cfg) = setup().await;
    let w = &cfg.workspace_id;
    let c = conversation(&pool, w).await;
    let input = json!({"message":"Check inventory"});
    let first = jobs::enqueue(&pool, w, c, "key", input.clone())
        .await
        .unwrap();
    let again = jobs::enqueue(&pool, w, c, "key", input).await.unwrap();
    assert_eq!(first["run_id"], again["run_id"]);
    assert!(
        jobs::enqueue(&pool, w, c, "key", json!({"message":"Something else"}))
            .await
            .is_err()
    );
    assert!(
        jobs::enqueue(&pool, w, c, "different", json!({"message":"Parallel turn"}))
            .await
            .is_err()
    );
    let (a, b) = tokio::join!(jobs::claim(&pool, w), jobs::claim(&pool, w));
    let mut claimed = vec![a.unwrap(), b.unwrap()]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    assert_eq!(claimed.len(), 1);
    let run = claimed.pop().unwrap();
    assert!(jobs::get(&pool, "other-workspace", run.id).await.is_err());
    jobs::control(&pool, w, run.id, "pause").await.unwrap();
    assert!(!jobs::active(&pool, &run).await.unwrap());
    jobs::finish(&pool, &run, Some(json!({"answer":"stale"})), None)
        .await
        .unwrap();
    assert_eq!(
        jobs::get(&pool, w, run.id).await.unwrap()["status"],
        "paused"
    );
    jobs::control(&pool, w, run.id, "resume").await.unwrap();
    let resumed = jobs::claim(&pool, w).await.unwrap().unwrap();
    assert_ne!(resumed.lease, run.lease);
    assert!(!jobs::heartbeat(&pool, &run).await.unwrap());
    sqlx::query("UPDATE agent_runs SET lease_until=now()-interval '1 minute' WHERE id=$1")
        .bind(run.id)
        .execute(&pool)
        .await
        .unwrap();
    let recovered = jobs::claim(&pool, w).await.unwrap().unwrap();
    assert_eq!(recovered.attempt, 3);
    assert_ne!(recovered.lease, resumed.lease);
    jobs::control(&pool, w, run.id, "cancel").await.unwrap();
    assert!(
        jobs::event(&pool, &recovered, "tool_started", json!({}))
            .await
            .is_err()
    );
    assert_eq!(
        jobs::get(&pool, w, run.id).await.unwrap()["status"],
        "cancelled"
    );
}

#[tokio::test]
#[ignore = "Requires dedicated PostgreSQL test database"]
async fn protected_api_reports_and_missing_model_are_honest() {
    let (pool, cfg) = setup().await;
    let app = api::router(AppState {
        pool: pool.clone(),
        config: Arc::new(cfg.clone()),
    });
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/inventory")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/sales/summary?from=2026-08-31&to=2026-07-01")
                .header("authorization", format!("Bearer {}", cfg.api_key))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let c = conversation(&pool, &cfg.workspace_id).await;
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/chat")
                .header("authorization", format!("Bearer {}", cfg.api_key))
                .header("content-type", "application/json")
                .header("idempotency-key", "chat")
                .body(Body::from(
                    json!({"conversation_id":c,"message":"Hello"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    for format in [ReportFormat::Csv, ReportFormat::Docx, ReportFormat::Pdf] {
        let request = ReportRequest {
            kind: ReportKind::Sales,
            format,
            range: Some(range()),
        };
        let artifact = reports::generate(&pool, &cfg, &cfg.workspace_id, None, &request)
            .await
            .unwrap();
        let bytes: Vec<u8> = sqlx::query_scalar("SELECT bytes FROM artifacts WHERE id=$1")
            .bind(
                artifact["artifact_id"]
                    .as_str()
                    .unwrap()
                    .parse::<Uuid>()
                    .unwrap(),
            )
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(!bytes.is_empty());
        if matches!(request.format, ReportFormat::Pdf) {
            assert!(bytes.starts_with(b"%PDF"));
        }
    }
    jobs::enqueue(
        &pool,
        &cfg.workspace_id,
        c,
        "report-run",
        json!({"message":"Export"}),
    )
    .await
    .unwrap();
    let run = jobs::claim(&pool, &cfg.workspace_id)
        .await
        .unwrap()
        .unwrap();
    let request = ReportRequest {
        kind: ReportKind::Sales,
        format: ReportFormat::Csv,
        range: Some(range()),
    };
    let first = reports::generate(&pool, &cfg, &cfg.workspace_id, Some(&run), &request)
        .await
        .unwrap();
    let again = reports::generate(&pool, &cfg, &cfg.workspace_id, Some(&run), &request)
        .await
        .unwrap();
    assert_eq!(first["artifact_id"], again["artifact_id"]);
    jobs::control(&pool, &cfg.workspace_id, run.id, "cancel")
        .await
        .unwrap();
    assert!(
        reports::generate(&pool, &cfg, &cfg.workspace_id, Some(&run), &request)
            .await
            .is_err()
    );
}

#[tokio::test]
#[ignore = "Requires dedicated PostgreSQL test database"]
async fn strands_calls_real_tool_and_streams_result_with_mock_model() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let (pool, mut cfg) = setup().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_for_handler = calls.clone();
    // Ephemeral, local test fixture only. No production or external model calls.
    let mock=axum::Router::new().route("/v1/chat/completions",axum::routing::post(move|Json(body):Json<Value>|{
        let calls=calls_for_handler.clone();
        async move {
            let call=calls.fetch_add(1,Ordering::SeqCst);
            let serialized = body.to_string();
            assert!(!serialized.contains("activeInventoryPopulation"));
            assert!(!serialized.contains("RAW_MANIFEST_SENTINEL"));
            assert!(!serialized.contains("STALE_SOURCE_SENTINEL"));
            assert_eq!(body["reasoning"]["effort"], "none");
            assert_eq!(body["provider"]["sort"], "latency");
            let (delta,finish)=if call==0 {
                assert_eq!(body["stream"],true);
                (json!({"role":"assistant","tool_calls":[{"index":0,"id":"call-sales","type":"function","function":{"name":"get_sales_summary","arguments":"{\"from\":\"2026-07-01\",\"to\":\"2026-08-31\"}"}}]}),"tool_calls")
            }else{
                let messages=body["messages"].as_array().unwrap();
                let tool=messages.iter().find(|m|m["role"]=="tool").expect("Real tool result included in model context");
                assert!(tool["content"].to_string().contains("35.5"));
                (json!({"role":"assistant","content":"Gross line sales are NGN 35.5 across 3 tickets in this test snapshot."}),"stop")
            };
            let chunk=|delta:Value,reason:Value|json!({"id":"completion-test","object":"chat.completion.chunk","created":1,"model":"fixture-model","choices":[{"index":0,"delta":delta,"finish_reason":reason}]});
            ([("content-type","text/event-stream")],format!("data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",chunk(delta,Value::Null),chunk(json!({}),json!(finish))))
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    cfg.model_base_url = Some(format!("http://{}/v1", listener.local_addr().unwrap()));
    cfg.model_name = Some("fixture-model".into());
    cfg.model_request_options =
        json!({"reasoning":{"effort":"none"},"provider":{"sort":"latency"}});
    let server = tokio::spawn(async move { axum::serve(listener, mock).await.unwrap() });
    cfg.model_api_key = "test-key-never-logged".into();
    let mut worker = Worker::spawn(&cfg)
        .await
        .expect("Build the worker first: cd worker && npm ci && npm run build");
    let c = conversation(&pool, &cfg.workspace_id).await;
    jobs::enqueue(
        &pool,
        &cfg.workspace_id,
        c,
        "old-context",
        json!({"message":"Old inventory question"}),
    )
    .await
    .unwrap();
    let old = jobs::claim(&pool, &cfg.workspace_id)
        .await
        .unwrap()
        .unwrap();
    jobs::finish(
        &pool,
        &old,
        Some(json!({"answer":"STALE_SOURCE_SENTINEL 464 items"})),
        None,
    )
    .await
    .unwrap();
    let request = jobs::enqueue(
        &pool,
        &cfg.workspace_id,
        c,
        "mock-chat",
        json!({"message":"What are my sales for July and August 2026?"}),
    )
    .await
    .unwrap();
    assert_eq!(
        backhaus_ai_backend::agent::work_one(&pool, Arc::new(cfg.clone()), &mut worker)
            .await
            .unwrap(),
        Worked::Done
    );
    worker.shutdown().await;
    server.abort();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let run = jobs::get(
        &pool,
        &cfg.workspace_id,
        request["run_id"].as_str().unwrap().parse().unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(run["status"], "completed", "{run}");
    assert!(run["result"]["answer"].as_str().unwrap().contains("35.5"));
    let app = api::router(AppState {
        pool: pool.clone(),
        config: Arc::new(cfg.clone()),
    });
    let response = app
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/v1/agents/runs/{}/events",
                    run["id"].as_str().unwrap()
                ))
                .header("authorization", format!("Bearer {}", cfg.api_key))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = String::from_utf8(
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert!(
        body.contains("tool_started") && body.contains("text_delta") && body.contains("completed")
    );
}

#[tokio::test]
#[ignore = "Requires dedicated PostgreSQL test database"]
async fn failed_jobs_back_off_and_stop_after_three_attempts() {
    let (pool, cfg) = setup().await;
    let c = conversation(&pool, &cfg.workspace_id).await;
    jobs::enqueue(
        &pool,
        &cfg.workspace_id,
        c,
        "retry",
        json!({"message":"Read sales"}),
    )
    .await
    .unwrap();
    for attempt in 1..=3 {
        let run = jobs::claim(&pool, &cfg.workspace_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(run.attempt, attempt);
        jobs::finish(&pool, &run, None, Some("model_timeout"))
            .await
            .unwrap();
        let state = jobs::get(&pool, &cfg.workspace_id, run.id).await.unwrap();
        assert_eq!(
            state["status"],
            if attempt < 3 { "queued" } else { "failed" }
        );
        assert!(
            jobs::claim(&pool, &cfg.workspace_id)
                .await
                .unwrap()
                .is_none()
        );
        if attempt < 3 {
            sqlx::query("UPDATE agent_runs SET available_at=now() WHERE id=$1")
                .bind(run.id)
                .execute(&pool)
                .await
                .unwrap();
        }
    }
}

#[tokio::test]
#[ignore = "Requires dedicated PostgreSQL test database"]
async fn dashboard_restores_scoped_tasks_and_report_links_without_secrets() {
    let (pool, cfg) = setup().await;
    let c = conversation(&pool, &cfg.workspace_id).await;
    jobs::enqueue(
        &pool,
        &cfg.workspace_id,
        c,
        "dashboard",
        json!({"message":"Check inventory"}),
    )
    .await
    .unwrap();
    let run = jobs::claim(&pool, &cfg.workspace_id)
        .await
        .unwrap()
        .unwrap();
    jobs::event(&pool, &run, "tool_started", json!({"tool":"get_inventory"}))
        .await
        .unwrap();
    // This test exercises retrieval/scoping; report rendering is covered separately.
    let artifact_id = Uuid::new_v4();
    sqlx::query("INSERT INTO artifacts(id,workspace_id,run_id,request_hash,filename,media_type,bytes,metadata) VALUES($1,$2,$3,'fixture','inventory.csv','text/csv',$4,$5)")
        .bind(artifact_id).bind(&cfg.workspace_id).bind(run.id).bind(b"Name\nFlour\n".to_vec())
        .bind(json!({"title":"Inventory","format":"csv"})).execute(&pool).await.unwrap();
    let artifact = json!({"artifact_id":artifact_id});
    let app = api::router(AppState {
        pool: pool.clone(),
        config: Arc::new(cfg.clone()),
    });
    for path in [
        "/v1/status".to_string(),
        "/v1/agents".into(),
        format!("/v1/conversations/{c}"),
        "/v1/agents/runs".into(),
    ] {
        let denied = app
            .clone()
            .oneshot(Request::builder().uri(&path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(&path)
                    .header("authorization", format!("Bearer {}", cfg.api_key))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        assert!(!String::from_utf8_lossy(&bytes).contains(&cfg.api_key));
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        if path == "/v1/status" {
            assert_eq!(value["model_configured"], false);
            assert_eq!(value["capabilities"]["vendor_sending"], false);
            assert_eq!(value["capabilities"]["purchase_order_drafts"], true);
        } else if path == "/v1/agents" {
            assert_eq!(value["agents"].as_array().unwrap().len(), 3);
            assert_eq!(value["agents"][0]["id"], "sales");
        } else {
            assert_eq!(value["runs"][0]["input"]["message"], "Check inventory");
            if path.contains("conversations") {
                assert_eq!(
                    value["runs"][0]["artifacts"][0]["artifact_id"],
                    artifact["artifact_id"]
                );
            } else {
                assert_eq!(
                    value["runs"][0]["activity"]["payload"]["tool"],
                    "get_inventory"
                );
            }
        }
    }
    let mut other = cfg.clone();
    other.workspace_id = "unrelated-workspace".into();
    let foreign = api::router(AppState {
        pool,
        config: Arc::new(other),
    });
    for path in [
        format!("/v1/conversations/{c}"),
        format!(
            "/v1/artifacts/{}",
            artifact["artifact_id"].as_str().unwrap()
        ),
    ] {
        let response = foreign
            .clone()
            .oneshot(
                Request::builder()
                    .uri(path)
                    .header("authorization", format!("Bearer {}", cfg.api_key))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}

#[tokio::test]
#[ignore = "Opt-in only: makes paid requests to the configured model using synthetic data"]
async fn configured_model_reads_inventory_through_strands() {
    if std::env::var("BACKHAUS_LIVE_MODEL_TEST").as_deref() != Ok("1") {
        println!("Skipping paid model smoke test; set BACKHAUS_LIVE_MODEL_TEST=1 to opt in.");
        return;
    }
    let model = Config::from_env().expect("Private model configuration is required");
    let (pool, mut cfg) = setup().await;
    cfg.model_base_url = model.model_base_url;
    cfg.model_name = model.model_name;
    cfg.model_api_key = model.model_api_key;
    cfg.model_request_options = model.model_request_options;
    cfg.model_timeout = Duration::from_secs(120);
    let c = conversation(&pool, &cfg.workspace_id).await;
    let queued = jobs::enqueue(&pool, &cfg.workspace_id, c, "live-model-smoke", json!({"message":"Use get_inventory once with limit 1, then tell me the total number of inventory items and how many are below par. One sentence only."})).await.unwrap();
    let id = Uuid::parse_str(queued["run_id"].as_str().unwrap()).unwrap();
    let mut worker = Worker::spawn(&cfg).await.unwrap();
    assert_eq!(
        backhaus_ai_backend::agent::work_one(&pool, Arc::new(cfg.clone()), &mut worker)
            .await
            .unwrap(),
        Worked::Done
    );
    worker.shutdown().await;
    let run = jobs::get(&pool, &cfg.workspace_id, id).await.unwrap();
    assert_eq!(
        run["status"], "completed",
        "Live model failed: {}",
        run["error_code"]
    );
    assert!(!run["result"]["answer"].as_str().unwrap().is_empty());
    let called: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent_events WHERE run_id=$1 AND event_type='tool_completed' AND payload->>'tool'='get_inventory')").bind(id).fetch_one(&pool).await.unwrap();
    assert!(called, "Model must use the real SQL inventory tool");
    println!("Live model completed a real inventory tool call against synthetic test data.");
}

#[tokio::test]
#[ignore = "Requires dedicated PostgreSQL test database"]
async fn model_context_uses_fresh_workspace_counts_and_excludes_source_metadata() {
    let (pool, cfg) = setup().await;
    let context = data::model_context(&pool, &cfg.workspace_id).await.unwrap();
    assert_eq!(context["inventory"]["total_items"], 50);
    assert_eq!(context["inventory"]["below_par"], 25);
    assert!(!context.to_string().contains("464"));
    assert!(!context.to_string().contains("RAW_MANIFEST_SENTINEL"));
    let inventory = data::inventory(
        &pool,
        &cfg.workspace_id,
        &InventoryQuery {
            limit: Some(3),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let compact = data::compact_inventory(&inventory);
    assert_eq!(compact["total_items"], 50);
    assert_eq!(compact["items"].as_array().unwrap().len(), 3);
    assert!(compact.get("coverage").is_none());
    assert!(compact.get("notes").is_none());
    assert_eq!(
        data::model_context(&pool, "unrelated-workspace")
            .await
            .unwrap()["inventory"]["total_items"],
        0
    );
    sqlx::query("DELETE FROM inventory_items WHERE workspace_id=$1 AND id='item-0'")
        .bind(&cfg.workspace_id)
        .execute(&pool)
        .await
        .unwrap();
    let fresh = data::model_context(&pool, &cfg.workspace_id).await.unwrap();
    assert_eq!(fresh["inventory"]["total_items"], 49);
    assert_eq!(fresh["inventory"]["below_par"], 24);
}

#[tokio::test]
#[ignore = "Requires dedicated PostgreSQL test database"]
async fn scoped_agents_pause_catch_up_isolate_and_checkpoint_atomically() {
    use backhaus_ai_backend::scoped_agents as agents;
    let (pool, cfg) = setup().await;
    let w = &cfg.workspace_id;
    agents::control(&pool, w, "sales", "resume").await.unwrap();
    let initial = agents::list(&pool, w).await.unwrap();
    assert_eq!(initial["agents"].as_array().unwrap().len(), 3);
    assert_eq!(initial["monitor_online"], false);
    let instance = Uuid::new_v4();
    agents::heartbeat(&pool, w, instance).await.unwrap();
    assert!(agents::check_one(&pool, w, false).await.unwrap());
    assert!(agents::check_one(&pool, w, false).await.unwrap());
    assert!(!agents::check_one(&pool, w, false).await.unwrap());
    let before = agents::list(&pool, w).await.unwrap();
    assert_eq!(before["agents"][1]["observation"]["below_par"], 25);
    assert_eq!(before["agents"][0]["status"], "watching");
    let checked = before["agents"][1]["checked_revision"].as_i64().unwrap();
    agents::control(&pool, w, "inventory", "pause")
        .await
        .unwrap();
    // Database rollback must not produce a pending change.
    let mut tx = pool.begin().await.unwrap();
    sqlx::query(
        "UPDATE inventory_items SET current_balance=20 WHERE workspace_id=$1 AND id='item-0'",
    )
    .bind(w)
    .execute(&mut *tx)
    .await
    .unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(
        agents::list(&pool, w).await.unwrap()["agents"][1]["data_revision"],
        checked
    );
    // One bulk statement advances one revision; pause retains its old checkpoint.
    sqlx::query("UPDATE inventory_items SET current_balance=20 WHERE workspace_id=$1 AND id IN ('item-0','item-2')").bind(w).execute(&pool).await.unwrap();
    assert!(!agents::check_one(&pool, w, false).await.unwrap());
    let paused = agents::list(&pool, w).await.unwrap();
    assert_eq!(paused["agents"][1]["status"], "paused");
    assert_eq!(paused["agents"][1]["checked_revision"], checked);
    assert_eq!(paused["agents"][1]["data_revision"], checked + 1);
    assert_eq!(
        paused["agents"][0]["checked_revision"],
        before["agents"][0]["checked_revision"]
    );
    agents::disconnect(&pool, instance).await.unwrap();
    // A replacement monitor sees the same saved state, including pause.
    let replacement = Uuid::new_v4();
    agents::heartbeat(&pool, w, replacement).await.unwrap();
    let resumed = agents::control(&pool, w, "inventory", "resume")
        .await
        .unwrap();
    assert_eq!(resumed["agents"][1]["status"], "pending");
    let (a, b) = tokio::join!(
        agents::check_one(&pool, w, false),
        agents::check_one(&pool, w, false)
    );
    assert!(a.as_ref().is_ok_and(|v| *v) || b.as_ref().is_ok_and(|v| *v));
    let after = agents::list(&pool, w).await.unwrap();
    assert_eq!(after["agents"][1]["checked_revision"], checked + 1);
    assert_eq!(after["agents"][1]["observation"]["below_par"], 23);
    let audit:i64=sqlx::query_scalar("SELECT COUNT(*) FROM scoped_agent_checks WHERE workspace_id=$1 AND role='inventory' AND revision=$2").bind(w).bind(checked+1).fetch_one(&pool).await.unwrap();
    assert_eq!(audit, 1);
    assert!(!agents::check_one(&pool, w, false).await.unwrap());
    assert!(
        agents::control(&pool, "foreign-workspace", "inventory", "pause")
            .await
            .is_err()
    );
    assert!(
        agents::list(&pool, "foreign-workspace").await.unwrap()["agents"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(
        agents::control(&pool, w, "payments", "resume")
            .await
            .is_err()
    );
    assert!(agents::control(&pool, w, "sales", "delete").await.is_err());
    // Sales writes only invalidate Sales; exact line values feed its check.
    sqlx::query("UPDATE sales_lines SET unit_price=12 WHERE workspace_id=$1 AND id=11")
        .bind(w)
        .execute(&pool)
        .await
        .unwrap();
    assert!(agents::check_one(&pool, w, false).await.unwrap());
    let changed = agents::list(&pool, w).await.unwrap();
    assert_eq!(changed["agents"][1]["checked_revision"], checked + 1);
    assert_eq!(
        changed["agents"][0]["observation"]["recent_sales"]
            .as_str()
            .unwrap()
            .parse::<rust_decimal::Decimal>()
            .unwrap(),
        rust_decimal::Decimal::new(412, 1)
    );
    agents::disconnect(&pool, replacement).await.unwrap();
    assert_eq!(
        agents::list(&pool, w).await.unwrap()["monitor_online"],
        false
    );
}

#[tokio::test]
#[ignore = "Requires dedicated PostgreSQL test database"]
async fn shutdown_returns_chat_to_queue_without_spending_attempt_or_accepting_stale_result() {
    let (pool, cfg) = setup().await;
    let c = conversation(&pool, &cfg.workspace_id).await;
    jobs::enqueue(
        &pool,
        &cfg.workspace_id,
        c,
        "shutdown",
        json!({"message":"Count stock"}),
    )
    .await
    .unwrap();
    let run = jobs::claim(&pool, &cfg.workspace_id)
        .await
        .unwrap()
        .unwrap();
    jobs::release(&pool, &run).await.unwrap();
    let queued = jobs::get(&pool, &cfg.workspace_id, run.id).await.unwrap();
    assert_eq!(queued["status"], "queued");
    assert_eq!(queued["attempt"], 0);
    let resumed = jobs::claim(&pool, &cfg.workspace_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resumed.id, run.id);
    assert_eq!(resumed.attempt, 1);
    assert_ne!(resumed.lease, run.lease);
    jobs::finish(&pool, &run, Some(json!({"answer":"stale"})), None)
        .await
        .unwrap();
    assert_eq!(
        jobs::get(&pool, &cfg.workspace_id, run.id).await.unwrap()["status"],
        "running"
    );
    jobs::control(&pool, &cfg.workspace_id, run.id, "pause")
        .await
        .unwrap();
    jobs::release(&pool, &resumed).await.unwrap();
    assert_eq!(
        jobs::get(&pool, &cfg.workspace_id, run.id).await.unwrap()["status"],
        "paused"
    );
}

#[test]
fn chat_history_keeps_recent_whole_exchanges_with_a_byte_budget() {
    use backhaus_ai_backend::conversations::bounded_history;
    let pairs = (0..6)
        .map(|i| (format!("question {i}"), format!("answer {i}")))
        .collect();
    let recent = bounded_history(pairs);
    assert_eq!(recent.len(), 4);
    assert_eq!(recent[0].0, "question 3");
    assert_eq!(recent[3].0, "question 0");
    let bounded = bounded_history(vec![
        ("new".into(), "🥐".repeat(2000)),
        ("older".into(), "x".repeat(5000)),
    ]);
    assert_eq!(bounded.len(), 1);
    assert!(
        bounded
            .iter()
            .map(|(a, b)| a.len() + b.len())
            .sum::<usize>()
            <= 12_000
    );
}

#[tokio::test]
#[ignore = "Requires dedicated PostgreSQL test database"]
async fn archived_chats_are_read_only_preserve_partial_answers_and_isolate_context() {
    use backhaus_ai_backend::conversations as chats;
    let (pool, cfg) = setup().await;
    let w = &cfg.workspace_id;
    let c = conversation(&pool, w).await;
    jobs::enqueue(
        &pool,
        w,
        c,
        "archive-first",
        json!({"message":"First question"}),
    )
    .await
    .unwrap();
    let first = jobs::claim(&pool, w).await.unwrap().unwrap();
    jobs::finish(
        &pool,
        &first,
        Some(json!({"answer":"Private answer","context_version":2})),
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        chats::model_history(&pool, w, c, Uuid::nil())
            .await
            .unwrap()
            .len(),
        1
    );
    jobs::enqueue(
        &pool,
        w,
        c,
        "archive-running",
        json!({"message":"Second question"}),
    )
    .await
    .unwrap();
    let running = jobs::claim(&pool, w).await.unwrap().unwrap();
    jobs::event(
        &pool,
        &running,
        "text_delta",
        json!({"text":"Partial answer"}),
    )
    .await
    .unwrap();
    let archived = chats::archive(&pool, w, c).await.unwrap();
    assert!(!archived["archived_at"].is_null());
    assert_eq!(chats::archive(&pool, w, c).await.unwrap(), archived);
    assert!(!jobs::active(&pool, &running).await.unwrap());
    jobs::finish(&pool, &running, Some(json!({"answer":"Late result"})), None)
        .await
        .unwrap();
    assert!(
        jobs::event(&pool, &running, "text_delta", json!({"text":"Late token"}))
            .await
            .is_err()
    );
    jobs::release(&pool, &running).await.unwrap();
    let history = chats::history(&pool, w, c, &chats::Page::default())
        .await
        .unwrap();
    assert_eq!(history["runs"][0]["result"]["answer"], "Private answer");
    assert_eq!(history["runs"][1]["result"]["answer"], "Partial answer");
    assert_eq!(history["runs"][1]["status"], "cancelled");
    assert!(
        jobs::enqueue(&pool, w, c, "after-archive", json!({"message":"Continue"}))
            .await
            .is_err()
    );
    assert!(jobs::control(&pool, w, running.id, "resume").await.is_err());
    assert!(
        chats::model_history(&pool, w, c, Uuid::nil())
            .await
            .unwrap()
            .is_empty()
    );
    let fresh = conversation(&pool, w).await;
    assert!(
        chats::model_history(&pool, w, fresh, Uuid::nil())
            .await
            .unwrap()
            .is_empty()
    );
    let archives = chats::archives(&pool, w, &chats::Page::default())
        .await
        .unwrap();
    assert_eq!(archives["conversations"].as_array().unwrap().len(), 1);
    assert_eq!(archives["conversations"][0]["title"], "First question");
    assert!(
        chats::archives(&pool, "foreign-workspace", &chats::Page::default())
            .await
            .unwrap()["conversations"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(chats::archive(&pool, "foreign-workspace", c).await.is_err());
    assert!(
        chats::history(&pool, "foreign-workspace", c, &chats::Page::default())
            .await
            .is_err()
    );
    assert!(
        chats::archives(&pool, "foreign-workspace", &chats::Page { before: Some(c) })
            .await
            .is_err()
    );
    // Archive and enqueue serialize on the conversation; no live work survives.
    let race = conversation(&pool, w).await;
    let (_, closed) = tokio::join!(
        jobs::enqueue(&pool, w, race, "archive-race", json!({"message":"Race"})),
        chats::archive(&pool, w, race)
    );
    closed.unwrap();
    let active:i64=sqlx::query_scalar("SELECT COUNT(*) FROM agent_runs WHERE conversation_id=$1 AND status IN ('queued','running','paused')").bind(race).fetch_one(&pool).await.unwrap();
    assert_eq!(active, 0);
    let mut api_config = cfg.clone();
    api_config.model_base_url = Some("http://127.0.0.1:9/v1".into());
    api_config.model_name = Some("fixture".into());
    let app = api::router(AppState {
        pool: pool.clone(),
        config: Arc::new(api_config),
    });
    let denied = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/conversations/{c}/archive"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
    for (method, path, body, expected) in [
        (
            "GET",
            "/v1/conversations/archives".to_string(),
            None,
            StatusCode::OK,
        ),
        (
            "GET",
            format!("/v1/conversations/{c}"),
            None,
            StatusCode::OK,
        ),
        (
            "POST",
            format!("/v1/conversations/{c}/archive"),
            None,
            StatusCode::OK,
        ),
        (
            "POST",
            "/v1/chat".into(),
            Some(json!({"conversation_id":c,"message":"Continue archived chat"})),
            StatusCode::CONFLICT,
        ),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(path)
                    .header("authorization", format!("Bearer {}", cfg.api_key))
                    .header("content-type", "application/json")
                    .header("idempotency-key", "archived-api")
                    .body(
                        body.map(|v| Body::from(v.to_string()))
                            .unwrap_or_else(Body::empty),
                    )
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), expected);
    }
}

#[tokio::test]
#[ignore = "Requires dedicated PostgreSQL test database"]
async fn archives_and_long_conversations_page_without_losing_messages() {
    use backhaus_ai_backend::conversations as chats;
    let (pool, cfg) = setup().await;
    let w = &cfg.workspace_id;
    for n in 0..21 {
        let c = conversation(&pool, w).await;
        if n == 0 {
            sqlx::query("INSERT INTO agent_runs(id,workspace_id,conversation_id,request_key,input,status,result,created_at) SELECT gen_random_uuid(),$1,$2,'page-'||n,jsonb_build_object('message','Question '||n),'completed',jsonb_build_object('answer','Answer '||n),now()+n*interval '1 second' FROM generate_series(1,103) n")
                .bind(w).bind(c).execute(&pool).await.unwrap();
            let newest = chats::history(&pool, w, c, &chats::Page::default())
                .await
                .unwrap();
            assert_eq!(newest["runs"].as_array().unwrap().len(), 100);
            assert_eq!(newest["runs"][0]["input"]["message"], "Question 4");
            let older = chats::history(
                &pool,
                w,
                c,
                &chats::Page {
                    before: Some(newest["next_before"].as_str().unwrap().parse().unwrap()),
                },
            )
            .await
            .unwrap();
            assert_eq!(older["runs"].as_array().unwrap().len(), 3);
            assert_eq!(older["runs"][0]["input"]["message"], "Question 1");
            assert!(older["next_before"].is_null());
        }
        chats::archive(&pool, w, c).await.unwrap();
    }
    let first = chats::archives(&pool, w, &chats::Page::default())
        .await
        .unwrap();
    let next = chats::archives(
        &pool,
        w,
        &chats::Page {
            before: Some(first["next_before"].as_str().unwrap().parse().unwrap()),
        },
    )
    .await
    .unwrap();
    assert_eq!(first["conversations"].as_array().unwrap().len(), 20);
    assert_eq!(next["conversations"].as_array().unwrap().len(), 1);
    assert!(next["next_before"].is_null());
    let last = &next["conversations"][0]["id"];
    assert!(
        first["conversations"]
            .as_array()
            .unwrap()
            .iter()
            .all(|c| c["id"] != *last)
    );
}

#[tokio::test]
#[ignore = "Requires dedicated PostgreSQL test database"]
async fn table_pages_are_scoped_bounded_and_complete() {
    use backhaus_ai_backend::tables::{self, TableQuery};
    let (pool, cfg) = setup().await;
    let mut ids = std::collections::HashSet::new();
    for (page, expected) in [(1, 20), (2, 20), (3, 10)] {
        let result = tables::page(
            &pool,
            &cfg.workspace_id,
            "inventory",
            &TableQuery {
                page: Some(page),
                status: None,
                search: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(result["total"], 50);
        assert_eq!(result["pages"], 3);
        assert_eq!(result["page_size"], 20);
        assert_eq!(result["items"].as_array().unwrap().len(), expected);
        for row in result["items"].as_array().unwrap() {
            assert!(ids.insert(row["id"].as_str().unwrap().to_owned()));
            assert_eq!(row["par_level"], "10");
            assert!(matches!(
                row["status"].as_str(),
                Some("Below par" | "In stock")
            ));
            assert!(row.get("source").is_none());
        }
    }
    let last = tables::page(
        &pool,
        &cfg.workspace_id,
        "inventory",
        &TableQuery {
            page: Some(10),
            status: None,
            search: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(last["page"], 3);
    let below = tables::page(
        &pool,
        &cfg.workspace_id,
        "inventory",
        &TableQuery {
            page: None,
            status: Some("below_par".into()),
            search: None,
        },
    )
    .await
    .unwrap();
    assert!(below["total"].as_i64().unwrap() > 0);
    assert!(
        below["items"]
            .as_array()
            .unwrap()
            .iter()
            .all(|row| row["status"] == "Below par")
    );
    let searched = tables::page(
        &pool,
        &cfg.workspace_id,
        "inventory",
        &TableQuery {
            page: None,
            status: None,
            search: Some("%".into()),
        },
    )
    .await
    .unwrap();
    assert_eq!(searched["total"], 0);
    assert!(
        tables::page(
            &pool,
            &cfg.workspace_id,
            "sales",
            &TableQuery {
                page: None,
                status: None,
                search: Some("rice".into()),
            },
        )
        .await
        .is_err()
    );
    let sales = tables::page(&pool, &cfg.workspace_id, "sales", &TableQuery::default())
        .await
        .unwrap();
    assert_eq!(sales["items"][0]["id"], "3");
    assert_eq!(sales["items"][0]["gross_line_sales"], "0");
    assert_eq!(
        sales["items"][2]["gross_line_sales"]
            .as_str()
            .unwrap()
            .parse::<rust_decimal::Decimal>()
            .unwrap(),
        rust_decimal::Decimal::new(305, 1)
    );
    for kind in ["sales", "inventory", "menu", "purchase-orders"] {
        let result = tables::page(&pool, "unrelated-workspace", kind, &TableQuery::default())
            .await
            .unwrap();
        assert_eq!(result["total"], 0);
        assert!(result["items"].as_array().unwrap().is_empty());
    }
    let app = api::router(AppState {
        pool: pool.clone(),
        config: Arc::new(cfg.clone()),
    });
    for (path, authenticated, expected) in [
        ("/v1/tables/inventory?page=2", true, StatusCode::OK),
        ("/v1/tables/inventory", false, StatusCode::UNAUTHORIZED),
        ("/v1/tables/inventory?page=0", true, StatusCode::BAD_REQUEST),
        (
            "/v1/tables/inventory?page=50001",
            true,
            StatusCode::BAD_REQUEST,
        ),
        (
            "/v1/tables/inventory?limit=1000",
            true,
            StatusCode::BAD_REQUEST,
        ),
        ("/v1/tables/unknown", true, StatusCode::NOT_FOUND),
    ] {
        let mut builder = Request::builder().uri(path);
        if authenticated {
            builder = builder.header("authorization", format!("Bearer {}", cfg.api_key));
        }
        let response = app
            .clone()
            .oneshot(builder.body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), expected, "{path}");
    }
}

#[tokio::test]
#[ignore = "Requires dedicated PostgreSQL test database"]
async fn menu_backfill_preserves_inventory_and_pages_prices() {
    use backhaus_ai_backend::tables::{self, TableQuery};
    let (pool, cfg) = setup().await;
    let bytes = fixture(&cfg.workspace_id);
    sqlx::query("DELETE FROM menu_items WHERE workspace_id=$1")
        .bind(&cfg.workspace_id)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE inventory_items SET par_level=7 WHERE workspace_id=$1")
        .bind(&cfg.workspace_id)
        .execute(&pool)
        .await
        .unwrap();
    assert_eq!(
        import::menu_snapshot(&pool, &cfg.workspace_id, &bytes)
            .await
            .unwrap()["menu_items_inserted"],
        1
    );
    assert_eq!(
        import::menu_snapshot(&pool, &cfg.workspace_id, &bytes)
            .await
            .unwrap()["menu_items_inserted"],
        0
    );
    assert!(
        import::menu_snapshot(&pool, "wrong-workspace", &bytes)
            .await
            .is_err()
    );
    let mut changed = bytes.clone();
    changed.push(b' ');
    assert!(
        import::menu_snapshot(&pool, &cfg.workspace_id, &changed)
            .await
            .is_err()
    );
    let par: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM inventory_items WHERE workspace_id=$1 AND par_level=7",
    )
    .bind(&cfg.workspace_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(par, 50);
    let workspace = format!("test-{}", Uuid::new_v4());
    let mut snapshot: Value = serde_json::from_slice(&fixture(&workspace)).unwrap();
    snapshot["menu"]["menuItems"] = json!((1..=41).map(|id| json!({"id":id,"name":format!("Meal {id:02}"),"groupCode":"Food","portions":[{"name":"Normal","prices":[{"price":10.10},{"price":20.20}]}]})).collect::<Vec<_>>());
    import::snapshot(&pool, &workspace, &serde_json::to_vec(&snapshot).unwrap())
        .await
        .unwrap();
    let result = tables::page(
        &pool,
        &workspace,
        "menu",
        &TableQuery {
            page: Some(3),
            status: None,
            search: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(result["total"], 41);
    assert_eq!(result["items"].as_array().unwrap().len(), 1);
    assert_eq!(result["items"][0]["name"], "Meal 41");
    assert_eq!(
        result["items"][0]["price_min"]
            .as_str()
            .unwrap()
            .parse::<rust_decimal::Decimal>()
            .unwrap(),
        rust_decimal::Decimal::new(101, 1)
    );
    assert_eq!(
        result["items"][0]["price_max"]
            .as_str()
            .unwrap()
            .parse::<rust_decimal::Decimal>()
            .unwrap(),
        rust_decimal::Decimal::new(202, 1)
    );
}

#[tokio::test]
#[ignore = "Requires dedicated PostgreSQL test database"]
async fn record_details_are_scoped_and_page_related_items() {
    use backhaus_ai_backend::tables::{self, TableQuery};
    let (pool, cfg) = setup().await;
    let workspace = &cfg.workspace_id;
    let sale = tables::detail(&pool, workspace, "sales", "1", &TableQuery::default())
        .await
        .unwrap();
    assert_eq!(sale["total"], 3);
    assert_eq!(sale["record"]["ticket_number"], "1");
    assert_eq!(sale["items"][2]["billable"], false);
    assert!(sale["record"].get("source").is_none());
    assert_eq!(
        sale["record"]["gross_line_sales"]
            .as_str()
            .unwrap()
            .parse::<rust_decimal::Decimal>()
            .unwrap(),
        rust_decimal::Decimal::new(305, 1)
    );
    for i in 0..25 {
        sqlx::query("INSERT INTO inventory_movements(workspace_id,id,item_id,business_date,movement_type,quantity_delta,source) VALUES($1,$2,'item-0','2026-08-01','delivery',1,'{}')")
            .bind(workspace).bind(format!("movement-{i:02}")).execute(&pool).await.unwrap();
    }
    let first = tables::detail(
        &pool,
        workspace,
        "inventory",
        "item-0",
        &TableQuery::default(),
    )
    .await
    .unwrap();
    let last = tables::detail(
        &pool,
        workspace,
        "inventory",
        "item-0",
        &TableQuery {
            page: Some(2),
            status: None,
            search: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(first["items"].as_array().unwrap().len(), 20);
    assert_eq!(last["items"].as_array().unwrap().len(), 5);
    assert_ne!(first["items"][0]["id"], last["items"][0]["id"]);
    assert_eq!(first["record"]["balance"], "2");
    assert_eq!(first["record"]["status"], "Below par");
    assert_eq!(first["record"]["supplier"], Value::Null);
    sqlx::query("UPDATE menu_items SET portions=$2 WHERE workspace_id=$1 AND id=1")
        .bind(workspace).bind(json!([{"name":"Normal","multiplier":1,"secret":"must-not-leak","prices":[{"priceTag":null,"price":10.1},{"priceTag":"Takeaway","price":12.2}]}])).execute(&pool).await.unwrap();
    let menu = tables::detail(&pool, workspace, "menu", "1", &TableQuery::default())
        .await
        .unwrap();
    assert_eq!(menu["items"][0]["name"], "Normal");
    assert_eq!(menu["items"][0]["prices"][0]["amount"], "10.1");
    assert_eq!(menu["items"][0]["prices"][1]["label"], "Takeaway");
    assert!(!menu.to_string().contains("must-not-leak"));
    for (kind, id) in [("sales", "1"), ("inventory", "item-0"), ("menu", "1")] {
        assert!(
            tables::detail(&pool, "another-workspace", kind, id, &TableQuery::default())
                .await
                .is_err()
        );
        assert!(
            tables::detail(
                &pool,
                workspace,
                kind,
                id,
                &TableQuery {
                    page: Some(0),
                    status: None,
                    search: None,
                }
            )
            .await
            .is_err()
        );
    }
    let app = api::router(AppState {
        pool: pool.clone(),
        config: Arc::new(cfg.clone()),
    });
    for (path, auth, status) in [
        ("/v1/tables/menu/1", true, StatusCode::OK),
        ("/v1/tables/menu/1", false, StatusCode::UNAUTHORIZED),
        ("/v1/tables/menu/invalid", true, StatusCode::NOT_FOUND),
        ("/v1/tables/menu/99999", true, StatusCode::NOT_FOUND),
        (
            "/v1/tables/inventory/item-0?page=0",
            true,
            StatusCode::BAD_REQUEST,
        ),
        ("/v1/tables/purchase-orders/1", true, StatusCode::NOT_FOUND),
    ] {
        let mut builder = Request::builder().uri(path);
        if auth {
            builder = builder.header("authorization", format!("Bearer {}", cfg.api_key));
        }
        let response = app
            .clone()
            .oneshot(builder.body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), status, "{path}");
    }
}

#[tokio::test]
#[ignore = "Requires dedicated PostgreSQL test database"]
async fn empty_reports_return_no_data_without_creating_files() {
    let (pool, mut cfg) = setup().await;
    cfg.typst_bin = "/renderer-must-not-run-for-empty-reports".into();
    let empty_range = DateRange {
        from: "2026-09-06".parse().unwrap(),
        to: "2026-09-12".parse().unwrap(),
    };
    for format in [ReportFormat::Csv, ReportFormat::Docx, ReportFormat::Pdf] {
        let args = ReportRequest {
            kind: ReportKind::Sales,
            format,
            range: Some(empty_range.clone()),
        };
        let result = reports::generate(&pool, &cfg, &cfg.workspace_id, None, &args)
            .await
            .unwrap();
        assert_eq!(result["status"], "no_data");
        assert_eq!(result["requested_range"]["from"], "2026-09-06");
        assert_eq!(result["available_range"]["from"], "2026-07-31");
        assert_eq!(result["available_range"]["to"], "2026-08-01");
        assert!(result.get("artifact_id").is_none());
        assert!(result.get("download_url").is_none());
    }
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM artifacts WHERE workspace_id=$1")
        .bind(&cfg.workspace_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
    let summary = data::compact_sales(
        &data::sales(&pool, &cfg.workspace_id, &empty_range)
            .await
            .unwrap(),
    );
    assert_eq!(summary["status"], "no_data");
    assert!(summary["gross_line_sales"].is_null());
    assert!(summary["ticket_total"].is_null());
    for kind in [ReportKind::Sales, ReportKind::Inventory] {
        let args = ReportRequest {
            kind,
            format: ReportFormat::Pdf,
            range: Some(empty_range.clone()),
        };
        let result = reports::generate(&pool, &cfg, "empty-workspace", None, &args)
            .await
            .unwrap();
        assert_eq!(result["status"], "no_data");
        assert!(result["available_range"].is_null());
    }
    // Real records with a zero total are different from an empty query.
    let zero_range = DateRange {
        from: "2026-08-01".parse().unwrap(),
        to: "2026-08-01".parse().unwrap(),
    };
    let zero_summary = data::compact_sales(
        &data::sales(&pool, &cfg.workspace_id, &zero_range)
            .await
            .unwrap(),
    );
    assert_eq!(zero_summary["ticket_count"], 1);
    assert_eq!(zero_summary["gross_line_sales"], "0");
    let report = reports::collect(
        &pool,
        &cfg.workspace_id,
        &ReportRequest {
            kind: ReportKind::Sales,
            format: ReportFormat::Csv,
            range: Some(zero_range),
        },
    )
    .await
    .unwrap();
    assert_eq!(report.rows.len(), 1);
    assert!(!reports::render_csv(&report).unwrap().is_empty());
    let app = api::router(AppState {
        pool: pool.clone(),
        config: Arc::new(cfg.clone()),
    });
    let response = app.oneshot(Request::builder().method("POST").uri("/v1/reports")
        .header("authorization",format!("Bearer {}",cfg.api_key)).header("content-type","application/json")
        .body(Body::from(json!({"kind":"sales","format":"pdf","range":{"from":"2026-09-06","to":"2026-09-12"}}).to_string())).unwrap()).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(body["status"], "no_data");
}
