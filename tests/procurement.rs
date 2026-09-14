mod common;
use backhaus_ai_backend::{jobs, outreach, research, settings};
use serde_json::json;
use uuid::Uuid;
#[test]
fn contacts_require_source_evidence_and_whatsapp_never_guesses_country() {
    let source = json!({"content":"Acme Foods supplies fresh produce. Email sales@acme.example or call +234 800 111 2233."});
    let mut c = research::Candidate {
        category: None,
        search_id: Uuid::new_v4(),
        source_index: 0,
        name: "Acme Foods".into(),
        email: Some("sales@acme.example".into()),
        phone: Some("+234 800 111 2233".into()),
        evidence_quote: "Acme Foods supplies fresh produce.".into(),
    };
    assert!(research::validate_candidate(&c, &source).is_ok());
    c.email = Some("guessed@acme.example".into());
    assert!(research::validate_candidate(&c, &source).is_err());
    assert!(outreach::whatsapp_url("08001112233", "test").is_err());
    let url =
        outreach::whatsapp_url("+234 800 111 2233", "Hello & thanks\nQuotation only").unwrap();
    assert!(url.starts_with("https://wa.me/2348001112233?text="));
    assert!(url.contains("%26"));
}
#[tokio::test]
#[ignore = "Requires dedicated PostgreSQL test database"]
async fn location_scope_enquiry_dedup_review_and_discard() {
    let (pool, mut cfg) = common::setup().await;
    let w = &cfg.workspace_id;
    let v = common::vendor(&pool, w, "Supplier", true).await;
    assert!(
        outreach::create(
            &pool,
            w,
            &cfg,
            v,
            outreach::Create {
                item_ids: vec!["item-0".into()]
            }
        )
        .await
        .is_err()
    );
    let input = || settings::SettingsInput {
        business_name: "Test restaurant".into(),
        city: "Lagos".into(),
        country: "Nigeria".into(),
        reply_to: String::new(),
        auto_reply: true,
        version: 0,
    };
    let saved = settings::save(&pool, w, &cfg, input()).await.unwrap();
    assert_eq!(saved["location_ready"], true);
    assert!(settings::save(&pool, w, &cfg, input()).await.is_err());
    let create = || outreach::Create {
        item_ids: vec!["item-0".into()],
    };
    let d = outreach::create(&pool, w, &cfg, v, create()).await.unwrap();
    let t: Uuid = d["thread"]["id"].as_str().unwrap().parse().unwrap();
    assert!(
        d["thread"]["initial_body"]
            .as_str()
            .unwrap()
            .contains("8 kg")
    );
    let duplicate = outreach::create(&pool, w, &cfg, v, create()).await.unwrap();
    assert_eq!(duplicate["thread"]["id"], d["thread"]["id"]);
    assert!(
        outreach::detail(&pool, "another-workspace", t)
            .await
            .is_err()
    );
    cfg.resend_key = Some("test-key-not-used".into());
    cfg.resend_from = Some("test@example.invalid".into());
    assert!(
        outreach::send_email(&pool, &cfg.workspace_id, &cfg, t, None)
            .await
            .unwrap_err()
            .to_string()
            .contains("Demo contacts")
    );
    let key = Uuid::new_v4();
    let reply = || outreach::PasteReply {
        body: "Our rice is NGN 4000 per bag. Please confirm delivery location.".into(),
        request_id: key,
    };
    let r = outreach::paste_reply(&pool, &cfg.workspace_id, t, reply())
        .await
        .unwrap();
    assert_eq!(
        outreach::paste_reply(&pool, &cfg.workspace_id, t, reply())
            .await
            .unwrap()["reused"],
        true
    );
    let run = jobs::Run {
        id: r["run_id"].as_str().unwrap().parse().unwrap(),
        workspace: cfg.workspace_id.clone(),
        kind: "supplier_reply".into(),
        conversation: None,
        input: json!({"message_id":r["message_id"],"thread_id":t}),
        lease: Uuid::new_v4(),
        attempt: 1,
    };
    assert!(
        outreach::reply_context(&pool, &run).await.unwrap()["reply"]
            .as_str()
            .unwrap()
            .contains("4000")
    );
    let review = || outreach::Review {
        summary:
            "Supplier quoted NGN 4000 per bag, but pack size and delivery details are missing."
                .into(),
        missing_fields: vec!["pack_size".into(), "delivery".into()],
        needs_person: false,
    };
    assert_eq!(
        outreach::review(&pool, &run, review()).await.unwrap()["reply_prepared"],
        true
    );
    assert_eq!(
        outreach::review(&pool, &run, review()).await.unwrap()["reused"],
        true
    );
    let d = outreach::detail(&pool, &cfg.workspace_id, t).await.unwrap();
    assert_eq!(d["messages"].as_array().unwrap().len(), 3);
    assert!(
        d["messages"][2]["body"]
            .as_str()
            .unwrap()
            .contains("not an order or acceptance")
    );
    let draft_id = Uuid::new_v4();
    let draft = || outreach::ReplyDraft {
        body: "Could you quote for eight kg with delivery included?".into(),
        request_id: draft_id,
    };
    assert_eq!(
        outreach::draft_reply(&pool, &cfg.workspace_id, t, draft())
            .await
            .unwrap()["saved"],
        true
    );
    assert_eq!(
        outreach::draft_reply(&pool, &cfg.workspace_id, t, draft())
            .await
            .unwrap()["reused"],
        true
    );
    let updated = outreach::detail(&pool, &cfg.workspace_id, t).await.unwrap();
    assert!(
        updated["messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["status"] == "superseded")
    );
    assert_eq!(
        outreach::enquiries(&pool, &cfg.workspace_id, v, 1)
            .await
            .unwrap()["total"],
        1
    );
    assert!(
        outreach::enquiries(&pool, "another-workspace", v, 1)
            .await
            .is_err()
    );
    outreach::discard(&pool, &cfg.workspace_id, t)
        .await
        .unwrap();
    assert!(
        outreach::paste_reply(&pool, &cfg.workspace_id, t, reply())
            .await
            .is_err()
    );
    assert!(
        outreach::whatsapp(&pool, &cfg.workspace_id, t)
            .await
            .is_err()
    );
    assert_eq!(
        outreach::detail(&pool, &cfg.workspace_id, t).await.unwrap()["thread"]["status"],
        "discarded"
    );
    let task = outreach::tasks(&pool, &cfg.workspace_id, &cfg)
        .await
        .unwrap();
    assert_eq!(task["threads"].as_array().unwrap().len(), 1);
}
#[tokio::test]
#[ignore = "Requires dedicated PostgreSQL test database"]
async fn evidence_and_vetting_are_workspace_and_run_scoped() {
    let (pool, cfg) = common::setup().await;
    let w = &cfg.workspace_id;
    let mut tx = pool.begin().await.unwrap();
    let id = jobs::enqueue_system(&mut tx, w, "vendor_research", "research-test", json!({}))
        .await
        .unwrap()
        .unwrap();
    tx.commit().await.unwrap();
    let run = jobs::Run {
        id,
        workspace: w.clone(),
        kind: "vendor_research".into(),
        conversation: None,
        input: json!({}),
        lease: Uuid::new_v4(),
        attempt: 1,
    };
    let search = Uuid::new_v4();
    sqlx::query("INSERT INTO vendor_searches(id,workspace_id,run_id,query,sources) VALUES($1,$2,$3,'test',$4)").bind(search).bind(w).bind(id).bind(json!([{"url":"https://supplier.example/contact","title":"Acme Foods","content":"Acme Foods supplies produce. Email sales@acme.example."}])).execute(&pool).await.unwrap();
    let candidate = || research::Candidate {
        category: None,
        search_id: search,
        source_index: 0,
        name: "Acme Foods".into(),
        email: Some("sales@acme.example".into()),
        phone: None,
        evidence_quote: "Acme Foods supplies produce.".into(),
    };
    let v = research::save(&pool, &run, candidate()).await.unwrap();
    assert_eq!(
        research::save(&pool, &run, candidate()).await.unwrap()["id"],
        v["id"]
    );
    let mut wrong = run.clone();
    wrong.id = Uuid::new_v4();
    assert!(research::save(&pool, &wrong, candidate()).await.is_err());
    research::vet(
        &pool,
        &run,
        research::Vetting {
            review_source_indices: vec![],
            vendor_id: v["id"].as_str().unwrap().parse().unwrap(),
            summary: "Only supplier claims found; independent reviews not found in this search."
                .into(),
            reviews_found: false,
            search_id: search,
            source_indices: vec![0],
        },
    )
    .await
    .unwrap();
    assert_eq!(research::latest(&pool, w).await.unwrap()["id"], json!(id));
}
/// Opt-in live search smoke test. Uses an isolated test workspace, never sends messages.
#[tokio::test]
#[ignore = "Requires PostgreSQL and BACKHAUS_LIVE_RESEARCH=1; incurs bounded OpenRouter search calls"]
async fn live_supplier_research_saves_evidence() {
    if std::env::var("BACKHAUS_LIVE_RESEARCH").as_deref() != Ok("1") {
        return;
    }
    let (pool, cfg) = common::setup().await;
    let live = backhaus_ai_backend::config::Config::from_env().unwrap();
    let mut cfg = cfg;
    cfg.model_name = live.model_name;
    cfg.model_base_url = live.model_base_url;
    cfg.model_api_key = live.model_api_key;
    cfg.model_request_options = live.model_request_options;
    cfg.model_timeout = std::time::Duration::from_secs(360);
    sqlx::query("UPDATE inventory_items SET name='Palm Oil' WHERE workspace_id=$1 AND id='item-0'")
        .bind(&cfg.workspace_id)
        .execute(&pool)
        .await
        .unwrap();
    settings::save(
        &pool,
        &cfg.workspace_id,
        &cfg,
        settings::SettingsInput {
            business_name: "Test restaurant".into(),
            city: "Lagos".into(),
            country: "Nigeria".into(),
            reply_to: String::new(),
            auto_reply: false,
            version: 0,
        },
    )
    .await
    .unwrap();
    let r = research::start(
        &pool,
        &cfg.workspace_id,
        &cfg,
        research::ResearchRequest {
            item_ids: vec!["item-0".into()],
        },
    )
    .await
    .unwrap();
    let mut worker = backhaus_ai_backend::worker::Worker::spawn(&cfg)
        .await
        .unwrap();
    let (_stop_sender, stop) = tokio::sync::watch::channel(false);
    backhaus_ai_backend::agent::work_one_until(
        &pool,
        std::sync::Arc::new(cfg.clone()),
        stop,
        &mut worker,
    )
    .await
    .unwrap();
    worker.shutdown().await;
    let latest = research::latest(&pool, &cfg.workspace_id).await.unwrap();
    println!(
        "Live search status: {}. Summary: {}",
        latest["status"], latest["result"]
    );
    assert_eq!(latest["id"], r["run_id"]);
    let count:i64=sqlx::query_scalar("SELECT count(*) FROM vendors WHERE workspace_id=$1 AND source='web_research' AND jsonb_array_length(evidence)>0").bind(&cfg.workspace_id).fetch_one(&pool).await.unwrap();
    assert!(count > 0, "No suppliers with evidence saved: {latest}");
    assert_eq!(latest["status"], "completed");
    let vetted: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM vendors WHERE workspace_id=$1 AND vetting<>'{}'::jsonb",
    )
    .bind(&cfg.workspace_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(vetted > 0, "No vetting assessment saved");
    println!(
        "Live research saved {count} suppliers with evidence, {vetted} vetted; no messages sent."
    );
    assert_eq!(latest["input"]["items"][0]["quantity_needed"], "8");
    let supplier: Uuid = sqlx::query_scalar(
        "SELECT id FROM vendors WHERE workspace_id=$1 AND source='web_research' LIMIT 1",
    )
    .bind(&cfg.workspace_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    let enquiry = outreach::create(
        &pool,
        &cfg.workspace_id,
        &cfg,
        supplier,
        outreach::Create {
            item_ids: vec!["item-0".into()],
        },
    )
    .await
    .unwrap();
    let thread: Uuid = enquiry["thread"]["id"].as_str().unwrap().parse().unwrap();
    outreach::paste_reply(&pool,&cfg.workspace_id,thread,outreach::PasteReply{body:"We can supply palm oil in 5 litre containers. Stock is available this week. Please let us know your delivery location.".into(),request_id:Uuid::new_v4()}).await.unwrap();
    let mut worker = backhaus_ai_backend::worker::Worker::spawn(&cfg)
        .await
        .unwrap();
    let (_sender, stop) = tokio::sync::watch::channel(false);
    backhaus_ai_backend::agent::work_one_until(
        &pool,
        std::sync::Arc::new(cfg.clone()),
        stop,
        &mut worker,
    )
    .await
    .unwrap();
    worker.shutdown().await;
    let conversation = outreach::detail(&pool, &cfg.workspace_id, thread)
        .await
        .unwrap();
    assert!(
        conversation["messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["direction"] == "inbound" && m["review"].as_str().is_some())
    );
    assert!(
        !conversation["messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["status"] == "sent")
    );
    println!("Live Strands reply review persisted; no outreach sent.");
}

#[tokio::test]
#[ignore = "Requires dedicated PostgreSQL test database"]
async fn completed_offer_stops_followups_and_remains_available_for_review() {
    let (pool, cfg) = common::setup().await;
    let w = &cfg.workspace_id;
    let v = common::vendor(&pool, w, "Quote supplier", true).await;
    settings::save(
        &pool,
        w,
        &cfg,
        settings::SettingsInput {
            business_name: "Test restaurant".into(),
            city: "Lagos".into(),
            country: "Nigeria".into(),
            reply_to: String::new(),
            auto_reply: true,
            version: 0,
        },
    )
    .await
    .unwrap();
    let create = || outreach::Create {
        item_ids: vec!["item-0".into()],
    };
    let d = outreach::create(&pool, w, &cfg, v, create()).await.unwrap();
    let t: Uuid = d["thread"]["id"].as_str().unwrap().parse().unwrap();
    for complete in [false, true] {
        let r = outreach::paste_reply(&pool, w, t, outreach::PasteReply {
            body: if complete { "8 kg available at NGN 4000/kg, minimum 1 kg, free delivery tomorrow, pay on delivery." } else { "NGN 4000/kg." }.into(),
            request_id: Uuid::new_v4(),
        }).await.unwrap();
        let run = jobs::Run {
            id: r["run_id"].as_str().unwrap().parse().unwrap(),
            workspace: w.clone(),
            kind: "supplier_reply".into(),
            conversation: None,
            input: json!({"message_id":r["message_id"],"thread_id":t}),
            lease: Uuid::new_v4(),
            attempt: 1,
        };
        let review = || {
            outreach::Review {
            summary: if complete { "Complete quote: 8 kg at NGN 4000/kg, 1 kg minimum, delivery tomorrow free, pay on delivery." } else { "Delivery and terms missing." }.into(),
            missing_fields: if complete { vec![] } else { vec!["delivery".into(), "payment_terms".into()] }, needs_person: false,
        }
        };
        let result = outreach::review(&pool, &run, review()).await.unwrap();
        assert_eq!(result["offer_ready"], complete);
        assert_eq!(result["reply_prepared"], !complete);
        assert_eq!(
            outreach::review(&pool, &run, review()).await.unwrap()["reused"],
            true
        );
        let d = outreach::detail(&pool, w, t).await.unwrap();
        assert_eq!(
            d["thread"]["status"],
            if complete {
                "offer_ready"
            } else {
                "reply_ready"
            }
        );
        let pending = d["messages"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|m| m["status"] == "review_pending")
            .count();
        assert_eq!(pending, if complete { 0 } else { 1 });
    }
    let reopened = outreach::create(&pool, w, &cfg, v, create()).await.unwrap();
    assert_eq!(reopened["thread"]["id"], t.to_string());
    assert_eq!(reopened["thread"]["status"], "offer_ready");
    assert_eq!(
        reopened["messages"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|m| m["direction"] == "outbound")
            .count(),
        2
    );
    // One original enquiry and one superseded request for missing details; no
    // acknowledgement is queued that could return the completed offer to waiting.
}
