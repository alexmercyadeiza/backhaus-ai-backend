//! Inventory-to-purchase-order workflow: detection, ordering rules, missing
//! details, idempotency under retries and concurrency, pause/resume catch-up,
//! approvals and the dashboard pages.
mod common;
use axum::{
    Json,
    body::Body,
    http::{Request, StatusCode},
};
use backhaus_ai_backend::{
    api::{self, AppState},
    purchasing, scoped_agents as agents, tables,
};
use common::*;
use http_body_util::BodyExt;
use rust_decimal::Decimal;
use serde_json::{Value, json};
use std::sync::Arc;
use tower::ServiceExt;
use uuid::Uuid;

fn d(value: &str) -> Decimal {
    value.parse().unwrap()
}
/// Run every pending role check (sales and inventory) and return the inventory findings.
async fn check(pool: &sqlx::PgPool, workspace: &str) -> Value {
    let mut ran = false;
    while agents::check_one(pool, workspace, false).await.unwrap() {
        ran = true;
    }
    assert!(ran, "expected a pending check");
    let list = agents::list(pool, workspace).await.unwrap();
    list["agents"][1]["observation"].clone()
}
fn find<'a>(orders: &'a [Value], vendor: &str) -> &'a Value {
    orders
        .iter()
        .find(|o| o["vendor"] == vendor)
        .unwrap_or_else(|| panic!("no order for {vendor}: {orders:?}"))
}

#[test]
fn ordering_rule_rounds_up_respects_minimums_and_open_orders() {
    let p = |balance, par, target: Option<&str>, on_order, upp, min| {
        purchasing::packs_to_order(
            d(balance),
            d(par),
            target.map(d),
            d(on_order),
            d(upp),
            d(min),
        )
    };
    assert_eq!(p("2", "10", None, "0", "1", "1"), Some(d("8")));
    assert_eq!(p("2", "10", None, "0", "5", "1"), Some(d("2")));
    assert_eq!(p("2", "10", None, "0", "1", "12"), Some(d("12")));
    assert_eq!(p("2", "10", Some("25"), "0", "1", "1"), Some(d("23")));
    assert_eq!(
        p("2", "10", Some("4"), "0", "1", "1"),
        Some(d("8")),
        "target below par still fills to par"
    );
    assert_eq!(
        p("2", "10", None, "8", "1", "1"),
        None,
        "covered by an approved order"
    );
    assert_eq!(p("2.8", "5", None, "0", "1", "1"), Some(d("3")));
    assert_eq!(p("2", "10", None, "0", "0", "1"), None);
    assert!(purchasing::describe_reason("manual_approval_required").len() > 10);
}

#[tokio::test]
#[ignore = "Requires dedicated PostgreSQL test database"]
async fn below_par_items_become_costed_drafts_and_missing_details_are_surfaced() {
    let (pool, cfg) = setup().await;
    let w = &cfg.workspace_id;
    // Automatic approval at or below NGN 500: Gamma (80) qualifies, Alpha never does.
    policy(&pool, w, "500", Some("5000")).await;
    let alpha = vendor(&pool, w, "Alpha Foods", true).await;
    let beta = vendor(&pool, w, "Beta Supplies", false).await;
    let gamma = vendor(&pool, w, "Gamma Wholesale", true).await;
    assign(&pool, w, "item-0", alpha, Some("100"), "1", "1", None).await;
    assign(&pool, w, "item-2", alpha, Some("50"), "5", "1", None).await;
    assign(&pool, w, "item-4", alpha, Some("10"), "1", "1", Some("25")).await;
    assign(&pool, w, "item-6", beta, None, "1", "1", None).await;
    assign(&pool, w, "item-8", beta, Some("100"), "1", "1", None).await;
    assign(&pool, w, "item-12", gamma, Some("10"), "1", "1", None).await;
    assign(&pool, w, "item-1", gamma, Some("10"), "1", "1", None).await; // item-1 is stocked
    let observation = check(&pool, w).await;
    assert_eq!(observation["below_par"], 25);
    let orders = observation["orders"].as_array().unwrap();
    assert_eq!(orders.len(), 3, "{observation}");
    let alpha_order = find(orders, "Alpha Foods");
    assert_eq!(alpha_order["status"], "draft");
    assert_eq!(alpha_order["line_count"], 3);
    assert_eq!(d(alpha_order["subtotal"].as_str().unwrap()), d("1130.00"));
    assert_eq!(alpha_order["approval_reason"], "exceeds_auto_approval_limit");
    let beta_order = find(orders, "Beta Supplies");
    assert_eq!(beta_order["line_count"], 1);
    assert_eq!(beta_order["approval_reason"], "vendor_details_incomplete");
    assert_eq!(
        beta_order["attention"][0]["reason"],
        "vendor_contact_missing"
    );
    let gamma_order = find(orders, "Gamma Wholesale");
    assert_eq!(gamma_order["status"], "approved");
    assert_eq!(observation["auto_approved"], 1);
    assert_eq!(gamma_order["approval_kind"], "automatic");
    assert_eq!(gamma_order["approval_reason"], "within_auto_approval_limit");
    assert_eq!(d(gamma_order["subtotal"].as_str().unwrap()), d("80.00"));
    let reasons: Vec<(String, String)> = observation["attention"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| {
            (
                a["item_id"].as_str().unwrap().into(),
                a["reason"].as_str().unwrap().into(),
            )
        })
        .collect();
    assert!(reasons.contains(&("item-6".into(), "no_price".into())));
    assert!(reasons.contains(&("item-10".into(), "no_vendor".into())));
    assert!(
        observation["summary"]
            .as_str()
            .unwrap()
            .contains("3 orders prepared")
    );
    let detail = purchasing::order_detail(
        &pool,
        w,
        alpha_order["id"].as_str().unwrap().parse().unwrap(),
        1,
        20,
    )
    .await
    .unwrap();
    let lines = detail["items"].as_array().unwrap();
    assert_eq!(lines.len(), 3);
    let line0 = lines.iter().find(|l| l["item_id"] == "item-0").unwrap();
    assert_eq!(d(line0["quantity_packs"].as_str().unwrap()), d("8"));
    assert_eq!(d(line0["line_total"].as_str().unwrap()), d("800.00"));
    let line2 = lines.iter().find(|l| l["item_id"] == "item-2").unwrap();
    assert_eq!(d(line2["quantity_packs"].as_str().unwrap()), d("2"));
    assert_eq!(d(line2["quantity_units"].as_str().unwrap()), d("10"));
    let line4 = lines.iter().find(|l| l["item_id"] == "item-4").unwrap();
    assert_eq!(d(line4["quantity_packs"].as_str().unwrap()), d("23"));
    assert_eq!(detail["record"]["events"][0]["type"], "drafted");
    assert_eq!(
        detail["record"]["vendor_contact"]["email"],
        "alpha-foods@example.invalid"
    );
    // The model view carries no contact details.
    let model_view = purchasing::order_for_model(
        &pool,
        w,
        alpha_order["id"].as_str().unwrap().parse().unwrap(),
    )
    .await
    .unwrap();
    assert!(model_view.get("vendor_contact").is_none());
    assert_eq!(model_view["lines"].as_array().unwrap().len(), 3);
    assert!(
        !purchasing::findings_for_model(&pool, w)
            .await
            .unwrap()
            .to_string()
            .contains("example.invalid")
    );

    // Repeating the check on a new revision without stock changes keeps the same orders.
    sqlx::query("UPDATE vendors SET notes='touched' WHERE workspace_id=$1 AND id=$2")
        .bind(w)
        .bind(alpha)
        .execute(&pool)
        .await
        .unwrap();
    let again = check(&pool, w).await;
    // Alpha and Beta stay as drafts; Gamma was approved by the agent and now covers its shortage.
    assert_eq!(again["orders"].as_array().unwrap().len(), 2, "{again}");
    assert!(
        again["orders"]
            .as_array()
            .unwrap()
            .iter()
            .all(|o| o["change"] == "unchanged")
    );
    assert_eq!(again["covered_by_open_orders"], 1);
    let all = orders_in(&pool, w).await;
    assert_eq!(all.len(), 3, "no duplicate orders: {all:?}");
    assert!(all.iter().filter(|o| o["status"] == "draft").all(|o| o["version"] == 1));

    // A stock change revises the existing draft in place.
    set_balance(&pool, w, "item-0", "5").await;
    let revised = check(&pool, w).await;
    let alpha_revised = find(revised["orders"].as_array().unwrap(), "Alpha Foods");
    assert_eq!(alpha_revised["change"], "revised");
    assert_eq!(alpha_revised["id"], alpha_order["id"]);
    assert_eq!(d(alpha_revised["subtotal"].as_str().unwrap()), d("830.00"));
    let all = orders_in(&pool, w).await;
    assert_eq!(find(&all, "Alpha Foods")["version"], 2);

    purchasing::decide(
        &pool,
        w,
        gamma_order["id"].as_str().unwrap().parse().unwrap(),
        "approve",
        "tester",
        None,
    )
    .await
    .unwrap();

    // Once nothing is short for that vendor the draft is withdrawn, not left stale.
    set_balance(&pool, w, "item-0", "20").await;
    set_balance(&pool, w, "item-2", "20").await;
    set_balance(&pool, w, "item-4", "30").await;
    let withdrawn = check(&pool, w).await;
    assert_eq!(
        withdrawn["withdrawn"][0], alpha_order["number"],
        "{withdrawn}"
    );
    assert_eq!(
        find(&orders_in(&pool, w).await, "Alpha Foods")["status"],
        "withdrawn"
    );

    // Approved quantities count as on order, so the shortage is not ordered twice.
    let covered = withdrawn["attention"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["item_id"] == "item-12")
        .unwrap();
    assert_eq!(covered["reason"], "covered_by_open_order");
    set_balance(&pool, w, "item-12", "0").await;
    let extra = check(&pool, w).await;
    let gamma_orders: Vec<&Value> = orders_in(&pool, w)
        .await
        .into_iter()
        .collect::<Vec<_>>()
        .iter()
        .filter(|o| o["vendor"] == "Gamma Wholesale")
        .cloned()
        .collect::<Vec<_>>()
        .leak()
        .iter()
        .collect();
    assert_eq!(gamma_orders.len(), 2, "{extra}");
    let new_gamma = find(extra["orders"].as_array().unwrap(), "Gamma Wholesale");
    assert_eq!(new_gamma["change"], "drafted");
    let detail = purchasing::order_detail(
        &pool,
        w,
        new_gamma["id"].as_str().unwrap().parse().unwrap(),
        1,
        20,
    )
    .await
    .unwrap();
    assert_eq!(
        d(detail["items"][0]["quantity_packs"].as_str().unwrap()),
        d("2")
    );
    assert_eq!(
        d(detail["items"][0]["on_order_units"].as_str().unwrap()),
        d("8")
    );
}
async fn orders_in(pool: &sqlx::PgPool, workspace: &str) -> Vec<Value> {
    orders(pool, workspace).await
}

#[tokio::test]
#[ignore = "Requires dedicated PostgreSQL test database"]
async fn concurrent_checks_retries_and_reviews_do_not_duplicate_work() {
    let (pool, cfg) = setup().await;
    let w = &cfg.workspace_id;
    let alpha = vendor(&pool, w, "Alpha Foods", true).await;
    for i in (0..20).step_by(2) {
        assign(
            &pool,
            w,
            &format!("item-{i}"),
            alpha,
            Some("100"),
            "1",
            "1",
            None,
        )
        .await;
    }
    let (a, b, c) = tokio::join!(
        agents::check_one(&pool, w, true),
        agents::check_one(&pool, w, true),
        agents::check_one(&pool, w, true)
    );
    let ran = [a, b, c]
        .into_iter()
        .filter(|r| r.as_ref().is_ok_and(|v| *v))
        .count();
    assert!(ran >= 1);
    let first_orders = orders(&pool, w).await;
    assert_eq!(first_orders.len(), 1, "{first_orders:?}");
    assert_eq!(first_orders[0]["line_count"], 10);
    let reviews: Vec<(String, String)> = sqlx::query_as("SELECT request_key,status FROM agent_runs WHERE workspace_id=$1 AND kind='inventory_review' ORDER BY created_at")
        .bind(w).fetch_all(&pool).await.unwrap();
    assert_eq!(reviews.len(), 1, "one review per revision: {reviews:?}");
    assert!(reviews[0].0.starts_with("inventory-review:"));
    assert_eq!(reviews[0].1, "queued");
    // A retried check of the same revision is a no-op; the next revision adds one review.
    assert!(!agents::check_one(&pool, w, true).await.unwrap());
    set_balance(&pool, w, "item-0", "3").await;
    let (x, y) = tokio::join!(
        agents::check_one(&pool, w, true),
        agents::check_one(&pool, w, true)
    );
    assert!(x.unwrap() || y.unwrap());
    assert_eq!(orders(&pool, w).await.len(), 1);
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_runs WHERE workspace_id=$1 AND kind='inventory_review'",
    )
    .bind(w)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(count, 2);
    // Without a model configured no review is queued.
    set_balance(&pool, w, "item-0", "4").await;
    assert!(agents::check_one(&pool, w, false).await.unwrap());
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_runs WHERE workspace_id=$1 AND kind='inventory_review'",
    )
    .bind(w)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(count, 2);
    // Reviews attach only to the checkpoint they describe.
    let run = backhaus_ai_backend::jobs::Run {
        id: Uuid::new_v4(),
        workspace: w.clone(),
        kind: "inventory_review".into(),
        conversation: None,
        input: json!({"revision": 0}),
        lease: Uuid::new_v4(),
        attempt: 1,
    };
    purchasing::record_review(&pool, &run, &json!({"answer":"old"}))
        .await
        .unwrap();
    let list = agents::list(&pool, w).await.unwrap();
    assert!(list["agents"][1]["observation"]["review"].is_null());
    let current = list["agents"][1]["checked_revision"].as_i64().unwrap();
    let run = backhaus_ai_backend::jobs::Run {
        input: json!({"revision": current}),
        ..run
    };
    // An unpersisted/cancelled run cannot attach a result even to the current revision.
    purchasing::record_review(&pool, &run, &json!({"answer":"Untrusted"}))
        .await
        .unwrap();
    assert!(agents::list(&pool, w).await.unwrap()["agents"][1]["observation"]["review"].is_null());
    sqlx::query("INSERT INTO agent_runs(id,workspace_id,request_key,kind,status,input) VALUES($1,$2,'checkpoint-test','inventory_review','completed',$3)")
        .bind(run.id).bind(w).bind(&run.input).execute(&pool).await.unwrap();
    purchasing::record_review(&pool, &run, &json!({"answer":"One order prepared."}))
        .await
        .unwrap();
    let list = agents::list(&pool, w).await.unwrap();
    assert_eq!(
        list["agents"][1]["observation"]["review"],
        "One order prepared."
    );
    assert_eq!(list["agents"][1]["review"]["status"], "completed");
    // Workspace isolation: another workspace sees nothing of this.
    assert!(orders(&pool, "other-workspace").await.is_empty());
    assert_eq!(
        purchasing::findings_for_model(&pool, "other-workspace")
            .await
            .unwrap()["status"],
        "no_check_yet"
    );
}

#[tokio::test]
#[ignore = "Requires dedicated PostgreSQL test database"]
async fn pause_and_resume_catch_up_from_durable_state() {
    let (pool, cfg) = setup().await;
    let w = &cfg.workspace_id;
    let alpha = vendor(&pool, w, "Alpha Foods", true).await;
    assign(&pool, w, "item-0", alpha, Some("100"), "1", "1", None).await;
    assign(&pool, w, "item-1", alpha, Some("100"), "1", "1", None).await;
    let first = check(&pool, w).await;
    assert_eq!(first["orders"][0]["line_count"], 1);
    agents::control(&pool, w, "inventory", "pause")
        .await
        .unwrap();
    set_balance(&pool, w, "item-1", "1").await; // now short as well
    set_balance(&pool, w, "item-0", "1").await;
    assert!(
        !agents::check_one(&pool, w, false).await.unwrap(),
        "paused agents start no work"
    );
    assert_eq!(orders(&pool, w).await[0]["line_count"], 1);
    let paused = agents::list(&pool, w).await.unwrap();
    assert_eq!(paused["agents"][1]["status"], "paused");
    assert!(
        paused["agents"][1]["data_revision"].as_i64().unwrap()
            > paused["agents"][1]["checked_revision"].as_i64().unwrap()
    );
    agents::control(&pool, w, "inventory", "resume")
        .await
        .unwrap();
    let caught_up = check(&pool, w).await;
    assert_eq!(
        caught_up["orders"][0]["line_count"], 2,
        "coalesced changes handled in one check"
    );
    assert_eq!(caught_up["orders"][0]["change"], "revised");
    let list = agents::list(&pool, w).await.unwrap();
    assert_eq!(
        list["agents"][1]["checked_revision"],
        list["agents"][1]["data_revision"]
    );
    assert!(!agents::check_one(&pool, w, false).await.unwrap());
    let o = orders(&pool, w).await;
    assert_eq!(o.len(), 1);
    assert_eq!(d(o[0]["subtotal"].as_str().unwrap()), d("1800.00"));
}

#[tokio::test]
#[ignore = "Requires dedicated PostgreSQL test database"]
async fn approvals_enforce_limits_transitions_and_repeat_safely() {
    let (pool, cfg) = setup().await;
    let w = &cfg.workspace_id;
    policy(&pool, w, "0", Some("1000")).await;
    let alpha = vendor(&pool, w, "Alpha Foods", true).await;
    let beta = vendor(&pool, w, "Beta Supplies", true).await;
    let gamma = vendor(&pool, w, "Gamma Wholesale", true).await;
    assign(&pool, w, "item-0", alpha, Some("100"), "1", "1", None).await; // 800
    assign(&pool, w, "item-2", beta, Some("500"), "1", "1", None).await; // 4000 > limit
    assign(&pool, w, "item-4", gamma, Some("10"), "1", "1", None).await; // 80
    check(&pool, w).await;
    let all = orders(&pool, w).await;
    let alpha_id: Uuid = find(&all, "Alpha Foods")["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let beta_id: Uuid = find(&all, "Beta Supplies")["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let gamma_id: Uuid = find(&all, "Gamma Wholesale")["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(
        find(&all, "Beta Supplies")["approval_reason"],
        "exceeds_approval_limit"
    );
    assert_eq!(
        find(&all, "Alpha Foods")["approval_reason"],
        "manual_approval_required"
    );
    let approved = purchasing::decide(&pool, w, alpha_id, "approve", "tester", Some("ok"))
        .await
        .unwrap();
    assert_eq!(approved["status"], "approved");
    assert_eq!(approved["approval_kind"], "manual");
    assert_eq!(approved["changed"], true);
    assert_eq!(approved["decided_by"], "tester");
    let repeated = purchasing::decide(&pool, w, alpha_id, "approve", "tester", None)
        .await
        .unwrap();
    assert_eq!(repeated["changed"], false);
    assert_eq!(repeated["version"], approved["version"]);
    let events: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM purchase_order_events WHERE purchase_order_id=$1 AND event_type='approved'").bind(alpha_id).fetch_one(&pool).await.unwrap();
    assert_eq!(events, 1);
    assert!(
        purchasing::decide(&pool, w, alpha_id, "reject", "tester", None)
            .await
            .is_err()
    );
    let blocked = purchasing::decide(&pool, w, beta_id, "approve", "tester", None)
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(blocked.contains("approval limit"), "{blocked}");
    assert_eq!(
        orders(&pool, w)
            .await
            .iter()
            .find(|o| o["vendor"] == "Beta Supplies")
            .unwrap()["status"],
        "draft"
    );
    let rejected = purchasing::decide(&pool, w, gamma_id, "reject", "tester", Some("not needed"))
        .await
        .unwrap();
    assert_eq!(rejected["status"], "rejected");
    assert_eq!(
        purchasing::decide(&pool, w, gamma_id, "reject", "tester", None)
            .await
            .unwrap()["changed"],
        false
    );
    assert!(
        purchasing::decide(&pool, w, gamma_id, "approve", "tester", None)
            .await
            .is_err()
    );
    assert!(
        purchasing::decide(&pool, w, gamma_id, "delete", "tester", None)
            .await
            .is_err()
    );
    assert!(
        purchasing::decide(
            &pool,
            "other-workspace",
            alpha_id,
            "approve",
            "tester",
            None
        )
        .await
        .is_err()
    );
    // Two clicks at once record one decision.
    policy(&pool, w, "0", Some("100000")).await;
    let (x, y) = tokio::join!(
        purchasing::decide(&pool, w, beta_id, "approve", "tester", None),
        purchasing::decide(&pool, w, beta_id, "approve", "tester", None)
    );
    let changes = [x.unwrap(), y.unwrap()]
        .iter()
        .filter(|v| v["changed"] == true)
        .count();
    assert_eq!(changes, 1);
    // The approved quantity is on order; a recheck neither duplicates nor withdraws it.
    sqlx::query("UPDATE vendors SET notes='touch' WHERE workspace_id=$1 AND id=$2")
        .bind(w)
        .bind(beta)
        .execute(&pool)
        .await
        .unwrap();
    let after = check(&pool, w).await;
    assert!(
        after["orders"]
            .as_array()
            .unwrap()
            .iter()
            .all(|o| o["vendor"] != "Beta Supplies")
    );
    assert_eq!(
        orders(&pool, w)
            .await
            .iter()
            .filter(|o| o["vendor"] == "Beta Supplies")
            .count(),
        1
    );
    // HTTP surface: authenticated, workspace-scoped, idempotent, honest status codes.
    let app = api::router(AppState {
        pool: pool.clone(),
        config: Arc::new(cfg.clone()),
    });
    let post = |path: String, auth: bool| {
        let mut builder = Request::builder()
            .method("POST")
            .uri(path)
            .header("content-type", "application/json");
        if auth {
            builder = builder.header("authorization", format!("Bearer {}", cfg.api_key));
        }
        builder
            .body(Body::from(json!({"note":"via api"}).to_string()))
            .unwrap()
    };
    let denied = app
        .clone()
        .oneshot(post(
            format!("/v1/purchase-orders/{beta_id}/approve"),
            false,
        ))
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
    let response = app
        .clone()
        .oneshot(post(format!("/v1/purchase-orders/{beta_id}/approve"), true))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(body["changed"], false);
    let response = app
        .clone()
        .oneshot(post(
            format!("/v1/purchase-orders/{gamma_id}/approve"),
            true,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let response = app
        .clone()
        .oneshot(post(
            format!("/v1/purchase-orders/{}/approve", Uuid::new_v4()),
            true,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let response = app
        .clone()
        .oneshot(post(
            format!("/v1/purchase-orders/{alpha_id}/archive"),
            true,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let _ = Json(());
}

#[tokio::test]
#[ignore = "Requires dedicated PostgreSQL test database"]
async fn purchase_order_pages_and_details_are_real_scoped_and_paginated() {
    let (pool, cfg) = setup().await;
    let w = &cfg.workspace_id;
    for i in 0..21 {
        let v = vendor(&pool, w, &format!("Vendor {i:02}"), true).await;
        assign(
            &pool,
            w,
            &format!("item-{}", i * 2),
            v,
            Some("100"),
            "1",
            "1",
            None,
        )
        .await;
    }
    check(&pool, w).await;
    let page1 = tables::page(
        &pool,
        w,
        "purchase-orders",
        &tables::TableQuery {
            page: Some(1),
            status: None,
            search: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(page1["available"], true);
    assert_eq!(page1["total"], 21);
    assert_eq!(page1["pages"], 2);
    assert_eq!(page1["items"].as_array().unwrap().len(), 20);
    assert_eq!(page1["items"][0]["requires_approval"], true);
    assert!(
        page1["items"][0]["approval_explanation"]
            .as_str()
            .unwrap()
            .len()
            > 10
    );
    let page2 = tables::page(
        &pool,
        w,
        "purchase-orders",
        &tables::TableQuery {
            page: Some(2),
            status: None,
            search: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(page2["items"].as_array().unwrap().len(), 1);
    let ids: std::collections::HashSet<String> = page1["items"]
        .as_array()
        .unwrap()
        .iter()
        .chain(page2["items"].as_array().unwrap())
        .map(|i| i["id"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(ids.len(), 21, "pages are complete and disjoint");
    let first: Uuid = page1["items"][0]["id"].as_str().unwrap().parse().unwrap();
    purchasing::decide(&pool, w, first, "approve", "tester", None)
        .await
        .unwrap();
    let drafts = tables::page(
        &pool,
        w,
        "purchase-orders",
        &tables::TableQuery {
            page: Some(1),
            status: Some("draft".into()),
            search: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(drafts["total"], 20);
    let open = tables::page(
        &pool,
        w,
        "purchase-orders",
        &tables::TableQuery {
            page: Some(1),
            status: Some("open".into()),
            search: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(open["total"], 21);
    assert!(
        open["items"]
            .as_array()
            .unwrap()
            .iter()
            .all(|o| o["status"] == "draft"),
        "drafts sort first"
    );
    let open2 = tables::page(
        &pool,
        w,
        "purchase-orders",
        &tables::TableQuery {
            page: Some(2),
            status: Some("open".into()),
            search: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(open2["items"][0]["status"], "approved");
    assert!(
        tables::page(
            &pool,
            w,
            "purchase-orders",
            &tables::TableQuery {
                page: Some(1),
                status: Some("paid".into()),
                search: None,
            }
        )
        .await
        .is_err()
    );
    assert!(
        tables::page(
            &pool,
            w,
            "inventory",
            &tables::TableQuery {
                page: Some(1),
                status: Some("draft".into()),
                search: None,
            }
        )
        .await
        .is_err()
    );
    let detail = tables::detail(
        &pool,
        w,
        "purchase-orders",
        &first.to_string(),
        &tables::TableQuery::default(),
    )
    .await
    .unwrap();
    assert_eq!(detail["record"]["status"], "approved");
    assert_eq!(detail["record"]["vendor"], page1["items"][0]["vendor"]);
    assert_eq!(detail["items"].as_array().unwrap().len(), 1);
    assert_eq!(detail["record"]["events"][0]["type"], "approved");
    assert!(
        tables::detail(
            &pool,
            "other-workspace",
            "purchase-orders",
            &first.to_string(),
            &tables::TableQuery::default()
        )
        .await
        .is_err()
    );
    assert!(
        tables::detail(
            &pool,
            w,
            "purchase-orders",
            "not-a-uuid",
            &tables::TableQuery::default()
        )
        .await
        .is_err()
    );
    assert_eq!(
        tables::page(
            &pool,
            "other-workspace",
            "purchase-orders",
            &tables::TableQuery::default()
        )
        .await
        .unwrap()["total"],
        0
    );
    let app = api::router(AppState {
        pool: pool.clone(),
        config: Arc::new(cfg.clone()),
    });
    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/v1/tables/purchase-orders/{first}"))
                .header("authorization", format!("Bearer {}", cfg.api_key))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}
