//! Synthetic dataset invariants, deterministic re-initialization, guarded reset
//! and the documented starting scenario (including the first inventory check).
mod common;
use backhaus_ai_backend::{demo, scoped_agents as agents};
use common::*;
use rust_decimal::Decimal;
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

fn as_of() -> chrono::NaiveDate {
    "2026-09-14".parse().unwrap()
}
fn fresh_workspace() -> String {
    format!("demo-{}", Uuid::new_v4())
}
async fn ticket_digest(pool: &PgPool, w: &str) -> String {
    sqlx::query_scalar::<_, String>("SELECT md5(string_agg(id::text||':'||business_date||':'||total_amount::text, ',' ORDER BY id)) FROM sales_tickets WHERE workspace_id=$1").bind(w).fetch_one(pool).await.unwrap()
}
async fn count(pool: &PgPool, sql: &str, workspace: &str) -> i64 {
    sqlx::query_scalar(sql)
        .bind(workspace)
        .fetch_one(pool)
        .await
        .unwrap()
}

#[test]
fn complete_weeks_end_before_the_anchor() {
    let (recent, previous) = demo::complete_weeks("2026-09-14".parse().unwrap()); // a Monday
    assert_eq!(recent.start.to_string(), "2026-09-07");
    assert_eq!(recent.end.to_string(), "2026-09-13");
    assert_eq!(previous.start.to_string(), "2026-08-31");
    assert_eq!(previous.end.to_string(), "2026-09-06");
    let (recent, _) = demo::complete_weeks("2026-09-13".parse().unwrap()); // a Sunday: current week is not complete
    assert_eq!(recent.end.to_string(), "2026-09-06");
}

#[tokio::test]
#[ignore = "Requires dedicated PostgreSQL test database"]
async fn synthetic_dataset_matches_the_documented_scenario_and_is_deterministic() {
    let (pool, _) = setup().await;
    let w = fresh_workspace();
    let options = demo::Options {
        as_of: as_of(),
        seed: demo::DEFAULT_SEED,
    };
    let first = demo::init(&pool, &w, &options).await.unwrap();
    assert_eq!(first["status"], "initialized");
    assert_eq!(
        count(
            &pool,
            "SELECT COUNT(*) FROM menu_items WHERE workspace_id=$1",
            &w
        )
        .await,
        20
    );
    assert_eq!(
        count(
            &pool,
            "SELECT COUNT(DISTINCT category) FROM menu_items WHERE workspace_id=$1",
            &w
        )
        .await,
        2
    );
    let categories: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT category FROM menu_items WHERE workspace_id=$1 ORDER BY 1",
    )
    .bind(&w)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(categories, vec!["Drinks", "Food"]);
    assert_eq!(
        count(
            &pool,
            "SELECT COUNT(*) FROM inventory_items WHERE workspace_id=$1",
            &w
        )
        .await,
        50
    );
    assert_eq!(count(&pool, "SELECT COUNT(*) FROM inventory_items WHERE workspace_id=$1 AND (par_level IS NULL OR par_level<=0)", &w).await, 0);
    assert_eq!(
        count(
            &pool,
            "SELECT COUNT(*) FROM vendors WHERE workspace_id=$1",
            &w
        )
        .await,
        7
    );
    assert_eq!(count(&pool, "SELECT COUNT(*) FROM inventory_items i WHERE i.workspace_id=$1 AND NOT EXISTS (SELECT 1 FROM vendor_items v WHERE v.workspace_id=i.workspace_id AND v.item_id=i.id)", &w).await, 1);
    let short: Vec<String> = sqlx::query_scalar("SELECT name FROM inventory_items WHERE workspace_id=$1 AND current_balance<par_level ORDER BY name").bind(&w).fetch_all(&pool).await.unwrap();
    assert_eq!(
        short,
        vec!["Chicken Breast", "Lemons", "Palm Oil", "Premium Whisky"]
    );
    // Sales: 61 days ending at the anchor, coherent totals, voided tickets excluded from gross.
    let sales = sqlx::query("SELECT COUNT(*) AS tickets, COUNT(DISTINCT business_date) AS days, MIN(business_date) AS first, MAX(business_date) AS last FROM sales_tickets WHERE workspace_id=$1").bind(&w).fetch_one(&pool).await.unwrap();
    use sqlx::Row;
    assert_eq!(sales.get::<i64, _>("days"), 61);
    assert_eq!(sales.get::<chrono::NaiveDate, _>("last"), as_of());
    assert_eq!(
        sales.get::<chrono::NaiveDate, _>("first").to_string(),
        "2026-07-16"
    );
    assert_eq!(count(&pool, "SELECT COUNT(*) FROM sales_tickets t WHERE t.workspace_id=$1 AND t.total_amount<>0 AND t.total_amount<>(SELECT SUM(quantity*unit_price) FROM sales_lines l WHERE l.workspace_id=t.workspace_id AND l.ticket_id=t.id)", &w).await, 0);
    assert!(count(&pool, "SELECT COUNT(*) FROM sales_tickets WHERE workspace_id=$1 AND business_date=(SELECT MAX(business_date) FROM sales_tickets WHERE workspace_id=$1)", &w).await >= 1, "today has sales");
    // Independent arithmetic: the API's gross for the recent complete week equals SQL over billable lines of non-voided tickets.
    let range = backhaus_ai_backend::data::DateRange {
        from: "2026-09-07".parse().unwrap(),
        to: "2026-09-13".parse().unwrap(),
    };
    let api = backhaus_ai_backend::data::sales(&pool, &w, &range)
        .await
        .unwrap();
    let sql: Decimal = sqlx::query_scalar("SELECT COALESCE(SUM(l.quantity*l.unit_price),0) FROM sales_lines l JOIN sales_tickets t ON t.workspace_id=l.workspace_id AND t.id=l.ticket_id WHERE l.workspace_id=$1 AND l.billable AND t.total_amount<>0 AND t.business_date BETWEEN '2026-09-07' AND '2026-09-13'").bind(&w).fetch_one(&pool).await.unwrap();
    assert_eq!(
        api["gross_line_sales"]
            .as_str()
            .unwrap()
            .parse::<Decimal>()
            .unwrap(),
        sql
    );
    let ranking = backhaus_ai_backend::data::sales_ranking(&pool, &w, &range, 3)
        .await
        .unwrap();
    assert_eq!(ranking["items"][0]["item"], "Jollof Rice & Grilled Chicken");
    assert_eq!(ranking["coverage"]["status"], "complete");
    let previous = backhaus_ai_backend::data::sales_ranking(
        &pool,
        &w,
        &backhaus_ai_backend::data::DateRange {
            from: "2026-08-24".parse().unwrap(),
            to: "2026-08-30".parse().unwrap(),
        },
        1,
    )
    .await
    .unwrap();
    let recent_total: Decimal = api["gross_line_sales"].as_str().unwrap().parse().unwrap();
    let previous_total: Decimal = sqlx::query_scalar("SELECT COALESCE(SUM(l.quantity*l.unit_price),0) FROM sales_lines l JOIN sales_tickets t ON t.workspace_id=l.workspace_id AND t.id=l.ticket_id WHERE l.workspace_id=$1 AND l.billable AND t.total_amount<>0 AND t.business_date BETWEEN '2026-08-31' AND '2026-09-06'").bind(&w).fetch_one(&pool).await.unwrap();
    assert!(
        recent_total > previous_total,
        "recent complete week is busier: {recent_total} vs {previous_total}"
    );
    assert_eq!(previous["coverage"]["days_with_records"], 7);
    let empty = backhaus_ai_backend::data::sales_ranking(
        &pool,
        &w,
        &backhaus_ai_backend::data::DateRange {
            from: "2025-01-01".parse().unwrap(),
            to: "2025-01-07".parse().unwrap(),
        },
        3,
    )
    .await
    .unwrap();
    assert_eq!(empty["coverage"]["status"], "no_records");
    assert!(empty["items"].as_array().unwrap().is_empty());
    // Deterministic: a second workspace with the same seed and anchor is identical.
    let w2 = fresh_workspace();
    demo::init(&pool, &w2, &options).await.unwrap();
    assert_eq!(
        ticket_digest(&pool, &w).await,
        ticket_digest(&pool, &w2).await
    );
    // Re-running is a no-op; a different anchor is refused without a reset.
    assert_eq!(
        demo::init(&pool, &w, &options).await.unwrap()["status"],
        "already_initialized"
    );
    assert!(
        demo::init(
            &pool,
            &w,
            &demo::Options {
                as_of: as_of() - chrono::Duration::days(1),
                seed: demo::DEFAULT_SEED
            }
        )
        .await
        .is_err()
    );
    assert_eq!(
        count(
            &pool,
            "SELECT COUNT(*) FROM sales_tickets WHERE workspace_id=$1",
            &w
        )
        .await,
        sales.get::<i64, _>("tickets")
    );
    // The first inventory check produces exactly the documented starting scenario.
    let mut ran = false;
    while agents::check_one(&pool, &w, false).await.unwrap() {
        ran = true;
    }
    assert!(ran);
    let orders = common::orders(&pool, &w).await;
    let by_vendor = |name: &str| orders.iter().find(|o| o["vendor"] == name).cloned();
    let lemons = by_vendor("Lagoon Fresh Produce").expect("produce order");
    assert_eq!(lemons["status"], "draft");
    assert!(lemons["approval_kind"].is_null());
    assert_eq!(
        lemons["subtotal"]
            .as_str()
            .unwrap()
            .parse::<Decimal>()
            .unwrap(),
        Decimal::from(3000)
    );
    let chicken = by_vendor("Harbour Proteins").expect("proteins order");
    assert_eq!(chicken["status"], "draft");
    assert_eq!(chicken["approval_reason"], "manual_approval_required");
    assert_eq!(
        chicken["subtotal"]
            .as_str()
            .unwrap()
            .parse::<Decimal>()
            .unwrap(),
        Decimal::from(27000)
    );
    let whisky = by_vendor("Ridge Spirits & Wine").expect("spirits order");
    assert_eq!(whisky["approval_reason"], "exceeds_approval_limit");
    assert_eq!(
        whisky["subtotal"]
            .as_str()
            .unwrap()
            .parse::<Decimal>()
            .unwrap(),
        Decimal::from(450000)
    );
    assert!(
        by_vendor("Golden Grain Provisions").is_none(),
        "nothing short for provisions yet"
    );
    assert_eq!(orders.len(), 3);
    let findings = backhaus_ai_backend::purchasing::findings_for_model(&pool, &w)
        .await
        .unwrap();
    let missing: Vec<&Value> = findings["attention"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|a| a["reason"] == "no_vendor")
        .collect();
    assert_eq!(missing.len(), 1);
    assert_eq!(missing[0]["item"], "Palm Oil");
    assert_eq!(findings["attention_total"], 1, "{findings}");
}

#[tokio::test]
#[ignore = "Requires dedicated PostgreSQL test database"]
async fn reset_is_guarded_scoped_and_clears_derived_state() {
    let (pool, cfg) = setup().await;
    // The snapshot fixture workspace is not demo-owned: refused without the explicit flag.
    let options = demo::Options {
        as_of: as_of(),
        seed: 7,
    };
    let refused = demo::reset(&pool, &cfg.workspace_id, &options, false).await;
    assert!(refused.is_err(), "imported data needs --replace-imported");
    assert_eq!(
        count(
            &pool,
            "SELECT COUNT(*) FROM inventory_items WHERE workspace_id=$1",
            &cfg.workspace_id
        )
        .await,
        50
    );
    // A demo workspace with derived state: runs, orders, artifacts, checkpoints.
    let w = fresh_workspace();
    demo::init(&pool, &w, &options).await.unwrap();
    while agents::check_one(&pool, &w, true).await.unwrap() {}
    let c = conversation(&pool, &w).await;
    backhaus_ai_backend::jobs::enqueue(&pool, &w, c, "k", serde_json::json!({"message":"hi"}))
        .await
        .unwrap();
    let first_order: Uuid =
        sqlx::query_scalar("SELECT id FROM purchase_orders WHERE workspace_id=$1 LIMIT 1")
            .bind(&w)
            .fetch_one(&pool)
            .await
            .unwrap();
    backhaus_ai_backend::purchasing::export_pdf(&pool, &cfg, &w, first_order)
        .await
        .ok();
    agents::control(&pool, &w, "sales", "pause").await.unwrap();
    let other = fresh_workspace();
    demo::init(&pool, &other, &options).await.unwrap();
    let result = demo::reset(&pool, &w, &options, false).await.unwrap();
    assert_eq!(result["status"], "reset");
    assert!(result["removed"]["purchase_orders"].as_u64().unwrap() >= 3);
    for table in [
        "purchase_orders",
        "purchase_order_events",
        "agent_runs",
        "conversations",
        "artifacts",
        "scoped_agent_checks",
        "vendor_items",
    ] {
        let before = count(
            &pool,
            &format!("SELECT COUNT(*) FROM {table} WHERE workspace_id=$1"),
            &w,
        )
        .await;
        if table == "vendor_items" {
            assert_eq!(before, 49);
        } else {
            assert_eq!(before, 0, "{table} cleared");
        }
    }
    let state: (bool, i64) = sqlx::query_as(
        "SELECT enabled,checked_revision FROM scoped_agents WHERE workspace_id=$1 AND role='sales'",
    )
    .bind(&w)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        state,
        (false, -1),
        "Finance remains paused with cleared checkpoints"
    );
    // Untouched: the other demo workspace and the snapshot workspace.
    assert_eq!(
        count(
            &pool,
            "SELECT COUNT(*) FROM inventory_items WHERE workspace_id=$1",
            &other
        )
        .await,
        50
    );
    assert_eq!(
        count(
            &pool,
            "SELECT COUNT(*) FROM sales_tickets WHERE workspace_id=$1",
            &cfg.workspace_id
        )
        .await,
        3
    );
    // Re-init after reset rebuilds the same scenario.
    while agents::check_one(&pool, &w, false).await.unwrap() {}
    assert_eq!(common::orders(&pool, &w).await.len(), 3);
}

#[tokio::test]
#[ignore = "Requires dedicated PostgreSQL test database"]
async fn reset_refuses_a_running_runtime_and_queued_reviews_are_coalesced() {
    use backhaus_ai_backend::jobs;
    let (pool, _) = setup().await;
    let w = fresh_workspace();
    let options = demo::Options {
        as_of: as_of(),
        seed: demo::DEFAULT_SEED,
    };
    demo::init(&pool, &w, &options).await.unwrap();
    let mut session = pool.acquire().await.unwrap().detach();
    sqlx::query("SELECT pg_advisory_lock_shared(hashtextextended($1,0))")
        .bind(format!("demo-runtime:{w}"))
        .execute(&mut session)
        .await
        .unwrap();
    assert!(
        demo::reset(&pool, &w, &options, false)
            .await
            .unwrap_err()
            .to_string()
            .contains("Stop the backend")
    );
    drop(session);
    while agents::check_one(&pool, &w, true).await.unwrap() {}
    sqlx::query("UPDATE inventory_items SET current_balance=current_balance-1 WHERE workspace_id=$1 AND id='prov-basmati'").bind(&w).execute(&pool).await.unwrap();
    while agents::check_one(&pool, &w, true).await.unwrap() {}
    let queued: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_runs WHERE workspace_id=$1 AND status='queued'",
    )
    .bind(&w)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(queued, 1);
    let c = conversation(&pool, &w).await;
    jobs::enqueue(
        &pool,
        &w,
        c,
        "chat-priority",
        serde_json::json!({"message":"hi"}),
    )
    .await
    .unwrap();
    assert_eq!(jobs::claim(&pool, &w).await.unwrap().unwrap().kind, "chat");
}
