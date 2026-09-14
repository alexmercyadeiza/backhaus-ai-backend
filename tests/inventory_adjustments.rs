//! Stock adjustments: validation, atomic movement + balance, idempotency,
//! concurrency, and the change-driven check that follows.
mod common;
use backhaus_ai_backend::{
    inventory::{self, MovementKind, MovementRequest},
    scoped_agents as agents,
};
use common::*;
use rust_decimal::Decimal;
use serde_json::json;

fn req(
    kind: MovementKind,
    quantity: &str,
    reason: &str,
    expected: Option<&str>,
) -> MovementRequest {
    MovementRequest {
        kind,
        quantity: quantity.parse().unwrap(),
        reason: reason.into(),
        expected_balance: expected.map(|e| e.parse().unwrap()),
    }
}
async fn balance(pool: &sqlx::PgPool, w: &str, item: &str) -> Decimal {
    sqlx::query_scalar(
        "SELECT current_balance FROM inventory_items WHERE workspace_id=$1 AND id=$2",
    )
    .bind(w)
    .bind(item)
    .fetch_one(pool)
    .await
    .unwrap()
}

#[tokio::test]
#[ignore = "Requires dedicated PostgreSQL test database"]
async fn adjustments_validate_apply_once_and_wake_the_inventory_agent() {
    let (pool, cfg) = setup().await;
    let w = &cfg.workspace_id;
    let alpha = vendor(&pool, w, "Alpha Foods", true).await;
    assign(&pool, w, "item-1", alpha, Some("100"), "5", "1", None).await; // item-1 stocked at 20, par 10
    while agents::check_one(&pool, w, false).await.unwrap() {}
    assert!(orders(&pool, w).await.is_empty());
    // Validation
    assert!(
        inventory::record(
            &pool,
            w,
            "item-1",
            "k1",
            &req(MovementKind::Issue, "0", "x", None)
        )
        .await
        .is_err()
    );
    assert!(
        inventory::record(
            &pool,
            w,
            "item-1",
            "k1",
            &req(MovementKind::Issue, "1", "   ", None)
        )
        .await
        .is_err()
    );
    assert!(
        inventory::record(
            &pool,
            w,
            "item-1",
            "k1",
            &req(MovementKind::Issue, "1.0001", "precision", None)
        )
        .await
        .is_err()
    );
    let negative = inventory::record(
        &pool,
        w,
        "item-1",
        "k1",
        &req(MovementKind::Issue, "25", "too much", None),
    )
    .await
    .err()
    .unwrap()
    .to_string();
    assert!(negative.contains("only 20 on hand"), "{negative}");
    assert!(
        inventory::record(
            &pool,
            "other-workspace",
            "item-1",
            "k1",
            &req(MovementKind::Issue, "1", "scope", None)
        )
        .await
        .is_err()
    );
    assert_eq!(balance(&pool, w, "item-1").await, Decimal::from(20));
    // Apply once: same key + payload returns the stored result; different payload conflicts.
    let first = inventory::record(
        &pool,
        w,
        "item-1",
        "k1",
        &req(MovementKind::Issue, "12", "Kitchen issue", Some("20")),
    )
    .await
    .unwrap();
    assert_eq!(first["item"]["balance"], "8");
    assert_eq!(first["item"]["status"], "Below par");
    assert_eq!(first["reused"], false);
    let again = inventory::record(
        &pool,
        w,
        "item-1",
        "k1",
        &req(MovementKind::Issue, "12", "Kitchen issue", Some("20")),
    )
    .await
    .unwrap();
    assert_eq!(again["reused"], true);
    assert_eq!(again["movement"]["id"], first["movement"]["id"]);
    assert_eq!(balance(&pool, w, "item-1").await, Decimal::from(8));
    assert!(
        inventory::record(
            &pool,
            w,
            "item-1",
            "k1",
            &req(MovementKind::Issue, "1", "different", None)
        )
        .await
        .is_err()
    );
    let stale = inventory::record(
        &pool,
        w,
        "item-1",
        "k2",
        &req(MovementKind::Issue, "1", "stale", Some("20")),
    )
    .await
    .err()
    .unwrap()
    .to_string();
    assert!(stale.contains("changed since"), "{stale}");
    let movements: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM inventory_movements WHERE workspace_id=$1 AND item_id='item-1' AND movement_type='issue'").bind(w).fetch_one(&pool).await.unwrap();
    assert_eq!(movements, 1);
    // The committed change woke the agent: one check drafts 3 packs (short 2 → ceil(2/5)=1? no: par 10 - 8 = 2 units → 1 pack of 5).
    let mut ran = false;
    while agents::check_one(&pool, w, false).await.unwrap() {
        ran = true;
    }
    assert!(ran);
    let o = orders(&pool, w).await;
    assert_eq!(o.len(), 1);
    assert_eq!(o[0]["line_count"], 1);
    assert_eq!(
        o[0]["subtotal"]
            .as_str()
            .unwrap()
            .parse::<Decimal>()
            .unwrap(),
        Decimal::from(100)
    );
    // Concurrent adjustments serialize on the row: both apply, nothing is lost.
    let delivery_a = req(MovementKind::Receipt, "3", "delivery a", None);
    let delivery_b = req(MovementKind::Receipt, "4", "delivery b", None);
    let (a, b) = tokio::join!(
        inventory::record(&pool, w, "item-1", "ka", &delivery_a),
        inventory::record(&pool, w, "item-1", "kb", &delivery_b)
    );
    a.unwrap();
    b.unwrap();
    assert_eq!(balance(&pool, w, "item-1").await, Decimal::from(15));
    // A count replaces the balance; the movement carries the delta and the balance after.
    let counted = inventory::record(
        &pool,
        w,
        "item-1",
        "kc",
        &req(MovementKind::Count, "11", "stock take", None),
    )
    .await
    .unwrap();
    assert_eq!(counted["movement"]["quantity"], "-4");
    assert_eq!(counted["movement"]["balance_after"], "11");
    // Pause, change, resume: one catch-up check reflects the latest state and no duplicate order.
    agents::control(&pool, w, "inventory", "pause")
        .await
        .unwrap();
    inventory::record(
        &pool,
        w,
        "item-1",
        "kd",
        &req(MovementKind::Issue, "8", "while paused", None),
    )
    .await
    .unwrap();
    assert!(
        !agents::check_one(&pool, w, false).await.unwrap() || orders(&pool, w).await.len() == 1
    );
    agents::control(&pool, w, "inventory", "resume")
        .await
        .unwrap();
    while agents::check_one(&pool, w, false).await.unwrap() {}
    let o = orders(&pool, w).await;
    assert_eq!(o.len(), 1, "revised in place, not duplicated: {o:?}");
    assert_eq!(
        o[0]["subtotal"]
            .as_str()
            .unwrap()
            .parse::<Decimal>()
            .unwrap(),
        Decimal::from(200)
    ); // 3 on hand, par 10: short 7 → 2 packs of 5
    assert_eq!(o[0]["version"], 2, "revised in place");
    // Rechecking unchanged data cannot create a duplicate (vendor touch bumps the revision only).
    sqlx::query("UPDATE vendors SET notes='touch' WHERE workspace_id=$1 AND id=$2")
        .bind(w)
        .bind(alpha)
        .execute(&pool)
        .await
        .unwrap();
    while agents::check_one(&pool, w, false).await.unwrap() {}
    assert_eq!(orders(&pool, w).await.len(), 1);
    let _ = json!({});
}
