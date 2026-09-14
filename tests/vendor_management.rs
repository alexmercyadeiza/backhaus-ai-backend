//! Vendor and rule management: validation, stale edits, scoped references,
//! rule-triggered rechecks, threshold boundaries, approved snapshots, rejected drafts.
mod common;
use backhaus_ai_backend::{
    purchasing, scoped_agents as agents,
    vendors::{self, PolicyInput, RuleInput, VendorInput},
};
use common::*;
use rust_decimal::Decimal;
use serde_json::Value;
use uuid::Uuid;

fn vendor_input(name: &str, email: Option<&str>, version: Option<i32>) -> VendorInput {
    VendorInput {
        name: name.into(),
        contact_name: None,
        email: email.map(String::from),
        phone: None,
        notes: None,
        version,
    }
}
fn rule(
    price: Option<&str>,
    upp: &str,
    min: &str,
    target: Option<&str>,
    version: Option<i32>,
) -> RuleInput {
    RuleInput {
        supplier_reference: None,
        order_unit: "pack".into(),
        units_per_pack: upp.parse().unwrap(),
        pack_price: price.map(|p| p.parse().unwrap()),
        minimum_order_quantity: min.parse().unwrap(),
        reorder_target: target.map(|t| t.parse().unwrap()),
        version,
    }
}
async fn drain(pool: &sqlx::PgPool, w: &str) {
    while agents::check_one(pool, w, false).await.unwrap() {}
}
fn d(v: &Value) -> Decimal {
    v.as_str().unwrap().parse().unwrap()
}

#[tokio::test]
#[ignore = "Requires dedicated PostgreSQL test database"]
async fn vendors_and_rules_validate_version_and_resolve_exceptions() {
    let (pool, cfg) = setup().await;
    let w = &cfg.workspace_id;
    assert!(
        vendors::create(&pool, w, &vendor_input("", None, None))
            .await
            .is_err()
    );
    assert!(
        vendors::create(&pool, w, &vendor_input("Bad Mail", Some("nope"), None))
            .await
            .is_err()
    );
    let created = vendors::create(
        &pool,
        w,
        &vendor_input("Alpha Foods", Some("a@example.invalid"), None),
    )
    .await
    .unwrap();
    let id: Uuid = created["id"].as_str().unwrap().parse().unwrap();
    assert_eq!(created["version"], 1);
    assert!(
        vendors::create(&pool, w, &vendor_input("alpha foods", None, None))
            .await
            .is_err(),
        "duplicate name"
    );
    assert!(
        vendors::update(
            &pool,
            w,
            id,
            &vendor_input("Alpha Foods Ltd", None, Some(99))
        )
        .await
        .is_err(),
        "stale version"
    );
    let updated = vendors::update(
        &pool,
        w,
        id,
        &vendor_input("Alpha Foods Ltd", Some("b@example.invalid"), Some(1)),
    )
    .await
    .unwrap();
    assert_eq!(updated["version"], 2);
    assert!(
        vendors::update(
            &pool,
            "other-workspace",
            id,
            &vendor_input("X", None, Some(2))
        )
        .await
        .is_err()
    );
    // Rules: validation and scope.
    assert!(
        vendors::assign(
            &pool,
            w,
            id,
            "item-0",
            &rule(Some("10"), "0", "1", None, None)
        )
        .await
        .is_err()
    );
    assert!(
        vendors::assign(
            &pool,
            w,
            id,
            "item-0",
            &rule(Some("10"), "1", "1.5", None, None)
        )
        .await
        .is_err()
    );
    assert!(
        vendors::assign(
            &pool,
            w,
            id,
            "item-0",
            &rule(Some("-1"), "1", "1", None, None)
        )
        .await
        .is_err()
    );
    assert!(
        vendors::assign(
            &pool,
            w,
            id,
            "item-0",
            &rule(Some("10.123"), "1", "1", None, None)
        )
        .await
        .is_err()
    );
    assert!(
        vendors::assign(
            &pool,
            w,
            id,
            "missing-item",
            &rule(Some("10"), "1", "1", None, None)
        )
        .await
        .is_err()
    );
    assert!(
        vendors::assign(
            &pool,
            w,
            Uuid::new_v4(),
            "item-0",
            &rule(Some("10"), "1", "1", None, None)
        )
        .await
        .is_err()
    );
    drain(&pool, w).await;
    let findings = purchasing::findings_for_model(&pool, w).await.unwrap();
    assert!(
        findings["attention"]
            .as_array()
            .unwrap()
            .iter()
            .any(|a| a["item_id"] == "item-0" && a["reason"] == "no_vendor")
    );
    // Assign without a price: flagged as no_price, never free.
    let unpriced = vendors::assign(&pool, w, id, "item-0", &rule(None, "1", "1", None, None))
        .await
        .unwrap();
    assert!(unpriced["pack_price"].is_null());
    drain(&pool, w).await;
    let findings = purchasing::findings_for_model(&pool, w).await.unwrap();
    assert!(
        findings["attention"]
            .as_array()
            .unwrap()
            .iter()
            .any(|a| a["item_id"] == "item-0" && a["reason"] == "no_price"),
        "{findings}"
    );
    assert!(orders(&pool, w).await.is_empty());
    // Stale rule edit conflicts; correct version resolves the exception into a costed draft.
    assert!(
        vendors::assign(
            &pool,
            w,
            id,
            "item-0",
            &rule(Some("100"), "1", "1", None, Some(0))
        )
        .await
        .is_err()
    );
    let priced = vendors::assign(
        &pool,
        w,
        id,
        "item-0",
        &rule(Some("100"), "4", "1", None, Some(1)),
    )
    .await
    .unwrap();
    assert_eq!(priced["version"], 2);
    drain(&pool, w).await;
    let o = orders(&pool, w).await;
    assert_eq!(o.len(), 1);
    assert_eq!(o[0]["line_count"], 1);
    assert_eq!(
        d(&o[0]["subtotal"]),
        Decimal::from(200),
        "short 8 of 4-unit packs → 2 packs × 100"
    );
    // Unassigned list excludes the item now; removing the rule brings it back and withdraws the draft.
    let unassigned = vendors::unassigned_items(&pool, w).await.unwrap();
    assert!(
        !unassigned["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|i| i["id"] == "item-0")
    );
    vendors::unassign(&pool, w, id, "item-0").await.unwrap();
    drain(&pool, w).await;
    assert_eq!(orders(&pool, w).await[0]["status"], "withdrawn");
    let audit: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM purchasing_config_events WHERE workspace_id=$1")
            .bind(w)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(audit >= 5, "config changes are audited: {audit}");
}

#[tokio::test]
#[ignore = "Requires dedicated PostgreSQL test database"]
async fn policy_thresholds_are_exact_and_versioned() {
    let (pool, cfg) = setup().await;
    let w = &cfg.workspace_id;
    let v = vendor(&pool, w, "Alpha Foods", true).await;
    assign(&pool, w, "item-0", v, Some("100"), "1", "1", None).await; // 8 short → 800
    assert!(
        vendors::update_policy(
            &pool,
            w,
            &PolicyInput {
                auto_approve_limit: "-1".parse().unwrap(),
                approval_limit: None,
                version: 0
            }
        )
        .await
        .is_err()
    );
    assert!(
        vendors::update_policy(
            &pool,
            w,
            &PolicyInput {
                auto_approve_limit: "1000".parse().unwrap(),
                approval_limit: Some("500".parse().unwrap()),
                version: 0
            }
        )
        .await
        .is_err(),
        "approval limit below automatic limit"
    );
    assert!(
        vendors::update_policy(
            &pool,
            w,
            &PolicyInput {
                auto_approve_limit: "1".parse().unwrap(),
                approval_limit: None,
                version: 3
            }
        )
        .await
        .is_err(),
        "no policy yet: version must be 0"
    );
    let saved = vendors::update_policy(
        &pool,
        w,
        &PolicyInput {
            auto_approve_limit: "0".parse().unwrap(),
            approval_limit: Some("5000".parse().unwrap()),
            version: 0,
        },
    )
    .await
    .unwrap();
    assert_eq!(saved["version"], 1);
    drain(&pool, w).await;
    let o = orders(&pool, w).await;
    assert_eq!(
        o[0]["status"], "draft",
        "all orders wait for a person: {o:?}"
    );
    assert!(o[0]["approval_kind"].is_null());
    purchasing::decide(
        &pool,
        w,
        o[0]["id"].as_str().unwrap().parse().unwrap(),
        "approve",
        "tester",
        None,
    )
    .await
    .unwrap();
    // Lower the automatic limit by one kobo: a new shortage at the same vendor needs manual approval.
    assert!(
        vendors::update_policy(
            &pool,
            w,
            &PolicyInput {
                auto_approve_limit: "0".parse().unwrap(),
                approval_limit: Some("5000".parse().unwrap()),
                version: 0
            }
        )
        .await
        .is_err(),
        "stale version"
    );
    vendors::update_policy(
        &pool,
        w,
        &PolicyInput {
            auto_approve_limit: "0".parse().unwrap(),
            approval_limit: Some("5000".parse().unwrap()),
            version: 1,
        },
    )
    .await
    .unwrap();
    assign(&pool, w, "item-2", v, Some("100"), "1", "1", None).await;
    drain(&pool, w).await;
    let o = orders(&pool, w).await;
    let draft = o
        .iter()
        .find(|x| x["status"] == "draft")
        .expect("new draft");
    assert_eq!(draft["approval_reason"], "manual_approval_required");
    assert_eq!(d(&draft["subtotal"]), Decimal::from(800));
    // Approval limit boundary: exactly at the limit approves; one kobo above is refused.
    let id: Uuid = draft["id"].as_str().unwrap().parse().unwrap();
    vendors::update_policy(
        &pool,
        w,
        &PolicyInput {
            auto_approve_limit: "0".parse().unwrap(),
            approval_limit: Some("799.99".parse().unwrap()),
            version: 2,
        },
    )
    .await
    .unwrap();
    assert!(
        purchasing::decide(&pool, w, id, "approve", "tester", None)
            .await
            .is_err()
    );
    vendors::update_policy(
        &pool,
        w,
        &PolicyInput {
            auto_approve_limit: "0".parse().unwrap(),
            approval_limit: Some("800".parse().unwrap()),
            version: 3,
        },
    )
    .await
    .unwrap();
    assert_eq!(
        purchasing::decide(&pool, w, id, "approve", "tester", None)
            .await
            .unwrap()["status"],
        "approved"
    );
    // Within the agent limit: the agent approves the next order itself.
    let raised = vendors::update_policy(
        &pool,
        w,
        &PolicyInput {
            auto_approve_limit: "5000".parse().unwrap(),
            approval_limit: None,
            version: 4,
        },
    )
    .await
    .unwrap();
    assert_eq!(raised["auto_approve_limit"], "5000");
    assign(&pool, w, "item-4", v, Some("100"), "1", "1", None).await;
    drain(&pool, w).await;
    let auto = orders(&pool, w)
        .await
        .into_iter()
        .find(|x| x["approval_kind"] == "automatic")
        .expect("an order approved by the agent");
    assert_eq!(auto["status"], "approved");
    assert_eq!(auto["approval_reason"], "within_auto_approval_limit");
    vendors::update_policy(
        &pool,
        w,
        &PolicyInput {
            auto_approve_limit: "0".parse().unwrap(),
            approval_limit: None,
            version: 5,
        },
    )
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "Requires dedicated PostgreSQL test database"]
async fn approved_orders_keep_their_snapshot_and_rejected_drafts_are_not_recreated_unchanged() {
    let (pool, cfg) = setup().await;
    let w = &cfg.workspace_id;
    let v = vendor(&pool, w, "Alpha Foods", true).await;
    assign(&pool, w, "item-0", v, Some("100"), "1", "1", None).await;
    drain(&pool, w).await;
    let o = orders(&pool, w).await;
    let id: Uuid = o[0]["id"].as_str().unwrap().parse().unwrap();
    let approved = purchasing::decide(&pool, w, id, "approve", "tester", None)
        .await
        .unwrap();
    assert_eq!(approved["vendor"], "Alpha Foods");
    assert_eq!(
        approved["vendor_contact"]["email"],
        "alpha-foods@example.invalid"
    );
    // Edit the vendor and its price afterwards.
    let version: i32 =
        sqlx::query_scalar("SELECT version FROM vendors WHERE workspace_id=$1 AND id=$2")
            .bind(w)
            .bind(v)
            .fetch_one(&pool)
            .await
            .unwrap();
    vendors::update(
        &pool,
        w,
        v,
        &vendor_input(
            "Alpha Foods Renamed",
            Some("new@example.invalid"),
            Some(version),
        ),
    )
    .await
    .unwrap();
    let rule_version: i32 = sqlx::query_scalar("SELECT version FROM vendor_items WHERE workspace_id=$1 AND vendor_id=$2 AND item_id='item-0'").bind(w).bind(v).fetch_one(&pool).await.unwrap();
    vendors::assign(
        &pool,
        w,
        v,
        "item-0",
        &rule(Some("999"), "1", "1", None, Some(rule_version)),
    )
    .await
    .unwrap();
    drain(&pool, w).await;
    let detail = purchasing::order_detail(&pool, w, id, 1, 20).await.unwrap();
    assert_eq!(
        detail["record"]["vendor"], "Alpha Foods",
        "approved snapshot keeps the original vendor name"
    );
    assert_eq!(
        detail["record"]["vendor_contact"]["email"],
        "alpha-foods@example.invalid"
    );
    assert_eq!(
        detail["record"]["vendor_current_name"],
        "Alpha Foods Renamed"
    );
    assert_eq!(
        d(&detail["items"][0]["pack_price"]),
        Decimal::from(100),
        "approved lines keep their price"
    );
    assert_eq!(d(&detail["record"]["subtotal"]), Decimal::from(800));
    let pdf = purchasing::export_pdf(&pool, &cfg, w, id).await.unwrap();
    let again = purchasing::export_pdf(&pool, &cfg, w, id).await.unwrap();
    assert_eq!(
        pdf["artifact_id"], again["artifact_id"],
        "one PDF per order version"
    );
    assert_eq!(again["reused"], true);
    let bytes: Vec<u8> =
        sqlx::query_scalar("SELECT bytes FROM artifacts WHERE id=$1 AND workspace_id=$2")
            .bind(
                pdf["artifact_id"]
                    .as_str()
                    .unwrap()
                    .parse::<Uuid>()
                    .unwrap(),
            )
            .bind(w)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(bytes.starts_with(b"%PDF"));
    assert!(
        purchasing::export_pdf(&pool, &cfg, "other-workspace", id)
            .await
            .is_err()
    );
    // A rejected draft is not re-created while nothing changes; a changed situation is a new proposal.
    assign(&pool, w, "item-2", v, Some("50"), "1", "1", None).await; // 8 short → 400 (new draft since item-0 is on order)
    drain(&pool, w).await;
    let draft = orders(&pool, w)
        .await
        .into_iter()
        .find(|x| x["status"] == "draft")
        .expect("draft for item-2");
    let draft_id: Uuid = draft["id"].as_str().unwrap().parse().unwrap();
    purchasing::decide(&pool, w, draft_id, "reject", "tester", Some("not now"))
        .await
        .unwrap();
    sqlx::query("UPDATE vendors SET notes='touch' WHERE workspace_id=$1 AND id=$2")
        .bind(w)
        .bind(v)
        .execute(&pool)
        .await
        .unwrap();
    drain(&pool, w).await;
    let all = orders(&pool, w).await;
    assert!(
        all.iter().all(|x| x["status"] != "draft"),
        "rejected proposal not recreated: {all:?}"
    );
    let findings = purchasing::findings_for_model(&pool, w).await.unwrap();
    assert!(
        findings["attention"]
            .as_array()
            .unwrap()
            .iter()
            .any(|a| a["reason"] == "rejected_unchanged"),
        "{findings}"
    );
    set_balance(&pool, w, "item-2", "1").await; // now 9 short: a different proposal
    drain(&pool, w).await;
    let new_draft = orders(&pool, w)
        .await
        .into_iter()
        .find(|x| x["status"] == "draft")
        .expect("new proposal after change");
    assert_eq!(d(&new_draft["subtotal"]), Decimal::from(450));
    assert_ne!(new_draft["id"], draft["id"]);
}

#[tokio::test]
#[ignore = "Requires dedicated PostgreSQL test database"]
async fn preferred_vendor_can_switch_back_and_rules_cannot_target_below_par() {
    let (pool, cfg) = setup().await;
    let w = &cfg.workspace_id;
    let a = vendor(&pool, w, "A", true).await;
    let b = vendor(&pool, w, "B", true).await;
    vendors::assign(
        &pool,
        w,
        a,
        "item-0",
        &rule(Some("10"), "1", "1", None, None),
    )
    .await
    .unwrap();
    vendors::assign(
        &pool,
        w,
        b,
        "item-0",
        &rule(Some("12"), "1", "1", None, None),
    )
    .await
    .unwrap();
    let v: i32 = sqlx::query_scalar("SELECT version FROM vendor_items WHERE workspace_id=$1 AND vendor_id=$2 AND item_id='item-0'").bind(w).bind(a).fetch_one(&pool).await.unwrap();
    vendors::assign(
        &pool,
        w,
        a,
        "item-0",
        &rule(Some("10"), "1", "1", None, Some(v)),
    )
    .await
    .unwrap();
    let preferred: Vec<Uuid> = sqlx::query_scalar("SELECT vendor_id FROM vendor_items WHERE workspace_id=$1 AND item_id='item-0' AND preferred").bind(w).fetch_all(&pool).await.unwrap();
    assert_eq!(preferred, vec![a]);
    assert!(
        vendors::assign(
            &pool,
            w,
            a,
            "item-0",
            &rule(Some("10"), "1", "1", Some("2"), Some(v + 1))
        )
        .await
        .is_err()
    );
}

#[tokio::test]
#[ignore = "Requires dedicated PostgreSQL test database"]
async fn same_pack_count_still_refreshes_stock_details_and_stale_approval_is_refused() {
    let (pool, cfg) = setup().await;
    let w = &cfg.workspace_id;
    let v = vendor(&pool, w, "Alpha", true).await;
    assign(&pool, w, "item-0", v, Some("100"), "10", "1", None).await;
    drain(&pool, w).await;
    let id: Uuid = orders(&pool, w).await[0]["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let before = purchasing::order_detail(&pool, w, id, 1, 20).await.unwrap();
    set_balance(&pool, w, "item-0", "1").await;
    drain(&pool, w).await;
    let after = purchasing::order_detail(&pool, w, id, 1, 20).await.unwrap();
    assert_eq!(d(&after["items"][0]["current_balance"]), Decimal::ONE);
    assert_eq!(before["record"]["subtotal"], after["record"]["subtotal"]);
    assert!(
        purchasing::decide_versioned(&pool, w, id, "approve", "test", None, Some(1))
            .await
            .is_err()
    );
    purchasing::decide_versioned(&pool, w, id, "approve", "test", None, Some(2))
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "Requires dedicated PostgreSQL test database"]
async fn sales_comparison_uses_exact_values_and_refuses_missing_coverage() {
    use backhaus_ai_backend::data::{self, DateRange};
    let (pool, cfg) = setup().await;
    let range = |day: &str| DateRange {
        from: day.parse().unwrap(),
        to: day.parse().unwrap(),
    };
    let result = data::sales_comparison(
        &pool,
        &cfg.workspace_id,
        &range("2026-08-01"),
        &range("2026-07-31"),
    )
    .await
    .unwrap();
    assert_eq!(result["comparable"], true);
    assert_eq!(
        d(&result["periods"]["previous"]["gross_line_sales"]),
        Decimal::new(355, 1)
    );
    assert_eq!(d(&result["change"]), -Decimal::new(355, 1));
    assert_eq!(d(&result["change_percent"]), Decimal::from(-100));
    let missing = data::sales_comparison(
        &pool,
        &cfg.workspace_id,
        &range("2026-09-01"),
        &range("2026-07-31"),
    )
    .await
    .unwrap();
    assert_eq!(missing["comparable"], false);
    assert!(missing["change"].is_null());
    assert!(missing["periods"]["current"]["gross_line_sales"].is_null());
}
