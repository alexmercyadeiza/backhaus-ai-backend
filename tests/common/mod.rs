#![allow(dead_code, clippy::too_many_arguments)]
use backhaus_ai_backend::{config::Config, data::DateRange, import};
use serde_json::{Value, json};
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::{path::PathBuf, time::Duration};
use uuid::Uuid;

pub fn fixture(workspace: &str) -> Vec<u8> {
    let inventory:Vec<Value>=(0..50).map(|i|json!({"id":format!("item-{i}"),"org_id":workspace,"name":format!("Item {i:02}"),"category":"Kitchen","unit":"kg","par_level":10,"current_balance":if i%2==0 {2}else{20},"unit_cost":0.1,"supplier":null,"needs_review":false})).collect();
    let order = |id, ticket, price, qty, billable| json!({"id":id,"ticketId":ticket,"menuItemId":1,"menuItemName":"Meal","price":price,"quantity":qty,"calculatePrice":billable});
    serde_json::to_vec(&json!({
        "metadata":{"activeInventoryPopulation":464,"sourceInternalNote":"RAW_MANIFEST_SENTINEL","orgId":workspace,"currency":"NGN","from":"2026-07-01","to":"2026-08-31","exportedAt":"2026-09-12T00:00:00Z"},
        "inventory":inventory,"inventoryMovements":[],"menu":{"menuItems":[{"id":1,"groupCode":"Food"}]},
        "tickets":[
            {"id":1,"ticketNumber":"1","date":"2026-07-31T19:00:00Z","totalAmount":30.5,"orders":[order(11,1,10.1,3,true),order(12,1,0.1,2,true),order(13,1,999.0,1,false)]},
            {"id":2,"ticketNumber":"2","date":"2026-08-01T02:00:00Z","totalAmount":5,"orders":[order(21,2,5.0,1,true)]},
            {"id":3,"ticketNumber":"3","date":"2026-08-01T13:00:00Z","totalAmount":0,"orders":[order(31,3,100.0,1,true)]}
        ]
    })).unwrap()
}
pub fn worker_script() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("worker/dist/worker.js")
}
pub fn fake_worker_script() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake-worker.mjs")
}
pub fn config(workspace: &str, url: &str) -> Config {
    Config {
        database_url: url.into(),
        bind: "127.0.0.1:0".parse().unwrap(),
        api_key: "test-secret-that-is-at-least-32-characters".into(),
        login: None,
        resend_key: None,
        resend_from: None,
        cors_origin: "http://localhost:5173".into(),
        workspace_id: workspace.into(),
        db_max_connections: 2,
        model_base_url: None,
        model_name: None,
        model_api_key: String::new(),
        model_request_options: json!({}),
        model_timeout: Duration::from_secs(15),
        worker_poll: Duration::from_secs(1),
        typst_bin: "typst".into(),
        node_bin: "node".into(),
        worker_script: worker_script(),
    }
}
/// Config that runs the protocol test double instead of the Strands worker.
/// The double reads its behaviour from the model name Rust passes to it.
pub fn fake_worker_config(cfg: &Config, mode: &str) -> Config {
    let mut cfg = cfg.clone();
    cfg.model_base_url = Some("http://127.0.0.1:9/v1".into());
    cfg.model_name = Some(format!("fake:{mode}"));
    cfg.worker_script = fake_worker_script();
    cfg
}
pub async fn setup() -> (PgPool, Config) {
    let url = std::env::var("TEST_DATABASE_URL")
        .expect("Set TEST_DATABASE_URL to the dedicated backhaus_ai_test database");
    assert!(
        url.split('?')
            .next()
            .unwrap()
            .ends_with("/backhaus_ai_test"),
        "Tests require the dedicated backhaus_ai_test database"
    );
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .unwrap();
    sqlx::migrate!().run(&pool).await.unwrap();
    let workspace = format!("test-{}", Uuid::new_v4());
    import::snapshot(&pool, &workspace, &fixture(&workspace))
        .await
        .unwrap();
    let cfg = config(&workspace, &url);
    (pool, cfg)
}
pub async fn conversation(pool: &PgPool, workspace: &str) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query("INSERT INTO conversations(id,workspace_id) VALUES($1,$2)")
        .bind(id)
        .bind(workspace)
        .execute(pool)
        .await
        .unwrap();
    id
}
pub fn range() -> DateRange {
    DateRange {
        from: "2026-07-01".parse().unwrap(),
        to: "2026-08-31".parse().unwrap(),
    }
}
pub async fn vendor(pool: &PgPool, workspace: &str, name: &str, contact: bool) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query("INSERT INTO vendors(workspace_id,id,name,email,source) VALUES($1,$2,$3,$4,'local_fixture')")
        .bind(workspace).bind(id).bind(name).bind(contact.then(|| format!("{}@example.invalid", name.to_lowercase().replace(' ', "-"))))
        .execute(pool).await.unwrap();
    id
}
pub async fn assign(
    pool: &PgPool,
    workspace: &str,
    item: &str,
    vendor: Uuid,
    price: Option<&str>,
    units_per_pack: &str,
    minimum: &str,
    target: Option<&str>,
) {
    sqlx::query("INSERT INTO vendor_items(workspace_id,item_id,vendor_id,order_unit,units_per_pack,pack_price,minimum_order_quantity,reorder_target,source) VALUES($1,$2,$3,'pack',$4::numeric,$5::numeric,$6::numeric,$7::numeric,'local_fixture')")
        .bind(workspace).bind(item).bind(vendor).bind(units_per_pack).bind(price).bind(minimum).bind(target)
        .execute(pool).await.unwrap();
}
pub async fn policy(pool: &PgPool, workspace: &str, auto: &str, limit: Option<&str>) {
    sqlx::query("INSERT INTO purchasing_policies(workspace_id,auto_approve_limit,approval_limit) VALUES($1,$2::numeric,$3::numeric) ON CONFLICT(workspace_id) DO UPDATE SET auto_approve_limit=EXCLUDED.auto_approve_limit,approval_limit=EXCLUDED.approval_limit,updated_at=now()")
        .bind(workspace).bind(auto).bind(limit).execute(pool).await.unwrap();
}
pub async fn set_balance(pool: &PgPool, workspace: &str, item: &str, balance: &str) {
    sqlx::query(
        "UPDATE inventory_items SET current_balance=$3::numeric WHERE workspace_id=$1 AND id=$2",
    )
    .bind(workspace)
    .bind(item)
    .bind(balance)
    .execute(pool)
    .await
    .unwrap();
}
pub async fn orders(pool: &PgPool, workspace: &str) -> Vec<Value> {
    sqlx::query_scalar::<_, Value>("SELECT jsonb_build_object('id',o.id,'number',o.number,'vendor',v.name,'status',o.status,'subtotal',o.subtotal::text,'line_count',o.line_count,'version',o.version,'approval_kind',o.approval_kind,'approval_reason',o.approval_reason,'attention',o.attention) FROM purchase_orders o JOIN vendors v ON v.workspace_id=o.workspace_id AND v.id=o.vendor_id WHERE o.workspace_id=$1 ORDER BY v.name,o.created_at,o.id")
        .bind(workspace).fetch_all(pool).await.unwrap()
}
