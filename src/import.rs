use anyhow::{Context, Result, ensure};
use chrono::{Duration, NaiveDate};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use std::{collections::HashSet, str::FromStr};
use uuid::Uuid;

fn array<'a>(v: &'a Value, k: &str) -> Result<&'a Vec<Value>> {
    v[k].as_array()
        .with_context(|| format!("Missing array: {k}"))
}
fn text<'a>(v: &'a Value, k: &str) -> Result<&'a str> {
    v[k].as_str()
        .with_context(|| format!("Missing string: {k}"))
}
fn integer(v: &Value, k: &str) -> Result<i64> {
    v[k].as_i64()
        .with_context(|| format!("Missing integer: {k}"))
}
fn number(v: &Value, k: &str) -> Result<Decimal> {
    Decimal::from_str(&v[k].to_string()).with_context(|| format!("Invalid decimal: {k}"))
}
fn optional_number(v: &Value, k: &str) -> Result<Option<Decimal>> {
    if v[k].is_null() {
        Ok(None)
    } else {
        Ok(Some(number(v, k)?))
    }
}
pub fn business_day(iso: &str) -> Result<NaiveDate> {
    ensure!(
        iso.len() >= 13 && iso.is_ascii(),
        "Invalid source timestamp"
    );
    let date = NaiveDate::parse_from_str(&iso[..10], "%Y-%m-%d")?;
    let hour: u8 = iso[11..13].parse()?;
    ensure!(hour < 24, "Invalid timestamp hour");
    Ok(if hour < 6 {
        date - Duration::days(1)
    } else {
        date
    })
}

pub async fn snapshot(pool: &PgPool, workspace: &str, bytes: &[u8]) -> Result<Value> {
    ensure!(bytes.len() <= 100 * 1024 * 1024, "Snapshot exceeds 100 MiB");
    let s: Value = serde_json::from_slice(bytes)?;
    ensure!(
        s["metadata"]["orgId"].as_str() == Some(workspace),
        "Snapshot workspace does not match WORKSPACE_ID"
    );
    ensure!(s["metadata"]["currency"] == "NGN", "Expected NGN snapshot");
    let from = NaiveDate::parse_from_str(text(&s["metadata"], "from")?, "%Y-%m-%d")?;
    let to = NaiveDate::parse_from_str(text(&s["metadata"], "to")?, "%Y-%m-%d")?;
    ensure!(from <= to, "Invalid snapshot range");
    let inventory = array(&s, "inventory")?;
    let tickets = array(&s, "tickets")?;
    ensure!(
        !tickets.is_empty() && inventory.len() == 50,
        "Expected sales records and 50 inventory items"
    );
    let digest = format!("{:x}", Sha256::digest(bytes));
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1)::bigint)")
        .bind(format!("snapshot:{workspace}"))
        .execute(&mut *tx)
        .await?;
    if let Some(id) = sqlx::query_scalar::<_, Uuid>(
        "SELECT id FROM dataset_imports WHERE workspace_id=$1 AND sha256=$2",
    )
    .bind(workspace)
    .bind(&digest)
    .fetch_optional(&mut *tx)
    .await?
    {
        return Ok(json!({"status":"already_imported","import_id":id}));
    }
    let existing: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM dataset_imports WHERE workspace_id=$1")
            .bind(workspace)
            .fetch_one(&mut *tx)
            .await?;
    ensure!(
        existing == 0,
        "Workspace already contains a different snapshot; use a fresh database to avoid overwriting test work"
    );
    sqlx::query("INSERT INTO workspaces(id,name) VALUES($1,'Backhaus test workspace') ON CONFLICT DO NOTHING").bind(workspace).execute(&mut *tx).await?;
    let mut item_ids = HashSet::new();
    for r in inventory {
        ensure!(
            text(r, "org_id")? == workspace,
            "Inventory workspace mismatch"
        );
        let id = text(r, "id")?;
        ensure!(item_ids.insert(id), "Duplicate inventory ID");
        sqlx::query("INSERT INTO inventory_items(workspace_id,id,name,category,unit,par_level,current_balance,unit_cost,supplier,needs_review,source) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)")
            .bind(workspace).bind(id).bind(text(r,"name")?).bind(r["category"].as_str()).bind(r["unit"].as_str()).bind(optional_number(r,"par_level")?).bind(number(r,"current_balance")?).bind(optional_number(r,"unit_cost")?).bind(r["supplier"].as_str()).bind(r["needs_review"]==true || r["needs_review"]==1).bind(r).execute(&mut *tx).await?;
    }
    for r in array(&s, "inventoryMovements")? {
        ensure!(
            text(r, "org_id")? == workspace && item_ids.contains(text(r, "item_id")?),
            "Invalid movement relationship"
        );
        sqlx::query("INSERT INTO inventory_movements(workspace_id,id,item_id,business_date,movement_type,quantity_delta,source) VALUES($1,$2,$3,$4,$5,$6,$7)")
            .bind(workspace).bind(text(r,"id")?).bind(text(r,"item_id")?).bind(NaiveDate::parse_from_str(text(r,"date")?,"%Y-%m-%d")?).bind(text(r,"type")?).bind(number(r,"quantity_delta")?).bind(r).execute(&mut *tx).await?;
    }
    insert_menu(&mut tx, workspace, &s).await?;
    let menu = array(&s["menu"], "menuItems")?;
    let mut days = HashSet::new();
    let mut lines = 0usize;
    for t in tickets {
        let day = business_day(text(t, "date")?)?;
        ensure!(
            day >= from && day <= to,
            "Ticket outside requested business-date range"
        );
        days.insert(day);
        let ticket_id = integer(t, "id")?;
        sqlx::query("INSERT INTO sales_tickets(workspace_id,id,ticket_number,business_date,total_amount,source) VALUES($1,$2,$3,$4,$5,$6)")
            .bind(workspace).bind(ticket_id).bind(text(t,"ticketNumber")?).bind(day).bind(number(t,"totalAmount")?).bind(t).execute(&mut *tx).await?;
        for o in array(t, "orders")? {
            ensure!(
                integer(o, "ticketId")? == ticket_id,
                "Order/ticket mismatch"
            );
            let category = menu
                .iter()
                .find(|m| m["id"] == o["menuItemId"])
                .and_then(|m| m["groupCode"].as_str());
            sqlx::query("INSERT INTO sales_lines(workspace_id,id,ticket_id,item_name,category,quantity,unit_price,billable,source) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)")
                .bind(workspace).bind(integer(o,"id")?).bind(ticket_id).bind(text(o,"menuItemName")?).bind(category).bind(number(o,"quantity")?).bind(number(o,"price")?).bind(o["calculatePrice"]!=false).bind(o).execute(&mut *tx).await?;
            lines += 1;
        }
    }
    let id = Uuid::new_v4();
    let mut metadata = s["metadata"].clone();
    metadata["sales_ticket_count"] = json!(tickets.len());
    metadata["sales_line_count"] = json!(lines);
    metadata["days_with_records"] = json!(days.len());
    metadata["inventory_sample_count"] = json!(inventory.len());
    metadata["missing_days_are_unknown"] = json!(true);
    sqlx::query("INSERT INTO dataset_imports(id,workspace_id,sha256,metadata) VALUES($1,$2,$3,$4)")
        .bind(id)
        .bind(workspace)
        .bind(digest)
        .bind(metadata)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(
        json!({"status":"imported","import_id":id,"tickets":tickets.len(),"lines":lines,"inventory_items":inventory.len(),"days_with_records":days.len()}),
    )
}

async fn insert_menu(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    workspace: &str,
    snapshot: &Value,
) -> Result<usize> {
    let mut inserted = 0;
    for item in array(&snapshot["menu"], "menuItems")? {
        let id = integer(item, "id")?;
        let name = item["name"]
            .as_str()
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(str::to_owned)
            .unwrap_or_else(|| format!("Menu item {id}"));
        let portions = item["portions"].as_array().cloned().unwrap_or_default();
        let mut prices = Vec::new();
        for portion in &portions {
            if let Some(values) = portion["prices"].as_array() {
                for value in values {
                    if !value["price"].is_null() {
                        prices.push(number(value, "price")?);
                    }
                }
            }
        }
        inserted+=sqlx::query("INSERT INTO menu_items(workspace_id,id,name,category,portions,price_min,price_max,source) VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT(workspace_id,id) DO NOTHING")
            .bind(workspace).bind(id).bind(name).bind(item["groupCode"].as_str()).bind(json!(portions)).bind(prices.iter().min().copied()).bind(prices.iter().max().copied()).bind(item).execute(&mut **tx).await?.rows_affected() as usize;
    }
    Ok(inserted)
}

/// Backfill menu data from the exact snapshot already imported into this workspace.
/// Existing inventory balances/par levels and existing menu edits are preserved.
pub async fn menu_snapshot(pool: &PgPool, workspace: &str, bytes: &[u8]) -> Result<Value> {
    ensure!(bytes.len() <= 100 * 1024 * 1024, "Snapshot exceeds 100 MiB");
    let snapshot: Value = serde_json::from_slice(bytes)?;
    ensure!(
        snapshot["metadata"]["orgId"].as_str() == Some(workspace),
        "Snapshot workspace mismatch"
    );
    let digest = format!("{:x}", Sha256::digest(bytes));
    let mut tx = pool.begin().await?;
    let imported: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM dataset_imports WHERE workspace_id=$1 AND sha256=$2)",
    )
    .bind(workspace)
    .bind(digest)
    .fetch_one(&mut *tx)
    .await?;
    ensure!(
        imported,
        "Menu backfill requires the original imported snapshot"
    );
    let count = insert_menu(&mut tx, workspace, &snapshot).await?;
    tx.commit().await?;
    Ok(json!({"menu_items_inserted":count}))
}
