//! Stock adjustments recorded through the dashboard: validated decimals,
//! atomic movement + balance writes, row locks, and idempotent requests.
use crate::error::{Error, Result};
use chrono::{Duration, NaiveDate, Timelike};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub enum MovementKind {
    /// Stock leaves the store (kitchen issue, sale of stock, waste).
    Issue,
    /// Stock arrives.
    Receipt,
    /// A physical count replaces the balance.
    Count,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MovementRequest {
    #[serde(rename = "type")]
    pub kind: MovementKind,
    pub quantity: Decimal,
    pub reason: String,
    /// Optional optimistic check: the balance the operator saw.
    pub expected_balance: Option<Decimal>,
}

/// Lagos business date: the sales day rolls over at 06:00 local time (UTC+1).
pub fn business_date_now() -> NaiveDate {
    let lagos = chrono::Utc::now() + Duration::hours(1);
    if lagos.time().hour() < 6 {
        lagos.date_naive() - Duration::days(1)
    } else {
        lagos.date_naive()
    }
}

fn validate(request: &MovementRequest) -> Result<()> {
    let reason = request.reason.trim();
    if reason.is_empty() || reason.chars().count() > 200 {
        return Err(Error::Invalid(
            "Give a reason of 1 to 200 characters".into(),
        ));
    }
    if request.quantity.scale() > 3 {
        return Err(Error::Invalid(
            "Quantities use at most 3 decimal places".into(),
        ));
    }
    if request.quantity > Decimal::from(1_000_000) {
        return Err(Error::Invalid("Quantity must be at most 1,000,000".into()));
    }
    match request.kind {
        MovementKind::Count => {
            if request.quantity < Decimal::ZERO {
                return Err(Error::Invalid(
                    "A counted balance cannot be negative".into(),
                ));
            }
        }
        _ => {
            if request.quantity <= Decimal::ZERO {
                return Err(Error::Invalid("Quantity must be greater than zero".into()));
            }
        }
    }
    Ok(())
}

fn request_hash(item: &str, request: &MovementRequest) -> String {
    let body = json!({"item":item,"type":match request.kind { MovementKind::Issue=>"issue", MovementKind::Receipt=>"receipt", MovementKind::Count=>"count" },
        "quantity":request.quantity.normalize().to_string(),"reason":request.reason.trim(),"expected_balance":request.expected_balance.map(|d|d.normalize().to_string())});
    format!("{:x}", Sha256::digest(body.to_string().as_bytes()))
}

/// Apply one movement. The same idempotency key with the same payload returns
/// the original result; a different payload under the same key conflicts.
pub async fn record(
    pool: &PgPool,
    workspace: &str,
    item: &str,
    key: &str,
    request: &MovementRequest,
) -> Result<Value> {
    if key.is_empty() || key.len() > 128 {
        return Err(Error::Invalid(
            "Idempotency-Key must be 1..128 characters".into(),
        ));
    }
    validate(request)?;
    let hash = request_hash(item, request);
    let mut tx = pool.begin().await?;
    // The key spans items; serialize it before item locking so competing uses
    // on different items return a conflict rather than a unique-index error.
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!("movement:{workspace}:{key}"))
        .execute(&mut *tx)
        .await?;
    // Lock the item first so concurrent adjustments serialize on the row.
    let row = sqlx::query("SELECT name,unit,current_balance,par_level FROM inventory_items WHERE workspace_id=$1 AND id=$2 FOR UPDATE")
        .bind(workspace).bind(item).fetch_optional(&mut *tx).await?.ok_or(Error::NotFound)?;
    if let Some(existing) = sqlx::query("SELECT request_hash,response FROM inventory_movement_requests WHERE workspace_id=$1 AND request_key=$2")
        .bind(workspace).bind(key).fetch_optional(&mut *tx).await?
    {
        let stored: String = existing.try_get("request_hash")?;
        if stored != hash {
            return Err(Error::Conflict("Idempotency-Key already belongs to a different adjustment".into()));
        }
        let mut response: Value = existing.try_get("response")?;
        response["reused"] = json!(true);
        return Ok(response);
    }
    let name: String = row.try_get("name")?;
    let unit: Option<String> = row.try_get("unit")?;
    let current: Decimal = row.try_get("current_balance")?;
    let par: Option<Decimal> = row.try_get("par_level")?;
    if let Some(expected) = request.expected_balance
        && expected != current
    {
        return Err(Error::Conflict(format!(
            "Stock changed since you loaded it: on hand is now {}",
            current.normalize()
        )));
    }
    let (delta, movement_type) = match request.kind {
        MovementKind::Issue => (-request.quantity, "issue"),
        MovementKind::Receipt => (request.quantity, "received"),
        MovementKind::Count => (request.quantity - current, "count"),
    };
    let balance = current + delta;
    if balance < Decimal::ZERO {
        return Err(Error::Invalid(format!(
            "Cannot issue {} {}: only {} on hand",
            request.quantity.normalize(),
            unit.as_deref().unwrap_or("units"),
            current.normalize()
        )));
    }
    let movement_id = format!("mv-{}", Uuid::new_v4());
    let date = business_date_now();
    sqlx::query("INSERT INTO inventory_movements(workspace_id,id,item_id,business_date,movement_type,quantity_delta,source,reason,balance_after) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)")
        .bind(workspace).bind(&movement_id).bind(item).bind(date).bind(movement_type).bind(delta)
        .bind(json!({"origin":"dashboard","request_key":key})).bind(request.reason.trim()).bind(balance)
        .execute(&mut *tx).await?;
    sqlx::query("UPDATE inventory_items SET current_balance=$3 WHERE workspace_id=$1 AND id=$2")
        .bind(workspace)
        .bind(item)
        .bind(balance)
        .execute(&mut *tx)
        .await?;
    let status = match par {
        Some(p) if p > Decimal::ZERO && balance < p => "Below par",
        Some(p) if p > Decimal::ZERO => "In stock",
        _ => "Par not set",
    };
    let response = json!({
        "movement":{"id":movement_id,"type":movement_type,"date":date,"quantity":delta.normalize().to_string(),"reason":request.reason.trim(),"balance_after":balance.normalize().to_string()},
        "item":{"id":item,"name":name,"unit":unit,"balance":balance.normalize().to_string(),"previous_balance":current.normalize().to_string(),"par_level":par.map(|p|p.normalize().to_string()),"status":status},
        "reused":false
    });
    sqlx::query("INSERT INTO inventory_movement_requests(workspace_id,request_key,request_hash,movement_id,response) VALUES($1,$2,$3,$4,$5)")
        .bind(workspace).bind(key).bind(&hash).bind(&movement_id).bind(&response).execute(&mut *tx).await?;
    tx.commit().await?;
    tracing::info!(item, movement_type, "Stock adjustment recorded");
    Ok(response)
}
