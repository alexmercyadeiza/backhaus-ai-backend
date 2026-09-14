//! Supplier records, item ordering rules and the purchasing policy: validated,
//! workspace-scoped, version-aware (stale edits conflict) and audited.
use crate::{
    error::{Error, Result},
    purchasing::ACTOR_DASHBOARD,
};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::{Value, json};
use sqlx::{PgPool, Postgres, Row, Transaction};
use uuid::Uuid;

const PAGE_SIZE: i64 = 20;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VendorInput {
    pub name: String,
    #[serde(default)]
    pub contact_name: Option<String>,
    #[serde(default)]
    pub email: Option<String>,
    #[serde(default)]
    pub phone: Option<String>,
    #[serde(default)]
    pub notes: Option<String>,
    /// Required when updating; must match the stored version.
    #[serde(default)]
    pub version: Option<i32>,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuleInput {
    #[serde(default)]
    pub supplier_reference: Option<String>,
    pub order_unit: String,
    pub units_per_pack: Decimal,
    /// `null` keeps the price unknown; the item is then flagged, never treated as free.
    #[serde(default)]
    pub pack_price: Option<Decimal>,
    pub minimum_order_quantity: Decimal,
    #[serde(default)]
    pub reorder_target: Option<Decimal>,
    /// 0 or omitted creates the rule; otherwise it must match the stored version.
    #[serde(default)]
    pub version: Option<i32>,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyInput {
    pub auto_approve_limit: Decimal,
    #[serde(default)]
    pub approval_limit: Option<Decimal>,
    pub version: i32,
}

fn text(value: Option<&String>, field: &str, max: usize) -> Result<Option<String>> {
    match value.map(|v| v.trim()) {
        None | Some("") => Ok(None),
        Some(v) if v.chars().count() > max => Err(Error::Invalid(format!(
            "{field} must be at most {max} characters"
        ))),
        Some(v) => Ok(Some(v.to_owned())),
    }
}
struct VendorFields {
    name: String,
    contact: Option<String>,
    email: Option<String>,
    phone: Option<String>,
    notes: Option<String>,
}
fn validate_vendor(input: &VendorInput) -> Result<VendorFields> {
    let name = input.name.trim();
    if name.is_empty() || name.chars().count() > 200 {
        return Err(Error::Invalid(
            "Vendor name must be 1 to 200 characters".into(),
        ));
    }
    let email = text(input.email.as_ref(), "Email", 200)?;
    if let Some(e) = &email
        && (!e.contains('@') || e.contains(char::is_whitespace))
    {
        return Err(Error::Invalid("Email must look like an address".into()));
    }
    Ok(VendorFields {
        name: name.to_owned(),
        contact: text(input.contact_name.as_ref(), "Contact name", 120)?,
        email,
        phone: text(input.phone.as_ref(), "Phone", 50)?,
        notes: text(input.notes.as_ref(), "Notes", 1000)?,
    })
}
fn validate_rule(input: &RuleInput) -> Result<()> {
    let unit = input.order_unit.trim();
    if unit.is_empty() || unit.chars().count() > 60 {
        return Err(Error::Invalid(
            "Order unit must be 1 to 60 characters".into(),
        ));
    }
    if input.units_per_pack <= Decimal::ZERO
        || input.units_per_pack > Decimal::from(1_000_000)
        || input.units_per_pack.scale() > 3
    {
        return Err(Error::Invalid(
            "Units per pack must be positive with at most 3 decimals".into(),
        ));
    }
    if let Some(price) = input.pack_price
        && (price < Decimal::ZERO || price > Decimal::from(1_000_000_000) || price.scale() > 2)
    {
        return Err(Error::Invalid(
            "Price per pack must be zero or more with at most 2 decimals".into(),
        ));
    }
    if input.minimum_order_quantity <= Decimal::ZERO
        || input.minimum_order_quantity.scale() > 0
        || input.minimum_order_quantity > Decimal::from(100_000)
    {
        return Err(Error::Invalid(
            "Minimum order quantity must be a whole number of packs".into(),
        ));
    }
    if let Some(target) = input.reorder_target
        && (target <= Decimal::ZERO || target.scale() > 3 || target > Decimal::from(1_000_000))
    {
        return Err(Error::Invalid(
            "Reorder target must be positive with at most 3 decimals".into(),
        ));
    }
    if let Some(reference) = &input.supplier_reference
        && reference.chars().count() > 80
    {
        return Err(Error::Invalid(
            "Supplier reference must be at most 80 characters".into(),
        ));
    }
    Ok(())
}

async fn audit(
    tx: &mut Transaction<'_, Postgres>,
    workspace: &str,
    entity: &str,
    entity_id: &str,
    action: &str,
    payload: Value,
) -> Result<()> {
    sqlx::query("INSERT INTO purchasing_config_events(workspace_id,entity,entity_id,action,actor,payload) VALUES($1,$2,$3,$4,$5,$6)")
        .bind(workspace).bind(entity).bind(entity_id).bind(action).bind(ACTOR_DASHBOARD).bind(payload).execute(&mut **tx).await?;
    Ok(())
}

pub async fn vendor_json(
    tx: &mut Transaction<'_, Postgres>,
    workspace: &str,
    id: Uuid,
) -> Result<Value> {
    sqlx::query_scalar::<_, Value>("SELECT jsonb_build_object('id',v.id,'name',v.name,'contact_name',v.contact_name,'email',v.email,'phone',v.phone,'notes',v.notes,'source',v.source,'version',v.version,'created_at',v.created_at,'updated_at',v.updated_at,'item_count',(SELECT COUNT(*) FROM vendor_items vi WHERE vi.workspace_id=v.workspace_id AND vi.vendor_id=v.id),'open_orders',(SELECT COUNT(*) FROM purchase_orders o WHERE o.workspace_id=v.workspace_id AND o.vendor_id=v.id AND o.status IN ('draft','approved'))) FROM vendors v WHERE v.workspace_id=$1 AND v.id=$2")
        .bind(workspace).bind(id).fetch_optional(&mut **tx).await?.ok_or(Error::NotFound)
}

pub async fn create(pool: &PgPool, workspace: &str, input: &VendorInput) -> Result<Value> {
    let VendorFields {
        name,
        contact,
        email,
        phone,
        notes,
    } = validate_vendor(input)?;
    let mut tx = pool.begin().await?;
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM vendors WHERE workspace_id=$1 AND lower(name)=lower($2))",
    )
    .bind(workspace)
    .bind(&name)
    .fetch_one(&mut *tx)
    .await?;
    if exists {
        return Err(Error::Conflict(
            "A vendor with this name already exists".into(),
        ));
    }
    let id = Uuid::new_v4();
    sqlx::query("INSERT INTO vendors(workspace_id,id,name,contact_name,email,phone,notes,source) VALUES($1,$2,$3,$4,$5,$6,$7,'manual')")
        .bind(workspace).bind(id).bind(&name).bind(&contact).bind(&email).bind(&phone).bind(&notes).execute(&mut *tx).await?;
    audit(
        &mut tx,
        workspace,
        "vendor",
        &id.to_string(),
        "created",
        json!({"name":name,"email":email,"phone":phone}),
    )
    .await?;
    let value = vendor_json(&mut tx, workspace, id).await?;
    tx.commit().await?;
    Ok(value)
}

pub async fn update(
    pool: &PgPool,
    workspace: &str,
    id: Uuid,
    input: &VendorInput,
) -> Result<Value> {
    let VendorFields {
        name,
        contact,
        email,
        phone,
        notes,
    } = validate_vendor(input)?;
    let version = input
        .version
        .ok_or_else(|| Error::Invalid("version is required when editing".into()))?;
    let mut tx = pool.begin().await?;
    let current: i32 = sqlx::query_scalar(
        "SELECT version FROM vendors WHERE workspace_id=$1 AND id=$2 FOR UPDATE",
    )
    .bind(workspace)
    .bind(id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(Error::NotFound)?;
    if current != version {
        return Err(Error::Conflict(
            "This vendor changed since you opened it; reload and try again".into(),
        ));
    }
    let clash: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM vendors WHERE workspace_id=$1 AND lower(name)=lower($2) AND id<>$3)")
        .bind(workspace).bind(&name).bind(id).fetch_one(&mut *tx).await?;
    if clash {
        return Err(Error::Conflict(
            "A vendor with this name already exists".into(),
        ));
    }
    sqlx::query("UPDATE vendors SET name=$3,contact_name=$4,email=$5,phone=$6,notes=$7,version=version+1,updated_at=now() WHERE workspace_id=$1 AND id=$2")
        .bind(workspace).bind(id).bind(&name).bind(&contact).bind(&email).bind(&phone).bind(&notes).execute(&mut *tx).await?;
    audit(
        &mut tx,
        workspace,
        "vendor",
        &id.to_string(),
        "updated",
        json!({"name":name,"email":email,"phone":phone,"version":version+1}),
    )
    .await?;
    let value = vendor_json(&mut tx, workspace, id).await?;
    tx.commit().await?;
    Ok(value)
}

pub async fn rule_json(
    tx: &mut Transaction<'_, Postgres>,
    workspace: &str,
    vendor: Uuid,
    item: &str,
) -> Result<Value> {
    sqlx::query_scalar::<_, Value>("SELECT jsonb_build_object('item_id',vi.item_id,'name',i.name,'unit',i.unit,'category',i.category,'current_balance',i.current_balance::text,'par_level',i.par_level::text,'vendor_id',vi.vendor_id,'supplier_reference',vi.supplier_reference,'order_unit',vi.order_unit,'units_per_pack',vi.units_per_pack::text,'pack_price',vi.pack_price::text,'minimum_order_quantity',vi.minimum_order_quantity::text,'reorder_target',vi.reorder_target::text,'preferred',vi.preferred,'version',vi.version,'updated_at',vi.updated_at) FROM vendor_items vi JOIN inventory_items i ON i.workspace_id=vi.workspace_id AND i.id=vi.item_id WHERE vi.workspace_id=$1 AND vi.vendor_id=$2 AND vi.item_id=$3")
        .bind(workspace).bind(vendor).bind(item).fetch_optional(&mut **tx).await?.ok_or(Error::NotFound)
}

/// Create or update the ordering rule for an item at a vendor and make that
/// vendor the item's preferred supplier.
pub async fn assign(
    pool: &PgPool,
    workspace: &str,
    vendor: Uuid,
    item: &str,
    input: &RuleInput,
) -> Result<Value> {
    validate_rule(input)?;
    let mut tx = pool.begin().await?;
    let vendor_exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM vendors WHERE workspace_id=$1 AND id=$2)")
            .bind(workspace)
            .bind(vendor)
            .fetch_one(&mut *tx)
            .await?;
    let item_par: Option<Option<Decimal>> = sqlx::query_scalar(
        "SELECT par_level FROM inventory_items WHERE workspace_id=$1 AND id=$2 FOR UPDATE",
    )
    .bind(workspace)
    .bind(item)
    .fetch_optional(&mut *tx)
    .await?;
    if !vendor_exists || item_par.is_none() {
        return Err(Error::NotFound);
    }
    if let (Some(target), Some(par)) = (input.reorder_target, item_par.flatten())
        && target < par
    {
        return Err(Error::Invalid(
            "Reorder target cannot be below the item's par level".into(),
        ));
    }
    let existing: Option<i32> = sqlx::query_scalar("SELECT version FROM vendor_items WHERE workspace_id=$1 AND vendor_id=$2 AND item_id=$3 FOR UPDATE")
        .bind(workspace).bind(vendor).bind(item).fetch_optional(&mut *tx).await?;
    let reference = text(input.supplier_reference.as_ref(), "Supplier reference", 80)?;
    let action = match existing {
        Some(current) => {
            if input.version.unwrap_or(0) != current {
                return Err(Error::Conflict(
                    "This rule changed since you opened it; reload and try again".into(),
                ));
            }
            // Release the other preferred slot before setting this rule true.
            sqlx::query("UPDATE vendor_items SET preferred=false,version=version+1,updated_at=now() WHERE workspace_id=$1 AND item_id=$2 AND vendor_id<>$3 AND preferred")
                .bind(workspace).bind(item).bind(vendor).execute(&mut *tx).await?;
            sqlx::query("UPDATE vendor_items SET supplier_reference=$4,order_unit=$5,units_per_pack=$6,pack_price=$7,minimum_order_quantity=$8,reorder_target=$9,preferred=true,version=version+1,updated_at=now() WHERE workspace_id=$1 AND vendor_id=$2 AND item_id=$3")
                .bind(workspace).bind(vendor).bind(item).bind(&reference).bind(input.order_unit.trim()).bind(input.units_per_pack).bind(input.pack_price).bind(input.minimum_order_quantity).bind(input.reorder_target)
                .execute(&mut *tx).await?;
            "updated"
        }
        None => {
            if input.version.unwrap_or(0) != 0 {
                return Err(Error::Conflict(
                    "This rule no longer exists; reload and try again".into(),
                ));
            }
            // Another vendor may hold the preferred slot; this assignment takes it over.
            sqlx::query("UPDATE vendor_items SET preferred=false,version=version+1,updated_at=now() WHERE workspace_id=$1 AND item_id=$2 AND preferred")
                .bind(workspace).bind(item).execute(&mut *tx).await?;
            sqlx::query("INSERT INTO vendor_items(workspace_id,item_id,vendor_id,supplier_reference,order_unit,units_per_pack,pack_price,minimum_order_quantity,reorder_target,preferred,source) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,true,'manual')")
                .bind(workspace).bind(item).bind(vendor).bind(&reference).bind(input.order_unit.trim()).bind(input.units_per_pack).bind(input.pack_price).bind(input.minimum_order_quantity).bind(input.reorder_target)
                .execute(&mut *tx).await?;
            "created"
        }
    };
    if action == "updated" {
        sqlx::query("UPDATE vendor_items SET preferred=false,version=version+1,updated_at=now() WHERE workspace_id=$1 AND item_id=$2 AND vendor_id<>$3 AND preferred")
            .bind(workspace).bind(item).bind(vendor).execute(&mut *tx).await?;
    }
    sqlx::query("UPDATE inventory_items SET supplier=(SELECT name FROM vendors WHERE workspace_id=$1 AND id=$3) WHERE workspace_id=$1 AND id=$2")
        .bind(workspace).bind(item).bind(vendor).execute(&mut *tx).await?;
    audit(&mut tx, workspace, "vendor_item", &format!("{vendor}:{item}"), action, json!({"order_unit":input.order_unit.trim(),"units_per_pack":input.units_per_pack.to_string(),"pack_price":input.pack_price.map(|p|p.to_string()),"minimum_order_quantity":input.minimum_order_quantity.to_string(),"reorder_target":input.reorder_target.map(|t|t.to_string())})).await?;
    let value = rule_json(&mut tx, workspace, vendor, item).await?;
    tx.commit().await?;
    tracing::info!(item, %vendor, action, "Ordering rule saved");
    Ok(value)
}

pub async fn unassign(pool: &PgPool, workspace: &str, vendor: Uuid, item: &str) -> Result<Value> {
    unassign_versioned(pool, workspace, vendor, item, None).await
}
pub async fn unassign_versioned(
    pool: &PgPool,
    workspace: &str,
    vendor: Uuid,
    item: &str,
    expected: Option<i32>,
) -> Result<Value> {
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT id FROM inventory_items WHERE workspace_id=$1 AND id=$2 FOR UPDATE")
        .bind(workspace)
        .bind(item)
        .execute(&mut *tx)
        .await?;
    let version: Option<i32> = sqlx::query_scalar("SELECT version FROM vendor_items WHERE workspace_id=$1 AND vendor_id=$2 AND item_id=$3 FOR UPDATE").bind(workspace).bind(vendor).bind(item).fetch_optional(&mut *tx).await?;
    if version.is_none() {
        return Err(Error::NotFound);
    }
    if expected.is_some() && expected != version {
        return Err(Error::Conflict(
            "This rule changed; reload before removing it".into(),
        ));
    }
    let removed = sqlx::query(
        "DELETE FROM vendor_items WHERE workspace_id=$1 AND vendor_id=$2 AND item_id=$3",
    )
    .bind(workspace)
    .bind(vendor)
    .bind(item)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    if removed == 0 {
        return Err(Error::NotFound);
    }
    sqlx::query("UPDATE inventory_items SET supplier=(SELECT v.name FROM vendor_items vi JOIN vendors v ON v.workspace_id=vi.workspace_id AND v.id=vi.vendor_id WHERE vi.workspace_id=$1 AND vi.item_id=$2 AND vi.preferred LIMIT 1) WHERE workspace_id=$1 AND id=$2")
        .bind(workspace).bind(item).execute(&mut *tx).await?;
    audit(
        &mut tx,
        workspace,
        "vendor_item",
        &format!("{vendor}:{item}"),
        "removed",
        json!({}),
    )
    .await?;
    tx.commit().await?;
    Ok(json!({"removed":true,"vendor_id":vendor,"item_id":item}))
}

pub async fn page(pool: &PgPool, workspace: &str, requested: i64) -> Result<Value> {
    let mut tx = pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *tx)
        .await?;
    let total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM vendors WHERE workspace_id=$1")
        .bind(workspace)
        .fetch_one(&mut *tx)
        .await?;
    let pages = ((total + PAGE_SIZE - 1) / PAGE_SIZE).max(1);
    let page = requested.min(pages).max(1);
    let items = sqlx::query_scalar::<_, Value>("SELECT jsonb_build_object('id',v.id,'name',v.name,'contact_name',v.contact_name,'email',v.email,'phone',v.phone,'source',v.source,'version',v.version,'updated_at',v.updated_at,'item_count',(SELECT COUNT(*) FROM vendor_items vi WHERE vi.workspace_id=v.workspace_id AND vi.vendor_id=v.id),'open_orders',(SELECT COUNT(*) FROM purchase_orders o WHERE o.workspace_id=v.workspace_id AND o.vendor_id=v.id AND o.status IN ('draft','approved'))) FROM vendors v WHERE v.workspace_id=$1 ORDER BY v.name,v.id LIMIT $2 OFFSET $3")
        .bind(workspace).bind(PAGE_SIZE).bind((page - 1) * PAGE_SIZE).fetch_all(&mut *tx).await?;
    tx.commit().await?;
    Ok(
        json!({"items":items,"total":total,"page":page,"page_size":PAGE_SIZE,"pages":pages,"available":true}),
    )
}

pub async fn detail(pool: &PgPool, workspace: &str, id: Uuid, requested: i64) -> Result<Value> {
    let mut tx = pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *tx)
        .await?;
    let record = vendor_json(&mut tx, workspace, id).await?;
    let total: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM vendor_items WHERE workspace_id=$1 AND vendor_id=$2",
    )
    .bind(workspace)
    .bind(id)
    .fetch_one(&mut *tx)
    .await?;
    let pages = ((total + PAGE_SIZE - 1) / PAGE_SIZE).max(1);
    let page = requested.min(pages).max(1);
    let items = sqlx::query_scalar::<_, Value>("SELECT jsonb_build_object('id',vi.item_id,'item_id',vi.item_id,'name',i.name,'unit',i.unit,'category',i.category,'current_balance',i.current_balance::text,'par_level',i.par_level::text,'supplier_reference',vi.supplier_reference,'order_unit',vi.order_unit,'units_per_pack',vi.units_per_pack::text,'pack_price',vi.pack_price::text,'minimum_order_quantity',vi.minimum_order_quantity::text,'reorder_target',vi.reorder_target::text,'preferred',vi.preferred,'version',vi.version) FROM vendor_items vi JOIN inventory_items i ON i.workspace_id=vi.workspace_id AND i.id=vi.item_id WHERE vi.workspace_id=$1 AND vi.vendor_id=$2 ORDER BY i.name,i.id LIMIT $3 OFFSET $4")
        .bind(workspace).bind(id).bind(PAGE_SIZE).bind((page - 1) * PAGE_SIZE).fetch_all(&mut *tx).await?;
    tx.commit().await?;
    Ok(
        json!({"record":record,"items":items,"total":total,"page":page,"pages":pages,"page_size":PAGE_SIZE}),
    )
}

/// Items with no preferred vendor, so the operator can assign them (bounded list).
pub async fn unassigned_items(pool: &PgPool, workspace: &str) -> Result<Value> {
    let items = sqlx::query_scalar::<_, Value>("SELECT jsonb_build_object('id',i.id,'name',i.name,'unit',i.unit,'category',i.category,'current_balance',i.current_balance::text,'par_level',i.par_level::text,'below_par',i.par_level>0 AND i.current_balance<i.par_level) FROM inventory_items i WHERE i.workspace_id=$1 AND NOT EXISTS (SELECT 1 FROM vendor_items vi WHERE vi.workspace_id=i.workspace_id AND vi.item_id=i.id AND vi.preferred) ORDER BY (i.par_level>0 AND i.current_balance<i.par_level) DESC,i.name,i.id LIMIT 100")
        .bind(workspace).fetch_all(pool).await?;
    Ok(json!({"items":items}))
}

pub async fn policy(pool: &PgPool, workspace: &str) -> Result<Value> {
    let row = sqlx::query("SELECT auto_approve_limit,approval_limit,version,updated_at FROM purchasing_policies WHERE workspace_id=$1")
        .bind(workspace).fetch_optional(pool).await?;
    Ok(match row {
        Some(row) => {
            json!({"auto_approve_limit":row.try_get::<Decimal,_>("auto_approve_limit")?.to_string(),"approval_limit":row.try_get::<Option<Decimal>,_>("approval_limit")?.map(|d|d.to_string()),"version":row.try_get::<i32,_>("version")?,"updated_at":row.try_get::<chrono::DateTime<chrono::Utc>,_>("updated_at")?,"currency":"NGN","configured":true})
        }
        None => {
            json!({"auto_approve_limit":"0","approval_limit":null,"version":0,"updated_at":null,"currency":"NGN","configured":false})
        }
    })
}

pub async fn update_policy(pool: &PgPool, workspace: &str, input: &PolicyInput) -> Result<Value> {
    if input.auto_approve_limit < Decimal::ZERO
        || input.auto_approve_limit.scale() > 2
        || input.auto_approve_limit > Decimal::from(1_000_000_000)
    {
        return Err(Error::Invalid(
            "Automatic approval limit must be zero or more with at most 2 decimals".into(),
        ));
    }
    if let Some(limit) = input.approval_limit {
        if limit <= Decimal::ZERO || limit.scale() > 2 || limit > Decimal::from(1_000_000_000) {
            return Err(Error::Invalid("Approval limit must be greater than zero with at most 2 decimals, or empty for no limit".into()));
        }
        if limit < input.auto_approve_limit {
            return Err(Error::Invalid(
                "The approval limit cannot be lower than the automatic approval limit".into(),
            ));
        }
    }
    let mut tx = pool.begin().await?;
    let current: Option<i32> = sqlx::query_scalar(
        "SELECT version FROM purchasing_policies WHERE workspace_id=$1 FOR UPDATE",
    )
    .bind(workspace)
    .fetch_optional(&mut *tx)
    .await?;
    match current {
        Some(version) if version != input.version => {
            return Err(Error::Conflict(
                "Purchasing limits changed since you opened them; reload and try again".into(),
            ));
        }
        Some(_) => {
            sqlx::query("UPDATE purchasing_policies SET auto_approve_limit=$2,approval_limit=$3,version=version+1,updated_at=now() WHERE workspace_id=$1")
                .bind(workspace).bind(input.auto_approve_limit).bind(input.approval_limit).execute(&mut *tx).await?;
        }
        None => {
            if input.version != 0 {
                return Err(Error::Conflict(
                    "No purchasing policy exists yet; reload and try again".into(),
                ));
            }
            sqlx::query("INSERT INTO purchasing_policies(workspace_id,auto_approve_limit,approval_limit) VALUES($1,$2,$3)")
                .bind(workspace).bind(input.auto_approve_limit).bind(input.approval_limit).execute(&mut *tx).await?;
        }
    }
    audit(&mut tx, workspace, "policy", workspace, "updated", json!({"auto_approve_limit":input.auto_approve_limit.to_string(),"approval_limit":input.approval_limit.map(|d|d.to_string())})).await?;
    tx.commit().await?;
    tracing::info!("Purchasing policy updated");
    policy(pool, workspace).await
}
