use crate::error::{Error, Result};
use chrono::NaiveDate;
use rust_decimal::Decimal;
#[allow(unused_imports)]
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{PgPool, Row};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DateRange {
    pub from: NaiveDate,
    pub to: NaiveDate,
}
impl DateRange {
    pub fn validate(&self) -> Result<()> {
        if self.from > self.to || (self.to - self.from).num_days() > 366 {
            return Err(Error::Invalid(
                "Date range must be ordered and at most 367 days".into(),
            ));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InventoryQuery {
    #[serde(default)]
    pub below_par: bool,
    pub search: Option<String>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

pub async fn sales(pool: &PgPool, workspace: &str, range: &DateRange) -> Result<Value> {
    range.validate()?;
    let rows=sqlx::query("SELECT t.business_date, COUNT(*) AS tickets, SUM(t.total_amount) AS ticket_total, SUM(COALESCE(l.gross,0)) AS gross FROM sales_tickets t LEFT JOIN LATERAL (SELECT SUM(quantity*unit_price) AS gross FROM sales_lines WHERE workspace_id=t.workspace_id AND ticket_id=t.id AND billable AND t.total_amount<>0) l ON true WHERE t.workspace_id=$1 AND t.business_date BETWEEN $2 AND $3 GROUP BY t.business_date ORDER BY t.business_date")
        .bind(workspace).bind(range.from).bind(range.to).fetch_all(pool).await?;
    let mut gross = Decimal::ZERO;
    let mut total = Decimal::ZERO;
    let mut count = 0i64;
    let mut days = Vec::new();
    for r in rows {
        let g: Decimal = r.try_get("gross")?;
        let t: Decimal = r.try_get("ticket_total")?;
        let n: i64 = r.try_get("tickets")?;
        gross += g;
        total += t;
        count += n;
        days.push(json!({"date":r.try_get::<NaiveDate,_>("business_date")?,"tickets":n,"gross_line_sales":g.to_string(),"ticket_total":t.to_string()}));
    }
    Ok(
        json!({"currency":"NGN","from":range.from,"to":range.to,"ticket_count":count,"gross_line_sales":gross.to_string(),"ticket_total":total.to_string(),"days_with_records":days.len(),"days":days,"coverage":coverage(pool,workspace).await?,"notes":["Gross line sales excludes voided lines and zero-total tickets. Ticket totals include different adjustments; do not add them to gross sales.","Days without records have unknown coverage; they are not confirmed zero-sales days."]}),
    )
}

pub async fn inventory(pool: &PgPool, workspace: &str, query: &InventoryQuery) -> Result<Value> {
    let limit = query.limit.unwrap_or(50);
    let offset = query.offset.unwrap_or(0);
    if !(1..=100).contains(&limit)
        || offset < 0
        || query.search.as_ref().is_some_and(|s| s.len() > 100)
    {
        return Err(Error::Invalid(
            "Use limit 1..100, nonnegative offset, and search at most 100 characters".into(),
        ));
    }
    let summary=sqlx::query("SELECT COUNT(*) AS count, COUNT(*) FILTER (WHERE par_level>0 AND current_balance<par_level) AS below, COUNT(*) FILTER (WHERE par_level IS NULL OR par_level<=0) AS unset FROM inventory_items WHERE workspace_id=$1").bind(workspace).fetch_one(pool).await?;
    let rows=sqlx::query("SELECT id,name,category,unit,par_level,current_balance,unit_cost,supplier,needs_review FROM inventory_items WHERE workspace_id=$1 AND (NOT $2 OR (par_level>0 AND current_balance<par_level)) AND ($3::text IS NULL OR strpos(lower(name),lower($3))>0) ORDER BY name,id LIMIT $4 OFFSET $5")
        .bind(workspace).bind(query.below_par).bind(&query.search).bind(limit).bind(offset).fetch_all(pool).await?;
    let items=rows.into_iter().map(|r| ->Result<Value>{Ok(json!({
        "id":r.try_get::<String,_>("id")?,"name":r.try_get::<String,_>("name")?,"category":r.try_get::<Option<String>,_>("category")?,"unit":r.try_get::<Option<String>,_>("unit")?,
        "par_level":r.try_get::<Option<Decimal>,_>("par_level")?.map(|d|d.to_string()),"current_balance":r.try_get::<Decimal,_>("current_balance")?.to_string(),"unit_cost":r.try_get::<Option<Decimal>,_>("unit_cost")?.map(|d|d.to_string()),"supplier":r.try_get::<Option<String>,_>("supplier")?,"needs_review":r.try_get::<bool,_>("needs_review")?
    }))}).collect::<Result<Vec<_>>>()?;
    Ok(
        json!({"items":items,"limit":limit,"offset":offset,"sample_total":summary.try_get::<i64,_>("count")?,"sample_below_par":summary.try_get::<i64,_>("below")?,"sample_par_unset":summary.try_get::<i64,_>("unset")?,"notes":["Balances are the current recorded stock. Missing or nonpositive par levels require configuration, not automatic reordering."],"coverage":coverage(pool,workspace).await?}),
    )
}

pub async fn coverage(pool: &PgPool, workspace: &str) -> Result<Value> {
    Ok(sqlx::query_scalar::<_,Value>("SELECT metadata FROM dataset_imports WHERE workspace_id=$1 ORDER BY imported_at DESC LIMIT 1").bind(workspace).fetch_optional(pool).await?.unwrap_or(json!({"status":"not_imported"})))
}

/// Model context is an allowlist, never the raw import manifest/source population.
pub async fn inventory_summary(pool: &PgPool, workspace: &str) -> Result<Value> {
    let row = sqlx::query("SELECT COUNT(*) AS total, COUNT(*) FILTER (WHERE par_level>0 AND current_balance<par_level) AS below, COUNT(*) FILTER (WHERE par_level IS NULL OR par_level<=0) AS unset FROM inventory_items WHERE workspace_id=$1")
        .bind(workspace).fetch_one(pool).await?;
    Ok(
        json!({"total_items":row.try_get::<i64,_>("total")?,"below_par":row.try_get::<i64,_>("below")?,"par_unset":row.try_get::<i64,_>("unset")?}),
    )
}

pub async fn model_context(pool: &PgPool, workspace: &str) -> Result<Value> {
    let coverage = coverage(pool, workspace).await?;
    Ok(json!({"inventory":inventory_summary(pool,workspace).await?,
        "sales":{"from":coverage.get("from"),"to":coverage.get("to"),"missing_days":"unknown"},
        "currency":"NGN","data_source":coverage.get("sources")}))
}

/// Ranked item sales for a period: exact sums in SQL, bounded output, with
/// coverage so the model can tell "no records" from "zero sales".
pub async fn sales_ranking(
    pool: &PgPool,
    workspace: &str,
    range: &DateRange,
    limit: i64,
) -> Result<Value> {
    range.validate()?;
    let limit = limit.clamp(1, 10);
    let rows = sqlx::query("SELECT l.item_name, SUM(l.quantity) AS quantity, SUM(l.quantity*l.unit_price) AS gross, COUNT(DISTINCT t.id) AS tickets FROM sales_lines l JOIN sales_tickets t ON t.workspace_id=l.workspace_id AND t.id=l.ticket_id WHERE l.workspace_id=$1 AND l.billable AND t.total_amount<>0 AND t.business_date BETWEEN $2 AND $3 GROUP BY l.item_name ORDER BY gross DESC, quantity DESC, l.item_name LIMIT $4")
        .bind(workspace).bind(range.from).bind(range.to).bind(limit).fetch_all(pool).await?;
    let coverage = sqlx::query("SELECT COUNT(DISTINCT business_date) AS days, COUNT(*) AS tickets, MIN(business_date) AS first_day, MAX(business_date) AS last_day FROM sales_tickets WHERE workspace_id=$1 AND business_date BETWEEN $2 AND $3")
        .bind(workspace).bind(range.from).bind(range.to).fetch_one(pool).await?;
    let days: i64 = coverage.try_get("days")?;
    let expected = (range.to - range.from).num_days() + 1;
    let items = rows.into_iter().enumerate().map(|(i, r)| -> Result<Value> { Ok(json!({"rank":i+1,"item":r.try_get::<String,_>("item_name")?,"quantity":r.try_get::<Decimal,_>("quantity")?.normalize().to_string(),"gross_line_sales":r.try_get::<Decimal,_>("gross")?.to_string(),"tickets":r.try_get::<i64,_>("tickets")?})) }).collect::<Result<Vec<_>>>()?;
    Ok(
        json!({"currency":"NGN","from":range.from,"to":range.to,"items":items,"limit":limit,
        "coverage":{"days_with_records":days,"days_in_range":expected,"tickets":coverage.try_get::<i64,_>("tickets")?,"first_day":coverage.try_get::<Option<NaiveDate>,_>("first_day")?,"last_day":coverage.try_get::<Option<NaiveDate>,_>("last_day")?,
            "status":if days==0 {"no_records"} else if days<expected {"partial"} else {"complete"}},
        "note":"Gross line sales exclude voided tickets. Days without records are not confirmed zero-sales days."}),
    )
}

/// Compare complete equal-length periods; calculations stay outside the model.
pub async fn sales_comparison(
    pool: &PgPool,
    workspace: &str,
    current: &DateRange,
    previous: &DateRange,
) -> Result<Value> {
    current.validate()?;
    previous.validate()?;
    if (current.to - current.from) != (previous.to - previous.from) {
        return Err(Error::Invalid("Compare periods of equal length".into()));
    }
    let periods = sqlx::query("WITH periods(label,start_day,end_day) AS (VALUES ('current',$2::date,$3::date),('previous',$4::date,$5::date)) SELECT p.label,COUNT(DISTINCT t.business_date) AS days,COALESCE(SUM(l.gross),0) AS gross FROM periods p LEFT JOIN sales_tickets t ON t.workspace_id=$1 AND t.business_date BETWEEN p.start_day AND p.end_day LEFT JOIN LATERAL (SELECT SUM(quantity*unit_price) AS gross FROM sales_lines WHERE workspace_id=t.workspace_id AND ticket_id=t.id AND billable AND t.total_amount<>0) l ON true GROUP BY p.label")
        .bind(workspace).bind(current.from).bind(current.to).bind(previous.from).bind(previous.to).fetch_all(pool).await?;
    let expected = (current.to - current.from).num_days() + 1;
    let mut values = json!({});
    let mut totals = std::collections::BTreeMap::new();
    let mut complete = true;
    for row in periods {
        let label: String = row.try_get("label")?;
        let days: i64 = row.try_get("days")?;
        let gross: Decimal = row.try_get("gross")?;
        let range = if label == "current" {
            current
        } else {
            previous
        };
        values[&label] = json!({"from":range.from,"to":range.to,"days_with_records":days,"days_in_range":expected,"gross_line_sales":if days==0 {None} else {Some(gross.to_string())}});
        complete &= days == expected;
        totals.insert(label, gross);
    }
    let before = totals["previous"];
    let delta = totals["current"] - before;
    Ok(
        json!({"currency":"NGN","periods":values,"comparable":complete,"change":if complete {Some(delta.to_string())} else {None},"change_percent":if complete && before!=Decimal::ZERO {Some((delta/before*Decimal::from(100)).round_dp(2).to_string())} else {None},"note":if !complete {"Incomplete coverage: do not infer growth from missing records"} else if before==Decimal::ZERO {"Percentage change is undefined because the previous period was zero"} else {"Comparison of recorded gross line sales; does not establish causes or profit"}}),
    )
}

/// Ordering rules for an item (by id or name search) or a vendor, for the model.
pub async fn vendor_rules(
    pool: &PgPool,
    workspace: &str,
    item: Option<&str>,
    vendor: Option<&str>,
) -> Result<Value> {
    let item = item.map(str::trim).filter(|s| !s.is_empty());
    let vendor = vendor.map(str::trim).filter(|s| !s.is_empty());
    if item.is_none() && vendor.is_none() {
        return Err(Error::Invalid("Give an item name or a vendor name".into()));
    }
    if item.is_some_and(|s| s.len() > 100) || vendor.is_some_and(|s| s.len() > 100) {
        return Err(Error::Invalid(
            "Search terms are at most 100 characters".into(),
        ));
    }
    let rows = sqlx::query_scalar::<_, Value>("SELECT jsonb_build_object('item_id',i.id,'item',i.name,'unit',i.unit,'current_balance',i.current_balance::text,'par_level',i.par_level::text,'vendor',v.name,'vendor_contact_known',(v.email IS NOT NULL OR v.phone IS NOT NULL),'supplier_reference',vi.supplier_reference,'order_unit',vi.order_unit,'units_per_pack',vi.units_per_pack::text,'pack_price',vi.pack_price::text,'minimum_order_quantity',vi.minimum_order_quantity::text,'reorder_target',COALESCE(vi.reorder_target,i.par_level)::text,'preferred',vi.preferred,'on_order_units',COALESCE((SELECT SUM(l.quantity_units) FROM purchase_order_lines l JOIN purchase_orders o ON o.id=l.purchase_order_id WHERE l.workspace_id=i.workspace_id AND l.item_id=i.id AND o.status='approved'),0)::text) FROM inventory_items i LEFT JOIN vendor_items vi ON vi.workspace_id=i.workspace_id AND vi.item_id=i.id LEFT JOIN vendors v ON v.workspace_id=vi.workspace_id AND v.id=vi.vendor_id WHERE i.workspace_id=$1 AND ($2::text IS NULL OR i.id=$2 OR strpos(lower(i.name),lower($2))>0) AND ($3::text IS NULL OR strpos(lower(COALESCE(v.name,'')),lower($3))>0) ORDER BY i.name,i.id LIMIT 20")
        .bind(workspace).bind(item).bind(vendor).fetch_all(pool).await?;
    Ok(
        json!({"rules":rows,"shown":rows.len(),"policy":policy_summary(pool,workspace).await?,"note":"An item without a vendor row has no ordering rule; a null pack_price means the price is unknown, not zero."}),
    )
}

async fn policy_summary(pool: &PgPool, workspace: &str) -> Result<Value> {
    Ok(sqlx::query_scalar::<_, Value>("SELECT COALESCE((SELECT jsonb_build_object('auto_approve_limit',auto_approve_limit::text,'approval_limit',approval_limit::text,'currency','NGN') FROM purchasing_policies WHERE workspace_id=$1),jsonb_build_object('auto_approve_limit','0','approval_limit',null,'currency','NGN'))")
        .bind(workspace).fetch_one(pool).await?)
}

pub fn compact_inventory(value: &Value) -> Value {
    json!({"total_items":value["sample_total"],"below_par":value["sample_below_par"],
        "par_unset":value["sample_par_unset"],"items":value["items"],"limit":value["limit"],"offset":value["offset"]})
}

pub fn compact_sales(value: &Value) -> Value {
    if value["ticket_count"] == 0 {
        return json!({"status":"no_data","currency":value["currency"],"from":value["from"],"to":value["to"],
            "ticket_count":0,"days_with_records":0,"gross_line_sales":null,"ticket_total":null,"daily":[],
            "message":"No sales records found for these dates. Missing records do not establish zero sales."});
    }
    json!({"currency":value["currency"],"from":value["from"],"to":value["to"],
        "gross_line_sales":value["gross_line_sales"],"ticket_total":value["ticket_total"],
        "ticket_count":value["ticket_count"],"days_with_records":value["days_with_records"],
        "daily":value["days"],"missing_days":"unknown"})
}
