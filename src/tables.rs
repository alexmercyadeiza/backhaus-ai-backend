use crate::{
    error::{Error, Result},
    purchasing,
};
use serde::Deserialize;
use serde_json::{Value, json};
use sqlx::PgPool;
use uuid::Uuid;

const PAGE_SIZE: i64 = 20;
#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableQuery {
    pub page: Option<i64>,
    /// Purchase orders: draft | approved | rejected | withdrawn | open (draft or approved).
    /// Inventory: below_par | out_of_stock | in_stock | par_not_set.
    pub status: Option<String>,
    /// Inventory only: case-insensitive match on item name or category.
    pub search: Option<String>,
}

/// Stock status label; the SQL below must stay in step with this.
const INVENTORY_STATUS_SQL: &str = "CASE WHEN current_balance<=0 THEN 'Out of stock' WHEN par_level IS NULL OR par_level<=0 THEN 'Par not set' WHEN current_balance<par_level THEN 'Below par' ELSE 'In stock' END";

pub async fn page(pool: &PgPool, workspace: &str, kind: &str, query: &TableQuery) -> Result<Value> {
    let requested = query.page.unwrap_or(1);
    if !(1..=50_000).contains(&requested) {
        return Err(Error::Invalid("Page must be between 1 and 50000".into()));
    }
    let (count_sql, rows_sql) = match kind {
        "sales" => (
            "SELECT COUNT(*) FROM sales_tickets WHERE workspace_id=$1",
            "SELECT jsonb_build_object('id',t.id::text,'ticket_number',t.ticket_number,'business_date',t.business_date,'department',t.source->>'departmentName','line_count',l.lines,'gross_line_sales',COALESCE(l.gross,0)::text,'ticket_total',t.total_amount::text) FROM (SELECT * FROM sales_tickets WHERE workspace_id=$1 ORDER BY business_date DESC,id DESC LIMIT $2 OFFSET $3) t LEFT JOIN LATERAL (SELECT COUNT(*) AS lines,SUM(quantity*unit_price) FILTER(WHERE billable AND t.total_amount<>0) AS gross FROM sales_lines WHERE workspace_id=t.workspace_id AND ticket_id=t.id) l ON true ORDER BY t.business_date DESC,t.id DESC",
        ),
        "inventory" => {
            return inventory_page(
                pool,
                workspace,
                requested,
                query.status.as_deref(),
                query.search.as_deref(),
            )
            .await;
        }
        "menu" => (
            "SELECT COUNT(*) FROM menu_items WHERE workspace_id=$1",
            "SELECT jsonb_build_object('id',id::text,'name',name,'category',category,'portions',jsonb_array_length(portions),'price_min',price_min::text,'price_max',price_max::text) FROM menu_items WHERE workspace_id=$1 ORDER BY name,id LIMIT $2 OFFSET $3",
        ),
        "purchase-orders" => {
            return purchase_orders_page(pool, workspace, requested, query.status.as_deref()).await;
        }
        "vendors" => return crate::vendors::page(pool, workspace, requested).await,
        _ => return Err(Error::NotFound),
    };
    if query.status.is_some() {
        return Err(Error::Invalid(
            "Status filters apply to purchase orders and inventory only".into(),
        ));
    }
    if query.search.is_some() {
        return Err(Error::Invalid("Search applies to inventory only".into()));
    }
    let mut tx = pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *tx)
        .await?;
    let total: i64 = sqlx::query_scalar(count_sql)
        .bind(workspace)
        .fetch_one(&mut *tx)
        .await?;
    let pages = ((total + PAGE_SIZE - 1) / PAGE_SIZE).max(1);
    let page = requested.min(pages);
    let items = sqlx::query_scalar::<_, Value>(rows_sql)
        .bind(workspace)
        .bind(PAGE_SIZE)
        .bind((page - 1) * PAGE_SIZE)
        .fetch_all(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(
        json!({"items":items,"total":total,"page":page,"page_size":PAGE_SIZE,"pages":pages,"available":true}),
    )
}

async fn inventory_page(
    pool: &PgPool,
    workspace: &str,
    requested: i64,
    status: Option<&str>,
    search: Option<&str>,
) -> Result<Value> {
    let status = match status {
        None => None,
        Some("below_par") => Some("Below par"),
        Some("out_of_stock") => Some("Out of stock"),
        Some("in_stock") => Some("In stock"),
        Some("par_not_set") => Some("Par not set"),
        Some(_) => return Err(Error::Invalid("Unknown inventory status filter".into())),
    };
    let search = search.map(str::trim).filter(|s| !s.is_empty());
    if search.is_some_and(|s| s.chars().count() > 120) {
        return Err(Error::Invalid(
            "Search must be 120 characters or fewer".into(),
        ));
    }
    // Escape LIKE wildcards so a literal "%" in the search stays literal.
    let pattern = search.map(|s| {
        format!(
            "%{}%",
            s.replace('\\', "\\\\")
                .replace('%', "\\%")
                .replace('_', "\\_")
        )
    });
    let filter = format!(
        "WHERE workspace_id=$1 AND ($2::text IS NULL OR {INVENTORY_STATUS_SQL}=$2) AND ($3::text IS NULL OR name ILIKE $3 OR category ILIKE $3)"
    );
    let mut tx = pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *tx)
        .await?;
    let total: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM inventory_items {filter}"))
        .bind(workspace)
        .bind(status)
        .bind(&pattern)
        .fetch_one(&mut *tx)
        .await?;
    let pages = ((total + PAGE_SIZE - 1) / PAGE_SIZE).max(1);
    let page = requested.min(pages);
    let items = sqlx::query_scalar::<_, Value>(&format!(
        "SELECT jsonb_build_object('id',id,'name',name,'category',category,'unit',unit,'balance',current_balance::text,'par_level',par_level::text,'unit_cost',unit_cost::text,'status',{INVENTORY_STATUS_SQL}) FROM inventory_items {filter} ORDER BY name,id LIMIT $4 OFFSET $5"
    ))
    .bind(workspace)
    .bind(status)
    .bind(&pattern)
    .bind(PAGE_SIZE)
    .bind((page - 1) * PAGE_SIZE)
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(
        json!({"items":items,"total":total,"page":page,"page_size":PAGE_SIZE,"pages":pages,"available":true}),
    )
}

async fn purchase_orders_page(
    pool: &PgPool,
    workspace: &str,
    requested: i64,
    status: Option<&str>,
) -> Result<Value> {
    let statuses: Vec<String> = match status {
        None => vec![],
        Some("open") => vec!["draft".into(), "approved".into()],
        Some(s @ ("draft" | "approved" | "rejected" | "withdrawn")) => vec![s.into()],
        Some(_) => {
            return Err(Error::Invalid(
                "Unknown purchase order status filter".into(),
            ));
        }
    };
    let mut tx = pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *tx)
        .await?;
    let total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM purchase_orders WHERE workspace_id=$1 AND (cardinality($2::text[])=0 OR status=ANY($2))")
        .bind(workspace).bind(&statuses).fetch_one(&mut *tx).await?;
    let pages = ((total + PAGE_SIZE - 1) / PAGE_SIZE).max(1);
    let page = requested.min(pages);
    let mut items = sqlx::query_scalar::<_, Value>("SELECT jsonb_build_object('id',o.id,'number',o.number,'vendor',v.name,'status',o.status,'approval_kind',o.approval_kind,'approval_reason',o.approval_reason,'subtotal',o.subtotal::text,'currency',o.currency,'line_count',o.line_count,'attention',o.attention,'requires_approval',o.status='draft','created_at',o.created_at,'updated_at',o.updated_at,'decided_at',o.decided_at) FROM purchase_orders o JOIN vendors v ON v.workspace_id=o.workspace_id AND v.id=o.vendor_id WHERE o.workspace_id=$1 AND (cardinality($2::text[])=0 OR o.status=ANY($2)) ORDER BY CASE o.status WHEN 'draft' THEN 0 ELSE 1 END,o.created_at DESC,o.number DESC LIMIT $3 OFFSET $4")
        .bind(workspace).bind(&statuses).bind(PAGE_SIZE).bind((page - 1) * PAGE_SIZE).fetch_all(&mut *tx).await?;
    tx.commit().await?;
    for item in &mut items {
        item["approval_explanation"] = json!(purchasing::describe_reason(
            item["approval_reason"].as_str().unwrap_or("")
        ));
    }
    Ok(
        json!({"items":items,"total":total,"page":page,"page_size":PAGE_SIZE,"pages":pages,"available":true}),
    )
}

/// Fetch only the selected record's related details; never expose raw source JSON.
pub async fn detail(
    pool: &PgPool,
    workspace: &str,
    kind: &str,
    id: &str,
    query: &TableQuery,
) -> Result<Value> {
    let requested = query.page.unwrap_or(1);
    if !(1..=50_000).contains(&requested) || id.len() > 200 {
        return Err(Error::Invalid("Invalid record or page".into()));
    }
    if kind == "purchase-orders" {
        let id = Uuid::parse_str(id).map_err(|_| Error::NotFound)?;
        return purchasing::order_detail(pool, workspace, id, requested, PAGE_SIZE).await;
    }
    if kind == "vendors" {
        let id = Uuid::parse_str(id).map_err(|_| Error::NotFound)?;
        return crate::vendors::detail(pool, workspace, id, requested).await;
    }
    let numeric_id = if matches!(kind, "sales" | "menu") {
        Some(id.parse::<i64>().map_err(|_| Error::NotFound)?)
    } else {
        None
    };
    let mut tx = pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *tx)
        .await?;
    let (mut record, count_sql, items_sql) = match kind {
        "sales" => {
            let record = sqlx::query_scalar::<_, Value>("SELECT jsonb_build_object('id',id::text,'ticket_number',ticket_number,'business_date',business_date,'department',source->>'departmentName','ticket_total',total_amount::text) FROM sales_tickets WHERE workspace_id=$1 AND id=$2")
                .bind(workspace).bind(numeric_id).fetch_optional(&mut *tx).await?.ok_or(Error::NotFound)?;
            (
                record,
                "SELECT COUNT(*) FROM sales_lines WHERE workspace_id=$1 AND ticket_id=$2::text::bigint",
                "SELECT jsonb_build_object('id',id::text,'name',item_name,'quantity',quantity::text,'unit_price',unit_price::text,'amount',(quantity*unit_price)::text,'billable',billable) FROM sales_lines WHERE workspace_id=$1 AND ticket_id=$2::text::bigint ORDER BY id LIMIT $3 OFFSET $4",
            )
        }
        "inventory" => {
            let record = sqlx::query_scalar::<_, Value>("SELECT jsonb_build_object('id',i.id,'name',i.name,'category',i.category,'unit',i.unit,'balance',i.current_balance::text,'par_level',i.par_level::text,'unit_cost',i.unit_cost::text,'supplier',COALESCE(v.name,i.supplier),'vendor_id',v.id,'vendor_source',v.source,'order_unit',vi.order_unit,'pack_price',vi.pack_price::text,'units_per_pack',vi.units_per_pack::text,'minimum_order_quantity',vi.minimum_order_quantity::text,'reorder_target',vi.reorder_target::text,'on_order_units',COALESCE((SELECT SUM(l.quantity_units) FROM purchase_order_lines l JOIN purchase_orders o ON o.id=l.purchase_order_id WHERE l.workspace_id=i.workspace_id AND l.item_id=i.id AND o.status='approved'),0)::text,'needs_review',i.needs_review,'status',CASE WHEN i.par_level IS NULL OR i.par_level<=0 THEN 'Par not set' WHEN i.current_balance<i.par_level THEN 'Below par' ELSE 'In stock' END) FROM inventory_items i LEFT JOIN vendor_items vi ON vi.workspace_id=i.workspace_id AND vi.item_id=i.id AND vi.preferred LEFT JOIN vendors v ON v.workspace_id=vi.workspace_id AND v.id=vi.vendor_id WHERE i.workspace_id=$1 AND i.id=$2")
                .bind(workspace).bind(id).fetch_optional(&mut *tx).await?.ok_or(Error::NotFound)?;
            (
                record,
                "SELECT COUNT(*) FROM inventory_movements WHERE workspace_id=$1 AND item_id=$2",
                "SELECT jsonb_build_object('id',id,'date',business_date,'type',movement_type,'quantity',quantity_delta::text,'reason',reason,'balance_after',balance_after::text) FROM inventory_movements WHERE workspace_id=$1 AND item_id=$2 ORDER BY business_date DESC,recorded_at DESC,id DESC LIMIT $3 OFFSET $4",
            )
        }
        "menu" => {
            let record = sqlx::query_scalar::<_, Value>("SELECT jsonb_build_object('id',id::text,'name',name,'category',category,'portions',jsonb_array_length(portions),'price_min',price_min::text,'price_max',price_max::text) FROM menu_items WHERE workspace_id=$1 AND id=$2")
                .bind(workspace).bind(numeric_id).fetch_optional(&mut *tx).await?.ok_or(Error::NotFound)?;
            (
                record,
                "SELECT jsonb_array_length(portions)::bigint FROM menu_items WHERE workspace_id=$1 AND id=$2::text::bigint",
                "SELECT jsonb_build_object('id',p.n::text,'name',p.value->>'name','multiplier',p.value->>'multiplier','prices',COALESCE((SELECT jsonb_agg(jsonb_build_object('label',price->>'priceTag','amount',price->>'price')) FROM jsonb_array_elements(CASE WHEN jsonb_typeof(p.value->'prices')='array' THEN p.value->'prices' ELSE '[]'::jsonb END) price),'[]'::jsonb)) FROM menu_items m CROSS JOIN LATERAL jsonb_array_elements(m.portions) WITH ORDINALITY p(value,n) WHERE m.workspace_id=$1 AND m.id=$2::text::bigint ORDER BY p.n LIMIT $3 OFFSET $4",
            )
        }
        _ => return Err(Error::NotFound),
    };
    let total: i64 = sqlx::query_scalar(count_sql)
        .bind(workspace)
        .bind(id)
        .fetch_one(&mut *tx)
        .await?;
    let pages = ((total + PAGE_SIZE - 1) / PAGE_SIZE).max(1);
    let page = requested.min(pages);
    let items = sqlx::query_scalar::<_, Value>(items_sql)
        .bind(workspace)
        .bind(id)
        .bind(PAGE_SIZE)
        .bind((page - 1) * PAGE_SIZE)
        .fetch_all(&mut *tx)
        .await?;
    if kind == "sales" {
        let gross: String = sqlx::query_scalar("SELECT COALESCE(SUM(l.quantity*l.unit_price) FILTER(WHERE l.billable AND t.total_amount<>0),0)::text FROM sales_tickets t LEFT JOIN sales_lines l ON l.workspace_id=t.workspace_id AND l.ticket_id=t.id WHERE t.workspace_id=$1 AND t.id=$2")
            .bind(workspace).bind(numeric_id).fetch_one(&mut *tx).await?;
        record["line_count"] = json!(total);
        record["gross_line_sales"] = json!(gross);
    }
    tx.commit().await?;
    Ok(
        json!({"record":record,"items":items,"total":total,"page":page,"pages":pages,"page_size":PAGE_SIZE}),
    )
}
