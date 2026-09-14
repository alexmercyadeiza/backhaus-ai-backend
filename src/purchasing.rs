//! Vendors, ordering rules, purchase-order drafts and approvals.
//! All comparisons and money arithmetic happen here in Rust with exact
//! decimals. The language model only reads these results.
use crate::{
    error::{Error, Result},
    jobs::Run,
    reports::REPORT_SLOTS,
};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use sqlx::{PgPool, Postgres, Row, Transaction};
use std::collections::BTreeMap;
use uuid::Uuid;

const MAX_CHECK_ITEMS: i64 = 500;
const MAX_ATTENTION: usize = 25;
pub const ACTOR_DASHBOARD: &str = "dashboard-operator";
pub const ACTOR_POLICY: &str = "purchasing-policy";
pub const ACTOR_AGENT: &str = "inventory-agent";

#[derive(Clone, Debug)]
pub struct Policy {
    pub auto_approve_limit: Decimal,
    pub approval_limit: Option<Decimal>,
}
impl Policy {
    pub fn to_json(&self) -> Value {
        json!({"auto_approve_limit":self.auto_approve_limit.to_string(),"approval_limit":self.approval_limit.map(|d|d.to_string()),"currency":"NGN"})
    }
}

pub async fn policy(tx: &mut Transaction<'_, Postgres>, workspace: &str) -> Result<Policy> {
    let row = sqlx::query(
        "SELECT auto_approve_limit,approval_limit FROM purchasing_policies WHERE workspace_id=$1",
    )
    .bind(workspace)
    .fetch_optional(&mut **tx)
    .await?;
    Ok(match row {
        Some(row) => Policy {
            auto_approve_limit: row.try_get("auto_approve_limit")?,
            approval_limit: row.try_get("approval_limit")?,
        },
        None => Policy {
            auto_approve_limit: Decimal::ZERO,
            approval_limit: None,
        },
    })
}

fn money(value: Decimal) -> Decimal {
    value.round_dp(2)
}
fn quantity(value: Decimal) -> Decimal {
    value.round_dp(3).normalize()
}

#[derive(Clone, Debug)]
struct Line {
    item_id: String,
    item_name: String,
    supplier_reference: Option<String>,
    order_unit: String,
    units_per_pack: Decimal,
    quantity_packs: Decimal,
    quantity_units: Decimal,
    pack_price: Decimal,
    line_total: Decimal,
    current_balance: Decimal,
    par_level: Decimal,
    reorder_target: Decimal,
    on_order_units: Decimal,
}
impl Line {
    fn fingerprint(&self) -> Value {
        line_fingerprint(&self.to_json())
    }
    fn to_json(&self) -> Value {
        json!({"item_id":self.item_id,"item":self.item_name,"supplier_reference":self.supplier_reference,"order_unit":self.order_unit,
            "units_per_pack":self.units_per_pack.to_string(),"quantity_packs":self.quantity_packs.to_string(),"quantity_units":self.quantity_units.to_string(),
            "pack_price":self.pack_price.to_string(),"line_total":self.line_total.to_string(),"current_balance":self.current_balance.to_string(),
            "par_level":self.par_level.to_string(),"reorder_target":self.reorder_target.to_string(),"on_order_units":self.on_order_units.to_string()})
    }
}
fn line_fingerprint(line: &Value) -> Value {
    let mut value = json!({});
    for key in ["item_id", "order_unit", "supplier_reference"] {
        value[key] = line[key].clone();
    }
    for key in [
        "quantity_packs",
        "pack_price",
        "units_per_pack",
        "quantity_units",
        "current_balance",
        "par_level",
        "reorder_target",
        "on_order_units",
    ] {
        value[key] = json!(
            line[key]
                .as_str()
                .and_then(|s| s.parse::<Decimal>().ok())
                .map(|d| d.normalize().to_string())
        );
    }
    value
}
#[derive(Clone, Debug)]
struct VendorGroup {
    vendor_id: Uuid,
    vendor_name: String,
    contact_missing: bool,
    lines: Vec<Line>,
}

/// Pure ordering rule, kept separate so it can be tested without a database.
/// Returns packs to order, or `None` when the shortage is already covered.
pub fn packs_to_order(
    current_balance: Decimal,
    par_level: Decimal,
    reorder_target: Option<Decimal>,
    on_order_units: Decimal,
    units_per_pack: Decimal,
    minimum_order_quantity: Decimal,
) -> Option<Decimal> {
    let target = reorder_target.unwrap_or(par_level).max(par_level);
    let shortage = target - current_balance - on_order_units;
    if shortage <= Decimal::ZERO || units_per_pack <= Decimal::ZERO {
        return None;
    }
    let packs = (shortage / units_per_pack).ceil();
    Some(packs.max(minimum_order_quantity.ceil()).max(Decimal::ONE))
}

fn approval_reason(
    policy: &Policy,
    subtotal: Decimal,
    attention: &[Value],
) -> (&'static str, bool) {
    if let Some(limit) = policy.approval_limit
        && subtotal > limit
    {
        return ("exceeds_approval_limit", false);
    }
    if !attention.is_empty() {
        return ("vendor_details_incomplete", false);
    }
    if policy.auto_approve_limit > Decimal::ZERO && subtotal <= policy.auto_approve_limit {
        return ("within_auto_approval_limit", true);
    }
    if policy.auto_approve_limit > Decimal::ZERO {
        ("exceeds_auto_approval_limit", false)
    } else {
        ("manual_approval_required", false)
    }
}

/// Open orders for the model and the dashboard, bounded and compact.
pub async fn list_for_model(pool: &PgPool, workspace: &str, status: Option<&str>) -> Result<Value> {
    let statuses: Vec<String> = match status {
        None | Some("open") => vec!["draft".into(), "approved".into()],
        Some(s @ ("draft" | "approved" | "rejected" | "withdrawn")) => vec![s.into()],
        Some(_) => {
            return Err(Error::Invalid(
                "status must be open, draft, approved, rejected or withdrawn".into(),
            ));
        }
    };
    let items = sqlx::query_scalar::<_, Value>("SELECT jsonb_build_object('id',o.id,'number',o.number,'vendor',COALESCE(o.vendor_snapshot->>'name',v.name),'status',o.status,'approval_kind',o.approval_kind,'approval_reason',o.approval_reason,'subtotal',o.subtotal::text,'currency',o.currency,'line_count',o.line_count,'attention',o.attention,'created_at',o.created_at,'decided_at',o.decided_at) FROM purchase_orders o JOIN vendors v ON v.workspace_id=o.workspace_id AND v.id=o.vendor_id WHERE o.workspace_id=$1 AND o.status=ANY($2) ORDER BY CASE o.status WHEN 'draft' THEN 0 ELSE 1 END,o.created_at DESC,o.number DESC LIMIT 20")
        .bind(workspace).bind(&statuses).fetch_all(pool).await?;
    let total: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM purchase_orders WHERE workspace_id=$1 AND status=ANY($2)",
    )
    .bind(workspace)
    .bind(&statuses)
    .fetch_one(pool)
    .await?;
    Ok(
        json!({"orders":items,"total":total,"shown":items.len(),"note":"Approval records intent only; sending to the vendor and payment are not connected."}),
    )
}

pub fn describe_reason(reason: &str) -> &'static str {
    match reason {
        "exceeds_approval_limit" => {
            "Total exceeds the configured approval limit; raise the limit in purchasing policy before approving"
        }
        "vendor_details_incomplete" => "Vendor details are incomplete; review before approving",
        "within_auto_approval_limit" => {
            "Approved automatically: total is within the automatic approval limit"
        }
        "exceeds_auto_approval_limit" => {
            "Total exceeds the automatic approval limit and needs your approval"
        }
        "manual_approval_required" => {
            "Automatic approval is disabled; every order needs your approval"
        }
        _ => "Needs your approval",
    }
}

async fn next_number(tx: &mut Transaction<'_, Postgres>, workspace: &str) -> Result<String> {
    let number:i32=sqlx::query_scalar("INSERT INTO purchase_order_counters(workspace_id,next_number) VALUES($1,2) ON CONFLICT(workspace_id) DO UPDATE SET next_number=purchase_order_counters.next_number+1 RETURNING next_number-1")
        .bind(workspace).fetch_one(&mut **tx).await?;
    Ok(format!("PO-{number:04}"))
}

async fn insert_lines(
    tx: &mut Transaction<'_, Postgres>,
    workspace: &str,
    order: Uuid,
    lines: &[Line],
) -> Result<()> {
    sqlx::query("DELETE FROM purchase_order_lines WHERE purchase_order_id=$1")
        .bind(order)
        .execute(&mut **tx)
        .await?;
    for (position, line) in lines.iter().enumerate() {
        sqlx::query("INSERT INTO purchase_order_lines(id,purchase_order_id,workspace_id,item_id,item_name,supplier_reference,order_unit,units_per_pack,quantity_packs,quantity_units,pack_price,line_total,current_balance,par_level,reorder_target,on_order_units,position) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)")
            .bind(Uuid::new_v4()).bind(order).bind(workspace).bind(&line.item_id).bind(&line.item_name).bind(&line.supplier_reference).bind(&line.order_unit)
            .bind(line.units_per_pack).bind(line.quantity_packs).bind(line.quantity_units).bind(line.pack_price).bind(line.line_total)
            .bind(line.current_balance).bind(line.par_level).bind(line.reorder_target).bind(line.on_order_units).bind(position as i32 + 1)
            .execute(&mut **tx).await?;
    }
    Ok(())
}

async fn record_event(
    tx: &mut Transaction<'_, Postgres>,
    workspace: &str,
    order: Uuid,
    event_type: &str,
    actor: &str,
    payload: Value,
) -> Result<()> {
    sqlx::query("INSERT INTO purchase_order_events(purchase_order_id,workspace_id,event_type,actor,payload) VALUES($1,$2,$3,$4,$5)")
        .bind(order).bind(workspace).bind(event_type).bind(actor).bind(payload).execute(&mut **tx).await?;
    Ok(())
}

/// Compare stock to par levels, apply vendor rules and persist one draft per
/// vendor. Runs inside the inventory check transaction, which already holds
/// the role lock, so concurrent checks cannot both prepare the same revision.
/// Returns the findings stored as the agent's observation.
pub async fn prepare_drafts(
    tx: &mut Transaction<'_, Postgres>,
    workspace: &str,
    revision: i64,
) -> Result<Value> {
    let policy = policy(tx, workspace).await?;
    let counts = sqlx::query("SELECT COUNT(*) AS total, COUNT(*) FILTER(WHERE par_level>0 AND current_balance<par_level) AS below, COUNT(*) FILTER(WHERE par_level IS NULL OR par_level<=0) AS unset FROM inventory_items WHERE workspace_id=$1")
        .bind(workspace).fetch_one(&mut **tx).await?;
    let total_items: i64 = counts.try_get("total")?;
    let below_par: i64 = counts.try_get("below")?;
    let par_unset: i64 = counts.try_get("unset")?;
    let rows = sqlx::query("SELECT i.id,i.name,i.par_level,i.current_balance,vi.vendor_id,v.name AS vendor_name,(v.email IS NULL AND v.phone IS NULL) AS contact_missing,vi.supplier_reference,vi.order_unit,vi.units_per_pack,vi.pack_price,vi.minimum_order_quantity,vi.reorder_target,COALESCE((SELECT SUM(l.quantity_units) FROM purchase_order_lines l JOIN purchase_orders o ON o.id=l.purchase_order_id WHERE l.workspace_id=i.workspace_id AND l.item_id=i.id AND o.status='approved'),0) AS on_order FROM inventory_items i LEFT JOIN vendor_items vi ON vi.workspace_id=i.workspace_id AND vi.item_id=i.id AND vi.preferred LEFT JOIN vendors v ON v.workspace_id=vi.workspace_id AND v.id=vi.vendor_id WHERE i.workspace_id=$1 AND i.par_level>0 AND i.current_balance<i.par_level ORDER BY i.name,i.id LIMIT $2")
        .bind(workspace).bind(MAX_CHECK_ITEMS).fetch_all(&mut **tx).await?;
    let mut attention: Vec<Value> = Vec::new();
    let mut covered = 0usize;
    let mut groups: BTreeMap<String, VendorGroup> = BTreeMap::new();
    for row in &rows {
        let item_id: String = row.try_get("id")?;
        let item_name: String = row.try_get("name")?;
        let par_level: Decimal = row.try_get("par_level")?;
        let current_balance: Decimal = row.try_get("current_balance")?;
        let on_order: Decimal = row.try_get("on_order")?;
        let vendor_id: Option<Uuid> = row.try_get("vendor_id")?;
        let Some(vendor_id) = vendor_id else {
            attention.push(json!({"item_id":item_id,"item":item_name,"reason":"no_vendor","detail":"No vendor is assigned to this item"}));
            continue;
        };
        let pack_price: Option<Decimal> = row.try_get("pack_price")?;
        let Some(pack_price) = pack_price else {
            attention.push(json!({"item_id":item_id,"item":item_name,"reason":"no_price","detail":"The vendor price for this item is not set"}));
            continue;
        };
        let units_per_pack: Decimal = row.try_get("units_per_pack")?;
        let minimum: Decimal = row.try_get("minimum_order_quantity")?;
        let reorder_target: Option<Decimal> = row.try_get("reorder_target")?;
        let Some(packs) = packs_to_order(
            current_balance,
            par_level,
            reorder_target,
            on_order,
            units_per_pack,
            minimum,
        ) else {
            covered += 1;
            attention.push(json!({"item_id":item_id,"item":item_name,"reason":"covered_by_open_order","detail":format!("{} units are already on an approved order",quantity(on_order))}));
            continue;
        };
        let vendor_name: String = row.try_get("vendor_name")?;
        let contact_missing: bool = row.try_get("contact_missing")?;
        let line = Line {
            item_id,
            item_name,
            supplier_reference: row.try_get("supplier_reference")?,
            order_unit: row.try_get("order_unit")?,
            units_per_pack,
            quantity_packs: packs,
            quantity_units: quantity(packs * units_per_pack),
            pack_price,
            line_total: money(packs * pack_price),
            current_balance,
            par_level,
            reorder_target: reorder_target.unwrap_or(par_level).max(par_level),
            on_order_units: on_order,
        };
        groups
            .entry(format!("{vendor_name}\u{0}{vendor_id}"))
            .or_insert_with(|| VendorGroup {
                vendor_id,
                vendor_name,
                contact_missing,
                lines: Vec::new(),
            })
            .lines
            .push(line);
    }
    let unset_rows = sqlx::query("SELECT id,name FROM inventory_items WHERE workspace_id=$1 AND (par_level IS NULL OR par_level<=0) ORDER BY name,id LIMIT 50")
        .bind(workspace).fetch_all(&mut **tx).await?;
    for row in unset_rows {
        attention.push(json!({"item_id":row.try_get::<String,_>("id")?,"item":row.try_get::<String,_>("name")?,"reason":"no_par","detail":"No par level is configured, so the shortage is unknown"}));
    }
    // Existing drafts are locked so a concurrent decision waits for this revision.
    let existing = sqlx::query("SELECT id,vendor_id,version FROM purchase_orders WHERE workspace_id=$1 AND status='draft' ORDER BY id FOR UPDATE")
        .bind(workspace).fetch_all(&mut **tx).await?;
    let mut drafts: BTreeMap<Uuid, (Uuid, i32)> = BTreeMap::new();
    for row in existing {
        drafts.insert(
            row.try_get("vendor_id")?,
            (row.try_get("id")?, row.try_get("version")?),
        );
    }
    let mut orders = Vec::new();
    let mut needs_approval = 0usize;
    let mut auto_approved = 0usize;
    let mut held = Vec::new();
    for group in groups.values() {
        let subtotal = money(group.lines.iter().map(|l| l.line_total).sum());
        let current: Vec<_> = group.lines.iter().map(Line::fingerprint).collect();
        // A proposal the operator just rejected is not re-created while the
        // situation is unchanged; different lines, quantities or prices are a new proposal.
        if !drafts.contains_key(&group.vendor_id) {
            let rejected = sqlx::query("SELECT o.id,o.number FROM purchase_orders o WHERE o.workspace_id=$1 AND o.vendor_id=$2 AND o.status='rejected' ORDER BY o.decided_at DESC NULLS LAST,o.created_at DESC LIMIT 1")
                .bind(workspace).bind(group.vendor_id).fetch_optional(&mut **tx).await?;
            if let Some(row) = rejected {
                let id: Uuid = row.try_get("id")?;
                let (_, previous_lines) =
                    order_lines(tx, workspace, id, MAX_CHECK_ITEMS, 0).await?;
                let previous: Vec<Value> = previous_lines.iter().map(line_fingerprint).collect();
                if previous == current {
                    let number: String = row.try_get("number")?;
                    held.push(json!({"vendor":group.vendor_name,"number":number,"reason":"rejected_unchanged","detail":format!("{number} was rejected and nothing has changed for {}; no new draft until stock, rules or prices change",group.vendor_name)}));
                    continue;
                }
            }
        }
        let mut order_attention = Vec::new();
        if group.contact_missing {
            order_attention.push(json!({"reason":"vendor_contact_missing","detail":"No email or phone is recorded for this vendor; the order cannot be sent yet"}));
        }
        let (reason, auto) = approval_reason(&policy, subtotal, &order_attention);
        let lines_json: Vec<Value> = group.lines.iter().map(Line::to_json).collect();
        let (order_id, number, change) = if let Some((id, version)) =
            drafts.remove(&group.vendor_id)
        {
            let (_, previous_lines) = order_lines(tx, workspace, id, MAX_CHECK_ITEMS, 0).await?;
            let previous: Vec<Value> = previous_lines.iter().map(line_fingerprint).collect();
            let number: String =
                sqlx::query_scalar("SELECT number FROM purchase_orders WHERE id=$1")
                    .bind(id)
                    .fetch_one(&mut **tx)
                    .await?;
            if previous == current {
                sqlx::query("UPDATE purchase_orders SET revision=$2,attention=$3,approval_reason=$4,updated_at=now() WHERE id=$1")
                    .bind(id).bind(revision).bind(json!(order_attention)).bind(reason).execute(&mut **tx).await?;
                (id, number, "unchanged")
            } else {
                insert_lines(tx, workspace, id, &group.lines).await?;
                sqlx::query("UPDATE purchase_orders SET subtotal=$2,line_count=$3,attention=$4,approval_reason=$5,revision=$6,version=$7,updated_at=now() WHERE id=$1")
                    .bind(id).bind(subtotal).bind(group.lines.len() as i32).bind(json!(order_attention)).bind(reason).bind(revision).bind(version+1).execute(&mut **tx).await?;
                record_event(tx, workspace, id, "revised", ACTOR_AGENT, json!({"revision":revision,"version":version+1,"subtotal":subtotal.to_string(),"line_count":group.lines.len(),"lines":lines_json})).await?;
                (id, number, "revised")
            }
        } else {
            let id = Uuid::new_v4();
            let number = next_number(tx, workspace).await?;
            sqlx::query("INSERT INTO purchase_orders(id,workspace_id,number,vendor_id,status,approval_reason,subtotal,line_count,attention,revision) VALUES($1,$2,$3,$4,'draft',$5,$6,$7,$8,$9)")
                .bind(id).bind(workspace).bind(&number).bind(group.vendor_id).bind(reason).bind(subtotal).bind(group.lines.len() as i32).bind(json!(order_attention)).bind(revision).execute(&mut **tx).await?;
            insert_lines(tx, workspace, id, &group.lines).await?;
            record_event(tx, workspace, id, "drafted", ACTOR_AGENT, json!({"revision":revision,"subtotal":subtotal.to_string(),"line_count":group.lines.len(),"lines":lines_json})).await?;
            (id, number, "drafted")
        };
        let status = if auto {
            sqlx::query("UPDATE purchase_orders SET status='approved',approval_kind='automatic',decided_at=now(),decided_by=$2,updated_at=now(),vendor_snapshot=(SELECT jsonb_build_object('name',v.name,'contact_name',v.contact_name,'email',v.email,'phone',v.phone) FROM vendors v WHERE v.workspace_id=purchase_orders.workspace_id AND v.id=purchase_orders.vendor_id) WHERE id=$1 AND status='draft'")
                .bind(order_id).bind(ACTOR_POLICY).execute(&mut **tx).await?;
            record_event(tx, workspace, order_id, "approved", ACTOR_POLICY, json!({"kind":"automatic","reason":reason,"subtotal":subtotal.to_string(),"auto_approve_limit":policy.auto_approve_limit.to_string()})).await?;
            auto_approved += 1;
            "approved"
        } else {
            needs_approval += 1;
            "draft"
        };
        orders.push(json!({"id":order_id,"number":number,"vendor":group.vendor_name,"status":status,"approval_kind":if auto {"automatic"} else {"manual"},
            "approval_explanation":describe_reason(reason),"approval_reason":reason,"blocked":reason=="exceeds_approval_limit","can_approve":!auto && reason!="exceeds_approval_limit","subtotal":subtotal.to_string(),"currency":"NGN","line_count":group.lines.len(),"change":change,"attention":order_attention}));
    }
    // Drafts whose vendor no longer has a shortage are withdrawn, never left stale.
    let mut withdrawn = Vec::new();
    for (_vendor, (id, _version)) in drafts {
        let number: String = sqlx::query_scalar("UPDATE purchase_orders SET status='withdrawn',decided_at=now(),decided_by=$2,decision_note='No items below par remain for this vendor',revision=$3,updated_at=now() WHERE id=$1 RETURNING number")
            .bind(id).bind(ACTOR_AGENT).bind(revision).fetch_one(&mut **tx).await?;
        record_event(
            tx,
            workspace,
            id,
            "withdrawn",
            ACTOR_AGENT,
            json!({"revision":revision,"reason":"no_shortage"}),
        )
        .await?;
        withdrawn.push(number);
    }
    for item in held {
        attention.push(item);
    }
    let attention_total = attention.len();
    attention.truncate(MAX_ATTENTION);
    let mut parts = vec![format!("{below_par} items below par")];
    if !orders.is_empty() {
        parts.push(format!(
            "{} order{} prepared",
            orders.len(),
            if orders.len() == 1 { "" } else { "s" }
        ));
    }
    let blocked = orders.iter().filter(|o| o["blocked"] == true).count();
    if needs_approval > blocked {
        parts.push(format!("{} awaiting approval", needs_approval - blocked));
    }
    if blocked > 0 {
        parts.push(format!("{blocked} blocked by limit"));
    }
    if auto_approved > 0 {
        parts.push(format!("{auto_approved} auto-approved"));
    }
    let needs_setup = attention_total.saturating_sub(covered);
    if needs_setup > 0 {
        parts.push(format!("{needs_setup} need attention"));
    }
    if covered > 0 {
        parts.push(format!("{covered} already on order"));
    }
    Ok(
        json!({"summary":parts.join(" · "),"total_items":total_items,"below_par":below_par,"par_unset":par_unset,"revision":revision,
        "orders":orders,"needs_approval":needs_approval,"blocked_orders":blocked,"auto_approved":auto_approved,"covered_by_open_orders":covered,
        "attention":attention,"attention_total":attention_total,"withdrawn":withdrawn,"policy":policy.to_json(),"review":Value::Null}),
    )
}

/// Attach the Strands review to the checkpoint it describes; a newer
/// revision keeps its own findings untouched.
pub async fn record_review(pool: &PgPool, run: &Run, result: &Value) -> Result<()> {
    let revision = run.input["revision"].as_i64().unwrap_or(-1);
    let answer = result["answer"].as_str().unwrap_or("");
    let answer: String = answer.chars().take(2000).collect();
    let updated = sqlx::query("UPDATE scoped_agents SET observation=COALESCE(observation,'{}'::jsonb)||jsonb_build_object('review',$3::text,'review_run_id',$4::uuid,'review_revision',$2::bigint),updated_at=now() WHERE workspace_id=$1 AND role='inventory' AND checked_revision=$2 AND EXISTS(SELECT 1 FROM agent_runs r WHERE r.id=$4 AND r.workspace_id=$1 AND r.status='completed')")
        .bind(&run.workspace).bind(revision).bind(answer).bind(run.id).execute(pool).await?.rows_affected();
    tracing::info!(run_id=%run.id, revision, attached = updated == 1, "Inventory review recorded");
    Ok(())
}

/// Compact findings for the model: the persisted check, without the previous review text.
pub async fn findings_for_model(pool: &PgPool, workspace: &str) -> Result<Value> {
    let observation = sqlx::query_scalar::<_, Option<Value>>(
        "SELECT observation FROM scoped_agents WHERE workspace_id=$1 AND role='inventory'",
    )
    .bind(workspace)
    .fetch_optional(pool)
    .await?
    .flatten();
    let Some(mut value) = observation else {
        return Ok(json!({"status":"no_check_yet"}));
    };
    if let Some(map) = value.as_object_mut() {
        map.remove("review");
        map.remove("review_run_id");
        map.remove("review_revision");
    }
    let orders = value["orders"].as_array().cloned().unwrap_or_default();
    value["blocked_orders"] = json!(
        orders
            .iter()
            .filter(|o| o["approval_reason"] == "exceeds_approval_limit")
            .collect::<Vec<_>>()
    );
    value["ready_for_manual_approval"] = json!(
        orders
            .iter()
            .filter(|o| o["status"] == "draft" && o["approval_reason"] != "exceeds_approval_limit")
            .collect::<Vec<_>>()
    );
    let attention = value["attention"].as_array().cloned().unwrap_or_default();
    value["requires_setup"] = json!(
        attention
            .iter()
            .filter(|a| a["reason"] != "covered_by_open_order")
            .collect::<Vec<_>>()
    );
    value["already_covered_no_action"] = json!(
        attention
            .iter()
            .filter(|a| a["reason"] == "covered_by_open_order")
            .collect::<Vec<_>>()
    );
    value["interpretation"] = json!(
        "Use ready_for_manual_approval for ordinary approval requests, blocked_orders for orders that cannot be approved under policy, requires_setup for missing configuration. Already-covered stock needs no additional order. Do not merge these categories."
    );
    Ok(value)
}

async fn order_json(
    tx: &mut Transaction<'_, Postgres>,
    workspace: &str,
    id: Uuid,
    include_events: bool,
) -> Result<Value> {
    let record = sqlx::query_scalar::<_, Value>("SELECT jsonb_build_object('id',o.id,'number',o.number,'status',o.status,'vendor',COALESCE(o.vendor_snapshot->>'name',v.name),'vendor_id',o.vendor_id,'vendor_current_name',v.name,'vendor_contact',CASE WHEN o.vendor_snapshot IS NOT NULL THEN o.vendor_snapshot||jsonb_build_object('source',v.source,'snapshot',true) ELSE jsonb_build_object('contact_name',v.contact_name,'email',v.email,'phone',v.phone,'source',v.source,'snapshot',false) END,'approval_kind',o.approval_kind,'approval_reason',o.approval_reason,'subtotal',o.subtotal::text,'currency',o.currency,'line_count',o.line_count,'attention',o.attention,'revision',o.revision,'version',o.version,'prepared_by',o.prepared_by,'decided_at',o.decided_at,'decided_by',o.decided_by,'decision_note',o.decision_note,'created_at',o.created_at,'updated_at',o.updated_at,'requires_approval',o.status='draft') FROM purchase_orders o JOIN vendors v ON v.workspace_id=o.workspace_id AND v.id=o.vendor_id WHERE o.workspace_id=$1 AND o.id=$2")
        .bind(workspace).bind(id).fetch_optional(&mut **tx).await?.ok_or(Error::NotFound)?;
    let mut record = record;
    record["approval_explanation"] = json!(describe_reason(
        record["approval_reason"].as_str().unwrap_or("")
    ));
    if include_events {
        let events = sqlx::query_scalar::<_, Value>("SELECT COALESCE(jsonb_agg(jsonb_build_object('id',e.id,'type',e.event_type,'actor',e.actor,'at',e.created_at,'payload',e.payload - 'lines') ORDER BY e.id DESC),'[]'::jsonb) FROM (SELECT * FROM purchase_order_events WHERE purchase_order_id=$1 AND workspace_id=$2 ORDER BY id DESC LIMIT 20) e")
            .bind(id).bind(workspace).fetch_one(&mut **tx).await?;
        record["events"] = events;
    }
    Ok(record)
}

pub async fn order_lines(
    tx: &mut Transaction<'_, Postgres>,
    workspace: &str,
    id: Uuid,
    limit: i64,
    offset: i64,
) -> Result<(i64, Vec<Value>)> {
    let total: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM purchase_order_lines WHERE purchase_order_id=$1 AND workspace_id=$2",
    )
    .bind(id)
    .bind(workspace)
    .fetch_one(&mut **tx)
    .await?;
    let items = sqlx::query_scalar::<_, Value>("SELECT jsonb_build_object('id',id,'item_id',item_id,'name',item_name,'supplier_reference',supplier_reference,'order_unit',order_unit,'units_per_pack',units_per_pack::text,'quantity_packs',quantity_packs::text,'quantity_units',quantity_units::text,'pack_price',pack_price::text,'line_total',line_total::text,'current_balance',current_balance::text,'par_level',par_level::text,'reorder_target',reorder_target::text,'on_order_units',on_order_units::text) FROM purchase_order_lines WHERE purchase_order_id=$1 AND workspace_id=$2 ORDER BY position LIMIT $3 OFFSET $4")
        .bind(id).bind(workspace).bind(limit).bind(offset).fetch_all(&mut **tx).await?;
    Ok((total, items))
}

/// Order detail for the dashboard: record, audit events and the first page of lines.
pub async fn order_detail(
    pool: &PgPool,
    workspace: &str,
    id: Uuid,
    page: i64,
    page_size: i64,
) -> Result<Value> {
    let mut tx = pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *tx)
        .await?;
    let record = order_json(&mut tx, workspace, id, true).await?;
    let (total, _) = order_lines(&mut tx, workspace, id, 1, 0).await?;
    let pages = ((total + page_size - 1) / page_size).max(1);
    let page = page.min(pages).max(1);
    let (_, items) = order_lines(&mut tx, workspace, id, page_size, (page - 1) * page_size).await?;
    tx.commit().await?;
    Ok(
        json!({"record":record,"items":items,"total":total,"page":page,"pages":pages,"page_size":page_size}),
    )
}

/// Compact order view for the model: no contact details, at most 20 lines.
pub async fn order_for_model(pool: &PgPool, workspace: &str, id: Uuid) -> Result<Value> {
    let mut tx = pool.begin().await?;
    let mut record = order_json(&mut tx, workspace, id, false).await?;
    let (total, items) = order_lines(&mut tx, workspace, id, 20, 0).await?;
    tx.commit().await?;
    if let Some(map) = record.as_object_mut() {
        map.remove("vendor_contact");
        map.remove("vendor_id");
    }
    record["lines"] = json!(items);
    record["line_total_count"] = json!(total);
    Ok(record)
}

/// Record a human decision. Safe to repeat: an order already in the requested
/// state returns unchanged, and invalid transitions are rejected.
pub async fn decide(
    pool: &PgPool,
    workspace: &str,
    id: Uuid,
    action: &str,
    actor: &str,
    note: Option<&str>,
) -> Result<Value> {
    decide_versioned(pool, workspace, id, action, actor, note, None).await
}

pub async fn decide_versioned(
    pool: &PgPool,
    workspace: &str,
    id: Uuid,
    action: &str,
    actor: &str,
    note: Option<&str>,
    expected_version: Option<i32>,
) -> Result<Value> {
    let note = note
        .map(str::trim)
        .filter(|n| !n.is_empty())
        .map(|n| n.chars().take(500).collect::<String>());
    let mut tx = pool.begin().await?;
    let row = sqlx::query(
        "SELECT status,subtotal,version FROM purchase_orders WHERE workspace_id=$1 AND id=$2 FOR UPDATE",
    )
    .bind(workspace)
    .bind(id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(Error::NotFound)?;
    let status: String = row.try_get("status")?;
    let subtotal: Decimal = row.try_get("subtotal")?;
    if status == "draft"
        && expected_version.is_some_and(|v| Some(v) != row.try_get::<i32, _>("version").ok())
    {
        return Err(Error::Conflict(
            "This order changed; reload its details before deciding".into(),
        ));
    }
    let (next, kind, event) = match (action, status.as_str()) {
        ("approve", "draft") => ("approved", Some("manual"), Some("approved")),
        ("approve", "approved") => ("approved", None, None),
        ("reject", "draft") => ("rejected", None, Some("rejected")),
        ("reject", "rejected") => ("rejected", None, None),
        ("approve" | "reject", other) => {
            return Err(Error::Conflict(format!(
                "A {other} order cannot be {}",
                if action == "approve" {
                    "approved"
                } else {
                    "rejected"
                }
            )));
        }
        _ => return Err(Error::Invalid("Use approve or reject".into())),
    };
    let mut changed = false;
    if let Some(event) = event {
        if event == "approved" {
            let policy = policy(&mut tx, workspace).await?;
            if let Some(limit) = policy.approval_limit
                && subtotal > limit
            {
                return Err(Error::Conflict(format!(
                    "Order total NGN {subtotal} exceeds the approval limit of NGN {limit}; adjust the purchasing policy first"
                )));
            }
        }
        sqlx::query("UPDATE purchase_orders SET status=$3,approval_kind=COALESCE($4,approval_kind),decided_at=now(),decided_by=$5,decision_note=$6,version=version+1,updated_at=now(),vendor_snapshot=(SELECT jsonb_build_object('name',v.name,'contact_name',v.contact_name,'email',v.email,'phone',v.phone) FROM vendors v WHERE v.workspace_id=purchase_orders.workspace_id AND v.id=purchase_orders.vendor_id) WHERE workspace_id=$1 AND id=$2 AND status='draft'")
            .bind(workspace).bind(id).bind(next).bind(kind).bind(actor).bind(&note).execute(&mut *tx).await?;
        record_event(
            &mut tx,
            workspace,
            id,
            event,
            actor,
            json!({"kind":kind,"note":note,"subtotal":subtotal.to_string()}),
        )
        .await?;
        changed = true;
    }
    let mut record = order_json(&mut tx, workspace, id, true).await?;
    tx.commit().await?;
    record["changed"] = json!(changed);
    if changed {
        tracing::info!(order=%id, action, actor, "Purchase order decision recorded");
    }
    Ok(record)
}

/// Purchase-order PDF as a workspace-scoped artifact. One artifact per order
/// version and status: approved exports reflect the approved snapshot, and
/// later supplier edits never change them.
pub async fn export_pdf(
    pool: &PgPool,
    config: &crate::config::Config,
    workspace: &str,
    id: Uuid,
) -> Result<Value> {
    let mut tx = pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *tx)
        .await?;
    let record = order_json(&mut tx, workspace, id, true).await?;
    let (total, lines) = order_lines(&mut tx, workspace, id, MAX_CHECK_ITEMS, 0).await?;
    if total > MAX_CHECK_ITEMS {
        return Err(Error::Invalid("Order exceeds the PDF line limit".into()));
    }
    tx.commit().await?;
    let status = record["status"].as_str().unwrap_or("draft");
    let version = record["version"].as_i64().unwrap_or(1);
    use sha2::{Digest, Sha256};
    let content = json!({"id":id,"version":version,"status":status,"vendor":record["vendor"],"contact":record["vendor_contact"],"reason":record["approval_reason"],"lines":lines});
    let hash = format!(
        "po-pdf-layout2:{:x}",
        Sha256::digest(content.to_string().as_bytes())
    );
    if let Some(existing) = sqlx::query_scalar::<_, Value>("SELECT jsonb_build_object('artifact_id',id,'download_url','/v1/artifacts/'||id,'metadata',metadata,'reused',true) FROM artifacts WHERE workspace_id=$1 AND run_id IS NULL AND request_hash=$2")
        .bind(workspace).bind(&hash).fetch_optional(pool).await?
    {
        return Ok(existing);
    }
    let restaurant: String = sqlx::query_scalar("SELECT name FROM workspaces WHERE id=$1")
        .bind(workspace)
        .fetch_one(pool)
        .await?;
    let contact = &record["vendor_contact"];
    let contact_line = [
        contact["contact_name"].as_str(),
        contact["email"].as_str(),
        contact["phone"].as_str(),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>()
    .join(" · ");
    let when = |key: &str| {
        record[key]
            .as_str()
            .map(|s| s.chars().take(16).collect::<String>().replace('T', " "))
            .unwrap_or_default()
    };
    let (status_label, watermark, prepared_label) = match status {
        "draft" => (
            "DRAFT — awaiting approval",
            "Draft: not approved, not sent, not an order confirmation",
            format!("Prepared {} UTC", when("created_at")),
        ),
        "approved" => (
            if record["approval_kind"] == "automatic" {
                "APPROVED (automatic)"
            } else {
                "APPROVED"
            },
            "Approved internally; not yet sent to the vendor and not paid",
            format!(
                "Approved {} UTC by {}",
                when("decided_at"),
                record["decided_by"].as_str().unwrap_or("policy")
            ),
        ),
        "rejected" => (
            "REJECTED",
            "Rejected: no order",
            format!("Rejected {} UTC", when("decided_at")),
        ),
        _ => (
            "WITHDRAWN",
            "Withdrawn: no order",
            format!("Withdrawn {} UTC", when("decided_at")),
        ),
    };
    let details = vec![
        format!("Status: {status_label}"),
        format!(
            "Prepared by inventory agent, revision {}",
            record["revision"]
        ),
        format!(
            "Approval: {}",
            describe_reason(record["approval_reason"].as_str().unwrap_or(""))
        ),
        format!("Lines: {total}"),
    ];
    let money = |v: &Value| {
        v.as_str()
            .and_then(|s| s.parse::<Decimal>().ok())
            .map(|d| format!("{:.2}", d))
            .unwrap_or_else(|| "0.00".into())
    };
    let document = json!({
        "restaurant": restaurant, "number": record["number"], "status_label": status_label, "watermark": watermark, "prepared_label": prepared_label,
        "vendor": {"name": record["vendor"], "contact": if contact_line.is_empty() { "Contact not recorded".to_owned() } else { contact_line }},
        "details": details,
        "lines": lines.iter().map(|l| json!({"item": l["name"], "packs": format!("{} × {}", l["quantity_packs"].as_str().unwrap_or("0"), l["order_unit"].as_str().unwrap_or("")), "units": format!("{} units (on hand {}, par {})", l["quantity_units"].as_str().unwrap_or("0"), l["current_balance"].as_str().unwrap_or("0"), l["par_level"].as_str().unwrap_or("0")), "price": money(&l["pack_price"]), "total": money(&l["line_total"])})).collect::<Vec<_>>(),
        "currency": "NGN", "total": money(&record["subtotal"]),
        "notes": [
            "Quantities are rounded up to whole packs and to the vendor's minimum order; totals are pack price × packs.",
            "Sending this order to the vendor and payment are not part of this document.",
            format!("Generated by Backhaus AI from purchase order {} version {}.", record["number"].as_str().unwrap_or(""), version),
        ],
    });
    let _permit = REPORT_SLOTS
        .try_acquire()
        .map_err(|_| Error::Unavailable("Report renderer is busy; retry shortly".into()))?;
    let bytes = crate::reports::render_purchase_order_pdf(&document, &config.typst_bin).await?;
    let metadata = json!({"title":format!("Purchase order {}", record["number"].as_str().unwrap_or("")),"format":"pdf","status":status,"version":version,"order_id":id,"note":watermark});
    let artifact_id: Uuid = sqlx::query_scalar("INSERT INTO artifacts(id,workspace_id,run_id,request_hash,filename,media_type,bytes,metadata) VALUES($1,$2,NULL,$3,$4,'application/pdf',$5,$6) ON CONFLICT DO NOTHING RETURNING id")
        .bind(Uuid::new_v4()).bind(workspace).bind(&hash).bind(format!("{}-{}.pdf", record["number"].as_str().unwrap_or("purchase-order"), status)).bind(&bytes).bind(&metadata)
        .fetch_optional(pool).await?
        .unwrap_or(sqlx::query_scalar("SELECT id FROM artifacts WHERE workspace_id=$1 AND run_id IS NULL AND request_hash=$2").bind(workspace).bind(&hash).fetch_one(pool).await?);
    Ok(
        json!({"artifact_id":artifact_id,"download_url":format!("/v1/artifacts/{artifact_id}"),"metadata":metadata,"reused":false}),
    )
}
