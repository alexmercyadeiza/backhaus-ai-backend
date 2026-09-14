//! Supplier enquiries only. Rust owns recipients, quantities, send policy and deduplication.
use crate::{
    config::Config,
    error::{Error, Result},
    jobs::{self, Run},
    research, settings,
};
use serde::Deserialize;
use serde_json::{Value, json};
use sqlx::{PgPool, Row};
use uuid::Uuid;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Create {
    pub item_ids: Vec<String>,
}
pub async fn create(
    pool: &PgPool,
    workspace: &str,
    config: &Config,
    vendor: Uuid,
    input: Create,
) -> Result<Value> {
    create_enquiry(pool, workspace, config, vendor, input.item_ids, None).await
}

async fn create_enquiry(
    pool: &PgPool,
    workspace: &str,
    config: &Config,
    vendor: Uuid,
    item_ids: Vec<String>,
    researched_brief: Option<(Uuid, String)>,
) -> Result<Value> {
    let s = settings::get(pool, workspace, config).await?;
    if s["location_ready"] != true {
        return Err(Error::Conflict(
            "Set your city and country in Settings first.".into(),
        ));
    }
    if researched_brief.is_none() && (item_ids.is_empty() || item_ids.len() > 5) {
        return Err(Error::Invalid("Select one to five items.".into()));
    }
    let mut ids = item_ids;
    ids.sort();
    ids.dedup();
    let supplies_key = match &researched_brief {
        Some((run, brief)) => json!({"research_run_id":run,"request":brief}),
        None => json!(ids),
    };
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
        .bind(format!("outreach:{workspace}:{vendor}"))
        .execute(&mut *tx)
        .await?;
    let v: Value =
        sqlx::query_scalar("SELECT to_jsonb(v) FROM vendors v WHERE workspace_id=$1 AND id=$2")
            .bind(workspace)
            .bind(vendor)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(Error::NotFound)?;
    if let Some(id)=sqlx::query_scalar::<_,Uuid>("SELECT id FROM supplier_threads WHERE workspace_id=$1 AND vendor_id=$2 AND item_ids=$3 AND status IN ('draft','awaiting_reply','reply_received','reply_ready','offer_ready','whatsapp_opened','send_failed','needs_attention') ORDER BY created_at DESC LIMIT 1").bind(workspace).bind(vendor).bind(&supplies_key).fetch_optional(&mut *tx).await? {drop(tx); return detail(pool,workspace,id).await;}
    if v["research_status"] == "discarded" {
        return Err(Error::Conflict(
            "This vendor has been discarded. Shortlist it before preparing outreach.".into(),
        ));
    }
    let lines = if let Some((_, brief)) = &researched_brief {
        format!(
            "Requested supplies and requirements:\n{brief}\n\nIf quantities or specifications are not stated, please provide your available options and minimum order."
        )
    } else {
        let items:Vec<Value>=sqlx::query_scalar("SELECT jsonb_build_object('name',name,'unit',unit,'needed',greatest(par_level-current_balance,0)::text) FROM inventory_items WHERE workspace_id=$1 AND id=ANY($2) ORDER BY name").bind(workspace).bind(&ids).fetch_all(&mut *tx).await?;
        if items.len() != ids.len() {
            return Err(Error::Invalid("Choose items from your inventory.".into()));
        }
        items
            .iter()
            .map(|i| {
                format!(
                    "• {}: {} {} to replenish",
                    i["name"].as_str().unwrap_or(""),
                    i["needed"].as_str().unwrap_or("quantity to confirm"),
                    i["unit"].as_str().unwrap_or("units")
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    };
    let id = Uuid::new_v4();
    let subject = format!("Supply enquiry [BH-{id}]");
    let body = format!(
        "Hello {},\n\nI'm contacting you for {} in {}, {}. Could you provide a quotation for these supplies?\n\n{}\n\nPlease include your price, pack size, minimum order, availability, delivery cost and lead time, and payment terms.\n\nThis is a quotation request only, not an order confirmation. Thank you.",
        v["name"].as_str().unwrap_or(""),
        s["business_name"].as_str().unwrap_or("Backhaus"),
        s["city"].as_str().unwrap_or(""),
        s["country"].as_str().unwrap_or(""),
        lines
    );
    sqlx::query("INSERT INTO supplier_threads(id,workspace_id,vendor_id,item_ids,subject,initial_body,reply_to,recipient_email,recipient_phone) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)").bind(id).bind(workspace).bind(vendor).bind(&supplies_key).bind(&subject).bind(&body).bind(s["reply_to"].as_str().unwrap_or("")).bind(v["email"].as_str()).bind(v["phone"].as_str()).execute(&mut *tx).await?;
    sqlx::query("INSERT INTO supplier_messages(id,thread_id,workspace_id,direction,channel,body,subject,status) VALUES($1,$2,$3,'outbound','email',$4,$5,'draft')").bind(Uuid::new_v4()).bind(id).bind(workspace).bind(body).bind(subject).execute(&mut *tx).await?;
    tx.commit().await?;
    detail(pool, workspace, id).await
}
/// Explicit operator approval starts the initial enquiry, never a later counter-offer.
/// Only the inventory items attached to this supplier's research are used.
pub async fn approve_candidate(
    pool: &PgPool,
    workspace: &str,
    config: &Config,
    vendor: Uuid,
) -> Result<Value> {
    approve_candidate_at(
        pool,
        workspace,
        config,
        vendor,
        "https://api.resend.com/emails",
    )
    .await
}
async fn approve_candidate_at(
    pool: &PgPool,
    workspace: &str,
    config: &Config,
    vendor: Uuid,
    endpoint: &str,
) -> Result<Value> {
    let supplier: Value =
        sqlx::query_scalar("SELECT to_jsonb(v) FROM vendors v WHERE workspace_id=$1 AND id=$2")
            .bind(workspace)
            .bind(vendor)
            .fetch_optional(pool)
            .await?
            .ok_or(Error::NotFound)?;
    if supplier["research_status"] == "discarded" {
        return Err(Error::Conflict(
            "This supplier was discarded. Restore it before approving outreach.".into(),
        ));
    }
    let input:Value=sqlx::query_scalar("SELECT r.input || jsonb_build_object('research_run_id',r.id) FROM agent_runs r JOIN vendors v ON v.workspace_id=r.workspace_id AND v.id=$2 WHERE r.workspace_id=$1 AND r.kind='vendor_research' AND v.evidence @> jsonb_build_array(jsonb_build_object('run_id',r.id)) ORDER BY r.created_at DESC,r.id DESC LIMIT 1")
        .bind(workspace).bind(vendor).fetch_optional(pool).await?
        .ok_or_else(||Error::Invalid("No research supplies are linked to this vendor. Open the vendor and choose the supplies for an enquiry.".into()))?;
    let draft = if let Some(brief) = input["request"].as_str().filter(|s| !s.trim().is_empty()) {
        let research_id = input["research_run_id"]
            .as_str()
            .and_then(|s| Uuid::parse_str(s).ok())
            .ok_or(Error::NotFound)?;
        create_enquiry(
            pool,
            workspace,
            config,
            vendor,
            Vec::new(),
            Some((research_id, brief.to_owned())),
        )
        .await?
    } else {
        let requested: Vec<String> = input["items"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|i| i["id"].as_str().map(str::to_owned))
            .collect();
        let ids:Vec<String>=sqlx::query_scalar("SELECT id FROM inventory_items WHERE workspace_id=$1 AND id=ANY($2) AND par_level IS NOT NULL AND current_balance<par_level ORDER BY id")
        .bind(workspace).bind(&requested).fetch_all(pool).await?;
        if ids.is_empty() {
            return Err(Error::Conflict(
                "These researched supplies are no longer below par. No enquiry was sent.".into(),
            ));
        }
        create(pool, workspace, config, vendor, Create { item_ids: ids }).await?
    };
    let thread: Uuid = draft["thread"]["id"]
        .as_str()
        .ok_or(Error::NotFound)?
        .parse()
        .map_err(|_| Error::NotFound)?;
    let approved=sqlx::query("UPDATE vendors SET research_status='shortlisted',version=version+1,updated_at=now() WHERE workspace_id=$1 AND id=$2 AND research_status<>'discarded'").bind(workspace).bind(vendor).execute(pool).await?;
    if approved.rows_affected() == 0 {
        return Err(Error::Conflict(
            "This supplier was discarded before approval completed.".into(),
        ));
    }
    // Select the original message, so repeat approval cannot send a later negotiation draft.
    let first:Uuid=sqlx::query_scalar("SELECT id FROM supplier_messages WHERE workspace_id=$1 AND thread_id=$2 AND direction='outbound' AND channel='email' ORDER BY created_at,id LIMIT 1")
        .bind(workspace).bind(thread).fetch_one(pool).await?;
    let email = draft["thread"]["recipient_email"]
        .as_str()
        .unwrap_or("")
        .trim();
    if email.is_empty() {
        let phone = draft["thread"]["recipient_phone"].as_str().unwrap_or("");
        return Ok(
            json!({"status":if phone.is_empty(){"needs_contact"}else{"whatsapp_ready"},"thread_id":thread}),
        );
    }
    match send_email_at(pool, workspace, config, thread, Some(first), endpoint).await {
        Ok(sent) => Ok(json!({"status":"sent","thread_id":thread,"reused":sent["reused"]==true})),
        Err(error) => {
            // Preflight failures (for example missing sender configuration) also stay visible.
            let mut tx = pool.begin().await?;
            let failed=sqlx::query("UPDATE supplier_messages SET status='failed',error=$3,updated_at=now() WHERE workspace_id=$1 AND id=$2 AND status IN ('draft','failed')")
                .bind(workspace).bind(first).bind(error.to_string()).execute(&mut *tx).await?;
            if failed.rows_affected() > 0 {
                sqlx::query("UPDATE supplier_threads SET status='send_failed',updated_at=now() WHERE workspace_id=$1 AND id=$2 AND status<>'discarded'").bind(workspace).bind(thread).execute(&mut *tx).await?;
            }
            tx.commit().await?;
            Ok(json!({"status":"send_failed","thread_id":thread,"error":error.to_string()}))
        }
    }
}

pub async fn detail(pool: &PgPool, workspace: &str, id: Uuid) -> Result<Value> {
    let thread:Value=sqlx::query_scalar("SELECT to_jsonb(t)||jsonb_build_object('vendor',v.name,'evidence',v.evidence,'website',v.website,'recipient_email',CASE WHEN EXISTS(SELECT 1 FROM supplier_messages m WHERE m.thread_id=t.id AND m.first_attempt_at IS NOT NULL) THEN t.recipient_email ELSE v.email END,'recipient_phone',CASE WHEN EXISTS(SELECT 1 FROM supplier_messages m WHERE m.thread_id=t.id AND m.first_attempt_at IS NOT NULL) THEN t.recipient_phone ELSE v.phone END,'reply_to',CASE WHEN EXISTS(SELECT 1 FROM supplier_messages m WHERE m.thread_id=t.id AND m.first_attempt_at IS NOT NULL) THEN t.reply_to ELSE coalesce(s.reply_to,'') END) FROM supplier_threads t JOIN vendors v ON v.workspace_id=t.workspace_id AND v.id=t.vendor_id LEFT JOIN business_settings s ON s.workspace_id=t.workspace_id WHERE t.workspace_id=$1 AND t.id=$2").bind(workspace).bind(id).fetch_optional(pool).await?.ok_or(Error::NotFound)?;
    let messages:Vec<Value>=sqlx::query_scalar("SELECT to_jsonb(m)-'send_payload' FROM supplier_messages m WHERE workspace_id=$1 AND thread_id=$2 ORDER BY created_at,id LIMIT 100").bind(workspace).bind(id).fetch_all(pool).await?;
    Ok(json!({"thread":thread,"messages":messages}))
}
pub async fn tasks(pool: &PgPool, workspace: &str, config: &Config) -> Result<Value> {
    let threads:Vec<Value>=sqlx::query_scalar("SELECT jsonb_build_object('id',t.id,'vendor_id',v.id,'vendor',v.name,'status',t.status,'updated_at',t.updated_at,'subject',t.subject) FROM supplier_threads t JOIN vendors v ON v.workspace_id=t.workspace_id AND v.id=t.vendor_id WHERE t.workspace_id=$1 ORDER BY t.updated_at DESC LIMIT 20").bind(workspace).fetch_all(pool).await?;
    let vendors:Vec<Value>=sqlx::query_scalar("SELECT jsonb_build_object('id',id,'name',name,'email',email,'phone',phone,'website',website,'evidence',evidence,'source',source,'vetting',vetting,'research_status',research_status) FROM vendors WHERE workspace_id=$1 ORDER BY (source='web_research') DESC,updated_at DESC,id").bind(workspace).fetch_all(pool).await?;
    Ok(
        json!({"settings":settings::get(pool,workspace,config).await?,"threads":threads,"vendors":vendors,"research":research::latest(pool,workspace).await?}),
    )
}
/// Require an explicitly international number; never guess its country code.
pub fn whatsapp_url(phone: &str, body: &str) -> Result<String> {
    let digits = phone
        .chars()
        .filter(char::is_ascii_digit)
        .collect::<String>();
    if !phone.trim().starts_with('+') || !(8..=15).contains(&digits.len()) {
        return Err(Error::Invalid(
            "Save this vendor's phone with its international country code, starting with +.".into(),
        ));
    }
    let mut url = reqwest::Url::parse(&format!("https://wa.me/{digits}")).expect("constant URL");
    url.query_pairs_mut().append_pair("text", body);
    Ok(url.to_string())
}
pub async fn whatsapp(pool: &PgPool, workspace: &str, id: Uuid) -> Result<Value> {
    let d = detail(pool, workspace, id).await?;
    if d["thread"]["status"] == "discarded" {
        return Err(Error::Conflict("This enquiry was discarded.".into()));
    }
    let body = d["messages"]
        .as_array()
        .and_then(|a| {
            a.iter()
                .rev()
                .find(|m| m["direction"] == "outbound" && m["status"] != "superseded")
        })
        .and_then(|m| m["body"].as_str())
        .unwrap_or(d["thread"]["initial_body"].as_str().unwrap_or(""));
    let url = whatsapp_url(d["thread"]["recipient_phone"].as_str().unwrap_or(""), body)?;
    // Opening a composer is not evidence of sending. Never change status to sent.
    sqlx::query("UPDATE supplier_threads SET status=CASE WHEN status='draft' THEN 'whatsapp_opened' ELSE status END,updated_at=now() WHERE workspace_id=$1 AND id=$2").bind(workspace).bind(id).execute(pool).await?;
    Ok(json!({"url":url,"status":"whatsapp_opened"}))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PasteReply {
    pub body: String,
    pub request_id: Uuid,
}
pub async fn paste_reply(
    pool: &PgPool,
    workspace: &str,
    thread: Uuid,
    input: PasteReply,
) -> Result<Value> {
    if input.body.trim().is_empty() || input.body.len() > 12000 {
        return Err(Error::Invalid(
            "Enter a supplier reply up to 12,000 characters.".into(),
        ));
    }
    let d = detail(pool, workspace, thread).await?;
    if d["thread"]["status"] == "discarded" {
        return Err(Error::Conflict("This enquiry was discarded.".into()));
    }
    ingest(
        pool,
        workspace,
        thread,
        "whatsapp",
        &input.body,
        d["thread"]["subject"].as_str().unwrap_or(""),
        &format!("paste:{}", input.request_id),
        None,
    )
    .await
}
#[allow(clippy::too_many_arguments)]
async fn ingest(
    pool: &PgPool,
    workspace: &str,
    thread: Uuid,
    channel: &str,
    body: &str,
    subject: &str,
    provider: &str,
    internet: Option<&str>,
) -> Result<Value> {
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
        .bind(format!("send:{thread}"))
        .execute(&mut *tx)
        .await?;
    let state: Option<String> = sqlx::query_scalar(
        "SELECT status FROM supplier_threads WHERE workspace_id=$1 AND id=$2 FOR UPDATE",
    )
    .bind(workspace)
    .bind(thread)
    .fetch_optional(&mut *tx)
    .await?;
    if state.ok_or(Error::NotFound)? == "discarded" {
        return Err(Error::Conflict("This enquiry was discarded.".into()));
    }

    let id = Uuid::new_v4();
    let inserted=sqlx::query("INSERT INTO supplier_messages(id,thread_id,workspace_id,direction,channel,body,subject,status,provider_id,internet_id,received_at) VALUES($1,$2,$3,'inbound',$4,$5,$6,'received',$7,$8,now()) ON CONFLICT(provider_id) DO NOTHING")
        .bind(id).bind(thread).bind(workspace).bind(channel).bind(body).bind(subject).bind(provider).bind(internet).execute(&mut *tx).await?.rows_affected();
    if inserted == 0 {
        return Ok(json!({"reused":true}));
    }
    // A new response makes any unsent automatic question stale, even while its
    // review is queued. Keep sent/attempted messages intact for their audit trail.
    sqlx::query("UPDATE supplier_messages SET status='superseded',updated_at=now() WHERE thread_id=$1 AND source_message_id IS NOT NULL AND status='review_pending' AND first_attempt_at IS NULL")
        .bind(thread).execute(&mut *tx).await?;
    let run = jobs::enqueue_system(
        &mut tx,
        workspace,
        "supplier_reply",
        &format!("supplier-reply:{id}"),
        json!({"message_id":id,"thread_id":thread}),
    )
    .await?;
    sqlx::query("UPDATE supplier_threads SET status='reply_received',updated_at=now() WHERE workspace_id=$1 AND id=$2").bind(workspace).bind(thread).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(json!({"message_id":id,"run_id":run}))
}
pub async fn reply_context(pool: &PgPool, run: &Run) -> Result<Value> {
    let id = run.input["message_id"]
        .as_str()
        .and_then(|s| Uuid::parse_str(s).ok())
        .ok_or(Error::NotFound)?;
    let mut context:Value=sqlx::query_scalar("SELECT jsonb_build_object('reply',m.body,'request',t.initial_body,'channel',m.channel,'vendor',v.name) FROM supplier_messages m JOIN supplier_threads t ON t.id=m.thread_id AND t.workspace_id=m.workspace_id JOIN vendors v ON v.id=t.vendor_id AND v.workspace_id=t.workspace_id WHERE m.workspace_id=$1 AND m.id=$2 AND m.direction='inbound'").bind(&run.workspace).bind(id).fetch_optional(pool).await?.ok_or(Error::NotFound)?;
    let history:Vec<Value>=sqlx::query_scalar("SELECT jsonb_build_object('direction',h.direction,'body',left(h.body,1800),'review',h.review) FROM supplier_messages h JOIN supplier_messages current ON current.thread_id=h.thread_id AND current.workspace_id=h.workspace_id WHERE current.workspace_id=$1 AND current.id=$2 AND h.id<>current.id AND h.created_at<=current.created_at AND h.status NOT IN ('superseded','discarded') ORDER BY h.created_at DESC,h.id DESC LIMIT 6").bind(&run.workspace).bind(id).fetch_all(pool).await?;
    context["recent_history"] = json!(history.into_iter().rev().collect::<Vec<_>>());
    Ok(context)
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Review {
    pub summary: String,
    pub missing_fields: Vec<String>,
    pub needs_person: bool,
}
pub async fn review(pool: &PgPool, run: &Run, input: Review) -> Result<Value> {
    const FIELDS: &[&str] = &[
        "price",
        "pack_size",
        "minimum_order",
        "availability",
        "delivery",
        "payment_terms",
    ];
    if input.summary.trim().is_empty()
        || input.summary.len() > 1200
        || input.missing_fields.len() > 6
        || input
            .missing_fields
            .iter()
            .any(|v| !FIELDS.contains(&v.as_str()))
    {
        return Err(Error::Invalid(
            "Provide a short review and only the supported missing fields.".into(),
        ));
    }
    let id = run.input["message_id"]
        .as_str()
        .and_then(|s| Uuid::parse_str(s).ok())
        .ok_or(Error::NotFound)?;
    let mut tx = pool.begin().await?;
    let thread_id = run.input["thread_id"]
        .as_str()
        .and_then(|v| Uuid::parse_str(v).ok())
        .ok_or(Error::NotFound)?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
        .bind(format!("send:{thread_id}"))
        .execute(&mut *tx)
        .await?;

    let row=sqlx::query("SELECT * FROM supplier_messages WHERE workspace_id=$1 AND id=$2 AND direction='inbound' FOR UPDATE").bind(&run.workspace).bind(id).fetch_optional(&mut *tx).await?.ok_or(Error::NotFound)?;
    let thread: Uuid = row.try_get("thread_id")?;
    if thread != thread_id {
        return Err(Error::NotFound);
    }
    let discarded: bool = sqlx::query_scalar(
        "SELECT status='discarded' FROM supplier_threads WHERE id=$1 FOR UPDATE",
    )
    .bind(thread)
    .fetch_one(&mut *tx)
    .await?;
    if discarded {
        return Err(Error::Conflict("This enquiry was discarded.".into()));
    }
    let existing: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM supplier_messages WHERE source_message_id=$1)",
    )
    .bind(id)
    .fetch_one(&mut *tx)
    .await?;
    if existing || row.try_get::<Option<String>, _>("review")?.is_some() {
        return Ok(json!({"reviewed":true,"reused":true}));
    }
    sqlx::query(
        "UPDATE supplier_messages SET review=$2,status='reviewed',updated_at=now() WHERE id=$1",
    )
    .bind(id)
    .bind(&input.summary)
    .execute(&mut *tx)
    .await?;
    // A later supplier response may already be queued for review. Do not let an
    // older model run overwrite its progress or send questions it has answered.
    let newer: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM supplier_messages WHERE thread_id=$1 AND direction='inbound' AND (created_at,id)>($2,$3))")
        .bind(thread).bind(row.try_get::<chrono::DateTime<chrono::Utc>, _>("created_at")?).bind(id).fetch_one(&mut *tx).await?;
    if newer {
        tx.commit().await?;
        return Ok(json!({"reviewed":true,"superseded":true,"reply_prepared":false}));
    }
    sqlx::query("UPDATE supplier_messages SET status='superseded',updated_at=now() WHERE thread_id=$1 AND source_message_id IS NOT NULL AND status='review_pending' AND first_attempt_at IS NULL")
        .bind(thread).execute(&mut *tx).await?;
    let previous:i64=sqlx::query_scalar("SELECT count(*) FROM supplier_messages WHERE thread_id=$1 AND source_message_id IS NOT NULL").bind(thread).fetch_one(&mut *tx).await?;
    let operator_draft:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM supplier_messages WHERE thread_id=$1 AND direction='outbound' AND source_message_id IS NULL AND status='draft' AND created_at>$2)").bind(thread).bind(row.try_get::<chrono::DateTime<chrono::Utc>,_>("created_at")?).fetch_one(&mut *tx).await?;
    let complete = input.missing_fields.is_empty();
    let needs_person = input.needs_person || (!complete && previous >= 3) || operator_draft;
    if !needs_person && !complete {
        let missing = input
            .missing_fields
            .iter()
            .map(|s| s.replace('_', " "))
            .collect::<Vec<_>>()
            .join(", ");
        let body = format!(
            "Thank you for your reply. Could you also confirm the {missing}? Our purchasing team will review the complete quotation before making a decision. This is not an order or acceptance of terms."
        );
        sqlx::query("INSERT INTO supplier_messages(id,thread_id,workspace_id,direction,channel,body,subject,status,source_message_id,review_run_id,reply_to_id) VALUES($1,$2,$3,'outbound',$4,$5,$6,'review_pending',$7,$8,$9)").bind(Uuid::new_v4()).bind(thread).bind(&run.workspace).bind(row.try_get::<String,_>("channel")?).bind(body).bind(row.try_get::<String,_>("subject")?).bind(id).bind(run.id).bind(row.try_get::<Option<String>,_>("internet_id")?).execute(&mut *tx).await?;
    }
    sqlx::query("UPDATE supplier_threads SET status=$2,updated_at=now() WHERE id=$1")
        .bind(thread)
        .bind(if needs_person {
            "needs_attention"
        } else if complete {
            "offer_ready"
        } else {
            "reply_ready"
        })
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(
        json!({"reviewed":true,"needs_person":needs_person,"reply_prepared":!needs_person && !complete,"offer_ready":!needs_person && complete}),
    )
}
/// Freeze the exact provider payload before the first attempt. Retries use its ID for 23h.
pub async fn send_email(
    pool: &PgPool,
    workspace: &str,
    config: &Config,
    thread: Uuid,
    message: Option<Uuid>,
) -> Result<Value> {
    send_email_at(
        pool,
        workspace,
        config,
        thread,
        message,
        "https://api.resend.com/emails",
    )
    .await
}
async fn send_email_at(
    pool: &PgPool,
    workspace: &str,
    config: &Config,
    thread: Uuid,
    message: Option<Uuid>,
    endpoint: &str,
) -> Result<Value> {
    let key = config
        .resend_key
        .as_ref()
        .ok_or_else(|| Error::Unavailable("Email is not configured.".into()))?;
    let from = config
        .resend_from
        .as_ref()
        .ok_or_else(|| Error::Unavailable("Email sender is not configured.".into()))?;
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
        .bind(format!("send:{thread}"))
        .execute(&mut *tx)
        .await?;
    sqlx::query("UPDATE supplier_threads t SET recipient_email=v.email,recipient_phone=v.phone,reply_to=coalesce(s.reply_to,'') FROM vendors v LEFT JOIN business_settings s ON s.workspace_id=v.workspace_id WHERE t.workspace_id=$1 AND t.id=$2 AND v.workspace_id=t.workspace_id AND v.id=t.vendor_id AND NOT EXISTS(SELECT 1 FROM supplier_messages m WHERE m.thread_id=t.id AND m.first_attempt_at IS NOT NULL)").bind(workspace).bind(thread).execute(&mut *tx).await?;
    let row=sqlx::query("SELECT m.*,t.recipient_email,t.reply_to FROM supplier_messages m JOIN supplier_threads t ON t.id=m.thread_id WHERE m.workspace_id=$1 AND m.thread_id=$2 AND m.direction='outbound' AND m.channel='email' AND ($3::uuid IS NULL OR m.id=$3) ORDER BY m.created_at DESC LIMIT 1 FOR UPDATE OF m").bind(workspace).bind(thread).bind(message).fetch_optional(&mut *tx).await?.ok_or(Error::NotFound)?;
    let discarded: bool = sqlx::query_scalar(
        "SELECT status='discarded' FROM supplier_threads WHERE id=$1 FOR UPDATE",
    )
    .bind(thread)
    .fetch_one(&mut *tx)
    .await?;
    if discarded {
        return Err(Error::Conflict("This enquiry was discarded.".into()));
    }
    let id: Uuid = row.try_get("id")?;
    let status: String = row.try_get("status")?;
    if matches!(status.as_str(), "discarded" | "superseded") {
        return Err(Error::Conflict("This message is no longer active.".into()));
    }
    if status == "sent" {
        return Ok(json!({"status":"sent","reused":true}));
    }
    if status == "sending"
        && row.try_get::<chrono::DateTime<chrono::Utc>, _>("updated_at")?
            > chrono::Utc::now() - chrono::Duration::seconds(90)
    {
        return Err(Error::Conflict(
            "This message is already being sent.".into(),
        ));
    }
    let reviewed: Option<Uuid> = row.try_get("review_run_id")?;
    if let Some(run) = reviewed {
        let enabled: bool = sqlx::query_scalar("SELECT coalesce((SELECT enabled FROM scoped_agents WHERE workspace_id=$1 AND role='procurement'),false)").bind(workspace).fetch_one(&mut *tx).await?;
        if !enabled {
            return Err(Error::Conflict("Procurement agent is paused.".into()));
        }
        let complete: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM agent_runs WHERE id=$1 AND status='completed')",
        )
        .bind(run)
        .fetch_one(&mut *tx)
        .await?;
        if !complete {
            return Err(Error::Conflict(
                "The agent review must finish before sending. If it failed or was stopped, write a new reply draft.".into(),
            ));
        }
    }
    let email: Option<String> = row.try_get("recipient_email")?;
    let email = email
        .filter(|e| {
            e.contains('@')
                && !e.to_lowercase().ends_with(".invalid")
                && !e.contains(['\r', '\n', '<', '>'])
        })
        .ok_or_else(|| {
            Error::Invalid(
                "This vendor needs a real email address. Demo contacts cannot receive mail.".into(),
            )
        })?;
    if let Some(first) =
        row.try_get::<Option<chrono::DateTime<chrono::Utc>>, _>("first_attempt_at")?
        && first < chrono::Utc::now() - chrono::Duration::hours(23)
    {
        return Err(Error::Conflict("The send result needs manual verification in Resend; its safe retry window has closed.".into()));
    }
    let payload = if let Some(payload) = row.try_get::<Option<Value>, _>("send_payload")? {
        payload
    } else {
        let mut p = json!({"from":from,"to":[email],"subject":row.try_get::<String,_>("subject")?,"text":row.try_get::<String,_>("body")?});
        let reply: String = row.try_get("reply_to")?;
        if !reply.is_empty() {
            p["reply_to"] = json!(reply);
        }
        if let Some(internet) = row.try_get::<Option<String>, _>("reply_to_id")? {
            p["headers"] = json!({"In-Reply-To":internet});
        }
        p
    };
    sqlx::query("UPDATE supplier_messages SET status='sending',send_payload=$2,first_attempt_at=coalesce(first_attempt_at,now()),updated_at=now(),error=NULL WHERE id=$1").bind(id).bind(&payload).execute(&mut *tx).await?;
    tx.commit().await?;
    let result=async {
        let response=research::client().post(endpoint).timeout(std::time::Duration::from_secs(10)).bearer_auth(key).header("Idempotency-Key",format!("supplier-{id}")).json(&payload).send().await.map_err(|_|Error::Unavailable("Email connection interrupted. Retry this same message safely.".into()))?;
        let value=research::response_json(response).await?;let provider=value["id"].as_str().ok_or_else(||Error::Unavailable("Email provider returned no message ID.".into()))?;
        let mut tx=pool.begin().await?;
        sqlx::query("UPDATE supplier_messages SET status='sent',provider_id=$2,sent_at=now(),updated_at=now(),error=NULL WHERE id=$1").bind(id).bind(provider).execute(&mut *tx).await?;
        sqlx::query("UPDATE supplier_threads SET status='awaiting_reply',updated_at=now() WHERE id=$1").bind(thread).execute(&mut *tx).await?;tx.commit().await?;Ok::<_,Error>(json!({"status":"sent","message_id":id}))
    }.await;
    if let Err(e) = &result {
        sqlx::query("UPDATE supplier_threads SET status='send_failed',updated_at=now() WHERE id=$1 AND status<>'discarded'").bind(thread).execute(pool).await?;
        sqlx::query("UPDATE supplier_messages SET status='failed',error=$2,updated_at=now() WHERE id=$1 AND status='sending'").bind(id).bind(e.to_string()).execute(pool).await?;
    }
    result
}
fn sender_address(value: &str) -> String {
    value
        .rsplit_once('<')
        .map(|(_, s)| s.trim_end_matches('>'))
        .unwrap_or(value)
        .trim()
        .to_lowercase()
}
/// Scan metadata first; retrieve bodies only for our recipient, supplier and unique thread tag.
pub async fn sync(pool: &PgPool, workspace: &str, config: &Config) -> Result<Value> {
    sync_at(pool, workspace, config, "https://api.resend.com").await
}
async fn sync_at(pool: &PgPool, workspace: &str, config: &Config, endpoint: &str) -> Result<Value> {
    let key = config
        .resend_key
        .as_ref()
        .ok_or_else(|| Error::Unavailable("Email is not configured.".into()))?;
    let threads:Vec<Value>=sqlx::query_scalar("SELECT to_jsonb(t) FROM supplier_threads t WHERE workspace_id=$1 AND status<>'discarded' AND reply_to<>'' AND EXISTS(SELECT 1 FROM supplier_messages m WHERE m.thread_id=t.id AND m.channel='email' AND m.status='sent') ORDER BY updated_at DESC LIMIT 100").bind(workspace).fetch_all(pool).await?;
    if threads.is_empty() {
        return Ok(json!({"received":0}));
    }
    let response = research::client()
        .get(format!("{endpoint}/emails/receiving"))
        .query(&[("limit", "100")])
        .bearer_auth(key)
        .send()
        .await
        .map_err(|_| Error::Unavailable("Could not check replies.".into()))?;
    let list = research::response_json(response).await?;
    let mut received = 0;
    for m in list["data"].as_array().into_iter().flatten() {
        let Some(thread) = threads.iter().find(|t| {
            m["subject"]
                .as_str()
                .unwrap_or("")
                .contains(&format!("[BH-{}]", t["id"].as_str().unwrap_or("")))
                && sender_address(m["from"].as_str().unwrap_or(""))
                    == t["recipient_email"].as_str().unwrap_or("").to_lowercase()
                && m["to"].as_array().is_some_and(|a| {
                    a.iter().any(|v| {
                        v.as_str().is_some_and(|v| {
                            v.eq_ignore_ascii_case(t["reply_to"].as_str().unwrap_or(""))
                        })
                    })
                })
        }) else {
            continue;
        };
        let Some(id) = m["id"].as_str() else {
            continue;
        };
        if sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM supplier_messages WHERE provider_id=$1)",
        )
        .bind(format!("received:{id}"))
        .fetch_one(pool)
        .await?
        {
            continue;
        }
        let url = format!(
            "{endpoint}/emails/receiving/{}",
            Uuid::parse_str(id)
                .map_err(|_| Error::Unavailable("Invalid received message identifier.".into()))?
        );
        let response = research::client()
            .get(url)
            .bearer_auth(key)
            .send()
            .await
            .map_err(|_| Error::Unavailable("Could not retrieve a supplier reply.".into()))?;
        let email = research::response_json(response).await?;
        let headers = &email["headers"];
        if headers.as_object().is_some_and(|h| {
            h.iter().any(|(k, v)| {
                (k.eq_ignore_ascii_case("auto-submitted") && v.as_str() != Some("no"))
                    || (k.eq_ignore_ascii_case("precedence")
                        && matches!(v.as_str(), Some("bulk" | "list" | "junk")))
            })
        }) {
            continue;
        }
        let body = email["text"]
            .as_str()
            .unwrap_or("")
            .chars()
            .take(12000)
            .collect::<String>();
        if body.trim().is_empty() {
            continue;
        }
        let thread =
            Uuid::parse_str(thread["id"].as_str().unwrap_or("")).map_err(|_| Error::NotFound)?;
        let result = ingest(
            pool,
            workspace,
            thread,
            "email",
            &body,
            m["subject"].as_str().unwrap_or(""),
            &format!("received:{id}"),
            email["message_id"].as_str(),
        )
        .await?;
        if result["reused"] != true {
            received += 1;
        }
    }
    Ok(json!({"received":received}))
}
pub async fn process_replies(pool: &PgPool, workspace: &str, config: &Config) -> Result<()> {
    let enabled:bool=sqlx::query_scalar("SELECT coalesce((SELECT enabled FROM scoped_agents WHERE workspace_id=$1 AND role='procurement'),false) ").bind(workspace).fetch_one(pool).await?;
    if !enabled {
        return Ok(());
    }
    let _ = sync(pool, workspace, config).await?;
    let auto_reply: bool = sqlx::query_scalar(
        "SELECT coalesce((SELECT auto_reply FROM business_settings WHERE workspace_id=$1),false)",
    )
    .bind(workspace)
    .fetch_one(pool)
    .await?;
    if !auto_reply {
        return Ok(());
    }
    let rows=sqlx::query("SELECT m.id,m.thread_id FROM supplier_messages m JOIN agent_runs r ON r.id=m.review_run_id WHERE m.workspace_id=$1 AND m.status='review_pending' AND m.channel='email' AND r.status='completed' ORDER BY m.created_at LIMIT 3").bind(workspace).fetch_all(pool).await?;
    for row in rows {
        if let Err(error) = send_email(
            pool,
            workspace,
            config,
            row.try_get("thread_id")?,
            Some(row.try_get("id")?),
        )
        .await
        {
            tracing::warn!(%error,"Supplier follow-up remains available for manual review");
        }
    }
    Ok(())
}
pub async fn discard(pool: &PgPool, workspace: &str, thread: Uuid) -> Result<Value> {
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
        .bind(format!("send:{thread}"))
        .execute(&mut *tx)
        .await?;
    let busy:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM supplier_messages WHERE thread_id=$1 AND workspace_id=$2 AND status='sending')").bind(thread).bind(workspace).fetch_one(&mut *tx).await?;
    if busy {
        return Err(Error::Conflict(
            "A message is being sent. Wait for its result before discarding.".into(),
        ));
    }
    let changed=sqlx::query("UPDATE supplier_threads SET status='discarded',updated_at=now() WHERE workspace_id=$1 AND id=$2").bind(workspace).bind(thread).execute(&mut *tx).await?.rows_affected();
    if changed == 0 {
        return Err(Error::NotFound);
    }
    sqlx::query("UPDATE supplier_messages SET status='discarded',updated_at=now() WHERE thread_id=$1 AND direction='outbound' AND status IN ('draft','review_pending','failed')").bind(thread).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(json!({"status":"discarded"}))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateAction {
    pub status: String,
}
pub async fn candidate_action(
    pool: &PgPool,
    workspace: &str,
    id: Uuid,
    input: CandidateAction,
) -> Result<Value> {
    if !matches!(input.status.as_str(), "shortlisted" | "discarded") {
        return Err(Error::Invalid("Choose shortlist or discard.".into()));
    }
    // Discarding a vendor with an active enquiry must also stop future replies.
    if input.status == "discarded" {
        let threads:Vec<Uuid>=sqlx::query_scalar("SELECT id FROM supplier_threads WHERE workspace_id=$1 AND vendor_id=$2 AND status<>'discarded'").bind(workspace).bind(id).fetch_all(pool).await?;
        for thread in threads {
            discard(pool, workspace, thread).await?;
        }
    }
    let result=sqlx::query("UPDATE vendors SET research_status=$3,version=version+1,updated_at=now() WHERE workspace_id=$1 AND id=$2").bind(workspace).bind(id).bind(&input.status).execute(pool).await?;
    if result.rows_affected() == 0 {
        return Err(Error::NotFound);
    }
    Ok(json!({"status":input.status}))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplyDraft {
    pub body: String,
    pub request_id: Uuid,
}
/// An operator-written reply/counter-offer is always a draft until the send button is used.
pub async fn draft_reply(
    pool: &PgPool,
    workspace: &str,
    thread: Uuid,
    input: ReplyDraft,
) -> Result<Value> {
    if input.body.trim().is_empty() || input.body.len() > 5000 {
        return Err(Error::Invalid(
            "Enter a reply up to 5,000 characters.".into(),
        ));
    }
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
        .bind(format!("send:{thread}"))
        .execute(&mut *tx)
        .await?;
    let t = sqlx::query(
        "SELECT subject,status FROM supplier_threads WHERE workspace_id=$1 AND id=$2 FOR UPDATE",
    )
    .bind(workspace)
    .bind(thread)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(Error::NotFound)?;
    if t.try_get::<String, _>("status")? == "discarded" {
        return Err(Error::Conflict("This enquiry was discarded.".into()));
    }
    let existing: Option<(Uuid, String)> = sqlx::query_as(
        "SELECT thread_id,body FROM supplier_messages WHERE id=$1 AND workspace_id=$2",
    )
    .bind(input.request_id)
    .bind(workspace)
    .fetch_optional(&mut *tx)
    .await?;
    if let Some((t, b)) = existing {
        if t != thread || b != input.body.trim() {
            return Err(Error::Conflict("Draft identifier already used.".into()));
        }
        return Ok(json!({"saved":true,"reused":true}));
    }
    let sending: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM supplier_messages WHERE thread_id=$1 AND status='sending')",
    )
    .bind(thread)
    .fetch_one(&mut *tx)
    .await?;
    if sending {
        return Err(Error::Conflict(
            "Wait for the current send to finish.".into(),
        ));
    }
    sqlx::query("UPDATE supplier_messages SET status='superseded',updated_at=now() WHERE thread_id=$1 AND direction='outbound' AND status IN ('draft','review_pending') AND first_attempt_at IS NULL").bind(thread).execute(&mut *tx).await?;
    let in_reply:Option<String>=sqlx::query_scalar("SELECT internet_id FROM supplier_messages WHERE thread_id=$1 AND direction='inbound' AND channel='email' ORDER BY created_at DESC LIMIT 1").bind(thread).fetch_optional(&mut *tx).await?.flatten();
    sqlx::query("INSERT INTO supplier_messages(id,thread_id,workspace_id,direction,channel,body,subject,status,reply_to_id) VALUES($1,$2,$3,'outbound','email',$4,$5,'draft',$6)").bind(input.request_id).bind(thread).bind(workspace).bind(input.body.trim()).bind(t.try_get::<String,_>("subject")?).bind(in_reply).execute(&mut *tx).await?;
    sqlx::query("UPDATE supplier_threads SET status='reply_ready',updated_at=now() WHERE id=$1")
        .bind(thread)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(json!({"saved":true}))
}
pub async fn enquiries(
    pool: &PgPool,
    workspace: &str,
    vendor: Uuid,
    requested: i64,
) -> Result<Value> {
    let mut tx = pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *tx)
        .await?;
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM vendors WHERE workspace_id=$1 AND id=$2)")
            .bind(workspace)
            .bind(vendor)
            .fetch_one(&mut *tx)
            .await?;
    if !exists {
        return Err(Error::NotFound);
    }
    let total: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM supplier_threads WHERE workspace_id=$1 AND vendor_id=$2",
    )
    .bind(workspace)
    .bind(vendor)
    .fetch_one(&mut *tx)
    .await?;
    let pages = ((total + 19) / 20).max(1);
    let page = requested.clamp(1, pages);
    let items:Vec<Value>=sqlx::query_scalar("SELECT jsonb_build_object('id',id,'vendor_id',vendor_id,'subject',subject,'status',status,'updated_at',updated_at) FROM supplier_threads WHERE workspace_id=$1 AND vendor_id=$2 ORDER BY created_at DESC,id DESC LIMIT 20 OFFSET $3").bind(workspace).bind(vendor).bind((page-1)*20).fetch_all(&mut *tx).await?;
    tx.commit().await?;
    Ok(json!({"items":items,"total":total,"page":page,"pages":pages,"page_size":20}))
}

#[cfg(test)]
mod transport_tests {
    use super::*;
    use axum::{
        Json, Router,
        extract::State,
        http::{HeaderMap, StatusCode},
        routing::{get, post},
    };
    use std::sync::{Arc, Mutex};
    #[derive(Clone)]
    struct Mock {
        calls: Arc<Mutex<Vec<(String, Value)>>>,
        gets: Arc<Mutex<usize>>,
        subject: String,
        received: Uuid,
        sent: Uuid,
    }
    async fn send(
        State(s): State<Mock>,
        h: HeaderMap,
        Json(v): Json<Value>,
    ) -> (StatusCode, Json<Value>) {
        let mut calls = s.calls.lock().unwrap();
        calls.push((h["idempotency-key"].to_str().unwrap().to_owned(), v));
        if calls.len() == 1 {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error":"retry"})),
            )
        } else {
            (StatusCode::OK, Json(json!({"id":s.sent})))
        }
    }
    async fn list(State(s): State<Mock>) -> Json<Value> {
        Json(
            json!({"has_more":false,"data":[{"id":s.received,"from":"Supplier <supplier@example.test>","to":["inbox@receive.example"],"subject":format!("Re: {}",s.subject)},{"id":Uuid::new_v4(),"from":"unrelated@example.test","to":["inbox@receive.example"],"subject":"Unrelated private message"}]}),
        )
    }
    async fn body(State(s): State<Mock>) -> Json<Value> {
        *s.gets.lock().unwrap() += 1;
        Json(
            json!({"text":"Pack size is 5 kg. Delivery takes two days.","message_id":"<supplier-reply@example.test>","headers":{}}),
        )
    }
    #[tokio::test]
    #[ignore = "Requires dedicated PostgreSQL test database"]
    async fn resend_retry_reuses_payload_and_received_body_is_scoped_and_deduplicated() {
        let url = std::env::var("TEST_DATABASE_URL").unwrap();
        assert!(url.ends_with("/backhaus_ai_test"));
        let pool = PgPool::connect(&url).await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();
        let workspace = format!("transport-{}", Uuid::new_v4());
        sqlx::query("INSERT INTO workspaces(id,name) VALUES($1,'Transport test')")
            .bind(&workspace)
            .execute(&pool)
            .await
            .unwrap();
        let vendor = Uuid::new_v4();
        let thread = Uuid::new_v4();
        let message = Uuid::new_v4();
        let subject = format!("Supply enquiry [BH-{thread}]");
        sqlx::query("INSERT INTO vendors(workspace_id,id,name,email,source) VALUES($1,$2,'Supplier','supplier@example.test','manual')").bind(&workspace).bind(vendor).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO supplier_threads(id,workspace_id,vendor_id,item_ids,subject,initial_body,reply_to,recipient_email) VALUES($1,$2,$3,'[]',$4,'Quote please','inbox@receive.example','supplier@example.test')").bind(thread).bind(&workspace).bind(vendor).bind(&subject).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO supplier_messages(id,thread_id,workspace_id,direction,channel,body,subject,status) VALUES($1,$2,$3,'outbound','email','Quote please',$4,'draft')").bind(message).bind(thread).bind(&workspace).bind(&subject).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO business_settings(workspace_id,reply_to) VALUES($1,'inbox@receive.example')").bind(&workspace).execute(&pool).await.unwrap();
        let cfg = Config {
            database_url: url,
            bind: "127.0.0.1:0".parse().unwrap(),
            api_key: "test-only-secret-not-real".into(),
            login: None,
            resend_key: Some("test-key".into()),
            resend_from: Some("restaurant@example.test".into()),
            cors_origin: "http://localhost:5173".into(),
            workspace_id: workspace.clone(),
            db_max_connections: 2,
            model_base_url: None,
            model_name: None,
            model_api_key: String::new(),
            model_request_options: json!({}),
            model_timeout: std::time::Duration::from_secs(10),
            worker_poll: std::time::Duration::from_secs(1),
            typst_bin: "typst".into(),
            node_bin: "node".into(),
            worker_script: std::path::PathBuf::new(),
        };
        let mock = Mock {
            calls: Arc::default(),
            gets: Arc::default(),
            subject,
            received: Uuid::new_v4(),
            sent: Uuid::new_v4(),
        };
        let app = Router::new()
            .route("/emails", post(send))
            .route("/emails/receiving", get(list))
            .route("/emails/receiving/{id}", get(body))
            .with_state(mock.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        assert!(
            send_email_at(
                &pool,
                &workspace,
                &cfg,
                thread,
                None,
                &format!("{base}/emails")
            )
            .await
            .is_err()
        );
        sqlx::query("UPDATE supplier_messages SET body='Edited after uncertain send' WHERE id=$1")
            .bind(message)
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!(
            send_email_at(
                &pool,
                &workspace,
                &cfg,
                thread,
                None,
                &format!("{base}/emails")
            )
            .await
            .unwrap()["status"],
            "sent"
        );
        assert_eq!(
            send_email_at(
                &pool,
                &workspace,
                &cfg,
                thread,
                None,
                &format!("{base}/emails")
            )
            .await
            .unwrap()["reused"],
            true
        );
        {
            let calls = mock.calls.lock().unwrap();
            assert_eq!(calls.len(), 2);
            assert_eq!(calls[0], calls[1]);
            assert_eq!(calls[0].1["text"], "Quote please");
        }
        assert_eq!(
            sync_at(&pool, &workspace, &cfg, &base).await.unwrap()["received"],
            1
        );
        assert_eq!(
            sync_at(&pool, &workspace, &cfg, &base).await.unwrap()["received"],
            0
        );
        assert_eq!(
            *mock.gets.lock().unwrap(),
            1,
            "Never retrieve unrelated inbox content or duplicate replies"
        );
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM agent_runs WHERE workspace_id=$1 AND kind='supplier_reply'",
        )
        .bind(&workspace)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(count, 1);
        discard(&pool, &workspace, thread).await.unwrap();
        assert!(
            send_email_at(
                &pool,
                &workspace,
                &cfg,
                thread,
                None,
                &format!("{base}/emails")
            )
            .await
            .is_err()
        );
        server.abort();

        // Approving a researched supplier must prepare and send the correct supplies once.
        sqlx::query(
            "UPDATE business_settings SET city='Abuja',country='Nigeria' WHERE workspace_id=$1",
        )
        .bind(&workspace)
        .execute(&pool)
        .await
        .unwrap();
        for (id, name) in [
            ("peppers", "Bell Peppers"),
            ("lemons", "Lemons"),
            ("whisky", "Premium Whisky"),
        ] {
            sqlx::query("INSERT INTO inventory_items(workspace_id,id,name,unit,par_level,current_balance,source) VALUES($1,$2,$3,'kg',10,2,'{}')")
                .bind(&workspace).bind(id).bind(name).execute(&pool).await.unwrap();
        }
        let mut tx = pool.begin().await.unwrap();
        let research_id = jobs::enqueue_system(
            &mut tx,
            &workspace,
            "vendor_research",
            "approved-outreach",
            json!({"city":"Abuja","country":"Nigeria","items":[{"id":"peppers"},{"id":"lemons"}]}),
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        let candidate = Uuid::new_v4();
        let evidence = json!([{"run_id":research_id}]);
        sqlx::query("INSERT INTO vendors(workspace_id,id,name,email,source,evidence) VALUES($1,$2,'Produce Supplier','produce@example.test','web_research',$3)").bind(&workspace).bind(candidate).bind(&evidence).execute(&pool).await.unwrap();
        // An unrelated manually prepared enquiry must never be used by approval.
        create(
            &pool,
            &workspace,
            &cfg,
            candidate,
            Create {
                item_ids: vec!["lemons".into(), "whisky".into()],
            },
        )
        .await
        .unwrap();
        let mail = Mock {
            calls: Arc::default(),
            gets: Arc::default(),
            sent: Uuid::new_v4(),
            ..mock.clone()
        };
        let app = Router::new()
            .route("/emails", post(send))
            .with_state(mail.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/emails", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let failed = approve_candidate_at(&pool, &workspace, &cfg, candidate, &endpoint)
            .await
            .unwrap();
        assert_eq!(failed["status"], "send_failed");
        let approved = approve_candidate_at(&pool, &workspace, &cfg, candidate, &endpoint)
            .await
            .unwrap();
        assert_eq!(approved["status"], "sent");
        assert_eq!(approved["thread_id"], failed["thread_id"]);
        let enquiry: Uuid = approved["thread_id"].as_str().unwrap().parse().unwrap();
        let detail = detail(&pool, &workspace, enquiry).await.unwrap();
        assert_eq!(detail["thread"]["status"], "awaiting_reply");
        assert_eq!(detail["messages"].as_array().unwrap().len(), 1);
        {
            let calls = mail.calls.lock().unwrap();
            assert_eq!(calls.len(), 2);
            assert_eq!(
                calls[0], calls[1],
                "retry must retain recipient, payload and idempotency key"
            );
            let body = calls[1].1["text"].as_str().unwrap();
            assert!(body.contains("Bell Peppers") && body.contains("Lemons"));
            assert!(!body.contains("Whisky"));
        }
        draft_reply(
            &pool,
            &workspace,
            enquiry,
            ReplyDraft {
                body: "Unapproved counter-offer; do not send".into(),
                request_id: Uuid::new_v4(),
            },
        )
        .await
        .unwrap();
        assert_eq!(
            approve_candidate_at(&pool, &workspace, &cfg, candidate, &endpoint)
                .await
                .unwrap()["reused"],
            true
        );
        assert_eq!(
            mail.calls.lock().unwrap().len(),
            2,
            "repeat approval must not send a counter-offer"
        );
        assert!(
            approve_candidate_at(&pool, "other-workspace", &cfg, candidate, &endpoint)
                .await
                .is_err()
        );
        candidate_action(
            &pool,
            &workspace,
            candidate,
            CandidateAction {
                status: "discarded".into(),
            },
        )
        .await
        .unwrap();
        assert!(
            approve_candidate_at(&pool, &workspace, &cfg, candidate, &endpoint)
                .await
                .is_err()
        );
        let phone_only = Uuid::new_v4();
        sqlx::query("INSERT INTO vendors(workspace_id,id,name,phone,source,evidence) VALUES($1,$2,'Phone Supplier','+2348001234567','web_research',$3)").bind(&workspace).bind(phone_only).bind(evidence).execute(&pool).await.unwrap();
        assert_eq!(
            approve_candidate_at(&pool, &workspace, &cfg, phone_only, &endpoint)
                .await
                .unwrap()["status"],
            "whatsapp_ready"
        );
        assert_eq!(
            mail.calls.lock().unwrap().len(),
            2,
            "phone-only suppliers are never emailed"
        );
        server.abort();
    }
}
