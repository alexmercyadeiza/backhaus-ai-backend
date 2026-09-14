//! Change-driven monitors. Sales checks are read-only SQL. The inventory check
//! also prepares purchase-order drafts in Rust and, when a model is
//! configured, queues one Strands review per coalesced revision. No external
//! writes, sending or payments happen here.
use crate::{
    agent::KIND_INVENTORY_REVIEW,
    error::{Error, Result},
    jobs, purchasing,
};
use serde_json::{Value, json};
use sqlx::{PgPool, Postgres, Row, Transaction};
use uuid::Uuid;

pub async fn list(pool: &PgPool, workspace: &str) -> Result<Value> {
    let online: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent_monitor_instances WHERE workspace_id=$1 AND heartbeat_at>now()-interval '15 seconds')")
        .bind(workspace).fetch_one(pool).await?;
    let agents = sqlx::query_scalar::<_,Value>("SELECT jsonb_build_object('id',a.role,'enabled',a.enabled,'checked_revision',a.checked_revision,'data_revision',r.revision,'last_checked_at',a.last_checked_at,'observation',a.observation,'status',CASE WHEN NOT a.enabled THEN 'paused' WHEN NOT $2 THEN 'offline' WHEN a.checked_revision<r.revision THEN 'pending' ELSE 'watching' END,'review',CASE WHEN a.role='inventory' THEN (SELECT jsonb_build_object('run_id',x.id,'status',x.status,'revision',(x.input->>'revision')::bigint,'error_code',x.error_code,'updated_at',x.updated_at) FROM agent_runs x WHERE x.workspace_id=a.workspace_id AND x.kind='inventory_review' ORDER BY x.created_at DESC,x.id DESC LIMIT 1) END) FROM scoped_agents a JOIN agent_data_revisions r USING(workspace_id,role) WHERE a.workspace_id=$1 ORDER BY CASE a.role WHEN 'sales' THEN 0 ELSE 1 END")
        .bind(workspace).bind(online).fetch_all(pool).await?;
    Ok(json!({"agents":agents,"monitor_online":online}))
}

pub async fn control(pool: &PgPool, workspace: &str, role: &str, action: &str) -> Result<Value> {
    let enabled = match action {
        "pause" => false,
        "resume" => true,
        _ => return Err(Error::Invalid("Use pause or resume".into())),
    };
    let updated = sqlx::query(
        "UPDATE scoped_agents SET enabled=$3,updated_at=now() WHERE workspace_id=$1 AND role=$2",
    )
    .bind(workspace)
    .bind(role)
    .bind(enabled)
    .execute(pool)
    .await?
    .rows_affected();
    if updated == 0 {
        return Err(Error::NotFound);
    }
    tracing::info!(agent = role, enabled, "Agent monitoring changed");
    list(pool, workspace).await
}

pub async fn heartbeat(pool: &PgPool, workspace: &str, instance: Uuid) -> Result<()> {
    sqlx::query("INSERT INTO agent_monitor_instances(id,workspace_id) VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET heartbeat_at=now()")
        .bind(instance).bind(workspace).execute(pool).await?;
    // Expired instances cannot report a monitor online after a crash.
    sqlx::query("DELETE FROM agent_monitor_instances WHERE workspace_id=$1 AND heartbeat_at<now()-interval '1 minute'")
        .bind(workspace).execute(pool).await?;
    Ok(())
}

pub async fn disconnect(pool: &PgPool, instance: Uuid) -> Result<()> {
    sqlx::query("DELETE FROM agent_monitor_instances WHERE id=$1")
        .bind(instance)
        .execute(pool)
        .await?;
    Ok(())
}

/// Snapshot, prepared orders and checkpoint commit together. A pause waits for
/// this short SQL-only check, and returns only when no new checks can start.
/// Other workers skip the locked role, so one revision is processed once.
/// `review` queues a Strands review of the inventory findings (requires a model).
pub async fn check_one(pool: &PgPool, workspace: &str, review: bool) -> Result<bool> {
    let mut tx = pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *tx)
        .await?;
    let row=sqlx::query("SELECT a.role,r.revision FROM scoped_agents a JOIN agent_data_revisions r USING(workspace_id,role) WHERE a.workspace_id=$1 AND a.enabled AND a.checked_revision<r.revision ORDER BY a.role FOR UPDATE OF a SKIP LOCKED LIMIT 1")
        .bind(workspace).fetch_optional(&mut *tx).await?;
    let Some(row) = row else {
        return Ok(false);
    };
    let role: String = row.try_get("role")?;
    let revision: i64 = row.try_get("revision")?;
    let observation = match role.as_str() {
        "inventory" => purchasing::prepare_drafts(&mut tx, workspace, revision).await?,
        "sales" => sales_check(&mut tx, workspace).await?,
        _ => return Err(Error::Invalid("Unknown agent role".into())),
    };
    sqlx::query("INSERT INTO scoped_agent_checks(id,workspace_id,role,revision,observation) VALUES($1,$2,$3,$4,$5)")
        .bind(Uuid::new_v4()).bind(workspace).bind(&role).bind(revision).bind(&observation).execute(&mut *tx).await?;
    sqlx::query("UPDATE scoped_agents SET checked_revision=$3,last_checked_at=now(),observation=$4 WHERE workspace_id=$1 AND role=$2")
        .bind(workspace).bind(&role).bind(revision).bind(&observation).execute(&mut *tx).await?;
    let mut queued = None;
    if review && role == "inventory" {
        // Coalesce superseded queued reviews; never make chat wait for a backlog.
        sqlx::query("WITH cancelled AS (UPDATE agent_runs SET status='cancelled',error_code='superseded_revision',updated_at=now() WHERE workspace_id=$1 AND kind='inventory_review' AND status='queued' RETURNING id) INSERT INTO agent_events(run_id,event_type,payload) SELECT id,'cancelled',jsonb_build_object('reason','superseded_revision') FROM cancelled")
            .bind(workspace).execute(&mut *tx).await?;
        // One review per revision; the request key makes retries and concurrent checks no-ops.
        queued = jobs::enqueue_system(
            &mut tx,
            workspace,
            KIND_INVENTORY_REVIEW,
            &format!("inventory-review:{revision}"),
            json!({"role":"inventory","revision":revision}),
        )
        .await?;
    }
    tx.commit().await?;
    tracing::info!(agent=%role, revision, orders=observation["orders"].as_array().map(Vec::len).unwrap_or(0), review_run=?queued, "Agent data check completed");
    Ok(true)
}

async fn sales_check(tx: &mut Transaction<'_, Postgres>, workspace: &str) -> Result<Value> {
    // Compare the latest two recorded seven-day windows, never infer that gaps are zero.
    let value: Value = sqlx::query_scalar("WITH bounds AS (SELECT MAX(business_date) AS last_day FROM sales_tickets WHERE workspace_id=$1), daily AS (SELECT t.business_date,COUNT(*) AS tickets,SUM(COALESCE(l.gross,0)) AS gross FROM sales_tickets t CROSS JOIN bounds b LEFT JOIN LATERAL (SELECT SUM(quantity*unit_price) AS gross FROM sales_lines WHERE workspace_id=t.workspace_id AND ticket_id=t.id AND billable AND t.total_amount<>0) l ON true WHERE t.workspace_id=$1 AND t.business_date BETWEEN b.last_day-13 AND b.last_day GROUP BY t.business_date) SELECT jsonb_build_object('through',b.last_day,'from',b.last_day-6,'currency','NGN','recent_sales',COALESCE(SUM(d.gross) FILTER(WHERE d.business_date>=b.last_day-6),0)::text,'previous_sales',COALESCE(SUM(d.gross) FILTER(WHERE d.business_date<b.last_day-6),0)::text,'recent_days',COUNT(*) FILTER(WHERE d.business_date>=b.last_day-6),'previous_days',COUNT(*) FILTER(WHERE d.business_date<b.last_day-6),'recent_tickets',COALESCE(SUM(d.tickets) FILTER(WHERE d.business_date>=b.last_day-6),0)) FROM bounds b LEFT JOIN daily d ON true GROUP BY b.last_day")
        .bind(workspace).fetch_one(&mut **tx).await?;
    let mut result = value;
    result["summary"] = if result["through"].is_null() {
        json!("No sales records to analyze yet")
    } else {
        json!(format!(
            "{} tickets · {} of 7 days recorded",
            result["recent_tickets"], result["recent_days"]
        ))
    };
    Ok(result)
}
