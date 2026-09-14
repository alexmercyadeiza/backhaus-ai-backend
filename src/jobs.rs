use crate::error::{Error, Result};
use serde_json::{Value, json};
use sqlx::{PgPool, Row};
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct Run {
    pub id: Uuid,
    pub workspace: String,
    pub kind: String,
    pub conversation: Option<Uuid>,
    pub input: Value,
    pub lease: Uuid,
    pub attempt: i32,
}

pub async fn enqueue(
    pool: &PgPool,
    workspace: &str,
    conversation: Uuid,
    key: &str,
    input: Value,
) -> Result<Value> {
    if key.is_empty() || key.len() > 128 {
        return Err(Error::Invalid(
            "Idempotency-Key must be 1..128 characters".into(),
        ));
    }
    let mut tx = pool.begin().await?;
    let archived = sqlx::query_scalar::<_, bool>(
        "SELECT archived_at IS NOT NULL FROM conversations WHERE workspace_id=$1 AND id=$2 FOR UPDATE",
    )
    .bind(workspace)
    .bind(conversation)
    .fetch_optional(&mut *tx)
    .await?;
    if archived.ok_or(Error::NotFound)? {
        return Err(Error::Conflict(
            "Archived conversations are read-only. Start a new chat.".into(),
        ));
    }
    if let Some(row) = sqlx::query(
        "SELECT id,input,conversation_id FROM agent_runs WHERE workspace_id=$1 AND request_key=$2",
    )
    .bind(workspace)
    .bind(key)
    .fetch_optional(&mut *tx)
    .await?
    {
        if row.try_get::<Value, _>("input")? != input
            || row.try_get::<Option<Uuid>, _>("conversation_id")? != Some(conversation)
        {
            return Err(Error::Conflict(
                "Idempotency-Key already belongs to a different request".into(),
            ));
        }
        return Ok(
            json!({"run_id":row.try_get::<Uuid,_>("id")?,"conversation_id":conversation,"reused":true}),
        );
    }
    let busy:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent_runs WHERE workspace_id=$1 AND conversation_id=$2 AND status IN ('queued','running','paused'))").bind(workspace).bind(conversation).fetch_one(&mut *tx).await?;
    if busy {
        return Err(Error::Conflict(
            "This conversation already has unfinished work".into(),
        ));
    }
    let id = Uuid::new_v4();
    sqlx::query("INSERT INTO agent_runs(id,workspace_id,conversation_id,request_key,input,kind) VALUES($1,$2,$3,$4,$5,'chat')").bind(id).bind(workspace).bind(conversation).bind(key).bind(input).execute(&mut *tx).await?;
    sqlx::query("INSERT INTO agent_events(run_id,event_type,payload) VALUES($1,'queued','{}')")
        .bind(id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(json!({"run_id":id,"conversation_id":conversation,"reused":false}))
}

/// Queue background agent work (no conversation) inside the caller's
/// transaction. The request key makes repeated checks of the same revision a
/// no-op, so retries and concurrent checks cannot queue duplicate reviews.
pub async fn enqueue_system(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    workspace: &str,
    kind: &str,
    key: &str,
    input: Value,
) -> Result<Option<Uuid>> {
    let id = Uuid::new_v4();
    let inserted = sqlx::query_scalar::<_, Uuid>("INSERT INTO agent_runs(id,workspace_id,conversation_id,request_key,input,kind) VALUES($1,$2,NULL,$3,$4,$5) ON CONFLICT (workspace_id,request_key) DO NOTHING RETURNING id")
        .bind(id).bind(workspace).bind(key).bind(input).bind(kind).fetch_optional(&mut **tx).await?;
    if let Some(id) = inserted {
        sqlx::query("INSERT INTO agent_events(run_id,event_type,payload) VALUES($1,'queued','{}')")
            .bind(id)
            .execute(&mut **tx)
            .await?;
    }
    Ok(inserted)
}

pub async fn claim(pool: &PgPool, workspace: &str) -> Result<Option<Run>> {
    let mut tx = pool.begin().await?;
    sqlx::query("WITH recovered AS (UPDATE agent_runs SET status=CASE WHEN attempt<max_attempts THEN 'queued' ELSE 'failed' END, error_code='worker_lease_expired',lease_token=NULL,lease_until=NULL,updated_at=now() WHERE workspace_id=$1 AND status='running' AND lease_until<now() RETURNING id,status) INSERT INTO agent_events(run_id,event_type,payload) SELECT id,status,jsonb_build_object('reason','worker_lease_expired') FROM recovered").bind(workspace).execute(&mut *tx).await?;
    let lease = Uuid::new_v4();
    let row=sqlx::query("UPDATE agent_runs SET status='running',attempt=attempt+1,lease_token=$2,lease_until=now()+interval '30 seconds',updated_at=now() WHERE id=(SELECT id FROM agent_runs WHERE workspace_id=$1 AND status='queued' AND available_at<=now() ORDER BY CASE WHEN kind='chat' THEN 0 ELSE 1 END,created_at FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING id,workspace_id,conversation_id,input,attempt,kind")
        .bind(workspace).bind(lease).fetch_optional(&mut *tx).await?;
    let run = if let Some(r) = row {
        let id: Uuid = r.try_get("id")?;
        sqlx::query(
            "INSERT INTO agent_events(run_id,event_type,payload) VALUES($1,'running','{}')",
        )
        .bind(id)
        .execute(&mut *tx)
        .await?;
        Some(Run {
            id,
            workspace: r.try_get("workspace_id")?,
            kind: r.try_get("kind")?,
            conversation: r.try_get("conversation_id")?,
            input: r.try_get("input")?,
            lease,
            attempt: r.try_get("attempt")?,
        })
    } else {
        None
    };
    tx.commit().await?;
    Ok(run)
}

pub async fn active(pool: &PgPool, run: &Run) -> Result<bool> {
    Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent_runs WHERE id=$1 AND workspace_id=$2 AND status='running' AND lease_token=$3 AND lease_until>now())").bind(run.id).bind(&run.workspace).bind(run.lease).fetch_one(pool).await?)
}
pub async fn heartbeat(pool: &PgPool, run: &Run) -> Result<bool> {
    Ok(sqlx::query("UPDATE agent_runs SET lease_until=now()+interval '30 seconds' WHERE id=$1 AND workspace_id=$2 AND status='running' AND lease_token=$3 AND lease_until>now()")
        .bind(run.id).bind(&run.workspace).bind(run.lease).execute(pool).await?.rows_affected()==1)
}
pub async fn event(pool: &PgPool, run: &Run, kind: &str, payload: Value) -> Result<()> {
    let written=sqlx::query("WITH owned AS (SELECT id FROM agent_runs WHERE id=$1 AND workspace_id=$2 AND lease_token=$3 AND status='running' AND lease_until>now() FOR UPDATE) INSERT INTO agent_events(run_id,event_type,payload) SELECT id,$4,$5 FROM owned")
        .bind(run.id).bind(&run.workspace).bind(run.lease).bind(kind).bind(&payload).execute(pool).await?.rows_affected();
    if written == 0 {
        return Err(Error::Conflict(
            "Run paused, cancelled, or lease lost".into(),
        ));
    }
    if kind != "text_delta" {
        tracing::info!(run_id=%run.id, event=kind, tool=payload["tool"].as_str().unwrap_or(""), elapsed_ms=payload["elapsed_ms"].as_u64(), "Chat progress");
    }
    Ok(())
}
pub async fn finish(
    pool: &PgPool,
    run: &Run,
    result: Option<Value>,
    error: Option<&str>,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    let status = if error.is_none() {
        "completed"
    } else if run.attempt < 3 {
        "queued"
    } else {
        "failed"
    };
    let changed=sqlx::query("UPDATE agent_runs SET status=$4,result=$5,error_code=$6,available_at=now()+CASE WHEN $4='queued' THEN interval '5 seconds' * attempt ELSE interval '0 seconds' END,lease_token=NULL,lease_until=NULL,updated_at=now() WHERE id=$1 AND workspace_id=$2 AND status='running' AND lease_token=$3 AND lease_until>now()")
        .bind(run.id).bind(&run.workspace).bind(run.lease).bind(status).bind(&result).bind(error).execute(&mut *tx).await?.rows_affected();
    if changed == 1 {
        sqlx::query("INSERT INTO agent_events(run_id,event_type,payload) VALUES($1,$2,$3)")
            .bind(run.id)
            .bind(status)
            .bind(json!({"result":result,"error_code":error}))
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    if changed == 1 {
        tracing::info!(run_id=%run.id, status, error_code=error, "Chat request finished");
    }
    Ok(())
}
pub async fn control(pool: &PgPool, workspace: &str, id: Uuid, action: &str) -> Result<Value> {
    let mut tx = pool.begin().await?;
    let archived=sqlx::query_scalar::<_,bool>("SELECT c.archived_at IS NOT NULL FROM conversations c WHERE c.workspace_id=$1 AND c.id=(SELECT r.conversation_id FROM agent_runs r WHERE r.workspace_id=$1 AND r.id=$2) FOR UPDATE")
        .bind(workspace).bind(id).fetch_optional(&mut *tx).await?.unwrap_or(false);
    if archived {
        return Err(Error::Conflict(
            "Archived conversations are read-only.".into(),
        ));
    }

    let status: Option<String> = sqlx::query_scalar(
        "SELECT status FROM agent_runs WHERE workspace_id=$1 AND id=$2 FOR UPDATE",
    )
    .bind(workspace)
    .bind(id)
    .fetch_optional(&mut *tx)
    .await?;
    let status = status.ok_or(Error::NotFound)?;
    let next = match (action, status.as_str()) {
        ("pause", "queued" | "running") => "paused",
        ("pause", "paused") => "paused",
        ("resume", "paused") => "queued",
        ("cancel", "queued" | "running" | "paused" | "cancelled") => "cancelled",
        _ => {
            return Err(Error::Conflict(
                "Action is not valid for the current run status".into(),
            ));
        }
    };
    sqlx::query("UPDATE agent_runs SET status=$3,lease_token=NULL,lease_until=NULL,updated_at=now() WHERE workspace_id=$1 AND id=$2").bind(workspace).bind(id).bind(next).execute(&mut *tx).await?;
    sqlx::query("INSERT INTO agent_events(run_id,event_type,payload) VALUES($1,$2,'{}')")
        .bind(id)
        .bind(next)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(json!({"run_id":id,"status":next}))
}
pub async fn get(pool: &PgPool, workspace: &str, id: Uuid) -> Result<Value> {
    sqlx::query_scalar("SELECT jsonb_build_object('id',id,'kind',kind,'conversation_id',conversation_id,'status',status,'attempt',attempt,'input',input,'result',result,'error_code',error_code,'created_at',created_at,'updated_at',updated_at) FROM agent_runs WHERE workspace_id=$1 AND id=$2").bind(workspace).bind(id).fetch_optional(pool).await?.ok_or(Error::NotFound)
}

/// Return interrupted work to the queue without spending a failure attempt.
pub async fn release(pool: &PgPool, run: &Run) -> Result<()> {
    release_for(pool, run, "worker_shutdown").await
}
pub async fn release_for(pool: &PgPool, run: &Run, reason: &str) -> Result<()> {
    let mut tx = pool.begin().await?;
    let changed = sqlx::query("UPDATE agent_runs SET status='queued',attempt=GREATEST(attempt-1,0),lease_token=NULL,lease_until=NULL,available_at=now(),updated_at=now() WHERE id=$1 AND workspace_id=$2 AND status='running' AND lease_token=$3")
        .bind(run.id).bind(&run.workspace).bind(run.lease).execute(&mut *tx).await?.rows_affected();
    if changed == 1 {
        sqlx::query("INSERT INTO agent_events(run_id,event_type,payload) VALUES($1,'queued',jsonb_build_object('reason',$2::text))").bind(run.id).bind(reason).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    if changed == 1 {
        tracing::info!(run_id=%run.id, reason, "Run returned to queue");
    }
    Ok(())
}
