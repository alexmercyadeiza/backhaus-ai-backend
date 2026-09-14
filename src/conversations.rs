use crate::error::{Error, Result};
use serde::Deserialize;
use serde_json::{Value, json};
use sqlx::{PgPool, Row};
use uuid::Uuid;

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Page {
    pub before: Option<Uuid>,
}

pub async fn archive(pool: &PgPool, workspace: &str, id: Uuid) -> Result<Value> {
    let mut tx = pool.begin().await?;
    let exists = sqlx::query_scalar::<_, Uuid>(
        "SELECT id FROM conversations WHERE workspace_id=$1 AND id=$2 FOR UPDATE",
    )
    .bind(workspace)
    .bind(id)
    .fetch_optional(&mut *tx)
    .await?;
    if exists.is_none() {
        return Err(Error::NotFound);
    }
    // Fence active work and preserve any streamed text from its latest attempt.
    let stopped = sqlx::query_scalar::<_,Uuid>("UPDATE agent_runs r SET status='cancelled',lease_token=NULL,lease_until=NULL,updated_at=now(),result=COALESCE(result,jsonb_build_object('answer',COALESCE((SELECT string_agg(e.payload->>'text','' ORDER BY e.id) FROM agent_events e WHERE e.run_id=r.id AND e.event_type='text_delta' AND e.id>COALESCE((SELECT MAX(s.id) FROM agent_events s WHERE s.run_id=r.id AND s.event_type='running'),0)),''))) WHERE workspace_id=$1 AND conversation_id=$2 AND status IN ('queued','running','paused') RETURNING id")
        .bind(workspace).bind(id).fetch_all(&mut *tx).await?;
    for run_id in stopped {
        sqlx::query("INSERT INTO agent_events(run_id,event_type,payload) VALUES($1,'cancelled','{\"reason\":\"conversation_archived\"}')")
            .bind(run_id).execute(&mut *tx).await?;
    }
    let value=sqlx::query_scalar::<_,Value>("UPDATE conversations SET archived_at=COALESCE(archived_at,now()) WHERE workspace_id=$1 AND id=$2 RETURNING jsonb_build_object('conversation_id',id,'archived_at',archived_at)")
        .bind(workspace).bind(id).fetch_one(&mut *tx).await?;
    tx.commit().await?;
    tracing::info!(conversation_id=%id, "Conversation archived");
    Ok(value)
}

pub async fn archives(pool: &PgPool, workspace: &str, page: &Page) -> Result<Value> {
    if let Some(before) = page.before {
        let valid:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM conversations WHERE workspace_id=$1 AND id=$2 AND archived_at IS NOT NULL)")
            .bind(workspace).bind(before).fetch_one(pool).await?;
        if !valid {
            return Err(Error::NotFound);
        }
    }
    let mut rows=sqlx::query_scalar::<_,Value>("SELECT jsonb_build_object('id',c.id,'archived_at',c.archived_at,'title',COALESCE((SELECT left(r.input->>'message',100) FROM agent_runs r WHERE r.workspace_id=c.workspace_id AND r.conversation_id=c.id ORDER BY r.created_at,r.id LIMIT 1),'Untitled conversation')) FROM conversations c WHERE c.workspace_id=$1 AND c.archived_at IS NOT NULL AND ($2::uuid IS NULL OR (c.archived_at,c.id)<(SELECT b.archived_at,b.id FROM conversations b WHERE b.workspace_id=$1 AND b.id=$2)) ORDER BY c.archived_at DESC,c.id DESC LIMIT 21")
        .bind(workspace).bind(page.before).fetch_all(pool).await?;
    let has_more = rows.len() > 20;
    rows.truncate(20);
    let next_before = if has_more {
        rows.last().map(|r| r["id"].clone())
    } else {
        None
    };
    Ok(json!({"conversations":rows,"next_before":next_before}))
}

pub async fn history(pool: &PgPool, workspace: &str, id: Uuid, page: &Page) -> Result<Value> {
    let metadata=sqlx::query_scalar::<_,Value>("SELECT jsonb_build_object('archived_at',archived_at) FROM conversations WHERE workspace_id=$1 AND id=$2")
        .bind(workspace).bind(id).fetch_optional(pool).await?.ok_or(Error::NotFound)?;
    if let Some(before) = page.before {
        let valid:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent_runs WHERE workspace_id=$1 AND conversation_id=$2 AND id=$3)")
            .bind(workspace).bind(id).bind(before).fetch_one(pool).await?;
        if !valid {
            return Err(Error::NotFound);
        }
    }
    let mut rows=sqlx::query_scalar::<_,Value>("SELECT jsonb_build_object('id',r.id,'conversation_id',r.conversation_id,'status',r.status,'input',r.input,'result',r.result,'created_at',r.created_at,'artifacts',COALESCE((SELECT jsonb_agg(jsonb_build_object('artifact_id',a.id,'metadata',a.metadata)) FROM artifacts a WHERE a.workspace_id=r.workspace_id AND a.run_id=r.id),'[]'::jsonb)) FROM agent_runs r WHERE r.workspace_id=$1 AND r.conversation_id=$2 AND ($3::uuid IS NULL OR (r.created_at,r.id)<(SELECT b.created_at,b.id FROM agent_runs b WHERE b.workspace_id=$1 AND b.conversation_id=$2 AND b.id=$3)) ORDER BY r.created_at DESC,r.id DESC LIMIT 101")
        .bind(workspace).bind(id).bind(page.before).fetch_all(pool).await?;
    let has_more = rows.len() > 100;
    rows.truncate(100);
    let next_before = if has_more {
        rows.last().map(|r| r["id"].clone())
    } else {
        None
    };
    rows.reverse();
    Ok(
        json!({"conversation_id":id,"archived_at":metadata["archived_at"],"runs":rows,"next_before":next_before}),
    )
}

/// Newest-first input. Keep whole exchanges and a strict UTF-8 byte budget.
/// No extra model call is needed to summarize old conversations.
pub fn bounded_history(rows: Vec<(String, String)>) -> Vec<(String, String)> {
    let mut bytes = 0;
    let mut selected = Vec::new();
    for (user, answer) in rows.into_iter().take(4) {
        let size = user.len() + answer.len();
        if bytes + size > 12_000 {
            break;
        }
        bytes += size;
        selected.push((user, answer));
    }
    selected.reverse();
    selected
}

pub async fn model_history(
    pool: &PgPool,
    workspace: &str,
    conversation: Uuid,
    run: Uuid,
) -> Result<Vec<(String, String)>> {
    let rows=sqlx::query("SELECT r.input,r.result FROM agent_runs r JOIN conversations c ON c.id=r.conversation_id AND c.workspace_id=r.workspace_id WHERE r.workspace_id=$1 AND r.conversation_id=$2 AND c.archived_at IS NULL AND r.status='completed' AND r.result->>'context_version'='2' AND r.id<>$3 ORDER BY r.created_at DESC,r.id DESC LIMIT 4")
        .bind(workspace).bind(conversation).bind(run).fetch_all(pool).await?;
    let mut pairs = Vec::new();
    for r in rows {
        let input: Value = r.try_get("input")?;
        let output: Value = r.try_get("result")?;
        if let (Some(user), Some(answer)) = (input["message"].as_str(), output["answer"].as_str()) {
            pairs.push((user.to_owned(), answer.to_owned()));
        }
    }
    Ok(bounded_history(pairs))
}
