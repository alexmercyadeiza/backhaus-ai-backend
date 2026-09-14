use crate::{
    config::Config,
    conversations,
    data::{self, DateRange, InventoryQuery},
    error::{Error, Result},
    inventory, jobs, purchasing, reports, scoped_agents, tables, vendors,
};
use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, Path, Query, Request, State},
    http::{HeaderMap, StatusCode, header},
    middleware::{self, Next},
    response::{
        IntoResponse, Response, Sse,
        sse::{Event, KeepAlive},
    },
    routing::{get, post},
};
use futures::Stream;
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use std::{convert::Infallible, sync::Arc, time::Duration};
use subtle::ConstantTimeEq;
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use uuid::Uuid;

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
    pub config: Arc<Config>,
}
pub fn router(state: AppState) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(
            state
                .config
                .cors_origin
                .parse::<header::HeaderValue>()
                .expect("Validated CORS origin"),
        )
        .allow_methods([
            axum::http::Method::GET,
            axum::http::Method::POST,
            axum::http::Method::PATCH,
            axum::http::Method::PUT,
            axum::http::Method::DELETE,
        ])
        .allow_headers([
            header::AUTHORIZATION,
            header::CONTENT_TYPE,
            header::HeaderName::from_static("idempotency-key"),
            header::HeaderName::from_static("last-event-id"),
        ]);
    let protected = Router::new()
        .route("/v1/status", get(status))
        .route("/v1/coverage", get(coverage))
        .route("/v1/sales/summary", get(sales))
        .route("/v1/inventory", get(inventory))
        .route("/v1/tables/{kind}", get(table_page))
        .route("/v1/tables/{kind}/{id}", get(table_detail))
        .route("/v1/conversations", post(conversation))
        .route("/v1/conversations/archives", get(conversation_archives))
        .route("/v1/conversations/{id}/archive", post(conversation_archive))
        .route("/v1/conversations/{id}", get(conversation_history))
        .route("/v1/chat", post(chat))
        .route("/v1/agents", get(scoped_agent_list))
        .route(
            "/v1/agents/{role}/control/{action}",
            post(scoped_agent_control),
        )
        .route("/v1/agents/runs", get(runs))
        .route("/v1/agents/runs/{id}", get(run))
        .route("/v1/agents/runs/{id}/events", get(events))
        .route("/v1/agents/runs/{id}/{action}", post(control))
        .route("/v1/purchase-orders/{id}/pdf", post(purchase_order_pdf))
        .route(
            "/v1/purchase-orders/{id}/{action}",
            post(purchase_order_decision),
        )
        .route("/v1/inventory/{id}/movements", post(inventory_movement))
        .route("/v1/vendors", post(vendor_create))
        .route("/v1/vendors/unassigned-items", get(vendor_unassigned))
        .route("/v1/vendors/{id}", axum::routing::patch(vendor_update))
        .route(
            "/v1/vendors/{id}/items/{item}",
            axum::routing::put(vendor_assign).delete(vendor_unassign),
        )
        .route("/v1/purchasing/policy", get(policy_get).put(policy_update))
        .route("/v1/reports", post(report))
        .route("/v1/artifacts/{id}", get(artifact))
        .route_layer(middleware::from_fn_with_state(state.clone(), authenticate));
    Router::new()
        .route("/healthz", get(|| async { Json(json!({"status":"ok"})) }))
        .merge(protected)
        .layer(DefaultBodyLimit::max(16 * 1024))
        .layer(cors)
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
async fn authenticate(State(state): State<AppState>, request: Request, next: Next) -> Response {
    let supplied = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "))
        .unwrap_or("");
    let actual = Sha256::digest(supplied.as_bytes());
    let expected = Sha256::digest(state.config.api_key.as_bytes());
    if !bool::from(actual.ct_eq(&expected)) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error":"Unauthorized"})),
        )
            .into_response();
    }
    next.run(request).await
}
async fn status(State(s): State<AppState>) -> Result<Json<Value>> {
    let coverage = data::coverage(&s.pool, &s.config.workspace_id).await?;
    let synthetic = coverage["synthetic"] == true;
    Ok(Json(
        json!({"model_configured":s.config.model_base_url.is_some() && s.config.model_name.is_some(),
        "model":s.config.model_name,"data_source":if synthetic {"synthetic_demo"} else if coverage.get("from").is_some() {"imported_snapshot"} else {"none"},
        "restaurant":coverage["restaurant"],"agent_sdk":"strands-typescript",
        "capabilities":{"sales":true,"inventory":true,"reports":true,"purchase_order_drafts":true,"approvals":true,"purchase_order_pdf":true,"stock_adjustments":true,"vendor_management":true,"vendor_sending":false,"payments":false}}),
    ))
}
async fn conversation_history(
    State(s): State<AppState>,
    Path(id): Path<Uuid>,
    Query(page): Query<conversations::Page>,
) -> Result<Json<Value>> {
    Ok(Json(
        conversations::history(&s.pool, &s.config.workspace_id, id, &page).await?,
    ))
}
async fn conversation_archives(
    State(s): State<AppState>,
    Query(page): Query<conversations::Page>,
) -> Result<Json<Value>> {
    Ok(Json(
        conversations::archives(&s.pool, &s.config.workspace_id, &page).await?,
    ))
}
async fn conversation_archive(
    State(s): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>> {
    Ok(Json(
        conversations::archive(&s.pool, &s.config.workspace_id, id).await?,
    ))
}
async fn coverage(State(s): State<AppState>) -> Result<Json<Value>> {
    Ok(Json(data::coverage(&s.pool, &s.config.workspace_id).await?))
}
async fn sales(State(s): State<AppState>, Query(range): Query<DateRange>) -> Result<Json<Value>> {
    Ok(Json(
        data::sales(&s.pool, &s.config.workspace_id, &range).await?,
    ))
}
async fn inventory(
    State(s): State<AppState>,
    Query(query): Query<InventoryQuery>,
) -> Result<Json<Value>> {
    Ok(Json(
        data::inventory(&s.pool, &s.config.workspace_id, &query).await?,
    ))
}
async fn conversation(State(s): State<AppState>) -> Result<(StatusCode, Json<Value>)> {
    let id = Uuid::new_v4();
    sqlx::query("INSERT INTO conversations(id,workspace_id) VALUES($1,$2)")
        .bind(id)
        .bind(&s.config.workspace_id)
        .execute(&s.pool)
        .await?;
    Ok((StatusCode::CREATED, Json(json!({"conversation_id":id}))))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ChatRequest {
    conversation_id: Uuid,
    message: String,
}
async fn chat(
    State(s): State<AppState>,
    headers: HeaderMap,
    Json(input): Json<ChatRequest>,
) -> Result<(StatusCode, Json<Value>)> {
    if s.config.model_base_url.is_none() || s.config.model_name.is_none() {
        return Err(Error::Unavailable(
            "Set MODEL_BASE_URL and MODEL_NAME to enable chat".into(),
        ));
    }
    let message = input.message.trim();
    if message.is_empty() || message.len() > 4000 {
        return Err(Error::Invalid("Message must be 1..4000 bytes".into()));
    }
    let key = headers
        .get("idempotency-key")
        .and_then(|h| h.to_str().ok())
        .ok_or_else(|| Error::Invalid("Idempotency-Key header is required".into()))?;
    let value = jobs::enqueue(
        &s.pool,
        &s.config.workspace_id,
        input.conversation_id,
        key,
        json!({"message":message}),
    )
    .await?;
    Ok((StatusCode::ACCEPTED, Json(value)))
}
async fn runs(State(s): State<AppState>) -> Result<Json<Value>> {
    let rows=sqlx::query_scalar::<_,Value>("SELECT jsonb_build_object('id',id,'conversation_id',conversation_id,'status',r.status,'attempt',r.attempt,'input',r.input,'created_at',r.created_at,'updated_at',r.updated_at,'activity',(SELECT jsonb_build_object('type',e.event_type,'payload',e.payload) FROM agent_events e WHERE e.run_id=r.id ORDER BY e.id DESC LIMIT 1)) FROM agent_runs r WHERE workspace_id=$1 ORDER BY created_at DESC LIMIT 50").bind(&s.config.workspace_id).fetch_all(&s.pool).await?;
    Ok(Json(json!({"runs":rows})))
}
async fn run(State(s): State<AppState>, Path(id): Path<Uuid>) -> Result<Json<Value>> {
    Ok(Json(jobs::get(&s.pool, &s.config.workspace_id, id).await?))
}
async fn control(
    State(s): State<AppState>,
    Path((id, action)): Path<(Uuid, String)>,
) -> Result<Json<Value>> {
    Ok(Json(
        jobs::control(&s.pool, &s.config.workspace_id, id, &action).await?,
    ))
}
async fn report(
    State(s): State<AppState>,
    Json(request): Json<reports::ReportRequest>,
) -> Result<(StatusCode, Json<Value>)> {
    let result =
        reports::generate(&s.pool, &s.config, &s.config.workspace_id, None, &request).await?;
    let status = if result["status"] == "no_data" {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    };
    Ok((status, Json(result)))
}
async fn artifact(State(s): State<AppState>, Path(id): Path<Uuid>) -> Result<Response> {
    let row = sqlx::query(
        "SELECT filename,media_type,bytes FROM artifacts WHERE workspace_id=$1 AND id=$2",
    )
    .bind(&s.config.workspace_id)
    .bind(id)
    .fetch_optional(&s.pool)
    .await?
    .ok_or(Error::NotFound)?;
    let filename: String = row.try_get("filename")?;
    let media: String = row.try_get("media_type")?;
    let bytes: Vec<u8> = row.try_get("bytes")?;
    Ok((
        [
            (header::CONTENT_TYPE, media),
            (
                header::CONTENT_DISPOSITION,
                format!("attachment; filename=\"{filename}\""),
            ),
            (header::CACHE_CONTROL, "private, no-store".into()),
        ],
        Body::from(bytes),
    )
        .into_response())
}
async fn events(
    State(s): State<AppState>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
) -> Result<Sse<impl Stream<Item = std::result::Result<Event, Infallible>>>> {
    jobs::get(&s.pool, &s.config.workspace_id, id).await?;
    let mut cursor = headers
        .get("last-event-id")
        .and_then(|h| h.to_str().ok())
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(0)
        .max(0);
    let stream = async_stream::stream! {
        let mut interval=tokio::time::interval(Duration::from_millis(500));
        loop {
            interval.tick().await;
            let rows=sqlx::query("SELECT e.id,e.event_type,e.payload FROM agent_events e JOIN agent_runs r ON r.id=e.run_id WHERE r.workspace_id=$1 AND r.id=$2 AND e.id>$3 ORDER BY e.id LIMIT 100")
                .bind(&s.config.workspace_id).bind(id).bind(cursor).fetch_all(&s.pool).await;
            match rows {
                Ok(rows)=>{
                    let full=rows.len()==100;
                    for row in rows {
                        let event_id:i64=row.get("id");
                        let kind:String=row.get("event_type");
                        let payload:Value=row.get("payload");
                        cursor=event_id;
                        yield Ok(Event::default().id(event_id.to_string()).event(kind.clone()).data(payload.to_string()));
                        if matches!(kind.as_str(),"completed"|"failed"|"cancelled"){return;}
                    }
                    if full {continue;}
                    match jobs::get(&s.pool,&s.config.workspace_id,id).await {
                        Ok(run) if matches!(run["status"].as_str(),Some("completed"|"failed"|"cancelled"))=>{
                            // A terminal event may have committed between the event read
                            // and status read. Drain it before ending the stream.
                            let pending=sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM agent_events WHERE run_id=$1 AND id>$2)").bind(id).bind(cursor).fetch_one(&s.pool).await;
                            match pending {
                                Ok(true)=>continue,
                                Ok(false)=>return,
                                Err(_)=>{yield Ok(Event::default().event("error").data("{\"error\":\"Event stream unavailable\"}"));return;}
                            }
                        },
                        Err(_)=>{yield Ok(Event::default().event("error").data("{\"error\":\"Event stream unavailable\"}"));return;},
                        _=>{}
                    }
                },
                Err(_)=>{yield Ok(Event::default().event("error").data("{\"error\":\"Event stream unavailable\"}"));return;}
            }
        }
    };
    Ok(Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15))))
}

async fn scoped_agent_list(State(s): State<AppState>) -> Result<Json<Value>> {
    Ok(Json(
        scoped_agents::list(&s.pool, &s.config.workspace_id).await?,
    ))
}
async fn scoped_agent_control(
    State(s): State<AppState>,
    Path((role, action)): Path<(String, String)>,
) -> Result<Json<Value>> {
    Ok(Json(
        scoped_agents::control(&s.pool, &s.config.workspace_id, &role, &action).await?,
    ))
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct DecisionRequest {
    note: Option<String>,
    version: Option<i32>,
}
async fn purchase_order_decision(
    State(s): State<AppState>,
    Path((id, action)): Path<(Uuid, String)>,
    body: Option<Json<DecisionRequest>>,
) -> Result<Json<Value>> {
    let input = body.map(|Json(b)| b).unwrap_or_default();
    Ok(Json(
        purchasing::decide_versioned(
            &s.pool,
            &s.config.workspace_id,
            id,
            &action,
            purchasing::ACTOR_DASHBOARD,
            input.note.as_deref(),
            input.version,
        )
        .await?,
    ))
}

async fn purchase_order_pdf(
    State(s): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<(StatusCode, Json<Value>)> {
    let result = purchasing::export_pdf(&s.pool, &s.config, &s.config.workspace_id, id).await?;
    let status = if result["reused"] == true {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    };
    Ok((status, Json(result)))
}
async fn inventory_movement(
    State(s): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(request): Json<inventory::MovementRequest>,
) -> Result<(StatusCode, Json<Value>)> {
    let key = headers
        .get("idempotency-key")
        .and_then(|h| h.to_str().ok())
        .ok_or_else(|| Error::Invalid("Idempotency-Key header is required".into()))?;
    if id.len() > 200 {
        return Err(Error::NotFound);
    }
    let result = inventory::record(&s.pool, &s.config.workspace_id, &id, key, &request).await?;
    let status = if result["reused"] == true {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    };
    Ok((status, Json(result)))
}
async fn vendor_create(
    State(s): State<AppState>,
    Json(input): Json<vendors::VendorInput>,
) -> Result<(StatusCode, Json<Value>)> {
    Ok((
        StatusCode::CREATED,
        Json(vendors::create(&s.pool, &s.config.workspace_id, &input).await?),
    ))
}
async fn vendor_update(
    State(s): State<AppState>,
    Path(id): Path<Uuid>,
    Json(input): Json<vendors::VendorInput>,
) -> Result<Json<Value>> {
    Ok(Json(
        vendors::update(&s.pool, &s.config.workspace_id, id, &input).await?,
    ))
}
async fn vendor_unassigned(State(s): State<AppState>) -> Result<Json<Value>> {
    Ok(Json(
        vendors::unassigned_items(&s.pool, &s.config.workspace_id).await?,
    ))
}
async fn vendor_assign(
    State(s): State<AppState>,
    Path((id, item)): Path<(Uuid, String)>,
    Json(input): Json<vendors::RuleInput>,
) -> Result<Json<Value>> {
    if item.len() > 200 {
        return Err(Error::NotFound);
    }
    Ok(Json(
        vendors::assign(&s.pool, &s.config.workspace_id, id, &item, &input).await?,
    ))
}
#[derive(Deserialize)]
struct RuleVersion {
    version: i32,
}
async fn vendor_unassign(
    State(s): State<AppState>,
    Path((id, item)): Path<(Uuid, String)>,
    Query(expected): Query<RuleVersion>,
) -> Result<Json<Value>> {
    if item.len() > 200 {
        return Err(Error::NotFound);
    }
    Ok(Json(
        vendors::unassign_versioned(
            &s.pool,
            &s.config.workspace_id,
            id,
            &item,
            Some(expected.version),
        )
        .await?,
    ))
}
async fn policy_get(State(s): State<AppState>) -> Result<Json<Value>> {
    Ok(Json(
        vendors::policy(&s.pool, &s.config.workspace_id).await?,
    ))
}
async fn policy_update(
    State(s): State<AppState>,
    Json(input): Json<vendors::PolicyInput>,
) -> Result<Json<Value>> {
    Ok(Json(
        vendors::update_policy(&s.pool, &s.config.workspace_id, &input).await?,
    ))
}

async fn table_page(
    State(s): State<AppState>,
    Path(kind): Path<String>,
    Query(query): Query<tables::TableQuery>,
) -> Result<Json<Value>> {
    Ok(Json(
        tables::page(&s.pool, &s.config.workspace_id, &kind, &query).await?,
    ))
}

async fn table_detail(
    State(s): State<AppState>,
    Path((kind, id)): Path<(String, String)>,
    Query(query): Query<tables::TableQuery>,
) -> Result<Json<Value>> {
    Ok(Json(
        tables::detail(&s.pool, &s.config.workspace_id, &kind, &id, &query).await?,
    ))
}
