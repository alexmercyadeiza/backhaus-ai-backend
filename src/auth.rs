//! Shared tester login. Sessions are private, bounded and invalidated on restart.
use crate::api::AppState;
use axum::{
    Json,
    extract::State,
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use subtle::ConstantTimeEq;
use uuid::Uuid;

const COOKIE: &str = "backhaus_session";
const TTL: Duration = Duration::from_secs(8 * 60 * 60);

#[derive(Clone)]
pub struct Login {
    email: String,
    password_hash: [u8; 32],
    sessions: Arc<Mutex<HashMap<String, Instant>>>,
}
impl Login {
    pub fn new(email: String, password: String) -> Self {
        Self {
            email: email.trim().to_lowercase(),
            password_hash: Sha256::digest(password.as_bytes()).into(),
            sessions: Arc::default(),
        }
    }
    pub fn valid(&self, headers: &HeaderMap) -> bool {
        let mut sessions = self.sessions.lock().unwrap();
        sessions.retain(|_, expires| *expires > Instant::now());
        token(headers).is_some_and(|value| sessions.contains_key(value))
    }
}
fn token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::COOKIE)?
        .to_str()
        .ok()?
        .split(';')
        .find_map(|part| part.trim().strip_prefix("backhaus_session="))
}
pub fn same_origin(headers: &HeaderMap, configured: &str) -> bool {
    let Some(origin) = headers.get(header::ORIGIN).and_then(|v| v.to_str().ok()) else {
        return false;
    };
    if origin == configured {
        return true;
    }
    // Both names address the same local tester app. Never allow arbitrary origins.
    let (Ok(actual), Ok(expected)) = (
        origin.parse::<axum::http::Uri>(),
        configured.parse::<axum::http::Uri>(),
    ) else {
        return false;
    };
    let local = |host: Option<&str>| matches!(host, Some("localhost" | "127.0.0.1"));
    local(actual.host())
        && local(expected.host())
        && actual.scheme_str() == expected.scheme_str()
        && actual.port_u16() == expected.port_u16()
}
fn reply(status: StatusCode, value: serde_json::Value, cookie: Option<String>) -> Response {
    let mut response = (status, Json(value)).into_response();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
    if let Some(cookie) = cookie {
        response
            .headers_mut()
            .insert(header::SET_COOKIE, cookie.parse().unwrap());
    }
    response
}
fn cookie(value: &str, age: u64, origin: &str) -> String {
    format!(
        "{COOKIE}={value}; Path=/; HttpOnly; SameSite=Strict; Max-Age={age}{}",
        if origin.starts_with("https://") {
            "; Secure"
        } else {
            ""
        }
    )
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Credentials {
    email: String,
    password: String,
}

pub async fn sign_in(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(input): Json<Credentials>,
) -> Response {
    if !same_origin(&headers, &state.config.cors_origin) {
        return reply(
            StatusCode::FORBIDDEN,
            json!({"error":"Sign in from the dashboard."}),
            None,
        );
    }
    let Some(login) = &state.config.login else {
        return reply(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({"error":"Sign-in credentials are not configured on the server."}),
            None,
        );
    };
    let email = Sha256::digest(input.email.trim().to_lowercase().as_bytes());
    let expected_email = Sha256::digest(login.email.as_bytes());
    let password = Sha256::digest(input.password.as_bytes());
    if !bool::from(email.ct_eq(&expected_email) & password.as_slice().ct_eq(&login.password_hash)) {
        tokio::time::sleep(Duration::from_millis(300)).await;
        return reply(
            StatusCode::UNAUTHORIZED,
            json!({"error":"Email or password is incorrect."}),
            None,
        );
    }
    let value = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    let mut sessions = login.sessions.lock().unwrap();
    sessions.retain(|_, expires| *expires > Instant::now());
    if sessions.len() >= 1000 {
        return reply(
            StatusCode::TOO_MANY_REQUESTS,
            json!({"error":"Too many sessions. Try again later."}),
            None,
        );
    }
    if let Some(previous) = token(&headers) {
        sessions.remove(previous);
    }
    sessions.insert(value.clone(), Instant::now() + TTL);
    reply(
        StatusCode::OK,
        json!({"authenticated":true,"email":login.email}),
        Some(cookie(&value, TTL.as_secs(), &state.config.cors_origin)),
    )
}
pub async fn session(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let valid = state
        .config
        .login
        .as_ref()
        .is_some_and(|login| login.valid(&headers));
    reply(StatusCode::OK, json!({"authenticated":valid}), None)
}
pub async fn sign_out(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if !same_origin(&headers, &state.config.cors_origin) {
        return reply(
            StatusCode::FORBIDDEN,
            json!({"error":"Sign out from the dashboard."}),
            None,
        );
    }
    if let (Some(login), Some(value)) = (&state.config.login, token(&headers)) {
        login.sessions.lock().unwrap().remove(value);
    }
    reply(
        StatusCode::OK,
        json!({"authenticated":false}),
        Some(cookie("", 0, &state.config.cors_origin)),
    )
}
