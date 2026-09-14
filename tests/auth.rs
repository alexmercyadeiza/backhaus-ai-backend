mod common;
use axum::{
    body::Body,
    http::{Request, StatusCode, header},
};
use backhaus_ai_backend::{
    api::{self, AppState},
    auth::Login,
};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use tower::ServiceExt;

fn app() -> axum::Router {
    let url = "postgres://unused@127.0.0.1:1/no_database_needed";
    let mut config = common::config("test-auth", url);
    config.login = Some(Login::new(
        "tester@example.invalid".into(),
        "test-password".into(),
    ));
    api::router(AppState {
        pool: PgPoolOptions::new().connect_lazy(url).unwrap(),
        config: Arc::new(config),
    })
}
fn req(method: &str, path: &str, body: Value, cookie: &str, origin: &str) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(path)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::COOKIE, cookie)
        .header(header::ORIGIN, origin)
        .body(Body::from(body.to_string()))
        .unwrap()
}
#[tokio::test]
async fn login_cookie_protects_routes_and_logout_revokes_it() {
    let app = app();
    let origin = "http://localhost:5173";
    let before = app
        .clone()
        .oneshot(req(
            "GET",
            "/v1/agents/runs/not-a-uuid",
            json!(null),
            "",
            origin,
        ))
        .await
        .unwrap();
    assert_eq!(before.status(), StatusCode::UNAUTHORIZED);
    let login = app
        .clone()
        .oneshot(req(
            "POST",
            "/v1/auth/login",
            json!({"email":" TESTER@example.invalid ","password":"test-password"}),
            "",
            origin,
        ))
        .await
        .unwrap();
    assert_eq!(login.status(), StatusCode::OK);
    let cookie = login.headers()[header::SET_COOKIE]
        .to_str()
        .unwrap()
        .to_owned();
    assert!(cookie.contains("HttpOnly"));
    assert!(cookie.contains("SameSite=Strict"));
    assert!(cookie.contains("Max-Age=28800"));
    assert_eq!(login.headers()[header::CACHE_CONTROL], "no-store");
    let cookie = cookie.split(';').next().unwrap();
    let session = app
        .clone()
        .oneshot(req("GET", "/v1/auth/session", json!(null), cookie, origin))
        .await
        .unwrap();
    let body: Value =
        serde_json::from_slice(&session.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(body["authenticated"], true);
    // Auth passes and Axum validates the malformed path without any database call.
    let allowed = app
        .clone()
        .oneshot(req(
            "GET",
            "/v1/agents/runs/not-a-uuid",
            json!(null),
            cookie,
            origin,
        ))
        .await
        .unwrap();
    assert_eq!(allowed.status(), StatusCode::BAD_REQUEST);
    let foreign = app
        .clone()
        .oneshot(req(
            "POST",
            "/v1/conversations",
            json!({}),
            cookie,
            "https://foreign.invalid",
        ))
        .await
        .unwrap();
    assert_eq!(foreign.status(), StatusCode::FORBIDDEN);
    let logout = app
        .clone()
        .oneshot(req("POST", "/v1/auth/logout", json!({}), cookie, origin))
        .await
        .unwrap();
    assert_eq!(logout.status(), StatusCode::OK);
    assert!(
        logout.headers()[header::SET_COOKIE]
            .to_str()
            .unwrap()
            .contains("Max-Age=0")
    );
    let after = app
        .oneshot(req(
            "GET",
            "/v1/agents/runs/not-a-uuid",
            json!(null),
            cookie,
            origin,
        ))
        .await
        .unwrap();
    assert_eq!(after.status(), StatusCode::UNAUTHORIZED);
}
#[tokio::test]
async fn incorrect_credentials_and_foreign_login_cannot_create_sessions() {
    let app = app();
    for (password, origin, expected) in [
        ("wrong", "http://localhost:5173", StatusCode::UNAUTHORIZED),
        (
            "test-password",
            "https://foreign.invalid",
            StatusCode::FORBIDDEN,
        ),
    ] {
        let result = app
            .clone()
            .oneshot(req(
                "POST",
                "/v1/auth/login",
                json!({"email":"tester@example.invalid","password":password}),
                "",
                origin,
            ))
            .await
            .unwrap();
        assert_eq!(result.status(), expected);
        assert!(!result.headers().contains_key(header::SET_COOKIE));
    }
}
