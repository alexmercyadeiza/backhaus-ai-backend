use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::json;
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Invalid(String),
    #[error("Not found")]
    NotFound,
    #[error("{0}")]
    Conflict(String),
    #[error("{0}")]
    Unavailable(String),
    #[error("Database operation failed")]
    Database(#[from] sqlx::Error),
    #[error("Report generation failed")]
    Report,
    #[error("Agent worker unavailable: {0}")]
    Worker(String),
}
impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let status = match self {
            Self::Invalid(_) => StatusCode::BAD_REQUEST,
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::Conflict(_) => StatusCode::CONFLICT,
            Self::Unavailable(_) | Self::Worker(_) => StatusCode::SERVICE_UNAVAILABLE,
            Self::Database(_) | Self::Report => StatusCode::INTERNAL_SERVER_ERROR,
        };
        if status.is_server_error() {
            tracing::warn!(status = status.as_u16(), "Request failed");
        }
        (status, Json(json!({"error": self.to_string()}))).into_response()
    }
}
pub type Result<T> = std::result::Result<T, Error>;
