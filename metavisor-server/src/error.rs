//! API error types

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use thiserror::Error;

/// Result type alias for API operations
pub type Result<T> = std::result::Result<T, ApiError>;

/// API error type
#[derive(Error, Debug)]
pub enum ApiError {
    #[error("{0}")]
    Core(#[from] metavisor_core::CoreError),

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Bad request: {0}")]
    BadRequest(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            ApiError::Core(e) => match e {
                metavisor_core::CoreError::TypeNotFound(_)
                | metavisor_core::CoreError::EntityNotFound(_)
                | metavisor_core::CoreError::RelationshipNotFound(_)
                | metavisor_core::CoreError::ClassificationNotFound(_) => {
                    (StatusCode::NOT_FOUND, e.to_string())
                }
                metavisor_core::CoreError::TypeAlreadyExists(_)
                | metavisor_core::CoreError::EntityAlreadyExists(_)
                | metavisor_core::CoreError::RelationshipAlreadyExists(_)
                | metavisor_core::CoreError::Conflict(_) => (StatusCode::CONFLICT, e.to_string()),
                metavisor_core::CoreError::Validation(_)
                | metavisor_core::CoreError::InvalidAttribute(_) => {
                    (StatusCode::BAD_REQUEST, e.to_string())
                }
                _ => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
            },
            ApiError::NotFound(msg) => (StatusCode::NOT_FOUND, msg),
            ApiError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            ApiError::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
        };

        (status, Json(json!({ "error": message }))).into_response()
    }
}
