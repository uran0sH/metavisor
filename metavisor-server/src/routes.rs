//! API routes

use axum::{
    routing::{delete, get, post, put},
    Router,
};
use std::sync::Arc;

use metavisor_core::TypeStore;

use crate::handlers::{
    create_types, delete_types, get_all_types, get_type_by_guid, get_type_by_name,
    list_type_headers, update_types, AppState,
};

/// Create the API router
pub fn create_router(type_store: Arc<dyn TypeStore>) -> Router {
    let state = AppState { type_store };

    Router::new()
        // Health check
        .route("/health", get(health))
        .route("/api/metavisor/v1", get(api_info))
        // Type management
        .route(
            "/api/metavisor/v1/types/typedefs",
            get(get_all_types).with_state(state.clone()),
        )
        .route(
            "/api/metavisor/v1/types/typedefs",
            post(create_types).with_state(state.clone()),
        )
        .route(
            "/api/metavisor/v1/types/typedefs",
            put(update_types).with_state(state.clone()),
        )
        .route(
            "/api/metavisor/v1/types/typedefs",
            delete(delete_types).with_state(state.clone()),
        )
        .route(
            "/api/metavisor/v1/types/typedefs/headers",
            get(list_type_headers).with_state(state.clone()),
        )
        .route(
            "/api/metavisor/v1/types/typedef/name/{name}",
            get(get_type_by_name).with_state(state.clone()),
        )
        .route(
            "/api/metavisor/v1/types/typedef/guid/{guid}",
            get(get_type_by_guid).with_state(state),
        )
}

/// Health check endpoint
async fn health() -> &'static str {
    "OK"
}

/// API info endpoint
async fn api_info() -> &'static str {
    "Metavisor API v1"
}
