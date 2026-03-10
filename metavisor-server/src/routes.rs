//! API routes

use axum::{
    routing::{delete, get, post, put},
    Router,
};
use std::sync::Arc;

use metavisor_core::{EntityStore, TypeStore};

use crate::handlers::{
    create_entities, create_entity, create_types, delete_entity_by_guid, delete_types,
    get_all_types, get_entity_by_guid, get_type_by_guid, get_type_by_name, list_type_headers,
    update_entity, update_types, AppState, EntityAppState,
};

/// Combined application state
#[derive(Clone)]
pub struct AppCombinedState {
    pub type_store: Arc<dyn TypeStore>,
    pub entity_store: Arc<dyn EntityStore>,
}

/// Create the API router
pub fn create_router(type_store: Arc<dyn TypeStore>, entity_store: Arc<dyn EntityStore>) -> Router {
    // Create type-specific states for handlers
    let type_state = AppState {
        type_store: type_store,
    };
    let entity_state = EntityAppState { entity_store };

    Router::new()
        // Health check
        .route("/health", get(health))
        .route("/api/metavisor/v1", get(api_info))
        // Type management
        .route(
            "/api/metavisor/v1/types/typedefs",
            get(get_all_types).with_state(type_state.clone()),
        )
        .route(
            "/api/metavisor/v1/types/typedefs",
            post(create_types).with_state(type_state.clone()),
        )
        .route(
            "/api/metavisor/v1/types/typedefs",
            put(update_types).with_state(type_state.clone()),
        )
        .route(
            "/api/metavisor/v1/types/typedefs",
            delete(delete_types).with_state(type_state.clone()),
        )
        .route(
            "/api/metavisor/v1/types/typedefs/headers",
            get(list_type_headers).with_state(type_state.clone()),
        )
        .route(
            "/api/metavisor/v1/types/typedef/name/{name}",
            get(get_type_by_name).with_state(type_state.clone()),
        )
        .route(
            "/api/metavisor/v1/types/typedef/guid/{guid}",
            get(get_type_by_guid).with_state(type_state),
        )
        // Entity management
        .route(
            "/api/metavisor/v1/entity",
            post(create_entity).with_state(entity_state.clone()),
        )
        .route(
            "/api/metavisor/v1/entity",
            put(update_entity).with_state(entity_state.clone()),
        )
        .route(
            "/api/metavisor/v1/entity/bulk",
            post(create_entities).with_state(entity_state.clone()),
        )
        .route(
            "/api/metavisor/v1/entity/guid/{guid}",
            get(get_entity_by_guid).with_state(entity_state.clone()),
        )
        .route(
            "/api/metavisor/v1/entity/guid/{guid}",
            delete(delete_entity_by_guid).with_state(entity_state),
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
