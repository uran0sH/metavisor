//! API routes

use axum::{
    routing::{delete, get, post, put},
    Router,
};
use std::sync::Arc;

use metavisor_core::MetavisorStore;
use metavisor_storage::DefaultMetavisorStore;

use crate::handlers::{
    add_classifications, basic_search, create_entities, create_entity, create_relationship,
    create_types, delete_entity_by_guid, delete_relationship_by_guid,
    delete_relationship_def_by_name, delete_type_by_name, delete_types, get_all_types,
    get_classifications, get_entity_by_guid, get_entity_by_unique_attribute,
    get_relationship_by_guid, get_relationship_def_by_name, get_storage_status, get_type_by_guid,
    get_type_by_name, list_relationship_defs, list_relationships_by_entity,
    list_relationships_by_type, list_type_headers, remove_classification, repair_storage,
    search_relations, update_classifications, update_entity, update_relationship, update_types,
    AdminAppState, ClassificationAppState, EntityAppState, MetavisorAppState, RelationshipAppState,
    SearchAppState,
};
use crate::mcp::{McpHttpService, McpState};

/// Create the API router
pub fn create_router(store: Arc<DefaultMetavisorStore>) -> Router {
    let store_dyn: Arc<dyn MetavisorStore> = store.clone();

    // Create type-specific states for handlers
    let type_state = MetavisorAppState {
        store: store_dyn.clone(),
    };
    let entity_state = EntityAppState {
        store: store_dyn.clone(),
    };
    let relationship_state = RelationshipAppState {
        store: store_dyn.clone(),
    };
    let classification_state = ClassificationAppState {
        store: store_dyn.clone(),
    };
    let search_state = SearchAppState {
        store: store_dyn.clone(),
    };
    let admin_state = AdminAppState {
        store: store.clone(),
    };
    let mcp_state = McpState { store: store_dyn };

    // Create MCP HTTP service with proper session management
    let mcp_service = McpHttpService::new(mcp_state);

    Router::new()
        // Health check
        .route("/health", get(health))
        .route("/api/metavisor/v1", get(api_info))
        .route(
            "/admin/storage/status",
            get(get_storage_status).with_state(admin_state),
        )
        .route(
            "/admin/storage/repair",
            post(repair_storage).with_state(AdminAppState {
                store: store.clone(),
            }),
        )
        // MCP endpoint
        .route("/mcp", post(handle_mcp).with_state(mcp_service.clone()))
        .route(
            "/mcp",
            axum::routing::get(handle_mcp).with_state(mcp_service.clone()),
        )
        .route(
            "/mcp",
            axum::routing::delete(handle_mcp).with_state(mcp_service),
        )
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
            get(get_type_by_guid).with_state(type_state.clone()),
        )
        .route(
            "/api/metavisor/v1/types/typedef/name/{name}",
            delete(delete_type_by_name).with_state(type_state.clone()),
        )
        .route(
            "/api/metavisor/v1/types/relationshipdef/name/{name}",
            get(get_relationship_def_by_name).with_state(type_state.clone()),
        )
        .route(
            "/api/metavisor/v1/types/relationshipdef/name/{name}",
            delete(delete_relationship_def_by_name).with_state(type_state.clone()),
        )
        .route(
            "/api/metavisor/v1/types/relationshipdefs",
            get(list_relationship_defs).with_state(type_state),
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
            delete(delete_entity_by_guid).with_state(entity_state.clone()),
        )
        .route(
            "/api/metavisor/v1/entity/uniqueAttribute/type/{type}",
            get(get_entity_by_unique_attribute).with_state(entity_state),
        )
        // Relationship management
        .route(
            "/api/metavisor/v1/relationship",
            post(create_relationship).with_state(relationship_state.clone()),
        )
        .route(
            "/api/metavisor/v1/relationship",
            put(update_relationship).with_state(relationship_state.clone()),
        )
        .route(
            "/api/metavisor/v1/relationship/guid/{guid}",
            get(get_relationship_by_guid).with_state(relationship_state.clone()),
        )
        .route(
            "/api/metavisor/v1/relationship/guid/{guid}",
            delete(delete_relationship_by_guid).with_state(relationship_state.clone()),
        )
        .route(
            "/api/metavisor/v1/relationship/entity/{entity_guid}",
            get(list_relationships_by_entity).with_state(relationship_state.clone()),
        )
        .route(
            "/api/metavisor/v1/relationship/type/{type_name}",
            get(list_relationships_by_type).with_state(relationship_state),
        )
        // Search
        .route(
            "/api/metavisor/v1/search/basic",
            post(basic_search).with_state(search_state.clone()),
        )
        .route(
            "/api/metavisor/v1/search/relations",
            post(search_relations).with_state(search_state),
        )
        // Classification management (Atlas-compatible)
        .route(
            "/api/metavisor/v1/entity/guid/{guid}/classifications",
            get(get_classifications).with_state(classification_state.clone()),
        )
        .route(
            "/api/metavisor/v1/entity/guid/{guid}/classifications",
            post(add_classifications).with_state(classification_state.clone()),
        )
        .route(
            "/api/metavisor/v1/entity/guid/{guid}/classifications",
            put(update_classifications).with_state(classification_state.clone()),
        )
        .route(
            "/api/metavisor/v1/entity/guid/{guid}/classifications/{classificationName}",
            delete(remove_classification).with_state(classification_state),
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
/// MCP endpoint handler - delegates to McpHttpService
async fn handle_mcp(
    axum::extract::State(service): axum::extract::State<McpHttpService>,
    req: http::Request<axum::body::Body>,
) -> axum::response::Response {
    service.handle(req).await
}
