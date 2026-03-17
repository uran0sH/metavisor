//! API routes

use axum::{
    routing::{delete, get, post, put},
    Router,
};
use std::sync::Arc;

use metavisor_core::MetavisorStore;

use crate::handlers::{
    add_classifications, create_entities, create_entity, create_relationship, create_types,
    delete_entity_by_guid, delete_relationship_by_guid, delete_types, get_all_classifications,
    get_all_types, get_classifications, get_entity_by_guid, get_graph_stats, get_input_lineage,
    get_lineage_graph, get_output_lineage, get_relationship_by_guid, get_type_by_guid,
    get_type_by_name, list_relationships_by_entity, list_relationships_by_type, list_type_headers,
    rebuild_graph, remove_classification, update_classifications, update_entity,
    update_relationship, update_types, ClassificationAppState, EntityAppState, GraphAppState,
    MetavisorAppState, RelationshipAppState,
};
use crate::mcp::{McpHttpService, McpState};

/// Create the API router
pub fn create_router(store: Arc<dyn MetavisorStore>) -> Router {
    // Create type-specific states for handlers
    let type_state = MetavisorAppState {
        store: store.clone(),
    };
    let entity_state = EntityAppState {
        store: store.clone(),
    };
    let relationship_state = RelationshipAppState {
        store: store.clone(),
    };
    let graph_state = GraphAppState {
        store: store.clone(),
    };
    let classification_state = ClassificationAppState {
        store: store.clone(),
    };
    let mcp_state = McpState { store };

    // Create MCP HTTP service with proper session management
    let mcp_service = McpHttpService::new(mcp_state);

    Router::new()
        // Health check
        .route("/health", get(health))
        .route("/api/metavisor/v1", get(api_info))
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
        // Lineage endpoints (Atlas-compatible)
        .route(
            "/api/metavisor/v1/lineage/{guid}",
            get(get_lineage_graph).with_state(graph_state.clone()),
        )
        // Convenience endpoints for input/output lineage
        .route(
            "/api/metavisor/v1/lineage/{guid}/inputs",
            get(get_input_lineage).with_state(graph_state.clone()),
        )
        .route(
            "/api/metavisor/v1/lineage/{guid}/outputs",
            get(get_output_lineage).with_state(graph_state.clone()),
        )
        // Graph management
        .route(
            "/api/metavisor/v1/graph/rebuild",
            post(rebuild_graph).with_state(graph_state.clone()),
        )
        .route(
            "/api/metavisor/v1/graph/stats",
            get(get_graph_stats).with_state(graph_state.clone()),
        )
        // Classification management (Atlas-compatible)
        .route(
            "/api/metavisor/v1/entity/guid/{guid}/classifications",
            get(get_classifications).with_state(classification_state.clone()),
        )
        .route(
            "/api/metavisor/v1/entity/guid/{guid}/classifications/all",
            get(get_all_classifications).with_state(classification_state.clone()),
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
