//! Lineage API handlers
//!
//! Provides endpoints for lineage tracking and graph operations

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;

use metavisor_core::{
    default_depth, default_true, LineageQueryOptions, LineageResult, MetavisorStore,
    TraversalDirection,
};

use crate::error::Result;

/// Application state containing graph store
#[derive(Clone)]
pub struct GraphAppState {
    pub store: Arc<dyn MetavisorStore>,
}

/// Query parameters for lineage endpoints
#[derive(Debug, Deserialize)]
pub struct LineageQueryParams {
    /// Maximum depth to traverse (default: 3)
    #[serde(default = "default_depth")]
    pub depth: usize,

    /// Filter by relationship types (comma-separated)
    pub relationship_types: Option<String>,

    /// Filter by entity types (comma-separated)
    pub entity_types: Option<String>,

    /// Include propagated classifications
    #[serde(default = "default_true")]
    pub include_propagated_classifications: bool,
}

impl From<LineageQueryParams> for LineageQueryOptions {
    fn from(params: LineageQueryParams) -> Self {
        let mut options = LineageQueryOptions::new()
            .with_depth(params.depth)
            .with_propagated_classifications(params.include_propagated_classifications);

        if let Some(types) = params.relationship_types {
            let types: Vec<String> = types.split(',').map(|s| s.trim().to_string()).collect();
            if !types.is_empty() {
                options = options.with_relationship_types(types);
            }
        }

        if let Some(types) = params.entity_types {
            let types: Vec<String> = types.split(',').map(|s| s.trim().to_string()).collect();
            if !types.is_empty() {
                options = options.with_entity_types(types);
            }
        }

        options
    }
}

/// Query parameters for direction
#[derive(Debug, Deserialize)]
pub struct DirectionQueryParams {
    /// Direction: INPUT, OUTPUT, or BOTH (default: BOTH)
    #[serde(default)]
    pub direction: Option<String>,

    /// Maximum depth to traverse (default: 3)
    #[serde(default = "default_depth")]
    pub depth: usize,

    /// Filter by relationship types (comma-separated)
    pub relationship_types: Option<String>,

    /// Filter by entity types (comma-separated)
    pub entity_types: Option<String>,

    /// Include propagated classifications
    #[serde(default = "default_true")]
    pub include_propagated_classifications: bool,
}

impl DirectionQueryParams {
    pub fn to_direction(&self) -> TraversalDirection {
        self.direction
            .as_ref()
            .and_then(|d| d.parse().ok())
            .unwrap_or(TraversalDirection::Both)
    }
}

/// Get input lineage (upstream) for an entity
///
/// GET /api/metavisor/v1/lineage/{guid}/inputs
pub async fn get_input_lineage(
    State(state): State<GraphAppState>,
    Path(guid): Path<String>,
    Query(params): Query<LineageQueryParams>,
) -> Result<Json<LineageResult>> {
    let options: LineageQueryOptions = params.into();
    let result = state
        .store
        .get_lineage(&guid, TraversalDirection::Input, options)
        .await?;
    Ok(Json(result))
}

/// Get output lineage (downstream) for an entity
///
/// GET /api/metavisor/v1/lineage/{guid}/outputs
pub async fn get_output_lineage(
    State(state): State<GraphAppState>,
    Path(guid): Path<String>,
    Query(params): Query<LineageQueryParams>,
) -> Result<Json<LineageResult>> {
    let options: LineageQueryOptions = params.into();
    let result = state
        .store
        .get_lineage(&guid, TraversalDirection::Output, options)
        .await?;
    Ok(Json(result))
}

/// Get full lineage graph for an entity
///
/// GET /api/metavisor/v1/lineage/{guid}/graph
pub async fn get_lineage_graph(
    State(state): State<GraphAppState>,
    Path(guid): Path<String>,
    Query(params): Query<DirectionQueryParams>,
) -> Result<Json<LineageResult>> {
    let direction = params.to_direction();
    let options: LineageQueryOptions = LineageQueryParams {
        depth: params.depth,
        relationship_types: params.relationship_types,
        entity_types: params.entity_types,
        include_propagated_classifications: params.include_propagated_classifications,
    }
    .into();

    let result = state.store.get_lineage(&guid, direction, options).await?;
    Ok(Json(result))
}

/// Rebuild the in-memory graph from persistent storage
///
/// POST /api/metavisor/v1/graph/rebuild
pub async fn rebuild_graph(State(state): State<GraphAppState>) -> Result<StatusCode> {
    state.store.rebuild_graph().await?;
    Ok(StatusCode::OK)
}

/// Get graph statistics
///
/// GET /api/metavisor/v1/graph/stats
pub async fn get_graph_stats(State(state): State<GraphAppState>) -> Result<Json<GraphStats>> {
    let stats = state.store.graph_stats();
    Ok(Json(GraphStats {
        node_count: stats.node_count,
        edge_count: stats.edge_count,
    }))
}

/// Graph statistics response
#[derive(Debug, serde::Serialize)]
pub struct GraphStats {
    pub node_count: usize,
    pub edge_count: usize,
}

#[derive(Debug, Serialize)]
pub struct LineageLookupResponse {
    pub guid: String,
    #[serde(flatten)]
    pub lineage: LineageResult,
}

/// Query parameters for uniqueAttribute lineage lookup
#[derive(Debug, Deserialize)]
pub struct UniqueAttributeQueryParams {
    /// Dynamic attribute filters (e.g., attr:qualifiedName)
    #[serde(flatten)]
    pub attrs: HashMap<String, serde_json::Value>,

    /// Direction: INPUT, OUTPUT, or BOTH (default: BOTH)
    #[serde(default)]
    pub direction: Option<String>,

    /// Maximum depth to traverse (default: 3)
    #[serde(default = "default_depth")]
    pub depth: usize,

    /// Filter by relationship types (comma-separated)
    pub relationship_types: Option<String>,

    /// Filter by entity types (comma-separated)
    pub entity_types: Option<String>,

    /// Include propagated classifications
    #[serde(default = "default_true")]
    pub include_propagated_classifications: bool,
}

/// Get lineage by type name and unique attributes
///
/// GET /api/metavisor/v1/lineage/uniqueAttribute/type/{type}?attr:qualifiedName=value&direction=BOTH
pub async fn get_lineage_by_unique_attribute(
    State(state): State<GraphAppState>,
    Path(type_name): Path<String>,
    Query(params): Query<UniqueAttributeQueryParams>,
) -> Result<Json<LineageLookupResponse>> {
    // Extract attribute filters from query params
    let mut unique_attrs = HashMap::new();

    for (key, value) in &params.attrs {
        if let Some(attr_name) = key.strip_prefix("attr:") {
            let attr_value = if let Some(s) = value.as_str() {
                serde_json::Value::String(s.to_string())
            } else {
                value.clone()
            };
            unique_attrs.insert(attr_name.to_string(), attr_value);
        }
    }

    // First get the entity to get its GUID
    let entity = state
        .store
        .get_entity_by_unique_attrs(&type_name, &unique_attrs)
        .await?;

    let guid = entity
        .guid
        .ok_or_else(|| crate::error::ApiError::NotFound("Entity has no GUID".to_string()))?;

    // Build lineage query options
    let mut options = LineageQueryOptions::new()
        .with_depth(params.depth)
        .with_propagated_classifications(params.include_propagated_classifications);

    if let Some(types) = &params.relationship_types {
        let types: Vec<String> = types.split(',').map(|s| s.trim().to_string()).collect();
        if !types.is_empty() {
            options = options.with_relationship_types(types);
        }
    }

    if let Some(types) = &params.entity_types {
        let types: Vec<String> = types.split(',').map(|s| s.trim().to_string()).collect();
        if !types.is_empty() {
            options = options.with_entity_types(types);
        }
    }

    // Parse direction
    let direction = params
        .direction
        .as_ref()
        .and_then(|d| d.parse().ok())
        .unwrap_or(TraversalDirection::Both);

    let result = state.store.get_lineage(&guid, direction, options).await?;

    // Return result with the entity GUID included
    Ok(Json(LineageLookupResponse {
        guid: guid.clone(),
        lineage: LineageResult {
            root_guid: guid,
            ..result
        },
    }))
}
