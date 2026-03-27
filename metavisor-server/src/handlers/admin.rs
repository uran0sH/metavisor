use axum::{extract::State, Json};
use serde::Serialize;
use std::sync::Arc;

use metavisor_core::MetavisorStore;
use metavisor_storage::DefaultMetavisorStore;

use crate::error::Result;

#[derive(Clone)]
pub struct AdminAppState {
    pub store: Arc<DefaultMetavisorStore>,
}

#[derive(Serialize)]
pub struct StorageStatusResponse {
    pub pending_projection_repairs: usize,
    pub graph: GraphStatusResponse,
}

#[derive(Serialize)]
pub struct GraphStatusResponse {
    pub node_count: usize,
    pub edge_count: usize,
}

#[derive(Serialize)]
pub struct RepairResponse {
    pub pending_projection_repairs_before: usize,
    pub pending_projection_repairs_after: usize,
    pub repaired_entities: usize,
    pub repaired_relationships: usize,
    pub failed_entities: usize,
    pub failed_relationships: usize,
}

pub async fn get_storage_status(State(state): State<AdminAppState>) -> Json<StorageStatusResponse> {
    let graph_stats = state.store.graph_stats();
    Json(StorageStatusResponse {
        pending_projection_repairs: state.store.pending_projection_repairs(),
        graph: GraphStatusResponse {
            node_count: graph_stats.node_count,
            edge_count: graph_stats.edge_count,
        },
    })
}

pub async fn repair_storage(State(state): State<AdminAppState>) -> Result<Json<RepairResponse>> {
    let pending_before = state.store.pending_projection_repairs();
    let (_report, repair_result) = state.store.repair_consistency().await?;

    Ok(Json(RepairResponse {
        pending_projection_repairs_before: pending_before,
        pending_projection_repairs_after: state.store.pending_projection_repairs(),
        repaired_entities: repair_result.repaired_entities,
        repaired_relationships: repair_result.repaired_relationships,
        failed_entities: repair_result.failed_entities,
        failed_relationships: repair_result.failed_relationships,
    }))
}
