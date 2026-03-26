//! Classification API handlers (Atlas API v2 compatible)
//!
//! Based on: https://github.com/apache/atlas/blob/master/intg/src/main/java/org/apache/atlas/model/instance/

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use std::sync::Arc;

use metavisor_core::{Classification, MetavisorStore};

use crate::error::Result;

/// Application state containing store
#[derive(Clone)]
pub struct ClassificationAppState {
    pub store: Arc<dyn MetavisorStore>,
}

/// Get classifications for an entity
///
/// GET /v2/entity/guid/{guid}/classifications
pub async fn get_classifications(
    State(state): State<ClassificationAppState>,
    Path(guid): Path<String>,
) -> Result<Json<Vec<Classification>>> {
    let classifications = state.store.get_classifications(&guid).await?;
    Ok(Json(classifications))
}

/// Add classifications to an entity
///
/// POST /v2/entity/guid/{guid}/classifications
pub async fn add_classifications(
    State(state): State<ClassificationAppState>,
    Path(guid): Path<String>,
    Json(classifications): Json<Vec<Classification>>,
) -> Result<StatusCode> {
    state
        .store
        .add_classifications(&guid, &classifications)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Update classifications for an entity (replace all)
///
/// PUT /v2/entity/guid/{guid}/classifications
pub async fn update_classifications(
    State(state): State<ClassificationAppState>,
    Path(guid): Path<String>,
    Json(classifications): Json<Vec<Classification>>,
) -> Result<StatusCode> {
    state
        .store
        .update_classifications(&guid, &classifications)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Remove a specific classification from an entity
///
/// DELETE /v2/entity/guid/{guid}/classification/{classificationName}
pub async fn remove_classification(
    State(state): State<ClassificationAppState>,
    Path((guid, classification_name)): Path<(String, String)>,
) -> Result<StatusCode> {
    state
        .store
        .remove_classification(&guid, &classification_name)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}
