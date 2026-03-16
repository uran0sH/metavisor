//! Entity API handlers (Atlas API v2 compatible)
//!
//! Based on: https://github.com/apache/atlas/blob/master/intg/src/main/java/org/apache/atlas/model/instance/

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use std::sync::Arc;

use metavisor_core::{
    EntitiesWithExtInfo, Entity, EntityHeader, EntityWithExtInfo, MetavisorStore,
};

use crate::error::Result;

/// Application state containing stores
#[derive(Clone)]
pub struct EntityAppState {
    pub store: Arc<dyn MetavisorStore>,
}

/// Create a single entity
///
/// POST /v2/entity
pub async fn create_entity(
    State(state): State<EntityAppState>,
    Json(entity): Json<Entity>,
) -> Result<(StatusCode, Json<EntityWithExtInfo>)> {
    let guid = state.store.create_entity(&entity).await?;

    // Return the created entity with the generated GUID
    let mut created = entity;
    created.guid = Some(guid);

    Ok((StatusCode::CREATED, Json(EntityWithExtInfo::new(created))))
}

/// Create multiple entities
///
/// POST /v2/entity/bulk
pub async fn create_entities(
    State(state): State<EntityAppState>,
    Json(entities): Json<Vec<Entity>>,
) -> Result<(StatusCode, Json<EntitiesWithExtInfo>)> {
    let mut created = EntitiesWithExtInfo::new();

    for entity in entities {
        let guid = state.store.create_entity(&entity).await?;
        let mut entity_with_guid = entity;
        entity_with_guid.guid = Some(guid);
        created.add_entity(entity_with_guid);
    }

    Ok((StatusCode::CREATED, Json(created)))
}

/// Get entity by GUID
///
/// GET /v2/entity/guid/{guid}
pub async fn get_entity_by_guid(
    State(state): State<EntityAppState>,
    Path(guid): Path<String>,
) -> Result<Json<EntityWithExtInfo>> {
    let entity = state.store.get_entity(&guid).await?;
    Ok(Json(EntityWithExtInfo::new(entity)))
}

/// Update entity
///
/// PUT /v2/entity
pub async fn update_entity(
    State(state): State<EntityAppState>,
    Json(entity): Json<Entity>,
) -> Result<Json<EntityWithExtInfo>> {
    state.store.update_entity(&entity).await?;
    Ok(Json(EntityWithExtInfo::new(entity)))
}

/// Delete entity by GUID
///
/// DELETE /v2/entity/guid/{guid}
pub async fn delete_entity_by_guid(
    State(state): State<EntityAppState>,
    Path(guid): Path<String>,
) -> Result<StatusCode> {
    state.store.delete_entity(&guid).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// List entity headers
///
/// GET /v2/entity/bulk/headers
#[allow(dead_code)]
pub async fn list_entity_headers(
    State(state): State<EntityAppState>,
) -> Result<Json<Vec<EntityHeader>>> {
    let headers = state.store.list_entities().await?;
    Ok(Json(headers))
}
