//! Entity API handlers (Atlas API v2 compatible)
//!
//! Based on: https://github.com/apache/atlas/blob/master/intg/src/main/java/org/apache/atlas/model/instance/

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;

use metavisor_core::{EntitiesWithExtInfo, Entity, EntityHeader, EntityRequest, MetavisorStore};

use crate::error::Result;

/// Application state containing stores
#[derive(Clone)]
pub struct EntityAppState {
    pub store: Arc<dyn MetavisorStore>,
}

#[derive(Serialize)]
pub struct EntityApiResponse {
    #[serde(rename = "entity")]
    entity: Entity,
    #[serde(rename = "referredEntities", skip_serializing_if = "HashMap::is_empty")]
    referred_entities: HashMap<String, Entity>,
}

impl EntityApiResponse {
    fn new(entity: Entity, referred_entities: HashMap<String, Entity>) -> Self {
        Self {
            entity,
            referred_entities,
        }
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
pub enum EntityPayload {
    Wrapped(EntityRequest),
    Plain(Entity),
}

impl EntityPayload {
    fn into_entity(self) -> Entity {
        match self {
            Self::Wrapped(request) => request.entity,
            Self::Plain(entity) => entity,
        }
    }
}

/// Validate entity input at handler layer
fn validate_entity_input(entity: &Entity) -> crate::error::Result<()> {
    // 1. Validate type_name is not empty
    if entity.type_name.trim().is_empty() {
        return Err(crate::error::ApiError::BadRequest(
            "Entity typeName cannot be empty".to_string(),
        ));
    }

    // 2. Validate GUID format if provided
    if let Some(ref guid) = entity.guid {
        if guid.trim().is_empty() {
            return Err(crate::error::ApiError::BadRequest(
                "Entity GUID cannot be empty string".to_string(),
            ));
        }
        // UUID format validation (basic check)
        if guid.len() < 32 {
            return Err(crate::error::ApiError::BadRequest(format!(
                "Invalid GUID format: '{}'",
                guid
            )));
        }
    }

    Ok(())
}

/// Create a single entity
///
/// POST /v2/entity
///
/// Request body (Atlas API v2 compatible):
/// ```json
/// {
///   "entity": {
///     "typeName": "column_meta",
///     "attributes": {...}
///   }
/// }
/// ```
pub async fn create_entity(
    State(state): State<EntityAppState>,
    Json(payload): Json<EntityPayload>,
) -> Result<(StatusCode, Json<EntityApiResponse>)> {
    let entity = payload.into_entity();

    // Handler-level input validation
    validate_entity_input(&entity)?;

    let guid = state.store.create_entity(&entity).await?;

    // Return the created entity with the generated GUID
    let mut created = entity;
    created.guid = Some(guid);

    Ok((
        StatusCode::CREATED,
        Json(EntityApiResponse::new(created, HashMap::new())),
    ))
}

/// Create multiple entities atomically
///
/// POST /v2/entity/bulk
///
/// Either all entities are created successfully, or none are (all-or-nothing).
pub async fn create_entities(
    State(state): State<EntityAppState>,
    Json(entities): Json<Vec<Entity>>,
) -> Result<(StatusCode, Json<EntitiesWithExtInfo>)> {
    // Handler-level input validation for all entities
    for entity in &entities {
        validate_entity_input(entity)?;
    }

    // Use atomic batch creation (all-or-nothing)
    let guids = state.store.batch_create_entities(&entities).await?;

    // Build response with created entities
    let mut created = EntitiesWithExtInfo::new();
    for (mut entity, guid) in entities.into_iter().zip(guids) {
        entity.guid = Some(guid);
        created.add_entity(entity);
    }

    Ok((StatusCode::CREATED, Json(created)))
}

/// Get entity by GUID
///
/// GET /v2/entity/guid/{guid}
pub async fn get_entity_by_guid(
    State(state): State<EntityAppState>,
    Path(guid): Path<String>,
) -> Result<Json<EntityApiResponse>> {
    let entity = state.store.get_entity(&guid).await?;
    Ok(Json(EntityApiResponse::new(entity, HashMap::new())))
}

/// Update entity
///
/// PUT /v2/entity
///
/// Request body (Atlas API v2 compatible):
/// ```json
/// {
///   "entity": {
///     "guid": "...",
///     "typeName": "column_meta",
///     "attributes": {...}
///   }
/// }
/// ```
pub async fn update_entity(
    State(state): State<EntityAppState>,
    Json(payload): Json<EntityPayload>,
) -> Result<Json<EntityApiResponse>> {
    let entity = payload.into_entity();
    state.store.update_entity(&entity).await?;
    Ok(Json(EntityApiResponse::new(entity, HashMap::new())))
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

/// Query parameters for uniqueAttribute lookup
///
/// Supports query parameters like `attr:qualifiedName=value`
#[derive(Debug, Deserialize)]
pub struct UniqueAttributeQueryParams {
    /// Dynamic attribute filters (e.g., attr:qualifiedName)
    #[serde(flatten)]
    pub attrs: HashMap<String, serde_json::Value>,
}

/// Get entity by type name and unique attributes
///
/// GET /api/metavisor/v1/entity/uniqueAttribute/type/{type}?attr:qualifiedName=value
pub async fn get_entity_by_unique_attribute(
    State(state): State<EntityAppState>,
    Path(type_name): Path<String>,
    Query(params): Query<UniqueAttributeQueryParams>,
) -> Result<Json<EntityApiResponse>> {
    // Extract attribute filters from query params (skip internal params)
    let mut unique_attrs = HashMap::new();

    for (key, value) in &params.attrs {
        // Handle "attr:" prefix for attribute names
        if let Some(attr_name) = key.strip_prefix("attr:") {
            // Convert value to string if it's a string, otherwise use as-is
            let attr_value = if let Some(s) = value.as_str() {
                serde_json::Value::String(s.to_string())
            } else {
                value.clone()
            };
            unique_attrs.insert(attr_name.to_string(), attr_value);
        }
    }

    let entity = state
        .store
        .get_entity_by_unique_attrs(&type_name, &unique_attrs)
        .await?;

    Ok(Json(EntityApiResponse::new(entity, HashMap::new())))
}
