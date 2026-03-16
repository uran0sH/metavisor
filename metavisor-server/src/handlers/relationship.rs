//! Relationship API handlers (Atlas API v2 compatible)
//!
//! Based on: https://github.com/apache/atlas/blob/master/intg/src/main/java/org/apache/atlas/model/instance/

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use std::collections::HashMap;
use std::sync::Arc;

use metavisor_core::{
    EntityHeader, MetavisorStore, Relationship, RelationshipHeader, RelationshipWithExtInfo,
};

use crate::error::Result;

/// Application state containing stores
#[derive(Clone)]
pub struct RelationshipAppState {
    pub store: Arc<dyn MetavisorStore>,
}

/// Create a single relationship
///
/// POST /v2/relationship
pub async fn create_relationship(
    State(state): State<RelationshipAppState>,
    Json(relationship): Json<Relationship>,
) -> Result<(StatusCode, Json<RelationshipWithExtInfo>)> {
    let guid = state.store.create_relationship(&relationship).await?;
    let created = state.store.get_relationship(&guid).await?;

    Ok((
        StatusCode::CREATED,
        Json(RelationshipWithExtInfo {
            relationship: created,
            referred_entities: HashMap::new(),
        }),
    ))
}

/// Get relationship by GUID
///
/// GET /v2/relationship/guid/{guid}
pub async fn get_relationship_by_guid(
    State(state): State<RelationshipAppState>,
    Path(guid): Path<String>,
) -> Result<Json<RelationshipWithExtInfo>> {
    let relationship = state.store.get_relationship(&guid).await?;

    // Build referred entities map (entities at endpoints)
    let mut referred_entities = HashMap::new();

    if let Some(ref end1) = &relationship.end1 {
        if let Some(ref guid) = &end1.guid {
            referred_entities.insert(
                guid.clone(),
                EntityHeader::new(&end1.type_name).with_guid(guid.clone()),
            );
        }
    }

    if let Some(ref end2) = &relationship.end2 {
        if let Some(ref guid) = &end2.guid {
            referred_entities.insert(
                guid.clone(),
                EntityHeader::new(&end2.type_name).with_guid(guid.clone()),
            );
        }
    }

    Ok(Json(RelationshipWithExtInfo {
        relationship,
        referred_entities,
    }))
}

/// Update relationship
///
/// PUT /v2/relationship
pub async fn update_relationship(
    State(state): State<RelationshipAppState>,
    Json(relationship): Json<Relationship>,
) -> Result<Json<RelationshipWithExtInfo>> {
    state.store.update_relationship(&relationship).await?;

    let updated = state
        .store
        .get_relationship(relationship.guid.as_ref().unwrap())
        .await?;

    Ok(Json(RelationshipWithExtInfo {
        relationship: updated,
        referred_entities: HashMap::new(),
    }))
}

/// Delete relationship by GUID
///
/// DELETE /v2/relationship/guid/{guid}
pub async fn delete_relationship_by_guid(
    State(state): State<RelationshipAppState>,
    Path(guid): Path<String>,
) -> Result<StatusCode> {
    state.store.delete_relationship(&guid).await?;

    Ok(StatusCode::NO_CONTENT)
}

/// List relationships by entity GUID (endpoint)
///
/// GET /v2/relationship/entity/{entityGuid}
pub async fn list_relationships_by_entity(
    State(state): State<RelationshipAppState>,
    Path(entity_guid): Path<String>,
) -> Result<Json<Vec<RelationshipHeader>>> {
    let headers = state
        .store
        .list_relationships_by_entity(&entity_guid)
        .await?;
    Ok(Json(headers))
}

/// List relationships by type name
///
/// GET /v2/relationship/type/{typeName}
pub async fn list_relationships_by_type(
    State(state): State<RelationshipAppState>,
    Path(type_name): Path<String>,
) -> Result<Json<Vec<RelationshipHeader>>> {
    let headers = state.store.list_relationships_by_type(&type_name).await?;
    Ok(Json(headers))
}
