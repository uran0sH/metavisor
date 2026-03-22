//! Search API handlers

use axum::{extract::State, Json};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use metavisor_core::{MetavisorStore, RelationshipHeader};

use crate::error::Result;

#[derive(Clone)]
pub struct SearchAppState {
    pub store: Arc<dyn MetavisorStore>,
}

#[derive(Debug, Deserialize)]
pub struct BasicSearchRequest {
    #[serde(rename = "typeName")]
    pub type_name: Option<String>,
    pub query: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct RelationSearchRequest {
    #[serde(rename = "typeName")]
    pub type_name: String,
    #[serde(rename = "relationshipFilters")]
    pub relationship_filters: Option<RelationshipFilters>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct RelationshipFilters {
    pub end1: Option<EndpointFilter>,
    pub end2: Option<EndpointFilter>,
}

#[derive(Debug, Deserialize)]
pub struct EndpointFilter {
    #[serde(rename = "typeName")]
    pub type_name: Option<String>,
    #[serde(rename = "uniqueAttributes")]
    pub unique_attributes: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Debug, Serialize)]
pub struct SearchResponse<T> {
    pub results: Vec<T>,
    pub total: usize,
    pub limit: usize,
    pub offset: usize,
}

pub async fn basic_search(
    State(state): State<SearchAppState>,
    Json(request): Json<BasicSearchRequest>,
) -> Result<Json<SearchResponse<RelationshipHeader>>> {
    let mut results = if let Some(type_name) = request.type_name.as_deref() {
        state.store.list_relationships_by_type(type_name).await?
    } else {
        state.store.list_relationships().await?
    };

    if let Some(query) = request.query.as_deref() {
        if let Some(qualified_name) = extract_qualified_name(query) {
            let mut filtered = Vec::new();
            for header in results {
                if relationship_has_qualified_name(&state, &header, &qualified_name).await? {
                    filtered.push(header);
                }
            }
            results = filtered;
        }
    }

    let (results, total, limit, offset) = paginate(results, request.limit, request.offset);
    Ok(Json(SearchResponse {
        results,
        total,
        limit,
        offset,
    }))
}

pub async fn search_relations(
    State(state): State<SearchAppState>,
    Json(request): Json<RelationSearchRequest>,
) -> Result<Json<SearchResponse<RelationshipHeader>>> {
    let mut results = state
        .store
        .list_relationships_by_type(&request.type_name)
        .await?;

    if let Some(filters) = &request.relationship_filters {
        let end1_filter = filters.end1.as_ref();
        let end2_filter = filters.end2.as_ref();

        let mut filtered = Vec::new();
        for header in results {
            let Some(guid) = header.guid.as_deref() else {
                continue;
            };
            let relationship = state.store.get_relationship(guid).await?;

            if matches_endpoint_filter(&state, relationship.end1.as_ref(), end1_filter).await?
                && matches_endpoint_filter(&state, relationship.end2.as_ref(), end2_filter).await?
            {
                filtered.push(header);
            }
        }
        results = filtered;
    }

    let (results, total, limit, offset) = paginate(results, request.limit, request.offset);
    Ok(Json(SearchResponse {
        results,
        total,
        limit,
        offset,
    }))
}

fn paginate<T>(
    mut items: Vec<T>,
    limit: Option<usize>,
    offset: Option<usize>,
) -> (Vec<T>, usize, usize, usize) {
    let offset = offset.unwrap_or(0);
    let limit = limit.unwrap_or(50);
    let total = items.len();
    let results = items.drain(..).skip(offset).take(limit).collect();
    (results, total, limit, offset)
}

fn extract_qualified_name(query: &str) -> Option<String> {
    let marker = "end2.uniqueAttributes.qualifiedName = '";
    let start = query.find(marker)? + marker.len();
    let tail = &query[start..];
    let end = tail.find('\'')?;
    Some(tail[..end].to_string())
}

async fn relationship_has_qualified_name(
    state: &SearchAppState,
    header: &RelationshipHeader,
    qualified_name: &str,
) -> Result<bool> {
    for endpoint in [header.end1.as_ref(), header.end2.as_ref()]
        .into_iter()
        .flatten()
    {
        if endpoint
            .unique_attributes
            .get("qualifiedName")
            .and_then(|v| v.as_str())
            == Some(qualified_name)
        {
            return Ok(true);
        }

        if let Some(guid) = endpoint.guid.as_deref() {
            let entity = state.store.get_entity(guid).await?;
            if entity
                .attributes
                .get("qualifiedName")
                .and_then(|v| v.as_str())
                == Some(qualified_name)
            {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

async fn matches_endpoint_filter(
    state: &SearchAppState,
    endpoint: Option<&metavisor_core::ObjectId>,
    filter: Option<&EndpointFilter>,
) -> Result<bool> {
    let Some(filter) = filter else {
        return Ok(true);
    };
    let Some(endpoint) = endpoint else {
        return Ok(false);
    };

    if let Some(expected_type) = filter.type_name.as_deref() {
        if endpoint.type_name != expected_type {
            return Ok(false);
        }
    }

    if let Some(unique_attributes) = filter.unique_attributes.as_ref() {
        let entity = if let Some(guid) = endpoint.guid.as_deref() {
            state.store.get_entity(guid).await?
        } else {
            state
                .store
                .get_entity_by_unique_attrs(&endpoint.type_name, &endpoint.unique_attributes)
                .await?
        };

        for (key, expected) in unique_attributes {
            if entity.attributes.get(key) != Some(expected) {
                return Ok(false);
            }
        }
    }

    Ok(true)
}
