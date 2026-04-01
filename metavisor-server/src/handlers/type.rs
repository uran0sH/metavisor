//! Type API handlers

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};

use metavisor_core::{TypeDef, TypeHeader, TypesDef};

use crate::error::Result;
use crate::handlers::MetavisorAppState;

/// Get all type definitions
pub async fn get_all_types(State(state): State<MetavisorAppState>) -> Result<Json<TypesDef>> {
    // Single scan to get all type definitions (more efficient than list_types + get_type for each)
    let type_defs = state.store.list_type_defs().await?;

    let mut result = TypesDef::new();
    for type_def in type_defs {
        add_type_to_result(&mut result, type_def);
    }

    Ok(Json(result))
}

/// Get type headers (minimal info)
pub async fn list_type_headers(
    State(state): State<MetavisorAppState>,
) -> Result<Json<Vec<TypeHeader>>> {
    let type_names = state.store.list_types().await?;
    let mut headers = Vec::new();

    for name in type_names {
        if let Ok(type_def) = state.store.get_type(&name).await {
            let guid = type_guid(&type_def);
            let category = type_def.category();
            headers.push(TypeHeader {
                guid,
                name: type_def.name().to_string(),
                category,
            });
        }
    }

    Ok(Json(headers))
}

/// Get type by name
pub async fn get_type_by_name(
    State(state): State<MetavisorAppState>,
    Path(name): Path<String>,
) -> Result<Json<TypeDef>> {
    let type_def = state.store.get_type(&name).await?;
    Ok(Json(type_def))
}

/// Get type by GUID
pub async fn get_type_by_guid(
    State(state): State<MetavisorAppState>,
    Path(guid): Path<String>,
) -> Result<Json<TypeDef>> {
    // Use O(1) GUID index lookup instead of O(n) scan
    let type_def = state.store.get_type_by_guid(&guid).await?;
    Ok(Json(type_def))
}

/// Create types
pub async fn create_types(
    State(state): State<MetavisorAppState>,
    Json(req): Json<TypesDef>,
) -> Result<(StatusCode, Json<TypesDef>)> {
    // Collect all type definitions for atomic batch creation
    let all_types = collect_type_defs(req);

    // Create all types atomically
    state.store.batch_create_types(&all_types).await?;

    // Build response from the successfully created types
    let mut created = TypesDef::new();
    for type_def in all_types {
        created.push(type_def);
    }

    Ok((StatusCode::CREATED, Json(created)))
}

/// Update types
pub async fn update_types(
    State(state): State<MetavisorAppState>,
    Json(req): Json<TypesDef>,
) -> Result<Json<TypesDef>> {
    // Collect all type definitions for atomic batch update
    let all_types = collect_type_defs(req);

    // Update all types atomically
    state.store.batch_update_types(&all_types).await?;

    // Build response
    let mut updated = TypesDef::new();
    for type_def in all_types {
        updated.push(type_def);
    }

    Ok(Json(updated))
}

/// Delete types by name
pub async fn delete_types(
    State(state): State<MetavisorAppState>,
    Json(req): Json<TypesDef>,
) -> Result<StatusCode> {
    let type_names = collect_type_names(&req);

    // Validate: filter out empty names and warn
    let type_names: Vec<String> = type_names
        .into_iter()
        .filter(|name| {
            if name.trim().is_empty() {
                tracing::warn!("Skipping empty type name in delete_types request");
                false
            } else {
                true
            }
        })
        .collect();

    // If no valid names after filtering, return early
    if type_names.is_empty() {
        return Ok(StatusCode::NO_CONTENT);
    }

    state.store.batch_delete_types(&type_names).await?;

    Ok(StatusCode::NO_CONTENT)
}

// Helper functions

fn generate_guid(name: &str) -> String {
    // Simple GUID generation from name using SHA-256
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(name.as_bytes());
    let hash = hasher.finalize();

    // Format as UUID-like string
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        hash[0], hash[1], hash[2], hash[3],
        hash[4], hash[5],
        hash[6], hash[7],
        hash[8], hash[9],
        hash[10], hash[11], hash[12], hash[13], hash[14], hash[15]
    )
}

fn type_guid(type_def: &TypeDef) -> String {
    type_def
        .guid()
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| generate_guid(type_def.name()))
}

fn collect_type_defs(req: TypesDef) -> Vec<TypeDef> {
    req.entity_defs
        .into_iter()
        .map(TypeDef::from)
        .chain(req.classification_defs.into_iter().map(TypeDef::from))
        .chain(req.struct_defs.into_iter().map(TypeDef::from))
        .chain(req.enum_defs.into_iter().map(TypeDef::from))
        .chain(req.relationship_defs.into_iter().map(TypeDef::from))
        .chain(req.business_metadata_defs.into_iter().map(TypeDef::from))
        .collect()
}

fn collect_type_names(req: &TypesDef) -> Vec<String> {
    req.entity_defs
        .iter()
        .map(|def| def.name.clone())
        .chain(req.classification_defs.iter().map(|def| def.name.clone()))
        .chain(req.struct_defs.iter().map(|def| def.name.clone()))
        .chain(req.enum_defs.iter().map(|def| def.name.clone()))
        .chain(req.relationship_defs.iter().map(|def| def.name.clone()))
        .chain(
            req.business_metadata_defs
                .iter()
                .map(|def| def.name.clone()),
        )
        .collect()
}

fn add_type_to_result(result: &mut TypesDef, type_def: TypeDef) {
    result.push(type_def);
}

/// Delete type by name
///
/// DELETE /api/metavisor/v1/types/typedef/name/{name}
pub async fn delete_type_by_name(
    State(state): State<MetavisorAppState>,
    Path(name): Path<String>,
) -> Result<StatusCode> {
    state.store.delete_type(&name).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Get relationship type by name
///
/// GET /api/metavisor/v1/types/relationshipdef/name/{name}
pub async fn get_relationship_def_by_name(
    State(state): State<MetavisorAppState>,
    Path(name): Path<String>,
) -> Result<Json<TypeDef>> {
    let type_def = state.store.get_type(&name).await?;

    // Verify it's a relationship type
    match &type_def {
        TypeDef::Relationship(_) => {}
        _ => {
            return Err(crate::error::ApiError::NotFound(format!(
                "Type '{}' is not a relationship type",
                name
            )));
        }
    }

    Ok(Json(type_def))
}

/// List all relationship type definitions
///
/// GET /api/metavisor/v1/types/relationshipdefs
pub async fn list_relationship_defs(
    State(state): State<MetavisorAppState>,
) -> Result<Json<TypesDef>> {
    let type_names = state.store.list_types().await?;

    let mut result = TypesDef::new();

    for name in type_names {
        if let Ok(TypeDef::Relationship(def)) = state.store.get_type(&name).await {
            result.relationship_defs.push(def);
        }
    }

    Ok(Json(result))
}

/// Delete relationship type by name
///
/// DELETE /api/metavisor/v1/types/relationshipdef/name/{name}
pub async fn delete_relationship_def_by_name(
    State(state): State<MetavisorAppState>,
    Path(name): Path<String>,
) -> Result<StatusCode> {
    // Verify it's a relationship type first
    let type_def = state.store.get_type(&name).await?;
    match &type_def {
        TypeDef::Relationship(_) => {}
        _ => {
            return Err(crate::error::ApiError::NotFound(format!(
                "Type '{}' is not a relationship type",
                name
            )));
        }
    }

    state.store.delete_type(&name).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;
    use metavisor_core::{
        BusinessMetadataDef, ClassificationDef, EntityDef, EnumDef, RelationshipDef, StructDef,
    };

    #[test]
    fn test_type_guid_prefers_explicit_guid() {
        let mut entity = EntityDef::new("Table");
        entity.guid = Some("atlas-guid".to_string());

        assert_eq!(type_guid(&TypeDef::from(entity)), "atlas-guid");
    }

    #[test]
    fn test_type_guid_falls_back_to_generated_guid() {
        let type_def = TypeDef::from(EntityDef::new("Table"));
        assert_eq!(type_guid(&type_def), generate_guid("Table"));
    }

    #[test]
    fn test_collect_type_names_preserves_all_categories() {
        let req = TypesDef {
            entity_defs: vec![EntityDef::new("EntityA")],
            classification_defs: vec![ClassificationDef::new("ClassA")],
            struct_defs: vec![StructDef::new("StructA")],
            enum_defs: vec![EnumDef::new("EnumA")],
            relationship_defs: vec![RelationshipDef::new("RelA")],
            business_metadata_defs: vec![BusinessMetadataDef::new("BmA")],
        };

        assert_eq!(
            collect_type_names(&req),
            vec!["EntityA", "ClassA", "StructA", "EnumA", "RelA", "BmA"]
        );
    }
}
