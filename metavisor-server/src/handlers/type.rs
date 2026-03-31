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
    let type_names = state.store.list_types().await?;

    let mut result = TypesDef::new();

    for name in type_names {
        if let Ok(type_def) = state.store.get_type(&name).await {
            add_type_to_result(&mut result, type_def);
        }
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
            let guid = type_def
                .guid()
                .map(|g| g.to_string())
                .unwrap_or_else(|| generate_guid(type_def.name()));
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
    // GUID is derived from name, so we need to list all types and find matching GUID
    let type_names = state.store.list_types().await?;

    for name in type_names {
        if generate_guid(&name) == guid {
            let type_def = state.store.get_type(&name).await?;
            return Ok(Json(type_def));
        }
    }

    Err(crate::error::ApiError::NotFound(format!(
        "Type with GUID {} not found",
        guid
    )))
}

/// Create types
pub async fn create_types(
    State(state): State<MetavisorAppState>,
    Json(req): Json<TypesDef>,
) -> Result<(StatusCode, Json<TypesDef>)> {
    // Collect all type definitions for atomic batch creation
    let all_types: Vec<TypeDef> = req
        .entity_defs
        .into_iter()
        .map(TypeDef::from)
        .chain(req.classification_defs.into_iter().map(TypeDef::from))
        .chain(req.struct_defs.into_iter().map(TypeDef::from))
        .chain(req.enum_defs.into_iter().map(TypeDef::from))
        .chain(req.relationship_defs.into_iter().map(TypeDef::from))
        .chain(req.business_metadata_defs.into_iter().map(TypeDef::from))
        .collect();

    // Create all types atomically
    state.store.batch_create_types(&all_types).await?;

    // Build response from the successfully created types
    let mut created = TypesDef::new();
    for type_def in all_types {
        match type_def {
            TypeDef::Entity(def) => created.entity_defs.push(def),
            TypeDef::Classification(def) => created.classification_defs.push(def),
            TypeDef::Struct(def) => created.struct_defs.push(def),
            TypeDef::Enum(def) => created.enum_defs.push(def),
            TypeDef::Relationship(def) => created.relationship_defs.push(def),
            TypeDef::BusinessMetadata(def) => created.business_metadata_defs.push(def),
        }
    }

    Ok((StatusCode::CREATED, Json(created)))
}

/// Update types
pub async fn update_types(
    State(state): State<MetavisorAppState>,
    Json(req): Json<TypesDef>,
) -> Result<Json<TypesDef>> {
    // Collect all type definitions for atomic batch update
    let all_types: Vec<TypeDef> = req
        .entity_defs
        .into_iter()
        .map(TypeDef::from)
        .chain(req.classification_defs.into_iter().map(TypeDef::from))
        .chain(req.struct_defs.into_iter().map(TypeDef::from))
        .chain(req.enum_defs.into_iter().map(TypeDef::from))
        .chain(req.relationship_defs.into_iter().map(TypeDef::from))
        .chain(req.business_metadata_defs.into_iter().map(TypeDef::from))
        .collect();

    // Update all types atomically
    state.store.batch_update_types(&all_types).await?;

    // Build response
    let mut updated = TypesDef::new();
    for type_def in all_types {
        match type_def {
            TypeDef::Entity(def) => updated.entity_defs.push(def),
            TypeDef::Classification(def) => updated.classification_defs.push(def),
            TypeDef::Struct(def) => updated.struct_defs.push(def),
            TypeDef::Enum(def) => updated.enum_defs.push(def),
            TypeDef::Relationship(def) => updated.relationship_defs.push(def),
            TypeDef::BusinessMetadata(def) => updated.business_metadata_defs.push(def),
        }
    }

    Ok(Json(updated))
}

/// Delete types by name
pub async fn delete_types(
    State(state): State<MetavisorAppState>,
    Json(req): Json<TypesDef>,
) -> Result<StatusCode> {
    // Delete entity types
    for entity_def in &req.entity_defs {
        state.store.delete_type(&entity_def.name).await?;
    }

    // Delete classification types
    for class_def in &req.classification_defs {
        state.store.delete_type(&class_def.name).await?;
    }

    // Delete struct types
    for struct_def in &req.struct_defs {
        state.store.delete_type(&struct_def.name).await?;
    }

    // Delete enum types
    for enum_def in &req.enum_defs {
        state.store.delete_type(&enum_def.name).await?;
    }

    // Delete relationship types
    for rel_def in &req.relationship_defs {
        state.store.delete_type(&rel_def.name).await?;
    }

    // Delete business metadata types
    for bm_def in &req.business_metadata_defs {
        state.store.delete_type(&bm_def.name).await?;
    }

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

fn add_type_to_result(result: &mut TypesDef, type_def: TypeDef) {
    match type_def {
        TypeDef::Entity(def) => {
            result.entity_defs.push(def);
        }
        TypeDef::Classification(def) => {
            result.classification_defs.push(def);
        }
        TypeDef::Struct(def) => {
            result.struct_defs.push(def);
        }
        TypeDef::Enum(def) => {
            result.enum_defs.push(def);
        }
        TypeDef::Relationship(def) => {
            result.relationship_defs.push(def);
        }
        TypeDef::BusinessMetadata(def) => {
            result.business_metadata_defs.push(def);
        }
    }
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
