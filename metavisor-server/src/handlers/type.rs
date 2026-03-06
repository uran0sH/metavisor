//! Type API handlers

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use std::sync::Arc;

use metavisor_core::{TypeDef, TypeHeader, TypeStore, TypesDef};

use crate::error::Result;

/// Application state containing stores
#[derive(Clone)]
pub struct AppState {
    pub type_store: Arc<dyn TypeStore>,
}

/// Get all type definitions
pub async fn get_all_types(State(state): State<AppState>) -> Result<Json<TypesDef>> {
    let type_names = state.type_store.list_types().await?;

    let mut result = TypesDef::new();

    for name in type_names {
        if let Ok(type_def) = state.type_store.get_type(&name).await {
            add_type_to_result(&mut result, type_def);
        }
    }

    Ok(Json(result))
}

/// Get type headers (minimal info)
pub async fn list_type_headers(State(state): State<AppState>) -> Result<Json<Vec<TypeHeader>>> {
    let type_names = state.type_store.list_types().await?;
    let mut headers = Vec::new();

    for name in type_names {
        if let Ok(type_def) = state.type_store.get_type(&name).await {
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
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<TypesDef>> {
    let type_def = state.type_store.get_type(&name).await?;

    let mut result = TypesDef::new();
    add_type_to_result(&mut result, type_def);

    Ok(Json(result))
}

/// Get type by GUID
pub async fn get_type_by_guid(
    State(state): State<AppState>,
    Path(guid): Path<String>,
) -> Result<Json<TypesDef>> {
    // GUID is derived from name, so we need to list all types and find matching GUID
    let type_names = state.type_store.list_types().await?;

    for name in type_names {
        if generate_guid(&name) == guid {
            return get_type_by_name(State(state), Path(name)).await;
        }
    }

    Err(crate::error::ApiError::NotFound(format!(
        "Type with GUID {} not found",
        guid
    )))
}

/// Create types
pub async fn create_types(
    State(state): State<AppState>,
    Json(req): Json<TypesDef>,
) -> Result<(StatusCode, Json<TypesDef>)> {
    let mut created = TypesDef::new();

    // Create entity types
    for entity_def in req.entity_defs {
        let type_def = TypeDef::from(entity_def);
        state.type_store.create_type(&type_def).await?;
        if let TypeDef::Entity(def) = type_def {
            created.entity_defs.push(def);
        }
    }

    // Create classification types
    for class_def in req.classification_defs {
        let type_def = TypeDef::from(class_def);
        state.type_store.create_type(&type_def).await?;
        if let TypeDef::Classification(def) = type_def {
            created.classification_defs.push(def);
        }
    }

    // Create struct types
    for struct_def in req.struct_defs {
        let type_def = TypeDef::from(struct_def);
        state.type_store.create_type(&type_def).await?;
        if let TypeDef::Struct(def) = type_def {
            created.struct_defs.push(def);
        }
    }

    // Create enum types
    for enum_def in req.enum_defs {
        let type_def = TypeDef::from(enum_def);
        state.type_store.create_type(&type_def).await?;
        if let TypeDef::Enum(def) = type_def {
            created.enum_defs.push(def);
        }
    }

    // Create relationship types
    for rel_def in req.relationship_defs {
        let type_def = TypeDef::from(rel_def);
        state.type_store.create_type(&type_def).await?;
        if let TypeDef::Relationship(def) = type_def {
            created.relationship_defs.push(def);
        }
    }

    Ok((StatusCode::CREATED, Json(created)))
}

/// Update types
pub async fn update_types(
    State(state): State<AppState>,
    Json(req): Json<TypesDef>,
) -> Result<Json<TypesDef>> {
    let mut updated = TypesDef::new();

    // Update entity types
    for entity_def in req.entity_defs {
        let type_def = TypeDef::from(entity_def);
        state.type_store.update_type(&type_def).await?;
        if let TypeDef::Entity(def) = type_def {
            updated.entity_defs.push(def);
        }
    }

    // Update classification types
    for class_def in req.classification_defs {
        let type_def = TypeDef::from(class_def);
        state.type_store.update_type(&type_def).await?;
        if let TypeDef::Classification(def) = type_def {
            updated.classification_defs.push(def);
        }
    }

    // Update struct types
    for struct_def in req.struct_defs {
        let type_def = TypeDef::from(struct_def);
        state.type_store.update_type(&type_def).await?;
        if let TypeDef::Struct(def) = type_def {
            updated.struct_defs.push(def);
        }
    }

    // Update enum types
    for enum_def in req.enum_defs {
        let type_def = TypeDef::from(enum_def);
        state.type_store.update_type(&type_def).await?;
        if let TypeDef::Enum(def) = type_def {
            updated.enum_defs.push(def);
        }
    }

    // Update relationship types
    for rel_def in req.relationship_defs {
        let type_def = TypeDef::from(rel_def);
        state.type_store.update_type(&type_def).await?;
        if let TypeDef::Relationship(def) = type_def {
            updated.relationship_defs.push(def);
        }
    }

    Ok(Json(updated))
}

/// Delete types by name
pub async fn delete_types(
    State(state): State<AppState>,
    Json(req): Json<TypesDef>,
) -> Result<StatusCode> {
    // Delete entity types
    for entity_def in &req.entity_defs {
        state.type_store.delete_type(&entity_def.name).await?;
    }

    // Delete classification types
    for class_def in &req.classification_defs {
        state.type_store.delete_type(&class_def.name).await?;
    }

    // Delete struct types
    for struct_def in &req.struct_defs {
        state.type_store.delete_type(&struct_def.name).await?;
    }

    // Delete enum types
    for enum_def in &req.enum_defs {
        state.type_store.delete_type(&enum_def.name).await?;
    }

    // Delete relationship types
    for rel_def in &req.relationship_defs {
        state.type_store.delete_type(&rel_def.name).await?;
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
