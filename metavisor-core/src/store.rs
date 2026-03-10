//! TypeStore and EntityStore traits for CRUD operations

use async_trait::async_trait;

use crate::{Entity, EntityHeader, Result, TypeDef};

/// TypeStore trait for managing type definitions
#[async_trait]
pub trait TypeStore: Send + Sync {
    /// Create a new type definition
    async fn create_type(&self, type_def: &TypeDef) -> Result<()>;

    /// Get a type definition by name
    async fn get_type(&self, name: &str) -> Result<TypeDef>;

    /// Update a type definition
    async fn update_type(&self, type_def: &TypeDef) -> Result<()>;

    /// Delete a type definition by name
    async fn delete_type(&self, name: &str) -> Result<()>;

    /// Check if a type exists
    async fn type_exists(&self, name: &str) -> Result<bool>;

    /// List all type names
    async fn list_types(&self) -> Result<Vec<String>>;

    /// List types by category
    async fn list_types_by_category(&self, category: crate::TypeCategory) -> Result<Vec<String>>;
}

/// EntityStore trait for managing entity instances
#[async_trait]
pub trait EntityStore: Send + Sync {
    /// Create a new entity
    async fn create_entity(&self, entity: &Entity) -> Result<String>;

    /// Get an entity by GUID
    async fn get_entity(&self, guid: &str) -> Result<Entity>;

    /// Get an entity by type name and unique attributes
    async fn get_entity_by_unique_attrs(
        &self,
        type_name: &str,
        unique_attrs: &std::collections::HashMap<String, serde_json::Value>,
    ) -> Result<Entity>;

    /// Update an entity
    async fn update_entity(&self, entity: &Entity) -> Result<()>;

    /// Delete an entity by GUID
    async fn delete_entity(&self, guid: &str) -> Result<()>;

    /// Check if an entity exists
    async fn entity_exists(&self, guid: &str) -> Result<bool>;

    /// List entity headers by type name
    async fn list_entities_by_type(&self, type_name: &str) -> Result<Vec<EntityHeader>>;

    /// List all entity headers
    async fn list_entities(&self) -> Result<Vec<EntityHeader>>;
}

/// Type prefix for KV storage
const TYPE_PREFIX: &[u8] = b"type:";

/// Entity prefix for KV storage
const ENTITY_PREFIX: &[u8] = b"entity:";

/// Entity type index prefix
const ENTITY_TYPE_INDEX_PREFIX: &[u8] = b"entity_type:";

/// Build the key for storing a type definition
pub fn type_key(name: &str) -> Vec<u8> {
    let mut key = TYPE_PREFIX.to_vec();
    key.extend_from_slice(name.as_bytes());
    key
}

/// Build the key for storing an entity by GUID
pub fn entity_key(guid: &str) -> Vec<u8> {
    let mut key = ENTITY_PREFIX.to_vec();
    key.extend_from_slice(guid.as_bytes());
    key
}

/// Build the key for entity type index
pub fn entity_type_index_key(type_name: &str, guid: &str) -> Vec<u8> {
    let mut key = ENTITY_TYPE_INDEX_PREFIX.to_vec();
    key.extend_from_slice(type_name.as_bytes());
    key.push(b':');
    key.extend_from_slice(guid.as_bytes());
    key
}
