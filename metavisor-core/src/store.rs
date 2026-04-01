//! TypeStore, EntityStore, RelationshipStore, and MetavisorStore traits for CRUD operations

use async_trait::async_trait;

use crate::{
    Classification, Entity, EntityHeader, Relationship, RelationshipHeader, Result, TypeDef,
};

// ============================================================================
// TypeStore Trait
// ============================================================================

/// TypeStore trait for managing type definitions
#[async_trait]
pub trait TypeStore: Send + Sync {
    /// Create a new type definition
    async fn create_type(&self, type_def: &TypeDef) -> Result<()>;

    /// Get a type definition by name
    async fn get_type(&self, name: &str) -> Result<TypeDef>;

    /// Get a type definition by GUID (using index lookup)
    async fn get_type_by_guid(&self, guid: &str) -> Result<TypeDef>;

    /// Update a type definition
    async fn update_type(&self, type_def: &TypeDef) -> Result<()>;

    /// Delete a type definition by name
    async fn delete_type(&self, name: &str) -> Result<()>;

    /// Check if a type exists
    async fn type_exists(&self, name: &str) -> Result<bool>;

    /// List all type names
    async fn list_types(&self) -> Result<Vec<String>>;

    /// List all type definitions (more efficient than list_types + get_type for each)
    async fn list_type_defs(&self) -> Result<Vec<TypeDef>>;

    /// List types by category
    async fn list_types_by_category(&self, category: crate::TypeCategory) -> Result<Vec<String>>;

    /// Create multiple type definitions atomically.
    /// Implementations must ensure all-or-nothing semantics.
    async fn batch_create_types(&self, type_defs: &[TypeDef]) -> Result<()>;

    /// Update multiple type definitions atomically.
    /// Implementations must ensure all-or-nothing semantics.
    async fn batch_update_types(&self, type_defs: &[TypeDef]) -> Result<()>;

    /// Delete multiple type definitions atomically.
    /// Implementations must ensure all-or-nothing semantics.
    async fn batch_delete_types(&self, names: &[String]) -> Result<()>;
}

// ============================================================================
// EntityStore Trait
// ============================================================================

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

    /// Create multiple entities atomically.
    /// Implementations must ensure all-or-nothing semantics.
    async fn batch_create_entities(&self, entities: &[Entity]) -> Result<Vec<String>>;
}

// ============================================================================
// RelationshipStore Trait
// ============================================================================

/// RelationshipStore trait for managing relationship instances
#[async_trait]
pub trait RelationshipStore: Send + Sync {
    /// Create a new relationship
    async fn create_relationship(&self, relationship: &Relationship) -> Result<String>;

    /// Get a relationship by GUID
    async fn get_relationship(&self, guid: &str) -> Result<Relationship>;

    /// Update a relationship
    async fn update_relationship(&self, relationship: &Relationship) -> Result<()>;

    /// Delete a relationship by GUID
    async fn delete_relationship(&self, guid: &str) -> Result<()>;

    /// Check if a relationship exists
    async fn relationship_exists(&self, guid: &str) -> Result<bool>;

    /// List relationships where the given entity GUID is an endpoint
    async fn list_relationships_by_entity(
        &self,
        entity_guid: &str,
    ) -> Result<Vec<RelationshipHeader>>;

    /// List relationships by relationship type name
    async fn list_relationships_by_type(&self, type_name: &str) -> Result<Vec<RelationshipHeader>>;

    /// List all relationship headers
    async fn list_relationships(&self) -> Result<Vec<RelationshipHeader>>;
}

// ============================================================================
// Storage Key Helpers
// ============================================================================

/// Type prefix for KV storage
pub const TYPE_PREFIX: &[u8] = b"type:";

/// Entity prefix for KV storage
const ENTITY_PREFIX: &[u8] = b"entity:";

/// Entity type index prefix
const ENTITY_TYPE_INDEX_PREFIX: &[u8] = b"entity_type:";

/// Relationship prefix for KV storage
const RELATIONSHIP_PREFIX: &[u8] = b"relationship:";

/// Relationship endpoint index prefix (for looking up relationships by entity GUID)
const RELATIONSHIP_ENDPOINT_INDEX_PREFIX: &[u8] = b"rel_endpoint:";

/// Relationship type index prefix
const RELATIONSHIP_TYPE_INDEX_PREFIX: &[u8] = b"rel_type:";

/// Entity unique attribute index prefix
/// Format: entity_unique\0{type_name}\0{attr_name}\0{attr_value} -> entity_guid
const ENTITY_UNIQUE_INDEX_PREFIX: &[u8] = b"entity_unique:";

/// Type GUID index prefix (for looking up type by GUID)
/// Format: type_guid:{guid} -> type_name
const TYPE_GUID_INDEX_PREFIX: &[u8] = b"type_guid:";

/// Build the key for storing a type definition
pub fn type_key(name: &str) -> Vec<u8> {
    let cap = TYPE_PREFIX.len() + name.len();
    let mut key = Vec::with_capacity(cap);
    key.extend_from_slice(TYPE_PREFIX);
    key.extend_from_slice(name.as_bytes());
    key
}

/// Build the key for type GUID index (for looking up type by GUID)
pub fn type_guid_index_key(guid: &str) -> Vec<u8> {
    let cap = TYPE_GUID_INDEX_PREFIX.len() + guid.len();
    let mut key = Vec::with_capacity(cap);
    key.extend_from_slice(TYPE_GUID_INDEX_PREFIX);
    key.extend_from_slice(guid.as_bytes());
    key
}

/// Build the key for storing an entity by GUID
pub fn entity_key(guid: &str) -> Vec<u8> {
    let cap = ENTITY_PREFIX.len() + guid.len();
    let mut key = Vec::with_capacity(cap);
    key.extend_from_slice(ENTITY_PREFIX);
    key.extend_from_slice(guid.as_bytes());
    key
}

/// Build the key for entity type index
pub fn entity_type_index_key(type_name: &str, guid: &str) -> Vec<u8> {
    let cap = ENTITY_TYPE_INDEX_PREFIX.len() + type_name.len() + 1 + guid.len();
    let mut key = Vec::with_capacity(cap);
    key.extend_from_slice(ENTITY_TYPE_INDEX_PREFIX);
    key.extend_from_slice(type_name.as_bytes());
    key.push(b':');
    key.extend_from_slice(guid.as_bytes());
    key
}

/// Build the key for storing a relationship by GUID
pub fn relationship_key(guid: &str) -> Vec<u8> {
    let cap = RELATIONSHIP_PREFIX.len() + guid.len();
    let mut key = Vec::with_capacity(cap);
    key.extend_from_slice(RELATIONSHIP_PREFIX);
    key.extend_from_slice(guid.as_bytes());
    key
}

/// Build the key for relationship endpoint index
/// Format: rel_endpoint:{entity_guid}:{relationship_guid}
pub fn relationship_endpoint_index_key(entity_guid: &str, relationship_guid: &str) -> Vec<u8> {
    let cap =
        RELATIONSHIP_ENDPOINT_INDEX_PREFIX.len() + entity_guid.len() + 1 + relationship_guid.len();
    let mut key = Vec::with_capacity(cap);
    key.extend_from_slice(RELATIONSHIP_ENDPOINT_INDEX_PREFIX);
    key.extend_from_slice(entity_guid.as_bytes());
    key.push(b':');
    key.extend_from_slice(relationship_guid.as_bytes());
    key
}

/// Build the key for relationship type index
/// Format: rel_type:{type_name}:{guid}
pub fn relationship_type_index_key(type_name: &str, guid: &str) -> Vec<u8> {
    let cap = RELATIONSHIP_TYPE_INDEX_PREFIX.len() + type_name.len() + 1 + guid.len();
    let mut key = Vec::with_capacity(cap);
    key.extend_from_slice(RELATIONSHIP_TYPE_INDEX_PREFIX);
    key.extend_from_slice(type_name.as_bytes());
    key.push(b':');
    key.extend_from_slice(guid.as_bytes());
    key
}

/// Build the key for entity unique attribute index
/// Format: entity_unique\0{type_name}\0{attr_name}\0{attr_value}
/// Uses null byte (\0) as separator to avoid ambiguity with values containing ':'
pub fn entity_unique_index_key(type_name: &str, attr_name: &str, attr_value: &str) -> Vec<u8> {
    let cap = ENTITY_UNIQUE_INDEX_PREFIX.len()
        + 1
        + type_name.len()
        + 1
        + attr_name.len()
        + 1
        + attr_value.len();
    let mut key = Vec::with_capacity(cap);
    key.extend_from_slice(ENTITY_UNIQUE_INDEX_PREFIX);
    key.push(b'\0');
    key.extend_from_slice(type_name.as_bytes());
    key.push(b'\0');
    key.extend_from_slice(attr_name.as_bytes());
    key.push(b'\0');
    key.extend_from_slice(attr_value.as_bytes());
    key
}

// ============================================================================
// MetavisorStore Trait - Unified Abstraction Layer
// ============================================================================

/// MetavisorStore - Unified abstraction layer for all metadata operations
///
/// This trait provides a unified interface that coordinates:
/// - KV storage (entities, relationships, types)
/// - Graph storage (lineage, classification propagation)
///
/// All operations are transactional, ensuring consistency between KV and Graph.
#[async_trait]
pub trait MetavisorStore: Send + Sync {
    // ========================================================================
    // Type Operations
    // ========================================================================

    /// Create a new type definition
    async fn create_type(&self, type_def: &TypeDef) -> Result<()>;

    /// Get a type definition by name
    async fn get_type(&self, name: &str) -> Result<TypeDef>;

    /// Get a type definition by GUID (using index lookup)
    async fn get_type_by_guid(&self, guid: &str) -> Result<TypeDef>;

    /// Update a type definition
    async fn update_type(&self, type_def: &TypeDef) -> Result<()>;

    /// Delete a type definition by name
    async fn delete_type(&self, name: &str) -> Result<()>;

    /// Check if a type exists
    async fn type_exists(&self, name: &str) -> Result<bool>;

    /// List all type names
    async fn list_types(&self) -> Result<Vec<String>>;

    /// List all type definitions (more efficient than list_types + get_type for each)
    async fn list_type_defs(&self) -> Result<Vec<TypeDef>>;

    /// Create multiple type definitions atomically
    async fn batch_create_types(&self, type_defs: &[TypeDef]) -> Result<()>;

    /// Update multiple type definitions atomically
    async fn batch_update_types(&self, type_defs: &[TypeDef]) -> Result<()>;

    /// Delete multiple type definitions atomically
    async fn batch_delete_types(&self, names: &[String]) -> Result<()>;

    // ========================================================================
    // Entity Operations (with Graph sync)
    // ========================================================================

    /// Create a new entity (syncs to graph)
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

    /// Delete an entity by GUID (syncs to graph)
    async fn delete_entity(&self, guid: &str) -> Result<()>;

    /// Check if an entity exists
    async fn entity_exists(&self, guid: &str) -> Result<bool>;

    /// List entity headers by type name
    async fn list_entities_by_type(&self, type_name: &str) -> Result<Vec<EntityHeader>>;

    /// List all entity headers
    async fn list_entities(&self) -> Result<Vec<EntityHeader>>;

    /// Create multiple entities atomically (with Graph sync)
    async fn batch_create_entities(&self, entities: &[Entity]) -> Result<Vec<String>>;

    // ========================================================================
    // Relationship Operations (with Graph sync)
    // ========================================================================

    /// Create a new relationship (syncs to graph)
    async fn create_relationship(&self, relationship: &Relationship) -> Result<String>;

    /// Get a relationship by GUID
    async fn get_relationship(&self, guid: &str) -> Result<Relationship>;

    /// Update a relationship
    async fn update_relationship(&self, relationship: &Relationship) -> Result<()>;

    /// Delete a relationship by GUID (syncs to graph)
    async fn delete_relationship(&self, guid: &str) -> Result<()>;

    /// Check if a relationship exists
    async fn relationship_exists(&self, guid: &str) -> Result<bool>;

    /// List relationships where the given entity GUID is an endpoint
    async fn list_relationships_by_entity(
        &self,
        entity_guid: &str,
    ) -> Result<Vec<RelationshipHeader>>;

    /// List relationships by relationship type name
    async fn list_relationships_by_type(&self, type_name: &str) -> Result<Vec<RelationshipHeader>>;

    /// List all relationship headers
    async fn list_relationships(&self) -> Result<Vec<RelationshipHeader>>;

    // ========================================================================
    // Classification Operations
    // ========================================================================

    /// Add classifications to an entity
    async fn add_classifications(
        &self,
        entity_guid: &str,
        classifications: &[Classification],
    ) -> Result<()>;

    /// Get classifications for an entity (direct only, not propagated)
    async fn get_classifications(&self, entity_guid: &str) -> Result<Vec<Classification>>;

    /// Update classifications for an entity (replaces all)
    async fn update_classifications(
        &self,
        entity_guid: &str,
        classifications: &[Classification],
    ) -> Result<()>;

    /// Remove a specific classification from an entity by name
    async fn remove_classification(
        &self,
        entity_guid: &str,
        classification_name: &str,
    ) -> Result<()>;

    /// Get graph statistics
    fn graph_stats(&self) -> GraphStats;
}

/// Graph statistics
#[derive(Debug, Clone, Default)]
pub struct GraphStats {
    pub node_count: usize,
    pub edge_count: usize,
}
