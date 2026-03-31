//! EntityStore implementation using KV store

use async_trait::async_trait;
use metavisor_core::{
    entity_key, entity_type_index_key, entity_unique_index_key, CoreError, Entity, EntityHeader,
    EntityStore, Result, TypeDef, TypeStore,
};
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

use crate::kv::{KvStore, WriteOp};

/// KvEntityStore - EntityStore implementation using surrealkv
pub struct KvEntityStore {
    kv: KvStore,
    type_store: Arc<dyn TypeStore>,
}

impl KvEntityStore {
    /// Create a new KvEntityStore with type validation
    pub fn new(kv: KvStore, type_store: Arc<dyn TypeStore>) -> Self {
        Self { kv, type_store }
    }

    /// Generate a new GUID for an entity
    fn generate_guid() -> String {
        Uuid::new_v4().to_string()
    }

    /// Convert a JSON value to a string suitable for use as an index key component.
    /// Returns None for non-primitive types.
    fn attr_to_index_value(value: &serde_json::Value) -> Option<String> {
        match value {
            serde_json::Value::String(s) => Some(s.clone()),
            serde_json::Value::Number(n) => Some(n.to_string()),
            serde_json::Value::Bool(b) => Some(b.to_string()),
            serde_json::Value::Null => None,
            _ => None, // Array, Object — serialization ambiguous
        }
    }

    /// Parse a unique index key to extract attribute details for error messages.
    /// Key format: `entity_unique:{type_name}:{attr_name}:{attr_value}`
    fn parse_unique_violation(key_desc: &str) -> String {
        let parts: Vec<&str> = key_desc.splitn(4, ':').collect();
        if parts.len() == 4 {
            format!(
                "attribute '{}' with value '{}' already exists",
                parts[2], parts[3]
            )
        } else {
            key_desc.to_string()
        }
    }

    /// Validate entity against its type definition
    async fn validate_entity(&self, entity: &Entity) -> Result<()> {
        // 1. Get type definition
        let type_def = self
            .type_store
            .get_type(&entity.type_name)
            .await
            .map_err(|_| CoreError::TypeNotFound(entity.type_name.clone()))?;

        // 2. Get attribute definitions (only for Entity types)
        let attr_defs = match type_def {
            TypeDef::Entity(ref def) => &def.attribute_defs,
            _ => return Ok(()), // Skip validation for non-entity types
        };

        // 3. Validate required attributes
        for attr_def in attr_defs {
            if !attr_def.is_optional && !entity.attributes.contains_key(&attr_def.name) {
                return Err(CoreError::Validation(format!(
                    "Required attribute '{}' is missing for type '{}'",
                    attr_def.name, entity.type_name
                )));
            }
        }

        // 4. Validate attribute types
        for (name, value) in &entity.attributes {
            if let Some(attr_def) = attr_defs.iter().find(|a| &a.name == name) {
                self.validate_attribute_type(&attr_def.type_name, value, name)?;
            }
        }

        Ok(())
    }

    /// Get unique attribute names for an entity type
    /// Returns empty vec if type not found (for graceful handling)
    async fn get_unique_attributes(&self, type_name: &str) -> Result<Vec<String>> {
        let type_def = match self.type_store.get_type(type_name).await {
            Ok(td) => td,
            Err(CoreError::TypeNotFound(_)) => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };

        let attr_defs = match type_def {
            TypeDef::Entity(def) => def.attribute_defs,
            _ => return Ok(Vec::new()),
        };

        let unique_attrs: Vec<String> = attr_defs
            .into_iter()
            .filter(|attr| attr.is_unique == Some(true))
            .map(|attr| attr.name)
            .collect();

        Ok(unique_attrs)
    }

    /// Build unique constraint checks and index write ops in a single pass.
    /// Returns `(checks, ops)` for the `conditional_batch_write` — no I/O, no TOCTOU.
    /// `exclude_guid`: when updating, skip the index key that currently belongs to this entity.
    fn build_unique_ops(
        entity: &Entity,
        is_delete: bool,
        unique_attrs: &[String],
        is_update: bool,
        existing_index_keys: &[(String, String)],
    ) -> (Vec<crate::kv::CheckOp>, Vec<WriteOp>) {
        use crate::kv::{CheckOp, WriteOp};

        let mut checks = Vec::new();
        let mut ops = Vec::new();

        for attr_name in unique_attrs {
            if let Some(value) = entity.attributes.get(attr_name) {
                let Some(attr_value) = Self::attr_to_index_value(value) else {
                    continue;
                };
                if attr_value.is_empty() {
                    continue;
                }

                let index_key = entity_unique_index_key(&entity.type_name, attr_name, &attr_value);

                // When updating: if this unique attr value is the same as the existing entity's,
                // the index key already exists and belongs to us — skip the check.
                let is_own = is_update
                    && existing_index_keys
                        .iter()
                        .any(|(name, val)| *name == *attr_name && *val == attr_value);

                if !is_own {
                    checks.push(CheckOp::Absent {
                        key: index_key.clone(),
                    });
                }

                // Value changed on update: delete old index entry
                if is_update && !is_own {
                    if let Some((_, old_val)) = existing_index_keys
                        .iter()
                        .find(|(name, _)| name == attr_name)
                    {
                        ops.push(WriteOp::Delete {
                            key: entity_unique_index_key(&entity.type_name, attr_name, old_val),
                        });
                    }
                }

                if is_delete {
                    ops.push(WriteOp::Delete { key: index_key });
                } else if let Some(ref guid) = entity.guid {
                    if let Ok(bytes) = serde_json::to_vec(guid) {
                        ops.push(WriteOp::Set {
                            key: index_key,
                            value: bytes,
                        });
                    }
                }
            } else if is_update {
                // Attribute was removed — delete its old index entry
                if let Some((_, old_val)) = existing_index_keys
                    .iter()
                    .find(|(name, _)| name == attr_name)
                {
                    ops.push(WriteOp::Delete {
                        key: entity_unique_index_key(&entity.type_name, attr_name, old_val),
                    });
                }
            }
        }

        (checks, ops)
    }

    /// Validate attribute value against its declared type
    fn validate_attribute_type(
        &self,
        type_name: &str,
        value: &serde_json::Value,
        attr_name: &str,
    ) -> Result<()> {
        // Handle primitive types
        let is_valid = match type_name {
            "string" => value.is_string(),
            "int" | "long" | "integer" => value.is_i64() || value.is_u64(),
            "short" => value.is_i64() || value.is_u64(),
            "byte" => value.is_i64() || value.is_u64(),
            "float" | "double" => value.is_number(),
            "boolean" => value.is_boolean(),
            "date" | "datetime" => value.is_string(), // String representation
            "array<string>" => value.is_array(),
            "array<int>" | "array<long>" => value.is_array(),
            "map<string,string>" | "map<string,object>" => value.is_object(),
            // For other types (references, custom types), skip validation
            _ => true,
        };

        if !is_valid {
            return Err(CoreError::Validation(format!(
                "Attribute '{}' has invalid type. Expected '{}', got value: {}",
                attr_name,
                type_name,
                if value.is_string() {
                    format!("\"{}\"", value.as_str().unwrap_or(""))
                } else {
                    value.to_string()
                }
            )));
        }

        Ok(())
    }
}

#[async_trait]
impl EntityStore for KvEntityStore {
    async fn create_entity(&self, entity: &Entity) -> Result<String> {
        // Validate entity against type definition
        self.validate_entity(entity).await?;

        // Generate GUID if not provided
        let guid = entity.guid.clone().unwrap_or_else(Self::generate_guid);

        let key = entity_key(&guid);

        // Prepare entity with GUID
        let mut entity_with_guid = entity.clone();
        entity_with_guid.guid = Some(guid.clone());

        // Fetch unique attribute names once, shared by check and index build
        let unique_attrs = self.get_unique_attributes(&entity.type_name).await?;

        // Build precondition checks: entity key must not exist + unique attrs must not exist
        let (unique_checks, unique_ops) =
            Self::build_unique_ops(&entity_with_guid, false, &unique_attrs, false, &[]);
        let mut checks = Vec::with_capacity(1 + unique_checks.len());
        checks.push(crate::kv::CheckOp::Absent { key: key.clone() });
        checks.extend(unique_checks);

        let header = entity_with_guid.to_header();

        // Build atomic batch: entity data + type index + unique indices
        let entity_bytes =
            serde_json::to_vec(&entity_with_guid).map_err(|e| CoreError::Storage(e.to_string()))?;
        let header_bytes =
            serde_json::to_vec(&header).map_err(|e| CoreError::Storage(e.to_string()))?;

        let mut ops = Vec::with_capacity(2 + unique_ops.len());
        ops.push(WriteOp::Set {
            key,
            value: entity_bytes,
        });
        ops.push(WriteOp::Set {
            key: entity_type_index_key(&entity.type_name, &guid),
            value: header_bytes,
        });
        ops.extend(unique_ops);

        self.kv
            .conditional_batch_write(checks, ops)
            .await
            .map_err(|e| {
                if let crate::error::StorageError::AlreadyExists(ref key_desc) = e {
                    if key_desc.starts_with("entity_unique:") {
                        let detail = Self::parse_unique_violation(key_desc);
                        CoreError::Validation(format!(
                            "Unique constraint violation on type '{}': {}",
                            entity.type_name, detail
                        ))
                    } else {
                        CoreError::EntityAlreadyExists(guid.clone())
                    }
                } else {
                    CoreError::Storage(e.to_string())
                }
            })?;

        Ok(guid)
    }

    async fn get_entity(&self, guid: &str) -> Result<Entity> {
        let key = entity_key(guid);

        self.kv
            .get(&key)
            .await
            .map_err(|e| CoreError::Storage(e.to_string()))?
            .ok_or_else(|| CoreError::EntityNotFound(guid.to_string()))
    }

    async fn get_entity_by_unique_attrs(
        &self,
        type_name: &str,
        unique_attrs: &HashMap<String, serde_json::Value>,
    ) -> Result<Entity> {
        if unique_attrs.is_empty() {
            return Err(CoreError::Validation(format!(
                "At least one unique attribute is required for type '{}'",
                type_name
            )));
        }

        // Try to find entity using unique attribute index
        // Use the first unique attribute to look up in the index
        let unique_attr_names = self.get_unique_attributes(type_name).await?;

        for (attr_name, attr_value) in unique_attrs {
            // Only use index if this attribute is marked as unique in the type definition
            if !unique_attr_names.contains(attr_name) {
                continue;
            }

            // Convert attribute value to string for index lookup
            let attr_value_str = match attr_value {
                serde_json::Value::String(s) => s.clone(),
                serde_json::Value::Number(n) => n.to_string(),
                serde_json::Value::Bool(b) => b.to_string(),
                _ => continue,
            };

            // Lookup in unique index
            let index_key = entity_unique_index_key(type_name, attr_name, &attr_value_str);
            if let Some(guid) = self
                .kv
                .get::<String>(&index_key)
                .await
                .map_err(|e| CoreError::Storage(e.to_string()))?
            {
                return self.get_entity(&guid).await;
            }
        }

        // Fallback: if no unique index found or no indexed attributes provided, scan all entities
        // This maintains backward compatibility for non-unique lookups
        let entities = self.list_entities_by_type(type_name).await?;
        for header in entities {
            let Some(guid) = header.guid else {
                continue;
            };

            let entity = self.get_entity(&guid).await?;
            let matches = unique_attrs
                .iter()
                .all(|(key, expected)| entity.attributes.get(key) == Some(expected));

            if matches {
                return Ok(entity);
            }
        }

        Err(CoreError::EntityNotFound(format!(
            "{} with unique attributes {:?}",
            type_name, unique_attrs
        )))
    }

    async fn update_entity(&self, entity: &Entity) -> Result<()> {
        // Validate entity against type definition
        self.validate_entity(entity).await?;

        let guid = entity.guid.as_ref().ok_or_else(|| {
            CoreError::Validation("Entity GUID is required for update".to_string())
        })?;

        let key = entity_key(guid);

        // Get the existing entity
        let existing = self
            .kv
            .get::<Entity>(&key)
            .await
            .map_err(|e| CoreError::Storage(e.to_string()))?
            .ok_or_else(|| CoreError::EntityNotFound(guid.clone()))?;

        // typeName is immutable
        if existing.type_name != entity.type_name {
            return Err(CoreError::Validation(
                "Cannot change entity typeName. Delete and recreate the entity instead."
                    .to_string(),
            ));
        }

        // Fetch unique attribute names once, shared by check and index build
        let unique_attrs = self.get_unique_attributes(&entity.type_name).await?;

        // Collect existing entity's unique index key values (to exclude self from checks)
        let existing_index_keys: Vec<(String, String)> = unique_attrs
            .iter()
            .filter_map(|name| {
                existing
                    .attributes
                    .get(name)
                    .and_then(Self::attr_to_index_value)
                    .map(|v| (name.clone(), v))
            })
            .collect();

        // Build precondition checks + index ops for unique constraints (atomic with writes)
        let (unique_checks, unique_ops) =
            Self::build_unique_ops(entity, false, &unique_attrs, true, &existing_index_keys);
        let checks = unique_checks;

        // Build atomic batch: entity data + type index + unique indices update
        let entity_bytes =
            serde_json::to_vec(entity).map_err(|e| CoreError::Storage(e.to_string()))?;
        let header = entity.to_header();
        let header_bytes =
            serde_json::to_vec(&header).map_err(|e| CoreError::Storage(e.to_string()))?;

        let mut ops = vec![
            WriteOp::Set {
                key,
                value: entity_bytes,
            },
            WriteOp::Set {
                key: entity_type_index_key(&entity.type_name, guid),
                value: header_bytes,
            },
        ];

        // Add unique index ops (only changed values due to is_own exclusion)
        ops.extend(unique_ops);

        self.kv
            .conditional_batch_write(checks, ops)
            .await
            .map_err(|e| {
                if let crate::error::StorageError::AlreadyExists(ref key_desc) = e {
                    let detail = Self::parse_unique_violation(key_desc);
                    CoreError::Validation(format!(
                        "Unique constraint violation on type '{}': {}",
                        entity.type_name, detail
                    ))
                } else {
                    CoreError::Storage(e.to_string())
                }
            })?;

        Ok(())
    }

    async fn delete_entity(&self, guid: &str) -> Result<()> {
        let key = entity_key(guid);

        // Read raw bytes for optimistic concurrency check and deserialize for index ops.
        // Using raw bytes avoids serde round-trip mismatch (key order, float repr, etc.)
        let entity_bytes = self
            .kv
            .get_raw(&key)
            .await
            .map_err(|e| CoreError::Storage(e.to_string()))?
            .ok_or_else(|| CoreError::EntityNotFound(guid.to_string()))?;

        let entity: Entity =
            serde_json::from_slice(&entity_bytes).map_err(|e| CoreError::Storage(e.to_string()))?;

        let checks = vec![crate::kv::CheckOp::ValueEquals {
            key: key.clone(),
            expected: entity_bytes,
        }];

        // Fetch unique attribute names once
        let unique_attrs = self.get_unique_attributes(&entity.type_name).await?;

        // Build atomic batch: delete entity data + type index + unique indices
        let mut ops = vec![
            WriteOp::Delete { key },
            WriteOp::Delete {
                key: entity_type_index_key(&entity.type_name, guid),
            },
        ];

        // Remove unique indices
        let (_, delete_ops) = Self::build_unique_ops(&entity, true, &unique_attrs, false, &[]);
        ops.extend(delete_ops);

        self.kv
            .conditional_batch_write(checks, ops)
            .await
            .map_err(|e| {
                if matches!(e, crate::error::StorageError::Conflict(_)) {
                    CoreError::EntityNotFound(guid.to_string())
                } else {
                    CoreError::Storage(e.to_string())
                }
            })?;

        Ok(())
    }

    async fn entity_exists(&self, guid: &str) -> Result<bool> {
        let key = entity_key(guid);
        self.kv
            .exists(&key)
            .await
            .map_err(|e| CoreError::Storage(e.to_string()))
    }

    async fn list_entities_by_type(&self, type_name: &str) -> Result<Vec<EntityHeader>> {
        let prefix = format!("entity_type:{type_name}:");
        let mut entries: Vec<(Vec<u8>, EntityHeader)> = self
            .kv
            .scan_prefix(prefix.as_bytes())
            .map_err(|e| CoreError::Storage(e.to_string()))?;

        let mut headers: Vec<EntityHeader> = entries.drain(..).map(|(_, header)| header).collect();
        headers.sort_by(|a, b| a.guid.cmp(&b.guid));
        Ok(headers)
    }

    async fn list_entities(&self) -> Result<Vec<EntityHeader>> {
        const ENTITY_PREFIX: &[u8] = b"entity:";

        let mut entries: Vec<(Vec<u8>, Entity)> = self
            .kv
            .scan_prefix(ENTITY_PREFIX)
            .map_err(|e| CoreError::Storage(e.to_string()))?;

        let mut headers: Vec<EntityHeader> = entries
            .drain(..)
            .map(|(_, entity)| entity.to_header())
            .collect();
        headers.sort_by(|a, b| a.guid.cmp(&b.guid));
        Ok(headers)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metavisor_core::{AttributeDef, Classification, EntityDef};
    use serde_json::json;
    use tempfile::TempDir;

    use crate::type_store::KvTypeStore;

    async fn create_test_stores() -> (KvEntityStore, Arc<KvTypeStore>) {
        let tempdir = TempDir::new().unwrap();
        let kv = KvStore::open(tempdir.path()).unwrap();
        let type_store = Arc::new(KvTypeStore::new(kv.clone()));
        let entity_store = KvEntityStore::new(kv, type_store.clone());
        (entity_store, type_store)
    }

    async fn create_test_type(type_store: &KvTypeStore) {
        let entity_def = EntityDef::new("TestTable")
            .attribute(AttributeDef::new("name", "string").required())
            .attribute(AttributeDef::new("rowCount", "int"));
        let type_def = TypeDef::from(entity_def);
        type_store.create_type(&type_def).await.unwrap();
    }

    #[tokio::test]
    async fn test_create_entity_with_valid_type() {
        let (store, type_store) = create_test_stores().await;
        create_test_type(&type_store).await;

        let entity = Entity::new("TestTable")
            .with_attribute("name", json!("users"))
            .with_attribute("rowCount", json!(1000));

        let guid = store.create_entity(&entity).await.unwrap();
        assert!(!guid.is_empty());

        let retrieved = store.get_entity(&guid).await.unwrap();
        assert_eq!(retrieved.type_name, "TestTable");
        assert_eq!(retrieved.attributes.get("name"), Some(&json!("users")));
    }

    #[tokio::test]
    async fn test_create_entity_missing_required_attribute() {
        let (store, type_store) = create_test_stores().await;
        create_test_type(&type_store).await;

        // Missing required "name" attribute
        let entity = Entity::new("TestTable").with_attribute("rowCount", json!(1000));

        let result = store.create_entity(&entity).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, CoreError::Validation(_)));
        assert!(err
            .to_string()
            .contains("Required attribute 'name' is missing"));
    }

    #[tokio::test]
    async fn test_create_entity_invalid_type_name() {
        let (store, _) = create_test_stores().await;

        // Type doesn't exist
        let entity = Entity::new("NonExistentType").with_attribute("name", json!("test"));

        let result = store.create_entity(&entity).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), CoreError::TypeNotFound(_)));
    }

    #[tokio::test]
    async fn test_create_entity_invalid_attribute_type() {
        let (store, type_store) = create_test_stores().await;
        create_test_type(&type_store).await;

        // rowCount should be int, but we pass a string
        let entity = Entity::new("TestTable")
            .with_attribute("name", json!("users"))
            .with_attribute("rowCount", json!("not a number"));

        let result = store.create_entity(&entity).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, CoreError::Validation(_)));
        assert!(err.to_string().contains("invalid type"));
    }

    #[tokio::test]
    async fn test_update_entity_with_validation() {
        let (store, type_store) = create_test_stores().await;
        create_test_type(&type_store).await;

        let entity = Entity::new("TestTable")
            .with_attribute("name", json!("original"))
            .with_attribute("rowCount", json!(100));

        let guid = store.create_entity(&entity).await.unwrap();

        // Valid update
        let mut updated = store.get_entity(&guid).await.unwrap();
        updated
            .attributes
            .insert("rowCount".to_string(), json!(200));

        store.update_entity(&updated).await.unwrap();

        // Invalid update - missing required attribute
        updated.attributes.remove("name");
        let result = store.update_entity(&updated).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_create_entity_with_guid() {
        let (store, type_store) = create_test_stores().await;
        create_test_type(&type_store).await;

        let entity = Entity::new("TestTable")
            .with_guid("custom-guid-123")
            .with_attribute("name", json!("test"));

        let guid = store.create_entity(&entity).await.unwrap();
        assert_eq!(guid, "custom-guid-123");

        let retrieved = store.get_entity(&guid).await.unwrap();
        assert_eq!(retrieved.guid, Some("custom-guid-123".to_string()));
    }

    #[tokio::test]
    async fn test_create_duplicate_entity() {
        let (store, type_store) = create_test_stores().await;
        create_test_type(&type_store).await;

        let entity = Entity::new("TestTable")
            .with_guid("dup-guid")
            .with_attribute("name", json!("test"));

        store.create_entity(&entity).await.unwrap();

        // Try to create again with same GUID
        let result = store.create_entity(&entity).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            CoreError::EntityAlreadyExists(_)
        ));
    }

    #[tokio::test]
    async fn test_delete_entity() {
        let (store, type_store) = create_test_stores().await;
        create_test_type(&type_store).await;

        let entity = Entity::new("TestTable").with_attribute("name", json!("temp"));

        let guid = store.create_entity(&entity).await.unwrap();
        assert!(store.entity_exists(&guid).await.unwrap());

        store.delete_entity(&guid).await.unwrap();
        assert!(!store.entity_exists(&guid).await.unwrap());

        // Try to get deleted entity
        let result = store.get_entity(&guid).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), CoreError::EntityNotFound(_)));
    }

    #[tokio::test]
    async fn test_entity_with_classification() {
        let (store, type_store) = create_test_stores().await;
        create_test_type(&type_store).await;

        let entity = Entity::new("TestTable")
            .with_attribute("name", json!("sensitive_data"))
            .with_classification(Classification::new("PII"))
            .with_label("confidential");

        let guid = store.create_entity(&entity).await.unwrap();

        let retrieved = store.get_entity(&guid).await.unwrap();
        assert_eq!(retrieved.classifications.len(), 1);
        assert_eq!(retrieved.classifications[0].type_name, "PII");
        assert_eq!(retrieved.labels, vec!["confidential"]);
    }

    #[tokio::test]
    async fn test_entity_not_found() {
        let (store, _) = create_test_stores().await;

        let result = store.get_entity("nonexistent-guid").await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), CoreError::EntityNotFound(_)));
    }

    // ============================================================================
    // Unique Attribute Index Tests
    // ============================================================================

    async fn create_test_type_with_unique_attr(type_store: &KvTypeStore) {
        let entity_def = EntityDef::new("UniqueTestTable")
            .attribute(AttributeDef::new("name", "string").required().unique())
            .attribute(AttributeDef::new("code", "string").unique())
            .attribute(AttributeDef::new("rowCount", "int"));
        let type_def = TypeDef::from(entity_def);
        type_store.create_type(&type_def).await.unwrap();
    }

    #[tokio::test]
    async fn test_unique_index_lookup() {
        let (store, type_store) = create_test_stores().await;
        create_test_type_with_unique_attr(&type_store).await;

        // Create an entity with unique attribute
        let entity = Entity::new("UniqueTestTable")
            .with_attribute("name", json!("test_table"))
            .with_attribute("code", json!("TT001"))
            .with_attribute("rowCount", json!(100));

        let guid = store.create_entity(&entity).await.unwrap();

        // Lookup by unique attribute using index
        let mut unique_attrs = HashMap::new();
        unique_attrs.insert("name".to_string(), json!("test_table"));

        let found = store
            .get_entity_by_unique_attrs("UniqueTestTable", &unique_attrs)
            .await
            .unwrap();

        assert_eq!(found.guid, Some(guid));
        assert_eq!(found.attributes.get("name"), Some(&json!("test_table")));
    }

    #[tokio::test]
    async fn test_unique_index_update() {
        let (store, type_store) = create_test_stores().await;
        create_test_type_with_unique_attr(&type_store).await;

        // Create entity
        let entity = Entity::new("UniqueTestTable")
            .with_attribute("name", json!("old_name"))
            .with_attribute("code", json!("TT001"));

        let guid = store.create_entity(&entity).await.unwrap();

        // Update unique attribute
        let mut updated = store.get_entity(&guid).await.unwrap();
        updated
            .attributes
            .insert("name".to_string(), json!("new_name"));

        store.update_entity(&updated).await.unwrap();

        // Old unique value should not find the entity
        let mut old_attrs = HashMap::new();
        old_attrs.insert("name".to_string(), json!("old_name"));
        let result = store
            .get_entity_by_unique_attrs("UniqueTestTable", &old_attrs)
            .await;
        assert!(result.is_err()); // Should not find

        // New unique value should find the entity
        let mut new_attrs = HashMap::new();
        new_attrs.insert("name".to_string(), json!("new_name"));
        let found = store
            .get_entity_by_unique_attrs("UniqueTestTable", &new_attrs)
            .await
            .unwrap();
        assert_eq!(found.guid, Some(guid));
    }

    #[tokio::test]
    async fn test_unique_index_delete() {
        let (store, type_store) = create_test_stores().await;
        create_test_type_with_unique_attr(&type_store).await;

        // Create entity
        let entity = Entity::new("UniqueTestTable")
            .with_attribute("name", json!("to_delete"))
            .with_attribute("code", json!("TT001"));

        let guid = store.create_entity(&entity).await.unwrap();

        // Verify entity can be found by unique attribute
        let mut unique_attrs = HashMap::new();
        unique_attrs.insert("name".to_string(), json!("to_delete"));
        let found = store
            .get_entity_by_unique_attrs("UniqueTestTable", &unique_attrs)
            .await
            .unwrap();
        assert_eq!(found.guid, Some(guid.clone()));

        // Delete entity
        store.delete_entity(&guid).await.unwrap();

        // Should not find deleted entity by unique attribute
        let result = store
            .get_entity_by_unique_attrs("UniqueTestTable", &unique_attrs)
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_unique_index_multiple_attributes() {
        let (store, type_store) = create_test_stores().await;
        create_test_type_with_unique_attr(&type_store).await;

        // Create entity with multiple unique attributes
        let entity = Entity::new("UniqueTestTable")
            .with_attribute("name", json!("my_table"))
            .with_attribute("code", json!("CODE123"));

        let guid = store.create_entity(&entity).await.unwrap();

        // Lookup by first unique attribute
        let mut attrs1 = HashMap::new();
        attrs1.insert("name".to_string(), json!("my_table"));
        let found1 = store
            .get_entity_by_unique_attrs("UniqueTestTable", &attrs1)
            .await
            .unwrap();
        assert_eq!(found1.guid, Some(guid.clone()));

        // Lookup by second unique attribute
        let mut attrs2 = HashMap::new();
        attrs2.insert("code".to_string(), json!("CODE123"));
        let found2 = store
            .get_entity_by_unique_attrs("UniqueTestTable", &attrs2)
            .await
            .unwrap();
        assert_eq!(found2.guid, Some(guid.clone()));

        // Lookup by both unique attributes (composite lookup)
        let mut attrs3 = HashMap::new();
        attrs3.insert("name".to_string(), json!("my_table"));
        attrs3.insert("code".to_string(), json!("CODE123"));
        let found3 = store
            .get_entity_by_unique_attrs("UniqueTestTable", &attrs3)
            .await
            .unwrap();
        assert_eq!(found3.guid, Some(guid));
    }

    #[tokio::test]
    async fn test_unique_index_performance() {
        let (store, type_store) = create_test_stores().await;
        create_test_type_with_unique_attr(&type_store).await;

        // Create 100 entities
        for i in 0..100 {
            let entity = Entity::new("UniqueTestTable")
                .with_attribute("name", json!(format!("table_{}", i)))
                .with_attribute("code", json!(format!("CODE{}", i)));
            store.create_entity(&entity).await.unwrap();
        }

        // Lookup by unique attribute should be O(1) regardless of entity count
        let mut unique_attrs = HashMap::new();
        unique_attrs.insert("name".to_string(), json!("table_50"));

        let found = store
            .get_entity_by_unique_attrs("UniqueTestTable", &unique_attrs)
            .await
            .unwrap();

        assert_eq!(found.attributes.get("name"), Some(&json!("table_50")));
    }

    #[tokio::test]
    async fn test_unique_constraint_violation_on_create() {
        let (store, type_store) = create_test_stores().await;
        create_test_type_with_unique_attr(&type_store).await;

        // Create first entity with unique attribute
        let entity1 = Entity::new("UniqueTestTable").with_attribute("name", json!("unique_name"));
        store.create_entity(&entity1).await.unwrap();

        // Attempt to create second entity with same unique attribute value
        let entity2 = Entity::new("UniqueTestTable").with_attribute("name", json!("unique_name"));
        let result = store.create_entity(&entity2).await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Unique constraint violation"),
            "Expected unique constraint error, got: {}",
            err_msg
        );
    }

    #[tokio::test]
    async fn test_unique_constraint_violation_on_update() {
        let (store, type_store) = create_test_stores().await;
        create_test_type_with_unique_attr(&type_store).await;

        // Create two entities with different unique values
        let entity1 = Entity::new("UniqueTestTable").with_attribute("name", json!("name_a"));
        let entity2 = Entity::new("UniqueTestTable").with_attribute("name", json!("name_b"));
        store.create_entity(&entity1).await.unwrap();
        let guid2 = store.create_entity(&entity2).await.unwrap();

        // Try to update entity2 to have the same unique value as entity1
        let mut updated = store.get_entity(&guid2).await.unwrap();
        updated
            .attributes
            .insert("name".to_string(), json!("name_a"));
        let result = store.update_entity(&updated).await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Unique constraint violation"),
            "Expected unique constraint error, got: {}",
            err_msg
        );
    }

    #[tokio::test]
    async fn test_update_entity_keeps_own_unique_value() {
        let (store, type_store) = create_test_stores().await;
        create_test_type_with_unique_attr(&type_store).await;

        // Create entity with unique attribute
        let entity = Entity::new("UniqueTestTable")
            .with_attribute("name", json!("my_name"))
            .with_attribute("code", json!("C001"));
        let guid = store.create_entity(&entity).await.unwrap();

        // Update non-unique attribute — should succeed
        let mut updated = store.get_entity(&guid).await.unwrap();
        updated.attributes.insert("rowCount".to_string(), json!(42));
        store.update_entity(&updated).await.unwrap();

        // Update unique attribute to a new value — should succeed
        let mut updated2 = store.get_entity(&guid).await.unwrap();
        updated2
            .attributes
            .insert("name".to_string(), json!("new_name"));
        store.update_entity(&updated2).await.unwrap();

        // Verify the update took effect
        let found = store.get_entity(&guid).await.unwrap();
        assert_eq!(found.attributes.get("name"), Some(&json!("new_name")));
    }

    #[tokio::test]
    async fn test_concurrent_create_unique_constraint() {
        let (store, type_store) = create_test_stores().await;
        create_test_type_with_unique_attr(&type_store).await;
        let store = Arc::new(store);

        // Spawn N concurrent creates with the same unique attribute value
        let mut handles = Vec::new();
        for _ in 0..10 {
            let store = store.clone();
            handles.push(tokio::spawn(async move {
                let entity =
                    Entity::new("UniqueTestTable").with_attribute("name", json!("race_value"));
                store.create_entity(&entity).await
            }));
        }

        let mut successes = 0usize;
        let mut violations = 0usize;
        for handle in handles {
            match handle.await.unwrap() {
                Ok(_) => successes += 1,
                Err(e) if e.to_string().contains("Unique constraint") => violations += 1,
                Err(_) => {}
            }
        }

        assert_eq!(
            successes, 1,
            "Expected exactly 1 successful create, got {}",
            successes
        );
        assert_eq!(
            successes + violations,
            10,
            "Expected remaining 9 to be constraint violations"
        );
    }
}
