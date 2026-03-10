//! EntityStore implementation using KV store

use async_trait::async_trait;
use metavisor_core::{
    entity_key, entity_type_index_key, CoreError, Entity, EntityHeader, EntityStore, Result,
    TypeDef, TypeStore,
};
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

use crate::kv::KvStore;

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

        // Check if entity already exists
        if self
            .kv
            .exists(&key)
            .await
            .map_err(|e| CoreError::Storage(e.to_string()))?
        {
            return Err(CoreError::EntityAlreadyExists(guid));
        }

        // Store the entity with the GUID
        let mut entity_with_guid = entity.clone();
        entity_with_guid.guid = Some(guid.clone());

        self.kv
            .put(&key, &entity_with_guid)
            .await
            .map_err(|e| CoreError::Storage(e.to_string()))?;

        // Create type index entry
        let type_index_key = entity_type_index_key(&entity.type_name, &guid);
        let header = entity_with_guid.to_header();
        self.kv
            .put(&type_index_key, &header)
            .await
            .map_err(|e| CoreError::Storage(e.to_string()))?;

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
        _unique_attrs: &HashMap<String, serde_json::Value>,
    ) -> Result<Entity> {
        // TODO: Implement unique attribute lookup with secondary index
        // For now, return an error indicating this is not yet supported
        Err(CoreError::Validation(format!(
            "Lookup by unique attributes is not yet supported for type '{}'",
            type_name
        )))
    }

    async fn update_entity(&self, entity: &Entity) -> Result<()> {
        // Validate entity against type definition
        self.validate_entity(entity).await?;

        let guid = entity.guid.as_ref().ok_or_else(|| {
            CoreError::Validation("Entity GUID is required for update".to_string())
        })?;

        let key = entity_key(guid);

        // Check if entity exists
        if !self
            .kv
            .exists(&key)
            .await
            .map_err(|e| CoreError::Storage(e.to_string()))?
        {
            return Err(CoreError::EntityNotFound(guid.clone()));
        }

        // Update the entity
        self.kv
            .put(&key, entity)
            .await
            .map_err(|e| CoreError::Storage(e.to_string()))?;

        // Update type index entry
        let type_index_key = entity_type_index_key(&entity.type_name, guid);
        let header = entity.to_header();
        self.kv
            .put(&type_index_key, &header)
            .await
            .map_err(|e| CoreError::Storage(e.to_string()))?;

        Ok(())
    }

    async fn delete_entity(&self, guid: &str) -> Result<()> {
        // First get the entity to find its type name
        let entity = self.get_entity(guid).await?;
        let key = entity_key(guid);

        // Delete the entity
        self.kv
            .delete(&key)
            .await
            .map_err(|e| CoreError::Storage(e.to_string()))?;

        // Delete type index entry
        let type_index_key = entity_type_index_key(&entity.type_name, guid);
        self.kv
            .delete(&type_index_key)
            .await
            .map_err(|e| CoreError::Storage(e.to_string()))?;

        Ok(())
    }

    async fn entity_exists(&self, guid: &str) -> Result<bool> {
        let key = entity_key(guid);
        self.kv
            .exists(&key)
            .await
            .map_err(|e| CoreError::Storage(e.to_string()))
    }

    async fn list_entities_by_type(&self, _type_name: &str) -> Result<Vec<EntityHeader>> {
        // TODO: Implement prefix scan for type index
        // For now, return empty list
        Ok(Vec::new())
    }

    async fn list_entities(&self) -> Result<Vec<EntityHeader>> {
        // TODO: Implement prefix scan for all entities
        // For now, return empty list
        Ok(Vec::new())
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
        let _ = type_store.create_type(&type_def).await;
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
}
