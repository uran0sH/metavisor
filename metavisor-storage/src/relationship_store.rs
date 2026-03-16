//! RelationshipStore implementation using KV store

use async_trait::async_trait;
use metavisor_core::{
    relationship_endpoint_index_key, relationship_key, relationship_type_index_key, CoreError,
    Relationship, RelationshipHeader, RelationshipStore, Result, TypeDef, TypeStore,
};
use std::sync::Arc;
use uuid::Uuid;

use crate::kv::{KvStore, WriteOp};

/// KvRelationshipStore - RelationshipStore implementation using surrealkv
pub struct KvRelationshipStore {
    kv: KvStore,
    type_store: Arc<dyn TypeStore>,
}

impl KvRelationshipStore {
    /// Create a new KvRelationshipStore with type validation
    pub fn new(kv: KvStore, type_store: Arc<dyn TypeStore>) -> Self {
        Self { kv, type_store }
    }

    /// Generate a new GUID for a relationship
    fn generate_guid() -> String {
        Uuid::new_v4().to_string()
    }

    /// Validate relationship against its type definition
    async fn validate_relationship(&self, relationship: &Relationship) -> Result<()> {
        // 1. Get relationship type definition
        let type_def = self
            .type_store
            .get_type(&relationship.type_name)
            .await
            .map_err(|_| CoreError::TypeNotFound(relationship.type_name.clone()))?;

        // 2. Verify it's a relationship type
        let rel_def = match type_def {
            TypeDef::Relationship(ref def) => def,
            _ => {
                return Err(CoreError::Validation(format!(
                    "Type '{}' is not a relationship type",
                    relationship.type_name
                )))
            }
        };

        // 3. Validate end1 entity type matches endDef1
        if let Some(ref end1) = relationship.end1 {
            if let Some(ref end_def1) = rel_def.end_def1 {
                if let Some(ref expected_type) = end_def1.type_name {
                    // Check if the entity type matches or is a subtype
                    // For now, we do a simple string comparison
                    // TODO: Support type inheritance validation
                    if !Self::type_matches_or_subtype(&end1.type_name, expected_type) {
                        return Err(CoreError::Validation(format!(
                            "end1 entity type '{}' does not match expected type '{}'",
                            end1.type_name, expected_type
                        )));
                    }
                }
            }
        } else {
            return Err(CoreError::Validation(
                "end1 is required for relationship".to_string(),
            ));
        }

        // 4. Validate end2 entity type matches endDef2
        if let Some(ref end2) = relationship.end2 {
            if let Some(ref end_def2) = rel_def.end_def2 {
                if let Some(ref expected_type) = end_def2.type_name {
                    if !Self::type_matches_or_subtype(&end2.type_name, expected_type) {
                        return Err(CoreError::Validation(format!(
                            "end2 entity type '{}' does not match expected type '{}'",
                            end2.type_name, expected_type
                        )));
                    }
                }
            }
        } else {
            return Err(CoreError::Validation(
                "end2 is required for relationship".to_string(),
            ));
        }

        Ok(())
    }

    /// Check if actual_type matches expected_type or is a subtype
    /// For now, does simple string comparison. TODO: implement proper inheritance check
    fn type_matches_or_subtype(actual_type: &str, expected_type: &str) -> bool {
        actual_type == expected_type
    }
}

#[async_trait]
impl RelationshipStore for KvRelationshipStore {
    async fn create_relationship(&self, relationship: &Relationship) -> Result<String> {
        // Validate relationship against type definition
        self.validate_relationship(relationship).await?;

        // Generate GUID if not provided
        let guid = relationship
            .guid
            .clone()
            .unwrap_or_else(Self::generate_guid);

        let key = relationship_key(&guid);

        // Check if relationship already exists
        if self
            .kv
            .exists(&key)
            .await
            .map_err(|e| CoreError::Storage(e.to_string()))?
        {
            return Err(CoreError::RelationshipAlreadyExists(guid));
        }

        // Prepare relationship with GUID
        let mut relationship_with_guid = relationship.clone();
        relationship_with_guid.guid = Some(guid.clone());
        let header = relationship_with_guid.to_header();

        // Build all write operations for atomic transaction
        let mut ops: Vec<WriteOp> = Vec::new();

        // 1. Store the main relationship data
        let rel_bytes = serde_json::to_vec(&relationship_with_guid)
            .map_err(|e| CoreError::Storage(e.to_string()))?;
        ops.push(WriteOp::Set {
            key,
            value: rel_bytes,
        });

        // 2. Create endpoint index for end1
        if let Some(ref end1) = relationship_with_guid.end1 {
            if let Some(ref end1_guid) = end1.guid {
                let endpoint_key = relationship_endpoint_index_key(end1_guid, &guid);
                let header_bytes =
                    serde_json::to_vec(&header).map_err(|e| CoreError::Storage(e.to_string()))?;
                ops.push(WriteOp::Set {
                    key: endpoint_key,
                    value: header_bytes,
                });
            }
        }

        // 3. Create endpoint index for end2
        if let Some(ref end2) = relationship_with_guid.end2 {
            if let Some(ref end2_guid) = end2.guid {
                let endpoint_key = relationship_endpoint_index_key(end2_guid, &guid);
                let header_bytes =
                    serde_json::to_vec(&header).map_err(|e| CoreError::Storage(e.to_string()))?;
                ops.push(WriteOp::Set {
                    key: endpoint_key,
                    value: header_bytes,
                });
            }
        }

        // 4. Create type index entry
        let type_index_key = relationship_type_index_key(&relationship_with_guid.type_name, &guid);
        let header_bytes =
            serde_json::to_vec(&header).map_err(|e| CoreError::Storage(e.to_string()))?;
        ops.push(WriteOp::Set {
            key: type_index_key,
            value: header_bytes,
        });

        // Execute all writes atomically
        self.kv
            .batch_write(ops)
            .await
            .map_err(|e| CoreError::Storage(e.to_string()))?;

        Ok(guid)
    }

    async fn get_relationship(&self, guid: &str) -> Result<Relationship> {
        let key = relationship_key(guid);

        self.kv
            .get(&key)
            .await
            .map_err(|e| CoreError::Storage(e.to_string()))?
            .ok_or_else(|| CoreError::RelationshipNotFound(guid.to_string()))
    }

    async fn update_relationship(&self, relationship: &Relationship) -> Result<()> {
        // Validate relationship against type definition
        self.validate_relationship(relationship).await?;

        let guid = relationship.guid.as_ref().ok_or_else(|| {
            CoreError::Validation("Relationship GUID is required for update".to_string())
        })?;

        let key = relationship_key(guid);

        // Check if relationship exists
        if !self
            .kv
            .exists(&key)
            .await
            .map_err(|e| CoreError::Storage(e.to_string()))?
        {
            return Err(CoreError::RelationshipNotFound(guid.clone()));
        }

        // Build batch operations
        let mut ops: Vec<WriteOp> = Vec::new();

        // Update the relationship
        let rel_bytes =
            serde_json::to_vec(&relationship).map_err(|e| CoreError::Storage(e.to_string()))?;
        ops.push(WriteOp::Set {
            key,
            value: rel_bytes,
        });

        // Update type index entry
        let type_index_key = relationship_type_index_key(&relationship.type_name, guid);
        let header = relationship.to_header();
        let header_bytes =
            serde_json::to_vec(&header).map_err(|e| CoreError::Storage(e.to_string()))?;
        ops.push(WriteOp::Set {
            key: type_index_key,
            value: header_bytes,
        });

        // Execute atomically
        self.kv
            .batch_write(ops)
            .await
            .map_err(|e| CoreError::Storage(e.to_string()))?;

        Ok(())
    }

    async fn delete_relationship(&self, guid: &str) -> Result<()> {
        // First get the relationship to find its endpoints
        let relationship = self.get_relationship(guid).await?;

        // Build batch delete operations
        let mut ops: Vec<WriteOp> = Vec::new();

        // Delete the main relationship data
        let key = relationship_key(guid);
        ops.push(WriteOp::Delete { key });

        // Delete endpoint indices
        if let Some(ref end1) = relationship.end1 {
            if let Some(ref end1_guid) = end1.guid {
                let endpoint_key = relationship_endpoint_index_key(end1_guid, guid);
                ops.push(WriteOp::Delete { key: endpoint_key });
            }
        }

        if let Some(ref end2) = relationship.end2 {
            if let Some(ref end2_guid) = end2.guid {
                let endpoint_key = relationship_endpoint_index_key(end2_guid, guid);
                ops.push(WriteOp::Delete { key: endpoint_key });
            }
        }

        // Delete type index entry
        let type_index_key = relationship_type_index_key(&relationship.type_name, guid);
        ops.push(WriteOp::Delete {
            key: type_index_key,
        });

        // Execute all deletes atomically
        self.kv
            .batch_write(ops)
            .await
            .map_err(|e| CoreError::Storage(e.to_string()))?;

        Ok(())
    }

    async fn relationship_exists(&self, guid: &str) -> Result<bool> {
        let key = relationship_key(guid);
        self.kv
            .exists(&key)
            .await
            .map_err(|e| CoreError::Storage(e.to_string()))
    }

    async fn list_relationships_by_entity(
        &self,
        entity_guid: &str,
    ) -> Result<Vec<RelationshipHeader>> {
        // Build prefix for endpoint index: rel_endpoint:{entity_guid}:
        let prefix = format!("rel_endpoint:{}:", entity_guid);
        let results: Vec<(Vec<u8>, RelationshipHeader)> = self
            .kv
            .scan_prefix(prefix.as_bytes())
            .map_err(|e| CoreError::Storage(e.to_string()))?;

        Ok(results.into_iter().map(|(_, header)| header).collect())
    }

    async fn list_relationships_by_type(&self, type_name: &str) -> Result<Vec<RelationshipHeader>> {
        // Build prefix for type index: rel_type:{type_name}:
        let prefix = format!("rel_type:{}:", type_name);
        let results: Vec<(Vec<u8>, RelationshipHeader)> = self
            .kv
            .scan_prefix(prefix.as_bytes())
            .map_err(|e| CoreError::Storage(e.to_string()))?;

        Ok(results.into_iter().map(|(_, header)| header).collect())
    }

    async fn list_relationships(&self) -> Result<Vec<RelationshipHeader>> {
        // Scan all relationships using the relationship: prefix
        let results: Vec<(Vec<u8>, Relationship)> = self
            .kv
            .scan_prefix(b"relationship:")
            .map_err(|e| CoreError::Storage(e.to_string()))?;

        Ok(results
            .into_iter()
            .map(|(_, rel)| rel.to_header())
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metavisor_core::{
        AttributeDef, EntityDef, ObjectId, PropagateTags, RelationshipDef, RelationshipEndDef,
        TypeDef,
    };
    use tempfile::TempDir;

    use crate::type_store::KvTypeStore;

    async fn create_test_stores() -> (KvRelationshipStore, std::sync::Arc<KvTypeStore>) {
        let tempdir = TempDir::new().unwrap();
        let kv = KvStore::open(tempdir.path()).unwrap();
        let type_store = std::sync::Arc::new(KvTypeStore::new(kv.clone()));
        let relationship_store = KvRelationshipStore::new(kv, type_store.clone());
        (relationship_store, type_store)
    }

    async fn create_test_relationship_type(type_store: &KvTypeStore) {
        // Create entity types first
        let table_def =
            EntityDef::new("Table").attribute(AttributeDef::new("name", "string").required());
        let column_def =
            EntityDef::new("Column").attribute(AttributeDef::new("name", "string").required());

        type_store
            .create_type(&TypeDef::from(table_def))
            .await
            .unwrap();
        type_store
            .create_type(&TypeDef::from(column_def))
            .await
            .unwrap();

        // Create relationship type
        let rel_def = RelationshipDef::new("table_columns")
            .category(metavisor_core::RelationshipCategory::Composition)
            .propagate_tags(PropagateTags::OneToTwo)
            .end1(RelationshipEndDef::new("Table", "columns"))
            .end2(RelationshipEndDef::new("Column", "table"));

        type_store
            .create_type(&TypeDef::from(rel_def))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_create_relationship_with_valid_type() {
        let (store, type_store) = create_test_stores().await;
        create_test_relationship_type(&type_store).await;

        let end1 = ObjectId::by_guid("Table", "table-guid-1");
        let end2 = ObjectId::by_guid("Column", "column-guid-1");

        let rel = Relationship::between("table_columns", end1, end2).with_label("contains");

        let guid = store.create_relationship(&rel).await.unwrap();
        assert!(!guid.is_empty());

        let retrieved = store.get_relationship(&guid).await.unwrap();
        assert_eq!(retrieved.type_name, "table_columns");
        assert_eq!(retrieved.label, Some("contains".to_string()));
    }

    #[tokio::test]
    async fn test_create_relationship_invalid_type() {
        let (store, _) = create_test_stores().await;

        let end1 = ObjectId::by_guid("Table", "table-guid-1");
        let end2 = ObjectId::by_guid("Column", "column-guid-1");

        // Relationship type doesn't exist
        let rel = Relationship::between("nonexistent_type", end1, end2);

        let result = store.create_relationship(&rel).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), CoreError::TypeNotFound(_)));
    }

    #[tokio::test]
    async fn test_create_relationship_type_mismatch() {
        let (store, type_store) = create_test_stores().await;
        create_test_relationship_type(&type_store).await;

        // end1 type should be "Table" but we use "Column"
        let end1 = ObjectId::by_guid("Column", "column-guid-1");
        let end2 = ObjectId::by_guid("Column", "column-guid-2");

        let rel = Relationship::between("table_columns", end1, end2);

        let result = store.create_relationship(&rel).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, CoreError::Validation(_)));
        assert!(err.to_string().contains("does not match expected type"));
    }

    #[tokio::test]
    async fn test_create_relationship_missing_endpoints() {
        let (store, type_store) = create_test_stores().await;
        create_test_relationship_type(&type_store).await;

        // Missing end2
        let rel = Relationship::new("table_columns")
            .with_end1(ObjectId::by_guid("Table", "table-guid-1"));

        let result = store.create_relationship(&rel).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, CoreError::Validation(_)));
        assert!(err.to_string().contains("end2 is required"));
    }

    #[tokio::test]
    async fn test_get_relationship_not_found() {
        let (store, _) = create_test_stores().await;

        let result = store.get_relationship("nonexistent-guid").await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            CoreError::RelationshipNotFound(_)
        ));
    }

    #[tokio::test]
    async fn test_delete_relationship() {
        let (store, type_store) = create_test_stores().await;
        create_test_relationship_type(&type_store).await;

        let end1 = ObjectId::by_guid("Table", "table-guid-1");
        let end2 = ObjectId::by_guid("Column", "column-guid-1");

        let rel = Relationship::between("table_columns", end1, end2);
        let guid = store.create_relationship(&rel).await.unwrap();

        assert!(store.relationship_exists(&guid).await.unwrap());

        store.delete_relationship(&guid).await.unwrap();
        assert!(!store.relationship_exists(&guid).await.unwrap());

        let result = store.get_relationship(&guid).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            CoreError::RelationshipNotFound(_)
        ));
    }

    #[tokio::test]
    async fn test_update_relationship() {
        let (store, type_store) = create_test_stores().await;
        create_test_relationship_type(&type_store).await;

        let end1 = ObjectId::by_guid("Table", "table-guid-1");
        let end2 = ObjectId::by_guid("Column", "column-guid-1");

        let rel = Relationship::between("table_columns", end1, end2).with_label("original");

        let guid = store.create_relationship(&rel).await.unwrap();

        // Update the relationship
        let mut updated = store.get_relationship(&guid).await.unwrap();
        updated.label = Some("updated".to_string());

        store.update_relationship(&updated).await.unwrap();

        let retrieved = store.get_relationship(&guid).await.unwrap();
        assert_eq!(retrieved.label, Some("updated".to_string()));
    }

    #[tokio::test]
    async fn test_create_duplicate_relationship() {
        let (store, type_store) = create_test_stores().await;
        create_test_relationship_type(&type_store).await;

        let end1 = ObjectId::by_guid("Table", "table-guid-1");
        let end2 = ObjectId::by_guid("Column", "column-guid-1");

        let rel = Relationship::between("table_columns", end1.clone(), end2.clone())
            .with_guid("dup-guid");

        store.create_relationship(&rel).await.unwrap();

        // Try to create again with same GUID
        let result = store.create_relationship(&rel).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            CoreError::RelationshipAlreadyExists(_)
        ));
    }

    #[tokio::test]
    async fn test_list_relationships_by_entity() {
        let (store, type_store) = create_test_stores().await;
        create_test_relationship_type(&type_store).await;

        let table1 = ObjectId::by_guid("Table", "table-1");
        let table2 = ObjectId::by_guid("Table", "table-2");
        let col1 = ObjectId::by_guid("Column", "column-1");
        let col2 = ObjectId::by_guid("Column", "column-2");
        let col3 = ObjectId::by_guid("Column", "column-3");

        // Create relationships: table1 -> col1, table1 -> col2, table2 -> col3
        let rel1 = Relationship::between("table_columns", table1.clone(), col1.clone());
        let rel2 = Relationship::between("table_columns", table1.clone(), col2.clone());
        let rel3 = Relationship::between("table_columns", table2.clone(), col3.clone());

        store.create_relationship(&rel1).await.unwrap();
        store.create_relationship(&rel2).await.unwrap();
        store.create_relationship(&rel3).await.unwrap();

        // List relationships for table1 - should have 2
        let rels_for_table1 = store.list_relationships_by_entity("table-1").await.unwrap();
        assert_eq!(rels_for_table1.len(), 2);

        // List relationships for col1 - should have 1
        let rels_for_col1 = store
            .list_relationships_by_entity("column-1")
            .await
            .unwrap();
        assert_eq!(rels_for_col1.len(), 1);

        // List relationships for table2 - should have 1
        let rels_for_table2 = store.list_relationships_by_entity("table-2").await.unwrap();
        assert_eq!(rels_for_table2.len(), 1);

        // List relationships for unknown entity - should have 0
        let rels_unknown = store.list_relationships_by_entity("unknown").await.unwrap();
        assert!(rels_unknown.is_empty());
    }

    #[tokio::test]
    async fn test_list_relationships_by_type() {
        let (store, type_store) = create_test_stores().await;
        create_test_relationship_type(&type_store).await;

        let table1 = ObjectId::by_guid("Table", "table-1");
        let col1 = ObjectId::by_guid("Column", "column-1");
        let col2 = ObjectId::by_guid("Column", "column-2");

        // Create 2 relationships of the same type
        let rel1 = Relationship::between("table_columns", table1.clone(), col1.clone());
        let rel2 = Relationship::between("table_columns", table1.clone(), col2.clone());

        store.create_relationship(&rel1).await.unwrap();
        store.create_relationship(&rel2).await.unwrap();

        // List relationships by type
        let rels = store
            .list_relationships_by_type("table_columns")
            .await
            .unwrap();
        assert_eq!(rels.len(), 2);

        // List relationships for non-existent type
        let no_rels = store
            .list_relationships_by_type("nonexistent")
            .await
            .unwrap();
        assert!(no_rels.is_empty());
    }

    #[tokio::test]
    async fn test_list_all_relationships() {
        let (store, type_store) = create_test_stores().await;
        create_test_relationship_type(&type_store).await;

        let table1 = ObjectId::by_guid("Table", "table-1");
        let col1 = ObjectId::by_guid("Column", "column-1");
        let col2 = ObjectId::by_guid("Column", "column-2");

        // Create 2 relationships
        let rel1 = Relationship::between("table_columns", table1.clone(), col1.clone());
        let rel2 = Relationship::between("table_columns", table1.clone(), col2.clone());

        store.create_relationship(&rel1).await.unwrap();
        store.create_relationship(&rel2).await.unwrap();

        // List all relationships
        let all_rels = store.list_relationships().await.unwrap();
        assert_eq!(all_rels.len(), 2);
    }
}
