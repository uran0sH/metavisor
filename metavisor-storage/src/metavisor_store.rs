//! MetavisorStore implementation - Unified abstraction layer
//!
//! Coordinates KV storage (entities, relationships, types) and Graph storage
//! (lineage, classification propagation) with transactional guarantees.

use async_trait::async_trait;
use std::sync::Arc;

use metavisor_core::{
    Classification, Entity, EntityDef, EntityHeader, EntityStore, GraphStats, GraphStore,
    LineageNode, LineageQueryOptions, LineageResult, MetavisorStore, ObjectId, PropagateTags,
    Relationship, RelationshipDef, RelationshipEndDef, RelationshipHeader, RelationshipStore,
    Result, TraversalDirection, TypeDef, TypeStore,
};

/// DefaultMetavisorStore - Default implementation of MetavisorStore
///
/// This implementation provides:
/// - Transactional operations across KV and Graph storage
/// - Automatic graph synchronization on entity/relationship changes
/// - Unified interface for all metadata operations
pub struct DefaultMetavisorStore {
    type_store: Arc<dyn TypeStore>,
    entity_store: Arc<dyn EntityStore>,
    relationship_store: Arc<dyn RelationshipStore>,
    graph_store: Arc<dyn GraphStore>,
}

impl DefaultMetavisorStore {
    /// Create a new DefaultMetavisorStore
    pub fn new(
        type_store: Arc<dyn TypeStore>,
        entity_store: Arc<dyn EntityStore>,
        relationship_store: Arc<dyn RelationshipStore>,
        graph_store: Arc<dyn GraphStore>,
    ) -> Self {
        Self {
            type_store,
            entity_store,
            relationship_store,
            graph_store,
        }
    }

    /// Initialize the graph from persisted data
    pub async fn initialize(&self) -> Result<()> {
        self.graph_store.rebuild_graph().await
    }

    /// Helper method to update entity and sync classifications to graph
    async fn update_entity_classifications(&self, entity: &Entity) -> Result<()> {
        // Update the entity in KV store
        self.entity_store.update_entity(entity).await?;

        // Extract classification names for graph update
        let classification_names: Vec<String> = entity
            .classifications
            .iter()
            .map(|c| c.type_name.clone())
            .collect();

        // Update the graph node to trigger re-propagation
        self.graph_store
            .update_entity_node(
                entity.guid.as_deref().unwrap_or(""),
                &entity.type_name,
                None,
                classification_names,
            )
            .await?;

        Ok(())
    }

    async fn ensure_lineage_relationship_types(&self, process_type: &str) -> Result<()> {
        let process_entity_type = match self.type_store.get_type(process_type).await? {
            TypeDef::Entity(def) => def,
            _ => {
                return Err(metavisor_core::CoreError::Validation(format!(
                    "Type '{}' is not an entity type",
                    process_type
                )))
            }
        };

        let input_type = self
            .extract_lineage_endpoint_type(&process_entity_type, "inputs")
            .unwrap_or_else(|| "column_meta".to_string());
        let output_type = self
            .extract_lineage_endpoint_type(&process_entity_type, "outputs")
            .unwrap_or_else(|| "column_meta".to_string());

        self.ensure_relationship_type_exists(
            RelationshipDef::new("process_inputs")
                .end1(
                    RelationshipEndDef::new(&input_type, "input")
                        .cardinality(metavisor_core::Cardinality::Set),
                )
                .end2(
                    RelationshipEndDef::new(process_type, "process")
                        .cardinality(metavisor_core::Cardinality::Single),
                ),
        )
        .await?;

        self.ensure_relationship_type_exists(
            RelationshipDef::new("process_outputs")
                .end1(
                    RelationshipEndDef::new(process_type, "process")
                        .cardinality(metavisor_core::Cardinality::Single),
                )
                .end2(
                    RelationshipEndDef::new(&output_type, "output")
                        .cardinality(metavisor_core::Cardinality::Set),
                ),
        )
        .await?;

        Ok(())
    }

    async fn ensure_relationship_type_exists(
        &self,
        relationship_def: RelationshipDef,
    ) -> Result<()> {
        let type_name = relationship_def.name.clone();
        if self.type_store.type_exists(&type_name).await? {
            return Ok(());
        }
        self.type_store
            .create_type(&TypeDef::Relationship(relationship_def))
            .await
    }

    fn extract_lineage_endpoint_type(
        &self,
        entity_def: &EntityDef,
        attr_name: &str,
    ) -> Option<String> {
        entity_def
            .attribute_defs
            .iter()
            .find(|attr| attr.name == attr_name)
            .and_then(|attr| Self::parse_array_object_type(&attr.type_name))
    }

    fn parse_array_object_type(type_name: &str) -> Option<String> {
        let inner = type_name
            .strip_prefix("array<")
            .and_then(|s| s.strip_suffix('>'))?;
        inner
            .strip_prefix("objectid<")
            .and_then(|s| s.strip_suffix('>'))
            .map(ToString::to_string)
    }

    fn parse_object_ids(value: Option<&serde_json::Value>) -> Vec<ObjectId> {
        value
            .and_then(|v| v.as_array())
            .into_iter()
            .flatten()
            .filter_map(|item| serde_json::from_value::<ObjectId>(item.clone()).ok())
            .collect()
    }

    async fn create_lineage_relationships_for_entity(
        &self,
        entity: &Entity,
        entity_guid: &str,
    ) -> Result<()> {
        let inputs = Self::parse_object_ids(entity.attributes.get("inputs"));
        let outputs = Self::parse_object_ids(entity.attributes.get("outputs"));

        if inputs.is_empty() && outputs.is_empty() {
            return Ok(());
        }

        self.ensure_lineage_relationship_types(&entity.type_name)
            .await?;

        let mut created_relationship_guids = Vec::new();

        for input in inputs {
            let relationship = Relationship::new("process_inputs")
                .with_end1(self.resolve_object_id(input).await?)
                .with_end2(ObjectId::by_guid(&entity.type_name, entity_guid))
                .with_propagate_tags(PropagateTags::OneToTwo);
            match self.create_relationship(&relationship).await {
                Ok(guid) => created_relationship_guids.push(guid),
                Err(err) => {
                    self.rollback_created_relationships(&created_relationship_guids)
                        .await;
                    return Err(err);
                }
            }
        }

        for output in outputs {
            let relationship = Relationship::new("process_outputs")
                .with_end1(ObjectId::by_guid(&entity.type_name, entity_guid))
                .with_end2(self.resolve_object_id(output).await?)
                .with_propagate_tags(PropagateTags::OneToTwo);
            match self.create_relationship(&relationship).await {
                Ok(guid) => created_relationship_guids.push(guid),
                Err(err) => {
                    self.rollback_created_relationships(&created_relationship_guids)
                        .await;
                    return Err(err);
                }
            }
        }

        Ok(())
    }

    async fn rollback_created_relationships(&self, relationship_guids: &[String]) {
        for guid in relationship_guids.iter().rev() {
            let _ = self.delete_relationship(guid).await;
        }
    }

    async fn resolve_object_id(&self, object_id: ObjectId) -> Result<ObjectId> {
        if object_id.guid.is_some() {
            return Ok(object_id);
        }

        let entity = self
            .entity_store
            .get_entity_by_unique_attrs(&object_id.type_name, &object_id.unique_attributes)
            .await?;

        let guid = entity.guid.ok_or_else(|| {
            metavisor_core::CoreError::Validation(format!(
                "Entity '{}' resolved from unique attributes has no GUID",
                object_id.type_name
            ))
        })?;

        Ok(ObjectId::by_guid(object_id.type_name, guid))
    }
}

#[async_trait]
impl MetavisorStore for DefaultMetavisorStore {
    // ========================================================================
    // Type Operations
    // ========================================================================

    async fn create_type(&self, type_def: &TypeDef) -> Result<()> {
        self.type_store.create_type(type_def).await
    }

    async fn get_type(&self, name: &str) -> Result<TypeDef> {
        self.type_store.get_type(name).await
    }

    async fn update_type(&self, type_def: &TypeDef) -> Result<()> {
        self.type_store.update_type(type_def).await
    }

    async fn delete_type(&self, name: &str) -> Result<()> {
        self.type_store.delete_type(name).await
    }

    async fn type_exists(&self, name: &str) -> Result<bool> {
        self.type_store.type_exists(name).await
    }

    async fn list_types(&self) -> Result<Vec<String>> {
        self.type_store.list_types().await
    }

    // ========================================================================
    // Entity Operations (with Graph sync)
    // ========================================================================

    async fn create_entity(&self, entity: &Entity) -> Result<String> {
        // Step 1: Create entity in KV store
        let guid = self.entity_store.create_entity(entity).await?;

        // Step 2: Add node to graph
        // If this fails, we attempt to rollback the KV write
        if let Err(e) = self
            .graph_store
            .add_entity_node(&guid, &entity.type_name)
            .await
        {
            // Rollback: delete the entity we just created
            let _ = self.entity_store.delete_entity(&guid).await;
            return Err(e);
        }

        let mut created_entity = entity.clone();
        created_entity.guid = Some(guid.clone());

        if let Err(e) = self
            .create_lineage_relationships_for_entity(&created_entity, &guid)
            .await
        {
            let _ = self.graph_store.remove_entity_node(&guid).await;
            let _ = self.entity_store.delete_entity(&guid).await;
            return Err(e);
        }

        Ok(guid)
    }

    async fn get_entity(&self, guid: &str) -> Result<Entity> {
        self.entity_store.get_entity(guid).await
    }

    async fn get_entity_by_unique_attrs(
        &self,
        type_name: &str,
        unique_attrs: &std::collections::HashMap<String, serde_json::Value>,
    ) -> Result<Entity> {
        self.entity_store
            .get_entity_by_unique_attrs(type_name, unique_attrs)
            .await
    }

    async fn update_entity(&self, entity: &Entity) -> Result<()> {
        self.entity_store.update_entity(entity).await?;

        // Update the node in the graph incrementally (much faster than rebuild_graph)
        let display_name = entity.attributes.get("name").and_then(|v| v.as_str());
        let classifications = entity
            .classifications
            .iter()
            .map(|c| c.type_name.clone())
            .collect();

        let updated = self
            .graph_store
            .update_entity_node(
                entity.guid.as_deref().unwrap_or(""),
                &entity.type_name,
                display_name,
                classifications,
            )
            .await?;

        // If the node wasn't found in the graph, add it
        if !updated {
            if let Some(ref guid) = entity.guid {
                self.graph_store
                    .add_entity_node(guid, &entity.type_name)
                    .await?;
            }
        }

        Ok(())
    }

    async fn delete_entity(&self, guid: &str) -> Result<()> {
        // Step 0: Get entity info first (needed for potential rollback)
        let entity = self.entity_store.get_entity(guid).await?;

        // Step 1: Remove from graph first
        self.graph_store.remove_entity_node(guid).await?;

        // Step 2: Delete from KV store
        if let Err(e) = self.entity_store.delete_entity(guid).await {
            // Rollback: re-add node to graph
            if let Err(rollback_err) = self
                .graph_store
                .add_entity_node(guid, &entity.type_name)
                .await
            {
                tracing::error!(
                    "Failed to rollback graph node after KV delete failure: {}",
                    rollback_err
                );
            }
            return Err(e);
        }

        Ok(())
    }

    async fn entity_exists(&self, guid: &str) -> Result<bool> {
        self.entity_store.entity_exists(guid).await
    }

    async fn list_entities_by_type(&self, type_name: &str) -> Result<Vec<EntityHeader>> {
        self.entity_store.list_entities_by_type(type_name).await
    }

    async fn list_entities(&self) -> Result<Vec<EntityHeader>> {
        self.entity_store.list_entities().await
    }

    // ========================================================================
    // Relationship Operations (with Graph sync)
    // ========================================================================

    async fn create_relationship(&self, relationship: &Relationship) -> Result<String> {
        let mut resolved_relationship = relationship.clone();
        if let Some(end1) = resolved_relationship.end1.take() {
            resolved_relationship.end1 = Some(self.resolve_object_id(end1).await?);
        }
        if let Some(end2) = resolved_relationship.end2.take() {
            resolved_relationship.end2 = Some(self.resolve_object_id(end2).await?);
        }

        // Step 1: Create relationship in KV store
        let guid = self
            .relationship_store
            .create_relationship(&resolved_relationship)
            .await?;

        // Step 2: Add edge to graph
        let (end1_guid, end2_guid) =
            match (&resolved_relationship.end1, &resolved_relationship.end2) {
                (Some(e1), Some(e2)) => match (&e1.guid, &e2.guid) {
                    (Some(g1), Some(g2)) => (g1.clone(), g2.clone()),
                    _ => {
                        // Missing GUIDs - rollback
                        let _ = self.relationship_store.delete_relationship(&guid).await;
                        return Err(metavisor_core::CoreError::Validation(
                            "Relationship endpoints must have GUIDs".to_string(),
                        ));
                    }
                },
                _ => {
                    // Missing endpoints - rollback
                    let _ = self.relationship_store.delete_relationship(&guid).await;
                    return Err(metavisor_core::CoreError::Validation(
                        "Relationship must have both endpoints".to_string(),
                    ));
                }
            };

        let propagate_tags = resolved_relationship
            .propagate_tags
            .unwrap_or(PropagateTags::None);

        if let Err(e) = self
            .graph_store
            .add_relationship_edge(
                &guid,
                &resolved_relationship.type_name,
                &end1_guid,
                &end2_guid,
                propagate_tags,
            )
            .await
        {
            // Rollback: delete the relationship we just created
            let _ = self.relationship_store.delete_relationship(&guid).await;
            return Err(e);
        }

        Ok(guid)
    }

    async fn get_relationship(&self, guid: &str) -> Result<Relationship> {
        self.relationship_store.get_relationship(guid).await
    }

    async fn update_relationship(&self, relationship: &Relationship) -> Result<()> {
        self.relationship_store
            .update_relationship(relationship)
            .await?;

        // Relationship updates can change endpoints and propagation rules; rebuild to keep lineage
        // and propagated classifications correct.
        self.graph_store.rebuild_graph().await?;

        Ok(())
    }

    async fn delete_relationship(&self, guid: &str) -> Result<()> {
        // Step 0: Get relationship info first (needed for potential rollback)
        let rel = self.relationship_store.get_relationship(guid).await?;

        // Extract endpoint GUIDs for graph operations
        let (end1_guid, end2_guid) = match (&rel.end1, &rel.end2) {
            (Some(e1), Some(e2)) => match (&e1.guid, &e2.guid) {
                (Some(g1), Some(g2)) => (g1.clone(), g2.clone()),
                _ => {
                    return Err(metavisor_core::CoreError::Validation(
                        "Relationship endpoints must have GUIDs".to_string(),
                    ));
                }
            },
            _ => {
                return Err(metavisor_core::CoreError::Validation(
                    "Relationship must have both endpoints".to_string(),
                ));
            }
        };
        let propagate_tags = rel.propagate_tags.unwrap_or(PropagateTags::None);

        // Step 1: Remove from graph first
        self.graph_store.remove_relationship_edge(guid).await?;

        // Step 2: Delete from KV store
        if let Err(e) = self.relationship_store.delete_relationship(guid).await {
            // Rollback: re-add edge to graph
            if let Err(rollback_err) = self
                .graph_store
                .add_relationship_edge(guid, &rel.type_name, &end1_guid, &end2_guid, propagate_tags)
                .await
            {
                tracing::error!(
                    "Failed to rollback graph edge after KV delete failure: {}",
                    rollback_err
                );
            }
            return Err(e);
        }

        Ok(())
    }

    async fn relationship_exists(&self, guid: &str) -> Result<bool> {
        self.relationship_store.relationship_exists(guid).await
    }

    async fn list_relationships_by_entity(
        &self,
        entity_guid: &str,
    ) -> Result<Vec<RelationshipHeader>> {
        self.relationship_store
            .list_relationships_by_entity(entity_guid)
            .await
    }

    async fn list_relationships_by_type(&self, type_name: &str) -> Result<Vec<RelationshipHeader>> {
        self.relationship_store
            .list_relationships_by_type(type_name)
            .await
    }

    async fn list_relationships(&self) -> Result<Vec<RelationshipHeader>> {
        self.relationship_store.list_relationships().await
    }

    // ========================================================================
    // Graph / Lineage Operations
    // ========================================================================

    async fn rebuild_graph(&self) -> Result<()> {
        self.graph_store.rebuild_graph().await
    }

    async fn get_lineage(
        &self,
        entity_guid: &str,
        direction: TraversalDirection,
        options: LineageQueryOptions,
    ) -> Result<LineageResult> {
        self.graph_store
            .get_lineage(entity_guid, direction, options)
            .await
    }

    async fn get_all_classifications(&self, entity_guid: &str) -> Result<Vec<Classification>> {
        self.graph_store.get_all_classifications(entity_guid).await
    }

    async fn add_classifications(
        &self,
        entity_guid: &str,
        classifications: &[Classification],
    ) -> Result<()> {
        // Get the current entity
        let mut entity = self.entity_store.get_entity(entity_guid).await?;

        // Add new classifications (avoid duplicates)
        for classification in classifications {
            if !entity
                .classifications
                .iter()
                .any(|c| c.type_name == classification.type_name)
            {
                entity.classifications.push(classification.clone());
            }
        }

        // Update entity and sync to graph
        self.update_entity_classifications(&entity).await?;

        Ok(())
    }

    async fn get_classifications(&self, entity_guid: &str) -> Result<Vec<Classification>> {
        let entity = self.entity_store.get_entity(entity_guid).await?;
        Ok(entity.classifications)
    }

    async fn update_classifications(
        &self,
        entity_guid: &str,
        classifications: &[Classification],
    ) -> Result<()> {
        // Get the current entity
        let mut entity = self.entity_store.get_entity(entity_guid).await?;

        // Replace all classifications
        entity.classifications = classifications.to_vec();

        // Update entity and sync to graph
        self.update_entity_classifications(&entity).await?;

        Ok(())
    }

    async fn remove_classification(
        &self,
        entity_guid: &str,
        classification_name: &str,
    ) -> Result<()> {
        // Get the current entity
        let mut entity = self.entity_store.get_entity(entity_guid).await?;

        // Remove the classification
        entity
            .classifications
            .retain(|c| c.type_name != classification_name);

        // Update entity and sync to graph
        self.update_entity_classifications(&entity).await?;

        Ok(())
    }

    async fn get_neighbors(
        &self,
        entity_guid: &str,
        direction: TraversalDirection,
    ) -> Result<Vec<LineageNode>> {
        self.graph_store.get_neighbors(entity_guid, direction).await
    }

    async fn path_exists(&self, from_guid: &str, to_guid: &str, max_depth: usize) -> Result<bool> {
        self.graph_store
            .path_exists(from_guid, to_guid, max_depth)
            .await
    }

    fn graph_stats(&self) -> GraphStats {
        GraphStats {
            node_count: self.graph_store.node_count(),
            edge_count: self.graph_store.edge_count(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metavisor_core::{AttributeDef, EntityDef, ObjectId, RelationshipDef, RelationshipEndDef};
    use serde_json::json;
    use tempfile::TempDir;

    use crate::entity_store::KvEntityStore;
    use crate::graph_store::InMemoryGraphStore;
    use crate::kv::KvStore;
    use crate::relationship_store::KvRelationshipStore;
    use crate::type_store::KvTypeStore;

    async fn create_test_store() -> (Arc<DefaultMetavisorStore>, Arc<KvTypeStore>) {
        let tempdir = TempDir::new().unwrap();
        let kv = KvStore::open(tempdir.path()).unwrap();

        let type_store = Arc::new(KvTypeStore::new(kv.clone()));
        let entity_store = Arc::new(KvEntityStore::new(kv.clone(), type_store.clone()));
        let relationship_store = Arc::new(KvRelationshipStore::new(kv.clone(), type_store.clone()));

        let graph_store: Arc<dyn GraphStore> = Arc::new(InMemoryGraphStore::new(
            entity_store.clone(),
            relationship_store.clone(),
        ));

        let store = Arc::new(DefaultMetavisorStore::new(
            type_store.clone(),
            entity_store,
            relationship_store,
            graph_store,
        ));

        (store, type_store)
    }

    async fn create_test_types(type_store: &KvTypeStore) {
        // Create entity types
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
    async fn test_create_entity_syncs_to_graph() {
        let (store, type_store) = create_test_store().await;
        create_test_types(&type_store).await;

        // Initially graph should be empty
        let stats = store.graph_stats();
        assert_eq!(stats.node_count, 0);

        // Create entity
        let entity = Entity::new("Table").with_attribute("name", json!("users"));
        let guid = store.create_entity(&entity).await.unwrap();

        // Verify entity exists in KV
        let retrieved = store.get_entity(&guid).await.unwrap();
        assert_eq!(retrieved.type_name, "Table");

        // Verify graph was updated
        let stats = store.graph_stats();
        assert_eq!(stats.node_count, 1);
    }

    #[tokio::test]
    async fn test_delete_entity_syncs_to_graph() {
        let (store, type_store) = create_test_store().await;
        create_test_types(&type_store).await;

        // Create entity
        let entity = Entity::new("Table").with_attribute("name", json!("users"));
        let guid = store.create_entity(&entity).await.unwrap();

        assert_eq!(store.graph_stats().node_count, 1);

        // Delete entity
        store.delete_entity(&guid).await.unwrap();

        // Verify entity is deleted from KV
        assert!(!store.entity_exists(&guid).await.unwrap());

        // Verify graph was updated
        assert_eq!(store.graph_stats().node_count, 0);
    }

    #[tokio::test]
    async fn test_create_relationship_syncs_to_graph() {
        let (store, type_store) = create_test_store().await;
        create_test_types(&type_store).await;

        // Create entities
        let table = Entity::new("Table").with_attribute("name", json!("users"));
        let table_guid = store.create_entity(&table).await.unwrap();

        let column = Entity::new("Column").with_attribute("name", json!("id"));
        let column_guid = store.create_entity(&column).await.unwrap();

        // Initially graph should have 2 nodes, 0 edges
        assert_eq!(store.graph_stats().node_count, 2);
        assert_eq!(store.graph_stats().edge_count, 0);

        // Create relationship
        let end1 = ObjectId::by_guid("Table", &table_guid);
        let end2 = ObjectId::by_guid("Column", &column_guid);
        let rel = Relationship::between("table_columns", end1, end2);

        let rel_guid = store.create_relationship(&rel).await.unwrap();

        // Verify relationship exists in KV
        let retrieved = store.get_relationship(&rel_guid).await.unwrap();
        assert_eq!(retrieved.type_name, "table_columns");

        // Verify graph was updated
        assert_eq!(store.graph_stats().edge_count, 1);
    }

    #[tokio::test]
    async fn test_delete_relationship_syncs_to_graph() {
        let (store, type_store) = create_test_store().await;
        create_test_types(&type_store).await;

        // Create entities and relationship
        let table = Entity::new("Table").with_attribute("name", json!("users"));
        let table_guid = store.create_entity(&table).await.unwrap();

        let column = Entity::new("Column").with_attribute("name", json!("id"));
        let column_guid = store.create_entity(&column).await.unwrap();

        let end1 = ObjectId::by_guid("Table", &table_guid);
        let end2 = ObjectId::by_guid("Column", &column_guid);
        let rel = Relationship::between("table_columns", end1, end2);
        let rel_guid = store.create_relationship(&rel).await.unwrap();

        assert_eq!(store.graph_stats().edge_count, 1);

        // Delete relationship
        store.delete_relationship(&rel_guid).await.unwrap();

        // Verify relationship is deleted from KV
        assert!(!store.relationship_exists(&rel_guid).await.unwrap());

        // Verify graph was updated
        assert_eq!(store.graph_stats().edge_count, 0);
    }

    #[tokio::test]
    async fn test_lineage_operations() {
        let (store, type_store) = create_test_store().await;
        create_test_types(&type_store).await;

        // Create entities
        let table = Entity::new("Table").with_attribute("name", json!("users"));
        let table_guid = store.create_entity(&table).await.unwrap();

        let column1 = Entity::new("Column").with_attribute("name", json!("id"));
        let column1_guid = store.create_entity(&column1).await.unwrap();

        let column2 = Entity::new("Column").with_attribute("name", json!("name"));
        let column2_guid = store.create_entity(&column2).await.unwrap();

        // Create relationships: table -> column1, table -> column2
        let rel1 = Relationship::between(
            "table_columns",
            ObjectId::by_guid("Table", &table_guid),
            ObjectId::by_guid("Column", &column1_guid),
        );
        store.create_relationship(&rel1).await.unwrap();

        let rel2 = Relationship::between(
            "table_columns",
            ObjectId::by_guid("Table", &table_guid),
            ObjectId::by_guid("Column", &column2_guid),
        );
        store.create_relationship(&rel2).await.unwrap();

        // Test get_neighbors
        let neighbors = store
            .get_neighbors(&table_guid, TraversalDirection::Output)
            .await
            .unwrap();
        assert_eq!(neighbors.len(), 2);

        // Test get_lineage
        let lineage = store
            .get_lineage(
                &table_guid,
                TraversalDirection::Output,
                LineageQueryOptions::new().with_depth(3),
            )
            .await
            .unwrap();
        assert_eq!(lineage.node_count(), 3); // table + 2 columns
        assert_eq!(lineage.edge_count(), 2);
    }

    #[tokio::test]
    async fn test_type_operations() {
        let (store, _type_store) = create_test_store().await;

        // Create type
        let entity_def =
            EntityDef::new("TestType").attribute(AttributeDef::new("name", "string").required());
        store.create_type(&TypeDef::from(entity_def)).await.unwrap();

        // Verify type exists
        assert!(store.type_exists("TestType").await.unwrap());

        // Get type
        let retrieved = store.get_type("TestType").await.unwrap();
        assert!(matches!(retrieved, TypeDef::Entity(_)));

        // List types
        let types = store.list_types().await.unwrap();
        assert!(types.contains(&"TestType".to_string()));

        // Delete type
        store.delete_type("TestType").await.unwrap();
        assert!(!store.type_exists("TestType").await.unwrap());
    }

    #[tokio::test]
    async fn test_add_classifications() {
        let (store, type_store) = create_test_store().await;
        create_test_types(&type_store).await;

        // Create entity without classifications
        let entity = Entity::new("Table").with_attribute("name", json!("users"));
        let guid = store.create_entity(&entity).await.unwrap();

        // Verify no classifications initially
        let classifications = store.get_classifications(&guid).await.unwrap();
        assert!(classifications.is_empty());

        // Add classification
        let classification = Classification::new("PII").with_propagate(true);
        store
            .add_classifications(&guid, std::slice::from_ref(&classification))
            .await
            .unwrap();

        // Verify classification was added
        let classifications = store.get_classifications(&guid).await.unwrap();
        assert_eq!(classifications.len(), 1);
        assert_eq!(classifications[0].type_name, "PII");

        // Add duplicate classification (should be ignored)
        store
            .add_classifications(&guid, &[classification])
            .await
            .unwrap();

        let classifications = store.get_classifications(&guid).await.unwrap();
        assert_eq!(classifications.len(), 1); // Still only 1
    }

    #[tokio::test]
    async fn test_update_classifications() {
        let (store, type_store) = create_test_store().await;
        create_test_types(&type_store).await;

        // Create entity
        let entity = Entity::new("Table").with_attribute("name", json!("users"));
        let guid = store.create_entity(&entity).await.unwrap();

        // Add initial classification
        let classification1 = Classification::new("PII");
        store
            .add_classifications(&guid, &[classification1])
            .await
            .unwrap();

        // Update with different classifications
        let classification2 = Classification::new("SENSITIVE");
        let classification3 = Classification::new("CONFIDENTIAL");
        store
            .update_classifications(&guid, &[classification2, classification3])
            .await
            .unwrap();

        // Verify classifications were replaced
        let classifications = store.get_classifications(&guid).await.unwrap();
        assert_eq!(classifications.len(), 2);
        let names: Vec<&str> = classifications
            .iter()
            .map(|c| c.type_name.as_str())
            .collect();
        assert!(names.contains(&"SENSITIVE"));
        assert!(names.contains(&"CONFIDENTIAL"));
        assert!(!names.contains(&"PII")); // Old one should be gone
    }

    #[tokio::test]
    async fn test_remove_classification() {
        let (store, type_store) = create_test_store().await;
        create_test_types(&type_store).await;

        // Create entity with classifications
        let classification1 = Classification::new("PII");
        let classification2 = Classification::new("SENSITIVE");
        let entity = Entity::new("Table")
            .with_attribute("name", json!("users"))
            .with_classification(classification1)
            .with_classification(classification2);
        let guid = store.create_entity(&entity).await.unwrap();

        // Verify initial classifications
        let classifications = store.get_classifications(&guid).await.unwrap();
        assert_eq!(classifications.len(), 2);

        // Remove one classification
        store.remove_classification(&guid, "PII").await.unwrap();

        // Verify classification was removed
        let classifications = store.get_classifications(&guid).await.unwrap();
        assert_eq!(classifications.len(), 1);
        assert_eq!(classifications[0].type_name, "SENSITIVE");

        // Remove non-existent classification (should succeed silently)
        store
            .remove_classification(&guid, "NONEXISTENT")
            .await
            .unwrap();

        // Verify nothing changed
        let classifications = store.get_classifications(&guid).await.unwrap();
        assert_eq!(classifications.len(), 1);
    }
}
