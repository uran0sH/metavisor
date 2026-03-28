//! Consistency checking and repair for Metavisor storage
//!
//! This module provides utilities to detect and repair inconsistencies between
//! KV storage and graph storage.

use std::collections::HashSet;

use metavisor_core::{EntityStore, GraphStore, RelationshipStore, Result};

/// Report of consistency check results
#[derive(Debug, Clone)]
pub struct ConsistencyReport {
    /// Entities in KV but missing in graph
    pub entities_missing_in_graph: Vec<String>,
    /// Relationships in KV but missing in graph
    pub relationships_missing_in_graph: Vec<String>,
    /// Total entities checked
    pub total_entities: usize,
    /// Total relationships checked
    pub total_relationships: usize,
}

impl ConsistencyReport {
    /// Check if the report indicates any inconsistencies
    pub fn is_consistent(&self) -> bool {
        self.entities_missing_in_graph.is_empty() && self.relationships_missing_in_graph.is_empty()
    }

    /// Get total number of issues
    pub fn issue_count(&self) -> usize {
        self.entities_missing_in_graph.len() + self.relationships_missing_in_graph.len()
    }
}

/// Consistency checker and repair tool
pub struct ConsistencyChecker;

impl ConsistencyChecker {
    /// Check consistency between KV and graph storage
    pub async fn check_consistency(
        entity_store: &dyn EntityStore,
        relationship_store: &dyn RelationshipStore,
        graph_store: &dyn GraphStore,
    ) -> Result<ConsistencyReport> {
        // Get all entities from KV
        let kv_entities = entity_store.list_entities().await?;
        let kv_entity_guids: HashSet<String> =
            kv_entities.iter().filter_map(|h| h.guid.clone()).collect();

        // Get all relationships from KV
        let kv_relationships = relationship_store.list_relationships().await?;
        let kv_relationship_guids: HashSet<String> = kv_relationships
            .iter()
            .filter_map(|h| h.guid.clone())
            .collect();

        // Get all GUIDs currently in the graph
        let graph_node_guids: HashSet<String> = graph_store.list_node_guids().into_iter().collect();
        let graph_edge_guids: HashSet<String> = graph_store.list_edge_guids().into_iter().collect();

        // Find entities in KV but missing from graph
        let entities_missing_in_graph: Vec<String> = kv_entity_guids
            .iter()
            .filter(|guid| !graph_node_guids.contains(*guid))
            .cloned()
            .collect();

        if !entities_missing_in_graph.is_empty() {
            tracing::warn!(
                "Found {} entities in KV but missing from graph: {:?}",
                entities_missing_in_graph.len(),
                entities_missing_in_graph
            );
        }

        // Find relationships in KV but missing from graph
        let relationships_missing_in_graph: Vec<String> = kv_relationship_guids
            .iter()
            .filter(|guid| !graph_edge_guids.contains(*guid))
            .cloned()
            .collect();

        if !relationships_missing_in_graph.is_empty() {
            tracing::warn!(
                "Found {} relationships in KV but missing from graph: {:?}",
                relationships_missing_in_graph.len(),
                relationships_missing_in_graph
            );
        }

        Ok(ConsistencyReport {
            entities_missing_in_graph,
            relationships_missing_in_graph,
            total_entities: kv_entity_guids.len(),
            total_relationships: kv_relationship_guids.len(),
        })
    }

    /// Repair inconsistencies by syncing missing data to graph
    pub async fn repair_consistency(
        entity_store: &dyn EntityStore,
        relationship_store: &dyn RelationshipStore,
        graph_store: &dyn GraphStore,
        report: &ConsistencyReport,
    ) -> Result<RepairResult> {
        let mut repaired_entities = 0;
        let mut repaired_relationships = 0;
        let mut failed_entities = 0;
        let mut failed_relationships = 0;

        // Repair missing entities
        for guid in &report.entities_missing_in_graph {
            match entity_store.get_entity(guid).await {
                Ok(entity) => {
                    let display_name = entity.attributes.get("name").and_then(|v| v.as_str());
                    let classifications: Vec<String> = entity
                        .classifications
                        .iter()
                        .map(|c| c.type_name.clone())
                        .collect();
                    match graph_store
                        .add_entity_node(guid, &entity.type_name, display_name, classifications)
                        .await
                    {
                        Ok(_) => {
                            tracing::info!("Repaired: added entity {} to graph", guid);
                            repaired_entities += 1;
                        }
                        Err(e) => {
                            tracing::error!("Failed to repair entity {} in graph: {}", guid, e);
                            failed_entities += 1;
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to get entity {} from KV during repair: {}", guid, e);
                    failed_entities += 1;
                }
            }
        }

        // Repair missing relationships
        for guid in &report.relationships_missing_in_graph {
            match relationship_store.get_relationship(guid).await {
                Ok(rel) => {
                    let end1_guid = rel
                        .end1
                        .as_ref()
                        .and_then(|e| e.guid.as_ref())
                        .map(|g| g.as_str());
                    let end2_guid = rel
                        .end2
                        .as_ref()
                        .and_then(|e| e.guid.as_ref())
                        .map(|g| g.as_str());
                    match (end1_guid, end2_guid) {
                        (Some(from), Some(to)) => {
                            match graph_store
                                .add_relationship_edge(
                                    guid,
                                    &rel.type_name,
                                    from,
                                    to,
                                    rel.propagate_tags.unwrap_or_default(),
                                )
                                .await
                            {
                                Ok(_) => {
                                    tracing::info!(
                                        "Repaired: added relationship {} to graph",
                                        guid
                                    );
                                    repaired_relationships += 1;
                                }
                                Err(e) => {
                                    tracing::error!(
                                        "Failed to repair relationship {} in graph: {}",
                                        guid,
                                        e
                                    );
                                    failed_relationships += 1;
                                }
                            }
                        }
                        _ => {
                            tracing::warn!(
                                "Skipping relationship {}: missing endpoint GUIDs",
                                guid
                            );
                            failed_relationships += 1;
                        }
                    }
                }
                Err(e) => {
                    tracing::error!(
                        "Failed to get relationship {} from KV during repair: {}",
                        guid,
                        e
                    );
                    failed_relationships += 1;
                }
            }
        }

        Ok(RepairResult {
            repaired_entities,
            repaired_relationships,
            failed_entities,
            failed_relationships,
        })
    }

    /// Full check and repair in one operation
    pub async fn check_and_repair(
        entity_store: &dyn EntityStore,
        relationship_store: &dyn RelationshipStore,
        graph_store: &dyn GraphStore,
    ) -> Result<(ConsistencyReport, RepairResult)> {
        let report = Self::check_consistency(entity_store, relationship_store, graph_store).await?;

        if report.is_consistent() {
            tracing::info!("Storage is consistent, no repair needed");
            return Ok((report, RepairResult::default()));
        }

        tracing::warn!(
            "Found {} consistency issues, starting repair",
            report.issue_count()
        );

        let repair_result =
            Self::repair_consistency(entity_store, relationship_store, graph_store, &report)
                .await?;

        Ok((report, repair_result))
    }
}

/// Result of repair operation
#[derive(Debug, Clone, Default)]
pub struct RepairResult {
    /// Number of entities successfully repaired
    pub repaired_entities: usize,
    /// Number of relationships successfully repaired
    pub repaired_relationships: usize,
    /// Number of entities that failed to repair
    pub failed_entities: usize,
    /// Number of relationships that failed to repair
    pub failed_relationships: usize,
}

impl RepairResult {
    /// Check if all repairs succeeded
    pub fn all_succeeded(&self) -> bool {
        self.failed_entities == 0 && self.failed_relationships == 0
    }

    /// Get total number of successful repairs
    pub fn total_repaired(&self) -> usize {
        self.repaired_entities + self.repaired_relationships
    }

    /// Get total number of failures
    pub fn total_failed(&self) -> usize {
        self.failed_entities + self.failed_relationships
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metavisor_core::{
        AttributeDef, Entity, EntityDef, ObjectId, PropagateTags, Relationship, RelationshipDef,
        RelationshipEndDef, TypeDef, TypeStore,
    };
    use serde_json::json;
    use std::sync::Arc;

    use crate::entity_store::KvEntityStore;
    use crate::grafeo_graph_store::GrafeoGraphStore;
    use crate::kv::KvStore;
    use crate::relationship_store::KvRelationshipStore;
    use crate::type_store::KvTypeStore;

    /// Build a complete test store setup (KV stores + empty in-memory graph).
    async fn setup() -> (
        Arc<KvTypeStore>,
        Arc<KvEntityStore>,
        Arc<KvRelationshipStore>,
        Arc<dyn GraphStore>,
    ) {
        let tempdir = tempfile::TempDir::new().unwrap();
        let kv = KvStore::open(tempdir.path()).unwrap();
        let type_store = Arc::new(KvTypeStore::new(kv.clone()));
        let entity_store = Arc::new(KvEntityStore::new(kv.clone(), type_store.clone()));
        let relationship_store = Arc::new(KvRelationshipStore::new(kv.clone(), type_store.clone()));
        let graph_store: Arc<dyn GraphStore> = Arc::new(GrafeoGraphStore::new_in_memory().unwrap());
        (type_store, entity_store, relationship_store, graph_store)
    }

    /// Create Table, Column types and a table_columns relationship type.
    async fn create_test_types(type_store: &KvTypeStore) {
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

    // ── unit tests (no async) ──────────────────────────────────────

    #[test]
    fn test_consistency_report() {
        let report = ConsistencyReport {
            entities_missing_in_graph: vec![],
            relationships_missing_in_graph: vec![],
            total_entities: 10,
            total_relationships: 5,
        };
        assert!(report.is_consistent());
        assert_eq!(report.issue_count(), 0);

        let report = ConsistencyReport {
            entities_missing_in_graph: vec!["guid1".to_string()],
            relationships_missing_in_graph: vec![],
            total_entities: 10,
            total_relationships: 5,
        };
        assert!(!report.is_consistent());
        assert_eq!(report.issue_count(), 1);
    }

    #[test]
    fn test_repair_result() {
        let result = RepairResult::default();
        assert!(result.all_succeeded());
        assert_eq!(result.total_repaired(), 0);

        let result = RepairResult {
            repaired_entities: 5,
            repaired_relationships: 3,
            failed_entities: 0,
            failed_relationships: 0,
        };
        assert!(result.all_succeeded());
        assert_eq!(result.total_repaired(), 8);
    }

    // ── integration tests (async, real KV + graph) ─────────────────

    #[tokio::test]
    async fn test_consistent_when_graph_matches_kv() {
        let (type_store, entity_store, _rel_store, graph_store) = setup().await;
        create_test_types(&type_store).await;

        // Write entity directly to KV (graph is empty)
        let entity = Entity::new("Table").with_attribute("name", json!("users"));
        let _guid = entity_store.create_entity(&entity).await.unwrap();
        assert_eq!(
            graph_store.node_count(),
            0,
            "graph should be empty before repair"
        );

        // Repair consistency → KV and graph should now be in sync
        let (report, repair) = ConsistencyChecker::check_and_repair(
            entity_store.as_ref(),
            &crate::relationship_store::KvRelationshipStore::new(
                crate::kv::KvStore::open(tempfile::TempDir::new().unwrap().path()).unwrap(),
                type_store.clone(),
            ),
            graph_store.as_ref(),
        )
        .await
        .unwrap();

        // Report reflects pre-repair state (found 1 missing), repair should have fixed it
        assert_eq!(report.total_entities, 1);
        assert_eq!(report.entities_missing_in_graph.len(), 1);
        assert!(repair.all_succeeded());
        assert_eq!(graph_store.node_count(), 1);

        // Re-check → should now be consistent
        let report2 = ConsistencyChecker::check_consistency(
            entity_store.as_ref(),
            &crate::relationship_store::KvRelationshipStore::new(
                crate::kv::KvStore::open(tempfile::TempDir::new().unwrap().path()).unwrap(),
                type_store.clone(),
            ),
            graph_store.as_ref(),
        )
        .await
        .unwrap();
        assert!(report2.is_consistent());
    }

    #[tokio::test]
    async fn test_detect_entity_missing_from_graph() {
        let (type_store, entity_store, relationship_store, graph_store) = setup().await;
        create_test_types(&type_store).await;

        // Write entity to KV only — graph stays empty
        let entity = Entity::new("Table").with_attribute("name", json!("orders"));
        let guid = entity_store.create_entity(&entity).await.unwrap();
        assert_eq!(graph_store.node_count(), 0);

        let report = ConsistencyChecker::check_consistency(
            entity_store.as_ref(),
            relationship_store.as_ref(),
            graph_store.as_ref(),
        )
        .await
        .unwrap();

        assert!(!report.is_consistent());
        assert_eq!(report.issue_count(), 1);
        assert_eq!(report.entities_missing_in_graph, vec![guid]);
    }

    #[tokio::test]
    async fn test_detect_relationship_missing_from_graph() {
        let (type_store, entity_store, relationship_store, graph_store) = setup().await;
        create_test_types(&type_store).await;

        // Create two entities (graph stays empty)
        let t1 = Entity::new("Table").with_attribute("name", json!("users"));
        let t1_guid = entity_store.create_entity(&t1).await.unwrap();
        let c1 = Entity::new("Column").with_attribute("name", json!("id"));
        let c1_guid = entity_store.create_entity(&c1).await.unwrap();

        // Create relationship in KV only
        let rel = Relationship::between(
            "table_columns",
            ObjectId::by_guid("Table", &t1_guid),
            ObjectId::by_guid("Column", &c1_guid),
        );
        let rel_guid = relationship_store.create_relationship(&rel).await.unwrap();

        assert_eq!(graph_store.node_count(), 0);
        assert_eq!(graph_store.edge_count(), 0);

        let report = ConsistencyChecker::check_consistency(
            entity_store.as_ref(),
            relationship_store.as_ref(),
            graph_store.as_ref(),
        )
        .await
        .unwrap();

        assert!(!report.is_consistent());
        // Both entities and the relationship are missing from graph
        assert_eq!(report.entities_missing_in_graph.len(), 2);
        assert!(report.entities_missing_in_graph.contains(&t1_guid));
        assert!(report.entities_missing_in_graph.contains(&c1_guid));
        assert_eq!(report.relationships_missing_in_graph, vec![rel_guid]);
        assert_eq!(report.issue_count(), 3);
    }

    #[tokio::test]
    async fn test_detect_partial_inconsistency() {
        // Some entities synced, some not
        let (type_store, entity_store, relationship_store, graph_store) = setup().await;
        create_test_types(&type_store).await;

        let e1 = Entity::new("Table").with_attribute("name", json!("synced"));
        let e1_guid = entity_store.create_entity(&e1).await.unwrap();

        let e2 = Entity::new("Table").with_attribute("name", json!("not_synced"));
        let e2_guid = entity_store.create_entity(&e2).await.unwrap();

        // Sync only e1 to graph
        graph_store
            .add_entity_node(&e1_guid, "Table", None, vec![])
            .await
            .unwrap();

        let report = ConsistencyChecker::check_consistency(
            entity_store.as_ref(),
            relationship_store.as_ref(),
            graph_store.as_ref(),
        )
        .await
        .unwrap();

        assert!(!report.is_consistent());
        assert_eq!(report.issue_count(), 1);
        assert_eq!(report.entities_missing_in_graph, vec![e2_guid]);
    }

    #[tokio::test]
    async fn test_repair_adds_missing_entities_to_graph() {
        let (type_store, entity_store, relationship_store, graph_store) = setup().await;
        create_test_types(&type_store).await;

        let e1 = Entity::new("Table").with_attribute("name", json!("a"));
        entity_store.create_entity(&e1).await.unwrap();
        let e2 = Entity::new("Column").with_attribute("name", json!("b"));
        entity_store.create_entity(&e2).await.unwrap();

        assert_eq!(graph_store.node_count(), 0);

        let report = ConsistencyChecker::check_consistency(
            entity_store.as_ref(),
            relationship_store.as_ref(),
            graph_store.as_ref(),
        )
        .await
        .unwrap();
        assert_eq!(report.issue_count(), 2);

        let repair = ConsistencyChecker::repair_consistency(
            entity_store.as_ref(),
            relationship_store.as_ref(),
            graph_store.as_ref(),
            &report,
        )
        .await
        .unwrap();

        assert_eq!(repair.repaired_entities, 2);
        assert!(repair.all_succeeded());
        assert_eq!(graph_store.node_count(), 2);
    }

    #[tokio::test]
    async fn test_repair_adds_missing_relationships_to_graph() {
        let (type_store, entity_store, relationship_store, graph_store) = setup().await;
        create_test_types(&type_store).await;

        // Create entities (graph empty)
        let t1 = Entity::new("Table").with_attribute("name", json!("users"));
        let t1_guid = entity_store.create_entity(&t1).await.unwrap();
        let c1 = Entity::new("Column").with_attribute("name", json!("id"));
        let c1_guid = entity_store.create_entity(&c1).await.unwrap();

        // Create relationship (graph empty)
        let rel = Relationship::between(
            "table_columns",
            ObjectId::by_guid("Table", &t1_guid),
            ObjectId::by_guid("Column", &c1_guid),
        );
        relationship_store.create_relationship(&rel).await.unwrap();

        assert_eq!(graph_store.node_count(), 0);
        assert_eq!(graph_store.edge_count(), 0);

        let report = ConsistencyChecker::check_consistency(
            entity_store.as_ref(),
            relationship_store.as_ref(),
            graph_store.as_ref(),
        )
        .await
        .unwrap();
        assert_eq!(report.issue_count(), 3); // 2 entities + 1 relationship

        let repair = ConsistencyChecker::repair_consistency(
            entity_store.as_ref(),
            relationship_store.as_ref(),
            graph_store.as_ref(),
            &report,
        )
        .await
        .unwrap();

        assert!(repair.all_succeeded());
        assert_eq!(repair.repaired_entities, 2);
        assert_eq!(repair.repaired_relationships, 1);
        assert_eq!(graph_store.node_count(), 2);
        assert_eq!(graph_store.edge_count(), 1);

        // Verify graph is now consistent
        let report2 = ConsistencyChecker::check_consistency(
            entity_store.as_ref(),
            relationship_store.as_ref(),
            graph_store.as_ref(),
        )
        .await
        .unwrap();
        assert!(report2.is_consistent());
    }

    #[tokio::test]
    async fn test_check_and_repair_idempotent() {
        let (type_store, entity_store, relationship_store, graph_store) = setup().await;
        create_test_types(&type_store).await;

        let entity = Entity::new("Table").with_attribute("name", json!("t"));
        entity_store.create_entity(&entity).await.unwrap();

        // First call: detects and repairs
        let (report, repair) = ConsistencyChecker::check_and_repair(
            entity_store.as_ref(),
            relationship_store.as_ref(),
            graph_store.as_ref(),
        )
        .await
        .unwrap();
        assert!(!report.is_consistent());
        assert_eq!(repair.repaired_entities, 1);

        // Second call: already consistent, no-op
        let (report2, repair2) = ConsistencyChecker::check_and_repair(
            entity_store.as_ref(),
            relationship_store.as_ref(),
            graph_store.as_ref(),
        )
        .await
        .unwrap();
        assert!(report2.is_consistent());
        assert_eq!(repair2.total_repaired(), 0);
    }

    #[tokio::test]
    async fn test_consistent_with_empty_stores() {
        let (_, entity_store, relationship_store, graph_store) = setup().await;

        let report = ConsistencyChecker::check_consistency(
            entity_store.as_ref(),
            relationship_store.as_ref(),
            graph_store.as_ref(),
        )
        .await
        .unwrap();

        assert!(report.is_consistent());
        assert_eq!(report.total_entities, 0);
        assert_eq!(report.total_relationships, 0);
    }
}
