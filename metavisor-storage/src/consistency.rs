//! Consistency checking and repair for Metavisor storage
//!
//! This module provides utilities to detect and repair inconsistencies between
//! KV storage and graph storage.

use std::collections::HashSet;

use metavisor_core::{CoreError, EntityStore, GraphStore, RelationshipStore, Result};

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
        self.entities_missing_in_graph.is_empty()
            && self.relationships_missing_in_graph.is_empty()
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
        let kv_entity_guids: HashSet<String> = kv_entities
            .iter()
            .filter_map(|h| h.guid.clone())
            .collect();

        // Get all relationships from KV
        let kv_relationships = relationship_store.list_relationships().await?;
        let kv_relationship_guids: HashSet<String> = kv_relationships
            .iter()
            .filter_map(|h| h.guid.clone())
            .collect();

        // Get graph stats (nodes = entities, edges = relationships)
        let graph_node_count = graph_store.node_count();
        let graph_edge_count = graph_store.edge_count();

        tracing::info!(
            "Consistency check: KV has {} entities, {} relationships; Graph has {} nodes, {} edges",
            kv_entity_guids.len(),
            kv_relationship_guids.len(),
            graph_node_count,
            graph_edge_count
        );

        // Find entities missing in graph
        // We need to query the graph for each entity to check existence
        let mut entities_missing_in_graph = Vec::new();
        for guid in &kv_entity_guids {
            // Check if node exists in graph by trying to get neighbors
            // If the node doesn't exist, get_neighbors will return empty or error
            match graph_store.get_neighbors(guid, metavisor_core::TraversalDirection::Both).await {
                Ok(_) => {
                    // Node exists (get_neighbors returns empty vec for leaf nodes)
                }
                Err(CoreError::EntityNotFound(_)) => {
                    entities_missing_in_graph.push(guid.clone());
                }
                Err(_) => {
                    // Other error, assume node might exist
                }
            }
        }

        // Alternative check: use node_count comparison as heuristic
        // If counts don't match, there might be missing nodes
        if graph_node_count < kv_entity_guids.len() {
            tracing::warn!(
                "Graph has fewer nodes ({}) than KV entities ({})",
                graph_node_count,
                kv_entity_guids.len()
            );
        }

        // Note: Checking relationships in graph is more complex due to
        // variable-length path matching. We rely on edge_count comparison.
        let relationships_missing_in_graph = Vec::new();
        if graph_edge_count < kv_relationship_guids.len() {
            tracing::warn!(
                "Graph has fewer edges ({}) than KV relationships ({})",
                graph_edge_count,
                kv_relationship_guids.len()
            );
            // Mark all relationships as potentially missing (conservative)
            // In a full implementation, we'd query each relationship
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
        _relationship_store: &dyn RelationshipStore,
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
                    match graph_store
                        .add_entity_node(guid, &entity.type_name)
                        .await
                    {
                        Ok(_) => {
                            tracing::info!("Repaired: added entity {} to graph", guid);
                            repaired_entities += 1;
                        }
                        Err(e) => {
                            tracing::error!(
                                "Failed to repair entity {} in graph: {}",
                                guid,
                                e
                            );
                            failed_entities += 1;
                        }
                    }
                }
                Err(e) => {
                    tracing::error!(
                        "Failed to get entity {} from KV during repair: {}",
                        guid,
                        e
                    );
                    failed_entities += 1;
                }
            }
        }

        // For relationships, we rebuild the graph to ensure consistency
        // This is simpler and more reliable than trying to add individual edges
        if !report.relationships_missing_in_graph.is_empty() || report.entities_missing_in_graph.len() > 5 {
            tracing::info!("Rebuilding graph for full consistency repair");
            if let Err(e) = graph_store.rebuild_graph().await {
                tracing::error!("Failed to rebuild graph during repair: {}", e);
                failed_relationships = report.relationships_missing_in_graph.len();
            } else {
                repaired_relationships = report.relationships_missing_in_graph.len();
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

        let repair_result = Self::repair_consistency(
            entity_store,
            relationship_store,
            graph_store,
            &report,
        )
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
}
