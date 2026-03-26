//! GraphStore trait definition
//!
//! Abstract interface for graph-based operations

use async_trait::async_trait;
use std::any::Any;

use crate::Result;

/// GraphStore trait for graph-based operations
///
/// This trait provides an abstraction layer for lineage tracking,
/// graph traversal, and classification propagation.
#[async_trait]
pub trait GraphStore: Send + Sync + Any {
    /// Convert to Any for downcasting to concrete types
    fn as_any(&self) -> &dyn Any;
    /// Build or rebuild the in-memory graph from persisted data
    ///
    /// This should be called on startup or when the graph needs to be
    /// synchronized with the underlying storage.
    async fn rebuild_graph(&self) -> Result<()>;

    /// Add an entity node to the graph
    ///
    /// This is called when a new entity is created.
    async fn add_entity_node(&self, entity_guid: &str, entity_type: &str) -> Result<()>;

    /// Remove an entity node from the graph
    ///
    /// This is called when an entity is deleted.
    /// All edges connected to this node will also be removed.
    async fn remove_entity_node(&self, entity_guid: &str) -> Result<()>;

    /// Update an entity node in the graph
    ///
    /// This is called when an entity is updated (e.g., type_name, display_name, or classifications changed).
    /// Returns true if the node was found and updated, false if not found.
    async fn update_entity_node(
        &self,
        entity_guid: &str,
        entity_type: &str,
        display_name: Option<&str>,
        classifications: Vec<String>,
    ) -> Result<bool>;

    /// Add a relationship edge to the graph
    ///
    /// This is called when a new relationship is created.
    /// The edge direction is from `from_guid` (end1) to `to_guid` (end2).
    async fn add_relationship_edge(
        &self,
        relationship_guid: &str,
        relationship_type: &str,
        from_guid: &str,
        to_guid: &str,
        propagate_tags: crate::types::PropagateTags,
    ) -> Result<()>;

    /// Remove a relationship edge from the graph
    ///
    /// This is called when a relationship is deleted.
    async fn remove_relationship_edge(&self, relationship_guid: &str) -> Result<()>;

    /// Get the total number of nodes in the graph
    fn node_count(&self) -> usize;

    /// Get the total number of edges in the graph
    fn edge_count(&self) -> usize;

    /// Check if the graph is empty
    fn is_empty(&self) -> bool {
        self.node_count() == 0
    }
}
