//! Graph types for lineage and traversal
//!
//! Core types for representing the entity relationship graph

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

use crate::types::PropagateTags;

/// Unique identifier for a node in the lineage graph (Entity GUID)
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(pub String);

impl NodeId {
    pub fn new(guid: impl Into<String>) -> Self {
        Self(guid.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for NodeId {
    fn from(guid: String) -> Self {
        Self(guid)
    }
}

impl From<&str> for NodeId {
    fn from(guid: &str) -> Self {
        Self(guid.to_string())
    }
}

/// Node in the lineage graph representing an entity
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageNode {
    /// Entity GUID
    pub id: NodeId,
    /// Entity type name
    pub entity_type: String,
    /// Entity display name (from attributes, typically "name" attribute)
    pub display_name: Option<String>,
    /// Direct classifications on this entity (classification type names)
    pub classifications: Vec<String>,
    /// Propagated classifications from upstream entities
    #[serde(default, skip_serializing_if = "HashSet::is_empty")]
    pub propagated_classifications: HashSet<String>,
}

impl LineageNode {
    pub fn new(guid: impl Into<String>, entity_type: impl Into<String>) -> Self {
        Self {
            id: NodeId::new(guid),
            entity_type: entity_type.into(),
            display_name: None,
            classifications: Vec::new(),
            propagated_classifications: HashSet::new(),
        }
    }

    pub fn with_display_name(mut self, name: impl Into<String>) -> Self {
        self.display_name = Some(name.into());
        self
    }

    pub fn with_classifications(mut self, classifications: Vec<String>) -> Self {
        self.classifications = classifications;
        self
    }

    pub fn add_classification(mut self, classification: impl Into<String>) -> Self {
        self.classifications.push(classification.into());
        self
    }

    pub fn add_propagated_classification(&mut self, classification: impl Into<String>) {
        self.propagated_classifications
            .insert(classification.into());
    }

    /// Get all classifications (direct + propagated)
    pub fn all_classifications(&self) -> Vec<&String> {
        let mut all: Vec<&String> = self.classifications.iter().collect();
        for c in &self.propagated_classifications {
            all.push(c);
        }
        all.sort();
        all.dedup();
        all
    }
}

/// Edge in the lineage graph representing a relationship
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageEdge {
    /// Relationship GUID
    pub id: String,
    /// Relationship type name
    pub relationship_type: String,
    /// Tag propagation direction
    pub propagate_tags: PropagateTags,
    /// Optional label
    pub label: Option<String>,
}

impl LineageEdge {
    pub fn new(
        guid: impl Into<String>,
        relationship_type: impl Into<String>,
        propagate_tags: PropagateTags,
    ) -> Self {
        Self {
            id: guid.into(),
            relationship_type: relationship_type.into(),
            propagate_tags,
            label: None,
        }
    }

    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }

    /// Check if classification should propagate in the forward direction (end1 -> end2)
    pub fn propagates_forward(&self) -> bool {
        matches!(
            self.propagate_tags,
            PropagateTags::OneToTwo | PropagateTags::Both
        )
    }

    /// Check if classification should propagate in the backward direction (end2 -> end1)
    pub fn propagates_backward(&self) -> bool {
        matches!(
            self.propagate_tags,
            PropagateTags::TwoToOne | PropagateTags::Both
        )
    }
}

/// Direction for lineage traversal
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TraversalDirection {
    /// Input lineage (upstream) - follow edges to sources
    Input,
    /// Output lineage (downstream) - follow edges to targets
    Output,
    /// Both directions
    #[default]
    Both,
}

impl std::fmt::Display for TraversalDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TraversalDirection::Input => write!(f, "INPUT"),
            TraversalDirection::Output => write!(f, "OUTPUT"),
            TraversalDirection::Both => write!(f, "BOTH"),
        }
    }
}

impl std::str::FromStr for TraversalDirection {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "INPUT" | "UPSTREAM" => Ok(TraversalDirection::Input),
            "OUTPUT" | "DOWNSTREAM" => Ok(TraversalDirection::Output),
            "BOTH" => Ok(TraversalDirection::Both),
            _ => Err(format!("Unknown traversal direction: {}", s)),
        }
    }
}

/// Edge information for API response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageEdgeInfo {
    /// Source entity GUID
    pub from_guid: String,
    /// Target entity GUID
    pub to_guid: String,
    /// Relationship GUID
    pub relationship_guid: String,
    /// Relationship type name
    pub relationship_type: String,
    /// Optional label
    pub label: Option<String>,
}

impl LineageEdgeInfo {
    pub fn new(
        from_guid: impl Into<String>,
        to_guid: impl Into<String>,
        relationship_guid: impl Into<String>,
        relationship_type: impl Into<String>,
    ) -> Self {
        Self {
            from_guid: from_guid.into(),
            to_guid: to_guid.into(),
            relationship_guid: relationship_guid.into(),
            relationship_type: relationship_type.into(),
            label: None,
        }
    }

    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }
}

/// Result of a lineage query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageResult {
    /// The starting entity GUID
    pub root_guid: String,
    /// Direction of traversal
    pub direction: TraversalDirection,
    /// Maximum depth traversed
    pub depth: usize,
    /// All nodes in the lineage (including root)
    pub nodes: Vec<LineageNode>,
    /// All edges in the lineage
    pub edges: Vec<LineageEdgeInfo>,
    /// GUID -> List of connected GUIDs mapping (for graph visualization)
    pub adjacency: HashMap<String, Vec<String>>,
}

impl LineageResult {
    pub fn new(root_guid: impl Into<String>, direction: TraversalDirection, depth: usize) -> Self {
        Self {
            root_guid: root_guid.into(),
            direction,
            depth,
            nodes: Vec::new(),
            edges: Vec::new(),
            adjacency: HashMap::new(),
        }
    }

    pub fn add_node(&mut self, node: LineageNode) {
        self.nodes.push(node);
    }

    pub fn add_edge(&mut self, edge: LineageEdgeInfo) {
        // Update adjacency
        self.adjacency
            .entry(edge.from_guid.clone())
            .or_default()
            .push(edge.to_guid.clone());
        self.edges.push(edge);
    }

    /// Get node by GUID
    pub fn get_node(&self, guid: &str) -> Option<&LineageNode> {
        self.nodes.iter().find(|n| n.id.as_str() == guid)
    }

    /// Get node count
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Get edge count
    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }
}

/// Options for lineage queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageQueryOptions {
    /// Maximum depth to traverse (default: 3)
    #[serde(default = "default_depth")]
    pub depth: usize,
    /// Filter by relationship types (None = all)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relationship_types: Option<Vec<String>>,
    /// Filter by entity types (None = all)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entity_types: Option<Vec<String>>,
    /// Include propagated classifications
    #[serde(default = "default_true")]
    pub include_propagated_classifications: bool,
}

pub fn default_depth() -> usize {
    3
}

pub fn default_true() -> bool {
    true
}

impl Default for LineageQueryOptions {
    fn default() -> Self {
        Self {
            depth: default_depth(),
            relationship_types: None,
            entity_types: None,
            include_propagated_classifications: true,
        }
    }
}

impl LineageQueryOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_depth(mut self, depth: usize) -> Self {
        self.depth = depth;
        self
    }

    pub fn with_relationship_types(mut self, types: Vec<String>) -> Self {
        self.relationship_types = Some(types);
        self
    }

    pub fn with_entity_types(mut self, types: Vec<String>) -> Self {
        self.entity_types = Some(types);
        self
    }

    pub fn with_propagated_classifications(mut self, include: bool) -> Self {
        self.include_propagated_classifications = include;
        self
    }

    /// Check if a relationship type matches the filter
    pub fn matches_relationship_type(&self, rel_type: &str) -> bool {
        match &self.relationship_types {
            Some(types) => types.iter().any(|t| t == rel_type),
            None => true,
        }
    }

    /// Check if an entity type matches the filter
    pub fn matches_entity_type(&self, entity_type: &str) -> bool {
        match &self.entity_types {
            Some(types) => types.iter().any(|t| t == entity_type),
            None => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_id() {
        let id = NodeId::new("test-guid");
        assert_eq!(id.as_str(), "test-guid");
        assert_eq!(id.to_string(), "test-guid");
    }

    #[test]
    fn test_lineage_node() {
        let node = LineageNode::new("guid-1", "Table")
            .with_display_name("users")
            .with_classifications(vec!["PII".to_string()]);

        assert_eq!(node.id.as_str(), "guid-1");
        assert_eq!(node.entity_type, "Table");
        assert_eq!(node.display_name, Some("users".to_string()));
        assert_eq!(node.classifications, vec!["PII"]);
    }

    #[test]
    fn test_lineage_node_all_classifications() {
        let mut node = LineageNode::new("guid-1", "Table")
            .with_classifications(vec!["PII".to_string(), "SENSITIVE".to_string()]);
        node.add_propagated_classification("FINANCIAL");

        let all = node.all_classifications();
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn test_lineage_edge_propagation() {
        let edge_forward = LineageEdge::new("rel-1", "process_inputs", PropagateTags::OneToTwo);
        assert!(edge_forward.propagates_forward());
        assert!(!edge_forward.propagates_backward());

        let edge_backward = LineageEdge::new("rel-2", "process_outputs", PropagateTags::TwoToOne);
        assert!(!edge_backward.propagates_forward());
        assert!(edge_backward.propagates_backward());

        let edge_both = LineageEdge::new("rel-3", "data_flow", PropagateTags::Both);
        assert!(edge_both.propagates_forward());
        assert!(edge_both.propagates_backward());

        let edge_none = LineageEdge::new("rel-4", "reference", PropagateTags::None);
        assert!(!edge_none.propagates_forward());
        assert!(!edge_none.propagates_backward());
    }

    #[test]
    fn test_traversal_direction() {
        assert_eq!(
            "INPUT".parse::<TraversalDirection>().unwrap(),
            TraversalDirection::Input
        );
        assert_eq!(
            "UPSTREAM".parse::<TraversalDirection>().unwrap(),
            TraversalDirection::Input
        );
        assert_eq!(
            "OUTPUT".parse::<TraversalDirection>().unwrap(),
            TraversalDirection::Output
        );
    }

    #[test]
    fn test_lineage_result() {
        let mut result = LineageResult::new("root-guid", TraversalDirection::Input, 3);

        let node = LineageNode::new("node-1", "Table");
        result.add_node(node);

        let edge = LineageEdgeInfo::new("node-1", "root-guid", "rel-1", "process_inputs");
        result.add_edge(edge);

        assert_eq!(result.node_count(), 1);
        assert_eq!(result.edge_count(), 1);
        assert!(result.adjacency.contains_key("node-1"));
    }

    #[test]
    fn test_query_options() {
        let options = LineageQueryOptions::new()
            .with_depth(5)
            .with_relationship_types(vec!["process_inputs".to_string()])
            .with_entity_types(vec!["Table".to_string()]);

        assert_eq!(options.depth, 5);
        assert!(options.matches_relationship_type("process_inputs"));
        assert!(!options.matches_relationship_type("other"));
        assert!(options.matches_entity_type("Table"));
        assert!(!options.matches_entity_type("Column"));
    }
}
