//! Graph types for lineage and traversal
//!
//! Core types for representing the entity relationship graph

use serde::{Deserialize, Serialize};

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_id() {
        let id = NodeId::new("test-guid");
        assert_eq!(id.as_str(), "test-guid");
        assert_eq!(id.to_string(), "test-guid");
    }
}
