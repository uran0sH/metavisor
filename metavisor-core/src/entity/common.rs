//! Common types for entity instances

use serde::{Deserialize, Serialize};

/// Entity status (AtlasEntityStatus)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EntityStatus {
    #[default]
    Active,
    Deleted,
    Purged,
}

impl EntityStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            EntityStatus::Active => "ACTIVE",
            EntityStatus::Deleted => "DELETED",
            EntityStatus::Purged => "PURGED",
        }
    }
}

impl std::fmt::Display for EntityStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_status_serialization() {
        let status = EntityStatus::Active;
        let json = serde_json::to_string(&status).unwrap();
        assert_eq!(json, "\"ACTIVE\"");

        let status: EntityStatus = serde_json::from_str("\"DELETED\"").unwrap();
        assert_eq!(status, EntityStatus::Deleted);
    }
}
