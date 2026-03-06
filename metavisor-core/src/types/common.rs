//! Common types and enums used across type definitions
//!
//! Corresponds to AtlasTypeCategory, Cardinality, IndexType in Apache Atlas.

use serde::{Deserialize, Serialize};

// ============================================================================
// Type Category
// ============================================================================

/// Type category (AtlasTypeCategory)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TypeCategory {
    #[default]
    Entity,
    Classification,
    Relationship,
    Struct,
    Enum,
    BusinessMetadata,
}

// ============================================================================
// Cardinality
// ============================================================================

/// Cardinality of an attribute
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Cardinality {
    #[default]
    Single,
    List,
    Set,
}

impl Cardinality {
    pub fn as_str(&self) -> &'static str {
        match self {
            Cardinality::Single => "SINGLE",
            Cardinality::List => "LIST",
            Cardinality::Set => "SET",
        }
    }
}

impl std::str::FromStr for Cardinality {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "SINGLE" => Ok(Cardinality::Single),
            "LIST" => Ok(Cardinality::List),
            "SET" => Ok(Cardinality::Set),
            _ => Err(format!("Unknown cardinality: {}", s)),
        }
    }
}

impl std::fmt::Display for Cardinality {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

// ============================================================================
// Index Type
// ============================================================================

/// Index type for an attribute (AtlasIndexType)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum IndexType {
    #[default]
    Default,
    String,
}

impl IndexType {
    pub fn as_str(&self) -> &'static str {
        match self {
            IndexType::Default => "DEFAULT",
            IndexType::String => "STRING",
        }
    }
}

impl std::fmt::Display for IndexType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cardinality_display() {
        assert_eq!(Cardinality::Single.to_string(), "SINGLE");
        assert_eq!(Cardinality::List.to_string(), "LIST");
        assert_eq!(Cardinality::Set.to_string(), "SET");
    }

    #[test]
    fn test_cardinality_from_str() {
        assert_eq!(
            "SINGLE".parse::<Cardinality>().unwrap(),
            Cardinality::Single
        );
        assert_eq!("list".parse::<Cardinality>().unwrap(), Cardinality::List);
    }
}
