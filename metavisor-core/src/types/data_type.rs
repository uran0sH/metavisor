//! Data types supported by the type system

use serde::{Deserialize, Serialize};

/// Data types supported by the type system
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "params")]
pub enum DataType {
    // Primitive types
    String,
    Int,
    Long,
    Float,
    Double,
    Boolean,
    Date,
    Timestamp,
    Bytes,

    // Complex types
    Array(Box<DataType>),
    Map {
        key: Box<DataType>,
        value: Box<DataType>,
    },

    // Reference to another type
    Reference(String),

    // Enum type with variants
    Enum(Vec<String>),
}

impl DataType {
    /// Check if this is a primitive type
    pub fn is_primitive(&self) -> bool {
        matches!(
            self,
            DataType::String
                | DataType::Int
                | DataType::Long
                | DataType::Float
                | DataType::Double
                | DataType::Boolean
                | DataType::Date
                | DataType::Timestamp
                | DataType::Bytes
        )
    }

    /// Get the type name for display
    pub fn type_name(&self) -> String {
        match self {
            DataType::String => "string".to_string(),
            DataType::Int => "int".to_string(),
            DataType::Long => "long".to_string(),
            DataType::Float => "float".to_string(),
            DataType::Double => "double".to_string(),
            DataType::Boolean => "boolean".to_string(),
            DataType::Date => "date".to_string(),
            DataType::Timestamp => "timestamp".to_string(),
            DataType::Bytes => "bytes".to_string(),
            DataType::Array(inner) => format!("array<{}>", inner.type_name()),
            DataType::Map { key, value } => {
                format!("map<{}, {}>", key.type_name(), value.type_name())
            }
            DataType::Reference(name) => format!("ref<{}>", name),
            DataType::Enum(variants) => format!("enum({})", variants.join(", ")),
        }
    }
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.type_name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_primitive_types() {
        assert!(DataType::String.is_primitive());
        assert!(DataType::Int.is_primitive());
        assert!(!DataType::Array(Box::new(DataType::String)).is_primitive());
    }

    #[test]
    fn test_type_name() {
        assert_eq!(DataType::String.type_name(), "string");
        assert_eq!(
            DataType::Array(Box::new(DataType::Int)).type_name(),
            "array<int>"
        );
        assert_eq!(
            DataType::Map {
                key: Box::new(DataType::String),
                value: Box::new(DataType::Int)
            }
            .type_name(),
            "map<string, int>"
        );
    }

    #[test]
    fn test_serialization() {
        let dt = DataType::Array(Box::new(DataType::String));
        let json = serde_json::to_string(&dt).unwrap();
        assert_eq!(json, r#"{"kind":"Array","params":{"kind":"String"}}"#);

        let parsed: DataType = serde_json::from_str(&json).unwrap();
        assert_eq!(dt, parsed);
    }
}
