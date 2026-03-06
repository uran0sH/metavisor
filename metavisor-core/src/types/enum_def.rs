//! Enum type definition (AtlasEnumDef)
//!
//! Contains:
//! - EnumDef - enum type definition
//! - EnumElementDef - enum element definition

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ============================================================================
// Enum Element Definition
// ============================================================================

/// Enum element definition (AtlasEnumElementDef)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnumElementDef {
    /// Element value
    pub value: String,

    /// Element description
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Ordinal position
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ordinal: Option<i32>,
}

impl EnumElementDef {
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
            description: None,
            ordinal: None,
        }
    }

    pub fn description(mut self, desc: impl Into<String>) -> Self {
        self.description = Some(desc.into());
        self
    }

    pub fn ordinal(mut self, ordinal: i32) -> Self {
        self.ordinal = Some(ordinal);
        self
    }
}

// ============================================================================
// Enum Definition
// ============================================================================

/// Enum type definition (AtlasEnumDef)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnumDef {
    /// Unique identifier
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guid: Option<String>,

    /// Type name
    pub name: String,

    /// Description
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Type version
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "typeVersion")]
    pub type_version: Option<String>,

    /// Service type
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "serviceType")]
    pub service_type: Option<String>,

    /// Enum elements
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[serde(rename = "elementDefs")]
    pub element_defs: Vec<EnumElementDef>,

    /// Default value
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "defaultValue")]
    pub default_value: Option<String>,

    /// Created by
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "createdBy")]
    pub created_by: Option<String>,

    /// Updated by
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "updatedBy")]
    pub updated_by: Option<String>,

    /// Create time (epoch milliseconds)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "createTime")]
    pub create_time: Option<i64>,

    /// Update time (epoch milliseconds)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "updateTime")]
    pub update_time: Option<i64>,

    /// Version
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<i64>,

    /// Options
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub options: HashMap<String, String>,
}

impl EnumDef {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            guid: None,
            description: None,
            type_version: None,
            service_type: None,
            element_defs: Vec::new(),
            default_value: None,
            created_by: None,
            updated_by: None,
            create_time: None,
            update_time: None,
            version: None,
            options: HashMap::new(),
        }
    }

    pub fn description(mut self, desc: impl Into<String>) -> Self {
        self.description = Some(desc.into());
        self
    }

    pub fn element(mut self, element: EnumElementDef) -> Self {
        self.element_defs.push(element);
        self
    }

    pub fn default_value(mut self, value: impl Into<String>) -> Self {
        self.default_value = Some(value.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_enum_element() {
        let element = EnumElementDef::new("ACTIVE")
            .description("Active status")
            .ordinal(1);

        assert_eq!(element.value, "ACTIVE");
        assert_eq!(element.description, Some("Active status".to_string()));
        assert_eq!(element.ordinal, Some(1));
    }

    #[test]
    fn test_enum_def() {
        let enum_def = EnumDef::new("Status")
            .description("Entity status")
            .element(EnumElementDef::new("ACTIVE").ordinal(1))
            .element(EnumElementDef::new("INACTIVE").ordinal(2))
            .default_value("ACTIVE");

        assert_eq!(enum_def.name, "Status");
        assert_eq!(enum_def.element_defs.len(), 2);
        assert_eq!(enum_def.default_value, Some("ACTIVE".to_string()));
    }
}
