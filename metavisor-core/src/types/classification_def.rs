//! Classification type definition (AtlasClassificationDef)

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::struct_def::AttributeDef;

/// Classification type definition (AtlasClassificationDef)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClassificationDef {
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

    /// Parent types (inheritance)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[serde(rename = "superTypes")]
    pub super_types: Vec<String>,

    /// Entity types this classification can apply to
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[serde(rename = "entityTypes")]
    pub entity_types: Vec<String>,

    /// Sub types (read-only, derived)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[serde(rename = "subTypes")]
    pub sub_types: Vec<String>,

    /// Attribute definitions
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[serde(rename = "attributeDefs")]
    pub attribute_defs: Vec<AttributeDef>,

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

impl ClassificationDef {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            guid: None,
            description: None,
            type_version: None,
            service_type: None,
            super_types: Vec::new(),
            entity_types: Vec::new(),
            sub_types: Vec::new(),
            attribute_defs: Vec::new(),
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

    pub fn super_type(mut self, name: impl Into<String>) -> Self {
        self.super_types.push(name.into());
        self
    }

    pub fn entity_type(mut self, name: impl Into<String>) -> Self {
        self.entity_types.push(name.into());
        self
    }

    pub fn attribute(mut self, attr: AttributeDef) -> Self {
        self.attribute_defs.push(attr);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classification_def() {
        let classification = ClassificationDef::new("PII")
            .description("Personally Identifiable Information")
            .entity_type("Asset")
            .super_type("Classification")
            .attribute(AttributeDef::new("sensitivity", "string"));

        assert_eq!(classification.name, "PII");
        assert_eq!(
            classification.description,
            Some("Personally Identifiable Information".to_string())
        );
        assert_eq!(classification.entity_types, vec!["Asset"]);
        assert_eq!(classification.super_types, vec!["Classification"]);
        assert_eq!(classification.attribute_defs.len(), 1);
    }
}
