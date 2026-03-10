//! Entity instance (AtlasEntity)
//!
//! Full entity with attributes, classifications, and metadata

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::{Classification, EntityHeader, EntityStatus, ObjectId};

/// Full entity instance (AtlasEntity)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entity {
    /// Entity type name (inherited from AtlasStruct)
    #[serde(rename = "typeName")]
    pub type_name: String,

    /// Entity attributes (inherited from AtlasStruct)
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub attributes: HashMap<String, serde_json::Value>,

    /// Unique identifier
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guid: Option<String>,

    /// Home ID (for replicated entities)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "homeId")]
    pub home_id: Option<String>,

    /// Whether this is a proxy entity
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "isProxy")]
    pub is_proxy: Option<bool>,

    /// Whether this entity is incomplete
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "isIncomplete")]
    pub is_incomplete: Option<bool>,

    /// Provenance type
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "provenanceType")]
    pub provenance_type: Option<i32>,

    /// Entity status
    #[serde(default)]
    pub status: EntityStatus,

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

    /// Relationship attributes
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub relationship_attributes: HashMap<String, serde_json::Value>,

    /// Classifications applied to this entity
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub classifications: Vec<Classification>,

    /// Meanings (glossary terms)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub meanings: Vec<ObjectId>,

    /// Custom attributes
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub custom_attributes: HashMap<String, String>,

    /// Business attributes (by business metadata name)
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub business_attributes: HashMap<String, HashMap<String, serde_json::Value>>,

    /// Labels/tags on this entity
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub labels: Vec<String>,
}

impl Entity {
    /// Create a new entity with the given type name
    pub fn new(type_name: impl Into<String>) -> Self {
        Self {
            type_name: type_name.into(),
            attributes: HashMap::new(),
            guid: None,
            home_id: None,
            is_proxy: None,
            is_incomplete: None,
            provenance_type: None,
            status: EntityStatus::default(),
            created_by: None,
            updated_by: None,
            create_time: None,
            update_time: None,
            version: None,
            relationship_attributes: HashMap::new(),
            classifications: Vec::new(),
            meanings: Vec::new(),
            custom_attributes: HashMap::new(),
            business_attributes: HashMap::new(),
            labels: Vec::new(),
        }
    }

    /// Set the GUID
    pub fn with_guid(mut self, guid: impl Into<String>) -> Self {
        self.guid = Some(guid.into());
        self
    }

    /// Set the status
    pub fn with_status(mut self, status: EntityStatus) -> Self {
        self.status = status;
        self
    }

    /// Add an attribute
    pub fn with_attribute(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.attributes.insert(key.into(), value);
        self
    }

    /// Add multiple attributes
    pub fn with_attributes(mut self, attrs: HashMap<String, serde_json::Value>) -> Self {
        self.attributes.extend(attrs);
        self
    }

    /// Add a classification
    pub fn with_classification(mut self, classification: Classification) -> Self {
        self.classifications.push(classification);
        self
    }

    /// Add a label
    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.labels.push(label.into());
        self
    }

    /// Get the entity's GUID, or return an error if not set
    pub fn require_guid(&self) -> Result<&str, crate::CoreError> {
        self.guid
            .as_deref()
            .ok_or_else(|| crate::CoreError::Validation("Entity GUID is required".to_string()))
    }

    /// Convert to ObjectId reference
    pub fn to_object_id(&self) -> Option<ObjectId> {
        self.guid
            .as_ref()
            .map(|g| ObjectId::by_guid(&self.type_name, g))
    }

    /// Convert to EntityHeader
    pub fn to_header(&self) -> EntityHeader {
        let mut header =
            EntityHeader::new(&self.type_name).with_guid(self.guid.clone().unwrap_or_default());

        header.status = Some(self.status);
        header.labels = self.labels.clone();
        header.attributes = self.attributes.clone();
        header.is_incomplete = self.is_incomplete;

        // Copy classification names
        header.classification_names = self
            .classifications
            .iter()
            .map(|c| c.type_name.clone())
            .collect();

        // Copy classifications
        header.classifications = self.classifications.clone();

        // Copy meanings
        header.meanings = self.meanings.clone();

        header
    }
}

// ============================================================================
// Entity ExtInfo types
// ============================================================================

/// Entity extended info with referred entities (AtlasEntityExtInfo)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityExtInfo {
    /// Referred entities indexed by GUID
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    #[serde(rename = "referredEntities")]
    pub referred_entities: HashMap<String, Entity>,
}

impl EntityExtInfo {
    /// Create new empty ext info
    pub fn new() -> Self {
        Self {
            referred_entities: HashMap::new(),
        }
    }

    /// Add a referred entity
    pub fn add_referred(&mut self, entity: Entity) {
        if let Some(guid) = &entity.guid {
            self.referred_entities.insert(guid.clone(), entity);
        }
    }

    /// Get a referred entity by GUID
    pub fn get_referred(&self, guid: &str) -> Option<&Entity> {
        self.referred_entities.get(guid)
    }

    /// Remove a referred entity by GUID
    pub fn remove_referred(&mut self, guid: &str) -> Option<Entity> {
        self.referred_entities.remove(guid)
    }

    /// Check if entity exists
    pub fn has_entity(&self, guid: &str) -> bool {
        self.referred_entities.contains_key(guid)
    }
}

impl Default for EntityExtInfo {
    fn default() -> Self {
        Self::new()
    }
}

/// Single entity with extended info (AtlasEntityWithExtInfo)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityWithExtInfo {
    /// The main entity
    pub entity: Entity,

    /// Referred entities (flattened into this struct via serde)
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    #[serde(rename = "referredEntities")]
    pub referred_entities: HashMap<String, Entity>,
}

impl EntityWithExtInfo {
    /// Create from an entity
    pub fn new(entity: Entity) -> Self {
        Self {
            entity,
            referred_entities: HashMap::new(),
        }
    }

    /// Add a referred entity
    pub fn add_referred(&mut self, entity: Entity) {
        if let Some(guid) = &entity.guid {
            self.referred_entities.insert(guid.clone(), entity);
        }
    }

    /// Get entity by GUID (checks both referred and main entity)
    pub fn get_entity(&self, guid: &str) -> Option<&Entity> {
        if self.entity.guid.as_deref() == Some(guid) {
            Some(&self.entity)
        } else {
            self.referred_entities.get(guid)
        }
    }
}

/// List of entities with extended info (AtlasEntitiesWithExtInfo)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntitiesWithExtInfo {
    /// The list of entities
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub entities: Vec<Entity>,

    /// Referred entities
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    #[serde(rename = "referredEntities")]
    pub referred_entities: HashMap<String, Entity>,
}

impl EntitiesWithExtInfo {
    /// Create empty
    pub fn new() -> Self {
        Self {
            entities: Vec::new(),
            referred_entities: HashMap::new(),
        }
    }

    /// Create from a single entity
    pub fn from_single(entity: Entity) -> Self {
        Self {
            entities: vec![entity],
            referred_entities: HashMap::new(),
        }
    }

    /// Create from a list of entities
    pub fn from_entities(entities: Vec<Entity>) -> Self {
        Self {
            entities,
            referred_entities: HashMap::new(),
        }
    }

    /// Add an entity to the list
    pub fn add_entity(&mut self, entity: Entity) {
        self.entities.push(entity);
    }

    /// Add a referred entity
    pub fn add_referred(&mut self, entity: Entity) {
        if let Some(guid) = &entity.guid {
            self.referred_entities.insert(guid.clone(), entity);
        }
    }

    /// Get entity by GUID (checks both list and referred)
    pub fn get_entity(&self, guid: &str) -> Option<&Entity> {
        self.entities
            .iter()
            .find(|e| e.guid.as_deref() == Some(guid))
            .or_else(|| self.referred_entities.get(guid))
    }
}

impl Default for EntitiesWithExtInfo {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_entity_creation() {
        let entity = Entity::new("Table")
            .with_guid("guid-123")
            .with_attribute("name", json!("users_table"))
            .with_attribute("rowCount", json!(1000))
            .with_classification(Classification::new("PII"))
            .with_label("sensitive");

        assert_eq!(entity.type_name, "Table");
        assert_eq!(entity.guid, Some("guid-123".to_string()));
        assert_eq!(entity.attributes.get("name"), Some(&json!("users_table")));
        assert_eq!(entity.classifications.len(), 1);
        assert_eq!(entity.labels, vec!["sensitive"]);
    }

    #[test]
    fn test_entity_serialization() {
        let entity = Entity::new("Table")
            .with_guid("guid-123")
            .with_attribute("name", json!("users"));

        let json_str = serde_json::to_string(&entity).unwrap();
        assert!(json_str.contains("\"typeName\":\"Table\""));
        assert!(json_str.contains("\"guid\":\"guid-123\""));
    }

    #[test]
    fn test_entity_to_header() {
        let entity = Entity::new("Table")
            .with_guid("guid-123")
            .with_classification(Classification::new("PII"));

        let header = entity.to_header();
        assert_eq!(header.type_name, "Table");
        assert_eq!(header.guid, Some("guid-123".to_string()));
        assert_eq!(header.classification_names, vec!["PII"]);
    }

    #[test]
    fn test_entity_with_ext_info() {
        let entity = Entity::new("Table").with_guid("guid-123");
        let referred = Entity::new("Column").with_guid("guid-456");

        let mut ext = EntityWithExtInfo::new(entity);
        ext.add_referred(referred);

        assert!(ext.get_entity("guid-123").is_some());
        assert!(ext.get_entity("guid-456").is_some());
    }

    #[test]
    fn test_entities_with_ext_info() {
        let entity1 = Entity::new("Table").with_guid("guid-123");
        let entity2 = Entity::new("Column").with_guid("guid-456");

        let mut ext = EntitiesWithExtInfo::from_entities(vec![entity1]);
        ext.add_entity(entity2);

        assert_eq!(ext.entities.len(), 2);
    }
}
