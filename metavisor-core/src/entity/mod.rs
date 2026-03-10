//! Entity instance module (Atlas API v2 compatible)
//!
//! This module provides entity instance types compatible with Apache Atlas API v2.
//!
//! # Structure
//!
//! - `entity` - AtlasEntity: Full entity with attributes, classifications, etc.
//! - `header` - AtlasEntityHeader: Minimal entity info for lists
//! - `reference` - AtlasObjectId: Entity reference by guid or typeName + uniqueAttributes
//! - `classification` - AtlasClassification: Classification attached to entities

mod classification;
mod common;
mod header;
mod instance;
mod reference;

pub use classification::{Classification, TimeBoundary};
pub use common::*;
pub use header::EntityHeader;
pub use instance::{EntitiesWithExtInfo, Entity, EntityExtInfo, EntityWithExtInfo};
pub use reference::ObjectId;
