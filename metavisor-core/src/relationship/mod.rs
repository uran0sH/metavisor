//! Relationship instance module (Atlas API v2 compatible)
//!
//! This module provides relationship instance types compatible with Apache Atlas API v2.
//!
//! # Structure
//!
//! - `Relationship` - AtlasRelationship: Full relationship with endpoints and attributes
//! - `RelationshipHeader` - AtlasRelationshipHeader: Minimal relationship info for lists
//! - `RelationshipStatus` - Status enum (ACTIVE, DELETED)

mod header;
mod instance;

pub use header::{RelationshipHeader, RelationshipHeaders};
pub use instance::{Relationship, RelationshipStatus, RelationshipWithExtInfo};
