//! API handlers

pub mod entity;
pub mod lineage;
pub mod relationship;
pub mod r#type;

use std::sync::Arc;

use metavisor_core::MetavisorStore;

pub use entity::*;
pub use lineage::*;
pub use r#type::*;
pub use relationship::*;

/// Unified application state containing MetavisorStore
#[derive(Clone)]
pub struct MetavisorAppState {
    pub store: Arc<dyn MetavisorStore>,
}
