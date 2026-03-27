//! API handlers

pub mod admin;
pub mod classification;
pub mod entity;

pub mod relationship;
pub mod search;
pub mod r#type;

use std::sync::Arc;

use metavisor_core::MetavisorStore;

pub use admin::*;
pub use classification::*;
pub use entity::*;

pub use r#type::*;
pub use relationship::*;
pub use search::*;

/// Unified application state containing MetavisorStore
#[derive(Clone)]
pub struct MetavisorAppState {
    pub store: Arc<dyn MetavisorStore>,
}
