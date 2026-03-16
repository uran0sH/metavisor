//! Metavisor Core - Core types and business logic
//!
//! This crate provides the core domain models for the Metavisor platform.

pub mod entity;
pub mod error;
pub mod graph;
pub mod relationship;
pub mod store;
pub mod types;

pub use entity::*;
pub use error::{CoreError, Result};
pub use graph::*;
pub use relationship::*;
pub use store::{GraphStats, MetavisorStore, *};
pub use types::*;
