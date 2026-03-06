//! Metavisor Core - Core types and business logic
//!
//! This crate provides the core domain models for the Metavisor platform.

pub mod error;
pub mod store;
pub mod types;

pub use error::{CoreError, Result};
pub use store::*;
pub use types::*;
