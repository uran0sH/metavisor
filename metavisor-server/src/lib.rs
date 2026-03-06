//! Metavisor Server - HTTP API
//!
//! This crate provides the REST API for the Metavisor platform.

pub mod error;
mod handlers;
pub mod routes;

pub use error::{ApiError, Result};
pub use routes::create_router;
