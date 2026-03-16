//! Graph module for lineage tracking and traversal
//!
//! This module provides graph-based capabilities for:
//! - Lineage tracking (upstream/downstream data flow)
//! - Graph traversal (BFS/DFS)
//! - Classification propagation through relationships

mod traits;
mod types;

pub use traits::GraphStore;
pub use types::*;
