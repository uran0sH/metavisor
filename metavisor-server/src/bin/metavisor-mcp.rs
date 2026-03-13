//! Metavisor MCP Server (stdio mode)
//!
//! A standalone MCP server that communicates over stdin/stdout.
//! This is designed to be used with Claude Code and other MCP clients
//! that support stdio transport.
//!
//! Usage:
//!   metavisor-mcp [OPTIONS]
//!
//! Options:
//!   -d, --data-dir <DIR>  Data directory for storage [default: ./data]

use std::sync::Arc;

use clap::Parser;
use metavisor_core::{EntityStore, RelationshipStore, TypeStore};
use metavisor_server::mcp::MetavisorMcpServer;
use metavisor_storage::{KvEntityStore, KvRelationshipStore, KvStore, KvTypeStore};
use rmcp::ServiceExt;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser, Debug)]
#[command(name = "metavisor-mcp")]
#[command(about = "Metavisor MCP Server (stdio mode) for Claude Code integration", long_about = None)]
struct Args {
    /// Data directory for storage
    #[arg(short, long, default_value = "./data")]
    data_dir: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Initialize tracing to file (not stdout, to avoid interfering with stdio protocol)
    // Logs go to stderr with a filter that can be controlled via RUST_LOG env var
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "metavisor=warn".into()),
        )
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .init();

    tracing::info!("Starting Metavisor MCP Server (stdio mode)");
    tracing::info!("Opening storage at {}", args.data_dir);

    // Initialize storage
    let kv_store = KvStore::open(&args.data_dir)?;
    let type_store: Arc<dyn TypeStore> = Arc::new(KvTypeStore::new(kv_store.clone()));
    let entity_store: Arc<dyn EntityStore> =
        Arc::new(KvEntityStore::new(kv_store.clone(), type_store.clone()));
    let relationship_store: Arc<dyn RelationshipStore> =
        Arc::new(KvRelationshipStore::new(kv_store, type_store.clone()));

    // Create the MCP server
    let server = MetavisorMcpServer::new(metavisor_server::mcp::McpState {
        type_store,
        entity_store,
        relationship_store,
    });

    tracing::info!("MCP server initialized, starting stdio transport");

    // Use stdio transport (stdin for input, stdout for output)
    let transport = (tokio::io::stdin(), tokio::io::stdout());

    // Serve the MCP server over stdio
    let server = server.serve(transport).await?;

    // Wait for the server to complete
    server.waiting().await?;

    Ok(())
}
