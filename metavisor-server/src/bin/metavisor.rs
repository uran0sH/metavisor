//! Metavisor server entry point
use std::net::SocketAddr;
use std::sync::Arc;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use clap::Parser;
use metavisor_server::create_router;
use metavisor_storage::{
    DefaultMetavisorStore, GrafeoGraphStore, KvEntityStore, KvRelationshipStore, KvStore,
    KvTypeStore,
};

#[derive(Parser, Debug)]
#[command(name = "metavisor")]
#[command(about = "Metavisor data governance and metadata management server", long_about = None)]
struct Args {
    /// Port to listen on
    #[arg(short, long, default_value_t = 31000)]
    port: u16,

    /// Data directory for storage
    #[arg(short, long, default_value = "./data")]
    data_dir: String,

    /// Graph data directory (for Grafeo)
    #[arg(long, default_value = "./data/graph")]
    graph_data_dir: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "metavisor=debug,tower_http=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Log graph store type
    tracing::info!(
        "Using graph store: {}",
        metavisor_storage::graph_store_type()
    );

    // Initialize storage
    tracing::info!("Opening storage at {}", args.data_dir);

    let kv_store = KvStore::open(&args.data_dir)?;
    let type_store = Arc::new(KvTypeStore::new(kv_store.clone()));
    let entity_store = Arc::new(KvEntityStore::new(kv_store.clone(), type_store.clone()));
    let relationship_store = Arc::new(KvRelationshipStore::new(
        kv_store.clone(),
        type_store.clone(),
    ));

    // Create Grafeo graph store wrapped in Arc
    let graph_store: Arc<GrafeoGraphStore> =
        Arc::new(GrafeoGraphStore::open(&args.graph_data_dir)?);

    let store = Arc::new(DefaultMetavisorStore::new(
        type_store,
        entity_store,
        relationship_store,
        graph_store.clone(),
    ));

    // Initialize with recovery (WAL + consistency check)
    tracing::info!("Initializing storage with recovery...");
    match store.initialize_with_recovery().await {
        Ok(result) => {
            if result.had_changes() {
                tracing::info!(
                    "Recovery completed: {} items repaired. Propagation pending.",
                    result.total_changes()
                );
            } else {
                tracing::info!("Storage is consistent, no recovery needed");
            }
        }
        Err(e) => {
            tracing::error!("Failed to initialize storage: {}", e);
            // Continue anyway - the system may still be functional
        }
    }

    // Create router
    let addr = SocketAddr::from(([0, 0, 0, 0], args.port));
    let router = create_router(store);

    tracing::info!("Metavisor server listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, router).await?;

    Ok(())
}
