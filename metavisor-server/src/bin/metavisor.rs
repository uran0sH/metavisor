//! Metavisor server entry point
use std::net::SocketAddr;
use std::sync::Arc;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use clap::Parser;
use metavisor_server::create_router;
use metavisor_storage::{
    DefaultMetavisorStore, GrafeoGraphStore, KvEntityStore, KvRelationshipStore, KvStore,
    KvTypeStore, WriteAheadLog,
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

    /// WAL data directory (for transaction logs)
    #[arg(long, default_value = "./data/wal")]
    wal_data_dir: String,

    /// Enable WAL (Write Ahead Log) for cross-storage transactions
    #[arg(long, default_value_t = true)]
    enable_wal: bool,
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
    let graph_store: Arc<GrafeoGraphStore> = Arc::new(GrafeoGraphStore::open(
        &args.graph_data_dir,
        entity_store.clone(),
        relationship_store.clone(),
    )?);

    // Create unified MetavisorStore (with WAL if enabled)
    let store = if args.enable_wal {
        tracing::info!("WAL enabled for cross-storage transactions");
        tracing::info!("Opening WAL storage at {}", args.wal_data_dir);
        let wal_store = KvStore::open(&args.wal_data_dir)?;
        let wal = Arc::new(WriteAheadLog::new(wal_store));
        Arc::new(DefaultMetavisorStore::with_wal(
            type_store,
            entity_store,
            relationship_store,
            graph_store.clone(),
            wal,
        ))
    } else {
        tracing::info!("WAL disabled, using legacy transaction mode");
        Arc::new(DefaultMetavisorStore::new(
            type_store,
            entity_store,
            relationship_store,
            graph_store.clone(),
        ))
    };

    // Initialize with recovery (WAL + consistency check)
    tracing::info!("Initializing storage with recovery...");
    match store.initialize_with_recovery().await {
        Ok(result) => {
            if result.had_changes() {
                tracing::info!(
                    "Recovery completed: {} WAL transactions, {} items repaired. Propagation pending.",
                    result.wal_recovery.len(),
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
