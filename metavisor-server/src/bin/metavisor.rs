//! Metavisor server entry point
use std::net::SocketAddr;
use std::sync::Arc;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use clap::Parser;
use metavisor_server::create_router;
use metavisor_storage::{
    DefaultMetavisorStore, InMemoryGraphStore, KvEntityStore, KvRelationshipStore, KvStore,
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

    // Initialize storage
    tracing::info!("Opening storage at {}", args.data_dir);

    let kv_store = KvStore::open(&args.data_dir)?;
    let type_store = Arc::new(KvTypeStore::new(kv_store.clone()));
    let entity_store = Arc::new(KvEntityStore::new(kv_store.clone(), type_store.clone()));
    let relationship_store = Arc::new(KvRelationshipStore::new(
        kv_store.clone(),
        type_store.clone(),
    ));
    let graph_store = Arc::new(InMemoryGraphStore::new(
        entity_store.clone(),
        relationship_store.clone(),
    ));

    // Create unified MetavisorStore
    let store = Arc::new(DefaultMetavisorStore::new(
        type_store,
        entity_store,
        relationship_store,
        graph_store,
    ));

    // Initialize graph from persisted data
    tracing::info!("Initializing graph from persisted data...");
    store.initialize().await?;
    tracing::info!("Graph initialized successfully");

    // Create router
    let addr = SocketAddr::from(([0, 0, 0, 0], args.port));
    let router = create_router(store);

    tracing::info!("Metavisor server listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, router).await?;

    Ok(())
}
