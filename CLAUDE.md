# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Metavisor is a data governance and metadata management platform (similar to Apache Atlas).

**Architecture Design**: See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for full design documentation.

## Tech Stack

| Component | Technology |
|-----------|------------|
| Web Framework | axum |
| Graph Structure | petgraph |
| Full-text Search | tantivy |
| KV Storage | surrealkv |
| Message Queue | trait abstraction (Kafka/NATS later) |
| Async Runtime | tokio |

## Project Structure (Workspace)

```
metavisor-core/      # Core: type system, graph engine, classification
metavisor-storage/   # Storage: KV store, graph persistence, search index
metavisor-server/    # HTTP server: routes, handlers, DTOs
│   └── src/bin/metavisor.rs  # Binary entry point
metavisor-mq/        # Message queue abstraction
```

## Build Commands

```bash
# Build all crates
cargo build

# Build release
cargo build --release

# Run the server
cargo run --bin metavisor

# Run tests (all crates)
cargo test

# Run tests for specific crate
cargo test -p metavisor-core

# Check compilation
cargo check

# Format
cargo fmt

# Lint
cargo clippy
```

## Run Server

```bash
cargo run --bin metavisor

# specify port
cargo run --bin metavisor -- --port 18999 

# specify data METAVISOR_DATA_DIR
cargo run --bin metavisor -- --port 8080 --data-dir /tmp/metavisor
```

## Integration Tests

Integration tests require a running server:

```bash
# Terminal 1: Start the server (default port 18999)
cargo run --bin metavisor

# Terminal 2: Run integration tests
cargo test --test type_api_integration
```

## Key Design Decisions

1. **Workspace Structure**: Each crate manages its own dependencies
2. **Error Handling**: Hybrid approach - each crate has its own error type, `metavisor-server` aggregates for HTTP responses
3. **Type System**: Flexible metadata modeling with inheritance support
4. **Column-level Lineage**: Track data flow at column granularity using petgraph
5. **Classification Propagation**: Tags like PII automatically propagate through lineage
6. **MQ Abstraction**: trait-based design allows swapping Kafka/NATS/in-memory
