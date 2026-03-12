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

## MCP (Model Context Protocol) Server

Metavisor includes an MCP server for AI assistant integration, built with the [rmcp](https://crates.io/crates/rmcp) SDK.

### Transport Modes

- **HTTP**: JSON-RPC over HTTP at `/mcp` endpoint (runs as part of the main server)
- **Stdio**: Standalone binary for direct process communication (used by Claude Code and other MCP clients)

### Running the Stdio MCP Server (for Claude Code)

The stdio MCP server is a standalone binary that communicates over stdin/stdout, making it compatible with Claude Code and other MCP clients.

```bash
# Run the stdio MCP server
cargo run --bin metavisor-mcp

# Or with a custom data directory
cargo run --bin metavisor-mcp -- --data-dir /path/to/data

# Build release binary
cargo build --release --bin metavisor-mcp
```

### Configuring Claude Code

Add the following to your Claude Code configuration to use Metavisor as an MCP server:

**macOS/Linux** (`~/.config/claude/claude_desktop_config.json`):
```json
{
  "mcpServers": {
    "metavisor": {
      "command": "/path/to/metavisor/target/release/metavisor-mcp",
      "args": ["--data-dir", "/path/to/metavisor/data"]
    }
  }
}
```

### Available Tools

| Tool | Description |
|------|-------------|
| `search_entities` | Search for data entities by type name |
| `get_entity` | Get entity details by GUID |
| `list_types` | List all type definitions |
| `get_type` | Get type definition details |
| `create_entity` | Create a new entity |
| `update_entity` | Update an existing entity |
| `delete_entity` | Delete an entity by GUID |
| `create_entity_type` | Create a new entity type definition |
| `update_entity_type` | Update an existing entity type |
| `delete_type` | Delete a type definition |
| `create_relationship` | Create a relationship between two entities |
| `get_relationship` | Get relationship details by GUID |
| `update_relationship` | Update an existing relationship |
| `delete_relationship` | Delete a relationship by GUID |
| `list_relationships_by_entity` | List all relationships for an entity |
| `list_relationships_by_type` | List all relationships of a specific type |

### Available Resources

| Resource URI | Description |
|-------------|-------------|
| `metavisor://entity/{guid}` | Access entity data as JSON |

### Endpoint

- **POST /mcp** - JSON-RPC over HTTP

### Testing MCP (HTTP Mode)

```bash
# Initialize
curl -X POST http://localhost:31000/mcp \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize"}'

# List available tools
curl -X POST http://localhost:31000/mcp \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/list"}'

# Call a tool
curl -X POST http://localhost:31000/mcp \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"list_types","arguments":{}}}'
```

## Key Design Decisions

1. **Workspace Structure**: Each crate manages its own dependencies
2. **Error Handling**: Hybrid approach - each crate has its own error type, `metavisor-server` aggregates for HTTP responses
3. **Type System**: Flexible metadata modeling with inheritance support
4. **Column-level Lineage**: Track data flow at column granularity using petgraph
5. **Classification Propagation**: Tags like PII automatically propagate through lineage
6. **MQ Abstraction**: trait-based design allows swapping Kafka/NATS/in-memory
7. **MCP Integration**: MCP server built with rmcp SDK, supports HTTP and stdio transports for AI assistant integration

## Relationship API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/metavisor/v1/relationship` | POST | Create a relationship |
| `/api/metavisor/v1/relationship` | PUT | Update a relationship |
| `/api/metavisor/v1/relationship/guid/{guid}` | GET | Get relationship by GUID |
| `/api/metavisor/v1/relationship/guid/{guid}` | DELETE | Delete relationship |
| `/api/metavisor/v1/relationship/entity/{entity_guid}` | GET | List relationships by entity |
| `/api/metavisor/v1/relationship/type/{type_name}` | GET | List relationships by type |

### Relationship Storage Keys

| Key Format | Description |
|------------|-------------|
| `relationship:{guid}` | Main relationship data |
| `rel_endpoint:{entity_guid}:{rel_guid}` | Endpoint index |
| `rel_type:{type_name}:{rel_guid}` | Type index |

### Transaction Support

Relationship operations use atomic transactions to ensure consistency:
- Create: writes main data + endpoint indices + type index atomically
- Delete: removes all indices atomically
- Uses `KvStore::batch_write()` for atomicity
