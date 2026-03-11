# Metavisor

A data governance and metadata management platform (similar to Apache Atlas), built with Rust.

## Overview

Metavisor provides enterprise-grade metadata management capabilities:

| Feature | Description |
|---------|-------------|
| **Type System** | Flexible metadata modeling with inheritance support |
| **Data Lineage** | Column-level lineage tracking from source to target |
| **Classification** | Dynamic tags (PII, sensitive, etc.) with propagation |
| **Search** | Full-text search for quick data asset discovery |

## Tech Stack

| Component | Technology |
|-----------|------------|
| Web Framework | [axum](https://github.com/tokio-rs/axum) |
| Graph Structure | [petgraph](https://github.com/petgraph/petgraph) |
| Full-text Search | [tantivy](https://github.com/quickwit-oss/tantivy) |
| KV Storage | [surrealkv](https://github.com/surrealdb/surrealkv) |
| Async Runtime | [tokio](https://tokio.rs/) |

## Project Structure

```
metavisor-core/      # Core: type system, graph engine, classification
metavisor-storage/   # Storage: KV store, graph persistence, search index
metavisor-server/    # HTTP server: routes, handlers, DTOs
│   └── src/bin/metavisor.rs  # Binary entry point
metavisor-mq/        # Message queue abstraction (trait-based)
```

## Getting Started

### Prerequisites

- Rust 1.70+ (edition 2021)
- Cargo

### Build

```bash
# Build all crates
cargo build

# Build release
cargo build --release
```

### Run Server

```bash
# Default configuration (port=31000, data_dir=./data)
cargo run --bin metavisor

# Specify port and data directory
cargo run --bin metavisor -- --port 8080 --data-dir /tmp/metavisor

# Show help
cargo run --bin metavisor -- --help
```

### Run Tests

```bash
# Run all tests
cargo test

# Run tests for specific crate
cargo test -p metavisor-core

# Integration tests (requires running server)
# Terminal 1: Start server
cargo run --bin metavisor

# Terminal 2: Run integration tests
cargo test --test type_api_integration
cargo test --test entity_api_integration
```

## API Usage

### Health Check

```bash
curl http://localhost:31000/health
# OK
```

### Create Type Definition

```bash
curl -X POST http://localhost:31000/api/metavisor/v1/types/typedefs \
  -H "Content-Type: application/json" \
  -d '{
    "entityDefs": [{
      "name": "DataSet",
      "superTypes": [],
      "attributeDefs": [
        {"name": "name", "typeName": "string", "isOptional": false},
        {"name": "description", "typeName": "string", "isOptional": true}
      ]
    }]
  }'
```

### Get Type Definition

```bash
curl http://localhost:31000/api/metavisor/v1/types/typedef/name/DataSet
```

### Create Entity

```bash
curl -X POST http://localhost:31000/api/metavisor/v1/entity \
  -H "Content-Type: application/json" \
  -d '{
    "typeName": "DataSet",
    "attributes": {
      "name": "users_table",
      "description": "User information table"
    },
    "labels": ["production"]
  }'
```

### Get Entity by GUID

```bash
curl http://localhost:31000/api/metavisor/v1/entity/guid/{guid}
```

### Update Entity

```bash
curl -X PUT http://localhost:31000/api/metavisor/v1/entity \
  -H "Content-Type: application/json" \
  -d '{
    "guid": "{guid}",
    "typeName": "DataSet",
    "attributes": {
      "name": "users_table",
      "description": "Updated description"
    }
  }'
```

### Delete Entity

```bash
curl -X DELETE http://localhost:31000/api/metavisor/v1/entity/guid/{guid}
```

### Bulk Create Entities

```bash
curl -X POST http://localhost:31000/api/metavisor/v1/entity/bulk \
  -H "Content-Type: application/json" \
  -d '[
    {"typeName": "DataSet", "attributes": {"name": "table1"}},
    {"typeName": "DataSet", "attributes": {"name": "table2"}}
  ]'
```

### Create Relationship Type

First, create a relationship type definition:

```bash
curl -X POST http://localhost:31000/api/metavisor/v1/types/typedefs \
  -H "Content-Type: application/json" \
  -d '{
    "relationshipDefs": [{
      "name": "table_columns",
      "relationshipCategory": "COMPOSITION",
      "propagateTags": "ONE_TO_TWO",
      "endDef1": {"type": "DataSet", "name": "columns"},
      "endDef2": {"type": "Column", "name": "table"}
    }]
  }'
```

### Create Relationship

```bash
curl -X POST http://localhost:31000/api/metavisor/v1/relationship \
  -H "Content-Type: application/json" \
  -d '{
    "typeName": "table_columns",
    "end1": {"typeName": "DataSet", "guid": "table-guid-1"},
    "end2": {"typeName": "Column", "guid": "column-guid-1"},
    "label": "contains"
  }'
```

### Get Relationship by GUID

```bash
curl http://localhost:31000/api/metavisor/v1/relationship/guid/{guid}
```

### Delete Relationship

```bash
curl -X DELETE http://localhost:31000/api/metavisor/v1/relationship/guid/{guid}
```

## MCP (Model Context Protocol) Integration

Metavisor includes an MCP server for AI assistant integration (Claude, ChatGPT, etc.), built with the [rmcp](https://crates.io/crates/rmcp) SDK.

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

### Available Resources

| Resource URI | Description |
|-------------|-------------|
| `metavisor://entity/{guid}` | Access entity data as JSON |

### MCP Endpoint

- **HTTP**: `POST http://localhost:31000/mcp` - JSON-RPC over HTTP
- **Stdio**: Run `metavisor-mcp` binary for stdio transport (for MCP clients)

### Testing MCP (HTTP)

```bash
# Initialize MCP session
curl -X POST http://localhost:31000/mcp \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize"}'

# List available tools
curl -X POST http://localhost:31000/mcp \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/list"}'

# Call a tool - search for entities
curl -X POST http://localhost:31000/mcp \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc":"2.0",
    "id":3,
    "method":"tools/call",
    "params":{
      "name":"search_entities",
      "arguments":{"type_name":"DataSet"}
    }
  }'

# Call a tool - list all types
curl -X POST http://localhost:31000/mcp \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc":"2.0",
    "id":4,
    "method":"tools/call",
    "params":{"name":"list_types","arguments":{}}
  }'
```

### Using with AI Assistants

**Claude Desktop / Cursor / VS Code MCP Client**:

Configure your MCP client settings:

```json
{
  "mcpServers": {
    "metavisor": {
      "url": "http://localhost:31000/mcp"
    }
  }
}
```

For stdio mode (if supported), use the `metavisor-mcp` binary.

## Development

### Code Quality

```bash
# Format code
cargo fmt

# Lint
cargo clippy

# Check compilation
cargo check
```

### Architecture

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for detailed design documentation.

## Roadmap

- [x] Project structure setup
- [x] Type system API (Atlas-compatible)
- [x] Entity CRUD operations
- [x] Type validation logic
- [x] MCP server for AI assistant integration
- [x] Relationship CRUD operations
- [ ] Data lineage tracking (petgraph integration)
- [ ] Classification with propagation
- [ ] Full-text search integration
- [ ] Message queue abstraction (Kafka/NATS)
- [ ] Web UI for lineage visualization

## License

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
