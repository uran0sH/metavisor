# Metavisor Architecture Design Document

## 1. Project Overview

Metavisor is a data governance and metadata management platform, comparable to Apache Atlas. The core goal is to provide enterprise-level metadata management, data lineage tracking, classification/tagging, and search/discovery capabilities.

### 1.1 Core Values

| Capability | Description |
|------------|-------------|
| **Metadata Type System** | Flexible data model definition with support for type inheritance and complex attributes |
| **Data Lineage Tracking** | Column-level lineage tracking for complete data flow from source to target |
| **Classification & Tagging** | Dynamic classification (PII/sensitive data, etc.) with lineage propagation support |
| **Search & Discovery** | Full-text search to help users quickly find data assets |

---

## 2. Technology Stack

| Domain | Technology | Description |
|--------|------------|-------------|
| Web Framework | **axum** | High-performance async framework based on Tokio/Hyper |
| Graph Data Structure | **petgraph** | In-memory graph structure for lineage relationship computation |
| Search Index | **LanceDB** | Embedded vector database with full-text search (Tantivy) and vector search (IVF-PQ) |
| KV Storage | **surrealkv** | Embedded versioned KV storage with LSM-tree architecture |
| Message Queue | **Abstraction Layer** | trait-based abstraction, future integration with Kafka/NATS, etc. |
| Async Runtime | **tokio** | Rust standard async runtime |
| MCP Server | **rmcp** | Model Context Protocol SDK for AI assistant integration |

---

## 3. System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         API Layer                                │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │   REST API  │  │  Handlers   │  │        DTO/Models       │  │
│  │   (axum)    │  │             │  │                         │  │
│  └─────────────┘  └─────────────┘  └─────────────────────────┘  │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                    MCP Server (rmcp)                        ││
│  │              AI Assistant Integration                       ││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                        Core Layer                                │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │ Type System │  │   Graph     │  │    Search Interface     │  │
│  │             │  │   Engine    │  │     (SearchStore)       │  │
│  └─────────────┘  └─────────────┘  └─────────────────────────┘  │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                    Message Queue (trait)                     ││
│  │              [Kafka] [NATS] [In-Memory]                     ││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                       Storage Layer                              │
│  ┌─────────────┐  ┌─────────────────────────────────────────┐   │
│  │   Graph     │  │            Search Index (LanceDB)        │   │
│  │ (petgraph)  │  │  ┌───────────┐ ┌───────────┐ ┌────────┐ │   │
│  │ In-memory   │  │  │  FTS      │ │  Vector   │ │ Scalar │ │   │
│  │   Compute   │  │  │ (Tantivy) │ │ (IVF-PQ)  │ │ Filter │ │   │
│  └─────────────┘  │  └───────────┘ └───────────┘ └────────┘ │   │
│                   └─────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                    KV Store (surrealkv)                  │    │
│  │                  Persistent Storage                      │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

---

## 4. Module Design

### 4.1 Directory Structure

```
# Workspace structure - each crate independently manages dependencies
Cargo.toml                  # workspace root config
Cargo.lock

metavisor-core/             # Core layer crate
├── Cargo.toml
└── src/
    ├── lib.rs
    ├── error.rs
    ├── store.rs            # Store traits (TypeStore, EntityStore, MetavisorStore)
    ├── types/              # Type system
    │   ├── mod.rs
    │   ├── entity.rs
    │   ├── type_def.rs
    │   └── attribute.rs
    ├── graph/              # Graph engine
    │   ├── mod.rs
    │   ├── lineage.rs
    │   └── traversal.rs
    └── classification/     # Classification tags
        ├── mod.rs
        └── tag.rs

metavisor-storage/          # Storage layer crate
├── Cargo.toml
└── src/
    ├── lib.rs
    ├── error.rs
    ├── kv.rs               # KV storage (surrealkv)
    ├── type_store.rs       # Type persistence
    ├── entity_store.rs     # Entity persistence
    ├── relationship_store.rs # Relationship persistence
    ├── graph_store.rs      # Graph (petgraph) for lineage
    ├── metavisor_store.rs  # Unified MetavisorStore implementation
    └── search/             # Search index (LanceDB)
        ├── mod.rs
        ├── store.rs        # SearchStore trait
        ├── lancedb_store.rs # LanceDB implementation
        ├── types.rs        # SearchQuery, SearchResult types
        ├── schema.rs       # LanceDB schema definitions
        └── embedding/      # Embedding providers
            ├── mod.rs
            ├── provider.rs # EmbeddingProvider trait
            └── openai.rs   # OpenAI embedding implementation

metavisor-server/           # HTTP server crate
├── Cargo.toml
└── src/
    ├── lib.rs
    ├── error.rs            # HTTP status code mapping
    ├── routes.rs
    ├── handlers/
    │   ├── mod.rs
    │   ├── entity.rs
    │   ├── lineage.rs
    │   └── search.rs
    ├── dto.rs
    ├── mcp/                # MCP server integration
    │   ├── mod.rs
    │   └── server.rs
    └── bin/
        ├── metavisor.rs    # HTTP server entry point
        └── metavisor-mcp.rs # Stdio MCP server entry point

metavisor-mq/               # Message queue crate
├── Cargo.toml
└── src/
    ├── lib.rs
    ├── error.rs
    ├── traits.rs           # trait definition
    └── memory.rs           # In-memory implementation
```

#### Workspace Cargo.toml

```toml
# Root Cargo.toml
[workspace]
members = [
    "metavisor-core",
    "metavisor-storage",
    "metavisor-server",
    "metavisor-mq",
]
resolver = "2"

[workspace.package]
version = "0.1.0"
edition = "2021"

# Shared dependency versions
[workspace.dependencies]
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
thiserror = "2"
anyhow = "1"
tracing = "0.1"
uuid = { version = "1", features = ["v4", "serde"] }
chrono = { version = "0.4", features = ["serde"] }

# Internal crate dependencies
metavisor-core = { path = "metavisor-core" }
metavisor-storage = { path = "metavisor-storage" }
metavisor-server = { path = "metavisor-server" }
metavisor-mq = { path = "metavisor-mq" }
```

#### Example Crate Dependencies

```toml
# metavisor-core/Cargo.toml
[package]
name = "metavisor-core"
version.workspace = true
edition.workspace = true

[dependencies]
serde.workspace = true
thiserror.workspace = true
uuid.workspace = true
chrono.workspace = true
petgraph = "0.6"

# metavisor-storage/Cargo.toml
[package]
name = "metavisor-storage"

[dependencies]
thiserror.workspace = true
tokio.workspace = true
surrealkv = { git = "https://github.com/surrealdb/surrealkv" }
lancedb = "0.10"
arrow-array = "52"
arrow-schema = "52"

# Optional embedding providers
[features]
default = []
embedding-openai = ["reqwest"]

[dependencies.reqwest]
version = "0.12"
optional = true

# metavisor-server/Cargo.toml
[package]
name = "metavisor-server"

[[bin]]
name = "metavisor"
path = "src/bin/metavisor.rs"

[dependencies]
tokio.workspace = true
serde.workspace = true
serde_json.workspace = true
tracing.workspace = true
tracing-subscriber = "0.3"
axum = "0.8"
tower = "0.5"
tower-http = "0.6"
metavisor-core.workspace = true
metavisor-storage.workspace = true
```

#### Error Handling Strategy

Adopting a **hybrid approach**: each crate defines its own error types, `metavisor-server` aggregates all errors for HTTP responses.

```rust
// metavisor-core/src/error.rs
#[derive(thiserror::Error, Debug)]
pub enum CoreError {
    #[error("Type not found: {0}")]
    TypeNotFound(String),

    #[error("Entity not found: {0}")]
    EntityNotFound(String),

    #[error("Validation error: {0}")]
    Validation(String),
}

// metavisor-storage/src/error.rs
#[derive(thiserror::Error, Debug)]
pub enum StorageError {
    #[error("Key not found: {0}")]
    NotFound(String),

    #[error("KV store error: {0}")]
    Kv(#[from] surrealkv::Error),

    #[error("Search index error: {0}")]
    Search(String),

    #[error("Embedding error: {0}")]
    Embedding(String),
}

// metavisor-server/src/error.rs
#[derive(thiserror::Error, Debug)]
pub enum ApiError {
    #[error("{0}")]
    Core(#[from] metavisor_core::CoreError),

    #[error("{0}")]
    Storage(#[from] metavisor_storage::StorageError),

    #[error("Internal error: {0}")]
    Internal(String),
}

// HTTP status code mapping
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            ApiError::Core(e) => match e {
                CoreError::TypeNotFound(_) | CoreError::EntityNotFound(_) => (StatusCode::NOT_FOUND, e.to_string()),
                CoreError::Validation(_) => (StatusCode::BAD_REQUEST, e.to_string()),
            },
            ApiError::Storage(e) => match e {
                StorageError::NotFound(_) => (StatusCode::NOT_FOUND, e.to_string()),
                _ => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
            },
            ApiError::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()),
        };
        (status, Json(json!({ "error": message }))).into_response()
    }
}
```

---

### 4.2 Core Concept Models

#### 4.2.1 Type System

```
┌──────────────────────────────────────────────────────────────┐
│                       Type System                             │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│   TypeDef (Type Definition)                                  │
│   ├── name: String                    # Type name            │
│   ├── super_types: Vec<TypeDefId>     # Parent types (inheritance)│
│   ├── attributes: Vec<AttributeDef>   # Attribute definition list│
│   └── options: TypeOptions            # Type options         │
│                                                              │
│   AttributeDef (Attribute Definition)                        │
│   ├── name: String                    # Attribute name       │
│   ├── data_type: DataType             # Data type            │
│   ├── is_required: bool               # Required flag        │
│   ├── is_unique: bool                 # Unique flag          │
│   └── default_value: Option<Value>    # Default value        │
│                                                              │
│   DataType (Data Types)                                       │
│   ├── Primitive (string, int, bool, float, date...)          │
│   ├── Array(Box<DataType>)            # Array type           │
│   ├── Map(Box<DataType>)              # Map type             │
│   ├── Reference(TypeDefId)            # Reference type       │
│   └── Enum(Vec<String>)               # Enum type            │
│                                                              │
│   Entity (Entity Instance)                                    │
│   ├── id: EntityId                    # Unique identifier    │
│   ├── type_name: String               # Type name            │
│   ├── attributes: HashMap<String, Value>  # Attribute values │
│   ├── classifications: Vec<ClassificationId>  # Classification tags│
│   └── created/updated: DateTime       # Timestamps           │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

#### 4.2.2 Lineage

```
┌──────────────────────────────────────────────────────────────┐
│                     Lineage Graph                             │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│   LineageNode                                                │
│   ├── entity_id: EntityId             # Associated entity   │
│   ├── column: Option<String>          # Column-level lineage │
│   └── node_type: LineageNodeType      # Input/Output/Transform│
│                                                              │
│   LineageEdge                                                │
│   ├── source: LineageNodeId                                   │
│   ├── target: LineageNodeId                                   │
│   ├── transformation: Option<String>  # Transform expression │
│   └── process_id: Option<EntityId>    # Associated process   │
│                                                              │
│   LineageQuery                                               │
│   ├── upstream(entity_id, depth)      # Upstream lineage     │
│   ├── downstream(entity_id, depth)    # Downstream lineage   │
│   └── column_lineage(entity_id, column) # Column-level lineage│
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

#### 4.2.3 Classification

```
┌──────────────────────────────────────────────────────────────┐
│                    Classification                             │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│   Classification (Classification Definition)                 │
│   ├── name: String                    # Classification name (PII, etc.)│
│   ├── description: String             # Description          │
│   ├── attributes: Vec<AttributeDef>   # Classification attributes│
│   └── super_types: Vec<ClassificationId>  # Inheritance      │
│                                                              │
│   ClassifiedEntity (Classified Entity)                       │
│   ├── entity_id: EntityId                                    │
│   ├── classification_id: ClassificationId                    │
│   ├── attributes: HashMap<String, Value>  # Classification attribute values│
│   └── propagate: bool                 # Propagate along lineage│
│                                                              │
│   Propagation Rules:                                         │
│   - Classifications can be configured to auto-propagate along lineage│
│   - PII classification auto-propagates to downstream columns │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

---

### 4.3 Message Queue Abstraction Layer

```rust
/// Message queue abstraction trait
pub trait MessageQueue: Send + Sync {
    /// Publish message
    async fn publish(&self, topic: &str, message: &[u8]) -> Result<()>;

    /// Subscribe to messages
    async fn subscribe(
        &self,
        topic: &str,
        handler: Box<dyn Fn(&[u8]) -> Result<()> + Send + Sync>,
    ) -> Result<Subscription>;
}

/// Message types
pub enum MetavisorEvent {
    EntityCreated(Entity),
    EntityUpdated(Entity),
    EntityDeleted(EntityId),
    ClassificationAdded(EntityId, ClassificationId),
    LineageUpdated(LineageEdge),
}
```

---

## 5. API Design

### 5.1 RESTful API Endpoints

```
# Type Management (Atlas compatible)
POST   /api/metavisor/v1/types/typedefs              # Bulk create types
GET    /api/metavisor/v1/types/typedefs              # Get all type definitions
PUT    /api/metavisor/v1/types/typedefs              # Bulk update types
DELETE /api/metavisor/v1/types/typedefs              # Bulk delete types
GET    /api/metavisor/v1/types/typedefs/headers      # Get type header list
GET    /api/metavisor/v1/types/typedef/name/:name    # Get type by name
GET    /api/metavisor/v1/types/typedef/guid/:guid    # Get type by GUID

# Entity Management (Atlas compatible)
POST   /api/metavisor/v1/entity                      # Create entity
POST   /api/metavisor/v1/entity/bulk                 # Bulk create entities
GET    /api/metavisor/v1/entity/guid/:guid           # Get entity by GUID
GET    /api/metavisor/v1/entity/uniqueAttribute/type/:typeName  # Get entity by unique attribute
PUT    /api/metavisor/v1/entity                      # Update entity
DELETE /api/metavisor/v1/entity/guid/:guid           # Delete entity

# Lineage Queries
GET    /api/metavisor/v1/lineage/:guid/inputs        # Upstream lineage
GET    /api/metavisor/v1/lineage/:guid/outputs       # Downstream lineage
GET    /api/metavisor/v1/lineage/:guid/graph         # Complete lineage graph
POST   /api/metavisor/v1/relationship                # Create lineage relationship

# Classification Management (Atlas compatible)
POST   /api/metavisor/v1/types/typedefs              # Create classification (classificationDefs)
GET    /api/metavisor/v1/entity/guid/:guid/classifications  # Get entity classifications
POST   /api/metavisor/v1/entity/guid/:guid/classifications  # Add classification to entity

# Search
GET    /api/metavisor/v1/search?q=keyword            # Full-text search
POST   /api/metavisor/v1/search/semantic             # Semantic (vector) search
POST   /api/metavisor/v1/search/hybrid               # Hybrid search (FTS + vector)
POST   /api/metavisor/v1/search/reindex              # Rebuild search index
GET    /api/metavisor/v1/search/stats                # Index statistics
```

### 5.2 Search API Examples

```json
// POST /api/metavisor/v1/search/semantic
{
    "query": "tables containing sensitive user information",
    "type_names": ["Table", "Column"],
    "limit": 10
}

// POST /api/metavisor/v1/search/hybrid
{
    "query": "user order data",
    "vector_weight": 0.7,
    "text_weight": 0.3,
    "limit": 10
}

// Response
{
    "hits": [
        {
            "guid": "abc-123",
            "type_name": "Table",
            "name": "users",
            "score": 0.89,
            "highlight": "...sensitive <em>user</em> information..."
        }
    ],
    "total": 42
}
```

---

## 6. Storage Strategy

| Data Type | Storage Location | Description |
|-----------|------------------|-------------|
| Type Definitions | KV Store | Schema information, low change frequency |
| Entity Data | KV Store + Search Index | KV stores complete data, LanceDB supports search |
| Lineage Relationships | Graph (In-memory) + KV | petgraph computes lineage, KV persists |
| Classifications/Tags | KV Store + Search Index | Supports search by classification |
| Search Index | LanceDB | Full-text search (Tantivy) + Vector search (IVF-PQ) |

### 6.1 LanceDB Search Index

LanceDB serves as the unified search index with three capabilities:

| Capability | Implementation | Use Case |
|------------|---------------|----------|
| **Full-text Search** | Built-in Tantivy | Search by name, description, attributes |
| **Vector Search** | IVF-PQ index | Semantic search with embeddings |
| **Scalar Filtering** | Lance columnar format | Filter by type, status, classification |

### 6.2 Index Synchronization

```
Entity Write Flow:
    │
    ├── 1. Write to KV Store (primary, blocking)
    │
    ├── 2. Write to Graph Store (lineage, with rollback)
    │
    └── 3. Write to Search Index (async, non-blocking)
            └── Failure logged, background retry
```

### 6.3 Embedding Strategy

Vector search is optional and configurable:

| Provider | Use Case | Configuration |
|----------|----------|---------------|
| **OpenAI** | Production, high quality | `embedding-openai` feature |
| **Local Model** | Offline, privacy-sensitive | Future: `embedding-local` feature |
| **Disabled** | No vector search needed | Default |

---

## 7. Development Phase Plan

### Phase 1: Basic Infrastructure
- [x] Project skeleton setup
- [x] Error handling framework
- [x] Storage layer basic implementation (KV)

### Phase 2: Type System
- [x] TypeDef / AttributeDef definitions
- [x] Entity CRUD
- [x] Type validation logic
- [x] Type system REST API implementation
- [x] Relationship CRUD

### Phase 3: Lineage Tracking
- [x] petgraph integration
- [x] Lineage relationship storage
- [x] Upstream/downstream queries
- [x] Lineage REST API implementation
- [x] Classification propagation through lineage

### Phase 4: Classification & Tagging
- [x] Classification definitions
- [x] Entity classification associations
- [x] Lineage propagation logic
- [x] Classification REST API implementation

### Phase 5: Search Enhancement (LanceDB)
- [ ] LanceDB integration
- [ ] SearchStore trait implementation
- [ ] Full-text search API
- [ ] Vector search (optional)
- [ ] Hybrid search (FTS + vector)

### Phase 6: MCP Integration
- [x] MCP server with rmcp SDK
- [x] HTTP transport (/mcp endpoint)
- [x] Stdio transport (standalone binary)
- [x] MCP tools for entity/lineage operations

### Phase 7: Message Queue
- [ ] MQ trait abstraction
- [ ] In-memory implementation
- [ ] Event publishing mechanism

---

## 8. Future Extensions

- **Multi-DataSource Hook**: Automatic metadata collection from Hive/MySQL/Spark, etc.
- **Security Integration**: Integration with permission systems like Apache Ranger
- **Distributed/k8s**: Scale from standalone to distributed
- **GraphQL API**: Alternative or complement to REST API
- **Web UI**: Lineage visualization, metadata browsing interface
- **Local Embedding Model**: Candle-transformers for offline vector search
- **LanceDB Enterprise**: Distributed vector search for large-scale deployments

---

## 9. References

- [LanceDB Search Design](./lancedb-search-design.md) - Detailed LanceDB integration design
- [Apache Atlas](https://atlas.apache.org/) - Reference architecture for metadata management
- [LanceDB Documentation](https://docs.lancedb.com/) - Vector database documentation
