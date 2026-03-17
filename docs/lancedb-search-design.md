# LanceDB Search Index Design

## 1. Overview

Replace Tantivy with LanceDB as the unified search index for Metavisor, supporting:
- **Full-text Search (FTS)** - Search entity names, descriptions, attributes
- **Vector Search** - Semantic search with embeddings
- **Scalar Filtering** - Filter by type, classification, status
- **Hybrid Search** - Combine FTS + vector search

## 2. Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    DefaultMetavisorStore                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │
│  │  TypeStore  │  │ EntityStore │  │Relationship │             │
│  │   (KV)      │  │    (KV)     │  │Store (KV)   │  ← Persistence│
│  └─────────────┘  └─────────────┘  └─────────────┘             │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    SearchIndex (LanceDB)                 │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐      │   │
│  │  │ Full-text   │  │   Vector    │  │   Scalar    │      │   │
│  │  │ Search      │  │   Search    │  │   Filter    │      │   │
│  │  │ (Tantivy)   │  │  (IVF-PQ)   │  │  (Lance)    │      │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘      │   │
│  └─────────────────────────────────────────────────────────┘   │
│                          ↑                                       │
│                    Search/Index Layer                           │
└─────────────────────────────────────────────────────────────────┘
```

## 3. Schema Design

### 3.1 Entity Search Table

```rust
// Table name: entities
Schema {
    // === Primary Key ===
    guid: String,              // Entity GUID (primary key)

    // === Basic Fields (scalar + FTS) ===
    type_name: String,         // Entity type name (for filtering)
    name: String,              // Entity name (FTS + storage)
    description: String,       // Description (FTS)
    status: String,            // Status: ACTIVE, DELETED, etc.

    // === Attributes (JSON + FTS) ===
    attributes_json: String,   // Full attributes JSON (storage)
    attributes_text: String,   // Searchable attribute text (FTS)

    // === Classifications ===
    classifications: Vec<String>,  // Classification names (filter + FTS)

    // === Timestamps ===
    created_at: Timestamp,     // Creation time
    updated_at: Timestamp,     // Update time

    // === Vector (optional) ===
    embedding: Option<Vec<f32>>,  // Semantic vector (1536 dims)
}
```

### 3.2 Index Configuration

```rust
// Full-text search index
FTSIndex {
    columns: ["name", "description", "attributes_text"],
    tokenizer: Tokenizer::default(),  // LanceDB built-in
}

// Vector index (optional, requires sufficient data)
VectorIndex {
    column: "embedding",
    index_type: IvfPq,
    metric: Cosine,
    num_partitions: num_rows / 4096,
    num_sub_vectors: 192,  // 1536 / 8
}

// Scalar indexes (accelerate filtering)
ScalarIndex {
    columns: ["type_name", "status", "classifications"],
}
```

## 4. API Design

### 4.1 SearchStore Trait

```rust
/// Search index trait
#[async_trait]
pub trait SearchStore: Send + Sync {
    /// Index a single entity
    async fn index_entity(&self, entity: &Entity) -> Result<()>;

    /// Batch index entities
    async fn index_entities(&self, entities: &[Entity]) -> Result<()>;

    /// Delete entity from index
    async fn delete_entity(&self, guid: &str) -> Result<()>;

    /// Full-text search
    async fn search(&self, query: &SearchQuery) -> Result<SearchResult>;

    /// Vector search (semantic search)
    async fn vector_search(
        &self,
        query: &str,           // Query text, auto-generate embedding
        options: VectorSearchOptions,
    ) -> Result<SearchResult>;

    /// Hybrid search (FTS + vector)
    async fn hybrid_search(
        &self,
        query: &str,
        options: HybridSearchOptions,
    ) -> Result<SearchResult>;

    /// Rebuild index from KV store
    async fn rebuild_index(&self) -> Result<()>;

    /// Get index statistics
    fn stats(&self) -> SearchStats;
}

/// Search query
pub struct SearchQuery {
    /// Search text
    pub text: String,

    /// Filter conditions
    pub filters: Option<SearchFilters>,

    /// Pagination
    pub limit: usize,
    pub offset: usize,
}

pub struct SearchFilters {
    /// Filter by entity types
    pub type_names: Option<Vec<String>>,

    /// Filter by classifications
    pub classifications: Option<Vec<String>>,

    /// Filter by status
    pub status: Option<String>,
}

/// Search result
pub struct SearchResult {
    pub hits: Vec<SearchHit>,
    pub total: usize,
}

pub struct SearchHit {
    pub guid: String,
    pub type_name: String,
    pub name: String,
    pub score: f32,
    pub highlight: Option<String>,
}

pub struct VectorSearchOptions {
    pub limit: usize,
    pub filters: Option<SearchFilters>,
    pub nprobes: Option<usize>,       // IVF partitions to search
    pub refine_factor: Option<usize>, // Reranking candidates
}

pub struct HybridSearchOptions {
    pub limit: usize,
    pub filters: Option<SearchFilters>,
    pub vector_weight: f32,  // 0.0 - 1.0
    pub text_weight: f32,    // 0.0 - 1.0
}

pub struct SearchStats {
    pub indexed_count: usize,
    pub index_size_bytes: u64,
    pub last_updated: Option<DateTime<Utc>>,
}
```

### 4.2 Integration with MetavisorStore

```rust
#[async_trait]
pub trait MetavisorStore: Send + Sync {
    // ... existing methods ...

    // === New Search Methods ===

    /// Full-text search entities
    async fn search_entities(&self, query: &SearchQuery) -> Result<SearchResult>;

    /// Semantic search entities
    async fn semantic_search(
        &self,
        query: &str,
        options: VectorSearchOptions,
    ) -> Result<SearchResult>;

    /// Hybrid search (FTS + semantic)
    async fn hybrid_search(
        &self,
        query: &str,
        options: HybridSearchOptions,
    ) -> Result<SearchResult>;

    /// Rebuild search index
    async fn rebuild_search_index(&self) -> Result<()>;

    /// Get search stats
    fn search_stats(&self) -> SearchStats;
}
```

## 5. Embedding Strategy

### 5.1 Provider Options

| Provider | Pros | Cons |
|----------|------|------|
| **OpenAI API** | High quality | Requires network, paid |
| **Local model (candle)** | Offline, free | Need to download model |
| **FastEmbed** | Lightweight, fast | Requires ONNX Runtime |
| **Configurable** | Flexible | Complex implementation |

### 5.2 Recommended: Optional + Disabled by Default

```rust
pub struct SearchConfig {
    /// Enable vector search
    pub enable_vector_search: bool,

    /// Embedding provider configuration
    pub embedding_provider: Option<EmbeddingConfig>,

    /// LanceDB data directory
    pub data_dir: PathBuf,
}

pub enum EmbeddingConfig {
    /// OpenAI API
    OpenAI {
        api_key: String,
        model: String,  // e.g., "text-embedding-3-small"
    },

    /// Local model (candle-transformers)
    Local {
        model_path: PathBuf,
    },

    /// FastEmbed (ONNX)
    FastEmbed {
        model: String,  // e.g., "BAAI/bge-small-en-v1.5"
    },
}

/// Embedding provider trait
#[async_trait]
pub trait EmbeddingProvider: Send + Sync {
    /// Generate embedding for text
    async fn embed(&self, text: &str) -> Result<Vec<f32>>;

    /// Generate embeddings for multiple texts
    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>>;

    /// Get embedding dimension
    fn dimension(&self) -> usize;
}
```

## 6. KV Store Synchronization

### 6.1 Write Flow

```
create_entity()
    │
    ├── 1. Write to KV Store (primary storage)
    │       └── Continue on success
    │
    ├── 2. Write to Graph Store (lineage)
    │       └── Rollback KV on failure
    │
    └── 3. Write to SearchIndex (async, non-blocking)
            └── Log warning on failure, background retry
```

### 6.2 Implementation

```rust
impl DefaultMetavisorStore {
    async fn create_entity(&self, entity: &Entity) -> Result<String> {
        // 1. KV write
        let guid = self.entity_store.create_entity(entity).await?;

        // 2. Graph write (with rollback)
        if let Err(e) = self.graph_store.add_entity_node(&guid, &entity.type_name).await {
            let _ = self.entity_store.delete_entity(&guid).await;
            return Err(e);
        }

        // 3. Search index (async, non-blocking)
        let search_store = self.search_store.clone();
        let guid_clone = guid.clone();
        let entity_clone = entity.clone();
        tokio::spawn(async move {
            if let Err(e) = search_store.index_entity(&entity_clone).await {
                tracing::warn!("Failed to index entity {}: {}", guid_clone, e);
                // TODO: Add to retry queue
            }
        });

        Ok(guid)
    }

    async fn update_entity(&self, entity: &Entity) -> Result<()> {
        // 1. Update KV
        self.entity_store.update_entity(entity).await?;

        // 2. Update graph
        // ... existing graph update logic ...

        // 3. Update search index (async)
        let search_store = self.search_store.clone();
        let entity_clone = entity.clone();
        tokio::spawn(async move {
            if let Err(e) = search_store.index_entity(&entity_clone).await {
                tracing::warn!("Failed to update search index: {}", e);
            }
        });

        Ok(())
    }

    async fn delete_entity(&self, guid: &str) -> Result<()> {
        // 1. Delete from graph
        self.graph_store.remove_entity_node(guid).await?;

        // 2. Delete from KV
        self.entity_store.delete_entity(guid).await?;

        // 3. Delete from search index (async)
        let search_store = self.search_store.clone();
        let guid_clone = guid.to_string();
        tokio::spawn(async move {
            if let Err(e) = search_store.delete_entity(&guid_clone).await {
                tracing::warn!("Failed to delete from search index: {}", e);
            }
        });

        Ok(())
    }
}
```

## 7. File Structure

```
metavisor-storage/src/
├── lib.rs
├── search/
│   ├── mod.rs              # Module exports
│   ├── store.rs            # SearchStore trait
│   ├── lancedb_store.rs    # LanceDB implementation
│   ├── types.rs            # SearchQuery, SearchResult, etc.
│   ├── embedding/
│   │   ├── mod.rs
│   │   ├── provider.rs     # EmbeddingProvider trait
│   │   ├── openai.rs       # OpenAI implementation
│   │   └── noop.rs         # No-op implementation (disabled)
│   └── schema.rs           # LanceDB schema definition
├── entity_store.rs         # (existing)
├── relationship_store.rs   # (existing)
├── graph_store.rs          # (existing)
├── metavisor_store.rs      # (modified: add search support)
└── index.rs                # (DELETE or keep as compat layer)
```

## 8. REST API Design

### 8.1 Search Endpoints

```
# Full-text search
GET /api/metavisor/v1/search?q={query}&type={typeName}&limit=10

# Semantic search
POST /api/metavisor/v1/search/semantic
{
    "query": "tables containing sensitive user information",
    "type_names": ["Table", "Column"],
    "limit": 10
}

# Hybrid search
POST /api/metavisor/v1/search/hybrid
{
    "query": "user order data",
    "vector_weight": 0.7,
    "text_weight": 0.3,
    "limit": 10
}

# Index management
POST /api/metavisor/v1/search/reindex   # Rebuild index
GET  /api/metavisor/v1/search/stats     # Index statistics
```

### 8.2 Response Format

```json
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

## 9. Dependencies

```toml
# metavisor-storage/Cargo.toml
[dependencies]
lancedb = "0.10"
arrow-array = "52"
arrow-schema = "52"

# Optional: embedding providers
[features]
default = []
embedding-openai = ["reqwest"]
embedding-local = ["candle-transformers", "candle-nn"]

[dependencies.reqwest]
version = "0.12"
optional = true

[dependencies.candle-transformers]
version = "0.4"
optional = true
```

## 10. Implementation Phases

### Phase 1: Basic Integration
- [ ] Add LanceDB dependency
- [ ] Implement SearchStore trait
- [ ] Implement LanceDbSearchStore (FTS only)
- [ ] Integrate into DefaultMetavisorStore
- [ ] Add REST API endpoints
- [ ] Unit tests

### Phase 2: Vector Search
- [ ] Implement EmbeddingProvider trait
- [ ] Add OpenAI embedding implementation
- [ ] Implement vector indexing
- [ ] Implement semantic search API
- [ ] Integration tests

### Phase 3: Optimization
- [ ] Local embedding model support
- [ ] Incremental index updates
- [ ] Performance tuning
- [ ] Monitoring and metrics

## 11. Configuration

```yaml
# metavisor-config.yaml
search:
  enabled: true
  data_dir: "./data/search"

  vector:
    enabled: true
    embedding:
      provider: "openai"  # or "local" or "none"
      model: "text-embedding-3-small"
      # api_key: "${OPENAI_API_KEY}"  # From environment

  index:
    fts:
      tokenizer: "default"
    vector:
      num_partitions: 256
      metric: "cosine"
```

## 12. Migration from Tantivy

1. **Delete** `metavisor-storage/src/index.rs`
2. **Update** `metavisor-storage/src/lib.rs` - remove tantivy exports
3. **Create** new `search/` module
4. **Modify** `metavisor_store.rs` - add search_store field
5. **Update** `metavisor-core/src/store.rs` - add search methods to trait
6. **Add** REST handlers in metavisor-server
