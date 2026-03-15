# 向量搜索设计文档

## 概述

本文档记录 Metavisor 向量搜索功能的存储方案调研结果和设计决策。

## 需求

Metavisor 需要支持以下核心能力：
- **血缘追踪**：数据血缘关系的存储和查询
- **关系管理**：实体间关系的 CRUD
- **向量搜索**：基于向量相似度的语义搜索

## 存储方案对比

### 调研过的方案

| 方案 | 类型 | 图+向量 | Rust 嵌入式 | 成熟度 | 结论 |
|------|------|---------|-------------|--------|------|
| LanceDB 替换 KV | 向量数据库 | ❌ | ✅ | 高 | ❌ 数据模型不匹配 |
| HelixDB | 图+向量数据库 | ✅ | ❌ 独立服务 | 中 | ❌ 非嵌入式 |
| FalkorDB | 图数据库 | ✅ | ❌ 无 Rust SDK | 高 | ❌ 无 Rust 支持 |
| IndraDB + LanceDB | 图 + 向量组合 | ✅ | ✅ | 中 | ⚠️ 可选方案 |
| Qdrant 服务 | 向量数据库 | ❌ | ❌ 独立服务 | 高 | ⚠️ 可选方案 |
| hora + KV | 纯向量索引 | ❌ | ✅ | 低 | ❌ 功能有限 |

### 关键调研结论

1. **LanceDB**
   - 开源，Apache 2.0 许可证
   - 嵌入式，Rust 原生
   - 但数据模型是列式表结构，不适合直接替换 KV 存储
   - 适合作为向量索引层

2. **Qdrant**
   - 官方 Rust 客户端需要独立服务
   - qdrant-lib（社区嵌入式）作者声明不适合生产使用
   - 如需使用建议部署独立 Qdrant 服务

3. **hora**
   - 纯 Rust HNSW 向量索引库
   - 功能有限，无过滤、无持久化
   - 需要自己实现持久化和 ID 映射

4. **HelixDB**
   - 纯 Rust 图+向量混合数据库
   - 非嵌入式，需要独立部署
   - 可作为未来的备选方案

## 最终方案

### 架构设计

```
┌─────────────────────────────────────────────────────────────┐
│                    Metavisor Storage                        │
├─────────────────┬─────────────────┬─────────────────────────┤
│   SurrealKV     │     Tantivy     │       LanceDB           │
│   (元数据KV)    │    (全文搜索)    │     (向量搜索)          │
├─────────────────┼─────────────────┼─────────────────────────┤
│ • 实体数据      │ • 中文全文索引   │ • 向量 + guid 引用      │
│ • 关系数据      │ • BM25 排序     │ • HNSW 索引             │
│ • 类型定义      │ • jieba 分词    │ • 过滤 + 向量搜索       │
│ • 索引映射      │                 │                         │
└─────────────────┴─────────────────┴─────────────────────────┘
```

### 各组件职责

| 组件 | 职责 | 查询场景 |
|------|------|----------|
| **SurrealKV** | 主存储、事务、索引 | 点查询、前缀扫描、原子写入 |
| **Tantivy** | 全文搜索 | 中文文本搜索、BM25 排序 |
| **LanceDB** | 向量搜索 | 语义相似度搜索、向量+过滤 |

### 为什么不把所有数据存入 LanceDB？

1. **Schema 固定**：LanceDB 是列式表结构，需要预定义 schema，不如 KV 灵活
2. **更新机制**：LanceDB 使用软删除 + 追加写，频繁更新有性能开销
3. **无传统事务**：没有 KV 存储的 batch_write 原子事务
4. **点查询效率**：列式存储对单点查询不如 KV 高效
5. **前缀扫描**：没有 KV 的原生前缀扫描能力

## 存储键设计（SurrealKV）

| Key 格式 | 说明 |
|----------|------|
| `entity:{guid}` | 实体数据 |
| `entity_type:{type}:{guid}` | 类型索引 |
| `type_def:{type_name}` | 类型定义 |
| `relationship:{guid}` | 关系数据 |
| `rel_endpoint:{entity_guid}:{rel_guid}` | 关系端点索引 |
| `rel_type:{type_name}:{rel_guid}` | 关系类型索引 |
| `vector_ref:{guid}` | 向量引用（指向 LanceDB） |

## 数据流设计

### 写入流程

```
1. Entity → SurrealKV (entity:{guid})
2. Type Index → SurrealKV (entity_type:{type}:{guid})
3. Text Fields → Tantivy (全文索引)
4. Vector + guid → LanceDB (向量索引，只存 guid 引用)
```

### 查询流程

```
全文搜索:  Query → Tantivy → [guids] → SurrealKV → Entities
向量搜索:  Vector → LanceDB → [guids] → SurrealKV → Entities
混合搜索:  并行查询 → RRF 合并 → SurrealKV → Entities
```

## 实现计划

### 阶段 1：添加 LanceDB 集成

1. 添加依赖
   ```toml
   [dependencies]
   lancedb = "0.4"
   ```

2. 创建 VectorIndex trait 抽象层
   ```rust
   pub trait VectorIndex: Send + Sync {
       fn add(&self, guid: &str, vector: &[f32], metadata: HashMap<String, Value>) -> Result<()>;
       fn search(&self, query: &[f32], k: usize, filter: Option<Filter>) -> Result<Vec<SearchResult>>;
       fn delete(&self, guid: &str) -> Result<()>;
   }
   ```

3. 实现 LanceDB 适配器

### 阶段 2：集成到 MetavisorStorage

1. 在 MetavisorStorage 中添加 vector_index 字段
2. 实现创建实体时同时写入向量索引
3. 实现向量搜索 API

### 阶段 3：混合搜索

1. 实现全文搜索和向量搜索的并行查询
2. 实现 RRF (Reciprocal Rank Fusion) 结果合并
3. 添加混合搜索 API 端点

## 参考资料

- [LanceDB GitHub](https://github.com/lancedb/lancedb)
- [LanceDB Documentation](https://lancedb.github.io/lancedb/)
- [Lance Format](https://docs.lancedb.com/lance)
- [Qdrant Rust Client](https://github.com/qdrant/rust-client)
- [hora - Rust HNSW Library](https://github.com/hora-search/hora)
- [HelixDB - Graph + Vector Database](https://github.com/HelixDB/helix-db)

## 变更历史

| 日期 | 变更 |
|------|------|
| 2025-03-15 | 初始版本，确定混合存储方案 |
