# Metavisor API Test Scripts

使用 Python 测试脚本验证 Metavisor API 功能。

## 环境准备

需要安装 [uv](https://docs.astral.sh/uv/)：

```bash
# macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh
```

## 使用方法

```bash
cd tests

# 运行所有测试
uv run python run_test_data.py

# 运行性能测试（默认使用 http://127.0.0.1:31000/api/metavisor/v1）
uv run python run_perf_test.py

# 指定不同 API 前缀（如 Atlas）
uv run python run_perf_test.py --base-url http://127.0.0.1:31000/api/atlas/v2 --requests 500 --concurrency 20

# 运行指定命令
uv run python run_test_data.py types
uv run python run_test_data.py entities
uv run python run_test_data.py relationships
uv run python run_test_data.py lineage-relationships
uv run python run_test_data.py query
uv run python run_test_data.py lineage
uv run python run_test_data.py list
uv run python run_test_data.py cleanup

# 查询类型定义
uv run python run_test_data.py get-type sql_meta

# 查询实体
uv run python run_test_data.py get-entity column_meta BDSP_SPCP.T80_PC8_CPS_PBK.PARTY_ID

# 通过 GUID 查询实体
uv run python run_test_data.py get-entity-by-guid <guid>

# 指定服务器地址（支持完整 API 路径）
uv run python run_test_data.py --base-url http://localhost:8080 all

# 指定 Atlas 兼容 API
uv run python run_test_data.py --base-url http://8.92.9.185:21000/api/atlas/v2 all
```

## 命令说明

| 命令 | 说明 |
|------|------|
| `all` | 运行所有测试（默认） |
| `types` | 创建类型定义 |
| `entities` | 创建实体 |
| `relationships` | 创建关系 |
| `lineage-relationships` | 创建血缘关系（含 process_inputs/outputs） |
| `query` | 运行查询测试（basic search + relations search） |
| `lineage` | 运行血缘测试 |
| `list` | 列出所有数据 |
| `get-type [name]` | 获取类型定义 |
| `get-entity [type] [qn]` | 通过 qualifiedName 获取实体 |
| `get-entity-by-guid <guid>` | 通过 GUID 获取实体 |
| `cleanup` | 删除所有测试数据 |

## 测试流程

1. 启动 Metavisor 服务器：
   ```bash
   cargo run --bin metavisor
   ```

2. 运行测试：
   ```bash
   uv run python run_test_data.py
   ```

3. 清理测试数据：
   ```bash
   uv run python run_test_data.py cleanup
   ```

## 性能测试

`run_perf_test.py` 用于压测 Metavisor API，支持混合读写负载。

### 使用方法

```bash
cd tests

# 默认参数（200请求，16并发）
uv run python run_perf_test.py

# Metavisor API（完整 base-url 包含 API 前缀）
uv run python run_perf_test.py \
  --base-url http://127.0.0.1:31000/api/metavisor/v1 \
  --requests 500 \
  --concurrency 20

# Atlas API 兼容模式
uv run python run_perf_test.py \
  --base-url http://8.92.9.185:21000/api/atlas/v2 \
  --requests 1000 \
  --concurrency 32
```

### 测试流程

1. **准备阶段**：清理并加载标准测试数据（类型、实体、关系）
2. **压测阶段**：并发执行混合读写操作

### 测试负载构成

| 操作类型 | 占比 | 接口 | 说明 |
|---------|------|------|------|
| **读操作** | ~45% | | |
| GET entity | 27% | `/entity/uniqueAttribute` | 通过 qualifiedName 查询 |
| GET typedef | 9% | `/types/typedef/name/{name}` | 查询类型定义 |
| GET relationship | 9% | `/relationship/guid/{guid}` | 查询关系详情 |
| **写操作** | ~55% | | |
| POST create type | 9% | `/types/typedefs` | 创建类型定义 |
| POST create entity | 27% | `/entity` | 创建实体（2种变体） |
| POST create relationship | 9% | `/relationship` | 创建关系 |
| POST update entity | 9% | `/entity` | 更新已有实体 |

### 输出指标

```
Total requests: 110
Concurrency: 8
Elapsed: 0.04s
Throughput: 2728.23 req/s
Success: 110
Failed: 0
Avg latency: 2.77 ms
P50 latency: 2.66 ms
P95 latency: 4.27 ms
P99 latency: 4.71 ms
```

- **吞吐量**：每秒处理的请求数
- **平均延迟**：所有请求的平均响应时间
- **P50/P95/P99**：延迟分位数，反映延迟分布
- **失败请求**：展示前5个失败样例

## 测试数据文件

| 文件 | 说明 |
|------|------|
| `data/sql_meta_type.json` | SQL 元数据类型定义 |
| `data/column_meta_type.json` | 列元数据类型定义 |
| `data/relationship_type.json` | 关系类型定义（join_relationship, sql_uses_column） |
| `data/column_meta_entity_*.json` | 列元数据实体 |
| `data/sql_meta_entity_*.json` | SQL 元数据实体 |
| `data/join_relationship_*.json` | JOIN 关系数据 |
| `data/sql_column_relationship_*.json` | SQL-列关系数据 |
| `data/query.json` | 查询请求数据 |

## API 格式说明

### uniqueAttribute 查询

使用 query parameter 格式 `?attr:{attributeName}={value}`：

```bash
# 通过 qualifiedName 查询实体
GET /api/metavisor/v1/entity/uniqueAttribute/type/column_meta?attr:qualifiedName=xxx

# 通过 qualifiedName 查询血缘
GET /api/metavisor/v1/lineage/uniqueAttribute/type/column_meta?attr:qualifiedName=xxx&direction=BOTH
```

### 血缘查询

使用 `direction` 参数指定方向：

```bash
# 上游血缘
GET /api/metavisor/v1/lineage/{guid}?depth=3&direction=INPUT

# 下游血缘
GET /api/metavisor/v1/lineage/{guid}?depth=3&direction=OUTPUT

# 完整血缘
GET /api/metavisor/v1/lineage/{guid}?depth=3&direction=BOTH
```

### 关系搜索

```bash
POST /api/metavisor/v1/search/relations
Content-Type: application/json

{
  "typeName": "join_relationship",
  "relationshipFilters": {
    "end1": {
      "typeName": "column_meta",
      "uniqueAttributes": {
        "qualifiedName": "xxx"
      }
    }
  },
  "limit": 50,
  "offset": 0
}
```

## 验证机制

测试脚本会在每个创建操作后自动验证：

- **类型定义**：POST 后 GET 验证 `name` 匹配
- **实体**：POST 后 GET 验证 `guid` 和 `typeName` 匹配
- **关系**：POST 后 GET 验证 `guid` 匹配
- **查询**：验证返回结果数量

清理操作使用保存的 GUID 直接调用 `/entity/guid/{guid}` 和 `/relationship/guid/{guid}` 删除。
