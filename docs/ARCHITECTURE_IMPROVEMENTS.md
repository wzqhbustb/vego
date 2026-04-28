# Vego 架构改进事项

> 记录当前因 Vego 底层限制而无法在 memory 层完全解决的问题，供未来 Vego 演进参考。
>
> 创建日期：2026-04-28

---

## 1. HNSW 批量插入缺乏事务回滚机制

### 问题描述

`DocumentStorage.InsertBatchContext`（Vego 层）的语义是"先校验全部文档，再逐条插入 HNSW"。但 HNSW 索引的插入是**立即生效且不可回滚**的——如果第 N 个文档的插入失败（如内存不足、维度不匹配、内部错误），前 N-1 个文档已经存在于 HNSW 索引中，而持久化层（列式存储）和 metadata JSON 尚未写入。

```
InsertBatchContext flow:
  1. Validate all docs (dimension, etc.) ✅
  2. For each doc:
     a. storage.Put(doc)          → 写入 buffer（未 flush 到磁盘）
     b. index.Add(doc.Vector)     → 立即写入 HNSW（不可逆）
  3. If step 2b fails at doc N:
     → docs 1..N-1 are in HNSW
     → docs N..end are not
     → metadata JSON not saved
     → caller gets error, no partial success info
```

### 影响

| 场景 | 后果 |
|------|------|
| `MemoryStore.StoreBatch` | 返回 error，但部分文档已进入 HNSW；重试可能产生重复向量 |
| `MemoryStore.Bootstrap` | 同上；大数据量导入时风险更高 |
| 重复向量 | HNSW 中存在多个相同 ID 的节点（Vego 用 DV 过滤旧版，但批量插入场景无旧版概念） |

### 为什么 memory 层无法解决

- HNSW 索引在 `index/hnsw.go` 中，memory 包不直接操作它
- `index.Add()` 没有 "prepare + commit" 接口，无法模拟事务
- HNSW 的图结构一旦建立连接（邻居关系），撤销需要 O(degree) 的局部重建，Go 实现中无此功能

### 当前 Workaround（memory 层）

1. **Pre-validation**：在调用 `InsertBatchContext` 前，对文档做结构校验（nil、空 ID、空 vector、维度匹配），提前排除明显错误
2. **注释说明**：明确告知调用方 `InsertBatchContext` 是"全有或全无"，但 HNSW 层的失败可能导致部分副作用残留
3. **错误信息增强**：返回的错误中包含文档数量和原始 Vego error，便于排查

```go
// memory/memory.go
for i, doc := range docs {
    if doc == nil { return nil, fmt.Errorf("item %d: document is nil", i) }
    if len(doc.Vector) != s.config.Dimension {
        return nil, fmt.Errorf("item %d (%s): vector dimension mismatch", i, doc.ID)
    }
}

if err := s.coll.InsertBatchContext(ctx, docs); err != nil {
    return nil, fmt.Errorf("insert batch (%d items): %w", len(docs), err)
}
```

### 建议的 Vego 层修复方案

#### 方案 A：HNSW 批量插入的两阶段提交

```go
func (c *Collection) InsertBatchContext(ctx context.Context, docs []*Document) error {
    // Phase 1: 准备阶段 —— 只校验，不插入
    for _, doc := range docs {
        if err := doc.Validate(c.dimension); err != nil {
            return err
        }
    }

    // Phase 2: 执行阶段 —— 先存 metadata + 列式存储，最后批量插入 HNSW
    // 如果 HNSW 插入失败，metadata 层可以回滚（buffer 尚未 flush）
    nodeIDs := make([]int, 0, len(docs))
    for _, doc := range docs {
        if err := c.storage.Put(doc); err != nil {
            // 回滚已插入的 HNSW 节点（需要新 API）
            c.index.RemoveBatch(nodeIDs)
            return err
        }
        nodeID, err := c.index.Add(doc.Vector)
        if err != nil {
            c.index.RemoveBatch(nodeIDs)
            return err
        }
        nodeIDs = append(nodeIDs, nodeID)
        c.docToNode[doc.ID] = nodeID
    }
    return nil
}
```

**需要 Vego 新增：**
- `HNSWIndex.RemoveBatch(nodeIDs []int)` —— 批量移除节点并修复邻居连接

#### 方案 B：批量插入改为单条事务

将 `InsertBatchContext` 的循环改为每个文档独立的事务边界：

```go
for _, doc := range docs {
    if err := c.InsertContext(ctx, doc); err != nil {
        // 返回部分成功信息：哪些已插入，哪些失败
        return &PartialBatchError{Inserted: i, Err: err}
    }
}
```

**代价：** 性能下降（每次 Insert 都涉及锁竞争），但语义清晰。

#### 方案 C：引入 Write-Ahead Log（WAL）

在 Vego 存储层引入轻量级 WAL：

```
1. 将批量操作写入 WAL（append-only）
2. 执行 HNSW 插入
3. 执行 metadata 持久化
4. 删除 WAL 记录

崩溃恢复时：
- 读取 WAL，重放未完成的操作
- 或回滚已完成的操作
```

**代价：** 增加 ~200 行代码和一个 WAL 文件，但解决所有非原子操作问题。

### 优先级评估

| 方案 | 复杂度 | 性能影响 | 推荐度 |
|------|--------|---------|--------|
| A（两阶段 + HNSW RemoveBatch）| 中 | 无 | ⭐⭐⭐⭐ 首选 |
| B（单条事务）| 低 | 显著下降 | ⭐⭐ 备选 |
| C（WAL）| 高 | 轻微下降 | ⭐⭐⭐ 长期考虑 |

---

## 2. 其他待 Vego 层支持的改进（记录备查）

### 2.1 Metadata 存储从 JSON 升级为 SQLite

详见 [vego_json_metadata_analysis.md](./vego_json_metadata_analysis.md)。

核心诉求：
- 增量更新（当前 JSON 全量覆盖）
- 事务支持
- 字段级索引（`state`、`type` 等）

### 2.2 `Collection.GetBatchContext` 公共 API

`Reconcile` 阶段的 `findCandidates` 需要对每个候选 ID 单独调用 `GetContext`，产生 N 次 DB round-trip。如果 Vego 暴露批量 Get API，可将 N 次降为 1 次。

```go
// 建议新增
func (c *Collection) GetBatchContext(ctx context.Context, ids []string) (map[string]*Document, error)
```

### 2.3 HNSW 遍历中内联 Metadata 过滤

当前 `SearchWithFilterContext` 的 `isExcluded` 回调每次都需要调用 `CheckVisibility`（涉及 storage 层锁）。如果 HNSW 搜索时能将 metadata 预加载到节点上，可在 HNSW 内部完成过滤，避免跨层回调开销。

```go
// 建议：HNSW 节点携带轻量 metadata
type Node struct {
    Vector   []float32
    Metadata map[string]interface{} // 或 string 类型的过滤字段
}
```

---

## 维护说明

本文档中的改进事项**不应在 memory 包内修复**——它们是 Vego 核心层的架构问题。当 Vego 层实现对应功能后，memory 包应：

1. 移除相关的 workaround 代码
2. 更新本文档，标记为"已解决"
3. 如有 breaking change，发布 migration 指南
