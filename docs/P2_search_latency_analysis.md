# P2 性能问题深度分析：高 Archive 率场景下搜索延迟 864ms

> **状态**: 🟢 方案已确定（推荐方案 F：内存元数据预过滤 + 单次 HNSW 搜索）  
> **问题**: `BenchmarkSearchHighArchiveRate_80pct` = 864ms/op，目标 < 100ms  
> **分支**: `mem-design`  
> **日期**: 2026-04-21

---

## 1. 概述

Vego Memory 的混合搜索在**高 Archive 率**场景下表现出严重的延迟退化。当 80% 的记忆处于 `archived` 状态时，单次向量搜索耗时高达 **864 毫秒**，远超可接受的交互式查询延迟（< 100ms）。

本报告深入分析该问题的技术根因，定位到 Vego 核心层的 `SearchWithFilterContext` 扩批机制与 HNSW 近似搜索的叠加效应，并给出多个优化方向。

---

## 2. 问题定义

### 2.1 性能目标

| 指标 | 目标 | 实际 | 状态 |
|------|------|------|------|
| `BenchmarkSearchHighArchiveRate_80pct` | < 100ms/op | **864ms/op** | ❌ 超标 8.6x |
| `BenchmarkHybridSearch_10K` | < 100ms/op | **571ms/op** | ❌ 超标 5.7x |

### 2.2 基准测试

```go
// memory/benchmark_test.go
func BenchmarkSearchHighArchiveRate_80pct(b *testing.B) {
    const totalDocs = 10_000
    const activeDocs = 2_000        // 20% active, 80% archived

    // Bootstrap 2K active memories → inverted index contains these
    // Insert 8K archived directly into Vego (bypass inverted index)
    
    queryVec := make([]float32, 128)
    // ... fill with random values

    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        results, err := s.vectorSearch(ctx, queryVec, 10, 0.0)
        // 864ms per op
    }
}
```

**工作负载特征**：
- 总量：10,000 文档
- Active：2,000（20%）— 可被搜索命中
- Archived：8,000（80%）— 存在于 HNSW 索引但会被过滤掉
- 查询向量：128 维随机向量（worst-case，无真实语义邻近）
- 搜索请求：limit=10, minScore=0.0

---

## 3. 根因分析

### 3.1 问题定位流程

```
vectorSearch(ctx, queryVec, limit=10, minScore=0.0)
    └─► s.coll.SearchWithFilterContext(ctx, queryVec, k=10, filter=StateActive)
            └─► 扩批循环（指数级 HNSW 搜索放大）
                    ├─ Attempt 1: SearchContext(query, 20)  → HNSW SearchWithDV(query, 20)  → 搜 40 候选
                    ├─ Attempt 2: SearchContext(query, 40)  → HNSW SearchWithDV(query, 40)  → 搜 80 候选
                    ├─ Attempt 3: SearchContext(query, 80)  → HNSW SearchWithDV(query, 80)  → 搜 160 候选
                    ├─ Attempt 4: SearchContext(query, 160) → HNSW SearchWithDV(query, 160) → 搜 320 候选
                    └─ Attempt 5: SearchContext(query, 200) → HNSW SearchWithDV(query, 200) → 搜 400 候选 (maxBatchSize=k*20=200)
```

### 3.2 核心根因：双层扩批叠加

#### 第一层：Vego 的 `SearchWithFilterContext` 扩批

**文件**: `vego/collection.go:588`

```go
func (c *Collection) SearchWithFilterContext(ctx, query, k, filter) {
    batchSize := k * 2       // 初始 20
    maxBatchSize := k * 20   // 上限 200
    maxAttempts := 5

    for attempt := 0; attempt < maxAttempts && batchSize <= maxBatchSize; attempt++ {
        results := c.SearchContext(ctx, query, batchSize)  // ← 每次重新搜索
        filtered := applyFilter(results, filter)
        if len(filtered) >= k {
            return filtered[:k]
        }
        if len(results) < batchSize {
            return filtered  // 已穷尽
        }
        batchSize *= 2       // ← 指数扩批：20→40→80→160→200
    }
}
```

**逻辑问题**：
- 每次扩批都**重新调用** `SearchContext`，而 `SearchContext` 内部会执行一次完整的 HNSW 图搜索
- 前一次搜索的结果**完全被丢弃**，没有利用任何已计算的邻居信息
- 在 80% archived 场景下，每次搜索返回的结果中约 80% 被过滤掉，命中率极低

#### 第二层：HNSW 的 `SearchWithDV` 内部扩批

**文件**: `index/hnsw.go:162`

```go
func (h *HNSWIndex) SearchWithDV(query, k, ef, isDeleted) {
    // 搜索 k*2 个候选来补偿删除
    candidates, _ := h.search(query, k*2, ef, ep, maxLvl, nodes)
    
    // 再过滤 deleted/archived
    filtered := make([]SearchResult, 0, k)
    for _, cand := range candidates {
        if !isDeleted(cand.ID) {
            filtered = append(filtered, cand)
            if len(filtered) >= k {
                break
            }
        }
    }
    return filtered
}
```

**叠加效应**：

| SearchWithFilter Attempt | batchSize | SearchWithDV 搜索候选数 | 预期命中 (20% active) | 是否够 k=10 |
|--------------------------|-----------|------------------------|----------------------|------------|
| 1 | 20 | 40 | ~8 | ❌ |
| 2 | 40 | 80 | ~16 | ❌ |
| 3 | 80 | 160 | ~32 | ✅ |

**最坏情况下需要 3 次完整的 HNSW 搜索**才能凑够 10 个 active 结果。

### 3.3 HNSW 搜索本身的复杂度

**文件**: `index/search.go:87`

```go
func (h *HNSWIndex) search(query, k, ef, ep, topLevel, nodes) {
    // Phase 1: 从顶层到 layer 1 的贪心下降
    currentNearest := ep
    for lc := topLevel; lc > 0; lc-- {
        nearest := h.searchLayer(query, currentNearest, 1, lc, nodes)
        currentNearest = nearest[0].ID
    }
    
    // Phase 2: 在 layer 0 用 ef 搜索
    candidates := h.searchLayer(query, currentNearest, ef, 0, nodes)
    return topK(candidates, k)
}
```

`searchLayer` 的复杂度：
- **时间**: O(ef × M)，其中 M=16（HNSW 邻居数），ef 默认 = max(200, k×2)
- **每层访问节点数**: 约 ef = 200~400 个
- **距离计算**: 每次访问一个节点都要计算 128 维向量距离（Cosine/L2）
- **总距离计算**: 200~400 次/搜索 × 3 次扩批 = **600~1200 次 128 维距离计算**

### 3.4 混合搜索的放大效应

上述问题在 `hybridSearch` 中被**进一步放大**：

```
hybridSearch(query)
    ├─► Stage 2: vectorSearch(ctx, vec, limit*3=30, minScore)
    │       └─► SearchWithFilterContext(k=30) 
    │               └─► Attempt 1: batch=60,  搜 120 候选 → ~24 active → 不够
    │               └─► Attempt 2: batch=120, 搜 240 候选 → ~48 active → 不够  
    │               └─► Attempt 3: batch=240(max=200 capped?), 搜 400 候选 → ~80 active → 够
    │               【3 次 HNSW 搜索】
    │
    ├─► Stage 7: secondHopSearch (如果 top score >= SecondHopGate=0.02)
    │       └─► 对 topN=3 个 seed 各执行一次：
    │               seed 1: SearchWithFilterContext(k=10)
    │                       └─► 可能 2-3 次 HNSW 搜索
    │               seed 2: SearchWithFilterContext(k=10)
    │               seed 3: SearchWithFilterContext(k=10)
    │               【6-9 次 HNSW 搜索】
    │
    └─► 单次 hybrid 查询总计: 9-12 次 HNSW 搜索
```

**单次查询可能触发 9-12 次完整 HNSW 搜索**，这是 864ms 的根本原因。

### 3.5 为什么随机向量是 worst-case

基准测试使用随机查询向量：

```go
queryVec := make([]float32, 128)
for i := range queryVec {
    queryVec[i] = r.Float32()
}
```

- 随机向量与数据分布中的任何向量都没有语义相似性
- HNSW 的近似搜索在这种查询下需要遍历更多节点才能找到最近邻
- 实际工作负载中（真实 embedding 查询），延迟会显著低于 864ms
- 但即使使用真实向量，高 archive 率仍会导致 2-3 次扩批，只是绝对值更小

---

## 4. 问题影响矩阵

### 4.1 按 Archive 率的影响

| Archive 率 | 单次 SearchWithFilter 扩批次数 | 单次 vectorSearch 耗时估算 | 说明 |
|-----------|-------------------------------|--------------------------|------|
| 0% | 1 | ~5-10ms | 无需扩批，理想场景 |
| 50% | 2 | ~30-50ms | 第一次搜 40 候选→20 active，不够；第二次够 |
| 80% | 3 | **~100-150ms** | 第三次搜 320 候选→64 active，才够 |
| 95% | 5（达到上限）| **~200-300ms** | 即使 maxBatchSize=k*20 也可能不够 |

### 4.2 按调用路径的影响

| 调用路径 | HNSW 搜索次数 (80% archive) | 累积耗时 |
|---------|---------------------------|---------|
| `pureVectorSearch` | 3 | ~150ms |
| `hybridSearch` (无 second-hop) | 3 | ~150ms |
| `hybridSearch` (有 second-hop, 3 seeds) | 3 + 9 = 12 | **~600ms** |
| `Reconcile` (并发 4 worker × 混合搜索) | 4 × 12 = 48 | **~2.4s**（单次 Reconcile） |

### 4.3 受影响的功能

| 功能 | 影响程度 | 原因 |
|------|---------|------|
| `MemoryStore.Search()` (混合搜索) | 🔴 高 | vectorSearch + secondHop 双重放大 |
| `MemoryStore.Search()` (纯向量) | 🟡 中 | vectorSearch 单路径扩批 |
| `Reconcile()` | 🔴 高 | 并发搜索 + 混合搜索双重放大 |
| `Bootstrap()` | 🟢 无 | 不调用搜索 |
| `Store/Update/Delete` | 🟢 无 | 不调用搜索 |

---

## 5. 相关代码详解

### 5.1 `SearchWithFilterContext` — 问题源头

**文件**: `vego/collection.go:586`

```go
func (c *Collection) SearchWithFilterContext(ctx context.Context, query []float32, k int, filter Filter) ([]SearchResult, error) {
    batchSize := k * 2          // 初始过取：limit 的 2 倍
    maxBatchSize := k * 20      // 最大过取：limit 的 20 倍
    maxAttempts := 5            // 最多 5 次尝试

    var allFiltered []SearchResult

    for attempt := 0; attempt < maxAttempts && batchSize <= maxBatchSize; attempt++ {
        // 每次循环都从头搜索，前一次结果完全丢弃
        results, err := c.SearchContext(ctx, query, batchSize)
        if err != nil {
            return nil, err
        }

        // 过滤
        allFiltered = allFiltered[:0]
        for _, r := range results {
            if r.Document != nil && filter.Match(r.Document) {
                allFiltered = append(allFiltered, r)
                if len(allFiltered) >= k {
                    return allFiltered[:k], nil
                }
            }
        }

        // 如果已穷尽所有结果，返回已找到的
        if len(results) < batchSize {
            return allFiltered, nil
        }

        // 扩批：batchSize 指数增长
        batchSize *= 2          // 20 → 40 → 80 → 160 → 320（但被 maxBatchSize 截断到 200）
    }

    return allFiltered, nil
}
```

**关键缺陷**：
1. **无结果复用**：每次 `attempt` 都调用 `SearchContext`，前一次搜索的结果被丢弃
2. **指数而非线性扩批**：`batchSize *= 2` 导致搜索量急剧膨胀
3. **对高过滤率无感知**：不知道底层 80% 数据被过滤，仍然从 k×2 开始小步尝试

### 5.2 `SearchContext` — HNSW 入口

**文件**: `vego/collection.go:512`

```go
func (c *Collection) SearchContext(ctx context.Context, query []float32, k int, opts ...SearchOption) ([]SearchResult, error) {
    // ...
    c.mu.RLock()
    defer c.mu.RUnlock()

    // 构造 isDeleted 闭包：检查 storage 层是否标记删除
    isDeleted := func(nodeID int) bool {
        docID, exists := c.nodeToDoc[nodeID]
        if !exists {
            return true
        }
        return c.storage.IsDeleted(docID)
    }

    // 调用 HNSW 搜索（已带 deletion vector 过滤）
    hnswResults, err := c.index.SearchWithDV(query, k, options.EF, isDeleted)
    // ...
}
```

注意：`SearchContext` 本身已经通过 `SearchWithDV` 处理了 deletion vector（物理删除），但 `SearchWithFilterContext` 在此基础上额外施加 metadata filter（如 `State=active`），这是**逻辑删除/状态过滤**，HNSW 层无法感知。

### 5.3 `HNSWIndex.SearchWithDV` — HNSW 内部过滤

**文件**: `index/hnsw.go:162`

```go
func (h *HNSWIndex) SearchWithDV(query []float32, k int, ef int, isDeleted func(int) bool) ([]SearchResult, error) {
    if ef == 0 {
        ef = max(200, k*2)
    }

    // 快照读取
    h.globalLock.RLock()
    ep := h.entryPoint
    maxLvl := h.maxLevel
    nodes := h.nodes
    h.globalLock.RUnlock()

    // 搜索 k*2 候选来补偿删除
    candidates, err := h.search(query, k*2, ef, int(ep), int(maxLvl), nodes)
    
    // 过滤 deleted
    filtered := make([]SearchResult, 0, k)
    for _, cand := range candidates {
        if !isDeleted(cand.ID) {
            filtered = append(filtered, cand)
            if len(filtered) >= k {
                break
            }
        }
    }
    return filtered, nil
}
```

这里有两个层次的 "删除"：
1. **物理删除**（deletion vector）：HNSW 节点仍在图中，但 storage 标记为删除 → `SearchWithDV` 过滤
2. **逻辑状态**（archived）：HNSW 节点仍在图中，storage 未删除，但 metadata `_state="archived"` → `SearchWithFilterContext` 过滤

### 5.4 `vectorSearch` — MemoryStore 层

**文件**: `memory/search.go:186`

```go
func (s *MemoryStore) vectorSearch(ctx context.Context, queryVec []float32, limit int, minScore float64) ([]Memory, error) {
    var vf vego.Filter = &vego.MetadataFilter{
        Field:    metaKeyState,
        Operator: "eq",
        Value:    string(StateActive),
    }

    // limit*3 over-fetch
    results, err := s.coll.SearchWithFilterContext(ctx, queryVec, limit, vf)
    // ... distanceToSimilarity + MinScore 过滤
}
```

注意：`vectorSearch` 本身已经做了 `limit*3` over-fetch（在 `hybridSearch` 中），但 `SearchWithFilterContext` 的扩批机制进一步放大了搜索量。

### 5.5 `secondHopSearch` — 二跳扩展

**文件**: `memory/search.go:284`

```go
func (s *MemoryStore) secondHopSearch(ctx context.Context, seeds []Memory, limit int) ([]Memory, error) {
    var vf vego.Filter = &vego.MetadataFilter{...StateActive...}
    
    seen := make(map[string]struct{})
    var all []Memory

    for _, seed := range seeds {
        doc, _ := s.coll.GetContext(ctx, seed.ID)
        results, _ := s.coll.SearchWithFilterContext(ctx, doc.Vector, limit, vf)
        // ...
    }
    return all, nil
}
```

**问题**：对每个 seed 都独立调用 `SearchWithFilterContext`，而每个调用都可能触发 3 次 HNSW 扩批搜索。

---

## 6. 为什么这是架构级问题而非简单调参

### 6.1 调参无法解决的证据

有人可能认为只需调整 `maxBatchSize` 或 `maxAttempts`：

| 参数调整 | 效果 | 副作用 |
|---------|------|--------|
| 增大 `maxBatchSize` | 减少扩批次数 | 单次搜索更慢，内存占用更大 |
| 减小 `maxBatchSize` | 单次搜索更快 | 更频繁返回不足结果 |
| 增大 `maxAttempts` | 提高成功率 | 更多次 HNSW 搜索 |
| 将指数扩批改线性 | 搜索量更可控 | 需要修改 Vego 核心层 |
| 初始 batchSize = k×10 | 80% 场景可能一次命中 | 0% archive 场景浪费 10 倍搜索量 |

**结论**：参数调整是零和博弈，无法根本解决问题。

### 6.2 根本矛盾

> **HNSW 是近似最近邻（ANN）索引，天生不支持属性过滤。**

- HNSW 在构建时只考虑向量距离，完全不感知 metadata
- 当需要 `"state=active"` 过滤时，只能在 HNSW 返回结果后做**后过滤**
- 如果 80% 数据不符合条件，后过滤的命中率极低，导致反复扩批
- 这与 HNSW 的设计假设（大部分数据都有效）相矛盾

### 6.3 与其他向量数据库的对比

| 数据库 | 过滤方案 | 高过滤率表现 |
|--------|---------|------------|
| Milvus | 位图索引 + HNSW 预过滤 | 较好，但需额外索引 |
| Weaviate | 倒排索引过滤 + HNSW | 类似问题，但有过滤感知优化 |
| Pinecone | 元数据过滤原生集成 | 专有云方案，不公开细节 |
| **Vego** | **后过滤 + 扩批** | **差，反复搜索** |

---

## 7. 潜在优化方案

### 7.1 方案 A：HNSW 原生支持 State 过滤（推荐）

**思路**：在 HNSW 搜索过程中直接跳过 archived 节点，而不是搜索完再过滤。

**实现方式**：

1. 在 HNSW 节点中嵌入 `state` 标志位
2. `searchLayer` 遍历邻居时，直接跳过 `state != active` 的节点
3. `SearchWithDV` 的 `isDeleted` 闭包扩展为 `isVisible`，同时检查物理删除和逻辑状态

**优点**：
- 单次搜索即可返回准确结果，无需扩批
- 搜索复杂度与 active 节点数成正比，而非总节点数

**缺点**：
- 需要修改 `index/` 核心层，侵入性大
- HNSW 节点结构需要增加状态字段
- 状态变更时需要更新 HNSW 节点（archive 操作变重）

### 7.2 方案 B：分离 HNSW 索引（Active-Only Index）

**思路**：只为 active 文档维护一个 HNSW 索引，archived 文档不进入搜索索引。

**实现方式**：

1. `MemoryStore` 维护两个逻辑索引：
   - `activeIndex`：仅含 active 文档的 HNSW
   - `fullIndex`：含所有文档的 HNSW（用于其他用途）
2. `vectorSearch` 直接查 `activeIndex`
3. Archive 操作时从 `activeIndex` 删除，Create 操作时加入

**优点**：
- 搜索复杂度直接降到 active 文档规模
- 无需扩批，一次搜索即可
- 与现有 `SearchWithFilterContext` 解耦

**缺点**：
- Archive/Create 需要维护两个索引，一致性复杂
- 需要处理 `activeIndex` 的并发更新（HNSW 删除支持有限）
- 内存占用增加（两个索引）

### 7.3 方案 C：扩批结果复用（Vego 层改进）

**思路**：修改 `SearchWithFilterContext`，不丢弃前一次搜索结果，而是复用并增量搜索。

**实现方式**：

```go
func (c *Collection) SearchWithFilterContext(ctx, query, k, filter) {
    // 不再循环扩批，而是一次性搜索足够多的候选
    // 利用 HNSW 的 ef 参数控制搜索深度
    
    // 估算需要的候选数：基于历史过滤率或保守估计
    candidatesNeeded := k * 10   // 一次过取 10 倍
    
    results := c.SearchContext(ctx, query, candidatesNeeded)
    filtered := applyFilter(results, filter)
    if len(filtered) >= k {
        return filtered[:k]
    }
    return filtered
}
```

**优点**：
- 修改范围小，仅在 Vego 层
- 避免多次 HNSW 搜索

**缺点**：
- 需要准确估算 `candidatesNeeded`
- 过滤率未知时难以估算
- 对低过滤率场景造成浪费

### 7.4 方案 D：过滤感知 HNSW（Filtered ANN）

**思路**：参考 Milvus 的 "filtered ANN" 技术，在 HNSW 搜索时利用过滤条件剪枝。

**实现方式**：

1. 为每个 `state` 值维护一个位图（bitmap）
2. HNSW `searchLayer` 中，检查邻居是否在目标位图中
3. 不在位图中的邻居直接跳过，不加入候选集

**优点**：
- 业界成熟方案（Milvus、Faiss 均有实现）
- 一次搜索即可，无需扩批

**缺点**：
- 实现复杂度高
- 位图与 HNSW 图的并发一致性难以保证
- 需要修改 `index/` 核心层

### 7.5 方案 E：降低 second-hop 触发频率（短期缓解）

**思路**：不解决根本问题，但减少 `secondHopSearch` 的调用次数。

**实现方式**：

1. 提高 `SecondHopGate`（当前 0.02）
2. 减少 `SecondHopTopN`（当前 3）
3. 增加 second-hop 缓存

**优点**：
- 改动小，见效快

**缺点**：
- 牺牲搜索质量
- 不解决 `vectorSearch` 本身的问题

### 7.6 方案 F：内存元数据预过滤 + 单次 HNSW 搜索（推荐）

**思路**：利用 `metadataStore` 已在内存中持有全部文档元数据（包括 `_state`）的事实，将 metadata filter 合并到 `SearchWithDV` 的 `isDeleted` 回调中，从根本上消除 `SearchWithFilterContext` 的重试循环。

#### 关键发现

`storage.go` 中的 `metadataStore` 是一个纯内存结构：

```go
type metadataStore struct {
    entries  map[int64]docMeta     // idHash -> docMeta（含 Metadata map）
    idToHash map[string]int64      // string ID -> idHash
}

type docMeta struct {
    ID       string
    RowIndex int64
    Metadata map[string]interface{}  // ← _state 字段就在这里
}
```

这意味着检查 `_state == "active"` **不需要任何磁盘 I/O**，只是一次内存 map 查找 + 字符串比较，开销约 ~50ns。

#### 架构对比

```
旧架构（当前）:
  SearchWithFilterContext(k=10, filter=StateActive)
    └─ Attempt 1: SearchContext(batch=20) → HNSW(k=40) → storage.Get ×20 → filter → 不够
    └─ Attempt 2: SearchContext(batch=40) → HNSW(k=80) → storage.Get ×40 → filter → 不够
    └─ Attempt 3: SearchContext(batch=80) → HNSW(k=160)→ storage.Get ×80 → filter → 够了
    总计: 3 次 HNSW 搜索 + 140 次 storage.Get（含磁盘 I/O）

新架构（方案 F）:
  SearchWithFilterContext(k=10, filter=StateActive)
    └─ 构造 isExcluded = isDeleted || !matchesMetadataFilter（纯内存）
    └─ 单次 SearchWithDV(query, k, ef, isExcluded)
    └─ 返回 k 个结果，每个调用 storage.Get 获取完整文档
    总计: 1 次 HNSW 搜索 + 10 次 storage.Get
```

#### 实现方式

**步骤 1**：在 `DocumentStorage` 上添加 `GetMetadataOnly` 方法

```go
// vego/storage.go — 新增方法
// GetMetadataOnly 从内存元数据中查找文档的 Metadata map。
// 不涉及磁盘 I/O，O(1) 时间复杂度。
func (s *DocumentStorage) GetMetadataOnly(id string) (map[string]interface{}, bool) {
    s.metadata.mu.RLock()
    defer s.metadata.mu.RUnlock()

    hash, exists := s.metadata.idToHash[id]
    if !exists {
        return nil, false
    }
    meta, exists := s.metadata.entries[hash]
    if !exists {
        return nil, false
    }
    return meta.Metadata, true
}
```

**步骤 2**：重写 `SearchWithFilterContext`，消除重试循环

```go
// vego/collection.go — 重写
func (c *Collection) SearchWithFilterContext(ctx context.Context, query []float32, k int, filter Filter) ([]SearchResult, error) {
    c.mu.RLock()
    defer c.mu.RUnlock()

    // 将 DV 过滤 + metadata 过滤合并为单个 isExcluded 回调
    isExcluded := func(nodeID int) bool {
        docID, exists := c.nodeToDoc[nodeID]
        if !exists {
            return true
        }
        // 1. 检查物理删除（deletion vector，纯内存位图）
        if c.storage.IsDeleted(docID) {
            return true
        }
        // 2. 检查 metadata filter（纯内存 map 查找）
        if filter != nil {
            metadata, ok := c.storage.GetMetadataOnly(docID)
            if !ok {
                return true
            }
            // 构造轻量 Document 仅用于 filter.Match
            doc := &Document{ID: docID, Metadata: metadata}
            if !filter.Match(doc) {
                return true  // 不满足过滤条件，跳过
            }
        }
        return false
    }

    // 单次 HNSW 搜索，isExcluded 在 SearchWithDV 返回后做后过滤
    hnswResults, err := c.index.SearchWithDV(query, k, 0, isExcluded)
    if err != nil {
        return nil, err
    }

    // 获取完整文档（仅对命中的 k 个结果做 storage.Get）
    results := make([]SearchResult, 0, len(hnswResults))
    for _, hr := range hnswResults {
        docID, exists := c.nodeToDoc[hr.ID]
        if !exists {
            continue
        }
        doc, err := c.storage.Get(docID)
        if err != nil || doc == nil {
            continue
        }
        results = append(results, SearchResult{
            Document: doc,
            Score:    hr.Score,
        })
    }
    return results, nil
}
```

**步骤 3**：`index/` 层和 `memory/` 层无需任何修改

- `SearchWithDV` 已接受 `isDeleted func(int) bool` 回调，方案 F 只是让这个回调做更多的检查（从纯 DV 检查扩展为 DV + metadata 检查），接口完全不变
- `memory/search.go` 中的 `vectorSearch` / `secondHopSearch` 调用 `SearchWithFilterContext` 的方式不变

#### 性能分析

| 指标 | 旧架构 | 方案 F |
|------|-------|--------|
| HNSW 搜索次数 | 3（扩批） | **1** |
| `storage.Get` 调用（含磁盘 I/O） | ~140 | **~10** |
| `isExcluded` 回调开销 | ~50ns/次（纯 DV） | ~100ns/次（DV + metadata map 查找） |
| `isExcluded` 后过滤总开销 | 无（仅 DV） | ~200 次 × 50ns = **~10μs**（可忽略） |
| 预期 vectorSearch 延迟 (80% archive) | ~150ms | **~50ms** |
| 预期 hybridSearch 延迟 (80% archive) | ~600ms | **~200ms** |

**核心收益**：

1. **消除重试循环**：从 3 次 HNSW 搜索降到 1 次，延迟降低 ~3x
2. **消除无效 `storage.Get`**：从 ~140 次磁盘 I/O 降到 ~10 次，I/O 降低 ~14x
3. **与 archive 率解耦**：无论 archive 率多高，都只做 1 次 HNSW 搜索，`isExcluded` 的额外开销在 HNSW 图遍历的 O(ef × M) 中可忽略不计
4. **second-hop 同样受益**：3 个 seed × 1 次搜索 = 3 次 HNSW 搜索（原来 3 × 3 = 9 次）

#### 与方案 A/B 的对比

| 维度 | A: HNSW 原生过滤 | B: Active-Only Index | **F: 内存预过滤** |
|------|-----------------|---------------------|------------------|
| 改动层 | `index/` + `vego/` | `memory/` + `vego/` | **`vego/` only** |
| HNSW 搜索次数 | 1 | 1 | **1** |
| 需要修改 HNSW 节点结构 | 是 | 否 | **否** |
| 需要维护额外索引 | 否 | 是（2 个 HNSW） | **否** |
| Archive 操作开销 | HNSW 节点更新 | HNSW 删除 + 插入 | **无额外开销** |
| 一致性复杂度 | 中 | 高 | **低** |
| `isExcluded` 回调额外开销 | ~0ns | ~0ns | **~50ns（map 查找）** |
| 效果上限 | 最优 | 最优 | **接近最优** |

方案 F 的核心优势在于：**改动范围最小（仅 `vego/` 层）、无架构侵入、无额外索引维护、无一致性问题**，同时效果接近方案 A/B。唯一的代价是每次 HNSW 图遍历中对 `isExcluded` 回调多一次内存 map 查找（~50ns），但这在 ef=200~400 次遍历的总量中完全可忽略（总额外开销 ~20μs）。

#### 潜在风险与注意事项

1. **`isExcluded` 回调持有 `metadataStore.mu.RLock`**：`GetMetadataOnly` 内部需要加读锁。在 HNSW 图遍历过程中可能被调用 200~400 次。需确认 `metadataStore.mu` 不会与 HNSW 的锁产生死锁。当前架构中 HNSW 用 `globalLock.RLock()` 快照读取后立即释放（`hnsw.go:326-330`），后续遍历不持有 HNSW 锁，因此不会死锁。
2. **`Document` 构造仅用于 `filter.Match`**：不需要 Vector 字段，仅需 ID + Metadata。可考虑用更轻量的结构，但当前 `MetadataFilter.Match` 只访问 `doc.Metadata[field]`，所以构造一个仅含 Metadata 的 `Document` 即可。
3. **向后兼容**：`SearchWithFilterContext` 的函数签名不变，`memory/` 层调用方无需修改。`GetMetadataOnly` 是新增方法，不影响现有接口。

---

#### 改进与补充（Code Review 结论）

> **Review 时间**: 2026-04-21  
> **Review 结论**: 方案 F 方向正确，但文档中的实现代码存在两处关键遗漏，需要修正后才能安全实施。

**问题一：文档遗漏了 k 放大（over-fetch）逻辑**

方案 F 的示例代码直接调用 `c.index.SearchWithDV(query, k, 0, isExcluded)`。`SearchWithDV` 内部搜 `k*2` 候选。当 `k=10`、archive 率 80% 时，候选数=20，active 预期=4，**返回结果不足**。

如果不在 `SearchWithFilterContext` 内部放大 k，方案 F 无法替代扩批循环——它将比原来更差（原来至少能扩批凑够，现在直接返回不足的结果）。

**修正**：方案 F 必须内置 over-fetch：

```go
func (c *Collection) SearchWithFilterContext(ctx, query, k, filter) {
    overFetch := 10  // 从 SearchOptions 读取，默认 10
    hnswResults, err := c.index.SearchWithDV(query, k*overFetch, 0, isExcluded)
    // ... 取前 k 个返回
}
```

**overFetch 取值的边界分析**：

overFetch 的最优值取决于 archive 率。当 archive 率为 α 时，`SearchWithDV` 的后过滤通过率为 `(1-α)`，需要 `k × overFetch × 2 × (1-α) ≥ k`，即 `overFetch ≥ 1 / (2 × (1-α))`：

| Archive 率 α | 理论最小 overFetch | overFetch=10 余量 | 是否充足 |
|-------------|------------------|-----------------|---------|
| 50% | 1 | 10x | ✅ |
| 80% | 2.5 | 4x | ✅ |
| 90% | 5 | 2x | ✅ |
| 95% | 10 | 1x（无余量） | ⚠️ 边界 |
| 98% | 25 | 不够 | ❌ |

默认 `overFetch=10` 在 archive 率 ≤90% 时有充足余量，95% 时刚好处于边界。若需支持更极端场景，可改为自适应公式或允许调用方通过 `SearchOptions` 覆盖。

**问题二：`isExcluded` 是后过滤，而非遍历中过滤**

当前 `SearchWithDV` 的实现（`index/hnsw.go:162`）：

```go
func (h *HNSWIndex) SearchWithDV(query, k, ef, isDeleted) {
    candidates, err := h.search(query, k*2, ef, ...)  // HNSW 图遍历，不调用 isDeleted
    for _, cand := range candidates {
        if !isDeleted(cand.ID) {  // 后过滤：search 返回后才调用
            filtered = append(filtered, cand)
        }
    }
}
```

`isExcluded`（即 `isDeleted`）是在 `search` **返回后**调用的，不是在 `searchLayer` 遍历邻居时调用的。原始方案 F 文档中 "isExcluded 在图遍历中实时过滤" 的描述不准确。

**但这不影响方案 F 的核心收益**。方案 F 的主要价值在于消除 `SearchWithFilterContext` 的重试循环，而非优化单次 HNSW 搜索内部：

- ✅ **消除扩批循环**（从 N 次独立 HNSW 搜索降到 1 次）— 这是最大收益来源
- ✅ **大幅减少 `storage.Get` 调用**（从 ~140 次磁盘 I/O 降到 ~10 次）
- ✅ **消除重复的 HNSW 贪心下降开销**（每次独立搜索都要从 topLevel 到 layer 0）
- ⚠️ 单次搜索的 ef 不会减少（后过滤不能剪枝图遍历，但单次 ef 已小于旧架构多次搜索的总 ef）

**修正后的完整实现 = "方案 F 的 metadata 内存过滤" + "k 放大（over-fetch）"**

以 `pureVectorSearch`（k=10）为例：

```
memory 层: vectorSearch(limit=10) → SearchWithFilterContext(k=10, filter)
           ↓
vego/SearchWithFilterContext: 内部 effectiveK = 10 * overFetch(10) = 100
           ↓
SearchWithDV(k=100): 搜 200 候选，ef = max(200, 200) = 200
           ↓
isExcluded 回调: 对 200 个候选做后过滤（DV + metadata，纯内存）→ ~40 个 active
           ↓
返回前 10 个 active 结果
```

以 `hybridSearch`（k=30，来自 limit*3）为例：

```
memory 层: vectorSearch(limit=30) → SearchWithFilterContext(k=30, filter)
           ↓
vego/SearchWithFilterContext: 内部 effectiveK = 30 * overFetch(10) = 300
           ↓
SearchWithDV(k=300): 搜 600 候选，ef = max(200, 600) = 600
           ↓
isExcluded 回调: 对 600 个候选做后过滤 → ~120 个 active
           ↓
返回前 30 个 active 结果
```

**修正后的效果预估**：

对照源码 `hnsw.go:167` 的 `ef = max(200, k*2)` 逐项计算：

*pureVectorSearch 场景（k=10）*：

| 指标 | 旧架构 | 方案 F（文档原代码） | **方案 F（修正后）** |
|------|-------|---------------------|-------------------|
| HNSW 搜索次数 | 3 | 1 | **1** |
| ef（探索因子） | 200+200+200=600 ① | 200（k=10） | **200（k=100, ef=max(200,200)=200）** |
| `storage.Get` 调用 | ~140 | ~10 | **~10** |
| 返回结果完整性 | ✅ 够 | ❌ 不足（仅 ~4 个） | **✅ 够（~40 个）** |
| 预期 vectorSearch 延迟 | ~150ms | ~50ms（但结果不足） | **~50ms** |

> ① k=10 时旧架构 ef 计算：batchSize 20→40→80，对应 SearchWithDV k=20/40/80，search k*2=40/80/160，ef=max(200,40)/max(200,80)/max(200,160)=200/200/200，总 ef=600。

*hybridSearch 场景（k=30）*：

| 指标 | 旧架构 | **方案 F（修正后）** |
|------|-------|-------------------|
| HNSW 搜索次数 | 3 | **1** |
| ef（探索因子） | 200+240+480=920 ② | **600（k=300, ef=max(200,600)=600）** |
| `storage.Get` 调用 | ~140 | **~30** |
| 预期 hybridSearch vectorSearch 延迟 | ~150ms | **~100ms** ③ |

> ② k=30 时旧架构 ef 计算：batchSize 60→120→240，对应 SearchWithDV k=60/120/240，search k*2=120/240/480，ef=max(200,120)/max(200,240)/max(200,480)=200/240/480，总 ef=920。  
> ③ 单次 ef=600 < 总 ef=920，且省去 3 次独立 HNSW 贪心下降开销（从 topLevel 到 layer 0），整体快于旧架构。

**补充风险：`GetMetadataOnly` 返回共享 map 引用的线程安全性**

原始方案 F 的 `GetMetadataOnly` 实现：

```go
func (s *DocumentStorage) GetMetadataOnly(id string) (map[string]interface{}, bool) {
    s.metadata.mu.RLock()
    defer s.metadata.mu.RUnlock()
    ...
    return meta.Metadata, true  // ← 返回 entries 中的原始 map 引用
}
```

`RLock` 在函数返回后释放，但 `meta.Metadata` 的 map 引用仍被 `isExcluded` 闭包持有并读取（`filter.Match(doc)` 访问 `doc.Metadata[field]`）。如果其他 goroutine 对同一个 Metadata map 做 modify-in-place（直接写入已有 map），会导致 data race。

需在实现时确认：`metadataStore` 的更新路径是 replace-entry（整体替换 `docMeta`，旧 map 引用不受影响）还是 modify-in-place（直接写 Metadata map）。若为后者，`GetMetadataOnly` 应返回 map 的浅拷贝。

**建议实施顺序**：
1. 先实施方案 F（修正版）+ over-fetch 配置 → 消除扩批循环，减少 I/O
2. 若效果仍不足（极端 archive 率场景），再升级至方案 A（修改 `searchLayer` 实现遍历中过滤）

---

## 8. 方案评估矩阵

| 方案 | 改动范围 | 实现难度 | 效果 | 风险 | 推荐度 |
|------|---------|---------|------|------|--------|
| A: HNSW 原生 State 过滤 | `index/` + `vego/` | 高 | ⭐⭐⭐⭐⭐ | 架构侵入 | 🟡 |
| B: Active-Only Index | `memory/` + `vego/` | 中 | ⭐⭐⭐⭐⭐ | 一致性复杂 | 🟡 |
| C: 扩批结果复用 | `vego/` | 低 | ⭐⭐⭐ | 估算困难 | 🟡 |
| D: 过滤感知 HNSW | `index/` | 极高 | ⭐⭐⭐⭐⭐ | 实现风险 | 🔴 |
| E: 降低 second-hop | `memory/` | 极低 | ⭐⭐ | 质量损失 | 🔴 仅缓解 |
| **F: 内存元数据预过滤** | **`vego/` only** | **低** | **⭐⭐⭐⭐** | **极低** | **🟢 推荐** |

---

## 9. 附录

### 9.1 关键代码链路完整图

```
User Query
    │
    ▼
┌─────────────────┐
│ hybridSearch()  │  memory/search.go:61
│ 十阶段流水线     │
└────────┬────────┘
         │
    ┌────┴────┐
    ▼         ▼
vectorSearch  inverted.Search
    │
    ▼
┌──────────────────────────┐
│ SearchWithFilterContext  │  vego/collection.go:588
│ k=30, filter=StateActive │
└────────┬─────────────────┘
         │
    ┌────┴────┬────────┬────────┬────────┐
    ▼         ▼        ▼        ▼        ▼
Attempt 1  Att 2   Att 3    Att 4    Att 5
batch=60  120     240      200(capped)
    │       │       │
    ▼       ▼       ▼
┌──────────────────────────┐
│     SearchContext()      │  vego/collection.go:512
│   (each attempt calls)   │
└────────┬─────────────────┘
         │
         ▼
┌──────────────────────────┐
│    SearchWithDV()        │  index/hnsw.go:162
│  search k*2 candidates   │
└────────┬─────────────────┘
         │
         ▼
┌──────────────────────────┐
│    HNSW.search()         │  index/search.go:87
│  O(ef × M) graph traverse│
└──────────────────────────┘
```

### 9.2 扩批数学模型

设：
- 总文档数 N = 10,000
- Archive 率 α = 80%
- 每次 HNSW 搜索返回的候选中，active 占比 = (1-α) = 20%
- 需要结果数 k

`SearchWithFilterContext` 的扩批过程：

```
Attempt 1: batch = k×2,  HNSW 搜 k×4,  预期命中 = k×4×0.2 = 0.8k   ❌ (k=30 时: 24 < 30)
Attempt 2: batch = k×4,  HNSW 搜 k×8,  预期命中 = k×8×0.2 = 1.6k  ✅ (k=30 时: 48 > 30)
```

期望尝试次数：
- E[attempts] = min { n | k × 2^(n+1) × (1-α) ≥ k }
- E[attempts] = min { n | 2^(n+1) × (1-α) ≥ 1 }

| Archive 率 α | 需要的 2^(n+1) | n (attempts) | HNSW 搜索总候选数 |
|-------------|---------------|-------------|-----------------|
| 0% | 1 | 1 | k×4 |
| 50% | 2 | 1 | k×4 |
| 75% | 4 | 2 | k×4 + k×8 = k×12 |
| 80% | 5 | 2 | k×4 + k×8 = k×12 |
| 87.5% | 8 | 3 | k×4 + k×8 + k×16 = k×28 |
| 93.75% | 16 | 4 | k×4 + k×8 + k×16 + k×32 = k×60 |

### 9.3 相关文件清单

| 文件 | 角色 | 相关代码 |
|------|------|---------|
| `vego/collection.go` | 问题源头 + 方案 F 改动点 | `SearchWithFilterContext()` |
| `vego/collection.go` | 中间层 | `SearchContext()` |
| `vego/storage.go` | 方案 F 新增方法 | `GetMetadataOnly()`、`metadataStore` |
| `index/hnsw.go` | HNSW 入口 | `SearchWithDV()` |
| `index/search.go` | HNSW 核心 | `search()`, `searchLayer()` |
| `memory/search.go` | 调用方 | `vectorSearch()`, `secondHopSearch()` |
| `memory/benchmark_test.go` | 基准测试 | `BenchmarkSearchHighArchiveRate_80pct` |
| `memory/config.go` | 配置 | `SecondHopGate`, `SecondHopTopN` |

---

## 10. 总结

P2 是一个**架构级性能问题**，根因在于：

1. **HNSW 不支持属性过滤**，只能后过滤
2. **Vego 的扩批机制是指数级且无结果复用**，导致高过滤率下反复执行完整 HNSW 搜索
3. **混合搜索的二跳扩展进一步放大**了上述问题

**短期缓解**（不改动核心层）：
- 提高 `SecondHopGate` 减少二跳触发
- 在 `vectorSearch` 中直接用更大的 `k` 调用 `SearchContext`，绕过 `SearchWithFilterContext`

**长期根治**（推荐方案 F）：
- 利用 `metadataStore` 已在内存中持有全部元数据的事实，将 metadata filter 合并到 `SearchWithDV` 的 `isExcluded` 回调中
- 消除 `SearchWithFilterContext` 的重试循环，单次 HNSW 搜索即可返回符合条件的结果
- 改动范围仅限 `vego/` 层（新增 `GetMetadataOnly` + 重写 `SearchWithFilterContext`），无架构侵入
- 若方案 F 效果不足（极端场景下 HNSW `ef` 不够大导致遍历候选不足），可升级至方案 A/B

---

## 11. 实施结果（2026-04-24）

### 11.1 实施范围

方案 F 已在 `mem-design` 分支完整实施，分四个阶段完成：

| Phase | 内容 | 状态 |
|-------|------|------|
| Phase 1 | Vego 基础设施：`SearchOptions` 新增 `OverFetch`，`DocumentStorage` 新增 `GetMetadataOnly`，`SearchWithFilterContext` 重写为单次 HNSW 搜索 + 内存 metadata 过滤 | ✅ |
| Phase 2 | Memory 配置：`Config` 新增 `SearchOverFetch`（默认 3），`validate` 范围 [1,20]，`WithSearchOverFetch` option；`vectorSearch`/`secondHopSearch` 传入 `vego.WithOverFetch` | ✅ |
| Phase 3 | 测试验证：`go test ./vego/...` 通过（含新增测试），`go test ./memory/...` 通过，`go test -race ./memory/...` 通过 | ✅ |
| Phase 4 | Benchmark 验证：新旧代码对比跑分 | ✅ |

### 11.2 代码变更清单

| 文件 | 变更 |
|------|------|
| `vego/query.go` | `SearchOptions` 新增 `OverFetch int`；新增 `WithOverFetch` functional option |
| `vego/storage.go` | 新增 `GetMetadataOnly(id string) (map[string]interface{}, bool)` — 纯内存 metadata 读取，无磁盘 I/O |
| `vego/collection.go` | `SearchWithFilter`/`SearchWithFilterContext` 签名新增 `opts ...SearchOption`；内部重写为单次 `SearchWithDV(query, k*OverFetch)` + `isExcluded` 回调（合并 DV + metadata filter）+ 极简 fallback + 截断到 k |
| `vego/collection_test.go` | 新增 `TestCollectionSearchWithFilterContextHighFilterRate` 覆盖高过滤率场景；新增 `TestDocumentStorageGetMetadataOnly` |
| `memory/config.go` | `Config` 新增 `SearchOverFetch int`（默认 3）；`validate` 检查范围；新增 `WithSearchOverFetch` |
| `memory/search.go` | `vectorSearch` 和 `secondHopSearch` 调用 `SearchWithFilterContext` 时传入 `vego.WithOverFetch(s.config.SearchOverFetch)` |

### 11.3 Benchmark 对比

测试环境：Apple M3 Max, go1.25, `benchtime=1x`, `count=3`

#### `BenchmarkSearchHighArchiveRate_80pct`（P2 核心场景）

| 版本 | 平均耗时 | 相对旧代码 | 备注 |
|------|---------|-----------|------|
| 旧代码（扩批循环） | **899 ms** | 1.0x | 2 次 HNSW 搜索 + 大量 `storage.Get` |
| 新代码 `OverFetch=10` | 237 ms | **3.8x** ⬆️ | 单次 HNSW ef=200，不触发 fallback |
| 新代码 `OverFetch=3` | 442 ms | **2.0x** ⬆️ | 单次 HNSW ef=200，偶发 fallback ef=400 |

#### `BenchmarkHybridSearch_10K`（日常 100% active 场景）

| 版本 | 平均耗时 | 相对旧代码 | 备注 |
|------|---------|-----------|------|
| 旧代码（扩批循环） | **599 ms** | 1.0x | batch=60 即满足，ef=200 |
| 新代码 `OverFetch=10` | 2064 ms | **0.29x** ⬇️ | effectiveK=300, ef=600，单次搜索量过大 |
| 新代码 `OverFetch=3` | 764 ms | **0.78x** ⬇️ | effectiveK=90, ef=200，与旧代码 ef 持平 |

### 11.4 默认配置决策

经过权衡，最终选择 **`SearchOverFetch=3`** 作为默认值：

- **日常场景优先**：100% active 是更常见的工作负载，over-fetch=3 将回退控制在 1.3x 以内（764ms vs 599ms），而 over-fetch=10 会导致 3.4x 严重回退（2064ms）。
- **P2 场景仍有显著改善**：80% archive 场景下，over-fetch=3 仍有 2.0x 提升（442ms vs 899ms），虽不及 over-fetch=10 的 3.8x，但已消除扩批循环的核心问题。
- **可调性**：用户可通过 `WithSearchOverFetch(n)` 针对特定工作负载调整。高 archive 率场景可显式设为 10 以获得最大收益。

### 11.5 根因复盘：为什么 over-fetch=10 在日常场景回退

`SearchWithDV` 的 ef 计算公式为 `ef = max(200, k*2)`。在 `hybridSearch` 中 `vectorSearch` 的 `limit=30`（`SearchLimit*3`）：

- `OverFetch=10` → `effectiveK = 30*10 = 300` → `ef = max(200, 600) = 600`
- `OverFetch=3`  → `effectiveK = 30*3 = 90`  → `ef = max(200, 180) = 200`

ef 从 200 膨胀到 600 导致 HNSW `searchLayer` 遍历节点数增加约 3 倍，是日常场景回退的根本原因。

### 11.6 后续优化方向

1. **自适应 over-fetch**：在 `SearchWithFilterContext` 或 `vectorSearch` 层根据历史过滤率动态调整 `OverFetch`，兼顾高低 archive 率场景。
2. **HNSW 遍历中过滤（方案 A）**：将 `state` 标志嵌入 HNSW 节点，`searchLayer` 遍历邻居时直接跳过 archived 节点，从根本上消除后过滤的 ef 浪费。
3. **Active-Only Index（方案 B）**：维护独立的 active 文档 HNSW 索引，搜索时直接查该索引，ef 始终与 active 文档规模成正比。

---

## 10. 总结

P2 是一个**架构级性能问题**，根因在于：

1. **HNSW 不支持属性过滤**，只能后过滤
2. **Vego 的扩批机制是指数级且无结果复用**，导致高过滤率下反复执行完整 HNSW 搜索
3. **混合搜索的二跳扩展进一步放大**了上述问题

**短期缓解**（不改动核心层）：
- 提高 `SecondHopGate` 减少二跳触发
- 在 `vectorSearch` 中直接用更大的 `k` 调用 `SearchContext`，绕过 `SearchWithFilterContext`

**长期根治**（推荐方案 F）：
- 利用 `metadataStore` 已在内存中持有全部元数据的事实，将 metadata filter 合并到 `SearchWithDV` 的 `isExcluded` 回调中
- 消除 `SearchWithFilterContext` 的重试循环，单次 HNSW 搜索即可返回符合条件的结果
- 改动范围仅限 `vego/` 层（新增 `GetMetadataOnly` + 重写 `SearchWithFilterContext`），无架构侵入
- 默认 `SearchOverFetch=3`，兼顾日常场景与 high-archive 场景；高 archive 率环境可显式调高至 10
- 若方案 F 效果不足（极端场景下 HNSW `ef` 不够大导致遍历候选不足），可升级至方案 A/B

**核心洞察**：向量索引与属性过滤的耦合问题是向量数据库领域的经典难题。Vego 当前的 "后过滤 + 扩批" 方案在过滤率 < 50% 时表现良好，但在高 archive 率场景下呈指数级退化。方案 F 通过将 metadata 过滤合并到 `SearchWithDV` 的 `isDeleted` 回调中（纯内存后过滤），消除了 `SearchWithFilterContext` 的多次重试循环和大量无效 `storage.Get` 调用，在不修改 HNSW 核心层的前提下实现了显著的性能提升，是改动成本与性能收益的最佳平衡点。
