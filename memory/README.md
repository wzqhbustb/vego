> ⚠️ **Beta**: API 可能在 v1.0 前微调。当前版本适合早期试用和内部集成。

# Vego Agent Memory Service

面向 AI Agent 的持久化记忆层，提供向量搜索 + 关键词搜索 + 时间感知召回 + 智能摄取/调和的全套记忆管理能力。

---

## 目录

- [快速开始](#快速开始)
- [配置指南](#配置指南)
- [核心 API](#核心-api)
  - [CRUD](#crud)
  - [搜索](#搜索)
  - [摄取](#摄取)
  - [批量与会话](#批量与会话)
- [搜索调优指南](#搜索调优指南)
- [记忆状态机](#记忆状态机)
- [记忆类型](#记忆类型)
- [线程安全](#线程安全)
- [Schema 迁移](#schema-迁移)
- [错误处理](#错误处理)

---

## 快速开始

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/wzqhbustb/vego/memory"
)

func main() {
    // 1. 打开记忆存储（WithEmbedding 必需，WithLLM 仅 ModeNormal 需要）
    s, err := memory.Open("./agent_memory",
        memory.WithDimension(128),
        memory.WithEmbedding("sk-xxx", "https://api.openai.com/v1", "text-embedding-3-small", 128),
        memory.WithLLM("sk-xxx", "https://api.openai.com/v1", "gpt-4o-mini", 0.1),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer s.Close()

    ctx := context.Background()

    // 2. 存储一条洞察记忆
    mem, err := s.Store(ctx, "用户喜欢使用 Go 语言", []string{"preference", "tech"})
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println("stored:", mem.ID)

    // 3. 搜索
    results, err := s.Search(ctx, "用户喜欢什么编程语言")
    if err != nil {
        log.Fatal(err)
    }
    for _, r := range results {
        fmt.Printf("  %.3f %s\n", r.Score, r.Content)
    }

    // 4. 摄取对话（自动提取事实 + 调和）
    res, err := s.Ingest(ctx, memory.IngestRequest{
        AgentID: "agent-1",
        Mode:    memory.ModeNormal,
        Messages: []memory.Message{
            {Role: "user", Content: "我明天要去上海出差"},
        },
    })
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("ingest: added=%d updated=%d\n", res.Added, res.Updated)
}
```

---

## 配置指南

通过 `Open(path, opts...)` 创建 MemoryStore。**`WithEmbedding` 是必需配置**（否则所有涉及向量的操作会返回 "embedder not configured" 错误）。其余配置均有合理默认值。

### 最小可用配置

```go
s, err := memory.Open("./data",
    memory.WithDimension(1536),
    memory.WithEmbedding("sk-xxx", "https://api.openai.com/v1", "text-embedding-3-small", 1536),
)
```

> LLM 配置（`WithLLM`）仅 `ModeNormal` 摄取和 Reconcile 需要。如果只用 `Store/Search/ModeRaw`，可省略。

### 完整配置项

```go
s, err := memory.Open("./data",
    // 存储
    memory.WithDataDir("./data"),
    memory.WithDimension(1536),

    // LLM（用于事实提取和调和）
    memory.WithLLM("sk-xxx", "https://api.openai.com/v1", "gpt-4o-mini", 0.1),

    // Embedding（用于向量搜索）
    memory.WithEmbedding("sk-xxx", "https://api.openai.com/v1", "text-embedding-3-small", 1536),

    // 搜索调优
    memory.WithSearchLimit(10),
    memory.WithSearchOverFetch(3),
    memory.WithSearchParams(0.3),       // MinScore
    memory.WithRRFK(60.0),
    memory.WithDistanceFunc("cosine"),   // cosine | l2 | ip
    memory.WithSecondHop(0.02, 0.3, 3), // gate, weight, topN
    memory.WithPinnedBoost(1.5),
    memory.WithRecencyBoost(1.05, 1.02), // week, month
    memory.WithGapStop(0.5),
    memory.WithDualChannelBonus(0.1),        // 默认 0（禁用）
    memory.WithVectorSimilarityWeight(0.2),  // 默认 0（禁用）

    // 摄取调优
    memory.WithIngestParams(50, 1_000_000), // maxFacts, maxConversationRunes
    memory.WithNearDupThreshold(0.95),       // 近似去重阈值，0=禁用

    // 输入校验
    memory.WithMaxContentLen(50000),
    memory.WithMaxTags(20),
    memory.WithMaxBulkSize(100),

    // 可观测性与企业部署
    memory.WithLogger(myLogger),                 // 自定义 slog.Logger
    memory.WithHTTPRoundTripper(myTransport),    // 自定义 TLS/代理

    // 提示词定制（默认英文）
    memory.WithFactExtractionPrompt(myPrompt),   // 自定义事实提取系统提示词
    memory.WithReconcilePrompt(myPrompt),        // 自定义协调决策系统提示词

    // BM25 调参（高级）
    memory.WithBM25Params(1.2, 0.75),            // k1, b
)
```

### 默认值速查

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `Dimension` | 1536 | 向量维度 |
| `SearchLimit` | 10 | 默认搜索结果数 |
| `SearchOverFetch` | 3 | 搜索过取倍数 |
| `MinScore` | 0.3 | 最小相似度阈值 (0-1) |
| `RRFK` | 60.0 | RRF 融合常数 K |
| `DistanceFunc` | cosine | 距离函数 |
| `PinnedBoost` | 1.5 | Pinned 记忆搜索权重倍数 |
| `RecencyBoostWeek` | 1.05 | ≤7 天的 recency 乘数 |
| `RecencyBoostMonth` | 1.02 | ≤30 天的 recency 乘数 |
| `GapStopRatio` | 0.5 | 分数断崖截断比例，0=禁用 |
| `SecondHopGate` | 0.02 | 二跳触发门限 |
| `SecondHopWeight` | 0.3 | 二跳 RRF 权重 |
| `SecondHopTopN` | 3 | 二跳种子数 |
| `DualChannelBonus` | 0 | 双通道命中乘数加成，0=禁用 |
| `VectorSimilarityWeight` | 0 | 向量相似度加权，0=禁用 |
| `MaxFacts` | 50 | 单次摄取最大事实数 |
| `NearDupThreshold` | 0 | 近似去重阈值，0=禁用 |
| `MaxContentLen` | 50000 | 单条内容最大字节 |
| `MaxTags` | 20 | 单条最大标签数 |
| `MaxBulkSize` | 100 | 批量操作最大数量 |
| `BM25K1` | 1.2 | BM25 词频饱和参数 |
| `BM25B` | 0.75 | BM25 长度归一化参数 |
| `FactExtractionPrompt` | (内置英文) | 自定义事实提取提示词 |
| `ReconcilePrompt` | (内置英文) | 自定义协调决策提示词 |

---

## 核心 API

### CRUD

```go
// Store — 创建新记忆（类型=insight，状态=active）
mem, err := s.Store(ctx, "内容", []string{"tag1", "tag2"})

// Get — 按 ID 读取
mem, err := s.Get(ctx, "memory-id")

// Update — 内容更新（Archive-and-Create：旧记忆归档，创建新版本）
newMem, err := s.Update(ctx, "old-id", "新内容", []string{"new-tag"})

// Delete — 软删除（状态→deleted，终态，搜索不可见）
err := s.Delete(ctx, "memory-id")

// Pause / Resume — 状态切换
err := s.Pause(ctx, "memory-id")   // active → paused（搜索不可见）
err := s.Resume(ctx, "memory-id")  // paused → active
```

### 搜索

```go
// 默认混合搜索（向量 + 关键词 + RRF + 二跳 + Pinned/Recency 加权）
results, err := s.Search(ctx, "查询文本")

// 纯向量搜索（跳过关键词和二跳，更快）
results, err := s.Search(ctx, "查询文本", memory.EnableHybrid(false))

// 自定义参数
results, err := s.Search(ctx, "查询文本",
    memory.Limit(20),              // 返回数量
    memory.MinScore(0.5),          // 最小相似度
    memory.WithFilter(memory.MemoryFilter{
        Tags:       []string{"preference"},
        MemoryType: string(memory.TypeInsight),
        AgentID:    "agent-1",
        SessionID:  "session-1",
    }),
)

// 搜索结果字段
for _, r := range results {
    r.ID          // 记忆 ID
    r.Content     // 内容文本
    r.Score       // 综合得分 (0-1)
    r.RelativeAge // "3 days ago"
    r.MemoryType  // insight | session | pinned
    r.State       // active | paused | archived | deleted
    r.Tags        // []string
}
```

### 摄取

```go
// ModeNormal：LLM 提取事实 → 调和（AgentID 必填）
res, err := s.Ingest(ctx, memory.IngestRequest{
    AgentID: "agent-1",
    Mode:    memory.ModeNormal,
    Messages: []memory.Message{
        {Role: "user", Content: "我偏好深色模式"},
        {Role: "assistant", Content: "已记住你的偏好"},
    },
})
// res.Added / res.Updated / res.Deleted / res.Skipped / res.NearDupSkipped
// NearDupSkipped: 因 NearDupThreshold 语义去重而跳过的事实数（未调用 LLM）

// ModeRaw：直接存储原始消息（SessionID 必填，精确去重）
res, err := s.Ingest(ctx, memory.IngestRequest{
    SessionID: "session-1",
    Mode:      memory.ModeRaw,
    Messages:  msgs,
})

// 直接摄取外部内容（不走 LLM）
memories := []*memory.Memory{...}
err := s.Bootstrap(ctx, memories)
```

### 批量与会话

```go
// 批量存储（并发 embed，最多 4 并行）
items := []memory.StoreItem{
    {Content: "msg1", Tags: []string{"a"}},
    {Content: "msg2", Tags: []string{"b"}},
}
mems, err := s.StoreBatch(ctx, items)

// 按会话批量查询
results, err := s.ListBySessionIDs(ctx,
    []string{"session-1", "session-2"},
    10, // 每个会话最多 10 条
)
// results["session-1"] → []Memory

// 通用列表查询
mems, err := s.List(ctx, memory.MemoryFilter{
    Tags:       []string{"preference"},
    State:      string(memory.StateActive),
    MemoryType: string(memory.TypeInsight),
    Limit:      20,
    Offset:     0,
})

// 统计
stats, err := s.Stats(ctx)
// stats.Total / Active / Paused / Archived / Deleted / ByType / Vego

// 物理压缩（删除 archived/deleted，回收磁盘空间）
// 阻塞性操作：持有写锁，会阻塞所有读写。建议维护窗口执行。
err := s.Compact(ctx)
```

---

## 搜索调优指南

### 1. 距离函数选择

| 场景 | 推荐 | 说明 |
|------|------|------|
| 文本嵌入 (OpenAI) | `cosine` | 默认，适合归一化向量 |
| 通用向量 | `l2` | Euclidean，未归一化向量 |
| 语义搜索 | `ip` | 内积，适合非归一化嵌入 |

```go
memory.WithDistanceFunc("cosine")
```

### 2. 结果质量控制

```go
// 提高精度（减少低质量结果）
memory.WithSearchParams(0.5)  // MinScore 从 0.3 提高到 0.5
memory.WithGapStop(0.3)       // 更激进的断崖截断

// 提高召回（允许更多结果）
memory.WithSearchParams(0.1)  // 降低门槛
memory.WithGapStop(0)         // 禁用截断
memory.WithSearchOverFetch(5) // 过取倍数提高
```

### 3. Pinned 记忆权重

Pinned 记忆不受 LLM 自动 UPDATE/DELETE 影响，搜索时自动 1.5x 加权：

```go
// 想让 pinned 更突出
memory.WithPinnedBoost(2.0)

// 想让 pinned 和普通记忆平等竞争
memory.WithPinnedBoost(1.0)
```

### 4. Recency 加权

新记忆获得额外搜索权重：

```go
// 强 recency 偏好（新记忆排前面）
memory.WithRecencyBoost(1.2, 1.1)

// 弱 recency 偏好
memory.WithRecencyBoost(1.02, 1.01)

// 禁用 recency（完全按语义排序）
memory.WithRecencyBoost(1.0, 1.0)
```

### 5. 二跳扩展

当首跳最高得分 ≥ `SecondHopGate` 时，用 Top-N 种子做二次搜索，扩大召回：

```go
// 更积极的二跳（更多种子、更高权重）
memory.WithSecondHop(0.01, 0.5, 5)

// 保守二跳
memory.WithSecondHop(0.1, 0.2, 2)

// 禁用二跳
memory.WithSecondHop(0, 0, 0)
```

### 6. 双通道命中奖励

同时出现在向量搜索和关键词搜索的结果获得额外加权：

```go
// 强化双通道命中（向量+关键词都匹配的结果排更前）
memory.WithDualChannelBonus(0.3)  // 得分 × 1.3

// 禁用
memory.WithDualChannelBonus(0)
```

### 7. 向量相似度加权

RRF 只看排名不看原始相似度。加入原始相似度可区分"同排名不同质量"：

```go
// 重视高相似度结果
memory.WithVectorSimilarityWeight(0.5)

// 禁用（纯 RRF）
memory.WithVectorSimilarityWeight(0)
```

### 8. 近似去重（摄取阶段）

在 LLM 调和前拦截极度相似的事实，节省 LLM 调用成本：

```go
// 相似度 ≥ 0.95 自动跳过
memory.WithNearDupThreshold(0.95)

// 禁用
memory.WithNearDupThreshold(0)
```

> 与 `ModeRaw` 的精确去重（SHA256）不同，NearDup 是**语义层面**的近似去重，适用于 `ModeNormal`。

---

## 记忆状态机

```
Create → active ──Pause──→ paused
         │  ↑                 │
         │  └──── Resume ─────┘
         │
         ├── Archive ──→ archived（终态，superseded_by 指向新版本）
         │
         └── Delete ───→ deleted（终态，软删除，搜索不可见）
```

| 状态 | 搜索可见 | List 可见 | Get 可读 |
|------|---------|----------|---------|
| `active` | ✅ | ✅ | ✅ |
| `paused` | ❌ | ✅（filter.State="" 时包含所有状态） | ✅ |
| `archived` | ❌ | ✅（filter.State="" 时包含所有状态） | ✅ |
| `deleted` | ❌ | ✅（filter.State="" 时包含所有状态） | ✅ |

> `List` 默认返回**所有状态**的记忆。传 `filter.State = "active"` 可只返回活跃记忆。

---

## 记忆类型

| 类型 | 创建方式 | 搜索权重 | 特殊规则 |
|------|---------|---------|---------|
| `insight` | LLM 自动提取 / 用户手动 Store | 1.0x | LLM 可 UPDATE/DELETE |
| `session` | ModeRaw 直接存储 | 1.0x | 无 |
| `pinned` | 通过 Bootstrap 导入（需显式设置类型） | **1.5x** | LLM 不可 DELETE；UPDATE 降级为 ADD |

> 创建 pinned 记忆（Store 不支持指定 MemoryType，需通过 Bootstrap）：
> ```go
> mem := &memory.Memory{Content: "重要规则", MemoryType: memory.TypePinned, State: memory.StateActive}
> s.Bootstrap(ctx, []*memory.Memory{mem})
> ```

---

## 错误处理

```go
// 所有 API 返回 error
mem, err := s.Get(ctx, "id")
if err != nil {
    // err 包含操作上下文："get: document not found"
    log.Printf("get failed: %v", err)
}

// 输入校验错误
_, err := s.Store(ctx, strings.Repeat("x", 100000), nil)
// → "content length 100000 exceeds max 50000"

_, err := s.Store(ctx, "ok", make([]string, 25))
// → "tag count 25 exceeds max 20"
```

---

## 资源管理

```go
s, err := memory.Open("./data", ...)
if err != nil {
    log.Fatal(err)
}
// 必须 Close 释放数据库句柄和 HTTP 连接
defer s.Close()
```

`Close()` 会：
1. 关闭底层 Vego 数据库
2. 释放 LLM 和 Embedding 客户端的 idle HTTP 连接

---

## 线程安全

`MemoryStore` 可安全用于并发场景：

- **读操作**（`Get` / `Search` / `List` / `Stats`）：lock-free，依赖 Vego 内部读写协调
- **写操作**（`Store` / `Update` / `Delete` / `Pause` / `Resume` / `Bootstrap` / `StoreBatch` / `Compact`）：内部通过 `sync.Mutex` 串行化
- `Compact` 会阻塞所有并发读写，大规模数据建议在低峰期执行

---

## Schema 迁移

`memory` 包内置版本控制（当前 `CurrentSchemaVersion = 1`）。打开存储时会自动检测旧版本数据并迁移：

```go
// 注册自定义迁移（从版本 0 升级到 1）
memory.RegisterMigration(0, func(m *memory.Memory) error {
    m.Tags = append(m.Tags, "migrated-v1")
    return nil
})
```

> `RegisterMigration` 非并发安全，应在 `init()` 或单 goroutine 设置时调用。
