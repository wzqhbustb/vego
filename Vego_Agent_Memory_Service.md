# Vego 嵌入式 Agent 记忆服务实现计划

## 设计原则

- **嵌入式优先**：作为 Go 库直接嵌入 Agent，零外部数据库依赖
- **底层复用 Vego**：向量搜索、列式存储、DeletionVector、RowIndex 全部复用现有能力
- **上层借鉴 mem9**：LLM 提取/调和的 Prompt 工程和多层容错设计直接移植
- **模块路径**：新代码放在 `/Users/wangyang/vego/memory/` 包下，与现有 `vego/`、`index/`、`storage/` 平行
- **单包扁平结构**：所有代码统一在 `package memory` 下，避免子包嵌套导致的循环依赖和 import 路径冲突

## 新增目录结构

```
vego/
├── memory/              # 新增：Agent 记忆服务层（单包，package memory）
│   ├── memory.go        # MemoryStore 主入口 API
│   ├── config.go        # 配置项（LLM、Embedding、搜索参数）
│   ├── types.go         # Memory、MemoryState、MemoryType 等领域类型 + Document 转换
│   ├── ingest.go        # 两阶段智能摄取流水线
│   ├── reconcile.go     # Phase 2 调和逻辑
│   ├── search.go        # 混合搜索 + RRF 融合 + 二跳扩展 + 距离→相似度转换
│   ├── temporal.go      # 时间感知系统
│   ├── inverted.go      # 轻量级倒排索引（全文搜索，标准 BM25）
│   ├── llm_client.go    # OpenAI 兼容 LLM 客户端（同包，无子包）
│   ├── embedder.go      # OpenAI 兼容 Embedding 客户端（同包，无子包）
│   └── memory_test.go   # 测试
├── vego/                # 现有：Collection API（不修改）
├── index/               # 现有：HNSW 索引（不修改）
└── storage/             # 现有：列式存储（不修改）
```

---

## Task 1: 领域类型定义 — `memory/types.go`

定义记忆的核心数据模型，对标 mem9 的 `domain/types.go`。

```go
package memory

import (
    "time"

    vego "github.com/wzqhbustb/vego/vego"
)

type MemoryType string
const (
    TypePinned  MemoryType = "pinned"
    TypeInsight MemoryType = "insight"
    TypeSession MemoryType = "session"  // 原始对话消息，用于 ModeRaw 直接存储
)

type MemoryState string
const (
    StateActive   MemoryState = "active"
    StatePaused   MemoryState = "paused"
    StateArchived MemoryState = "archived"
    StateDeleted  MemoryState = "deleted"
)

type Memory struct {
    ID           string
    Content      string
    MemoryType   MemoryType
    State        MemoryState
    Tags         []string
    Metadata     map[string]interface{} // 含 temporal 子字段
    Source       string                 // 创建者 Agent ID
    AgentID      string
    Version      int
    SupersededBy string                 // 历史链
    Score        float64                // 搜索得分（查询时填充，0-1，越高越相关）
    RelativeAge  string                 // 人类可读的时效（查询时填充）
    CreatedAt    time.Time
    UpdatedAt    time.Time
}

type MemoryFilter struct {
    Query      string
    Tags       []string
    State      string
    MemoryType string
    AgentID    string
    Limit      int
    Offset     int
    MinScore   float64
}
```

### Memory ↔ Vego Document 双向转换

Vego 的 `Document` 只有 `ID`、`Vector`、`Metadata`、`Timestamp` 四个字段，没有 `Content` 字段。
因此 Memory 的所有业务字段必须编码到 `Document.Metadata` 中，并定义明确的 key 约定：

```go
// Metadata key 约定（以下划线开头避免与用户自定义 metadata 冲突）
const (
    metaKeyContent      = "_content"
    metaKeyState        = "_state"
    metaKeyType         = "_type"
    metaKeyTags         = "_tags"
    metaKeyVersion      = "_version"
    metaKeySupersededBy = "_superseded_by"
    metaKeyAgentID      = "_agent_id"
    metaKeySource       = "_source"
    metaKeyCreatedAt    = "_created_at"
    metaKeyUpdatedAt    = "_updated_at"
    metaKeyUserMeta     = "_user_meta"   // 用户自定义 metadata（含 temporal）
)

// memoryToDoc 将 Memory 转为 Vego Document 用于存储。
// vec 是 Embedding 向量，由调用方负责生成。
func memoryToDoc(m *Memory, vec []float32) *vego.Document {
    meta := map[string]interface{}{
        metaKeyContent:      m.Content,
        metaKeyState:        string(m.State),
        metaKeyType:         string(m.MemoryType),
        metaKeyTags:         m.Tags,
        metaKeyVersion:      m.Version,
        metaKeySuperseededBy: m.SupersededBy,
        metaKeyAgentID:      m.AgentID,
        metaKeySource:       m.Source,
        metaKeyCreatedAt:    m.CreatedAt.Format(time.RFC3339),
        metaKeyUpdatedAt:    m.UpdatedAt.Format(time.RFC3339),
        metaKeyUserMeta:     m.Metadata,
    }
    return &vego.Document{
        ID:        m.ID,
        Vector:    vec,
        Metadata:  meta,
        Timestamp: m.UpdatedAt,
    }
}

// docToMemory 从 Vego Document 反序列化为 Memory。
func docToMemory(doc *vego.Document) *Memory {
    m := &Memory{ID: doc.ID}
    if v, ok := doc.Metadata[metaKeyContent].(string); ok {
        m.Content = v
    }
    if v, ok := doc.Metadata[metaKeyState].(string); ok {
        m.State = MemoryState(v)
    }
    if v, ok := doc.Metadata[metaKeyType].(string); ok {
        m.MemoryType = MemoryType(v)
    }
    if v, ok := doc.Metadata[metaKeyTags].([]interface{}); ok {
        for _, t := range v {
            if s, ok := t.(string); ok {
                m.Tags = append(m.Tags, s)
            }
        }
    }
    if v, ok := doc.Metadata[metaKeyVersion].(float64); ok {
        m.Version = int(v)
    }
    if v, ok := doc.Metadata[metaKeySupersededBy].(string); ok {
        m.SupersededBy = v
    }
    if v, ok := doc.Metadata[metaKeyAgentID].(string); ok {
        m.AgentID = v
    }
    if v, ok := doc.Metadata[metaKeySource].(string); ok {
        m.Source = v
    }
    if v, ok := doc.Metadata[metaKeyCreatedAt].(string); ok {
        m.CreatedAt, _ = time.Parse(time.RFC3339, v)
    }
    if v, ok := doc.Metadata[metaKeyUpdatedAt].(string); ok {
        m.UpdatedAt, _ = time.Parse(time.RFC3339, v)
    }
    if v, ok := doc.Metadata[metaKeyUserMeta].(map[string]interface{}); ok {
        m.Metadata = v
    }
    return m
}
```

---

## Task 2: LLM 客户端 — `memory/llm_client.go`

移植 mem9 的 `server/internal/llm/client.go`（270 行），内置 OpenAI 兼容 HTTP 客户端。
同属 `package memory`，无子包。

核心接口：
```go
type LLMClient struct {
    apiKey, baseURL, model string
    temperature float64
    http *http.Client
}

func NewLLMClient(cfg LLMConfig) *LLMClient  // apiKey 为空返回 nil
func (c *LLMClient) CompleteJSON(ctx context.Context, system, user string) (string, error)
func ParseJSON[T any](raw string) (T, error)
```

关键设计：
- 默认 model: `gpt-4o-mini`，temperature: `0.1`
- `response_format: json_object` 强制 JSON 输出
- HTTP 400 自动回退（兼容 Ollama/vLLM）
- 120s 超时
- **可观测性**：使用 `slog` 结构化日志替代 Prometheus metrics，记录请求耗时和 token 用量。
  嵌入式库不引入 Prometheus 依赖，调用方如需 metrics 可通过 slog handler 桥接

```go
// 示例：请求完成后的日志输出
slog.Info("llm request completed",
    "model", c.model,
    "duration_ms", duration.Milliseconds(),
    "prompt_tokens", usage.PromptTokens,
    "completion_tokens", usage.CompletionTokens,
)
```

---

## Task 3: Embedding 客户端 — `memory/embedder.go`

移植 mem9 的 `server/internal/embed/embedder.go`（124 行）。
同属 `package memory`，无子包。

```go
type Embedder struct {
    apiKey, baseURL, model string
    dims int
    http *http.Client
}

func NewEmbedder(cfg EmbedConfig) *Embedder   // apiKey 为空返回 nil
func (e *Embedder) Embed(ctx context.Context, text string) ([]float32, error)
func (e *Embedder) Dims() int
```

调用 OpenAI `/v1/embeddings` API，支持自定义 BaseURL（Ollama、LM Studio 等）。
使用 `slog` 记录请求耗时，不引入 Prometheus。

---

## Task 4: 轻量级倒排索引 — `memory/inverted.go`

自建简单的内存倒排索引，用于混合搜索的关键词分支。

```go
type InvertedIndex struct {
    mu         sync.RWMutex
    index      map[string][]string    // term → []memoryID
    docTerms   map[string][]string    // memoryID → []term（用于删除时清理）
    docLen     map[string]int         // memoryID → 文档词项数（用于 BM25 长度归一化）
    totalTerms int64                  // 所有文档词项总数（用于计算 avgdl）
    docCount   int
}

func NewInvertedIndex() *InvertedIndex
func (idx *InvertedIndex) Add(id, content string)
func (idx *InvertedIndex) Remove(id string)
func (idx *InvertedIndex) Search(query string, limit int) []ScoredID  // BM25 评分
```

实现细节：
- 分词：按空格 + Unicode 标点分割，转小写，中文按字符 bigram
- 评分：**标准 BM25**（含文档长度归一化），公式：
  ```
  score = IDF * (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * dl/avgdl))
  ```
  其中 `k1 = 1.2`，`b = 0.75`，`avgdl = totalTerms / docCount`，`dl = docLen[id]`
- **不做持久化**：Open() 时从 Vego Collection 遍历所有 active Document 重建索引。
  对 <100K 条记忆，重建耗时 <1s，简化实现且保证与 Vego 存储一致性
- 增量维护：Store/Update/Delete 时同步更新倒排索引
- 规模：内存中维护，适用于嵌入式场景（<100K 条记忆）

```go
// Open() 中的索引重建流程
func (s *MemoryStore) rebuildInvertedIndex() error {
    // 遍历 Vego Collection 中所有文档
    // 仅对 state == "active" 的文档建索引
    // 从 Metadata["_content"] 提取文本内容
    // 调用 s.inverted.Add(id, content)
}
```

---

## Task 5: 配置系统 — `memory/config.go`

使用与 Vego 一致的 functional options 模式。

```go
type Config struct {
    // 存储
    DataDir   string   // 数据目录，默认 "./vego_memory"
    Dimension int      // 向量维度，默认 1536

    // LLM
    LLMAPIKey      string
    LLMBaseURL     string  // 默认 "https://api.openai.com/v1"
    LLMModel       string  // 默认 "gpt-4o-mini"
    LLMTemperature float64 // 默认 0.1

    // Embedding
    EmbedAPIKey  string
    EmbedBaseURL string
    EmbedModel   string  // 默认 "text-embedding-3-small"
    EmbedDims    int     // 默认 1536

    // 搜索
    RRF_K             float64 // 默认 60.0
    MinScore          float64 // 默认 0.3（基于相似度，0-1 范围）
    SecondHopGate     float64 // 默认 0.5
    SecondHopWeight   float64 // 默认 0.3
    SecondHopTopN     int     // 默认 3
    PinnedBoost       float64 // 默认 1.5

    // 摄取
    MaxFacts          int     // 默认 50
    MaxConversationRunes int  // 默认 1000000

    // 距离函数（用于相似度转换）
    DistanceFunc  string  // "cosine" | "l2" | "ip"，默认 "cosine"
}

func WithLLM(apiKey, baseURL, model string) Option
func WithEmbedding(apiKey, baseURL, model string, dims int) Option
func WithDistanceFunc(name string) Option
// ... 更多 Option
```

---

## Task 6: MemoryStore 主 API — `memory/memory.go`

核心入口，封装 Vego DB + LLM + Embedding + 倒排索引。

```go
type MemoryStore struct {
    db       *vego.DB
    coll     *vego.Collection
    llm      *LLMClient       // 同包类型，无需 import 子包
    embedder *Embedder         // 同包类型，无需 import 子包
    inverted *InvertedIndex
    config   *Config
    mu       sync.Mutex        // 保护 Archive-and-Create 原子性
}

func Open(path string, opts ...Option) (*MemoryStore, error)
func (s *MemoryStore) Close() error

// 智能摄取（对标 mem9 的 Ingest）
func (s *MemoryStore) Ingest(ctx context.Context, req IngestRequest) (*IngestResult, error)

// 直接 CRUD
func (s *MemoryStore) Store(ctx context.Context, content string, tags []string) (*Memory, error)
func (s *MemoryStore) Get(ctx context.Context, id string) (*Memory, error)
func (s *MemoryStore) Update(ctx context.Context, id, content string, tags []string) (*Memory, error)
func (s *MemoryStore) Delete(ctx context.Context, id string) error

// 搜索召回
func (s *MemoryStore) Search(ctx context.Context, query string, opts ...SearchOption) ([]Memory, error)

// 批量操作
func (s *MemoryStore) StoreBatch(ctx context.Context, items []StoreItem) ([]Memory, error)
func (s *MemoryStore) Bootstrap(ctx context.Context, limit int) ([]Memory, error)
```

**Vego 集成方式**：
- `Open()` 内部调用 `vego.Open(path)` 创建 DB，获取 `"memories"` Collection，然后**遍历所有 active 文档重建倒排索引**
- `Store()` 使用 `memoryToDoc()` 转换，`Embedder.Embed()` 生成向量，`coll.InsertContext()` 存入 Vego
- `Get()` 使用 `coll.GetContext()` 获取 Document，再用 `docToMemory()` 反序列化
- `Search()` 先走 HNSW 向量搜索（返回 `[]SearchResult`），通过 `distanceToSimilarity()` 转换为相似度，再走倒排索引关键词搜索，最后 RRF 融合
- `Delete()` 调用 Vego 的 `coll.DeleteContext()` 逻辑删除（DeletionVector）。同时更新倒排索引 `inverted.Remove(id)`
- `Update()` 使用 Archive-and-Create 模式（见 Task 7 原子性设计）

---

## Task 7: 两阶段智能摄取 — `memory/ingest.go` + `memory/reconcile.go`

移植 mem9 的 `ingest.go`（1642 行）核心逻辑，简化为嵌入式场景。

### Phase 1: 事实提取 (`ingest.go`)

```go
func (s *MemoryStore) ExtractFacts(ctx context.Context, messages []Message) ([]ExtractedFact, error)
```

- 复用 mem9 的 14 条提取规则 System Prompt（含中英文示例）
- JSON 解析失败自动重试 + raw fallback 降级
- `query_intent` 过滤（丢弃搜索意图事实）
- 事实上限 50 条
- **ModeRaw 支持**：当 mode 为 raw 时，直接将消息存为 `TypeSession` 类型记忆，跳过 LLM 处理

### Phase 2: 调和 (`reconcile.go`)

```go
func (s *MemoryStore) Reconcile(ctx context.Context, agentID string, facts []ExtractedFact) (*IngestResult, error)
```

- 对每条事实并发搜索已有记忆（向量搜索 + 关键词搜索，4 worker）
- 整数 ID 映射防止 LLM 幻觉
- 复用 mem9 的 ADD/UPDATE/DELETE/NOOP Prompt
- DELETE 执行软删除：`coll.DeleteContext()` + 倒排索引 `Remove()`
- Pinned 记忆保护：不可自动删除，UPDATE 降级为 ADD

### Archive-and-Create 原子性设计

Vego Collection 没有事务机制。为保证 UPDATE 操作（旧记忆归档 + 新记忆创建）的一致性，
采用 **Insert-First + Mutex** 策略：

```go
func (s *MemoryStore) archiveAndCreate(ctx context.Context, oldID string, newMem *Memory, newVec []float32) error {
    s.mu.Lock()
    defer s.mu.Unlock()

    // 步骤 1：先插入新记忆（此时旧记忆仍为 active，不影响搜索正确性）
    newDoc := memoryToDoc(newMem, newVec)
    if err := s.coll.InsertContext(ctx, newDoc); err != nil {
        return fmt.Errorf("insert new memory: %w", err)
    }
    s.inverted.Add(newMem.ID, newMem.Content)

    // 步骤 2：更新旧记忆状态为 archived + 设置 superseded_by
    oldDoc, err := s.coll.GetContext(ctx, oldID)
    if err != nil {
        // 新记忆已插入但旧记忆更新失败 → 启动时恢复扫描会清理
        slog.Warn("archive old memory failed, orphan created", "old_id", oldID, "new_id", newMem.ID, "err", err)
        return nil // 不回滚，容忍短暂不一致
    }
    oldDoc.Metadata[metaKeyState] = string(StateArchived)
    oldDoc.Metadata[metaKeySupersededBy] = newMem.ID
    if err := s.coll.UpdateContext(ctx, oldDoc); err != nil {
        slog.Warn("archive old memory state update failed", "old_id", oldID, "err", err)
    }
    s.inverted.Remove(oldID)

    return nil
}
```

**崩溃恢复**：`Open()` 启动时扫描所有 active 文档，检测是否存在 superseded_by 非空但仍为 active 的记忆，自动修复为 archived。

---

## Task 8: 混合搜索 + RRF 融合 — `memory/search.go`

移植 mem9 的混合搜索流水线。

```go
func (s *MemoryStore) hybridSearch(ctx context.Context, filter MemoryFilter) ([]Memory, error)
```

### 距离→相似度转换层

Vego 搜索返回 `[]SearchResult`（含 `Distance float32`，越小越近），
而 RRF 和 MinScore 都需要相似度（越大越相关，0-1 范围）。
需要根据 Vego 使用的距离函数做转换：

```go
// distanceToSimilarity 将 Vego 的 distance 转为 0-1 相似度
func distanceToSimilarity(distance float32, distFunc string) float64 {
    d := float64(distance)
    switch distFunc {
    case "cosine":
        // Vego CosineDistance 返回 1 - cos(a,b)，范围 [0, 2]
        return 1.0 - d
    case "l2":
        // L2 距离范围 [0, +∞)，用 1/(1+d) 映射到 (0, 1]
        return 1.0 / (1.0 + d)
    case "ip":
        // InnerProduct 距离 = -dot(a,b)，归一化向量时 range [-1, 1]
        return (1.0 + (-d)) / 2.0
    default:
        return 1.0 / (1.0 + d)
    }
}

// toScoredMemories 将 Vego SearchResult 转为带相似度分数的 Memory 列表。
// 同时过滤掉非 active 状态的记忆。
func (s *MemoryStore) toScoredMemories(results []vego.SearchResult) []Memory {
    out := make([]Memory, 0, len(results))
    for _, r := range results {
        m := docToMemory(r.Document)
        if m.State != StateActive {
            continue // 过滤已删除/已归档记忆
        }
        m.Score = distanceToSimilarity(r.Distance, s.config.DistanceFunc)
        out = append(out, *m)
    }
    return out
}
```

### 八阶段流程

1. **时间归一化**：`NormalizeTemporalRecallQuery(query, now)`
2. **向量搜索**：调用 `s.coll.SearchContext(ctx, queryVec, limit*3)` 走 Vego HNSW
3. **距离转换**：`toScoredMemories()` 将 distance 转为 similarity，同时过滤非 active 记忆
4. **相似度过滤**：丢弃 similarity < MinScore (0.3) 的结果
5. **关键词搜索**：调用 `s.inverted.Search(query, limit*3)` 走倒排索引
6. **RRF 融合**：`score[id] += 1/(K + rank+1)`，K=60
7. **二跳扩展**：Top-3 结果作为种子再搜，权重 0.3。使用已缓存的 embedding 向量直接做二跳搜索，避免重复调用 Embedding API
8. **类型加权**：pinned x 1.5
9. **排序 + 分页 + 年龄标注**

---

## Task 9: 时间感知系统 — `memory/temporal.go`

移植 mem9 的 `temporal_fact.go`（1174 行）核心逻辑。

核心函数：
```go
// 事实提取后的时间归一化
func NormalizeTemporalFacts(facts []ExtractedFact, messages []Message, now time.Time) []ExtractedFact

// 搜索时的查询归一化
func NormalizeTemporalRecallQuery(query string, now time.Time) string

// 结果展示时的时间投影
func TemporalRecallProjection(content string, metadata map[string]interface{}) string
```

支持的时间表达：
- 英文：yesterday, today, tomorrow, last/this/next week/month/year, last/this/next summer/winter 等
- 中文：昨天、今天、明天、上周、本周、下周、上个月、去年、明年 等
- 绝对日期：2026-04-14、April 14 2026、2026年4月14日
- 对话锚点：从消息头部提取日期作为相对时间的锚点

TemporalMetadata 结构：
```go
type TemporalMetadata struct {
    Kind          string // explicit_absolute | deictic_relative | header_anchor_relative | local_anchor_relative
    AnchorSource  string // local | header | now
    Granularity   string // day | week | month | year | season
    ResolvedStart string // "2026-04-14"
    ResolvedEnd   string // "2026-04-14"
    Display       string // 人类可读
}
```

---

## Task 10: 测试与集成验证

### 单元测试 (`memory/memory_test.go` 等)

- LLM 客户端：Mock HTTP 测试 CompleteJSON
- Embedding 客户端：Mock HTTP 测试 Embed
- **Memory ↔ Document 转换**：memoryToDoc / docToMemory 双向序列化正确性
- **距离→相似度转换**：Cosine/L2/IP 三种距离函数的转换精度验证
- 倒排索引：分词、BM25 评分（含文档长度归一化）、增删改查
- RRF 融合：已知排名输入验证输出
- 时间感知：中英文相对时间解析、绝对日期检测、查询归一化
- **Archive-and-Create**：验证 insert-first 策略在正常和模拟崩溃场景下的正确性

### 集成测试

- 完整 Ingest 流程：消息输入 → 事实提取 → 调和 → 记忆存储
- **ModeRaw 流程**：消息直接存为 TypeSession 类型记忆，无 LLM 调用
- 混合搜索：向量 + 关键词 + RRF 融合验证 recall
- 生命周期：Create → Update(Archive-and-Create) → Delete → Search 不可见
- Pinned 保护：Pinned 记忆不可被 LLM 自动删除/覆盖
- **倒排索引重建**：Close → Open 后索引一致性验证
- **崩溃恢复**：模拟 archiveAndCreate 中途失败后 Open() 的自动修复

### Makefile 添加

```makefile
test-memory:
    go test -race -count=1 ./memory/...

test-memory-integration:
    VEGO_LLM_API_KEY=$(LLM_KEY) go test -tags=integration -race -count=1 -v ./memory/...
```

---

## 实现顺序（建议）

| 顺序 | Task | 依赖 | 预估工作量 |
|------|------|------|-----------|
| 1 | Task 1: 领域类型 + Document 转换 | 无 | 中 |
| 2 | Task 5: 配置系统 | Task 1 | 小 |
| 3 | Task 2: LLM 客户端 | 无 | 中（移植） |
| 4 | Task 3: Embedding 客户端 | 无 | 小（移植） |
| 5 | Task 4: 倒排索引（标准 BM25） | 无 | 中 |
| 6 | Task 6: MemoryStore 主 API | Task 1-5 | 中 |
| 7 | Task 8: 混合搜索 + 距离转换 | Task 4, 6 | 中 |
| 8 | Task 9: 时间感知 | 无 | 大（移植） |
| 9 | Task 7: 智能摄取 + Archive-and-Create | Task 2, 3, 6, 8, 9 | 大（移植） |
| 10 | Task 10: 测试 | 全部 | 中 |

Task 2/3/4 可以并行实现；Task 8/9 可以并行实现。

---

## 附录：Review 修复清单

| # | 问题 | 修复方案 |
|---|------|---------|
| 1 | Vego 返回 Distance（越小越好），设计假设 Score（越大越好） | 新增 `distanceToSimilarity()` 转换层，支持 Cosine/L2/IP 三种距离函数 |
| 2 | Vego Document 无 Content 字段，映射关系不明确 | 定义 `metaKey*` 常量约定 + `memoryToDoc()`/`docToMemory()` 双向转换函数 |
| 3 | Archive-and-Create 无事务保证，崩溃可能丢数据 | Insert-First + `sync.Mutex` 策略 + Open() 启动时崩溃恢复扫描 |
| 4 | 倒排索引序列化到 JSON 脆弱且冗余 | 不做持久化，Open() 时从 Vego 遍历 active 文档重建（<1s for <100K） |
| 5 | 缺少 `TypeSession` 记忆类型，ModeRaw 无法工作 | 新增 `TypeSession = "session"` |
| 6 | LLM 客户端依赖 Prometheus metrics，不适合嵌入式库 | 替换为 `slog` 结构化日志 |
| 7 | `memory/llm/` 和 `memory/embed/` 子包导致 import 路径膨胀和潜在冲突 | 扁平化为同包文件 `llm_client.go` 和 `embedder.go`，类型重命名为 `LLMClient` 和 `Embedder` |
| 8 | BM25 缺少文档长度归一化（b, avgdl），长文档总是得高分 | 使用标准 BM25 公式，新增 `docLen` 和 `totalTerms` 字段 |
