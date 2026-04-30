# Vego 嵌入式 Agent 记忆服务实现计划

## 设计原则

- **嵌入式优先**：作为 Go 库直接嵌入 Agent，零外部数据库依赖
- **底层复用 Vego**：向量搜索、列式存储、DeletionVector、RowIndex 全部复用现有能力
- **上层借鉴 mem9**：LLM 提取/调和的 Prompt 工程和多层容错设计直接移植
- **模块路径**：新代码放在 `/Users/wangyang/vego/memory/` 包下，与现有 `vego/`、`index/`、`storage/` 平行
- **单包扁平结构**：所有代码统一在 `package memory` 下，避免子包嵌套导致的循环依赖和 import 路径冲突

## Vego 底层能力约束（实现前必读）

以下是经过代码审查确认的 Vego 实际能力，设计中所有方案必须在这些约束内工作：

| 能力 | 实际情况 | 设计影响 |
|------|---------|---------|
| **Metadata 类型** | `map[string]interface{}`，经 JSON 持久化往返后 `int→float64`、`[]string→[]interface{}` | `docToMemory` 必须处理类型退化；或改用单 JSON 字段序列化 |
| **Delete 语义** | DeletionVector 软删除：搜索自动过滤，但 **`Get` 也会失败**（docToNode 映射被移除） | 不能用 `DeleteContext` 做 mem9 式软删除（state=deleted 但仍可按 ID 查）；必须用 `UpdateContext` 改 state 字段 |
| **搜索过滤** | `SearchWithFilter(query, k, filter)` 支持 metadata 条件过滤（post-search 自动扩批重试，最大 k×20） | 向量搜索应使用 `SearchWithFilter` + `MetadataFilter{Key:"_state", Op:"eq", Value:"active"}` 替代手动过滤 |
| **文档遍历** | **无公共 API**。内部 `GetAllValidDocuments()` 不可外部调用 | 需要为 Vego 新增 `ForEach(fn)` 公共方法，或通过 `docToNode` 遍历 ID + 逐条 `Get` |
| **Update 语义** | `UpdateContext` = MarkDeleted(旧) + Put(新) + 新 HNSW node，**同 ID 替换** | Archive-and-Create 不能使用 `UpdateContext`（它会覆盖同 ID），必须使用 Insert 新 ID + Update 旧 ID 改 state |

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
├── vego/                # 现有：Collection API（需新增 ForEach 公共方法）
├── index/               # 现有：HNSW 索引（不修改）
└── storage/             # 现有：列式存储（不修改）
```

---

## Task 0: ForEach 公共方法 — `vego/collection.go`

Vego 内部有 `storage.GetAllValidDocuments()`，但它是内部 API，不对 `memory` 包暴露。Task 0 为 `vego.Collection` 新增公共遍历方法。

### 背景与问题

| 现状 | 问题 |
|------|------|
| `storage.GetAllValidDocuments()` 存在 | 位于 `storage` 包，是内部实现 |
| `collection.go` 无遍历 API | `memory` 包无法遍历 Collection 中的文档 |
| DeletionVector 标记删除 | 遍历时需跳过已删除文档 |

**阻塞关系**：Task 0 是 Task 4/6/7 的前置依赖。

### 设计方案

```go
// vego/collection.go

// ForEach 遍历集合中所有未删除的文档
// fn: 回调函数，返回 true 继续遍历，false 停止
// 返回: 遍历过程中的错误（如果 fn 返回 false 则不返回错误）
func (c *Collection) ForEach(fn func(*Document) bool) error {
    // 1. 获取 Collection 的 storage
    // 2. 调用 storage.GetAllValidDocuments() 获取所有文档
    // 3. 对每个文档检查 DeletionVector，未删除的调用 fn
    // 4. 如果 fn 返回 false，停止遍历（不是错误）
    // 5. 返回遍历错误（如果有）
}
```

### 实现要点

1. **获取 storage reader**：通过 `c.storage` 访问底层存储
2. **调用 `GetAllValidDocuments()`**：获取所有有效文档（内部已过滤 DeletionVector）
3. **类型转换**：将 storage 层的文档格式转换为 `vego.Document`
4. **回调调用**：对每个文档调用 `fn`，根据返回值决定是否继续
5. **错误处理**：IO 错误应立即终止遍历并返回

```go
func (c *Collection) ForEach(fn func(*Document) bool) error {
    c.mu.RLock()
    defer c.mu.RUnlock()

    // 获取 storage reader
    reader, err := c.storage.GetReader()
    if err != nil {
        return fmt.Errorf("get storage reader: %w", err)
    }
    defer reader.Close()

    // 遍历所有有效文档
    return reader.GetAllValidDocuments(func(doc *storage.Document) error {
        // 检查 DeletionVector
        if c.deletionVector != nil && c.deletionVector.IsDeleted(doc.RowID) {
            return nil // 跳过已删除文档
        }

        // 转换为 vego.Document
        vegoDoc := &Document{
            ID:       doc.ID,
            Vector:   doc.Vector,
            Metadata: doc.Metadata,
        }

        // 调用回调
        if !fn(vegoDoc) {
            return ErrStopIteration
        }
        return nil
    })
}
```

### 变体方案（可选）

如果需要更灵活的遍历能力，可以同时提供：

```go
// 带过滤条件的遍历
func (c *Collection) ForEachFiltered(fn func(*Document) bool, filter func(*Document) bool) error

// 只遍历指定状态的文档（active/paused/archived）
func (c *Collection) ForEachByState(state MemoryState, fn func(*Document) bool) error
```

但对于 memory 服务的初始需求，简单的 `ForEach` 足够了。

### 错误处理

| 场景 | 行为 |
|------|------|
| fn 返回 false | 正常停止，不返回错误 |
| IO 错误 | 立即终止，返回错误 |
| 回调返回错误 | 立即终止，返回错误 |

### 性能考量

- 遍历是**只读操作**，不需要写锁（使用 RLock）
- 对于 <100K 文档的全量遍历，耗时 <1s
- memory 包在 `Open()` 时调用，用于重建倒排索引和 ContentHashIndex

### 验收标准

- [ ] `ForEach` 方法存在于 `vego.Collection`
- [ ] 遍历返回所有未删除文档
- [ ] DeletionVector 标记的文档被正确跳过
- [ ] `memory` 包可以调用 `ForEach` 重建索引
- [ ] 已有测试覆盖（如果 Collection 有测试文件）

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
    SessionID    string                 // 关联会话 ID（ModeRaw 时标识来源会话）
    Seq          int                    // 消息序号（ModeRaw 时会话内排序）
    ContentHash  string                 // SHA256(content)，ModeRaw 去重用
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
    SessionID  string  // 按会话 ID 过滤
    Limit      int
    Offset     int
    MinScore   float64
}
```

### Memory ↔ Vego Document 双向转换

Vego 的 `Document` 只有 `ID`、`Vector`、`Metadata`、`Timestamp` 四个字段，没有 `Content` 字段。
因此 Memory 的所有业务字段必须编码到 `Document.Metadata` 中。

**序列化策略：单 JSON 字段 + 少量索引字段**

Vego 的 Metadata 是 `map[string]interface{}`，经 JSON 持久化往返后类型会退化
（`int→float64`、`[]string→[]interface{}`）。逐字段存储 + 逐字段类型断言随字段增多极易出错。

采用**双层结构**：将完整 Memory 序列化为单个 JSON 字符串存入 `_data`，
仅将搜索过滤需要的字段（`_state`、`_type`）作为独立 key 存储，供 `SearchWithFilter` 使用：

```go
const (
    metaKeyData  = "_data"  // 完整 Memory JSON 字符串
    metaKeyState = "_state" // 冗余索引字段，供 SearchWithFilter 过滤
    metaKeyType  = "_type"  // 冗余索引字段，供 SearchWithFilter 过滤
)

// memoryToDoc 将 Memory 转为 Vego Document 用于存储。
// vec 是 Embedding 向量，由调用方负责生成。
func memoryToDoc(m *Memory, vec []float32) (*vego.Document, error) {
    data, err := json.Marshal(m)
    if err != nil {
        return nil, fmt.Errorf("marshal memory: %w", err)
    }
    meta := map[string]interface{}{
        metaKeyData:  string(data),          // 完整 Memory JSON
        metaKeyState: string(m.State),       // 冗余：供 SearchWithFilter 过滤
        metaKeyType:  string(m.MemoryType),  // 冗余：供 SearchWithFilter 过滤
    }
    return &vego.Document{
        ID:        m.ID,
        Vector:    vec,
        Metadata:  meta,
        Timestamp: m.UpdatedAt,
    }, nil
}

// docToMemory 从 Vego Document 反序列化为 Memory。
// 使用 json.Unmarshal 一次性还原，避免逐字段类型断言的脆弱性。
func docToMemory(doc *vego.Document) (*Memory, error) {
    dataStr, ok := doc.Metadata[metaKeyData].(string)
    if !ok {
        return nil, fmt.Errorf("document %s: missing _data field", doc.ID)
    }
    var m Memory
    if err := json.Unmarshal([]byte(dataStr), &m); err != nil {
        return nil, fmt.Errorf("unmarshal memory %s: %w", doc.ID, err)
    }
    m.ID = doc.ID // 以 Document.ID 为准
    return &m, nil
}
```

**优势**：
- Memory 结构体增删字段时，只需修改结构体定义，转换函数无需变更
- `json.Unmarshal` 自动处理类型映射，不受 Vego Metadata 的 JSON 往返类型退化影响
- `_state` 和 `_type` 冗余存储仅 2 个字符串字段，开销极小，但支持 `SearchWithFilter` 在索引层过滤

**数据一致性**：`_state`/`_type` 与 `_data` 中的对应字段必须同步更新。
由于所有写入都经过 `memoryToDoc`，一致性由代码路径保证。

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
- **前置依赖**：Vego Collection 当前**无公共遍历 API**（内部 `GetAllValidDocuments()` 不可外部调用）。
  需要先为 Vego 新增 `ForEach(fn func(*Document) bool) error` 公共方法，
  封装内部的 `storage.GetAllValidDocuments()` 并跳过 DeletionVector 标记的文档
- 增量维护：Store/Update/Delete 时同步更新倒排索引
- 规模：内存中维护，适用于嵌入式场景（<100K 条记忆）
- **重建 benchmark 要求**：Task 10 中必须包含 100K 条记忆（平均 500 字符）的重建耗时 benchmark，
  验证 <1s 假设。中文 bigram 分词产生 ~2x token 量，需纳入测试

```go
// Open() 中的索引重建流程
func (s *MemoryStore) rebuildInvertedIndex() error {
    // 前置：使用新增的 coll.ForEach() 遍历所有未删除文档
    return s.coll.ForEach(func(doc *vego.Document) bool {
        m, err := docToMemory(doc)
        if err != nil {
            slog.Warn("skip corrupt document during rebuild", "id", doc.ID, "err", err)
            return true // continue
        }
        if m.State != StateActive {
            return true // 仅对 active 文档建索引
        }
        s.inverted.Add(m.ID, m.Content)
        // 若为 TypeSession 且有 ContentHash，同步重建去重索引
        if m.MemoryType == TypeSession && m.ContentHash != "" {
            s.contentHashIndex.Add(m.SessionID, m.ContentHash, m.ID)
        }
        return true // continue
    })
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
    SearchLimit       int     // 默认 10
    SearchOverFetch   int     // 默认 5（SearchWithFilterContext 的过取倍数）
    RRFK              float64 // 默认 60.0
    MinScore          float64 // 默认 0.3（基于相似度，0-1 范围）
    SecondHopGate     float64 // 默认 0.02（低阈值确保二跳在高归档率下仍能触发；RRF K=60 时 max RRF≈0.033）
    SecondHopWeight   float64 // 默认 0.3
    SecondHopTopN     int     // 默认 3
    PinnedBoost       float64 // 默认 1.5
    RecencyBoostWeek  float64 // 默认 1.05（<=7 天记忆的分数乘数）
    RecencyBoostMonth float64 // 默认 1.02（<=30 天记忆的分数乘数）
    GapStopRatio      float64 // 默认 0.5（相邻结果分数下降超过此比例时截断，0=禁用）

    // 摄取
    MaxFacts          int     // 默认 50
    MaxConversationRunes int  // 默认 1000000

    // 距离函数（用于相似度转换）
    DistanceFunc  string  // "cosine" | "l2" | "ip"，默认 "cosine"
}

func WithLLM(apiKey, baseURL, model string) Option
func WithEmbedding(apiKey, baseURL, model string, dims int) Option
func WithDistanceFunc(name string) Option
func WithRecencyBoost(weekMultiplier, monthMultiplier float64) Option
func WithGapStop(ratio float64) Option  // 0 禁用
// ... 更多 Option
```

---

## Task 6: MemoryStore 主 API — `memory/memory.go`

核心入口，封装 Vego DB + LLM + Embedding + 倒排索引。

```go
type MemoryStore struct {
    db               *vego.DB
    coll             *vego.Collection
    llm              *LLMClient           // 同包类型，无需 import 子包
    embedder         *Embedder            // 同包类型，无需 import 子包
    inverted         *InvertedIndex
    contentHashIndex *ContentHashIndex    // Session 消息 ContentHash 去重索引
    config           *Config
    mu               sync.Mutex           // 保护 Archive-and-Create 原子性
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

// 状态管理
func (s *MemoryStore) Pause(ctx context.Context, id string) error
func (s *MemoryStore) Resume(ctx context.Context, id string) error

// 浏览与统计
func (s *MemoryStore) List(ctx context.Context, filter MemoryFilter) ([]Memory, error)
func (s *MemoryStore) Stats(ctx context.Context) (*MemoryStats, error)

// 批量操作
func (s *MemoryStore) StoreBatch(ctx context.Context, items []StoreItem) ([]Memory, error)
func (s *MemoryStore) Bootstrap(ctx context.Context, limit int) ([]Memory, error)
```

**Vego 集成方式**：
- `Open()` 内部调用 `vego.Open(path)` 创建 DB，获取 `"memories"` Collection，然后调用 `coll.ForEach()` **遍历所有 active 文档重建倒排索引**，同时扫描 `TypeSession` 文档重建 **ContentHash 去重索引**，并执行 **崩溃恢复扫描**（检测 superseded_by 非空但仍为 active 的记忆）
- `Store()` 使用 `memoryToDoc()` 转换，`Embedder.Embed()` 生成向量，`coll.InsertContext()` 存入 Vego
- `Get()` 使用 `coll.GetContext()` 获取 Document，再用 `docToMemory()` 反序列化
- `Search()` 使用 **`coll.SearchWithFilter()`**（而非 `SearchContext`），配合 `MetadataFilter{Key:"_state", Op:"eq", Value:"active"}` 在搜索层过滤非 active 记忆，通过 `distanceToSimilarity()` 转换为相似度，再走倒排索引关键词搜索，最后 RRF 融合
- `Delete()` **不使用 `coll.DeleteContext()`**（因为它会移除 docToNode 映射，导致 `Get` 也失败，不符合 mem9 的软删除语义）。而是通过 **`coll.UpdateContext()`** 将 `_state` 改为 `"deleted"`、同步更新 `_data` 中的 State 字段。同时更新倒排索引 `inverted.Remove(id)`
- `Update()` 使用 Archive-and-Create 模式（见 Task 7 原子性设计）

**Delete 语义说明**：
mem9 的所有删除都是软删除——记录保留在数据库中，仅将 state 改为 `deleted`，仍可按 ID 查询。
Vego 的 `DeleteContext` 是 DeletionVector 软删除，但删除后 `Get` 也会失败。
因此本设计中 `Delete` 使用 `UpdateContext` 修改 state 字段来模拟 mem9 的软删除行为。
`SearchWithFilter` 会自动排除 `_state != "active"` 的文档。

**Pause / Resume**：
- `Pause` 将 active 记忆改为 `paused` 状态，同时从倒排索引移除（搜索不可见），但仍可通过 `Get` 查询
- `Resume` 将 paused 记忆恢复为 `active`，并重新加入倒排索引
- 只有状态严格匹配时才允许操作（Pause 要求 active，Resume 要求 paused）

**List**：
- 纯遍历接口，不调用 Embedding API
- 遍历全量 Document，经 `matchesFilter` 过滤后按 `UpdatedAt` 降序返回
- 支持 `MemoryFilter` 的全部过滤字段（State/Tags/MemoryType/AgentID/SessionID）和分页（Offset/Limit）

**Stats**：
- 封装 `s.coll.Stats()` 获取 Vego 层指标（Count/IndexNodes/OrphanNodes/DeletionRate）
- 遍历全量 Document 统计 memory 层状态分布（active/paused/archived/deleted）和各类型数量

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

### 统一摄取入口 (`Ingest`)

`Ingest()` 是摄取的统一入口方法，根据 mode 自动选择处理路径，并在处理前根据 `MaxConversationRunes` 截断超长消息。

```go
// IngestRequest 是消息摄取的统一入参。
// 支持两种模式：
//   - ModeNormal: LLM 事实提取 → Reconcile（需提供 AgentID）
//   - ModeRaw:    直接 session 存储 + 去重（需提供 SessionID）
type IngestRequest struct {
    Messages  []Message
    Mode      IngestMode
    SessionID string // ModeRaw 时必填
    AgentID   string // ModeNormal 时必填
}

// Ingest 编排完整摄取流水线：
//
// ModeNormal:
//  1. 按 MaxConversationRunes 截断消息
//  2. ExtractFacts → Reconcile
//
// ModeRaw:
//  1. 按 MaxConversationRunes 截断消息
//  2. StoreRawMessages（内部处理 ExtractFacts(ModeRaw) + 去重 + 插入）
func (s *MemoryStore) Ingest(ctx context.Context, req IngestRequest) (*IngestResult, error)
```

此方法同时解决了 `MaxConversationRunes` 的生效问题：截断逻辑仅在 `Ingest()` 入口处执行，确保 LLM 提示词不会因过长消息而超出上下文窗口。

### ModeRaw 消息去重（ContentHash）

编程 Agent（Claude Code、Copilot 等）常采用**累积式发送**：每次调用发送完整对话历史。
例如第 3 轮对话包含第 1、2、3 轮全部消息。不去重会导致存储膨胀、搜索污染、BM25 IDF 失真。

```go
import "crypto/sha256"

// computeContentHash 计算消息内容的去重指纹
func computeContentHash(content string) string {
    h := sha256.Sum256([]byte(content))
    return hex.EncodeToString(h[:])
}

// storeRawMessages 存储原始对话消息，自动跳过已存在的重复内容
func (s *MemoryStore) storeRawMessages(ctx context.Context, sessionID string, messages []Message) (int, error) {
    stored := 0
    // 获取该会话当前最大 Seq，确保跨调用单调递增
    nextSeq := s.contentHashIndex.MaxSeq(sessionID) + 1
    for _, msg := range messages {
        hash := computeContentHash(msg.Content)

        // 去重检查：同 sessionID + contentHash → 跳过
        if s.hasContentHash(sessionID, hash) {
            continue
        }

        mem := &Memory{
            ID:          uuid.New().String(),
            Content:     msg.Content,
            MemoryType:  TypeSession,
            State:       StateActive,
            Tags:        msg.Tags,
            AgentID:     msg.AgentID,
            SessionID:   sessionID,
            Seq:         nextSeq, // 会话内全局递增，而非 batch index
            ContentHash: hash,
            Version:     1,
            CreatedAt:   time.Now(),
            UpdatedAt:   time.Now(),
        }

        vec, _ := s.embedIfAvailable(ctx, msg.Content)
        doc, err := memoryToDoc(mem, vec)
        if err != nil {
            return stored, fmt.Errorf("marshal session message: %w", err)
        }
        if err := s.coll.InsertContext(ctx, doc); err != nil {
            return stored, fmt.Errorf("insert session message: %w", err)
        }
        s.inverted.Add(mem.ID, mem.Content)
        s.contentHashIndex.Add(sessionID, hash, mem.ID, nextSeq)
        nextSeq++
        stored++
    }
    return stored, nil
}

// hasContentHash 检查同一会话中是否已存在相同内容
// 使用内存索引 map[sessionID+hash]memoryID 做 O(1) 查找
func (s *MemoryStore) hasContentHash(sessionID, hash string) bool {
    return s.contentHashIndex.Has(sessionID, hash)
}
```

**ContentHash 索引**：

```go
// ContentHashIndex 用于 ModeRaw 的消息去重和 Seq 追踪
type ContentHashIndex struct {
    mu     sync.RWMutex
    index  map[string]string // key="sessionID:hash" → value=memoryID
    maxSeq map[string]int    // key=sessionID → value=该会话当前最大 Seq
}

func (idx *ContentHashIndex) Has(sessionID, hash string) bool
func (idx *ContentHashIndex) Add(sessionID, hash, memoryID string, seq int)
func (idx *ContentHashIndex) MaxSeq(sessionID string) int  // 不存在返回 -1
```

与倒排索引一样，不持久化，`Open()` 时从 Vego Collection 中扫描 `TypeSession` 类型文档重建。

### Phase 2: 调和 (`reconcile.go`)

```go
func (s *MemoryStore) Reconcile(ctx context.Context, agentID string, facts []ExtractedFact) (*IngestResult, error)
```

- 对每条事实并发搜索已有记忆（向量搜索 + 关键词搜索，4 worker，使用 `semaphore` 控制并发上限）
- 整数 ID 映射防止 LLM 幻觉
- 复用 mem9 的 ADD/UPDATE/DELETE/NOOP Prompt
- DELETE 执行软删除：通过 `coll.UpdateContext()` 修改 `_state` 为 `"deleted"` + 同步更新 `_data` + 倒排索引 `Remove()`。
  **不使用 `coll.DeleteContext()`**，原因见 Task 6 的 Delete 语义说明
- Pinned 记忆保护：不可自动删除，UPDATE 降级为 ADD
- **并发控制**：Reconcile 阶段的并发搜索使用 `semaphore` 限制并发数（默认 4），防止嵌入式场景下 goroutine 爆炸。
  Phase 2（决策 + 执行）目前**串行执行**（逐条 fact 处理）。LLM 调用通过 `MemoryStore.llmSem`（权重 1 的独立 semaphore）
  串行化，作为防御性措施——即使未来 Phase 2 改为并发执行，也不会出现 LLM API 限流问题

### Archive-and-Create 原子性设计

Vego Collection 没有事务机制。且 `UpdateContext` 是同 ID 替换（MarkDeleted + Put），
不适合 Archive-and-Create 的"新旧两条记忆共存"语义。

采用 **Insert-First + Mutex** 策略，使用两次独立操作：
- `InsertContext(newDoc)` 创建新记忆（新 ID）
- `UpdateContext(oldDoc)` 修改旧记忆的 `_state` 和 `_data`（同 ID 原地更新 metadata）

```go
func (s *MemoryStore) archiveAndCreate(ctx context.Context, oldID string, newMem *Memory, newVec []float32) error {
    s.mu.Lock()
    defer s.mu.Unlock()

    // 步骤 1：先插入新记忆（新 ID，此时旧记忆仍为 active，不影响搜索正确性）
    newDoc, err := memoryToDoc(newMem, newVec)
    if err != nil {
        return fmt.Errorf("marshal new memory: %w", err)
    }
    if err := s.coll.InsertContext(ctx, newDoc); err != nil {
        return fmt.Errorf("insert new memory: %w", err)
    }
    s.inverted.Add(newMem.ID, newMem.Content)

    // 步骤 2：通过 UpdateContext 将旧记忆标记为 archived
    // UpdateContext 会 MarkDeleted(旧版本) + Put(新版本)，同 ID 替换 metadata
    oldDoc, err := s.coll.GetContext(ctx, oldID)
    if err != nil {
        // 新记忆已插入但旧记忆更新失败 → 启动时恢复扫描会清理
        slog.Warn("archive old memory failed, orphan created", "old_id", oldID, "new_id", newMem.ID, "err", err)
        return nil // 不回滚，容忍短暂不一致
    }
    // 从 _data 反序列化旧 Memory，修改 state 和 superseded_by，再序列化回去
    oldMem, err := docToMemory(oldDoc)
    if err != nil {
        slog.Warn("corrupt old memory during archive", "old_id", oldID, "err", err)
        return nil
    }
    oldMem.State = StateArchived
    oldMem.SupersededBy = newMem.ID
    oldMem.UpdatedAt = time.Now()
    archivedDoc, err := memoryToDoc(oldMem, oldDoc.Vector) // 保留原向量
    if err != nil {
        slog.Warn("marshal archived memory failed", "old_id", oldID, "err", err)
        return nil
    }
    if err := s.coll.UpdateContext(ctx, archivedDoc); err != nil {
        slog.Warn("archive old memory state update failed", "old_id", oldID, "err", err)
    }
    s.inverted.Remove(oldID)

    return nil
}
```

**崩溃恢复**：`Open()` 启动时通过 `ForEach` 扫描所有文档，检测是否存在 superseded_by 非空但仍为 active 的记忆，自动修复为 archived。

---

## Task 8: 混合搜索 + RRF 融合 — `memory/search.go`

移植 mem9 的混合搜索流水线。

```go
func (s *MemoryStore) hybridSearch(ctx context.Context, query string, filter MemoryFilter) ([]Memory, error)
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

// activeStateFilter 构建用于 SearchWithFilter 的 state=active 过滤器
func activeStateFilter() vego.Filter {
    return &vego.MetadataFilter{
        Key:   metaKeyState,
        Op:    "eq",
        Value: string(StateActive),
    }
}

// toScoredMemories 将 SearchWithFilter 返回的结果转为带相似度分数的 Memory 列表。
// SearchWithFilter 已经过滤了 DeletionVector 标记的文档和非 active 状态的文档，
// 此处无需再做 state 检查。
func (s *MemoryStore) toScoredMemories(results []vego.SearchResult) ([]Memory, error) {
    out := make([]Memory, 0, len(results))
    for _, r := range results {
        m, err := docToMemory(r.Document)
        if err != nil {
            slog.Warn("skip corrupt document in search results", "id", r.Document.ID, "err", err)
            continue
        }
        m.Score = distanceToSimilarity(r.Distance, s.config.DistanceFunc)
        out = append(out, *m)
    }
    return out, nil
}
```

### 搜索过滤策略

Vego 的 `SearchWithFilter` 在 HNSW 搜索后应用 metadata filter，自动扩大批次重试
（初始 k×2，每次翻倍，最大 k×20，最多 5 次）。这意味着：

- **正常情况**（archived/deleted 记忆占比 <50%）：首次 k×OverFetch 即可获得足够 active 结果
- **高归档率场景**（>80% 记忆已归档）：回退到 k×20，仍可能不足
- **兜底策略**：如果搜索返回结果少于 `limit`，不做额外补偿。
  这对嵌入式场景可接受——如果 95% 的记忆都已归档，说明需要 `Compact` 清理

实际实现使用 `SearchWithFilterContext`（带 context 支持和 over-fetch 参数的增强版）
替代最初设计的 `SearchWithFilter`：
1. 一次性大搜索（k×OverFetch）+ 单次回退（k×20），而非迭代扩批
2. 在 HNSW 搜索回调中直接通过 `CheckVisibility` 过滤已删除/归档的文档
3. 通过 `SearchOverFetch` 配置控制过取倍数（默认 5，范围 [1, 20]）

### 搜索流水线（十阶段）

1. **时间归一化**：`NormalizeTemporalRecallQuery(query, now)`
2. **向量搜索**：调用 `s.coll.SearchWithFilterContext(queryVec, limit*3, activeFilter, WithOverFetch(searchOverFetch))` 走 Vego HNSW + DV 过滤 + metadata 过滤
3. **距离转换**：`toScoredMemories()` 将 distance 转为 similarity（`SearchWithFilter` 已过滤非 active，此处无需重复检查）
4. **相似度过滤**：丢弃 similarity < MinScore (0.3) 的结果
5. **关键词搜索**：调用 `s.inverted.Search(query, limit*3)` 走倒排索引
6. **RRF 融合**：`score[id] += 1/(K + rank+1)`，K=60
7. **二跳扩展**：Top-3 结果作为种子再搜，权重 0.3。使用已缓存的 embedding 向量直接做二跳搜索，避免重复调用 Embedding API
8. **类型加权**：pinned x 1.5
9. **时效加权（Recency Boost）**：近期记忆分数提升（见下文）
10. **排序 + Gap Stop 截断 + 分页 + 年龄标注**

### 时效加权（Recency Boost）

编程 Agent 的记忆具有明显的时效性：当前项目的架构决策、最近的技术选型比几个月前的更相关。
从 mem9 的 Confidence Scoring 中提取 recency 组件，作为 RRF 后的轻量乘数：

```go
// applyRecencyBoost 对近期记忆施加分数加成。
// 在类型加权之后、排序之前执行。
func applyRecencyBoost(scores map[string]float64, mems map[string]Memory, now time.Time, weekBoost, monthBoost float64) {
    for id, m := range mems {
        age := now.Sub(m.UpdatedAt)
        switch {
        case age <= 7*24*time.Hour:
            scores[id] *= weekBoost   // <=7 天：+5%
        case age <= 30*24*time.Hour:
            scores[id] *= monthBoost  // <=30 天：+2%
        }
        // >30 天：不加成
    }
}
```

**设计说明**：
- 乘数而非加数，确保与 RRF 分数量级一致
- 默认 5%/2% 的力度足以在同等语义相关度下让近期记忆胜出，但不会让低相关的新记忆压过高相关的旧记忆
- 可通过 `WithRecencyBoost(1.0, 1.0)` 完全禁用

### Gap Stop 截断

避免返回"长尾噪声"——前几条高度相关，后面的勉强过了 MinScore 但实际无关。
从 mem9 的 Confidence Gap Stop 简化而来，不依赖完整的置信度评分：

```go
// applyGapStop 当相邻结果分数骤降时截断后续结果。
// ratio=0 表示禁用。默认 ratio=0.5（分数下降超过 50% 时截断）。
func applyGapStop(sorted []Memory, ratio float64) []Memory {
    if ratio <= 0 || len(sorted) <= 1 {
        return sorted
    }
    for i := 1; i < len(sorted); i++ {
        if sorted[i].Score < sorted[i-1].Score*(1-ratio) {
            return sorted[:i]
        }
    }
    return sorted
}
```

**设计说明**：
- 默认 `GapStopRatio=0.5`，即前一条分数 0.8 时，下一条低于 0.4 即截断
- 比 mem9 的绝对阈值 18（依赖 0-100 置信度）更通用，适配 RRF 的相对分数
- 在排序之后、分页之前执行
- 可通过 `WithGapStop(0)` 完全禁用

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
func TemporalRecallProjection(content string, metadata map[string]interface{}, now time.Time) string
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
- **Memory ↔ Document 转换**：memoryToDoc / docToMemory 双向序列化正确性（通过 JSON 往返验证类型稳定性）
- **距离→相似度转换**：Cosine/L2/IP 三种距离函数的转换精度验证
- 倒排索引：分词、BM25 评分（含文档长度归一化）、增删改查
- RRF 融合：已知排名输入验证输出
- 时间感知：中英文相对时间解析、绝对日期检测、查询归一化
- **Archive-and-Create**：验证 insert-first 策略在正常和模拟崩溃场景下的正确性
- **Recency Boost**：验证 <=7 天 / <=30 天 / >30 天三个区间的分数乘数正确性；验证 `WithRecencyBoost(1.0, 1.0)` 可完全禁用
- **Gap Stop**：验证分数骤降时截断行为；验证 ratio=0 禁用；验证单条结果和全部同分的边界情况
- **ContentHash 去重**：验证同 sessionID + 相同内容只存一次；不同 sessionID 相同内容各存一次
- **Seq 单调递增**：验证跨多次 storeRawMessages 调用后 Seq 全局递增，无间断无重复
- **Delete 语义**：验证 Delete 后 `Get` 仍可返回记忆（state=deleted），`Search` 不返回

### 集成测试

- 完整 Ingest 流程：消息输入 → 事实提取 → 调和 → 记忆存储
- **ModeRaw 流程**：消息直接存为 TypeSession 类型记忆，无 LLM 调用
- **ModeRaw 累积式去重**：模拟 Agent 3 轮累积发送（第 1 轮 3 条、第 2 轮 5 条、第 3 轮 8 条），验证最终只存 8 条而非 16 条
- 混合搜索：向量 + 关键词 + RRF 融合验证 recall
- **SearchWithFilter 集成**：验证 archived/deleted 记忆不出现在搜索结果中
- **Recency Boost 集成**：新旧记忆语义相同时，验证近期记忆排名更靠前
- **Gap Stop 集成**：构造高相关 + 低相关混合结果集，验证长尾被截断
- 生命周期：Create → Update(Archive-and-Create) → Delete → Search 不可见 → Get 可见(state=deleted)
- Pinned 保护：Pinned 记忆不可被 LLM 自动删除/覆盖
- **倒排索引 + ContentHash 索引重建**：Close → Open 后两个内存索引一致性验证
- **崩溃恢复**：模拟 archiveAndCreate 中途失败后 Open() 的自动修复

### Benchmark

- **倒排索引重建**：100K 条记忆（平均 500 字符，中英文混合）的 `rebuildInvertedIndex()` 耗时，验证 <1s 目标
- **SearchWithFilter 高归档率**：在 80% 记忆已归档的场景下，验证搜索结果完整性和延迟

### Makefile 添加

```makefile
test-memory:
    go test -race -count=1 ./memory/...

test-memory-integration:
    VEGO_LLM_API_KEY=$(LLM_KEY) go test -tags=integration -race -count=1 -v ./memory/...
```

---

## 实现顺序（建议）

| 顺序 | Task | 依赖 | 说明 |
|------|------|------|------|
| 0 | **前置：Vego 新增 ForEach API** | 无 | 为 `vego/collection.go` 新增 `ForEach(fn func(*Document) bool) error`，封装内部 `GetAllValidDocuments()` |
| 1 | Task 1: 领域类型 + Document 转换 | 无 | 含单 JSON 字段序列化方案 |
| 2 | Task 5: 配置系统 | Task 1 | 小 |
| 3 | Task 2: LLM 客户端 | 无 | 中（移植） |
| 4 | Task 3: Embedding 客户端 | 无 | 小（移植） |
| 5 | Task 4: 倒排索引（标准 BM25） | Task 0 | 中，依赖 ForEach |
| 6 | Task 6: MemoryStore 主 API | Task 0-5 | 中，含 Delete 软删除语义 |
| 7 | Task 8: 混合搜索 + 距离转换 | Task 4, 6 | 中，含 SearchWithFilter + Recency Boost + Gap Stop |
| 8 | Task 9: 时间感知 | 无 | 大（移植） |
| 9 | Task 7: 智能摄取 + Archive-and-Create | Task 2, 3, 6, 8, 9 | 大（移植），含并发控制 |
| 10 | Task 10: 测试 + Benchmark | 全部 | 中，含 100K 重建 benchmark |

Task 0 必须最先完成。Task 2/3/4 可以并行实现；Task 8/9 可以并行实现。

---

## 附录：Review 修复清单

| # | 问题 | 修复方案 |
|---|------|---------|
| 1 | Vego 返回 Distance（越小越好），设计假设 Score（越大越好） | 新增 `distanceToSimilarity()` 转换层，支持 Cosine/L2/IP 三种距离函数 |
| 2 | Vego Document 无 Content 字段，映射关系不明确 | **单 JSON 字段方案**：完整 Memory 序列化为 `_data` 字符串，`_state`/`_type` 冗余存储供 SearchWithFilter 过滤 |
| 3 | Archive-and-Create 无事务保证，崩溃可能丢数据 | Insert-First + `sync.Mutex` 策略 + Open() 启动时崩溃恢复扫描 |
| 4 | 倒排索引序列化到 JSON 脆弱且冗余 | 不做持久化，Open() 时从 Vego 遍历 active 文档重建（<1s for <100K） |
| 5 | 缺少 `TypeSession` 记忆类型，ModeRaw 无法工作 | 新增 `TypeSession = "session"` |
| 6 | LLM 客户端依赖 Prometheus metrics，不适合嵌入式库 | 替换为 `slog` 结构化日志 |
| 7 | `memory/llm/` 和 `memory/embed/` 子包导致 import 路径膨胀和潜在冲突 | 扁平化为同包文件 `llm_client.go` 和 `embedder.go`，类型重命名为 `LLMClient` 和 `Embedder` |
| 8 | BM25 缺少文档长度归一化（b, avgdl），长文档总是得高分 | 使用标准 BM25 公式，新增 `docLen` 和 `totalTerms` 字段 |
| 9 | ModeRaw 无 ContentHash 去重，累积式发送导致存储膨胀和搜索污染 | Memory 新增 SessionID/Seq/ContentHash 字段 + ContentHashIndex 内存索引 + storeRawMessages 去重写入 |
| 10 | 搜索结果缺少时效敏感性，旧记忆与新记忆同等对待 | 从 mem9 Confidence Scoring 提取 Recency Boost：<=7 天 ×1.05，<=30 天 ×1.02，可配置/禁用 |
| 11 | 搜索返回长尾噪声，低相关结果充斥末尾 | 从 mem9 Gap Stop 简化为相对比例截断：相邻分数下降 >50% 时截断，可配置/禁用 |
| 12 | Vego Metadata `map[string]interface{}` 经 JSON 往返后 `int→float64`，逐字段断言脆弱 | 改用单 JSON 字段 `_data` 存储完整 Memory，`json.Unmarshal` 一次性还原，消除类型退化风险 |
| 13 | Vego `DeleteContext` 会移除 docToNode 映射导致 `Get` 失败，不符合 mem9 软删除语义 | Delete 改用 `UpdateContext` 修改 `_state` 为 `"deleted"`，保留 `Get` 可访问性 |
| 14 | Vego `SearchContext` 无 metadata 过滤，archived/deleted 记忆污染搜索结果 | 改用 `SearchWithFilter` + `activeStateFilter()`，Vego 内部自动扩批重试 |
| 15 | Vego Collection 无公共遍历 API，无法重建倒排索引 | 需为 Vego 新增 `ForEach(fn func(*Document) bool) error` 公共方法 |
| 16 | `RRF_K` 命名不符合 Go 风格 | 改为 `RRFK` |
| 17 | `storeRawMessages` 用 batch index `i` 作为 Seq，跨调用不连续 | 改用 `ContentHashIndex.MaxSeq(sessionID) + 1` 确保全局递增 |
| 18 | Archive-and-Create 直接操作 `doc.Metadata[metaKeyState]`，与单 JSON 字段方案不一致 | 改为反序列化旧 Memory → 修改 State/SupersededBy → 重新 `memoryToDoc` 序列化 → `UpdateContext` |
| 19 | Reconcile 阶段并发搜索和 LLM 调用无并发控制 | 使用 `semaphore` 限制搜索并发（默认 4）和 LLM 调用并发（默认 1） |
| 20 | 倒排索引重建 <1s 的假设未经验证 | Task 10 新增 100K 条记忆的重建 benchmark |
