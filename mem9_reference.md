# mem9 (mnemos) 完整技术参考文档

> 本文档汇总了 mem9 系统的所有核心功能和技术细节，供 Vego Agent Memory Service 开发过程中随时查阅。

---

## 1. 项目概览

### 1.1 定位

mem9 是面向 AI 编程 Agent 的**持久化共享记忆层**，解决 Agent 对话间的"失忆"问题。
核心设计目标：跨会话记忆持久化、跨 Agent 记忆共享、消除本地文件锁定、支持团队协作。

### 1.2 技术栈

| 组件 | 技术 |
|------|------|
| 后端语言 | Go 1.22+ |
| 主数据库 | TiDB Cloud Starter（原生 VECTOR 类型 + EMBED_TEXT） |
| 备选数据库 | PostgreSQL（pgvector）、DB9（AWS RDS） |
| 架构模式 | Handler → Service → Repository（严格分层，无 ORM） |
| Agent 插件 | Claude Plugin（Bash hooks）、OpenClaw（TypeScript）、OpenCode（TypeScript） |
| 前端 | React SPA（dashboard/app/） |
| 静态站点 | Astro（site/，Netlify 部署） |

### 1.3 模块结构

| 路径 | 职责 |
|------|------|
| `server/internal/handler/` | HTTP 路由、请求解析、响应格式化 |
| `server/internal/service/` | 业务逻辑（搜索、摄取、调和、时间感知） |
| `server/internal/repository/` | 存储抽象接口 + TiDB/PG/DB9 实现 |
| `server/internal/domain/` | 领域类型、错误定义 |
| `server/internal/llm/` | OpenAI 兼容 LLM 客户端 |
| `server/internal/embed/` | OpenAI 兼容 Embedding 客户端 |
| `server/internal/config/` | 配置加载（环境变量） |
| `server/internal/middleware/` | 认证、限流、多租户 |
| `server/internal/tenant/` | 租户生命周期、连接池管理 |
| `server/internal/metrics/` | Prometheus 指标 |

### 1.4 功能树

```
mnemos
├── Memory Core
│   ├── Ingestion（两阶段智能摄取）
│   ├── Storage（混合索引 + 加密存储）
│   ├── Recall & Search
│   │   ├── Vector Search（向量搜索）
│   │   ├── Keyword Search（关键词搜索）
│   │   └── Temporal Recall（时间感知召回）
│   └── Analysis & Insights
├── Agent Integrations
│   ├── Claude Plugin
│   ├── OpenClaw Plugin
│   └── OpenCode Plugin
├── Tenant & Infrastructure
│   ├── Tenant Provisioning
│   ├── TiDB Zero Auto-Setup
│   └── Stateless Hooks
└── Evaluation & Benchmarking
    ├── MR-NIAH Benchmark
    ├── LoCoMo Evaluation
    └── E2E Validation
```

---

## 2. 领域模型

### 2.1 Memory 结构体

```go
type Memory struct {
    ID           string          // UUID v4
    Content      string          // 记忆内容文本
    MemoryType   MemoryType      // pinned | insight | session
    Source       string          // 创建来源
    Tags         []string        // JSON 数组，永不为 NULL
    Metadata     json.RawMessage // 含 temporal 子字段
    Embedding    []float32       // 向量嵌入（不序列化到 JSON）

    AgentID      string          // 关联的 Agent
    SessionID    string          // 关联的会话
    UpdatedBy    string          // 最后更新者
    SupersededBy string          // 被哪条新记忆取代

    State        MemoryState     // active | paused | archived | deleted
    Version      int             // 乐观锁版本号
    CreatedAt    time.Time
    UpdatedAt    time.Time

    Score        *float64        // 搜索得分（查询时填充）
    Confidence   *int            // 置信度分数（0-100，查询时填充）
    RelativeAge  string          // "3 days ago"（查询时填充）
}
```

### 2.2 记忆类型

| 类型 | 说明 | 创建方式 |
|------|------|---------|
| `pinned` | 用户手动保存的高优先级知识 | 用户显式 Store 操作 |
| `insight` | LLM 从对话中自动提取的事实 | Ingest Pipeline Phase 2 |
| `session` | 原始对话消息 | ModeRaw 直接存储 |

### 2.3 记忆状态机

```
           ┌──────────────────────────┐
           │                          │
  Create → active ──── Pause ───→ paused
           │  ↑                    │
           │  └──── Resume ────────┘
           │
           ├──── Archive ───→ archived（终态，superseded_by 指向新记忆）
           │
           └──── Delete ────→ deleted（终态，软删除，物理行保留）
```

**关键规则：**
- 所有删除都是**软删除**，永不物理删除
- `archived` 和 `deleted` 是终态，不可恢复
- 仅 `active` 状态的记忆可被搜索召回
- `paused` 状态的记忆在搜索中被排除，但可被管理接口查询

### 2.4 Pinned 记忆特殊保护

| 操作 | 对 insight 记忆 | 对 pinned 记忆 |
|------|----------------|---------------|
| 搜索权重 | 1.0x | **1.5x**（RRF 加权提升） |
| LLM 自动 DELETE | 正常执行 | **禁止**，跳过操作 |
| LLM 自动 UPDATE | Archive-and-Create | **降级为 ADD**（旧记忆不变） |
| 用户手动删除 | 正常执行 | 正常执行 |

### 2.5 记忆版本控制

两个独立的版本概念：

**1. 乐观锁（Version 字段）**
- 每次 SQL UPDATE 时 `SET version = version + 1`
- 可选 expectedVersion 参数做冲突检测（`WHERE version = ?`）
- 不匹配时返回 `ErrWriteConflict`

**2. 历史链（SupersededBy 字段）**
- UPDATE 执行 Archive-and-Create：
  1. 旧记忆 state → archived，superseded_by → newID
  2. 新记忆 state = active，version = 1
  3. 两步在一个 SQL 事务中原子完成（`ArchiveAndCreate()`）
- 新旧记忆的 version 独立（新记忆从 1 开始）
- 可通过 superseded_by 链追溯完整历史

### 2.6 MemoryFilter 查询参数

```go
type MemoryFilter struct {
    Query      string   // 搜索文本
    Tags       []string // 按标签过滤（JSON_CONTAINS）
    Source     string   // 来源过滤
    State      string   // 状态过滤
    MemoryType string   // 类型过滤
    AgentID    string   // Agent 过滤
    SessionID  string   // 会话过滤
    Limit      int      // 结果数量限制（默认 50，最大 200）
    Offset     int      // 分页偏移
    MinScore   float64  // 最小相似度阈值（0=使用默认 0.3，-1=禁用）
}
```

### 2.7 错误类型

```go
var (
    ErrNotFound      = errors.New("not found")
    ErrConflict      = errors.New("conflict")
    ErrDuplicateKey  = errors.New("duplicate key")
    ErrValidation    = errors.New("validation error")
    ErrWriteConflict = errors.New("write conflict")
)

type ValidationError struct {
    Field   string
    Message string
}
```

---

## 3. 智能记忆摄取（Ingest Pipeline）

### 3.1 总体架构

```
对话消息 ──→ [Phase 1: 事实提取] ──→ [Phase 2: 调和] ──→ 记忆变更
                  │                        │
                  LLM                      LLM
             (14 条提取规则)          (ADD/UPDATE/DELETE/NOOP)
```

**两种模式：**
- `ModeSmart`：执行完整的两阶段 LLM 处理
- `ModeRaw`：直接存储消息，不经过 LLM（用于 session 类型记忆）

### 3.2 Phase 1: 事实提取

**入口函数：** `ExtractPhase1(ctx, agentName, req) ([]ExtractedFact, error)`

**LLM 提取规则（14 条）：**
1. 提取用户明确表达的偏好、决策和知识
2. 每条事实必须独立可理解（不依赖上下文）
3. 消歧代词（he/she/it → 具体实体名）
4. 保留原始语言（中文事实用中文输出）
5. 保留精确的技术标识符（版本号、配置项、路径等）
6. 时间表达保持原样（不预解析"yesterday"等）
7. 不提取纯粹的查询意图（标记为 `query_intent` 后丢弃）
8. 合并相关的碎片信息为完整事实
9. 从代码讨论中提取架构决策和约定
10. 保留因果关系（"X because Y"）
11. 区分"用户说的"和"Agent 推断的"
12. 不超过 50 条事实
13. 输出 JSON 格式 `{"facts": [{"text": "...", "tags": [...]}]}`
14. 如果无有价值事实，返回 `{"facts": []}`

**额外的消息标签规则（4 条）：**
1. 为每条消息打标签（`topic`, `decision`, `question` 等）
2. 标签反映消息的主要意图
3. 一条消息可有多个标签
4. JSON 格式 `{"message_tags": [{"idx": 0, "tags": [...]}]}`

**容错机制：**
- JSON 解析失败 → 自动重试
- 重试仍失败 → raw fallback 降级（将整段对话作为单条 `raw_fallback` 事实）
- Markdown 围栏 → `StripMarkdownFences()` 自动清理
- 事实上限 50 条（超出截断）
- 对话长度上限 1,000,000 rune

### 3.3 Phase 2: 调和（Reconcile）

**入口函数：** `ReconcilePhase2(ctx, agentName, agentID, facts, sessionID) (*IngestResult, error)`

**三步流程：**

**步骤 1：搜索已有记忆**
```
对每条事实 ─→ 并发搜索（4 worker）
              ├── 向量搜索（limit=5，MinScore=0.3）
              └── 关键词/FTS 搜索（limit=5）
              合并去重 ─→ 每条事实最多关联 5 条已有记忆
              全部事实共享最多 60 条已有记忆
```

**步骤 2：LLM 决策**
- 将所有事实 + 所有已有记忆一次性发给 LLM
- 已有记忆使用**整数 ID 映射**（`1`, `2`, `3`...）防止 LLM 幻觉出不存在的 UUID
- LLM 对每条事实做出决策：
  - `ADD`：创建新记忆
  - `UPDATE <id>`：更新已有记忆（Archive-and-Create）
  - `DELETE <id>`：删除已有记忆
  - `NOOP`：不需要操作
- 年龄信息作为辅助参考（更老的记忆倾向于被更新）

**步骤 3：执行动作**

| 动作 | 执行逻辑 |
|------|---------|
| ADD | `addInsight()` → 生成 embedding → 创建新 insight 记忆 |
| UPDATE | `updateInsight()` → ArchiveAndCreate（旧记忆归档 + 新记忆创建，原子事务） |
| DELETE | `SetState(deleted)` → 软删除。**如果目标是 pinned 记忆则跳过** |
| NOOP | 无操作 |

**UPDATE 对 Pinned 记忆的降级：**
当 LLM 决定 UPDATE 一条 pinned 记忆时，不执行 Archive-and-Create，而是降级为 ADD（创建新记忆，旧的 pinned 记忆不变）。

### 3.4 IngestResult 输出

```go
type IngestResult struct {
    Status          string   // "complete" | "partial" | "failed"
    MemoriesChanged int      // ADD + UPDATE 执行数量
    InsightIDs      []string // 新创建的记忆 ID 列表
    Warnings        int      // 警告计数
    Error           string   // 错误信息
}
```

### 3.5 NearDupSearch（近似去重）

调和阶段会对每条事实调用 `NearDupSearch(factText)` 查找最近似的已有记忆。
**当前实现仅用于可观测性**（记录余弦相似度到 Prometheus metrics），**不执行实际去重抑制**。
未来可基于此实现自动跳过高度重复的事实。

### 3.6 Raw Mode（ModeRaw）

不经过 LLM，直接将消息内容存储为单条记忆：
- 类型：`insight`（带 `raw-fallback` 标签）
- 自动生成 embedding（如果 embedder 可用）
- 适用于快速存储或 LLM 不可用的降级场景

---

## 4. 混合搜索召回（Hybrid Search）

### 4.1 搜索策略路由

```go
func (s *MemoryService) Search(ctx, filter) {
    switch {
    case autoModel != "":
        return autoHybridSearch(...)    // TiDB 服务端自动 embedding（优先级最高）
    case embedder != nil:
        return hybridSearch(...)        // 客户端 embedding
    case ftsAvailable:
        return ftsOnlySearch(...)       // 仅全文搜索
    default:
        return keywordOnlySearch(...)   // 仅关键词（LIKE）
    }
}
```

> 注意：autoModel 优先级最高，设置后无论 embedder 是否存在都走 autoHybridSearch。

### 4.2 完整搜索流水线（autoHybridSearch）

```
查询文本
  │
  ▼
[1. 时间归一化] NormalizeTemporalRecallQuery(query, now)
  │              "last week" → "week of 2026-04-07 to 2026-04-13"
  ▼
[2. 向量搜索] AutoVectorSearch(query, filter, limit*3)
  │            或 VectorSearch(queryVec, filter, limit*3)
  ▼
[3. 最小分数过滤] 丢弃 score < MinScore (默认 0.3)
  │
  ▼
[4. 关键词搜索] FTSSearch(query, filter, limit*3)
  │              或 KeywordSearch(query, filter, limit*3)
  ▼
[5. RRF 融合] score[id] += 1/(60 + rank+1)
  │            向量和关键词各自的排名贡献分数
  ▼
[6. 二跳扩展] 如果 maxVecScore >= 0.5:
  │            Top-3 结果作为种子 → 并发向量搜索 → 去重 → 权重 0.3
  ▼
[7. 类型加权] pinned × 1.5, insight × 1.0
  │
  ▼
[8. 排序 + 分页 + 年龄标注]
  │
  ▼
返回 []Memory（带 Score 和 RelativeAge）
```

### 4.3 RRF 融合算法

**Reciprocal Rank Fusion (K=60)**

```go
const rrfK = 60.0

func rrfMerge(ftsResults, vecResults []Memory) map[string]float64 {
    scores := make(map[string]float64)
    for rank, m := range ftsResults {
        scores[m.ID] += 1.0 / (rrfK + float64(rank+1))
    }
    for rank, m := range vecResults {
        scores[m.ID] += 1.0 / (rrfK + float64(rank+1))
    }
    return scores
}
```

- 同时出现在向量和关键词结果中的记忆得分更高
- K=60 使得排名差异平滑化，避免头部结果过度主导

### 4.4 二跳语义扩展（Second-Hop Search）

**触发条件：** 最佳首跳向量得分 >= 0.5（`secondHopGateScore`）

**流程：**
1. 从首跳 RRF 结果中取 Top-3（`secondHopTopN`）
2. 用这 3 条记忆的 embedding 向量（已缓存）并发做向量搜索
3. 收集结果，排除种子 ID，按余弦相似度去重取最高分
4. 丢弃分数 < 0.3 的结果
5. 按余弦相似度排序，作为二跳排名
6. 二跳 RRF 权重：`0.3 / (60 + rank+1)`

**设计意图：** 首跳可能遗漏语义相关但用词不同的记忆。通过已找到的高相关记忆作为"跳板"，扩大召回范围。权重 0.3 确保间接匹配不会压过直接命中。

### 4.5 类型加权

```go
func applyTypeWeights(mems map[string]Memory, scores map[string]float64) {
    for id, m := range mems {
        if m.MemoryType == TypePinned {
            scores[id] *= 1.5  // Pinned 记忆 1.5 倍加权
        }
    }
}
```

### 4.6 最小分数控制（MinScore）

| 值 | 含义 |
|----|------|
| `0` | 使用默认阈值 0.3 |
| `0.3` | 丢弃相似度 < 0.3 的结果（默认） |
| `-1` | 禁用阈值，返回所有结果 |
| `0.5` | 更严格的过滤 |

### 4.7 数据库层搜索实现

**向量搜索（TiDB）：**
```sql
SELECT *, VEC_COSINE_DISTANCE(embedding, ?) AS score
FROM memories
WHERE state = 'active' AND embedding IS NOT NULL
ORDER BY VEC_COSINE_DISTANCE(embedding, ?) ASC
LIMIT ?
```
注意：`VEC_COSINE_DISTANCE(...)` 在 SELECT 和 ORDER BY 中必须逐字节一致。

**自动向量搜索（TiDB Serverless）：**
```sql
SELECT *, VEC_EMBED_COSINE_DISTANCE(?, embedding) AS score
FROM memories
WHERE state = 'active' AND embedding IS NOT NULL
ORDER BY VEC_EMBED_COSINE_DISTANCE(?, embedding) ASC
LIMIT ?
```
TiDB Serverless 自动将查询文本转为向量。

**全文搜索（FTS）：**
```sql
SELECT *, FTS_MATCH_WORD(content, ?) AS fts_score
FROM memories
WHERE state = 'active' AND FTS_MATCH_WORD(content, ?)
ORDER BY fts_score DESC
LIMIT ?
```

**关键词搜索（降级方案）：**
```sql
SELECT * FROM memories
WHERE state = 'active' AND content LIKE ?
LIMIT ?
```

### 4.8 置信度评分（Confidence Scoring）

用于多池召回时的最终排序和过滤：

```
confidence = 55% × RRF分数归一化
           + 20% × 向量余弦相似度（归一化到 0.3-1.0 区间）
           + 10% × 双通道命中奖励（同时在向量和关键词中出现）
           + recency 奖励（<=7天: +5%, <=30天: +2%）
           + evidence 奖励（按查询类型和内容匹配度计算）
           + sourcePrior（按查询形状和来源池计算的先验加权）
```

> 注意：向量相似度归一化公式为 `(similarity - 0.30) / 0.70`，将 0.3-1.0 范围映射到 0-1。

**多池召回参数：**

| 池 | 最大候选数 | 最小置信度 | 最大保留数 |
|----|-----------|-----------|-----------|
| pinned | 5 | 70 | 2 |
| insight | 10 | 65 | - |
| session | 10 | 65 | - |

**Gap Stop 机制：** 排名相邻的两条记忆置信度差 >= 18 时，截断后续结果。

---

## 5. 时间感知系统（Temporal Awareness）

### 5.1 为什么需要时间感知

**问题：** 记忆中的"明天有会议"在存储时是有意义的，但 3 天后再召回时"明天"已经指向了错误的日期。

**解决方案：** 在事实提取阶段将相对时间表达解析为绝对日期，存入 metadata；在搜索阶段将查询中的相对时间也做同样的归一化。

### 5.2 TemporalMetadata 结构

```go
type TemporalMetadata struct {
    Kind          string // 时间类型
    AnchorSource  string // 锚点来源
    Granularity   string // 粒度
    ResolvedStart string // 解析后的开始日期 "2026-04-14"
    ResolvedEnd   string // 解析后的结束日期 "2026-04-14"
    Display       string // 人类可读展示
}
```

### 5.3 四种时间类型（Kind）

| Kind | 说明 | 示例 |
|------|------|------|
| `explicit_absolute` | 文本中包含明确的绝对日期 | "2026年4月14日有会议" |
| `deictic_relative` | 指示性相对时间，锚点为当前时间 | "明天有会议"→ 2026-04-15 |
| `header_anchor_relative` | 相对时间，锚点为消息头部的日期 | 消息头 [on 13 April, 2026] + "昨天" → 2026-04-12 |
| `local_anchor_relative` | 文本内部包含锚点+偏移 | "2025年4月13日的后一天" → 2025-04-14 |

### 5.4 锚点来源（AnchorSource）

| 来源 | 说明 |
|------|------|
| `local` | 从事实文本内部提取 |
| `header` | 从对话消息头部提取（如 `[on 13 April, 2026]`） |
| `now` | 使用当前系统时间 |

### 5.5 粒度（Granularity）

`day` | `week` | `month` | `year` | `season`

### 5.6 支持的时间表达（正则解析）

**绝对日期：**
- ISO 格式：`2026-04-14`
- 英文长格式：`14 April 2026`、`April 14, 2026`
- 月-年：`January 2026`
- 中文完整：`2026年4月14日`
- 中文月日：`4月14日`
- 中文年月：`2026年4月`
- 纯年份：`2026`

**英文相对时间：**
- yesterday, today, tomorrow
- last/this/next week, weekend, month, year
- last/this/next summer, winter, spring, fall, autumn
- last/next Friday, Saturday, Sunday, Monday...
- the past week, the past weekend

**中文相对时间：**
- 前天、昨天、今天、明天、后天
- 上周、本周、这周、下周
- 上周一～上周天、本周一～本周天、下周一～下周天（32 种组合）
- 上个月、这个月、本月、下个月
- 去年、今年、明年

**锚定周期：**
- "the week before 14 April, 2026"
- "the month after January 2026"

### 5.7 三个核心函数

**1. `NormalizeTemporalFacts(facts, messages, now)` — 事实提取后**

对每条提取的事实：
1. 检查遗留标注格式 `(display|raw)`，提取 display
2. 尝试 local anchor relative（中文"某日期的后/前N天"）→ 改写 + 生成 metadata
3. 检查是否包含绝对日期 → 直接返回，标记为 `explicit_absolute`
4. 尝试 header anchor relative → 用消息头日期作为锚点解析
5. 最后 fallback → 用当前时间做 deictic relative

**2. `NormalizeTemporalRecallQuery(query, now)` — 搜索查询时**

将查询中的相对时间替换为绝对日期：
- "last week 的会议" → "2026-04-07 to 2026-04-13 的会议"
- "明天的日程" → "2026-04-15 的日程"

**3. `TemporalRecallProjection(content, metadata)` — 结果展示时**

当记忆内容包含相对时间时，追加时间后缀：
- "明天有会议" → "明天有会议 [time: 2026-04-15]"

### 5.8 锚点提取与选择

**从消息头部提取锚点：**
- 正则匹配 `[on 13 April, 2026]` 或 `[date: 13 April 2026]`
- 支持方括号标注格式

**锚点选择算法（多候选时）：**
- 将事实文本分词为 token 集合
- 对每个锚点候选计算 token 重叠度
- 选择重叠最高的锚点；如果歧义（多个同分）则放弃锚点使用 `now`

### 5.9 中文周几解析

32 种 token：上周一～上周天、本周一～本周天、这周一～这周天、下周一～下周天

**周起始规则：** 周一为一周起始（周日 = 7）

```go
func startOfChineseWeek(t time.Time) time.Time {
    wd := t.Weekday()
    if wd == 0 { wd = 7 }  // 周日=7
    return t.AddDate(0, 0, -int(wd-1))  // 回到周一
}
```

---

## 6. LLM 客户端

### 6.1 配置

```go
type Config struct {
    APIKey      string   // 必填（为空则 New() 返回 nil）
    BaseURL     string   // 默认 "https://api.openai.com/v1"
    Model       string   // 默认 "gpt-4o-mini"
    Temperature float64  // 默认 0.1
    DebugLLM    bool     // 日志中输出原始 LLM 响应
}
```

### 6.2 核心方法

```go
func (c *Client) Complete(ctx, system, user string) (string, error)      // 普通完成
func (c *Client) CompleteJSON(ctx, system, user string) (string, error)  // JSON 强制模式
func ParseJSON[T any](raw string) (T, error)                            // Markdown-aware JSON 解析
func StripMarkdownFences(s string) string                                // 去除 ```json...```
```

### 6.3 兼容性设计

| 特性 | 策略 |
|------|------|
| `response_format: json_object` | 如果 HTTP 400 → 自动重试不带 format（兼容 Ollama/vLLM） |
| `enable_thinking` 参数 | Qwen 模型自动禁用；其他模型如果 400 → 重试不带该参数 |
| 超时 | 120 秒 |
| 错误类型 | `HTTPStatusError{Code, Body}` 允许调用方按状态码处理 |

### 6.4 Token 计量

支持两种格式：
- OpenAI 风格：`usage.prompt_tokens_details.cached_tokens`
- Anthropic 风格：`usage.cache_creation_input_tokens` + `usage.cache_read_input_tokens`

---

## 7. Embedding 客户端

### 7.1 配置

```go
type Config struct {
    APIKey  string  // "local" 或空串 for Ollama
    BaseURL string  // 默认 "https://api.openai.com/v1"，可改为 http://localhost:11434/v1
    Model   string  // 默认 "text-embedding-3-small"
    Dims    int     // 默认 1536
}
```

### 7.2 核心方法

```go
func New(cfg Config) *Embedder                               // apiKey 和 baseURL 都为空则返回 nil
func (e *Embedder) Embed(ctx context.Context, text string) ([]float32, error)
func (e *Embedder) Dims() int
```

### 7.3 请求格式

```json
{
    "model": "text-embedding-3-small",
    "input": "要嵌入的文本",
    "encoding_format": "float"
}
```

`encoding_format: "float"` 兼容 OpenAI、Ollama、LM Studio。

---

## 8. 多租户与配置

### 8.1 服务端配置（环境变量）

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `PORT` | 8080 | 服务端口 |
| `MNEMO_DSN` | 必填 | 数据库连接串 |
| `MNEMO_DB_BACKEND` | tidb | 数据库后端：tidb/postgres/db9 |
| `MNEMO_EMBED_AUTO_MODEL` | - | TiDB 服务端自动 embedding 模型 |
| `MNEMO_EMBED_AUTO_DIMS` | 1024 | 自动 embedding 维度 |
| `MNEMO_EMBED_API_KEY` | - | 客户端 embedding API Key |
| `MNEMO_EMBED_BASE_URL` | - | 客户端 embedding Base URL |
| `MNEMO_EMBED_MODEL` | text-embedding-3-small | 客户端 embedding 模型 |
| `MNEMO_EMBED_DIMS` | 1536 | 客户端 embedding 维度 |
| `MNEMO_LLM_API_KEY` | - | LLM API Key（smart ingest 必填） |
| `MNEMO_LLM_BASE_URL` | OpenAI | LLM Base URL |
| `MNEMO_LLM_MODEL` | gpt-4o-mini | LLM 模型 |
| `MNEMO_LLM_TEMPERATURE` | 0.1 | LLM 温度 |
| `MNEMO_INGEST_MODE` | smart | 默认摄取模式 |
| `MNEMO_FTS_ENABLED` | false | 启用全文搜索 |
| `MNEMO_ENCRYPT_TYPE` | plain | 加密类型：plain/md5/kms |
| `MNEMO_RATE_LIMIT` | 100 | 限流（请求/秒） |
| `MNEMO_RATE_BURST` | 200 | 突发限流 |
| `MNEMO_TIDB_ZERO_ENABLED` | true | TiDB Zero 自动配置 |
| `MNEMO_DEBUG_LLM` | false | 调试日志 |

### 8.2 两种 Embedding 模式

| 模式 | 配置 | 原理 | 适用场景 |
|------|------|------|---------|
| Auto（服务端） | `MNEMO_EMBED_AUTO_MODEL` | TiDB `EMBED_TEXT()` 在数据库端自动生成向量 | TiDB Cloud Serverless |
| Client（客户端） | `MNEMO_EMBED_API_KEY` | 应用层调用 OpenAI/Ollama API 生成向量后写入 | 任意数据库 |

**重要规则：** Auto 模式下不写 `embedding` 列（由 TiDB 自动生成）。

### 8.3 多租户架构

- 请求头 `X-Mnemo-Agent-Id` 标识 Agent
- 请求头 `X-API-Key` 用于租户验证
- 每个租户独立数据库，连接池管理：
  - MaxIdle: 5, MaxOpen: 10, IdleTimeout: 10min
  - 全局总连接数限制: 200
- 中间件 `ResolveTenant` 从 URL 路径提取 tenantID → 验证 → 获取 DB 连接 → 注入 Context

---

## 9. Repository 接口

### 9.1 MemoryRepo 接口（存储后端必须实现）

```go
type MemoryRepo interface {
    // CRUD
    Create(ctx context.Context, m *domain.Memory) error
    GetByID(ctx context.Context, id string) (*domain.Memory, error)
    UpdateOptimistic(ctx context.Context, m *domain.Memory, expectedVersion int) error
    SoftDelete(ctx context.Context, id, agentName string) error
    ArchiveMemory(ctx context.Context, id, supersededBy string) error
    ArchiveAndCreate(ctx context.Context, archiveID, supersededBy string, newMem *domain.Memory) error
    SetState(ctx context.Context, id string, state domain.MemoryState) error
    List(ctx context.Context, f domain.MemoryFilter) ([]domain.Memory, int, error)
    Count(ctx context.Context) (int, error)
    BulkCreate(ctx context.Context, memories []*domain.Memory) error

    // 搜索
    VectorSearch(ctx context.Context, queryVec []float32, f domain.MemoryFilter, limit int) ([]domain.Memory, error)
    AutoVectorSearch(ctx context.Context, queryText string, f domain.MemoryFilter, limit int) ([]domain.Memory, error)
    KeywordSearch(ctx context.Context, query string, f domain.MemoryFilter, limit int) ([]domain.Memory, error)
    FTSSearch(ctx context.Context, query string, f domain.MemoryFilter, limit int) ([]domain.Memory, error)
    FTSAvailable() bool

    // 其他
    ListBootstrap(ctx context.Context, limit int) ([]domain.Memory, error)
    NearDupSearch(ctx context.Context, queryText string) (string, float64, error)
    CountStats(ctx context.Context) (int64, int64, error)
}
```

### 9.2 SessionRepo 接口

```go
type SessionRepo interface {
    BulkCreate(ctx context.Context, sessions []*domain.Session) error
    PatchTags(ctx context.Context, sessionID, contentHash string, tags []string) error
    AutoVectorSearch(ctx context.Context, query string, f domain.MemoryFilter, limit int) ([]domain.Memory, error)
    VectorSearch(ctx context.Context, queryVec []float32, f domain.MemoryFilter, limit int) ([]domain.Memory, error)
    FTSSearch(ctx context.Context, query string, f domain.MemoryFilter, limit int) ([]domain.Memory, error)
    KeywordSearch(ctx context.Context, query string, f domain.MemoryFilter, limit int) ([]domain.Memory, error)
    FTSAvailable() bool
    ListBySessionIDs(ctx context.Context, sessionIDs []string, limitPerSession int) ([]*domain.Session, error)
}
```

### 9.3 SQL 存储规则

- Tags 是 JSON 数组；存 `[]`，永不 `NULL`
- 用 `JSON_CONTAINS` 过滤 tags
- 向量搜索必须包含 `embedding IS NOT NULL`
- `VEC_COSINE_DISTANCE(...)` 在 SELECT 和 ORDER BY 中必须逐字节一致
- `INSERT ... ON DUPLICATE KEY UPDATE` 是标准 upsert 模式
- 原子版本递增：`SET version = version + 1`

---

## 10. 关键常量汇总

| 常量 | 值 | 用途 |
|------|-----|------|
| `rrfK` | 60.0 | RRF 融合常数 |
| `defaultMinScore` | 0.3 | 向量搜索最小相似度阈值 |
| `secondHopWeight` | 0.3 | 二跳搜索 RRF 权重 |
| `secondHopTopN` | 3 | 二跳种子数量 |
| `secondHopGateScore` | 0.5 | 触发二跳的最小首跳分数 |
| `maxContentLen` | 50000 | 单条记忆最大字符数 |
| `maxTags` | 20 | 单条记忆最大标签数 |
| `maxBulkSize` | 100 | 批量创建最大数量 |
| `maxExtractionConversationRunes` | 1000000 | Phase 1 对话最大长度 |
| `maxFacts` | 50 | Phase 1 最大事实数 |
| `pinnedBoost` | 1.5 | Pinned 记忆搜索权重倍数 |
| `defaultLimit` | 50 | 默认搜索结果数 |
| `maxLimit` | 200 | 最大搜索结果数 |
| `syncIngestTimeout` | 9min | 同步摄取超时 |
| `defaultConfidenceGapStop` | 18 | 置信度排名截断间隔 |
| `defaultPinnedMinConfidence` | 70 | Pinned 池最小置信度 |
| `defaultMixedMinConfidence` | 65 | 混合池最小置信度 |

---

## 11. Session 消息存储

### 11.1 Session 结构

```go
type Session struct {
    ID          string
    SessionID   string      // 关联的会话
    AgentID     string      // 关联的 Agent
    Source      string
    Seq         int         // 消息序号
    Role        string      // user | assistant
    Content     string
    ContentType string
    ContentHash string      // SHA256，用于去重
    Tags        []string
    Embedding   []float32
    State       MemoryState
    CreatedAt   time.Time
    UpdatedAt   time.Time
}
```

### 11.2 去重机制

- 每条消息计算 `SHA256(content)` 作为 ContentHash
- 相同 SessionID + ContentHash 的消息只存一次
- 支持 Agent 累积式发送（每次发全量对话），通过 hash 去重只保留新增消息

---

## 12. 设计决策备忘

### 12.1 CRDT 被明确拒绝

曾有 CRDT（Conflict-free Replicated Data Type）提案用于多 Agent 并发写入场景，最终被拒绝。
原因：LLM 驱动的调和（reconciliation）已经能有效处理冲突，且实现复杂度远低于 CRDT。

### 12.2 Archive-and-Create 而非原地更新

UPDATE 不使用原地修改，而是归档旧记忆 + 创建新记忆：
- 保留完整历史链（审计友好）
- 避免并发 UPDATE 的版本冲突
- 简化回滚逻辑

### 12.3 多数据库后端

| 后端 | 特点 | Auto Embedding |
|------|------|---------------|
| TiDB | 原生 VECTOR 类型，原生 FTS | 支持（EMBED_TEXT） |
| PostgreSQL | pgvector 扩展 | 不支持 |
| DB9 | AWS RDS 兼容 | 有限支持 |

### 12.4 插件无状态设计

所有 Agent 插件（Claude、OpenClaw、OpenCode）都是**无状态**的：
- 不在本地存储任何记忆数据
- 每次操作都通过 HTTP API 与 server 交互
- 插件可随时删除/重装不丢失数据
