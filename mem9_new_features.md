# mem9 新增功能分析（对比 mem9_reference.md）

> 分析时间：2026-04-14
> 对比基准：`/Users/wangyang/vego/mem9_reference.md`

---

## 1. 文件上传/导入系统（重大新功能）

参考文档完全未覆盖。这是一套完整的异步文件处理流水线。

### 领域模型

`server/internal/domain/upload.go`

```go
type TaskStatus string // pending | processing | done | failed
type FileType string   // session | memory

type UploadTask struct {
    TaskID      string
    TenantID    string
    FileName    string
    FilePath    string     // 不暴露给 API
    AgentID     string
    SessionID   string
    FileType    FileType
    TotalChunks int
    DoneChunks  int
    Status      TaskStatus
    ErrorMsg    string
    CreatedAt   time.Time
    UpdatedAt   time.Time
}
```

### API 端点（v1alpha1 和 v1alpha2 均有）

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/{tenantID}/imports` | 文件上传（multipart form，最大 50MB） |
| GET | `/{tenantID}/imports` | 列出该租户的所有上传任务 |
| GET | `/{tenantID}/imports/{id}` | 获取单个任务详情 |

### 后台 Worker

`server/internal/service/upload.go`

- **UploadWorker**：每 5 秒轮询 `FetchPending`，并发处理（默认 5 个 worker）
- **文件解析**：支持 JSON、JSONL、OpenClaw JSONL 格式、Markdown/纯文本降级
- **分块处理**：Session 文件按 50 条消息分块（`uploadChunkSize = 50`），Memory 文件按 100 条分批（`uploadMemoryBatchSize = 100`）
- **崩溃恢复**：Checkpoint-before-work 模式 —— `done_chunks` 在处理前更新，崩溃重启后跳过已完成的 chunk
- **文件清理**：仅在状态更新成功后才删除文件
- **任务超时**：`defaultTaskTimeout = 30min`

### OpenClaw JSONL 格式支持

```go
// 支持两种 JSONL 行格式：
// 1. 简单格式：{"role": "user", "content": "..."}
// 2. OpenClaw 格式：{"type": "message", "message": {"role": "user", "content": [{"type": "text", "text": "..."}]}}
```

### 配置项

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `MNEMO_UPLOAD_DIR` | `./uploads` | 文件存储目录 |
| `MNEMO_WORKER_CONCURRENCY` | 5 | Worker 并发数 |

### Repository 接口

```go
type UploadTaskRepo interface {
    Create(ctx, task)
    GetByID(ctx, taskID)
    ListByTenant(ctx, tenantID)
    UpdateStatus(ctx, taskID, status, errorMsg)
    UpdateProgress(ctx, taskID, doneChunks)
    UpdateTotalChunks(ctx, taskID, totalChunks)
    FetchPending(ctx, limit)
    ResetProcessing(ctx, staleTimeout) (int64, error)
}
```

### 数据库表

```sql
CREATE TABLE IF NOT EXISTS upload_tasks (
  task_id       VARCHAR(36)   PRIMARY KEY,
  tenant_id     VARCHAR(36)   NOT NULL,
  file_name     VARCHAR(255)  NOT NULL,
  file_path     TEXT          NOT NULL,
  agent_id      VARCHAR(100)  NULL,
  session_id    VARCHAR(100)  NULL,
  file_type     VARCHAR(20)   NOT NULL COMMENT 'session|memory',
  total_chunks  INT           NOT NULL DEFAULT 0,
  done_chunks   INT           NOT NULL DEFAULT 0,
  status        VARCHAR(20)   NOT NULL DEFAULT 'pending'
                COMMENT 'pending|processing|done|failed',
  error_msg     TEXT          NULL,
  created_at    TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
  updated_at    TIMESTAMP     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX idx_upload_tenant (tenant_id),
  INDEX idx_upload_poll (status, created_at)
);
```

---

## 2. 批量删除（新功能）

### API

`POST /v1alpha2/mem9s/memories/batch-delete`

```json
// 请求
{"ids": ["uuid1", "uuid2", ...]}
// 响应
{"deleted": 5}
```

### 实现

```go
const maxBulkDeleteSize = 1000

func (s *MemoryService) BulkDelete(ctx, ids []string, agentName string) (int64, error) {
    // 1. 校验：非空、不超过 1000 条
    // 2. 去重：按 ID 去重
    // 3. 调用 BulkSoftDelete（新增的 repo 方法）
}
```

### 新增 Repository 方法

```go
// MemoryRepo 新增
BulkSoftDelete(ctx context.Context, ids []string, agentName string) (int64, error)
```

---

## 3. 内容调和式创建（行为变更）

### 变更说明

当 LLM 可用时，`MemoryService.Create(content)` 不再直接插入数据库，而是走 `ReconcileContent()` 调和流程。

### 新方法：ReconcileContent

`server/internal/service/ingest.go`

```go
func (s *IngestService) ReconcileContent(ctx, agentName, agentID, sessionID string, contents []string) (*IngestResult, error) {
    // 1. 要求 LLM 必须存在（否则返回 ValidationError）
    // 2. 对每个 content：
    //    a. 截断到 32000 rune
    //    b. 包装为 "User: {content}" 格式
    //    c. 调用 extractFacts() 提取事实
    // 3. 将所有事实批量调和（reconcile）
    // 4. 返回 IngestResult
}
```

### Create 方法新逻辑

```go
func (s *MemoryService) Create(ctx, agentID, content string, tags, metadata) (*Memory, int, error) {
    if s.ingest.HasLLM() {
        // 新路径：走调和
        result := s.ingest.ReconcileContent(ctx, agentID, agentID, "", []string{content})
        // 调和后：将用户提供的 tags/metadata 补丁到新创建的 insight 上
        for _, id := range result.InsightIDs {
            // PatchTags + PatchMetadata
        }
        return latestInsight, result.MemoriesChanged + patchWrites, nil
    }
    // 原路径：无 LLM 时直接写入
    mem := &domain.Memory{...}
    s.memories.Create(ctx, mem)
    return mem, 1, nil
}
```

---

## 4. 插件上下文清洗（新安全机制）

### 功能

在 LLM 和存储路径前，移除插件注入的 `<relevant-memories>...</relevant-memories>` 标签。

### 实现

`server/internal/service/ingest.go`

```go
func StripInjectedContext(messages []IngestMessage) []IngestMessage {
    // 对每条消息：
    // 1. 移除 <relevant-memories>...</relevant-memories> 标签
    // 2. TrimSpace
    // 3. 过滤空消息
}

func stripMemoryTags(s string) string {
    // 循环查找并移除所有 <relevant-memories>...</relevant-memories> 块
    // 处理未闭合标签（恶意输入防护）
}
```

### 调用点

1. `handler.ingestMessages()` — 消息摄取入口
2. `IngestService.Ingest()` — 摄取流水线入口

双重清洗作为纵深防御。

---

## 5. 查询形状分类（召回系统新细节）

### 7 种查询形状

`server/internal/handler/recall.go`

```go
const (
    recallQueryShapeGeneral     // 默认兜底
    recallQueryShapeEntity      // "who"、"which"、"谁"、"哪个"
    recallQueryShapeCount       // "how many"、"有多少"、"几个"
    recallQueryShapeTime        // "when"、"什么时候"、"哪天"
    recallQueryShapeLocation    // "where"、"在哪"、"什么地方"
    recallQueryShapeEnumeration // "哪些"、"都有什么"、"what type of"
    recallQueryShapeExact       // "what"、"什么"
)
```

### 中英文双语模式匹配

```go
func classifyRecallQueryShape(query string) recallQueryShape {
    switch {
    case hasAnyPrefix(trimmed, "什么时候", "何时", "哪天", "哪年", "几月", "几号"):
        return recallQueryShapeTime
    case hasAnyPrefix(trimmed, "哪里", "哪儿", "在哪", "什么地方"):
        return recallQueryShapeLocation
    case strings.HasPrefix(lower, "how many"), strings.HasPrefix(lower, "how much"):
        return recallQueryShapeCount
    case hasAnyPrefix(trimmed, "有多少", "多少个", "几个", "几次"):
        return recallQueryShapeCount
    case isEnumerationRecallQuery(trimmed, lower):
        return recallQueryShapeEnumeration
    // ... 更多规则
    }
}
```

### 各形状的召回参数差异

| 形状 | 抓取倍数 | 二跳种子数 | Pinned 保留上限 | 最小置信度 |
|------|---------|-----------|---------------|-----------|
| 默认 | 3x | 3 | 2 | 65 |
| Enumeration | **4x** | **5** | **1** | **55** |

### 枚举查询特殊常量

```go
const (
    enumerationMinConfidence     = 55
    enumerationMaxBudget         = 20
    enumerationBudgetMultiplier  = 2
    enumerationCandidateLimit    = 24
    enumerationFetchMultiplier   = 4
    enumerationSecondHopTopN     = 5
    enumerationPinnedKeepMax     = 1
)
```

---

## 6. 时间意图检测（召回系统新细节）

`server/internal/handler/recall.go`

```go
type recallTemporalIntent int

const (
    recallTemporalIntentAny    // 模糊
    recallTemporalIntentPast   // 过去
    recallTemporalIntentFuture // 未来
)

func classifyRecallTemporalIntent(lower string) recallTemporalIntent {
    switch {
    // 过去
    case "when did", "happened", "ago", "last":
        return recallTemporalIntentPast
    // 中文默认过去
    case "什么时候", "何时", "几号", "哪天":
        return recallTemporalIntentPast
    // 未来
    case "when will", "planning", "scheduled", "upcoming":
        return recallTemporalIntentFuture
    case "什么时候会", "什么时候准备", "什么时候计划":
        return recallTemporalIntentFuture
    default:
        return recallTemporalIntentAny
    }
}
```

### 查询 Profile 结构

```go
type recallQueryProfile struct {
    shape          recallQueryShape
    lower          string
    temporalIntent recallTemporalIntent
    temporalTokens []string
}
```

---

## 7. v1alpha2 API 路由组（架构变更）

### 两套路由对比

| 特性 | v1alpha1 | v1alpha2 |
|------|----------|----------|
| 路径前缀 | `/v1alpha1/mem9s/{tenantID}` | `/v1alpha2/mem9s` |
| 认证方式 | 从 URL 提取 tenantID + tenant 中间件 | `X-API-Key` + `X-Mnemo-Agent-Id` 头 |
| 批量删除 | 无 | `POST /memories/batch-delete` |
| Provision | `POST /v1alpha1/mem9s`（无认证） | 无 |

### v1alpha2 完整路由

```
POST   /v1alpha2/mem9s/memories              — 创建记忆
GET    /v1alpha2/mem9s/memories              — 搜索/列出记忆
GET    /v1alpha2/mem9s/memories/{id}         — 获取单条记忆
PUT    /v1alpha2/mem9s/memories/{id}         — 更新记忆
DELETE /v1alpha2/mem9s/memories/{id}         — 删除记忆
POST   /v1alpha2/mem9s/memories/batch-delete — 批量删除
POST   /v1alpha2/mem9s/imports              — 文件上传
GET    /v1alpha2/mem9s/imports              — 列出任务
GET    /v1alpha2/mem9s/imports/{id}         — 任务详情
GET    /v1alpha2/mem9s/session-messages     — 会话消息
```

---

## 8. 计量（Metering）基础设施（新增）

### 配置项

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `MNEMO_METERING_ENABLED` | false | 启用计量 |
| `MNEMO_METERING_URL` | - | 目标 URL（支持 s3://、http://、https://） |
| `MNEMO_METERING_FLUSH_INTERVAL` | 10s | 刷新间隔 |

### 架构

- Metering Writer 异步批量写入 Event 记录
- 传输方式：S3 或 Webhook HTTP(S)
- 设计为**丢失容忍**：上传失败仅记录日志并丢弃

### 当前状态

基础设施已就位，但业务代码中**尚未插入** `Record()` 调用。

---

## 9. 集群黑名单（运维功能）

### 配置项

`MNEMO_CLUSTER_BLACKLIST`：逗号分隔的集群 ID

### 实现

`server/internal/middleware/auth.go`

```go
func classifyConnError(blacklist map[string]struct{}, clusterID string, err error) string {
    if _, blocked := blacklist[clusterID]; blocked && isSpendLimitError(err) {
        return "cluster_quota_exhausted"  // → HTTP 429
    }
    return "connection_error"              // → HTTP 503
}
```

---

## 10. UTM 营销归因（新增）

### 配置项

`MNEMO_UTM_ENABLED`：启用 UTM 跟踪

### 数据模型

```go
type TenantUTM struct {
    TenantID  string
    Source    string  // utm_source
    Medium    string  // utm_medium
    Campaign  string  // utm_campaign
    Content   string  // utm_content
    CreatedAt time.Time
}
```

### 数据库表

```sql
CREATE TABLE IF NOT EXISTS tenant_utm (
  tenant_id  VARCHAR(36)   NOT NULL PRIMARY KEY,
  source     VARCHAR(255)  NULL,
  medium     VARCHAR(255)  NULL,
  campaign   VARCHAR(255)  NULL,
  content    VARCHAR(255)  NULL,
  created_at TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_tenant_utm FOREIGN KEY (tenant_id) REFERENCES tenants(id)
);
```

### Repository 接口

```go
type UTMRepo interface {
    Create(ctx context.Context, utm *domain.TenantUTM) error
}
```

---

## 11. Sessions 表自动迁移（新增）

### 实现

`server/internal/service/tenant.go`

```go
func (s *TenantService) EnsureSessionsTable(ctx, db *sql.DB) error {
    // 1. 幂等创建 sessions 表（含 auto-embedding 列配置）
    // 2. 如果 autoModel != ""：创建向量索引 idx_sessions_cosine
    // 3. 如果 ftsEnabled：创建全文索引 idx_sessions_fts（MULTILINGUAL）
    // 所有索引创建都容忍"已存在"错误
}
```

### 触发时机

- 在 `resolveServices()` 首次为某租户创建服务实例时
- 通过后台 goroutine 异步执行
- 迁移失败仅记录 Warn 日志，不阻塞请求

---

## 12. 新增 Prometheus 指标

| 指标名 | 类型 | 标签 | 用途 |
|--------|------|------|------|
| `mnemo_llm_retry_total` | Counter | step, reason | LLM 重试次数（如 json_parse_retry, thinking_param_400_fallback） |
| `mnemo_embedding_requests_total` | Counter | step, model, status | 查询 Embedding 请求数 |
| `mnemo_active_memory_7d_total` | Gauge | cluster_id | 最近 7 天创建的活跃记忆数 |
| `mnemo_memory_changes_total` | Counter | cluster_id | ADD/UPDATE 变更计数 |

---

## 13. LLM CallScope（可观测性增强）

### 结构

```go
type CallScope struct {
    Step string // "extraction" | "extraction_and_classification" | "reconciliation"
}

func (s CallScope) enabled() bool {
    return s.Step != ""
}
```

### 新方法

```go
func (c *Client) CompleteJSONWithScope(ctx, system, user string, scope CallScope) (string, error)
```

### 效果

- 事实提取 LLM 调用标记为 `scope.Step = "extraction"`
- 调和 LLM 调用标记为 `scope.Step = "reconciliation"`
- 所有 LLM 指标（请求数、Token 数、延迟）均按 Step 维度拆分

---

## 14. MiniMax reasoning_split 支持（兼容性增强）

```go
func supportsReasoningSplit(model string) *bool {
    if strings.HasPrefix(strings.ToLower(model), "minimax-m2") {
        reasoningSplit := true
        return &reasoningSplit
    }
    return nil
}
```

- MiniMax M2 模型自动添加 `reasoning_split: true` 参数
- HTTP 400 时优雅回退（与 `enable_thinking` 相同的重试模式）

---

## 15. Embedder.Model() 方法（API 表面扩展）

```go
func (e *Embedder) Model() string {
    return e.model
}
```

参考文档仅记录了 `Embed()` 和 `Dims()`。新增 `Model()` 用于按模型维度统计 Embedding 请求指标。

---

## 16. 召回结果内容去重（搜索质量提升）

```go
func dedupRecallCandidatesByContent(candidates []RecallCandidate) []RecallCandidate {
    seen := make(map[string]struct{}, len(candidates))
    out := make([]RecallCandidate, 0, len(candidates))
    for _, candidate := range candidates {
        key := candidate.Memory.Content
        if key == "" {
            key = candidate.Memory.ID
        }
        if _, ok := seen[key]; ok {
            continue
        }
        seen[key] = struct{}{}
        out = append(out, candidate)
    }
    return out
}
```

- 按内容文本去重（不仅按 ID）
- 在 memory 和 session 候选搜索末端均有应用

---

## 17. TiDB Cloud Pool 配置（新增）

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `MNEMO_TIDBCLOUD_API_URL` | `https://serverless.tidbapi.com` | TiDB Cloud API 地址 |
| `MNEMO_TIDBCLOUD_POOL_ID` | `2` | Pool ID |

与 TiDB Zero 自动配置（`MNEMO_TIDB_ZERO_*`）独立。

---

## 18. IngestMessage.Seq 字段（API 表面扩展）

```go
type IngestMessage struct {
    Role    string `json:"role"`
    Content string `json:"content"`
    Seq     *int   `json:"seq,omitempty"` // 新增
}
```

- 可选的消息序号字段
- 参与 content hash 计算：`SessionContentHash(sessionID, role, content, seq)`
- 为 session 去重提供轮次级别的溯源能力

---

## 汇总

| # | 功能 | 重要程度 | 分类 |
|---|------|---------|------|
| 1 | 文件上传/导入系统 | **关键** | 新能力 |
| 2 | 批量删除 | **高** | 新 API |
| 3 | 内容调和式创建 | **高** | 行为变更 |
| 4 | 插件上下文清洗 | **高** | 新安全机制 |
| 5 | 查询形状分类（7 种） | **中** | 召回细节 |
| 6 | 时间意图检测 | **中** | 召回细节 |
| 7 | v1alpha2 API 路由组 | **中** | 架构 |
| 8 | 计量基础设施 | **中** | 基础设施 |
| 9 | 集群黑名单 | **低** | 运维 |
| 10 | UTM 营销归因 | **低** | 营销 |
| 11 | Sessions 表自动迁移 | **低** | 基础设施 |
| 12 | 新增 Prometheus 指标（4 个） | **低** | 可观测性 |
| 13 | LLM CallScope | **低** | 可观测性 |
| 14 | MiniMax reasoning_split | **低** | 兼容性 |
| 15 | Embedder.Model() | **低** | API 表面 |
| 16 | 召回内容去重 | **低** | 搜索质量 |
| 17 | TiDB Cloud Pool 配置 | **低** | 基础设施 |
| 18 | IngestMessage.Seq | **低** | API 表面 |
