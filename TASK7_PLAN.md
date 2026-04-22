# Task 7: 两阶段智能摄取 — 三阶段工作计划

> 原文档：`Vego_Agent_Memory_Service.md` §Task 7
> 目标：移植 mem9 `ingest.go`（1642 行）核心逻辑，简化为嵌入式场景。

---

## 总体架构

```
原始输入 (Messages / Documents)
    │
    ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Phase 1        │     │  Phase 2        │     │  Phase 3        │
│  事实提取       │ ──► │  调和           │ ──► │  Prompt 精调    │
│  ingest.go      │     │  reconcile.go   │     │  + 集成测试     │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

**核心接口：**

```go
// 入口：将原始输入转为结构化记忆
facts, err := s.ExtractFacts(ctx, messages)   // Phase 1
result, err := s.Reconcile(ctx, agentID, facts) // Phase 2
```

---

## Phase 1: `ingest.go` 骨架 + ModeRaw

### 目标
搭起事实提取的骨架，完成 **ModeRaw**（原始消息透传）闭环。ModeRaw 不依赖 LLM，纯本地逻辑，可独立测试。

### 交付物
- `memory/ingest.go`（含骨架代码）
- `memory/ingest_test.go`（覆盖率 ≥ 85%）
- `Message` / `ExtractedFact` / `IngestResult` / `IngestMode` 领域类型

### 工作内容

#### 1. 领域类型定义

```go
// IngestMode 控制摄取行为
type IngestMode int

const (
    ModeNormal IngestMode = iota // 默认：LLM 提取事实
    ModeRaw                      // 原始消息透传，跳过 LLM
)

// Message 是原始输入单元
type Message struct {
    Role      string   // "user" / "assistant" / "system"
    Content   string
    Tags      []string
    AgentID   string
    SessionID string
    Timestamp time.Time
}

// ExtractedFact 是 LLM 提取后的事实
type ExtractedFact struct {
    Content   string
    Tags      []string
    QueryIntent bool // true 表示搜索意图，应丢弃
    SourceMsg int    // 来源消息索引
}

// IngestResult 统计调和结果
type IngestResult struct {
    Added   int
    Updated int
    Deleted int
    Skipped int
}
```

#### 2. `ExtractFacts` 框架

```go
func (s *MemoryStore) ExtractFacts(ctx context.Context, messages []Message, mode IngestMode) ([]ExtractedFact, error)
```

- **ModeRaw**：每条消息转为一条 `ExtractedFact`（Content=msg.Content），**不调用 LLM**
- **ModeNormal**：框架搭好，先返回空实现或简化版（等 Phase 3 填 Prompt）

#### 3. `storeRawMessages` — ModeRaw 核心

```go
func (s *MemoryStore) storeRawMessages(ctx context.Context, sessionID string, messages []Message) (int, error)
```

- 计算 `SHA256(content)` 作为 `ContentHash`
- 查重：`s.contentHashIndex.Has(sessionID, hash)` → O(1) 跳过重复
- 去重通过的消息：
  - `MemoryType = TypeSession`
  - `Seq` 会话内全局递增（`MaxSeq(sessionID) + 1`）
  - `State = StateActive`
  - 调用 `s.embed` → `memoryToDoc` → `InsertContext`
  - 更新 `inverted` + `contentHashIndex`
- 返回实际存储条数

#### 4. `computeContentHash` 工具

```go
func computeContentHash(content string) string {
    h := sha256.Sum256([]byte(content))
    return hex.EncodeToString(h[:])
}
```

#### 5. `ContentHashIndex` 激活

- `Open()` 时 `rebuildIndexes` 扫描 `TypeSession` 文档重建索引（已有逻辑，需补全测试）
- `storeRawMessages` 中实际使用 `Has` / `Add` / `MaxSeq`
- 清理 `MaxSeq` 注释（"or 0 if none"，已对齐实现）

### 验收标准

- [ ] `ExtractFacts` ModeRaw 路径完整实现并测试通过
- [ ] `storeRawMessages` 去重逻辑正确：同 session 重复内容只存一次
- [ ] `ContentHashIndex` 不再是死代码，覆盖率 ≥ 85%
- [ ] Race test 通过
- [ ] 累积式对话场景测试：第 3 轮消息包含第 1、2 轮历史，去重后只新增 1 条

### 工作量估算

| 项 | 耗时 |
|---|---|
| 领域类型 + ingest.go 骨架 | 0.5 天 |
| ModeRaw 实现（storeRawMessages + ContentHash） | 0.5 天 |
| 测试（含 race + 去重场景） | 0.5 天 |
| **总计** | **~1.5 天** |

### 风险

| 风险 | 应对 |
|---|---|
| ContentHash 冲突（不同内容相同 hash） | SHA256 概率极低，文档标注；如需更强保证可加盐 |
| SessionID 为空导致去重失效 | `validate` 检查或默认 fallback |

---

## Phase 2: `reconcile.go` 骨架 + 整数 ID 映射

### 目标
完成调和框架：事实与已有记忆比对 → LLM 判定操作 → 执行 ADD/UPDATE/DELETE/NOOP。

### 交付物
- `memory/reconcile.go`（含骨架代码）
- `memory/reconcile_test.go`（覆盖率 ≥ 80%）
- `golang.org/x/sync/semaphore` 并发控制

### 工作内容

#### 1. `Reconcile` 框架

```go
func (s *MemoryStore) Reconcile(ctx context.Context, agentID string, facts []ExtractedFact) (*IngestResult, error)
```

#### 2. 候选记忆搜索

对每条 fact，并发搜索已有记忆：

```go
// 1. 向量搜索：用 fact.Content 的 embedding 搜相似记忆
vecResults, _ := s.coll.SearchWithFilterContext(ctx, vec, limit, activeFilter)

// 2. 关键词搜索：用 inverted index 搜含相同关键词的记忆
keywordResults := s.inverted.Search(fact.Content, limit)

// 3. 去重合并候选集
```

- 默认并发上限：**4 worker**（`semaphore.NewWeighted(4)`）
- 搜索用当前 `Search` + `inverted.Search`，不阻塞 Phase 3 的混合搜索改造

#### 3. 整数 ID 映射

给候选记忆分配临时整数 ID（1, 2, 3...），避免 LLM 看到 UUID 后幻觉编造：

```go
type candidateMapping struct {
    intID    int
    memoryID string
    memory   Memory
}

// Prompt 中只暴露整数 ID
// "候选记忆 1: {content}"
// "候选记忆 2: {content}"
```

#### 4. LLM 判定操作（简化版 Prompt）

Phase 2 先用简化 Prompt，Phase 3 再移植 mem9 完整版：

```
System: 你是一个记忆管理助手。给定一条新事实和若干候选记忆，
决定操作类型：ADD（新增）、UPDATE（替换旧记忆）、DELETE（删除旧记忆）、NOOP（无操作）。

规则：
- 如果候选记忆内容与事实几乎相同 → UPDATE
- 如果事实与候选记忆矛盾 → DELETE 旧 + ADD 新（即 UPDATE）
- 如果完全无关 → ADD
- 如果候选记忆是 Pinned 类型 → 不可 DELETE，UPDATE 降级为 ADD

请返回 JSON: {"action": "ADD|UPDATE|DELETE|NOOP", "target_id": 整数ID, "reason": "..."}
```

#### 5. 操作执行

| LLM 返回 | 执行 |
|---|---|
| ADD | `s.Store(ctx, fact.Content, fact.Tags)` |
| UPDATE | `s.Update(ctx, targetID, fact.Content, fact.Tags)` |
| DELETE | `s.Delete(ctx, targetID)` |
| NOOP | 跳过 |

#### 6. Pinned 记忆保护

```go
if candidate.MemoryType == TypePinned && action == DELETE {
    action = NOOP // 或降级为 ADD
}
```

#### 7. 并发控制

```go
import "golang.org/x/sync/semaphore"

var (
    searchSem = semaphore.NewWeighted(4)  // 搜索并发
    llmSem    = semaphore.NewWeighted(1)  // LLM 串行
)
```

### 验收标准

- [ ] `Reconcile` 框架跑通：单条 fact → 搜索候选 → LLM 判定 → 执行操作
- [ ] 整数 ID 映射正确：LLM 返回的 target_id 能准确映射回 memory UUID
- [ ] Pinned 保护生效：Pinned 记忆不会被 DELETE
- [ ] 并发控制生效：LLM 调用串行，搜索并发 ≤ 4
- [ ] Race test 通过

### 工作量估算

| 项 | 耗时 |
|---|---|
| reconcile.go 骨架 + 候选搜索 | 0.5 天 |
| 整数 ID 映射 + LLM 调用框架 | 0.5 天 |
| ADD/UPDATE/DELETE/NOOP 执行 + Pinned 保护 | 0.5 天 |
| 测试（含并发、错误路径） | 0.5 天 |
| **总计** | **~2 天** |

### 依赖

- **Phase 1 完成**（需要 `ExtractedFact` 类型）
- `golang.org/x/sync/semaphore` 依赖已存在于 Go 标准库生态，需确认是否已引入

---

## Phase 3: LLM Prompt 精细化 + 端到端集成

### 目标
移植 mem9 的完整 Prompt 和调优逻辑，完成端到端集成测试。

### 交付物
- 完整 System Prompt（14 条提取规则 + 调和规则）
- JSON 解析失败自动重试 + raw fallback
- `query_intent` 过滤
- 事实上限截断（默认 50 条）
- 端到端集成测试 + Benchmark baseline

### 工作内容

#### 1. 事实提取 Prompt（14 条规则）

移植 mem9 的 System Prompt，包含：
- 事实定义（What makes a good fact）
- 提取规则（14 条，如"合并同主题事实"、"丢弃寒暄"、"保留时间锚点"等）
- 中英文示例
- 输出格式：`[]ExtractedFact` JSON

```go
const factExtractionPrompt = `...` // 完整 Prompt
```

#### 2. 调和 Prompt（ADD/UPDATE/DELETE/NOOP）

移植 mem9 的调和 System Prompt：
- 操作定义和边界
- 相似度判断标准
- Pinned 记忆特殊处理
- 输出格式 JSON

#### 3. 错误恢复

```go
func (s *MemoryStore) extractFactsWithRetry(ctx context.Context, messages []Message) ([]ExtractedFact, error) {
    // 1. 带 response_format: json_object 请求
    // 2. JSON 解析失败 → 重试 1 次
    // 3. 仍失败 → raw fallback：每条消息作为独立 fact
}
```

#### 4. `query_intent` 过滤

```go
for _, f := range facts {
    if f.QueryIntent {
        continue // 丢弃搜索意图事实
    }
}
```

#### 5. 事实上限截断

```go
if len(facts) > s.config.MaxFacts {
    facts = facts[:s.config.MaxFacts]
}
```

#### 6. 端到端集成测试

```go
func TestIngestEndToEnd(t *testing.T) {
    // 1. Bootstrap 初始记忆
    // 2. ExtractFacts（ModeNormal）从消息中提取
    // 3. Reconcile 调和
    // 4. Search 验证结果正确
}
```

#### 7. Benchmark baseline

```go
func BenchmarkExtractFacts(b *testing.B)
func BenchmarkReconcile(b *testing.B)
```

### 验收标准

- [ ] 事实提取 Prompt 能稳定输出 JSON（≥ 95% 成功率）
- [ ] JSON 解析失败时重试 + fallback 生效
- [ ] `query_intent` 过滤正确丢弃搜索意图
- [ ] 超过 50 条事实自动截断
- [ ] 端到端测试覆盖：Extract → Reconcile → Search 完整链路
- [ ] Benchmark baseline 建立（记录耗时、LLM token 消耗）

### 工作量估算

| 项 | 耗时 |
|---|---|
| Prompt 移植 + 调优 | 1 天 |
| 错误恢复（重试 + fallback） | 0.5 天 |
| 端到端测试 + Benchmark | 0.5 天 |
| **总计** | **~2 天** |

### 依赖

- **Phase 1 + Phase 2 完成**
- 需要 LLM 服务可用（OpenAI 或本地 Ollama）用于 Prompt 调优

---

## 三阶段总览

| 阶段 | 核心文件 | 关键交付 | 工作量 | 前置依赖 |
|------|---------|---------|--------|---------|
| Phase 1 | `ingest.go` | ModeRaw 闭环 + ContentHash 去重 | ~1.5 天 | Task 6 完成 |
| Phase 2 | `reconcile.go` | 调和框架 + 整数 ID + 并发控制 | ~2 天 | Phase 1 |
| Phase 3 | Prompt 文本 | 完整 Prompt + 端到端集成 | ~2 天 | Phase 2 + LLM 服务 |
| **总计** | | | **~5.5 天** | |

---

## 附录：已有基础（Task 6 完成）

以下组件 Task 6 已就绪，Task 7 可直接复用：

| 组件 | 状态 | Task 7 使用方式 |
|------|------|----------------|
| `MemoryStore` CRUD | ✅ | Reconcile 中 ADD/UPDATE/DELETE |
| `archiveAndCreate` | ✅ | Reconcile UPDATE 直接调用 |
| `ContentHashIndex` | ✅（死代码） | Phase 1 ModeRaw 去重激活 |
| `InvertedIndex` | ✅ | Phase 2 候选关键词搜索 |
| `LLMClient` | ✅ | Phase 3 完整 Prompt 调用 |
| `Embedder` | ✅ | 全阶段 embedding |
| `SearchWithFilterContext` | ✅ | Phase 2 候选向量搜索 |
| `Config` / Options | ✅ | `MaxFacts`、`SearchLimit` 等 |
