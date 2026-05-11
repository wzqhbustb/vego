# Vego 架构设计

本文档描述 Vego 的分层架构，包括设计决策、包边界、依赖规则和演进策略。

> **说明：** 本文档描述的是 Vego 的**目标**分层架构，当前代码库正在向此架构演进。

---

## 1. 分层总览

```
┌─────────────────────────────────────────────────────────────────┐
│  第 5 层：应用服务层                   memory/                   │
│  Agent Memory、混合搜索、LLM 事实提取、状态机                    │
└─────────────────────────────────────────────────────────────────┘
                              │ 依赖
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  第 4 层：API / 编排层                 vego/                     │
│  DB, Collection, Document, Query, Config                        │
│  职责：协调索引引擎和存储引擎的调用顺序                          │
└─────────────────────────────────────────────────────────────────┘
                              │
               ┌──────────────┴──────────────┐
               ▼                             ▼
┌──────────────────────────┐  ┌──────────────────────────────────┐
│  第 3-A 层：索引引擎     │  │  第 3-B 层：存储引擎             │
│  index/                  │  │  storage/                        │
│                          │  │                                  │
│  HNSW 图构建与搜索       │  │  storage/catalog/  元数据管理    │
│  距离函数                │  │  storage/column/   列式读写      │
│  DeletionVector 过滤     │  │  storage/encoding/ 编码和压缩    │
│  自适应参数              │  │  storage/format/   文件结构定义  │
│                          │  │                                  │
│  不关心数据如何存储      │  │                                  │
│  只关心向量和图结构      │  │                                  │
└──────────────────────────┘  └──────────────────────────────────┘
               │                             │
               └──────────────┬──────────────┘
                              │ 共同依赖
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  第 2 层：IO 层                        vfs/                     │
│  文件读写，同步/异步 I/O，文件句柄管理                           │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  第 1 层：基础层                       core/                     │
│  Schema, Array, Buffer, RecordBatch, Bitmap, Builder            │
└─────────────────────────────────────────────────────────────────┘
```

---

## 2. 包结构

```
vego/
├── memory/              # 第 5 层：Agent Memory 服务
├── vego/                # 第 4 层：API / 编排层
├── index/               # 第 3-A 层：索引引擎（HNSW）
├── storage/             # 第 3-B 层：存储引擎
│   ├── catalog/         #   元数据管理（Snapshot、IDMapping、DeletionStore）
│   ├── column/          #   列式读写（ColumnWriter、ColumnReader）
│   ├── encoding/        #   自适应编码（ZSTD、RLE、BitPacking、BSS、Dictionary）
│   └── format/          #   文件结构（Header、Footer、PageIndex、BlockCache）
├── vfs/                 # 第 2 层：I/O 操作（同步/异步文件访问）
└── core/                # 第 1 层：内存格式（Schema、Array、RecordBatch）
```

---

## 3. Import 依赖规则

```
memory/  ──→  vego/  ──→  index/            ──→  core/
    └────────→  index/   (现状；将在 Step 2 消除)
                     ──→  storage/catalog/   ──→  core/, vfs/
                     ──→  storage/column/    ──→  storage/encoding/, storage/format/,
                                                  core/, vfs/
                                                  storage/encoding/ ──→ core/
                                                  storage/format/   ──→ core/
                                                  vfs/              ──→ (标准库)
                                                  core/             ──→ (标准库)
```

**关键约束：**

- `index/` 不 import `storage/` — 索引引擎不知道如何持久化。
- `storage/catalog/` 不 import `index/` — 元数据层不知道索引算法。
- `vego/` 是唯一同时 import `index/` 和 `storage/` 的包 — 它是编排层。
- `storage/column/` → `storage/encoding/` → `storage/format/` → `core/` 是单向依赖链。
- `core/` 和 `vfs/` 是独立的顶层包 — 索引引擎和存储引擎的共享基础设施。

**现状偏离 — `memory/` → `index/`：**

`memory/` 当前直接 import 了 `index/`。该依赖本质上是间接的 — `memory/` 使用的 `index/` 类型应通过 `vego/` 重新导出。消除路径：确保 `memory/` 只引用 `vego/` 暴露的公开类型，不直接引用 `index/` 的内部类型。将在 Step 2 中一并解决。

---

## 4. 各层详细设计

### 4.1 第 1 层：基础层（core/）

> **当前位置：** `storage/arrow/`。将在迁移 Step 0 中提升为顶层 `core/`。

纯内存数据表示。零外部依赖。

提供：Schema、Field、Array（Int32Array、Float32Array、FixedSizeListArray 等）、Buffer、RecordBatch、Bitmap、Builder。

设计：自研 Arrow 实现，无 CGO。零拷贝语义自底向上传递至整个技术栈。

### 4.2 第 2 层：IO 层（vfs/）

> **当前位置：** `storage/io/`。将在迁移 Step 0 中提升为顶层 `vfs/`。

磁盘访问的共享基础设施。提供同步和异步文件操作。

> **当前实现：** `storage/io/file_pool.go`。Step 0 原样提升至 `vfs/`，Step 3 修复。

```go
package vfs

type FileHandle interface {
    ReadAt(p []byte, off int64) (n int, err error)
    WriteAt(p []byte, off int64) (n int, err error)
    Sync() error
    Close() error
}

type FilePool struct {
    // 当前实现在 storage/io/file_pool.go。
    // 计划修复（Step 3）：
    //   - 将 sync.Mutex 替换为 sync.RWMutex
    //   - 删除重复的 Get/GetFile 方法
    //   - 修复 partial read 处理
}
```

### 4.3 第 3-A 层：索引引擎（index/）

纯内存图结构和搜索算法。索引引擎不知道如何持久化自身 — 它只暴露产出 RecordBatch 的序列化方法。

```go
package index

// HNSWIndex 是具体类型。此处不定义 interface。
// "Accept interfaces, return structs" — interface 由消费者（vego/）
// 在需要时定义，而非由提供者（index/）定义。
type HNSWIndex struct { ... }

func NewHNSW(config Config) *HNSWIndex

// 核心操作 — 纯内存
func (h *HNSWIndex) Add(id int, vector []float32) error
func (h *HNSWIndex) Search(query []float32, k int, filter func(int) bool) []SearchResult
func (h *HNSWIndex) Delete(id int)

// 序列化 — 产出 RecordBatch，不知道它们将被写到哪里。
// 当前实现返回单 batch；当数据集超过百万级时将引入回调式流式接口（见下方说明）。
func (h *HNSWIndex) MarshalNodes() (*core.RecordBatch, error)
func (h *HNSWIndex) MarshalConnections() (*core.RecordBatch, error)
func (h *HNSWIndex) MarshalMetadata() (*core.RecordBatch, error)
func (h *HNSWIndex) UnmarshalNodes(batch *core.RecordBatch) error
func (h *HNSWIndex) UnmarshalConnections(batch *core.RecordBatch) error
func UnmarshalMetadata(batch *core.RecordBatch) (*MetadataResult, error)
```

**为什么使用回调式流出：**

- 内存可控：100 万向量 × 768 维 × 4 字节 = ~3GB，不流式化会成为单个 RecordBatch。
- 与列存分页天然对齐：每个 batch 对应一个或几个 Page。
- 写入方可以流式处理，不需要缓存所有 batch。
- 无 Go 版本依赖问题（回调在任何 Go 版本可用；iter.Seq2 需要 1.23+）。

**为什么此层不定义 interface：**

- 目前只有一个实现（HNSW）。
- 将来加入 IVF-PQ 时，消费层（vego/）定义所需 interface：

```go
// vego/searcher.go — 由消费者定义
type Searcher interface {
    Search(query []float32, k int, filter func(int) bool) []SearchResult
}
```

这遵循 Go 的 "interface 属于消费者" 原则。

### 4.4 第 3-B 层：存储引擎（storage/）

重组为 4 个子包，职责清晰。

#### storage/catalog/ — 元数据管理（新增）

从当前 `vego/storage.go` 中剥离。管理 snapshot、ID 映射和删除向量。

```go
package catalog

// Snapshot 是 collection 状态的唯一真相来源。
// 它持有当前 collection 的所有元数据组件和文件路径。
type Snapshot struct {
    Path          string              // collection 目录
    DataFile      string              // vectors.lance 路径
    MetaFile      string              // metadata.json 路径
    Version       format.VersionPolicy // 格式版本
    MetaStore     *MetadataStore      // doc 元数据（ID、RowIndex、Metadata）
    DeletionStore *DeletionStore      // 软删除状态
}

// 事务协议（预留接口，当前未实现）。
// Phase 6 WAL + MVCC 时接入。
func (s *Snapshot) BeginTransaction() string
func (s *Snapshot) CommitTransaction(txnDir string) error
func (s *Snapshot) AbortTransaction(txnDir string)
func (s *Snapshot) RecoverFromCrash() error

// MetadataStore 管理 doc 元数据（ID、RowIndex、Metadata map）。
// 从 vego/storage.go 的 metadataStore 提取。
type MetadataStore struct {
    entries  map[int64]DocMeta
    idToHash map[string]int64
}
func (s *MetadataStore) GetByID(id string) (DocMeta, bool)
func (s *MetadataStore) Put(id string, hash int64, meta DocMeta)
func (s *MetadataStore) Delete(id string, hash int64)
func (s *MetadataStore) Save() error
func (s *MetadataStore) LoadWithRepair(...) error

// IDMapping 管理 docID <-> nodeID 双向映射。
// 延迟抽象：当前为具体 struct；消费者（vego/）在需要时可定义 interface。
type IDMapping struct {
    docToNode map[string]int
    nodeToDoc map[int]string
}
func (m *IDMapping) Map(docID string) (nodeID int, ok bool)
func (m *IDMapping) Reverse(nodeID int) (docID string, ok bool)
func (m *IDMapping) Put(docID string, nodeID int)
func (m *IDMapping) Delete(docID string)
func (m *IDMapping) All() map[string]int
func (m *IDMapping) Replace(docToNode map[string]int, nodeToDoc map[int]string)

// DeletionStore 管理内存中的软删除状态（roaring bitmap）。
// 独立于 index/ 包，使用相同的 .del 文件格式保证兼容。
// 延迟抽象：当前为具体 struct；消费者可后续定义 interface。
type DeletionStore struct { /* roaring bitmap */ }
func (ds *DeletionStore) MarkDeleted(rowID uint32)
func (ds *DeletionStore) IsDeleted(rowID uint32) bool
func (ds *DeletionStore) Count() int
func (ds *DeletionStore) Save(path string) error
func (ds *DeletionStore) Load(path string) error
```

**为什么需要这个子包：**

1. 当前元数据管理散落在 `vego/storage.go` 中，与列存读写混在一起。
2. 为 Phase 6 WAL + MVCC 提供自然扩展点。
3. 底层格式（当前为 JSON）可以替换为 SQLite/BoltDB，只需改这一个包。

#### storage/column/ — 列式读写（已有，接口精炼）

```go
package column

type ColumnWriter interface {
    WriteRecordBatch(batch *core.RecordBatch) error
    Close() error
}

type ColumnReader interface {
    ReadRecordBatch() (*core.RecordBatch, error)
    ReadRow(rowIndex int) (*core.RecordBatch, error)  // 通过 RowIndex 实现 O(1) 读取
    Close() error
}
```

#### storage/encoding/ — 自适应编码（已有，不变）

自适应编码选择：ZSTD、RLE、BitPacking、BSS、Dictionary。

#### storage/format/ — 文件结构（已有，不变）

Header、Footer、PageIndex、Version、BlockCache 定义。

### 4.5 第 4 层：API / 编排层（vego/）

从 "既编排又实现" 变为**纯编排**。这是唯一同时 import `index/` 和 `storage/` 的层。

```go
package vego

type Collection struct {
    index    *index.HNSWIndex           // 索引引擎（具体类型）
    snapshot *catalog.Snapshot          // collection 状态元数据
    idMapping *catalog.IDMapping        // ID 映射（具体类型）
    writer   column.ColumnWriter        // 数据写入
    reader   column.ColumnReader        // 数据读取
    buffer   *WriteBuffer              // 写缓冲（编排策略）
    config   *Config
    dirty   bool                     // 跟踪未提交的变更
}
```

#### 编排：Insert

```go
func (c *Collection) Insert(doc *Document) error {
    // 1. 验证
    if err := doc.Validate(c.config.Dimension); err != nil {
        return err
    }
    // 2. 索引引擎：添加向量
    nodeID := c.index.Add(doc.Vector)
    // 3. 元数据：记录映射
    c.ids.Put(doc.ID, nodeID)
    // 4. 写缓冲：累积数据
    c.buffer.Append(doc)
    c.dirty = true
    // 5. 自动 flush
    if c.buffer.ShouldFlush() {
        return c.flush()
    }
    return nil
}
```

#### 编排：Search

```go
func (c *Collection) Search(query []float32, k int) []SearchResult {
    // 1. 索引引擎：ANN 搜索（注入 DV 过滤）
    candidates := c.index.Search(query, k*2, c.dv.IsDeleted)
    // 2. 映射：nodeID -> docID
    // 3. 存储引擎：取文档数据
    // 4. 返回结果
}
```

#### 编排：Delete

```go
func (c *Collection) Delete(docID string) error {
    nodeID, ok := c.ids.DocToNode(docID)
    if !ok {
        return ErrDocumentNotFound
    }
    rowID, _ := c.ids.NodeToRow(nodeID)
    c.dv.MarkDeleted(rowID)     // 内存操作
    c.ids.Delete(docID)          // 内存操作
    c.dirty = true               // 标记脏

    // 不立即持久化。由以下时机触发：
    //   - 显式 collection.Save()
    //   - 自动 compact 触发时
    //   - DB.Close() 时
    return nil
}
```

#### 编排：Flush（事务性）

```go
func (c *Collection) flush() error {
    txnDir := c.snapshot.BeginTransaction()

    // 1. 数据写入 txnDir
    dataPath := txnDir + "/vectors.lance"
    if err := c.writeData(dataPath); err != nil {
        c.snapshot.AbortTransaction(txnDir)
        return err
    }

    // 2. 索引写入 txnDir（流式）
    indexPath := txnDir + "/index/"
    if err := c.writeIndex(indexPath); err != nil {
        c.snapshot.AbortTransaction(txnDir)
        return err
    }

    // 3. 元数据写入 txnDir
    if err := c.writeMetadata(txnDir); err != nil {
        c.snapshot.AbortTransaction(txnDir)
        return err
    }

    // 4. 原子提交：更新 snapshot（唯一的 commit point）
    if err := c.snapshot.CommitTransaction(txnDir); err != nil {
        return err
    }

    c.dirty = false
    return nil
}
```

**Crash safety 保证：**

- Crash 在步骤 1/2/3：txnDir 残留。`RecoverFromCrash()` 在下次启动时清理。
- Crash 在步骤 4（snapshot rename 之前）：同上。
- Crash 在步骤 4（snapshot rename 之后）：新数据已就位，一致。
- **snapshot.json 是最后一个被 rename 的文件** — 它是唯一的 commit point。

#### 职责迁移表

| 原职责 | 新归属 |
|---|---|
| writeBuffer + flush 策略 | 留在 API 层（`vego/buffer.go`） |
| metaStore（doc 元数据：ID/RowIndex/Metadata） | `storage/catalog/metadata.go` |
| docToNode/nodeToDoc 映射 | `storage/catalog/id_mapping.go` |
| deletionVector 管理 | `storage/catalog/deletion_store.go` |
| cachedRowIndex | `storage/column/reader.go` 内部 |
| version（格式版本） | `storage/format/version.go` |
| blockCache | `storage/format/blockcache.go`（不变） |
| 列存读写调用 | `storage/column/` 直接暴露接口 |

### 4.6 第 5 层：应用服务层（memory/）

不变。依赖第 4 层 API。提供 Agent Memory 功能。

关键子模块：

| 子模块 | 职责 |
|---|---|
| Ingest pipeline | Message → LLM 事实提取 → 向量化 → 存储 |
| Reconcile | 去重与冲突解决（ADD/UPDATE/DELETE/NOOP） |
| 混合搜索 | HNSW 向量搜索 + BM25 文本搜索 + RRF 分数融合 |
| 时间归一化 | 相对时间表达式 → 绝对时间戳 |
| Embedding | 可配置并发：串行 / 并行 worker 池 / 批量 |
| LLM client | JSON mode 三态控制、多格式响应解析 |

---

## 5. 关键设计决策

### 决策 1：保持 `storage/` 命名，内部重组

**选择：** 保留 `storage/` 包路径；内部新增 `catalog/` 子包。将共享基础设施（`storage/arrow/` → `core/`、`storage/io/` → `vfs/`）提升为顶层包 — 它们不属于存储引擎。

**理由：**
- Go module path 一旦被 go.sum 记录就有惯性。
- `storage/` 语义比 `store/` 更准确（store 容易与 "商店" 混淆）。
- 重命名的收益太小，不值得 breaking change。
- `core/` 和 `vfs/` 是 `index/` 和 `storage/` 共用的通用基础设施。它们之前被错误地嵌套在 `storage/` 下 — 提升它们是纠正归属，不是存储引擎改名。

### 决策 2：索引引擎不过早定义 interface

**选择：** API 层直接持有 `*index.HNSWIndex`。`index/` 包不定义 interface。

**理由：**
- Go 惯例："Accept interfaces, return structs"。
- 目前只有一个实现。现在定义 interface 是过度设计。
- 将来加入 IVF-PQ 时，消费者（`vego/`）定义它所需的 interface。

### 决策 3：回调式流出序列化

**选择：** `MarshalNodes(batchSize int, emit func(*RecordBatch) error) error`

**理由：**
- 避免大数据集产生多 GB 的单 RecordBatch。
- 与列存分页天然对齐。
- 写入方流式处理，不需要缓存。
- 无 Go 版本依赖（回调在任何版本可用；`iter.Seq2` 需要 1.23+）。
- `batchSize` 应由 API 层根据 Dimension 自动计算（目标：单 batch < 64MB），极端场景可覆盖。

### 决策 4：Catalog 管理事务性 flush

**选择：** `catalog.Snapshot` 拥有事务协议（Begin → Write All → Commit）。

**理由：**
- 所有文件先写到临时目录。
- snapshot.json 原子 rename 是唯一的 commit point。
- Crash 时，孤儿 txnDir 由 `RecoverFromCrash()` 清理。
- 这与 Lance 的 `_transactions/*.txn` + 原子 manifest 写入模式一致，简化为单文件场景。

### 决策 5：DeletionStore — API 层控制持久化

**选择：** 方案 A。DeletionStore 只管内存状态 + 序列化。API 层决定何时和写到哪里。

**理由：**
- 符合 "API 层纯编排" 原则。
- DeletionStore 不知道其他组件的 dirty 状态。
- Save 时机与数据 flush、索引 flush 联动 — 必须由编排层统一决策。
- `io.WriterTo` / `io.ReaderFrom` 接口与 Go 标准库（bufio、compress 等）组合良好。

---

## 6. 并发模型

```
锁层次结构（必须按此顺序获取）：

  DB 层（最外层）
  ├── db.mu (RWMutex)
  │   ├── RLock: Collections()
  │   └── Lock:  Collection(), DropCollection(), Close()
  │
  Collection 层（中间层）
  ├── c.mu (RWMutex)
  │   ├── RLock: Get(), Search(), Count(), Stats()
  │   └── Lock:  Insert(), Delete(), Update(), Save(), flush()
  │
  Index 层（HNSW 内部）
  ├── h.globalLock (RWMutex)
  │   ├── RLock: Search, SearchWithDV, Len
  │   └── Lock:  Add（阻塞所有并发 Search/Len）
  ├── h.mu (Mutex) — 仅保护 RNG（在 Add 内部获取）
  │
  Storage 层（最内层）
  └── Catalog、ColumnWriter/Reader 有各自的内部锁
```

规则：始终按 DB → Collection → Index → Storage 顺序获取锁。永远不要在持有内层锁时请求外层锁。

---

## 7. Roadmap 对齐

此分层架构直接支撑规划中的演进：

| Roadmap 阶段 | 影响的层 | 改动范围 |
|---|---|---|
| Phase 2: I/O 调度器 | 第 2 层（vfs/） | 重写 I/O 包 |
| Phase 2: Blob 存储 | 第 3-B 层（format/ + column/） | 新增 Blob 页类型 |
| Phase 3: Zone Map | 第 3-B 层（encoding/） | 编码时计算 min/max，存入 PageHeader |
| Phase 3: IVF-PQ | 第 3-A 层（index/） | 新增索引实现 |
| Phase 4: 预取 / MiniBlock | 第 2 层（vfs/）+ 第 3-B 层（format/） | 智能预取 + 页内块结构 |
| Phase 5: 云原生 | 第 2 层（vfs/） | 新增 S3/GCS 后端（对上层完全透明） |
| Phase 6: WAL | 第 3-B 层（catalog/） | 在 Snapshot 旁新增 WAL 文件管理 |
| Phase 6: MVCC | 第 3-B 层（catalog/）+ 第 4 层（vego/） | Snapshot 支持多版本，Collection 支持快照读 |
| Phase 6: 标量索引 | 第 3-A 层（index/） | 新增 BTree/Bloom 索引实现 |

**关键洞察：** `storage/catalog/` 为 Phase 6 提供了一个清晰的扩展点 — 从 "JSON 文件管理" 平滑演进到 "WAL + MVCC 版本管理"，而不影响其他层。

---

## 8. 迁移策略

重构分 4 步渐进式执行。每步保持测试全绿，可独立合并。

### Step 0：提升 `core/` 和 `vfs/` 为顶层包

- 将 `storage/arrow/` → 顶层 `core/`。包名从 `arrow` 变为 `core`。
- 将 `storage/io/` → 顶层 `vfs/`。包名从 `io` 变为 `vfs`。
- 更新所有类型引用：`arrow.Schema` → `core.Schema`、`arrow.Array` → `core.Array` 等。
- `index/` 和 `storage/` 都依赖这两个包，因此它们不能放在 `storage/` 内部。
- **影响面：** ~4 个包，~15 个文件。编译器会捕获所有遗漏。
- **风险：低** — 纯重构（包重命名 + 类型引用更新），无行为变更。

### Step 1：抽出 `storage/catalog/`

- 从 `vego/storage.go` 中剥离 Snapshot（collection 状态元数据）、IDMapping 和 DeletionStore 到 `storage/catalog/`。
- 将 metadataStore 逻辑（version、schema、文件路径）迁入 `catalog.Snapshot`。
- `vego/storage.go` 改为调用 catalog 接口，不再直接管理状态。
- **风险：低** — 纯重构，无行为变更。

### Step 2：索引引擎剥离持久化 ✅ 已完成

> **前置条件：** 必须先完成 Step 0 — `index/storage.go` 当前 import 了 `storage/arrow/` 和 `storage/column/`，需要先变为 `core/` 才能移除。

- 为 `*index.HNSWIndex` 新增 `MarshalNodes` / `MarshalConnections` / `MarshalMetadata` 方法，以及 `UnmarshalNodes` / `UnmarshalConnections` / `UnmarshalMetadata` 函数。
- 移除 `index/storage.go`（直接写 Lance 文件的代码 + 对 `storage/column/` 和 `storage/encoding/` 的非法 import）。
- 将 schema 构建逻辑（`SchemaForNodes`、`SchemaForConnections`、`SchemaForMetadata`）保留在 `index/` 中，仅依赖 `core/`。
- API 层（`vego/index_persist.go`）接管持久化编排，使用 `storage/column/` 进行文件 I/O。
- **状态：** 已完成。`index/` 包零 `storage/` 依赖（仅 `core/`），全量测试通过。

### Step 3：IO 层修复 + column 接口化

- FilePool：将 Mutex 替换为 RWMutex。
- 删除重复的 Get/GetFile 方法。
- 修复 partial read 处理。
- 在 `storage/column/` 中正式化 `ColumnWriter` / `ColumnReader` 接口。
- **风险：低** — 定向修复。

---

## 9. 磁盘布局

重构后，单个 collection 的磁盘布局：

```
collection_path/
├── snapshot.json          # 唯一的 commit point（原子 rename）
│   ├── version
│   ├── data_file: "vectors.lance"
│   ├── index_files: ["index/nodes.lance", "index/connections.lance"]
│   ├── deletion_file: "deletions.dv"
│   └── metadata
├── vectors.lance          # Lance 列式格式（数据）
│   ├── Header (8KB)
│   ├── Pages（压缩列：id_hash, vector, timestamp, metadata）
│   └── Footer (PageIndexList)
├── mappings.json          # docID <-> nodeID 双向映射
├── deletions.dv           # 删除向量（bitset）
├── index/                 # HNSW 索引（Lance 格式）
│   ├── nodes.lance        # id + vector + level
│   └── connections.lance  # node_id + layer + neighbor_id
└── .txn_*/                # 临时事务目录（恢复时清理）
```

**HNSW 持久化格式 — 性能门限：**

> 100 万节点冷启动加载必须在 5 秒内完成。如果 Lance 列存格式无法达标（因逐行重建邻接表导致大量随机 I/O），设计允许回退到自定义二进制格式（扁平邻接数组 + mmap）。`MarshalNodes`/`UnmarshalNodes` 的接口在设计上与格式无关 — 不强制使用 Lance。

---

## 10. 设计原则总结

1. **单向依赖。** 上层依赖下层，永远不反向。无循环 import。
2. **索引引擎与持久化无关。** 它产出/消费 RecordBatch。它永远不接触文件系统。
3. **API 层是纯编排。** 它协调索引 + 存储 + catalog。它不实现算法或格式。
4. **Interface 属于消费者。** 提供者导出具体类型。消费者定义它所需的 interface。
5. **通过原子 snapshot 保证 crash safety。** 所有写入先到临时目录。snapshot.json rename 是唯一的 commit point。
6. **流式优于缓存（未来）。** 当数据集达到百万级时，大数据将通过回调（emit 函数）处理，不收集为巨大切片。当前实现使用单 batch 以简化代码。
7. **延迟抽象直到需要时。** 三行相似代码优于一个过早的抽象。第二个实现到来时再添加抽象。
