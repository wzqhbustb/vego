# VFS 写入架构评估

> **背景**：项目目标是让所有 I/O 经过 `vfs/` 层，并对 `vfs` writer 进行异步优化，以维护 5 层架构的合理性。
>
> **评估结论**："所有 I/O 走 `vfs`" 合理，但应走 `vfs` 的**同步文件抽象**，而非强制塞进**异步调度器**；当前写入的瓶颈在 `storage/` 层的 O(n) 重写和串行列编码，`vfs` 写入优化是锦上添花而非雪中送炭。

---

## 1. 当前 `vfs` 的本质

`vfs` 当前不是**通用文件系统抽象层**，而是一个**异步 I/O 调度器**。其设计假设是：

> "大量并发读取请求 → 优先级队列 → 批量调度 → worker pool 执行"

这个模型对**读取**非常合理（解决 4x 并发退化），但对**写入**存在语义错位：

| 特性 | 读取需求 | 写入需求 | 匹配度 |
|------|---------|---------|--------|
| 顺序性 | 随机（不同 page 不同 offset） | 严格顺序（header → pages → footer，offset 连续增长） | ❌ |
| 延迟敏感 | 是（查询延迟） | 否（flush 可后台完成） | ⚠️ |
| 合并收益 | 高（相邻 read 合并为单次大 read） | 低（本来就是顺序写入，syscall 合并收益有限） | ❌ |
| 回调语义 | 读取完返回数据 | 写入完只需确认成功，不需要返回数据 | ⚠️ |
| Offset 确定性 | 发起时已知 | 写入过程中动态递增 | ❌ |

---

## 2. 关键冲突点：ColumnWriter 的 Offset 追踪

`storage/column/writer.go` 中，`writeColumn` 的写入流程是**同步、顺序、依赖当前 offset 的**：

```go
// pageOffset 必须在写入前确定
pageOffset := w.currentPos

// 写入 page（必须在确定 offset 后立即执行）
n, err := page.WriteTo(w.file)

// 更新下一页的起点（下一列依赖这个值）
w.currentPos += n

// 将精确的 offset/size 记录到 footer 的 PageIndex
w.footer.PageIndexList.Add(columnIndex, pageNum, pageOffset, int32(n), ...)
```

如果写入是异步的（"提交请求 → 稍后被调度"）：
- `currentPos` 的追踪会变得极复杂（需要预分配所有 offset）
- 写入失败后的回滚逻辑会变得难以处理
- 页顺序的保证需要额外的同步机制

这不是不能实现，但在当前阶段投入产出比很低。

---

## 3. 判断一："所有 I/O 走 vfs" — ✅ 合理，但需要分层语义

**不是**"所有 I/O 都走 vfs 的**异步调度器**"，而是"所有 I/O 都经过 vfs 的**统一抽象**"。

### 建议的 `vfs` 分层设计

```
┌─────────────────────────────────────────┐
│  vfs.AsyncIO  (异步 I/O 调度器)          │  ← 仅用于读取优化
│  - Read() / ReadPages()                  │
│  - PriorityQueue + Coalescing            │
│  - 解决并发读取退化                       │
└─────────────────────────────────────────┘
                    ▲
┌─────────────────────────────────────────┐
│  vfs.FileIO   (统一文件抽象层)            │  ← 所有 I/O 都走这里
│  - Create() / Open() / Close()           │
│  - ReadAt() / WriteAt() / Sync()         │
│  - 可接入 FilePool 句柄复用               │
│  - 可支持 O_DIRECT 等底层优化             │
└─────────────────────────────────────────┘
```

- **`storage/column/reader.go` 读取**：走 `vfs.FileIO` + 可选 `vfs.AsyncIO`
- **`storage/column/writer.go` 写入**：**只走 `vfs.FileIO`**（同步写入）
- **`vego/storage.go` flush/rewrite**：走 `vfs.FileIO`（同步写入）

这样所有 I/O 确实"经过 vfs"，但写入不强制异步化，保持了 Lance 文件格式所需的同步顺序语义。

### `vfs.FileIO` 接口设计建议

按 open mode 区分返回类型，避免臃肿的上帝接口：

```go
// FileIO 是统一的文件操作入口
type FileIO interface {
    // Create 创建写入文件（ColumnWriter 使用）
    Create(path string) (WriteFile, error)
    // Open 打开只读文件（ColumnReader 使用）
    Open(path string) (ReadFile, error)
    // OpenReadWrite 打开读写文件（header 回写、flush 场景）
    OpenReadWrite(path string) (ReadWriteFile, error)
    // Remove 删除文件
    Remove(path string) error
    // Rename 原子重命名（temp → final）
    Rename(oldPath, newPath string) error
}

// WriteFile 顺序写入器 — ColumnWriter 拿到的就是这个
type WriteFile interface {
    io.Writer           // 顺序追加写入
    io.WriterAt         // 随机写入（header 回写用）
    Sync() error        // fsync
    Close() error
    Position() int64    // 当前写入偏移
}

// ReadFile 随机读取器 — ColumnReader 拿到的就是这个
type ReadFile interface {
    io.ReaderAt         // 随机读取
    Close() error
    Size() (int64, error)
}

// ReadWriteFile 读写文件 — flush/rewrite 场景
type ReadWriteFile interface {
    ReadFile
    io.Writer
    io.WriterAt
    Sync() error
    Truncate(size int64) error
}
```

**设计原则**：
- ColumnWriter 只能拿到 `WriteFile`，无法做随机读取 → 依赖关系最小化
- ColumnReader 只能拿到 `ReadFile`，无法意外写入 → 安全性
- `Position()` 由 `WriteFile` 内部维护，替代 ColumnWriter 自己追踪 `currentPos`
- 底层实现初期直接包装 `*os.File`，未来可替换为 buffered/O_DIRECT/cloud backend

---

## 4. 判断二："vfs writer 异步优化" — ⚠️ 优先级不高，当前不是瓶颈

> **注意**：这里讨论的是 `vfs` 层**写入调度器**（Write Coalescing、Priority Queue、Worker Pool 等异步机制）的优化优先级，不影响 Phase A 的架构统一迁移（让 storage 经过 `vfs.FileIO` 同步抽象）。Phase A 是架构一致性需求，与此处的性能判断无关。

写入瓶颈分析（按影响排序）：

### 瓶颈 #1：`flush()` 的 O(n) 全量重写（`vego/storage.go`）

```go
func (s *DocumentStorage) flush() error {
    docs, err := s.readAllDocuments()           // ← 读全部现有数据
    allDocs := append(existingDocs, s.writeBuffer...)
    return s.rewriteStorage(allDocs)            // ← 写全部（含历史数据）
}
```

**这不是"写入慢"，而是"不该重写的时候重写了"**。
- 1M 文档时，每次 flush 都读写整个文件
- 无论 vfs 怎么优化单次写入，都无法改变 O(n) 的时间复杂度

### 瓶颈 #2：串行列编码（`storage/column/writer.go`）

```go
for colIdx := 0; colIdx < batch.NumCols(); colIdx++ {
    w.writeColumn(int32(colIdx), column)        // ← 一列一列串行
}
```

**这是 CPU 瓶颈，不是 I/O 瓶颈**。
- `pageWriter.WritePages` 内部做 Zstd/BSS/RLE 编码，CPU 密集
- `page.WriteTo(w.file)` 已经是顺序大块写入，单个 syscall 即可完成
- 异步 I/O 调度不会让它更快

### 瓶颈 #3：page 级写入 I/O

`page.WriteTo(w.file)` 已经是顺序写入，syscall 开销占比极小。`vfs` 的写入合并、批量调度对此场景的收益微乎其微。

---

## 5. 建议的架构演进路线

不要同时做两件事。按优先级分阶段推进：

### Phase A（先做）：让 storage 写入"经过" vfs 的同步抽象

**目标**：架构一致性，不追求性能提升。

**具体工作**：
- 在 `vfs/` 层定义/扩展统一的 `FileIO` 接口（或扩展 `FilePool`）
- `ColumnWriter` 从 `os.Create()` 改为 `vfs.FileIO.Create()`
- `Reader` 从 `os.Open()` 改为 `vfs.FileIO.Open()`
- 所有文件操作（Create/Open/ReadAt/WriteAt/Sync/Close）统一走 `vfs`
- 写入保持同步语义，不接入 `AsyncIO`

**交付标准**：
- `storage/column/` 的 reader/writer 不再直接引用 `os` 包进行文件操作
- `vego/storage.go` 的 flush/rewrite 路径使用 `vfs.FileIO`
- 所有现有测试通过，写入行为无变化

### Phase B（次做）：append-only 格式 + 后台 flush

**目标**：解决 O(n) 重写，实现 1M 向量 < 30s 写入目标。这是写入吞吐质变的关键 — 在此之前并行编码的收益被 O(n) flush 吞噬。

**具体工作**：
- 设计 append-only 文件格式（新 batch 追加为新的 Row Group）
- Manifest 系统（P0-8）追踪活跃 Row Group
- 后台 compaction 合并碎片化的 Row Group
- `flush()` 从 O(n) 降为 O(buffer_size)

**依赖**：
- Manifest 系统（P0-8）必须先完成
- 这是 Roadmap 中 Bottleneck 1 的根本解决方案

**对应 Roadmap**：P0-12 写入吞吐（100万向量 < 30s）

### Phase C（最后）：storage 层并行列编码

**目标**：解决 CPU 编码瓶颈，在 Phase B 解除 I/O 瓶颈后进一步提升吞吐。

**为什么排在 Phase B 之后**：即使编码速度提升 3x，若 flush 仍为 O(n) 全量重写，用户感知的写入延迟不会质变。Phase B 完成后，flush 降为 O(buffer_size)，此时编码速度才成为真正瓶颈。

**具体工作**：
- `WriterConfig` 增加 `AsyncWrite bool` / `NumWorkers int` 选项
- `WriteRecordBatch` 内部：所有列并行编码（goroutine-per-column），然后按列顺序收集结果、顺序写入
- 保持文件布局确定性（顺序写入 encoded pages）

**预期收益**：
- 多列（10+ 窄列）场景下 2.5–3.5x 编码吞吐提升
- 单列（宽向量列）场景下收益有限（Amdahl 定律）

**对应 Roadmap**：P1-2 Writer 异步优化（`WithAsyncWrite(true)` 达到 800–1200 MB/s）

---

## 6. vfs 写入器的未来优化时机

`vfs` 层的写入优化（Write Coalescing、Worker 隔离、批量 Write API、写缓冲）**并非不必要**，而是应该在以下场景出现时才有价值：

| 场景 | 何时出现 | vfs 写入优化的价值 |
|------|---------|-------------------|
| 后台 Compaction 多文件并发写入 | Phase B append-only 实现后 | ✅ 高 — 多文件并发写入需要统一调度 |
| WAL / 日志追加写入 | Phase 6 | ✅ 高 — 高频小写入需要合并和批量 fsync |
| 缓存回写（Dirty Page Flush） | Phase 3+ Page Cache 实现后 | ✅ 中 — 后台批量回写 |
| 当前 `ColumnWriter` 顺序写入 | 现在 | ⚠️ 低 — 顺序大块写入本身已足够高效 |

**结论**：`vfs` 写入优化应作为基础设施保留，但当前不需要投入大量工程资源。先把 `storage/` 层的瓶颈解决，当多文件并发写入场景出现后再启用 `vfs` 写入调度。

---

## 7. 总结

| 问题 | 判断 | 优先级 |
|------|------|--------|
| 所有 I/O 走 `vfs` | ✅ 合理，走 `vfs.FileIO` 同步抽象，非 `AsyncIO` | Phase A |
| `storage` 接入 `vfs` | ✅ 先做，统一文件句柄管理 | Phase A |
| append-only 格式 | ✅ 根本解决 O(n) 重写，是写入吞吐质变的关键 | Phase B |
| `storage` 并行编码 | ✅ 有价值，但需 Phase B 先解除 O(n) 瓶颈后才显效 | Phase C |
| `vfs` writer 异步优化 | ⚠️ 当前不必要，等待多文件并发写入场景（Compaction/WAL） | 未来 |

> **一句话总结**：Phase A 让 storage 写入经过 vfs 同步抽象（架构统一）→ Phase B 实现 append-only 格式解除 O(n) flush 瓶颈（性能质变）→ Phase C 并行列编码进一步提升吞吐（锦上添花）→ 未来多文件并发场景出现时启用 vfs 写入调度器。


---

## 8. 本次 IO 模块工作决定实施的具体改进（从 Lance 借鉴）

基于对 Lance I/O 架构的分析和 Vego 嵌入式定位的适配，以下 5 项改进将在本次 IO 模块迭代中实施。按优先级排序：

### 8.1 临时文件 + 原子 rename（P0）

**Lance 做法**：`LocalWriter` 先写入 `NamedTempFile`，`shutdown` 时调用 `persist()` 做原子重命名。

**Vego 实现**：
- `column.NewWriterWithVFS` 改为写 `.tmp` 文件
- `Writer.Close()` 完成后调用 `fs.Rename(tmpPath, finalPath)`
- 启动时扫描并清理残留 `.tmp` 文件

**收益**：崩溃后不会留下半写的 `vectors.lance`，下次启动可安全回退到旧版本或空文件。

**改动位置**：
- `storage/column/writer.go` — `NewWriterWithVFS` / `Close`
- `vego/storage.go` — `cleanupTempFiles` 扩展扫描 `.tmp.*`

---

### 8.2 请求合并 — 简化版（P1）

**Lance 做法**：`FileScheduler::submit_request()` 将相邻 range（间距 < `block_size`）合并为一次 I/O，读取后再按原始 range 边界切片返回。

**Vego 简化版**：
- 不需要 Lance 的拆分逻辑（`max_iop_size` 是为适配云 API 限制）
- 只需：按 offset 排序 → 合并相邻 range → 发起批量 `ReadAt` → 切片返回

**为什么嵌入式也适用**：
- `storage/column/reader.go` 的 `readPagesAsync` 目前每个 page 发一次 `IORequest`
- 100 个 8KB page = 100 次 `ReadAt` syscall
- 合并后只需 1–2 次大 `ReadAt`，syscall 开销显著降低

**改动位置**：`storage/column/reader.go` 的 `readPagesAsync`，在提交 I/O 请求前做 range 合并预处理。

---

### 8.3 元数据缓存（P1）

**Lance 做法**：`LanceCache` 缓存文件 footer、column metadata、schema，通过尾读（最后 `block_size` 字节）一次性抓全。

**Vego 简化版**：
- 复用已有的 `BlockCache`，把 footer / column metadata 作为特殊 key 缓存
- 或在 `Reader` 实例内部直接缓存 footer / schema（Reader 生命周期内文件不变）

**收益**：避免每次 `NewReader` 时重复 seek → 读 header → seek 到末尾读 footer → seek 回 column metadata。

**改动位置**：
- `storage/column/reader.go` — `NewReader*` 系列缓存 footer
- `storage/format/page.go` / `footer.go` — 支持 metadata 缓存 key

---

### 8.4 列缓冲攒 page（P2，配合 append-only 格式）

**Lance 做法**：`FileWriter` 按列缓冲数据（默认 8MB/列），攒够后才编码成大 page。

**Vego 实现**：
- `Writer` 增加 `dataCacheBytes uint64` 配置，默认 8MB
- 每列独立缓冲，达到阈值或显式 flush 时才调用 `pageWriter.WritePages`
- 配置为 0 时保持现有行为（即来即编）

**为什么必须配合 append-only**：
- 当前 `flush()` 是 O(n) 全量重写。如果加了缓冲但格式不变，攒 8MB 后一次 flush 仍会重写整个文件，收益被 O(n) 吃掉。
- 正确顺序：先实现 append-only Row Group（Roadmap P0-8），再引入列缓冲。

**改动位置**：
- `storage/column/writer.go` — `Writer` struct 增加 `columnBuffers`
- `storage/column/writer.go` — `WriteRecordBatch` 先写入缓冲，再判断是否 flush

---

### 8.5 字节预算背压 — 简化版（P3）

**Lance 做法**：双维度背压——IOPS 预算 + 字节预算。`io_buffer_size_bytes` 限制"已读取但未解码"的数据量，consumer 消费完才释放。

**Vego 简化版**：
- 在 `vfs.AsyncIO` 的 Scheduler 增加 `maxBytesInFlight` 参数
- 提交请求时检查：当前飞行字节数 + 请求 size > 上限则阻塞
- consumer 解码完成后调用回调释放预算
- 默认值 64MB

**当前状态**（详见 Section 10）：
- 读取路径已经通过 `r.asyncIO.Read()` 接入 `vfs.AsyncIO` scheduler，不再是 goroutine-per-page 自发执行
- 但 `Scheduler.Submit` 只检查队列长度，没有字节预算限制
- 嵌入式并发查询量通常 1–10 个，OOM 风险可控，因此优先级仍为 P3

**改动位置**：
- `vfs/scheduler.go` / `vfs/async.go` — 在 Scheduler 增加 `maxBytesInFlight` 字节预算（reader 层无需改动，因为 `readPagesAsync` 已经走 scheduler）

---

## 9. 实施优先级总结

| 优先级 | 改进项 | 与部署形态无关性 | 阻塞依赖 | 预计收益 |
|---|---|---|---|---|
| **P0** | 临时文件 + 原子 rename | ✅ 完全无关 | 无 | 数据安全基础 |
| **P1** | 请求合并（简化版） | ✅ 完全无关 | 无 | 读延迟 -30%~-50% |
| **P1** | 元数据缓存 | ✅ 完全无关 | 无 | 减少重复 metadata I/O |
| **P2** | 列缓冲攒 page | ✅ 完全无关 | append-only 格式 | 压缩比 +20~40% |
| **P3** | 字节预算背压 | ✅ 完全无关 | 无（读取路径已接入 scheduler，见 Section 10） | 防止高并发 OOM |

> **执行策略**：先做 8.1（安全）、8.2 和 8.3（读优化，独立且低风险），8.4 等 append-only 格式就绪后接入，8.5 在 scheduler 层增加字节预算即可（读取路径已接入 scheduler，实现成本降低）。

---

## 10. 当前状态勘误：读取路径已接入 `vfs.AsyncIO`

在编写本章规划前，重新审计了 `storage/column/reader.go` 的 `readPagesAsync` 实现，发现**读取路径实际上已经接入了 `vfs.AsyncIO` scheduler**，与第 8.5 节中"读取路径尚未接入 scheduler"的假设不同。

### 10.1 代码现状

```go
// storage/column/reader.go:478
resultCh := r.asyncIO.Read(ctx, r.fileID, pageIdx.Offset, pageIdx.Size)
```

`readPagesAsync` 当前流程：
1. 对 `pageIndices` 中每个 page 单独 spawn goroutine
2. 每个 goroutine 调用 `r.asyncIO.Read(...)` 提交一个 `IORequest`
3. 等待 `resultCh` 返回后本地解码

这意味着：
- ✅ **Scheduler 优先级队列已经在生效** — 请求会进入 `vfs.Scheduler` 的优先级堆
- ✅ **Executor 线程池已经在执行实际 I/O** — `FilePool` 负责 `ReadAt`
- ❌ **但还没有 range 合并** — 每个 page 仍是独立的 `IORequest`
- ❌ **但还没有字节预算背压** — `Scheduler.Submit` 只检查队列长度，不检查飞行字节数

### 10.2 对后续规划的影响

| 原计划假设 | 实际状态 | 影响 |
|---|---|---|
| P3 背压需要先让读取路径接入 scheduler | 已经接入 | **P3 前置依赖消除**，只需在 scheduler 增加字节预算 |
| P1 请求合并需要在 reader 层实现 | 确实需要在 reader 层实现 | 不变，仍是 Wave 2 重点 |
| P1 元数据缓存依赖 Reader 生命周期 | 可以在 Reader 内缓存 footer | 实现简单，风险低 |

**结论**：P3 背压的实施难度从"中"降为"低"，可以比原计划更早准备；但正确顺序仍然是 P0 → P1 → append-only → P2 → P3，不会因为 P3 变容易而打乱节奏。

---

## 11. IO 模块综合收尾规划

本章给出从当前状态到"I/O 基础设施可用"的完整路线图，包含 6 个 Waves。前 3 个 Waves（P0/P1）可立即执行；后 3 个 Waves（P2/P3 前置 + 最终实现）需要等待对应依赖完成。

### 11.1 Wave 1 — P0：原子写入（Atomic Rename）

**目标**：崩溃后不会留下半写文件。任何写入操作都遵循"写临时文件 → fsync → 原子 rename"模式。

**改动点 1：`storage/column/writer.go`**

```go
type Writer struct {
    // 新增
    fs        vfs.VFS  // 用于 Rename / Remove 临时文件
    finalPath string
    tmpPath   string
    // ... 现有字段
}

func NewWriterWithVFS(filename string, fs vfs.VFS, ...) (*Writer, error) {
    tmpPath := tempFileName(filename) // 使用 ".vego-tmp." infix
    file, err := fs.Create(tmpPath)
    // ...
    w.fs = fs
    w.finalPath = filename
    w.tmpPath = tmpPath
}

func (w *Writer) Close() error {
    if w.closed { return nil }
    // 1. 写 footer
    // 2. Sync
    if err := w.file.Sync(); err != nil { return err }
    if err := w.file.Close(); err != nil { return err }
    // 3. 原子 rename
    if err := w.fs.Rename(w.tmpPath, w.finalPath); err != nil {
        _ = w.fs.Remove(w.tmpPath) // 清理
        return err
    }
    w.closed = true
    return nil
}
```

**注意**：writer 需要保存 `vfs.VFS` 引用以调用 `Rename`。

**改动点 2：`vego/storage.go` `rewriteStorage()`**

```go
func (s *DocumentStorage) rewriteStorage(docs []*Document) error {
    // 当前：直接覆盖 dataFile
    // 目标：
    // 1. 写 dataFile.tmp.<nonce>
    // 2. fsync
    // 3. Rename 覆盖 dataFile
    // 4. 清理旧文件（OS 自动处理）
}
```

`saveDeletionVector()` 同样改为 temp + rename。

**改动点 3：启动时清理残留 `.tmp.*` 文件**

```go
// vego 临时文件命名约定：<originalname>.vego-tmp.<nonce>
// 使用 ".vego-tmp." 前缀避免与用户文件中偶然包含 ".tmp." 的情况冲突
const tempFileInfix = ".vego-tmp."

func cleanupTempFiles(path string, fs vfs.VFS) error {
    entries, err := fs.ReadDir(path)
    if err != nil {
        return err
    }
    for _, e := range entries {
        name := e.Name()
        // 严格匹配 vego 生成的临时文件：包含 ".vego-tmp." infix
        if strings.Contains(name, tempFileInfix) {
            _ = fs.Remove(filepath.Join(path, name))
        }
    }
    return nil
}

// generateNonce 生成临时文件后缀，避免并发冲突
func generateNonce() string {
    return fmt.Sprintf("%d-%x", time.Now().UnixNano(), rand.Int63())
}

// tempFileName 生成符合 vego 约定的临时文件名
func tempFileName(finalPath string) string {
    return finalPath + tempFileInfix + generateNonce()
}
```

**验收标准**：
- [ ] `Writer` 写 `.vego-tmp.` 文件，Close 时 rename
- [ ] `rewriteStorage` 写 `.vego-tmp.` 文件，完成后 rename
- [ ] `saveDeletionVector` 写 `.vego-tmp.` 文件，完成后 rename
- [ ] 启动时自动清理残留 `.vego-tmp.*`
- [ ] 模拟崩溃（kill -9 在写入中途）后，旧文件保持完整
- [ ] 所有现有测试通过，race detector 无报警

**单进程假设**：临时文件清理策略假设 Vego 以单进程方式访问数据目录，这是当前嵌入式定位的默认部署模式。`cleanupTempFiles` 在启动时无条件删除所有 `.vego-tmp.*` 文件，因此如果多个进程并发访问同一目录，可能误删其他进程正在写入的临时文件。未来若支持多进程并发访问，需引入文件锁（`flock`）或在临时文件名中嵌入 PID 来避免误删活跃临时文件。

---

### 11.2 Wave 2 — P1：请求合并（Range Coalescing）

**目标**：将相邻的 page 读取请求合并为单次 `ReadAt`，减少 syscall 次数。

**合并策略（简化版）**：

1. 按 `offset` 对 `pageIndices` 排序（保留原始索引映射）
2. 遍历排序后的 page，若 `next.offset - (current.offset + current.size) <= maxGap`，则合并
3. `maxGap` 默认取 `4KB`（一个典型 OS block size），可配置
4. 发起合并后的 `ReadAt`，按原始边界切片返回

**实现草图**：

```go
// storage/column/reader.go

type mergedRange struct {
    offset   int64
    size     int64
    indices  []int   // 原始 pageIndices 中的位置
}

func coalesceRanges(pages []format.PageIndex, maxGap int64, maxMergeSize int64) []mergedRange {
    if len(pages) == 0 { return nil }
    // 按 offset 排序，保留原始索引
    type indexed struct { idx int; p format.PageIndex }
    ordered := make([]indexed, len(pages))
    for i, p := range pages { ordered[i] = indexed{i, p} }
    sort.Slice(ordered, func(i, j int) bool {
        return ordered[i].p.Offset < ordered[j].p.Offset
    })

    const defaultMaxMergeSize = 1024 * 1024 // 1MB，防止单次读取过大导致内存峰值
    if maxMergeSize <= 0 { maxMergeSize = defaultMaxMergeSize }

    var ranges []mergedRange
    cur := mergedRange{
        offset:  ordered[0].p.Offset,
        size:    int64(ordered[0].p.Size),
        indices: []int{ordered[0].idx},
    }
    for i := 1; i < len(ordered); i++ {
        p := ordered[i].p
        end := cur.offset + cur.size
        gap := p.Offset - end
        candidateSize := p.Offset + int64(p.Size) - cur.offset
        if gap <= maxGap && candidateSize <= maxMergeSize {
            cur.size = candidateSize
            cur.indices = append(cur.indices, ordered[i].idx)
        } else {
            ranges = append(ranges, cur)
            cur = mergedRange{offset: p.Offset, size: int64(p.Size), indices: []int{ordered[i].idx}}
        }
    }
    ranges = append(ranges, cur)
    return ranges
}
```

**在 `readPagesAsync` 中接入**：

```go
func (r *Reader) readPagesAsync(pageIndices []format.PageIndex, dataType core.DataType) ([]core.Array, error) {
    if !r.useAsync || !r.asyncEnabled {
        return r.readPagesSync(pageIndices, dataType)
    }

    // 1. 合并相邻 range
    ranges := coalesceRanges(pageIndices, r.coalesceGap, r.maxMergeSize)

    // 2. 为每个合并后的 range 发起一次 Read
    // 【设计说明】这里有意直接调用 r.file.ReadAt，而不是走 r.asyncIO.Read()。
    // 原因：range 合并已经把 N 个 page 请求降成 M 个 range 请求（通常 M << N），
    // 再用 goroutine-per-range 执行同步 ReadAt 足够简单高效；若重新拆成单个 IORequest
    // 提交给 scheduler，反而失去合并的收益。未来若需要统一优先级/背压，可把合并后的
    // range 作为一个大 IORequest 提交给 scheduler（见 Wave 6）。
    //
    // 【注意】此路径绕过了 scheduler，因此 Wave 6 的字节预算背压对合并读取无效。
    // 如果后续需要对合并路径也施加背压，需将合并后的 range 作为单个大 IORequest
    // 提交给 scheduler（届时 req.Size = mergedRange.size）。
    arrays := make([]core.Array, len(pageIndices)) // slice 按索引写入，天然并发安全
    errChan := make(chan error, len(ranges))
    var wg sync.WaitGroup

    for _, mr := range ranges {
        wg.Add(1)
        go func(mr mergedRange) {
            defer wg.Done()
            buf := make([]byte, mr.size)
            n, err := r.file.ReadAt(buf, mr.offset)
            if err != nil {
                errChan <- err
                return
            }
            if int64(n) < mr.size {
                errChan <- fmt.Errorf("short read: got %d, want %d", n, mr.size)
                return
            }

            // 3. 按原始边界切片解码
            for _, pageIdx := range mr.indices {
                p := pageIndices[pageIdx]
                pageOff := p.Offset - mr.offset
                pageBuf := buf[pageOff : pageOff+int64(p.Size)]
                array, err := r.pageReader.ReadPageFromData(pageBuf, p.Encoding, p.NumValues, dataType)
                // ...
                arrays[pageIdx] = array // 直接写入对应索引位置
            }
        }(mr)
    }

    wg.Wait()
    return arrays, nil
}
```

**注意**：这里的 goroutine 是 range 级别的（可能包含多个 page），而不是 page 级别的。合并后 goroutine 数量从 `N pages` 降到 `M ranges`，`M <= N`。

**验收标准**：
- [ ] 100 个连续 8KB page 合并为 1 次 `ReadAt`
- [ ] 100 个间隔 1MB 的 page 保持 100 次独立读取
- [ ] 合并前后的解码结果与非合并路径完全一致（字节级相等）
- [ ] Benchmark 显示读延迟降低 30%+

---

### 11.3 Wave 3 — P1：元数据缓存

**目标**：避免每次 `NewReader` 都重复读取 header/footer/metadata。

**方案 A（首选）：Reader 内部缓存（sync.Once）**

```go
type Reader struct {
    // 现有字段 ...
    footerOnce   sync.Once
    cachedFooter *format.Footer
    footerErr    error
}

func (r *Reader) loadFooter() (*format.Footer, error) {
    r.footerOnce.Do(func() {
        r.cachedFooter, r.footerErr = r.readFooterFromFile()
    })
    return r.cachedFooter, r.footerErr
}
```

**方案 B（备选）：接入 `BlockCache`**

```go
// 使用特殊 key，例如 "<fileID>:footer"
cacheKey := column.GenerateCacheKey(fileID) + ":footer"
if cached, ok := blockCache.Get(cacheKey); ok {
    footer = cached.(*format.Footer)
} else {
    footer = readFooterFromFile()
    blockCache.Put(cacheKey, footer, int64(footer.Size()))
}
```

**推荐方案 A**，原因：
- `Reader` 生命周期内文件内容不变（Vego 当前是 copy-on-write / append-only）
- 实现简单，不需要处理缓存失效
- `BlockCache` 更适合 page data 这种大块、可共享的缓存

**验收标准**：
- [ ] 同一个 `Reader` 实例多次读取 footer 只发生一次 I/O
- [ ] `loadFooter()` 对并发调用是线程安全的（sync.Once 或 mutex）
- [ ] 不影响现有测试

---

### 11.4 Wave 4 — P2 前置：append-only 文件格式 + Manifest 系统

**目标**：从根本上解除 `flush()` 的 O(n) 重写瓶颈。没有这一步，P2 列缓冲无法显效。

**4.1 文件格式变更**

当前格式（伪代码）：

```
[Header][Column 0 Pages][Column 1 Pages]...[Footer]
```

append-only 格式：

```
[Global Header]
[Row Group 0 Header][RG0 Pages][RG0 Footer]
[Row Group 1 Header][RG1 Pages][RG1 Footer]
...
[Global Footer]  <-- 包含所有 Row Group 偏移、schema、metadata
```

- 每个 Row Group 是**自包含的 mini 文件**：有自己的 header/pages/footer
- `flush()` 新 batch 时，只需追加一个新的 Row Group
- `Global Footer` 在文件末尾，包含所有 Row Group 的偏移列表

**4.2 Manifest 系统（P0-8）**

> **注意**：`storage/format/manifest.go` 已有 `Manifest` 结构体（含 `Version`、`DataFiles`、`IndexFiles`、`ManifestManager` 等）。以下设计是**扩展现有实现**，增加 `RowGroups` 字段和 `Schema` 引用，而非替换。

```go
type Manifest struct {
    Version       int64
    Schema        *core.Schema
    RowGroups     []RowGroupInfo
    DeletedRows   []uint32  // 全局删除标记
}

type RowGroupInfo struct {
    ID           int
    FileOffset   int64
    NumRows      int64
    ColumnStats  []*format.ColumnStatistics
}
```

Manifest 持久化（多版本策略）：
- 文件命名：`manifest-<version>.json`（如 `manifest-000042.json`）
- 每次 `flush()` 写入新版本号的 manifest 文件（tmp + rename）
- 启动时取最大版本号的 manifest 作为 source of truth
- 保留最近 N 个版本（默认 3），GC 更旧的版本
- 多版本策略确保崩溃时总有上一个完整的 manifest 可回退

**GC 策略**：
- **触发时机**：每次成功写入新版本 manifest 并完成 rename 后，同步删除 `version < current - N` 的旧文件
- **幂等性**：删除操作使用 `os.Remove`，文件不存在时忽略（`os.IsNotExist`），无需原子性
- **GC 失败处理**：删除旧 manifest 失败不影响正确性（只浪费少量磁盘空间），记录 warning 日志即可，不返回错误
- **启动时补偿**：如果上次 GC 未完成（写入新版本后崩溃），启动时 `loadLatestManifest` 遍历目录时顺便清理多余版本

```go
// Manifest 文件命名
func manifestFileName(version int64) string {
    return fmt.Sprintf("manifest-%06d.json", version)
}

// 启动时加载最新 manifest
func loadLatestManifest(dir string, fs vfs.VFS) (*Manifest, error) {
    entries, _ := fs.ReadDir(dir)
    // 按版本号降序排列，取第一个能正确解析的
    // 如果最新的损坏（不应发生，因为是原子写入），自动回退到上一个版本
}
```

**4.2a Global Footer 定位（可选优化，非必须组件）**

Global Footer 是**读取加速缓存**，避免启动时必须解析 manifest 文件。但在嵌入式场景下 manifest 通常 < 10KB，加载耗时可忽略。因此：
- **初期实现可以省略 Global Footer**，只用 manifest
- 当 Row Group 数量极多（1000+）且启动延迟敏感时，再引入 Global Footer 作为优化
- 如果引入，其写入失败不影响数据正确性（恢复时以 manifest 为准）

**与 Reader 的兼容性说明**：省略 Global Footer 不影响 Reader 的正确性。Reader 始终从 manifest 获取 Row Group 列表，然后对每个 Row Group 分别读取其 header/footer 来获取 page offsets。Global Footer 仅在 Row Group 数量极大时（1000+）用于加速启动时的偏移定位，避免逐个读取每个 Row Group 的 footer。无论是否有 Global Footer，Reader 都需要适配多 Row Group 读取（见 4.4 节），重构工作量不变。

**4.3 `DocumentStorage.flush()` 改造**

```go
func (s *DocumentStorage) flush() error {
    // 当前：readAllDocuments + rewriteStorage(allDocs)
    // 目标：
    // 1. 将 s.writeBuffer 编码为新的 Row Group
    // 2. 追加到 column 文件末尾
    // 3. 更新 Manifest，写入新的 manifest.json
    // 4. 清空 writeBuffer
    // 5. 保存 deletion vector（增量或全量）
}
```

**4.4 Reader 支持多 Row Group**

```go
func (r *Reader) ReadRows(start, end int64) (*core.RecordBatch, error) {
    // 1. 从 manifest 找出跨越了哪些 Row Group
    // 2. 对每个 Row Group 调用 readRowGroup(start, end)
    // 3. 拼接结果
}
```

**4.5 Compaction（可选，先设计后实现）**

```go
func (s *DocumentStorage) Compact() error {
    // 1. 读取所有 Row Group
    // 2. 合并为更少、更大的 Row Group（例如目标 64MB / group）
    // 3. 原子重写 column 文件
    // 4. 更新 manifest
}
```

**4.6 Global Footer 原子性与崩溃恢复**

`Global Footer` 位于文件末尾，每次追加 Row Group 后都需要重写。这里存在一个边界风险：

- **正常流程**：追加 Row Group → 重写 Global Footer → fsync → 更新 manifest
- **崩溃场景**：如果恰好在重写 Global Footer 时进程崩溃，文件中会多出一个没有 footer 条目指向的"尾部孤儿 Row Group"

**恢复策略（以 manifest 为准）**：

```go
func recoverFromManifest(path string, fs vfs.VFS) (*Manifest, error) {
    // 1. 读取最新的 manifest.json（写 tmp + rename，自身是原子的）
    manifest, err := loadLatestManifest(path, fs)
    if err != nil {
        return nil, err
    }

    // 2. 校验 column 文件长度
    fileLen, _ := fs.Stat(columnFile)

    // 3. 计算 manifest 中最后一个 Row Group 的结束偏移
    lastRG := manifest.RowGroups[len(manifest.RowGroups)-1]
    expectedLen := lastRG.FileOffset + lastRG.Length

    // 4. 如果 fileLen > expectedLen，说明存在尾部垃圾数据，截断到 expectedLen
    if fileLen > expectedLen {
        _ = fs.Truncate(columnFile, expectedLen)
    }

    // 5. 如果 fileLen < expectedLen，说明 footer 写一半，以 manifest 为权威，
    //    但可能需要从上一个已知完整点恢复（更复杂的场景可留到后续版本）
    return manifest, nil
}
```

**关键原则**：
- **Manifest 是真理来源（source of truth）**：Row Group 是否有效由 manifest 决定，而不是 Global Footer
- **Global Footer 是读取加速缓存**：启动时可以用 footer 快速定位 Row Group，但启动恢复流程必须以 manifest 为准
- **每次 flush 顺序**：写新 Row Group → fsync → 重写 Global Footer → fsync → 写 manifest（tmp + rename）

**验收标准**：
- [ ] 新 batch 写入只追加，不读取历史数据
- [ ] 1M 向量写入时间 < 30s（Roadmap P0-12 目标）
- [ ] manifest 崩溃后可恢复（写 tmp + rename）
- [ ] Reader 能正确读取多 Row Group

---

### 11.5 Wave 5 — P2：列缓冲攒 page

**目标**：在 append-only 格式基础上，按列缓冲数据，达到阈值后编码成大 page，提升压缩率和吞吐。

**设计**：

```go
type Writer struct {
    // 新增
    dataCacheBytes uint64
    columnBuffers  []columnBuffer
}

type columnBuffer struct {
    field    *core.Field
    chunks   []core.Array  // 累积的 value arrays
    numBytes int64
}

func WithDataCacheBytes(bytes uint64) WriterOption {
    return func(w *Writer) { w.dataCacheBytes = bytes }
}
```

**流程**：

```go
func (w *Writer) WriteRecordBatch(batch *core.RecordBatch) error {
    for colIdx := 0; colIdx < batch.NumCols(); colIdx++ {
        buf := &w.columnBuffers[colIdx]
        buf.chunks = append(buf.chunks, batch.Column(colIdx))
        buf.numBytes += estimateSize(batch.Column(colIdx))
    }

    if w.shouldFlush() {
        return w.flushBuffers()
    }
    return nil
}

func (w *Writer) shouldFlush() bool {
    if w.dataCacheBytes == 0 { return true } // 即来即编
    // 按总字节数判断，避免窄列永远不触发 flush
    var totalBytes int64
    for _, buf := range w.columnBuffers {
        totalBytes += buf.numBytes
    }
    return totalBytes >= int64(w.dataCacheBytes)
}

func (w *Writer) flushBuffers() error {
    // 1. 对每列的 chunks 合并成一个大 Array
    // 2. 构建 RecordBatch
    // 3. 直接调用底层写入函数（绕过缓冲逻辑，避免递归）
    //    - asyncWrite=true 时: w.writeRecordBatchAsync(batch)
    //    - asyncWrite=false 时: 遍历调用 w.writeColumnSync(colIdx, batch)
    //    注意：不能调用 WriteRecordBatch()，否则会再次进入缓冲 → shouldFlush → flushBuffers 递归
    // 4. 清空 buffers
}
```

**关键约束**：
- 在 Wave 4 完成前**不启动实现**
- `dataCacheBytes=0` 时行为与现在完全一致（零成本开关）
- 缓冲数据丢失风险：只在 `flush()` 时持久化，上层 `DocumentStorage.flush()` 决定持久化频率

**验收标准**：
- [ ] `dataCacheBytes=0` 时现有测试全部通过
- [ ] `dataCacheBytes=8MB` 时宽表压缩比提升 20%+
- [ ] 缓冲达到阈值后正确生成 Row Group
- [ ] 崩溃恢复不丢失已 flush 的 Row Group

---

### 11.6 Wave 6 — P3：字节预算背压

**目标**：限制"已提交但尚未解码完成"的字节数，防止高并发读取时 OOM。

**当前状态修正**：读取路径已经接入 scheduler，因此背压直接在 `vfs.Scheduler` 层实现即可。

**6.1 Scheduler 增加字节预算**

```go
type Scheduler struct {
    // 新增
    maxBytesInFlight int64
    bytesInFlight    int64
    bytesCond        *sync.Cond
}

func NewScheduler(executor *Executor, maxQueueSize int, maxBytesInFlight int64) *Scheduler {
    s := &Scheduler{
        // ...
        maxBytesInFlight: maxBytesInFlight,
    }
    s.bytesCond = sync.NewCond(&s.mu)
    return s
}

func (s *Scheduler) Submit(req *IORequest) error {
    s.mu.Lock()
    for s.bytesInFlight+int64(req.Size) > s.maxBytesInFlight {
        s.bytesCond.Wait()
    }
    s.bytesInFlight += int64(req.Size)
    // ... 入队
    s.mu.Unlock()
}

func (s *Scheduler) completeRequest(req *IORequest) {
    s.mu.Lock()
    s.bytesInFlight -= int64(req.Size)
    s.bytesCond.Signal()
    s.mu.Unlock()
}
```

**6.1a completeRequest 的调用点**

当前 `Executor` 处理完 `IORequest` 后把结果发送到 `req.Callback`，没有统一的 completion 回调。需要新增一个包装层：

```go
// Executor 的 worker 在完成请求时调用
func (s *Scheduler) onRequestDone(req *IORequest, result IOResult) {
    s.completeRequest(req) // 释放字节预算
    // 继续原有的回调逻辑
    select {
    case req.Callback <- result:
    case <-req.Context.Done():
    }
}
```

或者更轻量的做法：在 `Executor` 内部完成 `ReadAt`/`WriteAt` 后，先调用 `scheduler.completeRequest(req)` 再写 `req.Callback`。这样字节预算的释放与 I/O 完成严格同步，与 consumer 是否已开始解码无关（保守但安全）。

后续 Wave 2 如果走 "大 IORequest 提交给 scheduler" 路线，同一个机制直接复用。

**6.2 Config 增加字节预算参数**

```go
type Config struct {
    Workers          int
    QueueSize        int
    SchedulerCap     int
    MaxBytesInFlight int64 // 新增，默认 64MB
}

func DefaultConfig() *Config {
    return &Config{
        // ...
        MaxBytesInFlight: 64 * 1024 * 1024,
    }
}
```

**6.3 Reader 层默认使用预算**

由于 `readPagesAsync` 已经通过 `r.asyncIO.Read()` 提交请求，背压会自动生效，无需 reader 额外改动。

如果 Wave 2 的请求合并也在，实际提交的是合并后的大请求，`req.Size` 是合并 range 的总字节数，背压效果更直接。

**验收标准**：
- [ ] 提交 100 个 16MB page 读取请求时，飞行字节数不超过 64MB
- [ ] 解码完成后飞行字节数正确下降
- [ ] 高并发读取 benchmark 不 OOM
- [ ] 背压关闭时（`MaxBytesInFlight=0`）行为与现在一致
- [ ] 明确文档：当前背压仅覆盖 scheduler 路径；Wave 2 的合并读取路径（直接 ReadAt）不受背压控制，需后续版本将合并 range 作为大 IORequest 提交给 scheduler 后统一管理

---

## 12. 依赖关系与执行顺序

### 12.1 依赖图

```
Wave 1: P0 原子写入
    │
    ▼
Wave 2: P1 请求合并 ──┐
    │                 │
Wave 3: P1 元数据缓存─┘ 并行，互相独立
    │
    ▼
Wave 4: P2 前置 — append-only 格式 + Manifest
    │
    ▼
Wave 5: P2 列缓冲攒 page

Wave 6: P3 字节预算背压 ── 可并行于 Wave 4/5，
                         但推荐在 Wave 2 之后（range 合并让预算更精确）
```

### 12.2 为什么必须按这个顺序

| 顺序约束 | 原因 |
|---|---|
| P0 先做 | 安全基线。后续 Waves 会追加文件、写 manifest，如果写入不原子，任何一步崩溃都可能导致不可恢复的状态。 |
| P1 在 P2 之前 | 读优化是独立的，不依赖格式变更；且 P1 能为 append-only 格式下更频繁的 manifest/footer 读取提速。 |
| Wave 4 在 Wave 5 之前 | 列缓冲攒 page 的收益建立在"追加新 Row Group"之上。若 `flush()` 仍是 O(n) 重写，缓冲只会让单次重写数据量更大，得不偿失。 |
| Wave 2 在 Wave 6 之前 | range 合并后，每个 `IORequest.Size` 更接近真实 I/O 字节数，背压计算更精确。没合并前 100 个 8KB page = 100 个请求，每个请求单独占预算，背压触发过早。 |

### 12.3 时间线建议（以当前 1 人全职当量估算）

| Wave | 预计工时 | 可并行 | 阻塞点 |
|---|---|---|---|
| Wave 1 P0 原子写入 | 2–3 天 | 否 | 无 |
| Wave 2 P1 请求合并 | 2–3 天 | 可与 Wave 3 并行 | 无 |
| Wave 3 P1 元数据缓存 | 1–2 天 | 可与 Wave 2 并行 | 无 |
| Wave 4 P2 前置 append-only | 3–4 周 | 否 | 需要格式设计评审 |
| Wave 5 P2 列缓冲 | 1 周 | 否 | 依赖 Wave 4 |
| Wave 6 P3 背压 | 2–3 天 | 可与 Wave 4/5 并行 | 推荐在 Wave 2 之后 |

**总周期**：约 5–6 周完成全部 6 个 Waves。

---

## 13. 验收标准、风险与回滚策略

### 13.1 综合验收标准

- [ ] **安全性**：任意 Wave 完成后，`kill -9` 在写入过程中不会损坏已有数据
- [ ] **一致性**：sync/async、合并/非合并、缓存/非缓存 路径的读取结果字节级一致
- [ ] **性能**：
  - P0：无性能退化
  - P1：读延迟降低 30%+
  - P2：1M 向量写入 < 30s，压缩比提升 20%+
  - P3：高并发读取 benchmark 不 OOM
- [ ] **兼容性**：`dataCacheBytes=0`、`useAsync=false`、`maxBytesInFlight=0` 时保持当前行为
- [ ] **可观测性**：Scheduler Stats 暴露 `BytesInFlight` 指标

### 13.2 主要风险

| 风险 | 概率 | 影响 | 缓解措施 |
|---|---|---|---|
| 原子 rename 在 Windows/网络文件系统上行为不一致 | 中 | 数据安全 | VFS 层抽象 `Rename`，对不同 backend 做适配；Windows 用 `MoveFileEx` 或 fallback 到 copy+delete |
| append-only 格式导致文件无限增长 | 中 | 磁盘空间 | 必须同时设计 Compaction；设置 `max_file_size` 触发自动合并 |
| append-only 格式与现有文件的向后兼容迁移 | 中 | 升级阻碍 | 提供 `migrate` 工具/函数将旧格式单 Row Group 文件转为新格式；首次 Open 时自动检测版本并触发迁移；迁移前备份原文件 |
| 列缓冲导致延迟 flush 丢失数据 | 低 | 数据丢失 | 缓冲只存在于 Writer 内存；真正持久化仍由上层 `flush()` 触发；文档明确说明 |
| 背压引入死锁（消费线程被阻塞在 Submit） | 低 | 查询 hang | 使用 context.WithTimeout；Budget 释放必须在 Executor callback 中保证 |
| range 合并后大 Read 导致单次解码内存峰值 | 中 | OOM | 合并上限限制（例如单个 merged range 不超过 1MB） |

### 13.3 回滚策略

每个 Wave 完成后打 tag：
- `v0.1.6-io-p0`：原子写入完成
- `v0.1.7-io-p1`：请求合并 + 元数据缓存完成
- `v0.1.8-io-p2-prep`：append-only 格式完成
- `v0.1.9-io-p2`：列缓冲完成
- `v0.1.10-io-p3`：背压完成

如果某个 Wave 引入问题，直接回退到上一个 tag，不影响其他 Waves 的代码。

---

## 14. 本次迭代决策

**建议的本次迭代范围**：**Wave 1 + Wave 2 + Wave 3**（P0 + P1 全部）。

理由：
1. **独立、低风险**：这三项不依赖 append-only 格式，也不互相阻塞
2. **立即收益**：原子写入解决数据安全，请求合并和元数据缓存显著降低读延迟
3. **为后续铺路**：P0 的安全基线是 append-only 格式的前提；P1 的合并让 P3 背压更精确

**明确不进入本次迭代**：
- Wave 4/5（append-only + 列缓冲）：需要单独的设计评审，周期 2–3 周
- Wave 6（背压）：实现简单，但建议等 Wave 2 合并完成后再接入，预算计算更准确

**下一步动作**：
1. 用户确认本次迭代范围（推荐 Wave 1+2+3）
2. 先实现 Wave 1 P0 原子写入
3. 再并行实现 Wave 2 请求合并 和 Wave 3 元数据缓存
4. 全部测试通过后，进入 Wave 4 append-only 格式的专项设计

---

> **最终结论**：I/O 模块的剩余工作已完整规划为 6 个 Waves，覆盖 P0/P1/P2/P3。P0/P1 可立即执行且收益明确；P2/P3 需要等待 append-only 格式等前置条件。建议本次迭代聚焦 Wave 1–3，稳扎稳打地为后续大改造奠定安全与性能基础。
