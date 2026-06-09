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
| 后台 Compaction 多文件并发写入 | Phase C append-only 实现后 | ✅ 高 — 多文件并发写入需要统一调度 |
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
