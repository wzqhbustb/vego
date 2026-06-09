# Lance 源码写入 I/O 分析

> **分析目标**：深入 Lance Rust 源码（`/Users/wangyang/lance_papers/lance`），研究其在 I/O 层面的写入设计和优化，为 Vego 的写入架构决策提供参考。
>
> **核心发现**：Lance 的 I/O Scheduler **仅用于读取**，写入走的是独立的 `Writer` trait 抽象；编码层并行、写入层串行；这与 Vego 当前架构评估的判断高度一致。

---

## 1. Lance 的 I/O 抽象架构

### 1.1 ObjectStore 包装器

Lance 基于 Apache Arrow Rust 的 `object_store` crate，在其之上做了薄封装：

```rust
// rust/lance-io/src/object_store.rs
pub struct ObjectStore {
    pub inner: Arc<dyn OSObjectStore>,   // arrow-rs object_store trait object
    scheme: String,
    block_size: usize,
    max_iop_size: u64,
    use_constant_size_upload_parts: bool,
    list_is_lexically_ordered: bool,
    io_parallelism: usize,
    io_tracker: IOTracker,
    store_prefix: String,
}
```

- `inner` 是 `dyn object_store::ObjectStore`，由 `LocalFileSystem`、`AmazonS3`、`GoogleCloudStorage`、`AzureBlobStore`、`InMemory` 等实现
- 所有文件访问（读/写）都通过这个统一抽象
- 本地文件在读写路径上被**分别处理**：
  - **读**：`LocalObjectReader` 打开 `std::fs::File`，`spawn_blocking` + `read_exact_at`
  - **写**：`ObjectStore::create(path)` 识别 `scheme == "file"` 返回 `LocalWriter`，其他 scheme 返回 `ObjectWriter`

### 1.2 Writer Trait

```rust
// rust/lance-io/src/traits.rs
#[async_trait]
pub trait Writer: AsyncWrite + Unpin + Send {
    async fn tell(&mut self) -> Result<usize>;
    async fn shutdown(&mut self) -> Result<WriteResult>;
}
```

**关键设计决策**：高层代码（`FileWriter`、manifest writer 等）只依赖 `Box<dyn Writer>`，完全解耦文件格式逻辑与存储后端（本地盘/S3/内存等）。

---

## 2. Lance 的写入路径

### 2.1 高层 API → Fragment

```
InsertBuilder::execute / execute_stream
  → write_uncommitted_stream_impl
    → write_fragments_internal (rust/lance/src/dataset/write.rs:913)
      → do_write_fragments (rust/lance/src/dataset/write.rs:485)
```

`do_write_fragments` 流式处理输入，按 `max_rows_per_file` 分块，反复调用 `WriterGenerator::new_writer`：

```rust
// rust/lance/src/dataset/write.rs:1197
let writer = object_store.create(&full_path).await?;
let file_writer = current_writer::FileWriter::try_new(
    writer,
    schema.clone(),
    FileWriterOptions {
        format_version: Some(storage_version),
        ..Default::default()
    },
)?;
```

- `object_store.create` 返回 `Box<dyn Writer>`
- `FileWriter`（`rust/lance-file/src/writer.rs`）持有该 Writer 并负责 Lance 文件格式

### 2.2 FileWriter 内部流水线

```rust
// rust/lance-file/src/writer.rs
pub struct FileWriter {
    writer: Box<dyn Writer>,
    schema: Option<LanceSchema>,
    column_writers: Vec<Box<dyn FieldEncoder>>,
    column_metadata: Vec<pbfile::ColumnMetadata>,
    // ...
}
```

每个 batch 的写入流程（`write_batch`，line 527）：

1. **编码** — `encode_batch` 将每个 array 推入 `FieldEncoder::maybe_encode(...)`
   - 每次返回 `Vec<EncodeTask>`，其中 `EncodeTask = BoxFuture<'static, Result<EncodedPage>>`
   - 所有列的任务收集到 `FuturesOrdered`，**编码并发执行**
2. **先写外部缓冲区**（如大型 out-of-line binary 数据）
3. **按完成顺序写 page**（`write_pages`，line 372）：

```rust
async fn write_pages(&mut self, mut encoding_tasks: FuturesOrdered<EncodeTask>) -> Result<()> {
    while let Some(encoding_task) = encoding_tasks.next().await {
        let encoded_page = encoding_task?;
        self.write_page(encoded_page).await?;
    }
    self.writer.flush().await?;
}
```

该函数中有一条关键注释：

> *"There is no parallelism needed here because 'writing' is really just submitting the buffer to the underlying write scheduler... we wouldn't want buffers getting mixed up across pages."*

4. **Finish**（`finish`，line 768）：
   - flush 剩余列数据
   - 写列元数据（可能从 spill file 回读）
   - 写全局缓冲区、schema descriptor
   - 写列元数据 offset table、global-buffer offset table
   - 写 footer + magic
   - `Writer::shutdown(self.writer.as_mut())`

---

## 3. Lance 的写入优化

### 3.1 并行列编码

- `FieldEncoder::maybe_encode` 返回 `Vec<EncodeTask>`（`BoxFuture`）
- 所有列的任务被收集到 `FuturesOrdered` 并发执行
- CPU 密集型编码（压缩、bit-packing 等）因此并行化
- **但 I/O 是串行的**，保证 page 顺序和文件布局确定性

### 3.2 列级写缓冲

```rust
// rust/lance-file/src/writer.rs:62
pub struct FileWriterOptions {
    /// How many bytes to use for buffering column data
    pub data_cache_bytes: Option<u64>,
    pub max_page_bytes: Option<u64>,
    pub keep_original_array: Option<bool>,
    pub encoding_strategy: Option<Arc<dyn FieldEncodingStrategy>>,
    pub format_version: Option<LanceFileVersion>,
}
```

默认 **每列 8 MiB 缓冲**。列写入器缓冲数据直到足以生成一个完整 page，避免小 page 和高 IOPS。

### 3.3 Page 元数据 Spill File

高并发场景（如 IVF shuffle，数千个分区 writer）下，`FileWriter::with_page_metadata_spill` 创建侧车临时文件：

```rust
struct PageMetadataSpill {
    writer: Box<dyn Writer>,
    column_buffers: Vec<Vec<u8>>,
    column_chunks: Vec<Vec<(u64, u32)>>,
    per_column_limit: usize,
}
```

Serialized page metadata 被 flush 到磁盘而非保留在内存，显著降低 writer 的 RSS。

### 3.4 云端上传缓冲 / 并行上传

```rust
// rust/lance-io/src/object_writer.rs
pub struct ObjectWriter {
    state: UploadState,
    buffer: Vec<u8>,
    use_constant_size_upload_parts: bool,
}

enum UploadState {
    Started(Arc<dyn ObjectStore>),
    CreatingUpload(BoxFuture<'static, OSResult<Box<dyn MultipartUpload>>>),
    InProgress { part_idx: u16, upload: Box<dyn MultipartUpload>, futures: JoinSet<...> },
    // ...
}
```

- 初始 part 大小：**5 MiB** (`INITIAL_UPLOAD_STEP`)
- 每 100 个 part 后大小增长（最大 5 GiB），支持最大约 **2.5 TB** 文件
- 最多 **`LANCE_UPLOAD_CONCURRENCY`（默认 10）个 part 同时上传**
- 对 `Connection reset by peer` / `RequestTimeout` 做 2–8 秒抖动的重试，最多 `LANCE_CONN_RESET_RETRIES`（默认 20）次
- 小文件（< 5 MiB）使用单次 `put` 而非 multipart

### 3.5 本地写入路径

```rust
// rust/lance-io/src/object_writer.rs:516
struct WritingState {
    writer: tokio::io::BufWriter<tokio::fs::File>,
    cursor: usize,
    temp_path: tempfile::TempPath,
    io_tracker: Arc<IOTracker>,
}
```

- 写入通过 `NamedTempFile` + `tokio::io::BufWriter<tokio::fs::File>`
- `shutdown` 时将 temp file `persist` 到最终路径，本地文件系统获得原子性
- writer 被 drop 且未 shutdown 时，temp file 自动删除

### 3.6 流式写入

`FileWriter` 和 `ObjectWriter` 都不会将整个文件物化到内存。page 编码完成后立即写入；multipart part 在 buffer 满后立即上传。

---

## 4. I/O Scheduler：仅用于读取

```rust
// rust/lance-io/src/scheduler.rs
pub struct ScanScheduler {
    object_store: Arc<ObjectStore>,
    io_queue: IoQueueType,   // Standard or Lite
    stats: Arc<StatsCollector>,
}
```

**关键发现**：`ScanScheduler` 是**只读**的，写入完全绕过它。

Scheduler 的功能：
- 限制在途 IOPS 到 `object_store.io_parallelism()`（本地默认 8，云端默认 64）
- 基于字节数的背压预算（`io_buffer_size_bytes`）
- 支持 `file+uring` 的 "lite" 模式
- `FileScheduler::submit_request` 合并相邻 range，按 `DEFAULT_MAX_IOP_SIZE`（默认 16 MiB，可配 `LANCE_MAX_IOP_SIZE`）拆分大读取

写入路径：

```
FileWriter → LocalWriter / ObjectWriter → tokio::fs 或 object_store::put / put_multipart
```

**完全没有经过 I/O Scheduler。**

---

## 5. Manifest / 事务文件处理

### 5.1 Manifest 命名方案

```rust
// rust/lance-table/src/io/commit.rs:82
enum ManifestNamingScheme {
    V1,  // _versions/{version}.manifest
    V2,  // _versions/{u64::MAX - version:020}.manifest
}
```

V2 方案利用字典序让最新版本在 listing 中排在最前，在有序对象存储上实现 O(1) 最新版本查找。

### 5.2 原子提交策略

`CommitHandler` 是可插拔的：

| Handler | 适用场景 | 机制 |
|---|---|---|
| `ConditionalPutCommitHandler` | Unix 本地、S3、GCS、Azure、内存等 | 先写入内存，然后 `object_store.inner.put_opts(..., PutMode::Create)`；若已存在则映射为 `CommitConflict` |
| `RenameCommitHandler` | Windows 本地 | 先写到 staging path，再 `rename_if_not_exists(staging, final)` |
| `UnsafeCommitHandler` | 显式启用 | 直接覆盖；仅记录 warning |
| `ExternalManifestCommitHandler` / `CommitLock` | `s3+ddb`、外部 catalog | 获取 lease/lock，写入，释放 |

默认使用 `ConditionalPutCommitHandler`，利用对象存储后端的原生 create-if-not-exists 语义，而非全局锁服务。

### 5.3 Version Hint

对于 listing 非字典序的存储（本地 FS、S3 Express），Lance 写入 `_versions/latest_version_hint.json`（内容为 `{"version":N}`）作为最佳努力的提示。读取时结合 hint + 并行 `HEAD` 探测来定位最新 manifest。正确性不依赖 hint。

---

## 6. Lance 设计原则 vs Vego 当前架构

| 设计原则 | Lance 实现 | 对 Vego 的启示 |
|---------|-----------|---------------|
| **统一 Writer trait** | `Box<dyn Writer>` over `AsyncWrite`，解耦格式与存储 | ✅ 与我们 Phase A 计划一致：定义 `vfs.FileIO` 统一抽象 |
| **编码并行、写入串行** | `FuturesOrdered<EncodeTask>` + 串行 `write_page` | ✅ 与我们 Phase B 计划一致：并行列编码，顺序写 page 保持文件布局 |
| **I/O Scheduler 只读** | `ScanScheduler` 仅处理读取，写入绕过 | ✅ 直接支持我们"vfs 写入优化当前不必要"的判断 |
| **不可变数据文件 + 版本化 manifest** | 写入 append-only fragment，更新/删除创建 sidecar，manifest 每事务重写 | 与 Vego 的 Phase C（append-only + Manifest）方向一致 |
| **存储原生原子性** | conditional put / rename_if_not_exists，而非全局锁 | Manifest 系统应优先使用 `temp + rename`（本地）或类似语义 |
| **内存意识** | 每列 8MB 缓冲、metadata spill file | Vego 的"累积缓冲区"（P0-10）与此等价 |
| **流式写入** | 不物化整个文件到内存 | 与 Vego Bottleneck 3（ForEach 流式）方向一致 |

---

## 7. 关键结论

### 7.1 最重要的发现

**Lance 的 I/O Scheduler 完全不做写入调度。** 写入路径是：

```
上层 FileWriter
  → 统一 Writer trait（LocalWriter / ObjectWriter）
    → tokio::io::BufWriter<tokio::fs::File>（本地）
    → object_store::put / multipart（云端）
```

读取路径才有 Scheduler、优先级队列、range 合并等复杂机制。

### 7.2 对 Vego 架构判断的验证

我们在 `docs/VFS_WRITE_ARCHITECTURE_EVAL.md` 中的判断与 Lance 实践**高度一致**：

| 我们的判断 | Lance 实践 | 一致性 |
|-----------|-----------|--------|
| "所有 I/O 走 vfs" 应走同步抽象，而非异步调度器 | ✅ Lance 写入走 `Writer` trait，读取才走 `ScanScheduler` | 完全一致 |
| "vfs writer 异步优化" 当前不必要 | ✅ Lance 写入完全绕过 I/O Scheduler | 完全一致 |
| 真正瓶颈在 storage 层的 O(n) 重写和串行编码 | ✅ Lance 用并行列编码 + 不可变 fragment + manifest 重写解决 | 方向一致 |
| 建议 Phase A 统一文件抽象、Phase B 并行编码 | ✅ Lance 架构正是：统一 `Writer` trait + 编码层 `FuturesOrdered` | 方向一致 |

### 7.3 建议的 Vego 演进路线（基于 Lance 验证）

1. **Phase A：统一文件抽象** — 在 `vfs/` 定义 `FileIO` / `Writer` trait，让 `storage/column/writer.go` 和 `vego/storage.go` 的所有文件操作经过 vfs
2. **Phase B：并行列编码** — 在 `storage/column/writer.go` 的 `WriteRecordBatch` 中引入 goroutine-per-column 编码，顺序写 page
3. **Phase C：append-only + Manifest** — 解决 `flush()` O(n) 重写，参考 Lance 的 fragment + manifest 模式
4. **Phase D（远期）**：当需要云存储/多文件并发写入时，再考虑在 vfs 写入层引入 Lance 式的 upload buffering / multipart / write-back 机制

Lance 已经替我们验证了这个路线是正确的。
