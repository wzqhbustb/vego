# Vego 路线图

## 概述

| 阶段 | 目标 | 时间线 | 关键交付物 |
|------|------|--------|-----------|
| Phase 0 ✅ | 统一 API 与基础 | 1-2 周 | 用户友好的 API、基础集成测试 |
| Phase 1 ✅ | 存储引擎加固 | 4-6 周 | 行索引、块缓存、Deletion Vector（框架）、Get() O(1) |
| **Phase 2** | MVP（最小可行产品） | 10-12 周 | CRUD 操作、Agent Memory、架构重构、Delete/Update 加固、I/O 调度器、Blob 描述符 |
| Phase 3 | Beta 版 | 8-10 周 | CMO、Zone Map、IVF-PQ 索引、Blob 分层存储、生产就绪 |
| Phase 4 | V1.0 性能版 | 10-12 周 | MiniBlock、预取、IVF-HNSW-PQ、Late Materialization |
| Phase 5 | V1.5 云原生版 | 12-16 周 | 对象存储、多模态优化、云存储支持 |
| Phase 6 | V2.0 企业版 | 20-24 周 | WAL、MVCC（简化版）、标量索引、时间点恢复 |

**状态图例**：✅ 已完成 · 🔄 进行中 · ⚠️ 部分/需跟进 · ❌ 未开始 · ~ 推迟

**当前重点**：Phase 2 作用域扩大 — 核心 MVP 已交付（Agent Memory ✅、架构重构 ✅ 于 v0.1.5、Delete/Update 加固 ✅）。剩余工作：I/O 调度器（关键）、Tombstone 带宽限期、Blob 描述符 + 内联层、Phase 1 存储收尾任务。Pack/Dedicated Blob 层级和 Pack GC 移至 Phase 3。

> **说明**：Phase 0（统一 API）和 Phase 1（存储引擎加固）已完成。架构重构（Phase 2）已合并到 `main` 分支，标签 `v0.1.5`。Phase 2 时间线从 6-8 周扩展到 10-12 周，以吸收 I/O 调度器、Tombstone 和最小 Blob 基础；完整 Blob 分层存储移至 Phase 3。若干非关键任务（备份/恢复、高级可观测性、结构化错误）已推迟至 Phase 6。详见 Phase 6 "第六层" 了解推迟的任务。

---

## Phase 0: 统一 API 与基础 ✅ 已完成

### 目标
创建统一、直观的 API，将 HNSW 向量搜索与列式存储相结合，让用户无需深入了解底层组件即可使用 Vego。

### 愿景
```go
// Vego API 应该如此简单：
db, _ := vego.Open("./mydb", vego.WithDimension(768))
coll, _ := db.Collection("documents")

// 插入自动生成的嵌入向量
coll.Insert(&vego.Document{
    ID:       vego.DocumentID(),
    Vector:   embedding,  // 你的 768 维向量
    Metadata: map[string]any{"title": "Hello", "author": "Alice"},
})

// 带元数据过滤的向量搜索
results, _ := coll.Search(queryVector, 10,
    vego.WithFilter(vego.MetadataFilter{
        Field: "author", Operator: "eq", Value: "Alice",
    }),
)
```

### 关键任务

#### 1. 统一的 DB/Collection API ✅
- [x] `vego.Open()` - 打开或创建数据库
- [x] `db.Collection()` - 获取或创建集合
- [x] `db.DropCollection()` - 删除集合
- [x] `db.Collections()` - 列出所有集合
- [x] `db.Close()` - 优雅关闭

#### 2. 以文档为中心的 Collection API ✅
- [x] `coll.Insert(doc)` - 插入单个文档
- [x] `coll.InsertBatch(docs)` - 批量插入
- [x] `coll.Get(id)` - 根据 ID 检索
- [x] `coll.Delete(id)` - 删除文档
- [x] `coll.Update(doc)` - 更新文档
- [x] `coll.Upsert(doc)` - 插入或更新

#### 3. 向量搜索 API ✅
- [x] `coll.Search(query, k, opts...)` - 向量相似度搜索
- [x] `coll.SearchWithFilter(query, k, filter)` - 带元数据过滤的搜索
- [x] `coll.SearchBatch(queries, k, opts...)` - 批量搜索

#### 4. 配置 API ✅
- [x] `vego.WithDimension(d)` - 设置向量维度
- [x] `vego.WithAdaptive(bool)` - 启用自适应调优
- [x] `vego.WithExpectedSize(n)` - 预期数据集大小
- [x] `vego.WithDistanceFunc(fn)` - 距离度量（L2/余弦/内积）
- [x] `vego.WithM(m)` - HNSW M 参数
- [x] `vego.WithEfConstruction(ef)` - HNSW 构建质量

#### 5. 持久化 API 🔄
- [x] `coll.Save()` - 将集合持久化到磁盘
- [x] `coll.Close()` - 关闭时自动保存
- [x] `coll.Load()` - 从磁盘重新加载（初始化时验证）
- [~] `db.Backup(path)` - 完整数据库备份（推迟至 Phase 6）
- [~] `db.Restore(path)` - 从备份恢复（推迟至 Phase 6）

#### 6. 性能与可观测性 📊
- [x] `coll.Stats()` - 集合统计信息
- [~] `db.Stats()` - 数据库级统计（推迟至 Phase 6）
- [~] 查询延迟指标（推迟至 Phase 6）
- [~] 索引构建进度回调（推迟至 Phase 6）

#### 7. 错误处理与可靠性 🔧
- [~] 结构化错误类型（推迟至 Phase 6）
- [~] 批量操作中的部分失败处理（推迟至 Phase 6）
- [~] 瞬态故障自动重试（推迟至 Phase 6）
- [x] 加载时损坏检测（基础验证已存在）

### 完成标准
- [x] 用户无需直接接触 `index` 或 `storage` 包即可执行所有 CRUD 操作
- [x] 示例展示真实用例（RAG、语义搜索、推荐系统、批量插入、持久化）
- [x] API 文档及使用模式
- [~] vego 包单元测试覆盖率 > 70%（目标移至 Phase 1）
- [x] 完整工作流的集成测试（e2e_test.go 覆盖核心工作流）

### API 设计原则

1. **简洁优先**：常用操作应该是一行代码
2. **合理默认**：自适应配置开箱即用
3. **渐进式披露**：初学者简单，专家强大
4. **一致性**：DB、Collection 和 Query API 采用相似模式
5. **快速失败**：在 API 边界验证，错误信息清晰

---

## Phase 1: 存储引擎加固 ✅ 已完成

### 目标
巩固存储基础，建立基准测试，确保后续开发无需返工。

### 关键任务

#### 第 1-2 周：文件格式基础 ✅
- [x] **Header/Footer 版本字段**（`storage/format/header.go:18`，`footer.go:17`）
  - Header 中的 `Version uint16` + Footer 中冗余 `Version` 用于校验
  - Magic number + 基于 flag 的特征检测（`FlagVersioned`）
- [x] **`VersionPolicy` 结构化版本管理**（`storage/format/version.go`）
  - `V1_0`、`V1_1`、`V1_2` 带 FeatureFlags 位图
  - `Encoded()` / `String()` / `HasFeature()` / `CanRead()`
- [x] **`VersionChecker` 运行时兼容性检查**（`storage/format/version.go:276`）
  - 主版本必须匹配；次版本向后兼容
  - `CheckReadCompatibility()` 返回结构化 `VersionError` 并附迁移建议
- [x] **Legacy 版本映射**（`storage/format/version.go:192`）
  - `NormalizeVersion()`：将旧版 `version=1` 映射为 `V1.0`（0x0100）
  - `version_legacy_test.go`：验证所有版本对的向后兼容性
- [x] **格式版本元数据**
  - Footer 存储显式版本字符串（`vego.format.version`）
  - 为前向/后向兼容打基础（完整策略见 ADR 4，Phase 2 期间细化）

#### 第 2-4 周：内存索引与缓存（关键路径）✅
- [x] **RowIndex 内存映射**（`vego/storage.go`，`catalog.IDMapping`）
  - `idToHash`（docID → internal ID）+ 反向映射，实现 O(1) 查找
  - 启动时从 `vectors.lance` footer RowIndex metadata 重建
  - 纯内存（<100万文档），重建成本远低于持久化复杂度
- [x] **`Get()` O(1) 路径**（`vego/storage.go:245`）
  - `bufferIndex` 检查（热路径：最新写入）
  - `tryReadByRowIndex()` → 直接定位 page/offset 读取持久化数据
  - 仅对无 RowIndex 的 legacy 文件回退到全表扫描
- [x] **BlockCache 实现**（`storage/format/blockcache.go`）
  - 64KB 块、shard 化 LRU（默认 64 shards）、线程安全
  - `Get`/`Put`/`Invalidate`/`Stats` API
  - 被列读取器、footer 读取器、RowIndex 加载器共用
- [ ] ~~**DocumentCache**~~（未实现）
  - Phase 1 原计划独立的文档级 LRU 缓存（默认 1万文档）
  - **决策**：BlockCache 已提供足够缓存能力；DocumentCache 无限期推迟
  - 搜索结果目前从 BlockCache 解码后的 page 中读取
- [x] **GetBatch 优化**（`vego/storage.go`）
  - 批量加载以减少搜索结果读取的 I/O 往返

#### 第 4-6 周：存储引擎加固 🔄
- [x] **Deletion Vector 内存位图**（`index/deletion_vector.go`）
  - 基于 `RoaringBitmap` 的行级删除标记
  - 线程安全的 `MarkDeleted()` / `IsDeleted()` / `Count()` / `Union()`
- [x] **DV 持久化**（`index/deletion_vector_persist.go`）
  - 序列化为 `.del` 侧车文件（varint 编码的 RoaringBitmap）
  - 加载时反序列化并与内存 DV 合并
- [x] **`SearchWithDV()` API**（`index/hnsw.go:161`）
  - 贪婪搜索返回候选集；通过 `isDeleted` 回调后过滤
  - 调用方控制 over-fetch（高删除率时用 `k*2`）
- [x] **端到端集成测试**
  - `index/search_with_dv_test.go`：并发插入/删除下 DV 正确性
  - `vego/e2e_test.go`：完整 CRUD → Search 管线
  - `collection_compact_*_test.go`：9 种压缩策略的正确性
- [x] **性能基线建立**（`bench_results/baseline.txt`）
  - 写入吞吐、读取延迟、搜索延迟、构建时间
  - 记录 4x 并发退化（9.2ms vs 2.3ms）
- [ ] **写入器异步优化**（移至 Phase 2）
  - 多列并行编码 + 保证顺序写入
  - 当前 ~330 MB/s 在目标场景下够用

#### 第 5-6 周：存储基础（非阻塞）
- [x] **错误分类系统**（`core/errors` 包）
  - 带上下文的结构化错误（`core.IO()`、`core.Validation()`）
  - 支持 `Unwrap()` 链式 `errors.Is()` 检查
  - 类堆栈追踪的上下文累积
- [x] **NullBitmap 统一设计**（`storage/encoding/nullbitmap.go`）
  - 跨所有编码器共享的 null bitmap 抽象
  - `Encode()` / `Decode()` / `IsNull()` / `SetNull()` API
- [x] **编码器 Null 支持**（全部编码器）
  - RLE（`rle.go` / `rle_decoder.go`）
  - BitPacking（`bitpacking.go` / `bitpacking_decoder.go`）
  - BSS（`bss.go` / `bss_decoder.go`）
  - Dictionary（`dictionary.go` / `dictionary_decoder.go`）
  - Zstd（`zstd.go` / `zstd_decoder.go`）
- [ ] **Delta 编码**（移至 Phase 2）
  - 时间戳、自增 ID 等单调递增数据的变长整数 Delta
  - `factory.go` 中预留 `EnableDeltaEncoding` 开关
- [ ] **页面级 Min/Max 统计**（移至 Phase 2）
  - `format.Page` 中的 `MinValue`/`MaxValue` 字段
  - 为 Phase 3 Zone Map 页面跳过打基础

#### Deletion Vector 框架（跨周任务）✅
- **设计原理**：参考 Lance 设计，逻辑删除替代物理删除，支持增量更新而无需全量重写
- **实现细节**：
  - [x] 内存：`RoaringBitmap` + `sync.RWMutex`
  - [x] 持久化：`.del` 侧车文件，varint 编码
  - [x] HNSW 集成：`SearchWithDV()` 后过滤
  - [x] 压缩：`Compact()` 重建图时排除 DV 标记节点
- **收益**：
  - ✅ 快速软删除（O(1) 位图标记）
  - ✅ 后台压缩均摊清理成本
  - ✅ 为 MVCC 打基础（DV 版本化实现快照隔离）
- **权衡**：
  - ❌ 内存略高（位图开销，约每行 1 bit）
  - ❌ 搜索需 DV 过滤（开销极小：位图检查为 O(1)）
- **API**：
  ```go
  type DeletionVector interface {
      Contains(rowID uint32) bool
      Set(rowID uint32)
      Count() int
      Serialize() ([]byte, error)
      Deserialize([]byte) error
  }
  ```

### 详细任务清单

| # | 任务 | 状态 | 关键文件 |
|---|------|------|----------|
| 1 | Header/Footer 版本字段 | ✅ | `storage/format/header.go`、`footer.go` |
| 2 | `VersionPolicy` + `VersionChecker` | ✅ | `storage/format/version.go` |
| 3 | Legacy 版本映射 | ✅ | `storage/format/version_legacy_test.go` |
| 4 | RowIndex 内存映射 | ✅ | `vego/storage.go`、`catalog/` |
| 5 | `Get()` O(1) via RowIndex | ✅ | `vego/storage.go:245` |
| 6 | BlockCache（64KB、LRU、shard 化） | ✅ | `storage/format/blockcache.go` |
| 7 | Deletion Vector 内存位图 | ✅ | `index/deletion_vector.go` |
| 9 | DV 持久化（.del 侧车文件） | ✅ | `index/deletion_vector_persist.go` |
| 10 | `SearchWithDV()` API | ✅ | `index/hnsw.go:161` |
| 11 | 端到端集成测试 | ✅ | `index/search_with_dv_test.go`、`vego/e2e_test.go` |
| 12 | 性能基线 | ✅ | `bench_results/baseline.txt` |
| 13 | 错误分类系统 | ✅ | `core/errors` |
| 14 | NullBitmap + 全部编码器 null 支持 | ✅ | `storage/encoding/nullbitmap.go` |
| 15 | DocumentCache（独立） | ❌ 未实现 | —（BlockCache 足够） |
| 17 | 写入器异步优化 | ❌ 推迟 | — |
| 18 | Delta 编码 | ❌ 推迟 | `factory.go`（预留） |
| 19 | 页面级 Min/Max 统计 | ❌ 推迟 | `format.Page`（预留） |

### 完成标准
- [x] 文件版本管理 ✅：能够检测和处理格式版本不匹配
- [x] Get() 操作平均 O(1) ✅（通过行索引 + 缓存）
- [ ] Search(k=10) 处理 10万文档在 < 100ms 内完成（对比当前 10+ 秒）🔄
- [x] 所有编码器通过往返测试 ✅（编码 → 解码 → 数据完整性）
- [x] `go test -race` 无竞态条件 ✅
- [x] 基准测试目标：写入/读取/Search 基线已建立 ✅
- [x] 代码测试覆盖率 > 60% ✅（vego 包实际 79.4%，2026-05）
- [x] Deletion Vector 框架 ✅：能够标记行为已删除并在搜索时过滤
- [x] Compact 实现 ✅：能够重建索引并清理已删除数据

### 依赖关系
- 第 1-2 周（文件版本）必须在任何磁盘格式变更前完成
- 第 2-4 周（行索引 + 缓存）可在文件版本稳定后开始
- 第 4-6 周（块缓存）依赖行索引进行缓存键管理
- 第 5-6 周任务为非阻塞，可并行进行

---

## Phase 2: MVP（最小可行产品）⭐ 当前优先

**时间线**：10-12 周（从 6-8 周修订，以吸收 I/O 调度器 + Tombstone + Blob 基础 + Phase 1 遗留任务）

### 目标
使系统能够处理真实世界的数据，具备基础 CRUD 和查询能力。参考 Lance 设计：向量存储（Page 内）与多模态存储（外部）分离，支持大对象懒加载。

Phase 2 交付 **最小** blob 基础（仅描述符格式 + 内联层）。Pack 文件、Dedicated 文件和 Pack GC 划入 Phase 3，以保持 Phase 2 可交付。

### 关键任务

#### HNSW 索引与 Deletion Vector 集成 ✅
- **Deletion Vector 集成 ✅**：使用 DV 替代物理删除
  - HNSW 节点通过 DV 标记删除，不从图中移除
  - 搜索结果通过 DV 过滤（每次结果 O(1) 检查）
  - 后台压缩定期回收空间
- **墓碑机制 ❌**：软删除带宽限期与恢复
  - **当前状态**：DV bitmap 仅提供即时软删除，无带宽限期、无恢复窗口
  - **未实现**：无代码存在（`index/tombstone.go`、`index/tombstone_persist.go` 不存在）
- **孤儿预防 ✅**：Update 使用 DV 标记旧版本，插入新版本
- **索引压缩 ✅**：后台重建移除 DV 标记节点并优化图结构
  - 阻塞式压缩已实现（自动触发 + 手动触发）
  - 轻量锁/后台双写优化待 Phase 4+
  - **实现策略**：详见 [COMPACTION.md](COMPACTION.md) 了解 9 种压缩策略的详细设计
  - **Phase 2（当前）**：阻塞式压缩（简单、可靠）
    - Compact 期间阻塞所有读写操作
    - 适合维护窗口和批处理场景
  - **未来优化**：轻量锁（Phase 4+）或后台双写（Phase 5+）
    - 实现在线服务的零停机压缩
    - 工程复杂度较高（4-5 倍工作量）

#### I/O 调度器重构（关键）🔄
- **问题**：当前 4x 并发 = 4x 性能退化
- **目标**：4x 并发退化 < 20%（对比当前 300%+）
- **状态**：骨架已实现（`vfs/scheduler.go` + `vfs/async.go` + `vfs/executor.go` + `storage/column/reader.go` `NewReaderWithAsyncIO`）。**生产路径未接入** — `vego/storage.go` 仍使用同步 `column.NewReader()` / `column.NewReaderWithCache()`；`NewReaderWithAsyncIO()` 仅用于测试。

**设计原理**：当前同步 I/O 模型在每个 goroutine 内串行化读取，导致并发下过度上下文切换和 OS 页缓存抖动。用户空间 I/O 调度器（参考 Lance 设计）集中 I/O 决策以最大化吞吐。

**架构**：两级调度器（全局准入 + 每文件分发）。详见下方子任务。

##### 子任务 1：核心调度器接口与类型 🔄
- [x] `IORequest` / `IORange` 结构体定义（`vfs/request.go`）
- [x] `Scheduler` 结构体含 `Submit()` / `SubmitBatch()`（`vfs/scheduler.go`）
- [x] `Executor` 工作池（`vfs/executor.go`）
- [x] `AsyncIO` 外观（`vfs/async.go`）
- [ ] `GetScheduler()` 单例 + `NewScheduler(cfg)` 工厂
- [ ] 完整 `Config` 结构体：`MaxInFlight`、`EnableCoalescing`、`CoalesceWindow`、`WorkersPerFile`、`FileIdleTimeout`

##### 子任务 2：请求合并引擎 ❌
- [ ] `Coalescer` 结构体：在时间窗口内将相邻/重叠的 `IORange` 请求分组
- [ ] 算法：按 `(FileID, Offset)` 排序，在 gap < threshold 时合并连续区间
- [ ] `CoalesceWindow`：可配置批处理窗口（默认 100µs）
- [ ] 单元测试：相邻合并、gap 阈值、多文件隔离、窗口超时

##### 子任务 3：带行号排序的优先级队列 🔄
- [x] `priorityQueue` 基于 `container/heap`（`vfs/scheduler.go:267`）
- [x] 基础优先级排序（`Priority` 字段）
- [ ] 优先级分层枚举：`PriorityHigh` / `PriorityNormal` / `PriorityLow`
- [ ] 同层内行号决胜：有利于顺序扫描
- [ ] 饥饿预防：基于年龄的优先级提升

##### 子任务 4：背压机制 🔄
- [x] 队列容量阻塞：`Submit()` 在 `queue.Len() >= maxQueueSize` 时阻塞
- [ ] `MaxInFlight` 字节限制（默认 64MB）— 当前仅有队列槽位限制
- [ ] `ErrBackpressure` / `ErrOverloaded` 非阻塞模式
- [ ] `InFlight() int64` 监控 API
- [ ] 自适应阈值自动调优

##### 子任务 5：每文件调度 ❌
- [ ] `FileScheduler` 结构体：每个活跃文件一个
- [ ] 每文件有界工作池（默认 2–4 个 worker）
- [ ] `SubmitToFile(fileID, req)` 从全局调度器路由
- [ ] 每文件头行阻塞隔离
- [ ] 空闲超时清理（默认 30s）

##### 子任务 6：存储层集成 🔄
- [x] `ColumnReader` 支持 `NewReaderWithAsyncIO()`（`storage/column/reader.go`）
- [x] `readPage()` / `readPagesBatch()` 在 `useAsync=true` 时走异步路径
- [ ] **生产接入**：`vego/storage.go` 必须改用 AsyncIO 创建 reader，而非 `NewReader()` / `NewReaderWithCache()`
- [ ] 多页读取批量 API
- [ ] 调度器为 nil 时的向后兼容回退

##### 子任务 7：指标与可观测性 🔄
- [x] 基础计数器：`Submitted`、`Completed`、`Errors`（`SchedulerStats` 部分）
- [ ] `SchedulerStats` 完整结构体：`CoalescedRequests`、`AvgQueueDepth`、`AvgWaitLatency`、`InFlightBytes`、`BackpressureEvents`
- [ ] Prometheus 格式导出（Phase 3 监控基础）

##### 完成标准（I/O 调度器）
- [ ] 生产读路径接入 AsyncIO（`vego/storage.go`）
- [ ] 4x 并发搜索延迟退化 < 20%（对比基线 1x 并发）
- [ ] 16x 并发延迟退化 < 50%
- [ ] 合并使顺序扫描下 I/O 系统调用减少 > 40%
- [ ] 背压在 1000+ 并发查询下防止 OOM
- [ ] 所有现有存储测试在调度器启用时通过

#### Phase 1 遗留任务（存储引擎收尾）

这些任务不影响 MVP 核心功能，但提升存储引擎完整度。从 Phase 1 移至 Phase 2。

##### 遗留任务 1：逐页 Min/Max 统计 ❌
- [ ] 在 `format.Page` 结构体中增加 `MinValue any` + `MaxValue any` 字段
- [ ] `PageWriter` 中按列类型比较：
  - 数值列：写入 page 时通过 `Compare()` 循环跟踪 min/max
  - 字符串列：跟踪字典序 min/max
  - 向量列：跳过（高维向量的 min/max 无意义）
- [ ] `PageWriter` 的 `UpdateStats(val any)` 方法 — 编码期间每值调用
- [ ] `Page.Stats() PageStats` — 返回 `{Min, Max, NullCount, RowCount}`
- [ ] min/max 序列化为 footer 元数据（不内联到 page 体，避免破坏 page 布局）
- [ ] `PageSkipper` 接口（Phase 3 Zone Map 使用）：
  ```go
  type PageSkipper interface {
      CanSkip(page PageStats, predicate Predicate) bool
  }
  ```
- [ ] 单元测试：验证每种编码器的 min/max 正确性、null 处理

##### 遗留任务 2：Delta 编码实现 ❌
- [ ] `storage/encoding/delta.go` 中的 `DeltaEncoder`：
  - 输入：已排序/近似排序的 int64/uint64 序列
  - 算法：首值存绝对值，后续值存 `delta = val[i] - val[i-1]`，使用 varint 编码
  - `Encode(values []int64) ([]byte, error)`
- [ ] `storage/encoding/delta_decoder.go` 中的 `DeltaDecoder`：
  - `Decode(data []byte, out []int64) error` — 重建原始值
  - 支持部分读取：通过累加 delta 定位到位置 N
- [ ] 集成到 `storage/encoding/factory.go`：
  - `EnableDeltaEncoding` 开关（factory.go 中已预留）
  - 自动检测资格：列类型为 `int64/uint64/timestamp` 且升序排列
  - 数据未排序时回退到 plain/ZSTD
- [ ] 最佳压缩比目标：时间戳 > 80%，自增 ID > 60%
- [ ] 单元测试：往返、边界情况（全同值、负 delta、溢出）、部分读取

##### 遗留任务 3：Writer 异步优化 ❌
- [ ] 当前状态：`ColumnWriter` / `PageWriter` 同步编码，一次一列
- [ ] 目标：多列并行编码，然后顺序写入 page（确定性文件布局）
- [ ] 实现计划：
  - [ ] `AsyncColumnWriter`：每列一个 goroutine 执行编码阶段
  - [ ] `sync.WaitGroup` 收集所有已编码 page 缓冲区
  - [ ] 顺序写入完成的 page（确保 page 顺序确定性）
  - [ ] 可配置工作池：`NumWriteWorkers`（默认 `runtime.GOMAXPROCS(0)`）
- [ ] 内存预算：限制在途编码 page 总量为 `MaxWriteBufferBytes`（默认 128MB）
- [ ] 预期吞吐：800–1200 MB/s（估计比当前 ~330 MB/s 提升 2.5–3.5 倍；实际收益取决于列数和编码组合，受 Amdahl 定律约束 — 单宽列收益极小，10+ 窄列收益最大）
- [ ] 集成：`WriterConfig` 增加 `WithAsyncWrite(bool)` 选项，默认关闭保安全
- [ ] 基准测试：比较 `BenchmarkWrite*` 前后，测量 wall-clock 时间和 CPU 利用率

#### Agent Memory 系统 ✅
- **目标**：为 AI Agent 提供嵌入式向量可搜索记忆，基于 Vego 的 HNSW + 列式存储构建
- **摄入管线 ✅**：统一 `Ingest()` 入口，两种模式：
  - **ModeNormal**：消息 → LLM 事实提取 → 与已有记忆 Reconcile
  - **ModeRaw**：消息 → 内容哈希去重 → 按会话顺序存储
- **调和（Reconcile）✅**：将提取的事实与已有记忆进行向量 + 关键词搜索比对，LLM 为每条事实决策 ADD/UPDATE/DELETE/NOOP
- **混合搜索 ✅**：10 阶段管线：
  - HNSW 向量搜索 + BM25 关键词搜索 + RRF 融合
  - 信号加权（置顶、时间衰减、双通道加分）
  - 二跳关联召回
  - 间隙截断 + 分页
- **基础设施 ✅**：
  - 内存 BM25 倒排索引（英文 + CJK 分词）
  - 时间表达式归一化（中英文相对时间 → 绝对时间 → 相对显示）
  - 会话消息内容哈希去重
  - 调和用近似重复检测
  - Schema 迁移系统
- **架构**：`memory/`（L5）→ `vego/`（L4），不直接导入 `index/` 或 `storage/`

#### 架构重构 ✅
- **目标**：建立清晰的 5 层依赖架构
- **已完成步骤**：
  - Step 0：提升 `storage/arrow/` → `core/`，`storage/errors/` → `core/`
  - Step 1：提升 `storage/io/` → `vfs/`
  - Step 2：隔离 `index/`（移除非法 storage 导入）
  - Step 3：清理 `memory/` → `vego/`（移除对 `index/` 的直接依赖）
- **I/O 层修复**（Step 1 / Step 3 期间完成）：
  - [x] **`FilePool` 句柄复用**（`vfs/file_pool.go`）
    - `sync.RWMutex` + 引用计数管理 OS 文件句柄
    - 防止并发列读取时 `too many open files`
  - [x] **Partial Read 修复**（`storage/format/footer.go`、`manifest.go`、`page.go`）
    - 对定长结构将裸 `Read()` 替换为 `io.ReadFull()`
    - 防止高 I/O 压力下的损坏读取
- **结果**：`core/`（L1）→ `vfs/`（L2）→ `index/`（L3-A）+ `storage/`（L3-B）→ `vego/`（L4）→ `memory/`（L5）
- **详情**：见 [ARCHITECTURE_CN.md](ARCHITECTURE_CN.md)

#### Blob 存储基础（最小范围 — 仅内联层）❌
- **目标（Phase 2 范围）**：确立 blob 描述符格式和可用的内联层（< 64KB），使列类型、API 表面和磁盘布局在 Phase 3 叠加 Pack 和 Dedicated 层之前就已确定。
- **状态**：未实现
- **推迟到 Phase 3**：Pack 文件管理器（64KB–4MB）、Dedicated blob 文件（> 4MB）、完整 `BlobStorage` 注册表路由、`take_blobs()` 流式 API、Pack GC、大 blob 边界测试。见 Phase 3 "Blob 存储：分层实现"。

**设计原理**：向量和大型二进制对象访问模式根本不同。向量小（768 维约 3KB）、计算密集、急切加载。多模态数据大（KB 到 GB）、I/O 密集、应懒加载。根据 ADR 10，blob 存储与向量/列式存储分离。Phase 2 仅锁定描述符 + 内联路径，使面向用户的类型稳定；Phase 3 扩展层级而不破坏 API。

##### 子任务 1：Blob 描述符格式 ❌
- [ ] `storage/format/blob.go` 中的 `BlobDescriptor` 结构体：
  ```go
  type BlobDescriptor struct {
      Kind     uint8   // 0=内联（Phase 2），1=pack（Phase 3），2=dedicated（Phase 3）
      Position uint64  // 目标文件内的字节偏移
      Size     uint64  // blob 大小（字节）
      FileID   uint32  // 内联为 0（位于列 page 中）；Phase 3 为 pack/.blob 预留
  }
  ```
- [ ] 序列化大小：每个描述符 21 字节（足够紧凑，可内联存储在 page 元数据中）
- [ ] `Encode()/Decode()` 二进制 I/O 方法
- [ ] **前向兼容**：
  - `Encode()` 在 21 字节载荷前写入 1 字节格式版本前缀（`0x01`）；`Decode()` 检查并拒绝未知版本。这样 Phase 3 可扩展描述符布局而不破坏 Phase 2 解析。
  - 编码器仅写入 `Kind=0`；解码器遇到 `Kind=1/2` 时以明确的"Phase 3 必需"错误拒绝，使现有读取器在升级数据出现时大声失败

##### 子任务 2：内联 Blob 存储（< 64KB）❌
- [ ] `storage/format/blob_inline.go` 中的 `InlineBlobWriter`：
  - 将 blob 字节直接作为变长二进制数组存储在列 page 中
  - `Write(blobs [][]byte) ([]BlobDescriptor, error)` — 返回所有 `Kind=0` 的描述符
  - 最大 blob 大小：64KB（通过 `MaxInlineBlobSize` 配置）；更大 blob 返回 `ErrBlobTooLargeForPhase2`（Phase 3 将路由到 Pack/Dedicated）
- [ ] `InlineBlobReader`：
  - `Read(desc BlobDescriptor) ([]byte, error)` — 使用 Position + Size 从 page 读取 blob
  - 无额外文件 I/O：blob 已在列 reader 加载的 page 中
- [ ] 权衡：内联 blob 增大 page 大小 → 每页行数减少 → 内存更高。最适合小缩略图、短文本、图标。

##### 子任务 3：最小列 + 集合接入（仅内联）❌
- [ ] 仅内联 blob 的 `BlobColumnWriter` / `BlobColumnReader`：
  - 列类型：`core.BlobType`（新 Arrow 扩展类型）
  - 每行存储 `[]BlobDescriptor`（21 字节 × N），payload 共置于列 page 中
- [ ] `BlobHandle` 类型：
  ```go
  type BlobHandle struct {
      desc  BlobDescriptor
      store BlobStorage
  }
  func (h BlobHandle) Read() ([]byte, error)
  func (h BlobHandle) Size() int64
  // Phase 3 随 Pack/Dedicated 层增加 ReadCloser/Range
  func (h BlobHandle) Close() error  // Phase 2 无操作；Phase 3 Pack/Dedicated 释放文件句柄
  ```
- [ ] `coll.Insert()` 接受 ≤ `MaxInlineBlobSize` 的 blob 字段；更大 blob 返回 `ErrBlobTooLargeForPhase2`，直到 Phase 3 落地
- [ ] `coll.Get()` 为 blob 字段返回 `BlobHandle`（调用方调用 `.Read()` 物化字节）

##### 子任务 4：测试（内联层）❌
- [ ] 往返测试：写 → 读 → SHA256 校验，尺寸 {1B, 1KB, 16KB, 64KB-1, 正好 64KB}
- [ ] 边界测试：blob > 64KB 返回 `ErrBlobTooLargeForPhase2`（Phase 3 将替换为自动分层路由）
- [ ] 兼容测试：`Kind=1` 或 `Kind=2` 的描述符被 Phase 2 读取器拒绝并附带 Phase 3 提示
- [ ] 并发写入：多页多小内联 blob，验证无跨行损坏

#### 存储引擎增强 🔄

##### 增强 1：累积缓冲区 ❌
- **问题**：小页面（< 4KB）导致 I/O 放大和差压缩比
- **目标**：所有列类型最小 64KB 页面
- **当前状态**：存储格式层无 `WriteBuffer` 实现。唯一的缓冲是 `vego/storage.go` 中的 `DocumentStorage.writeBuffer`，它是用于批处理写入的文档级内存缓冲 — 并非下文描述的页面级累积缓冲区。
- [ ] `storage/format/write_buffer.go` 中的 `WriteBuffer`：
  - 累积值直到缓冲区达到 `MinPageSize`（默认 64KB）或 `MaxPageRows`（默认 65535）
  - `Append(val any) (flushed bool, page *Page, err error)` — 缓冲区满前返回 nil page
  - `Flush() (*Page, error)` — 强制刷写剩余缓冲数据
- [ ] 按列类型大小估算：
  - 定宽类型（int32、float64）：已知 `sizeof(val)` × 数量
  - 变宽类型（string、binary）：`len(val)` 运行总和
  - 压缩类型：保守估计压缩后大小（原始大小的 50%）
- [ ] `Flush()` 触发时机：缓冲区阈值、集合关闭、或显式 `coll.Sync()`
- [ ] 基准测试：比较前后 page 数量和写入吞吐，目标 page 数量减少 > 60%

##### 增强 2：基础监控 ⚠️
- **当前**：Stats 接口部分实现
- [ ] `vego/metrics.go` 中的 `StorageMetrics` 结构体（无外部依赖 — 保持 `vego` 包零依赖）：
  ```go
  type StorageMetrics struct {
      IOCount       atomic.Int64    // 总 I/O 操作数
      IOBytes       atomic.Int64    // 总读取字节数
      CacheHits     atomic.Int64    // BlockCache 命中
      CacheMisses   atomic.Int64    // BlockCache 未命中
      CacheHitRate  float64         // 计算：hits / (hits + misses)
      EncodeLatency LatencyHistogram // 编码时间分布（自定义，Phase 3 增加 Prometheus 适配器）
      ReadLatency   LatencyHistogram // 读取时间分布
      ActiveReaders atomic.Int32    // 当前并发读取器
  }
  // LatencyHistogram 是简单分桶直方图（[]int64 buckets + 总计数）。
  // Phase 3 导出到 Prometheus；Phase 2 仅通过 Metrics() 快照暴露。
  type LatencyHistogram struct { ... }
  ```
- [ ] 集成点：
  - `vfs.ReadAt()` 包装器：增加 `IOCount`、`IOBytes`
  - `BlockCache.Get()`：增加 `CacheHits` 或 `CacheMisses`
  - 编码器 `Encode()` 调用：通过直方图计时
- [ ] `coll.Metrics() StorageMetrics` — 应用快照
- [ ] `WithMetrics(enabled bool)` 选项：禁用时零开销（默认**关闭**，兑现零开销承诺；生产部署选择开启）
- [ ] Prometheus 导出器（Phase 3）：`GET /metrics` 端点提供 Prometheus 格式数据

##### 增强 3：Manifest 系统 ⚠️
- **目标**：文件级元数据管理，Phase 5 MVCC 基础
- **当前状态**：`storage/format/manifest.go` 已有带 MVCC 版本追踪的 `ManifestManager`（`CreateVersion`/`CommitVersion`/`GetVersion`/`GetLatestVersion`）。缺失：文件注册表级 `ManifestEntry`（per-file CRC、行范围、文件类型），设计如下。
- [ ] `storage/manifest.go` 中的 `Manifest` 结构体：
  ```go
  type Manifest struct {
      Version     uint32              // manifest 格式版本
      Files       []ManifestEntry     // 该集合的所有文件
      SequenceNum uint64              // 单调递增
      CreatedAt   time.Time
      UpdatedAt   time.Time
  }
  type ManifestEntry struct {
      FilePath   string              // 集合目录内的相对路径
      FileType   FileType            // data / index / del / blob / pack
      Size       int64               // 文件大小（字节）
      Checksum   uint32              // 文件内容 CRC32
      RowCount   uint32              // 文件内行数
      MinRowID   uint32              // 行范围（裁剪用）
      MaxRowID   uint32
      CreatedAt  time.Time
  }
  ```
- [ ] `FileType` 枚举：`{DataFile, IndexFile, DelFile, TombstoneFile, PackFile, BlobFile}`
- [ ] Manifest 持久化：JSON 供人可读（manifest.json）+ 二进制供性能（manifest.bin）
- [ ] 原子更新：写入临时文件 → rename（防止损坏）
- [ ] `Manifest.Load(path string)` — 读取并验证（每项 CRC 检查）
- [ ] `Manifest.Add(entry ManifestEntry)` / `Manifest.Remove(filePath string)`
- [ ] 集成策略 — **Writer 拥有（方案 A）**：`PageWriter` / `ColumnWriter` 内部创建或替换数据文件时调用 `Manifest.Add()`，compact/reclaim 时调用 `Manifest.Remove()`。这使 manifest 始终正确，代价是 storage→manifest 依赖。（替代方案：将文件列表返回给 `vego` 层由其管理 manifest — 依赖图更简单，但如果 writer 在文件创建和 manifest 更新之间崩溃则存在竞态。从方案 A 开始保证正确性；若依赖成为问题再 revisit。）
- [ ] 单元测试：CRUD、同时读写、损坏检测、版本兼容

##### 增强 4：列裁剪（基础）❌
- **目标**：只读取所需列，减少触及列子集的查询 I/O
- [ ] `core/schema.go` 中的 `Schema` 结构体：
  ```go
  type Schema struct {
      Columns []ColumnMeta
  }
  type ColumnMeta struct {
      Name     string
      Type     core.DataType
      Nullable bool
  }
  ```
- [ ] `ReaderOptions.WithColumns(names []string)` — 指定加载哪些列
- [ ] `ColumnReader` 集成：
  - 解析 footer → 获取列偏移
  - 完全不读取未请求列的 page
  - 仍需读取 RowIndex（行解析始终需要）
- [ ] 搜索集成：`coll.Search(query, k, WithColumns("id", "title"))` — 回读时跳过向量列
- [ ] `ForEach` / `GetAllValidDocuments`：列裁剪避免将向量加载到内存
- [ ] 性能目标：10 列文件上单列查询 I/O 减少 > 70%
- [ ] 单元测试：验证未请求列零 I/O，验证结果正确性

##### 性能实现任务

###### 任务 1：Async I/O 内存预算 ⚠️
- [ ] `ReadAheadConfig`：`{MaxReadAheadBytes int64; MaxReadAheadPages int}`
- [ ] 限制总飞行读前预算为 `MaxReadAheadBytes`（默认 32MB）
- [ ] `ActiveReadAhead() int64` — 当前读前内存用量，用于监控
- [ ] 溢写到同步：预算耗尽时，新读取回退到同步路径
- [ ] 单元测试：预算执行、并发读取器内存追踪

###### 任务 2：BlockCache 调优 🔄
- [ ] 基于 `GOMAXPROCS` 自动调优缓存 shard 数量（当前：硬编码 64 shards；自动调优禁用或计算不合理值时保留 64 作为 fallback）
- [ ] 自适应缓存大小：`MaxCacheSize` 为可用系统内存的百分比（默认 25%）
- [ ] 顺序访问模式上的缓存预取：检测前向扫描 → 预加载下一 block
- [ ] `WarmCache(column string, rowRange RowRange)` — 已知热范围的显式预加载
- [ ] 基准测试：比较不同 shard 数量和大小下的缓存命中率

###### 任务 3：搜索 Goroutine 池 ⚠️
- [ ] `index/hnsw.go` 中的 `SearchWorkerPool`：
  - 并发搜索图遍历的有界 goroutine 池
  - 默认 workers：`min(GOMAXPROCS, 8)` — 防止过度订阅
  - `Submit(query) → channel` — worker 通过 channel 返回结果
- [ ] 预判 I/O 调度器集成：搜索 worker 向调度器提交读取，而非直接访问 OS
- [ ] 基准测试：4x/8x/16x 并发搜索，有/无 worker pool

###### 任务 4：基准测试套件 ⚠️
- [ ] CI 基准回归检测：
  - `bench_results/baseline.txt` — 参考数值
  - `make bench-compare` — 对比当前与基线，标记 >10% 退化
- [ ] 追踪的关键指标：
  - 写入：768 维向量的 MB/s（1K、10K、100K 批量大小）
  - 读取：Get() 延迟（冷缓存 vs 热缓存）
  - 搜索：10K、100K、1M 规模下 k=10 延迟
  - 并发：1x/4x/8x/16x 搜索吞吐
  - 内存：空闲 RSS、写入期间、并发搜索期间
- [ ] 目标：在 `bench_results/history/` 中维护基准历史以进行趋势分析

#### 已知瓶颈与解决路径

**当前问题**：多读取器并发因 OS 页缓存抖动和缺乏协调 I/O 导致严重退化：

```
并发 1:  2.3 ms
并发 4:  9.2 ms  (4x 退化！)
并发 16: 38 ms   (16x 退化！)
```

##### 瓶颈 1: flush() 全量重写 — O(n) ❌
- **位置**：`vego/storage.go:661` — 读取所有现有文档，追加缓冲区，重写整个文件
- **影响**：写入延迟随集合大小线性增长；1M 文档 → 多秒级 flush
- **解决路径**：追加写段文件（O(buffer_size)）→ 后台合并 → Manifest 追踪活跃段
- **依赖**：Manifest 系统（增强 3）
- **验收**：flush() 代价 = O(buffer_size)；1M 向量（768维）写入 < 30s

##### 瓶颈 2: GetBatch 顺序 I/O ❌
- **位置**：`vego/storage.go:365` — 循环顺序调用 Get()
- **影响**：GetBatch(k=10) 代价 ~10x 单次 Get()，而非批量 I/O 的 ~1x
- **解决路径**：批量 RowIndex 查找 → 按文件偏移排序 → 单次顺序扫描物化
- **验收**：GetBatch(k=10) < 2x 单次 Get() 延迟

##### 瓶颈 3: ForEach / GetAllValidDocuments 全量内存加载 ❌
- **位置**：`vego/storage.go:541`（GetAllValidDocuments），`storage.go:597`（ForEach）
- **影响**：全文件加载到内存；1GB 文件 = 1GB+ RSS 峰值
- **解决路径**：多 batch 文件格式 + `ReadNextBatch()` 迭代器 + 列裁剪
- **依赖**：列裁剪（增强 4）、Writer 多 batch 支持
- **验收**：1GB 文件 ForEach RSS < 100MB

##### 瓶颈 4: 并发退化 300%+ ⚠️
- **位置**：OS 页缓存抖动 — 并发 `vfs.File.ReadAt()` 调用导致内核竞争
- **影响**：4x 并发 = 4x 延迟（应为亚线性）
- **解决路径**：`vego/` 层接入 I/O 调度器（已有 `vfs/scheduler.go` + `vfs/async.go` 基础设施）
- **验收**：4x 并发退化 < 20%；16x < 50%

##### 瓶颈 5: 缓存效果未量化 ⚠️
- **位置**：`storage/format/blockcache.go` — Stats() 存在但未暴露到用户 API
- **影响**：无法在没有可见性的情况下调优缓存大小
- **解决路径**：通过 `coll.Metrics()` 暴露 `BlockCache.Stats()`（关联增强 2：基础监控）
- **验收**：重复查询 < 冷缓存延迟 20%（即 5x+ 提升）；命中率通过 `coll.Metrics()` 可见

### 实施优先级与依赖图

#### 优先级 1 — 阻塞 Phase 3（Phase 2 关闭前必须完成）

| 任务 | 位置 | 阻塞 |
|------|------|------|
| 列裁剪（基础） | `storage/column/reader.go` | Phase 3 ForEach 流式遍历、投影下推、并行列读取 |
| 逐页 Min/Max 统计 | `storage/format/page.go` | Phase 3 Zone Map（页面跳过） |
| I/O 调度器存储层集成 | `vego/storage.go` ← `vfs/scheduler.go` | Phase 3 并行列读取、并发读取扩展性 |

#### 优先级 2 — MVP 完整性（核心 Phase 2 交付物）

| 任务 | 理由 | 依赖 |
|------|------|------|
| 墓碑机制 | 生产安全的软删除恢复 | — |
| flush() 追加写优化 | 解决 1M 写入目标（瓶颈 1） | Manifest 系统 |
| Manifest 系统（文件注册表） | 段管理基础、flush 优化、Phase 5 MVCC | — |
| GetBatch 批量 I/O | 解决搜索结果物化性能（瓶颈 2） | — |
| Blob 描述符 + 内联层 | 锁定 API 表面供 Phase 3 Pack/独立层使用 | — |
| 累积缓冲区 | 减少小页面的 I/O 放大 | — |

#### 优先级 3 — 可吸收到 Phase 3

| 任务 | Phase 3 入口点 |
|------|----------------|
| Delta 编码 | Phase 3 存储优化 |
| Writer 异步优化 | Phase 4 性能 |
| 统一监控聚合 | Phase 3 Prometheus 导出器 |
| BlockCache 自动调优 | Phase 3 配置系统 |

#### 推荐执行顺序

```
Wave 1（并行）: 列裁剪 ‖ 逐页 Min/Max ‖ 累积缓冲区
Wave 2（并行）: Manifest 系统 ‖ 墓碑机制
Wave 3:         I/O 调度器存储层集成
Wave 4:         flush() 追加写优化（需要 Wave 2 的 Manifest）
Wave 5（并行）: GetBatch 批量 I/O ‖ Blob 内联层
```

### 完成标准

**已交付**：
- [x] **删除操作使用 Deletion Vector** ✅（`MarkDeleted()` + DV 已实现）
- [x] **更新操作使用 DV + Insert** ✅（无孤儿节点）
- [x] **索引压缩在大批量删除后减少大小** ✅（>30% 空间回收，`Compact()` 已实现）
- [x] **Agent Memory**：Ingest + Reconcile + 混合搜索管线 ✅
- [x] **架构重构**：5 层依赖结构已强制执行 ✅

---

#### P0 — Phase 2 不完成这些不关闭

如果时间不够，先裁减 P1 项；P0 项门控 Phase 2 里程碑。

| # | 交付物 | 验收标准 | 状态 |
|---|--------|----------|------|
| P0-1 | **I/O 调度器** | 4x 并发延迟退化 < 20%（对比当前 300%+） | ❌ |
| P0-2 | **I/O 调度器** | 16x 并发延迟退化 < 50% | ❌ |
| P0-3 | **I/O 调度器** | 合并使顺序扫描下 I/O 系统调用减少 > 40% | ❌ |
| P0-4 | **I/O 调度器** | 背压在 1000+ 并发查询下防止 OOM | ❌ |
| P0-5 | **I/O 调度器** | 所有现有存储测试在调度器启用时通过 | ❌ |
| P0-6 | **墓碑机制** | grace>0 生命周期工作（标记 → grace → 过期→DV → 窗口内恢复成功，窗口外失败）；grace=0 短路到 DV，无 goroutine 开销 | ❌ |
| P0-7 | **Blob 存储（Phase 2 最小范围）** | 描述符格式冻结 + 内联层（< 64KB）SHA256 往返；> 64KB 返回 `ErrBlobTooLargeForPhase2`；Pack/Dedicated 明确推迟到 Phase 3 | ❌ |
| P0-8 | **Manifest 系统** | 每集合 `manifest.json` + `manifest.bin` 含 CRC；原子 temp-rename 写入；CRUD API 被测试覆盖 | ❌ |
| P0-9 | **列裁剪（基础）** | `WithColumns([...])` 使 10 列文件上单列查询 I/O 减少 > 70%；`Search`/`ForEach`/`GetAllValidDocuments` 遵循它 | ❌ |
| P0-10 | **累积缓冲区** | 写入基准上 page 数量减少 > 60%；所有列类型强制最小 64KB page | ❌ |
| P0-11 | **1GB 文件可扩展性** | 单文件 1GB 向量数据读写不 OOM → *瓶颈 3（ForEach 流式）+ 列裁剪* | 🔄 |
| P0-12 | **写入吞吐** | 100万向量（768维）写入 < 30秒 → *瓶颈 1（flush 追加写）* | 🔄 |
| P0-13 | **缓存效果** | 重复查询 < 冷缓存延迟的 20%（即 5x+ 提升）→ *瓶颈 5（缓存可见性 + 调优）* | 🔄 |
| P0-14 | **逐页 Min/Max 统计** | 数值 + 字符串列 min/max 存储于 footer 元数据；null 感知；`PageSkipper` 接口定义（阻塞 Phase 3 Zone Map） | ❌ |

#### P1 — 有则发布；无则 Phase 3 吸收

这些提升存储引擎完整度，但不阻塞 MVP 里程碑。Phase 3 对每个都有自然的入口点。

| # | 交付物 | 验收标准 | Phase 3 入口点 | 状态 |
|---|--------|----------|----------------|------|
| P1-1 | **Delta 编码** | sorted int64/uint64 往返正确；时间戳基准压缩 > 80%；factory.go 中自动检测 | Phase 3 存储优化 | ❌ |
| P1-2 | **Writer 异步优化** | `WithAsyncWrite(true)` 在 `BenchmarkWrite*` 上达到 800–1200 MB/s（估计）；保留确定性 page 顺序 | Phase 4 性能 | ❌ |
| P1-3 | **存储指标（基础）** | `coll.Metrics()` 快照 + 通过 `WithMetrics(true)` 选择启用（默认关闭）；禁用时零开销 | Phase 3 Prometheus 导出器 | ❌ |

---

## Phase 3: Beta（生产就绪）

### 目标
生产级的可靠性、可观测性和查询优化，确保部署信心。参考 Lance：向量索引（ANN）与多模态存储分离。

### 关键任务

#### 存储优化
- **CMO（列元数据偏移）表**：O(1) 列查找，支持 1000+ 列
- **投影下推**：仅读取所需列
- **页面跳过（Zone Map）**：Min/Max 统计跳过无关页面
- **错误恢复**：文件损坏检测、部分读取
- **全面监控**：Prometheus 指标导出
- **配置系统**：可调缓存大小、压缩级别
- **流式读取**：大文件无需完全加载到内存
- **ForEach 流式遍历支持**：解决 `ForEach`/`GetAllValidDocuments` 全量加载内存瓶颈
  - 多 batch 文件格式 + `ReadNextBatch` API（替代单 batch 全量加载）
  - page 级缓存（缓存解码后的 page，替代磁盘块级 BlockCache）
  - 列裁剪读取（仅加载 metadata 列，跳过 Vector 列）
  - 前置：Phase 2 列裁剪（基础）
- **并行列读取**：多列并行加载（3-4x 性能提升）

#### 向量索引：IVF-PQ（新增 - 关键）
- **动机**：HNSW 内存使用 O(N)，不适合 >1000万 向量。IVF-PQ 使用 O(√N) 内存，可接受召回率损失。
- **组件**：
  - **IVF（倒排文件索引）**：K-means 聚类分区（nlist = 4*√N）
  - **PQ（乘积量化）**：将向量分成 m 子向量，量化到 k 中心点（通常 m=16, k=256）
  - **粗量化器**：分区分配的中心点
  - **码本**：每分区的 PQ 中心点
- **搜索流程**：
  1. 使用粗量化器找到最近的 nprobe 分区
  2. 加载选中分区的候选者 PQ 码
  3. 在压缩码上进行非对称距离计算（ADC）
  4. 使用原始向量对 top-k 重排序
- **API**：
  ```go
  index := NewIVFPQIndex(Config{
      Dimension: 768,
      Nlist: 256,      // 分区数
      M: 16,           // 子量化器数
      Nbits: 8,        // 每码位数（k=256）
      Metric: Cosine,
  })
  ```
- **内存节省**：1亿 向量 (768d) = 300GB 原始 → ~5GB（60倍减少）

#### Blob 存储：分层实现（基于 Phase 2）
**Phase 2 交付**：仅 `BlobDescriptor` 格式和内联层（< 64KB）。
**Phase 3 交付**：Pack 层（64KB–4MB）、Dedicated 层（> 4MB）、统一 `BlobStorage` 注册表、`take_blobs()` 流式传输、压缩期间 Pack GC。

- **Pack 文件管理器（64KB–4MB）**：
  - 仅追加 `.pack_NNNN` 侧车文件，在 `MaxPackFileSize`（默认 1GB）处自动滚动
  - `PackWriter.Write(blob) (BlobDescriptor, error)` — `Kind=1` 的描述符
  - `PackReader.Read(desc) / ReadCloser(desc)` — 随机和流式访问
- **Dedicated 文件支持**：> 4MB blob 作为独立 `.blob` 文件存储
  - > 100MB blob 的分段写入；描述符 footer 中 SHA256 保证完整性
  - `DedicatedReader.ReadRange(desc, offset, length)` — HTTP Range 风格部分读取
  - 生命周期：仅在父文档硬删除后删除（tombstone 过期后）
- **BlobStorage 接口与注册表**：基于大小路由内联 / pack / dedicated
  ```go
  type BlobStorage interface {
      Put(blob []byte) (BlobDescriptor, error)
      PutStream(reader io.Reader, size int64) (BlobDescriptor, error)
      Get(desc BlobDescriptor) ([]byte, error)
      GetStream(desc BlobDescriptor) (io.ReadCloser, error)
      GetRange(desc BlobDescriptor, offset, length int64) ([]byte, error)
      Delete(desc BlobDescriptor) error
  }
  ```
  - 默认路由：`size ≤ MaxInlineSize (64KB)` → 内联；`< MinDedicatedSize (4MB)` → pack；否则 dedicated
- **Pack GC**：压缩移除引用 pack blob 的行时，通过重写 pack 文件（跳过未引用范围）回收孤立条目
- **take_blobs() API**：大对象懒加载
  ```go
  func (c *Collection) TakeBlobs(column string, ids []string) ([]BlobFile, error)
  type BlobFile interface {
      io.ReadSeeker
      io.Closer
      Size() int64
  }
  ```
- **用例**：视频帧提取无需加载整个文件
  ```go
  blobs, _ := coll.TakeBlobs("video", []string{"vid001"})
  defer blobs[0].Close()
  
  // Seek 到指定偏移，流式读取
  blobs[0].Seek(1024*1024, io.SeekStart)  // 跳到 1MB
  chunk := make([]byte, 4096)
  blobs[0].Read(chunk)  // 读取 4KB 块
  ```
- **边界测试**（从 Phase 2 推迟的测试计划继承）：正好 4MB pack/dedicated 边界、500MB 流式、并发 pack 写入、GC 正确性
- **与 PyTorch 集成**：Go ML 框架的 `LanceDataset` 等价物

#### Late Materialization（新增）
- **概念**：先在轻量列上过滤，只为匹配行加载重 Blob
- **实现**：
  1. 搜索向量列 → 获取候选行 ID
  2. 应用元数据过滤 → 过滤后行 ID
  3. 仅为最终结果加载 Blob 列
- **收益**：过滤查询 I/O 减少 10x+

### 完成标准
- [ ] 1000 列文件打开时间 < 100ms（对比当前 O(n) 扫描）
- [ ] 单列查询 I/O 减少 90%
- [ ] 文件损坏定位到特定页面，支持部分恢复
- [ ] Prometheus 导出器，关键指标可观测
- [ ] IVF-PQ 索引：1000万 向量搜索 < 50ms，召回率 95%+
- [ ] Blob 存储：支持全部 3 层（内联/打包/独立），懒加载可用
- [ ] Late Materialization：过滤-后加载 I/O 减少 5x+

---

## Phase 4: V1.0（性能版）

### 目标
达到接近 Rust Lance 80% 的性能。聚焦算法优化而非硬件特定加速（Go 限制）。

### 关键任务
- **MiniBlock 架构重构**：页面内部块结构
- **智能预取**：顺序预取 + 步进预取（列式）
- **字符串压缩优化**：Snappy 作为 FSST 替代方案（务实选择）
- **内存池优化**：减少 GC 压力，细粒度对象池
- **自适应压缩级别**：基于数据特征自动选择压缩
- **批处理解码优化**：每次操作处理多个值

#### 向量索引：IVF-HNSW-PQ（新增）
- **混合索引**：结合 IVF（分区）+ HNSW（每分区图）+ PQ（压缩）
- **收益**：
  - IVF 将搜索空间从 N 减少到 N/nlist
  - 分区内 HNSW 提供快速精确搜索
  - PQ 内存减少 20-50x
- **用例**：十亿级向量搜索（如 10亿 向量 = ~100GB，PQ 对比 4TB 原始）
- **架构**：
  ```
  Level 1: IVF（256-4096 分区）
    └─ Level 2: 每分区 HNSW 图（小，适合缓存）
          └─ Level 3: 存储 PQ 码，重排序用原始向量
  ```

#### Late Materialization 增强（新增）
- **Blob 谓词下推**：加载前使用 Blob 元数据（大小、类型）过滤
- **部分 Blob 读取**：只读大文件头部/范围（如视频缩略图）
- **异步 Blob 预取**：基于访问模式的预测性 Blob 加载

#### 多模态查询优化（新增）
- **统一搜索 API**：结合向量搜索 + 元数据过滤 + Blob 存在性检查
  ```go
  results, _ := coll.MultimodalSearch(queryVector, 10,
      WithFilter("category = 'video'"),
      WithBlobCheck("thumbnail"),  // 只返回有缩略图的
  )
  ```

### 完成标准
- [ ] 压缩比：整数 > 70%，字符串 > 60%（Snappy）
- [ ] 顺序扫描性能提升 3x（对比 MVP）
- [ ] 解码开销 < 原始读取成本的 5%
- [ ] 单文件支持 100GB+ 数据集
- [ ] IVF-HNSW-PQ：1亿 向量搜索 < 20ms，召回率 90%+
- [ ] Late Materialization：过滤多模态查询 I/O 减少 10x

---

## Phase 5: V1.5（云原生版）

### 目标
将 Vego 从本地嵌入式存储扩展为云原生多模态向量数据库。

### 范围变更理由
- **移除 io_uring**：Go 生态不成熟、仅限 Linux、复杂性超过收益
- **移除 SIMD**：Go SIMD 支持有限；聚焦算法优化
- **焦点转移**：云存储集成对生产部署更有价值

### 关键任务
- **对象存储抽象**：本地/S3/GCS/Azure 的统一接口
  ```go
  type ObjectStore interface {
      Get(path string, range Range) ([]byte, error)
      Put(path string, data []byte) error
      List(prefix string) ([]ObjectMeta, error)
      Delete(path string) error
  }
  ```
- **云 Blob 存储**：在对象存储（S3）中存储大多模态数据
  - 热数据：本地缓存（LRU）
  - 温数据：S3 标准
  - 冷数据：S3 Glacier（通过生命周期策略）
- **流式上传/下载**：大文件分片上传、断点续传下载
- **凭证管理**：IAM 角色、访问密钥、环境变量支持
- **缓存策略**：分层缓存（本地 SSD → 分布式缓存 → 对象存储）

#### 多模态优化（新增）
- **视频流**：HTTP Range 请求支持浏览器播放
- **图像缩略图**：即时调整大小带缓存
- **Content-Type 检测**：从 Blob 内容推断 MIME 类型
- **预签名 URL**：私有 Blob 临时访问

### 完成标准
- [ ] S3/GCS/Azure Blob 存储支持
- [ ] 标准宽带下 100MB 文件上传 < 5秒
- [ ] 多模态流：视频 seek 延迟 < 100ms
- [ ] 向量搜索性能达到 Milvus/Lance 的 80%（本地），60%（云端）

---

## Phase 6: V2.0（企业版）- 长期

### 目标
从"存储引擎"演进为"数据库系统"。

### 关键任务（按优先级）

#### 第一层：数据安全（必需）
- **WAL（预写日志）**：崩溃恢复
- **校验和**：每页 CRC、每文件完整性验证
- **备份/快照**：时间点恢复

#### 第二层：事务 MVCC
- **快照隔离**：读取历史版本
- **乐观并发控制**：写写冲突检测
- **多版本并发控制**
- **范围外**：两阶段提交、分布式事务

#### 第三层：索引系统（扩展）
- **BTree 索引**：标量字段范围查询
- **Bloom Filter**：存在性查询、负向查找加速
- **倒排索引**：文本字段全文搜索（Phase 6 扩展）
- **向量索引**：HNSW（内存中）、IVF-PQ（磁盘）、IVF-HNSW-PQ（混合）

#### 第四层：分布式（推迟到 V2.0 后）
> **决策**：分布式功能推迟，因其与 Vego "嵌入式存储" 定位冲突。聚焦单节点性能和可靠性。

- ~~数据分区~~（V2.0 后）
- ~~分区裁剪~~（V2.0 后）
- ~~并行查询执行~~（V2.0 后）
- **单节点并行**：单节点内多核查询执行（保留）

#### 第五层：查询引擎（待定规划）
- **表达式系统（基础）**：简单过滤
- **行级过滤**：在 RecordBatch 上执行过滤器

#### 第六层：Phase 0 推迟任务（从 Phase 0 移至此处）
以下任务有意从 Phase 0 推迟，以专注于核心性能：

- **数据库备份/恢复**：`db.Backup(path)`、`db.Restore(path)` 用于灾难恢复
- **高级可观测性**：`db.Stats()`、查询延迟指标、索引构建进度回调
- **增强错误处理**：结构化错误类型、批量操作中的部分失败处理、瞬态故障自动重试
- **测试覆盖**：vego 包单元测试覆盖率 > 70%

### 完成标准
- [ ] 崩溃后 100% 数据恢复
- [ ] 支持并发读写（快照读）
- [ ] 标量查询性能提升 100x（BTree）

---

## 架构决策记录（ADR）

### ADR 1: API 优先设计
**背景**：用户无需理解 HNSW 或 Lance 内部即可使用 Vego  
**决策**：构建统一的 `vego` 包作为主要 API，`index` 和 `storage` 作为内部实现  
**影响**：更简单的用户体验、更易维护的代码库、更易测试

### ADR 2: 以文档为中心的模型
**背景**：向量数据库自然适合文档导向模式  
**决策**：主要 API 使用 Document（ID + 向量 + 元数据），而非原始向量  
**影响**：对用户更直观、支持元数据过滤、符合用例

### ADR 3: 放弃 FSST，采用 Snappy
**背景**：FSST 实现复杂度需要 2-3 周专门投入  
**决策**：v1.0 使用 Snappy，v1.5+ 重新评估 FSST  
**影响**：字符串压缩比从 70% 降至 60%，节省开发时间 2 周

### ADR 4: MiniBlock 必须支持向后兼容
**背景**：文件格式一旦发布，需要长期维护  
**决策**：读取器支持新旧格式；写入器默认使用新格式  
**影响**：读取器代码复杂度增加，但避免痛苦的用户数据迁移

### ADR 5: 事务采用乐观并发控制
**背景**：Lance 主要用于分析，写写冲突罕见  
**决策**：放弃悲观锁，采用 MVCC + 乐观冲突检测  
**影响**：极高的读性能；写冲突返回错误由应用层重试

### ADR 6: 优先块缓存而非 OS 页缓存
**背景**：Go 对 OS 页缓存控制较弱  
**决策**：用户空间块缓存用于精确的内存和预取控制  
**影响**：内存使用略高，但性能更可预测

### ADR 7: 异步 I/O 策略调整
**背景**：当前 AsyncIO 实现性能不如同步 I/O  
**决策**：Phase 1 默认使用同步 I/O，异步 I/O 作为实验性功能  
**影响**：API 必须支持两种模式；用户可显式选择

### ADR 8: 压缩策略
**背景**：小文件压缩开销 > 收益  
**决策**：< 1MB 文件使用 Plain 编码，> 1MB 使用 ZSTD  
**影响**：压缩比略低，速度显著提升

### ADR 9: Deletion Vector 替代物理删除
**背景**：HNSW 不支持高效删除；物理重建昂贵  
**决策**：采用 Lance 风格的 Deletion Vector (DV) 进行逻辑删除  
**权衡**：
- ✅ 快速软删除（O(1) 位图标记）
- ✅ 后台压缩均摊清理成本
- ✅ 为 MVCC 打基础
- ❌ 内存略高（位图开销）
- ❌ 搜索需要 DV 过滤（最小开销）

### ADR 10: 向量与多模态存储分离
**背景**：向量（小、计算密集）和多模态数据（大、I/O 密集）有不同访问模式  
**决策**：
- 向量：带 ANN 索引的 Page 内列式存储
- 多模态：带描述符的懒加载外部存储  
**影响**：
- ✅ 向量搜索不被大 Blob I/O 阻塞
- ✅ 多模态数据可流式/分页
- ✅ 独立扩展（热向量在内存，冷 Blob 在磁盘/S3）

### ADR 11: 放弃 io_uring 和 SIMD（Phase 5 范围变更）
**背景**：Phase 5 原计划 io_uring（仅限 Linux）和 SIMD（Go 限制）  
**决策**：两者都移除；聚焦对象存储和云集成  
**理由**：
- io_uring：Go 支持不成熟（需要 CGO 或实验运行时）；复杂性超过 10-15% 性能增益
- SIMD：Go `simd` 包实验性；纯 Go 算法优化（缓存局部性、预取）提供 80% 收益只需 20% 努力
- 云存储：对生产用例比本地 I/O 微优化更有影响  
**影响**：降低复杂性、更快交付、更广平台支持

### ADR 12: 5 层架构重构（Phase 2）
**背景**：原代码库依赖关系混乱 — `index/` 导入 `storage/`，`memory/` 直接导入 `index/`，共享类型埋在 `storage/arrow/` 中  
**决策**：重构为严格的 5 层架构：`core/`（L1）→ `vfs/`（L2）→ `index/`（L3-A）+ `storage/`（L3-B）→ `vego/`（L4）→ `memory/`（L5）  
**关键迁移**：
- `storage/arrow/` → `core/`（共享类型如 RecordBatch）
- `storage/errors/` → `core/`（共享错误类型）
- `storage/io/` → `vfs/`（文件 I/O 抽象）
- `index/` 隔离：无 storage 导入，通过 `core.RecordBatch` 做 Marshal/Unmarshal
- `memory/` 仅使用 `vego/` 的重导出（距离函数等）  
**影响**：清晰的依赖图，各层可独立测试，跨层安全并行开发。详见 [ARCHITECTURE_CN.md](ARCHITECTURE_CN.md)。

---

## 额外待办

### 测试
- [ ] 覆盖更多测试用例
- [ ] 编码/解码的模糊测试
- [ ] 容错混沌测试
- [ ] CI 中的性能回归测试

### 文档
- [x] API 参考文档（examples/README.md）
- [ ] 性能调优指南
- [ ] 部署和运维指南
- [ ] 从其他格式迁移指南（Parquet 等）

### 工具
- [ ] Vego 文件检查器/转储器
- [ ] 格式转换工具
- [ ] 基准对比工具
- [ ] 可视化分析器集成

---

## 为路线图做贡献

本路线图是活的文档。我们欢迎：
- 不同环境的性能基准测试结果
- 优先级调整建议
- 新功能或 ADR 提案
- 特定阶段可行性反馈

请在提交 PR 前开 issue 讨论任何路线图变更。
