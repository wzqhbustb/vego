# Vego 路线图

## 概述

| 阶段 | 目标 | 时间线 | 关键交付物 |
|------|------|--------|-----------|
| Phase 0 ✅ | 统一 API 与基础 | 1-2 周 | 用户友好的 API、基础集成测试 |
| Phase 1 ✅ | 存储引擎加固 | 4-6 周 | 行索引、块缓存、Deletion Vector（框架）、Get() O(1) |
| **Phase 2** | MVP（最小可行产品） | 6-8 周 | CRUD 操作、Agent Memory、架构重构、Delete/Update 加固 |
| Phase 3 | Beta 版 | 8-10 周 | CMO、Zone Map、IVF-PQ 索引、Blob 分层存储、生产就绪 |
| Phase 4 | V1.0 性能版 | 10-12 周 | MiniBlock、预取、IVF-HNSW-PQ、Late Materialization |
| Phase 5 | V1.5 云原生版 | 12-16 周 | 对象存储、多模态优化、云存储支持 |
| Phase 6 | V2.0 企业版 | 20-24 周 | WAL、MVCC（简化版）、标量索引、时间点恢复 |

**当前重点**：Phase 2 收尾 — Agent Memory ✅、架构重构 ✅（v0.1.5 已发布）、Delete/Update 加固 ✅。准备进入 Phase 3（IVF-PQ、Zone Map、Blob 存储）。

> **说明**：Phase 0（统一 API）和 Phase 1（存储引擎加固）已完成。若干非关键任务（备份/恢复、高级可观测性、结构化错误）已推迟至 Phase 6。详见 Phase 6 "第六层" 了解推迟的任务。

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
- **文件版本管理**：向 Header/Footer 添加版本字段，兼容性检查框架
- **格式演进策略**：设计未来模式变更的前向/后向兼容性

#### 第 2-4 周：内存索引与缓存（关键路径）✅
- **行索引实现 ✅**：idHash → rowIndex 映射，修复 Get() O(n) 复杂度
  - 启动时从 vectors.lance 构建（内存中，<100万文档无需持久化）
  - O(1) 文档检索查找
- **文档 LRU 缓存**：热文档缓存，用于频繁访问的向量
  - 缓存搜索结果以避免重复磁盘读取
  - 可配置容量（默认：1万文档）
  - ⚠️ 注：当前使用 BlockCache 作为页级缓存，独立的 DocumentCache 未实现
- **GetBatch 优化 ✅**：批量加载以减少搜索结果的 I/O 往返
- **rowIndex 和 BlockCache 的使用 ✅，Column Reader，AsyncIO 中也能用到 BlockCache**

#### 第 4-6 周：存储引擎加固 🔄
- **块缓存实现 ✅**：64KB 块、LRU 淘汰、线程安全的页面缓存
- ~~写入器异步优化~~（移至 Phase 2）：多列并行编码 + 顺序写入
- **性能基线建立 ✅**：基准测试套件已跑通并记录基线数据
- **端到端集成测试 ✅**：从写入到读取的完整路径覆盖，含缓存验证

#### 第 5-6 周：存储基础（非阻塞）
- ~~**Delta 编码实现**~~（移至 Phase 2）：时间序列数据的变长整数编码
- **错误分类系统 ✅**：`storage/errors` 包，结构化错误处理
- ~~**页面级统计（Min/Max）**~~（移至 Phase 2）：Phase 3 Zone Map 的基础
- **可空编码统一处理 ✅**：RLE / BitPacking / BSS / Dictionary / Zstd 全部支持 null

#### Deletion Vector 框架（新增）✅
- **设计原理 ✅**：参考 Lance 设计，使用逻辑删除替代物理删除，支持增量更新而无需全量重写
- **内存删除向量 ✅**：基于位图的行级删除标记（RoaringBitmap）
- **HNSW 集成 ✅**：`SearchWithDV()` API 在搜索期间过滤已删除节点
- **持久化 ✅**：将 DV 序列化为 `.del` 侧车文件
- **Compact 实现 ✅**：后台压缩重建索引，清理已删除数据
- **收益 ✅**：实现真正的 Update 支持、防止索引膨胀、为 MVCC 打基础
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

### 步骤
1. 错误分类系统 ✅
2. 端到端集成测试 ✅
3. 性能基线测试 ✅
4. 性能优化：
   - 索引构建性能（HNSW）
   - 查询性能（HNSW）
5. 文件版本管理机制 ✅
6. ~~页面级统计框架~~（移至 Phase 2）
7. ~~Delta 编码框架~~（移至 Phase 2）
8. 可空统一处理 ✅

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

### 目标
使系统能够处理真实世界的数据，具备基础 CRUD 和查询能力。参考 Lance 设计：向量存储（Page 内）与多模态存储（外部）分离，支持大对象懒加载。

### 关键任务

#### HNSW 索引与 Deletion Vector 集成 ✅
- **Deletion Vector 集成 ✅**：使用 DV 替代物理删除
  - HNSW 节点通过 DV 标记删除，不从图中移除
  - 搜索结果通过 DV 过滤（每次结果 O(1) 检查）
  - 后台压缩定期回收空间
- **墓碑机制 ⚠️**：带宽限期的软删除（当前通过 DeletionVector bitmap 标记实现即时软删除；带宽限期/恢复机制的独立 Tombstone 未实现）
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

#### I/O 调度器重構（关键）❌
- **问题**：当前 4x 并发 = 4x 性能退化
- **解决方案**：实现 Lance 风格的 I/O 调度器：
  - **请求合并 ❌**：合并相邻/小 I/O 请求
  - **优先级队列 ❌**：基于行号的优先级，优化顺序扫描
  - **背压 ❌**：限制进行中的 I/O 防止内存爆炸
  - **每文件调度 ❌**：每文件独立队列避免队头阻塞
- **状态**：未实现，计划推迟至 Phase 3 或作为独立优化项目
- **API**：
  ```go
  type IOScheduler interface {
      Submit(requests []IORange, priority int) Future<[]bytes>
      Coalesce(requests []IORange) []IORange
  }
  ```

#### Phase 1 延续任务（存储引擎收尾）
以下任务从 Phase 1 移至 Phase 2，不影响 MVP 核心功能但提升存储引擎完整度：
- **逐页 Min/Max 统计**：在 `format.Page` 结构体中增加 `MinValue`/`MaxValue` 字段，`PageWriter` 写入时逐页收集，为 Phase 3 Zone Map 页面跳过提供细粒度统计
- **Delta 编码实现**：实现变长整数 Delta 编解码器，适用于时间戳、自增 ID 等单调递增数据，启用 `factory.go` 中的 `EnableDeltaEncoding` 开关
- **Writer 异步优化**：Column Writer / PageWriter 当前为同步写入；实现多列并行编码 + 顺序写入，提升大批量写入吞吐（当前 ~330 MB/s 在目标场景下够用，但作为性能专项优化）

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
- **结果**：`core/`（L1）→ `vfs/`（L2）→ `index/`（L3-A）+ `storage/`（L3-B）→ `vego/`（L4）→ `memory/`（L5）
- **详情**：见 [ARCHITECTURE_CN.md](ARCHITECTURE_CN.md)

#### Blob 存储基础（新增）❌
- **目标**：支持多模态数据（图像、视频、音频），参考 Lance Blob v2 设计
- **存储策略**（3 层，类似 Lance）：
  - **内联 ❌**：< 64KB Blob 直接存储在 Page 中
  - **打包 ❌**：64KB ~ 4MB Blob 存储在 `.pack` 侧车文件（每文件最大 1GB）
  - **独立 ❌**：> 4MB Blob 存储在单独的 `.blob` 文件
- **描述符格式**：`struct { kind uint8; position uint64; size uint64; fileID uint32 }`
- **API 预览**：
- **状态**：未实现，计划作为 Phase 3 或独立功能模块
  ```go
  type BlobStorage interface {
      Write(data []byte) (BlobDescriptor, error)
      Read(desc BlobDescriptor) (io.ReadCloser, error)
  }
  ```

#### 存储引擎增强 🔄
- **累积缓冲区 🔄**：避免小页面（< 4KB）（Write Buffer 部分实现）
- **基础监控 ⚠️**：I/O 计数、缓存命中率、编码延迟（Stats 接口部分实现）
- **请求合并 ❌**：合并相邻 I/O 请求（待 I/O 调度器实现）
- **表抽象层 ⚠️**：用户的高级 API（Collection API 基础版本已可用）
- **Manifest 基础版本 ❌**：文件元数据管理（Phase 5 MVCC 的基础）
- **列裁剪（基础）❌**：仅读取所需列

#### 性能优化
  - 异步 I/O 内存开销
  - 多读取器并发退化（当前：4x 并发 = 4x  slowdown！）
    ```
    并发 1:  2.3 ms
    并发 4:  9.2 ms  (4x 退化！)
    并发 16: 38 ms   (16x 退化！)
    ```

### 完成标准
- [ ] 单文件 1GB 向量数据读写不 OOM 🔄
- [ ] 重复查询性能提升 5x+（缓存命中）🔄
- [ ] 写入 100万向量（768维）< 30秒 🔄
- [ ] I/O 调度器：4x 并发性能退化 < 20%（对比当前 300%）❌
- [x] **删除操作使用 Deletion Vector** ✅（`MarkDeleted()` + DV 实现）
- [x] **更新操作使用 DV + Insert** ✅（无孤儿节点）
- [ ] Blob 存储：支持内联（<64KB）和打包（64KB-4MB）存储 ❌
- [x] **索引压缩在大批量删除后减少大小** ✅（>30% 空间回收，`Compact()` 实现）
- [x] **Agent Memory**：Ingest + Reconcile + 混合搜索管线 ✅
- [x] **架构重构**：5 层依赖结构已建立并强制执行 ✅

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

#### Blob 存储：分层实现（新增）
- **独立文件支持**：>4MB Blob 作为独立 `.blob` 文件存储
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
