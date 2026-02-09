# Async IO

## 设计思想：
- Go 原生：使用 Go 的 goroutine 和 channel，避免复杂的回调；
- 可扩展：为未来支持 io_uring（Linux）预留接口；
- 向后兼容：不破坏现有的同步 API。

## 三层架构

┌─────────────────────────────────────────┐
│  High-Level API (Reader/Writer)         │
│  - 保持同步接口不变                        │
│  - 内部使用 AsyncIO                      │
└─────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────┐
│  I/O Scheduler Layer                    │
│  - Request Queue                        │
│  - Request Coalescing (合并相邻请求)      │
│  - Priority Scheduling                  │
│  - Prefetching                          │
└─────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────┐
│  I/O Executor Layer                     │
│  - Worker Pool (goroutine pool)         │
│  - Batch I/O Operations                 │
│  - File Handle Pool                     │
└─────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────┐
│  OS I/O Layer                           │
│  - os.File (current)                    │
│  - io_uring (future, Linux only)        │
│  - IOCP (future, Windows)               │
└─────────────────────────────────────────┘

## 文件结构

lance/
  io/
    ├── async.go          # 核心异步 I/O 接口
    ├── scheduler.go      # I/O 请求调度器
    ├── executor.go       # I/O 执行器（worker pool）
    ├── request.go        # I/O 请求和响应定义
    ├── cache.go          # 页缓存
    ├── prefetch.go       # 预取策略
    ├── file_pool.go      # 文件句柄池
    ├── stats.go          # I/O 统计和监控
    └── async_test.go     # 测试

## 细节考量

1. 是否需要定义：IO 请求的唯一标识？
2. 请求合并策略细化：请求合并（Coalescing）
   1. 采用分级合并策略
3. 与现有代码的集成的步骤：
    阶段	目标	改动范围
    Phase 1	实现基础 Scheduler 和 Executor	新增 lance/io 包
    Phase 2	改造 PageReader 使用 AsyncIO	修改 column/reader.go
    Phase 3	改造 PageWriter 使用 AsyncIO	修改 column/writer.go
    Phase 4	添加 Prefetch 和 Cache	增强 io/ 包
4. 错误处理和重试机制
    场景：
    - 磁盘 I/O 错误（临时性 vs 永久性）
    - 请求超时
    - 合并后的部分失败
5. 缓存层的更多设计细节
   - 是页缓存还是块缓存？
   - 建议采用 Block Cache（而非 Page Cache）：
     - 缓存单元：固定大小的 Block（如 64KB）
     - 多个 Page 可能共享一个 Block
     - 淘汰策略：LRU / LFU / 2Q
     - 与 Prefetch 集成：预取的块进入缓存
     - 结论
        页缓存：适合 OLTP 点查，简单直观
        块缓存：适合 OLAP 分析，I/O 效率高
        Lance 作为列式分析引擎，建议使用块缓存，理由：
            列式扫描是顺序访问模式
            压缩后的变长 Page 需要统一的物理管理
            更容易实现高效的预取和 I/O 合并


## 具体实施步骤：

### Phase1：实现基础 Scheduler 和 Executor
目标：建立异步 I/O 基础框架
范围：新增 lance/io 包

关键任务：
✅ request.go - 定义 IORequest 和 IOResult
✅ executor.go - Worker Pool 实现
✅ file_pool.go - 文件句柄池
✅ scheduler.go - 基础调度器（优先级队列）
✅ async.go - AsyncIO 主接口
⚠️ 不包含：合并、预取、缓存（留到后续）

验证标准：
- 单元测试覆盖率 > 80%
- 能够并发处理 1000+ 请求
- 无 race condition（go test -race）

### Phase2：改造 PageReader 使用 AsyncIO
目标：Reader 支持异步 I/O，保持向后兼容
范围：修改 column/reader.go

关键改动：
1. Reader 增加 asyncIO 字段（可选）
2. 新增 NewReaderWithAsyncIO() 构造函数
3. 修改内部 readPage() 方法：
   - 如果 asyncIO != nil，使用异步
   - 否则回退到同步 I/O
4. 支持批量读取：ReadPages(pageIndices) 利用 AsyncIO.ReadPages()

### Phase3：改造 PageWriter 使用 AsyncIO

目标：Writer 支持异步写入（可选）
范围：修改 writer.go

注意：写入的异步化比读取复杂，因为涉及：

写入顺序保证（Page 必须按顺序写）
Footer 必须在所有 Page 写完后写入
错误处理更严格（写失败需要回滚）
建议策略：

内部使用 buffered writes
Batch 内的 pages 可以并行编码，但串行写入
使用 sync.WaitGroup 确保所有写入完成

验证标准：

所有现有测试通过
文件格式与同步模式完全一致
大数据集写入性能提升 10-20%

### Phase4：添加 Prefetch 和 Cache
目标：增强性能，支持顺序扫描优化
范围：增强 io/ 包

新增文件：
✅ cache.go - 块缓存实现
✅ prefetch.go - 预取策略
✅ stats.go - I/O 统计

关键特性：

Block Cache（建议 64KB block size）
Sequential Prefetch（检测顺序访问）
Strided Prefetch（检测列式访问模式）
验证标准：

顺序扫描性能提升 2-3x
重复查询缓存命中率 > 90%
内存占用可控（不超过配置上限）

可选增强：
⚠️ Prefetch 策略：顺序预取 + 步进预取（Strided Prefetch for columnar scan）
⚠️ Pin 机制：热点块可以 pin 住不被驱逐
⚠️ 2Q/ARC 替代 LRU：更智能的缓存替换算法


## 补充

1. 注意错误处理和重试
2. 使用块缓存



