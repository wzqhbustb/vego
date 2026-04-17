# Baseline Benchmark Analysis

> Platform: Apple M3 Max, darwin/arm64, Go 1.24.13
> Date: 2026-04-17
> Source: `bench_results/baseline.txt`

文件包含两次 `make bench-all` 执行。第一次 Vego/Storage 路径错误只跑了 Index，第二次三个模块均执行但 Vego 部分因超时被 SIGQUIT 中断。以下以第二次完整数据为主。

---

## 一、HNSW Index Benchmark (10K vectors, D128)

| 指标 | Run 1 | Run 2 | 差异 |
|------|-------|-------|------|
| Build Throughput | 479.5 vec/s | 465.3 vec/s | -3.0% |
| Build Time | 20.85s | 21.49s | +3.1% |
| Memory (Heap) | 18.2 MB | 11.1 MB | -39% (GC timing) |
| Total Alloc | 3,635 MB | 3,635 MB | 一致 |
| Save | 34.8ms | 36.8ms | +5.6% |
| Load | 17.0ms | 16.5ms | -3.3% |
| Storage Size | 5.79 MB | 5.79 MB | 一致 |
| Compression | 0.84x | 0.84x | 一致 |
| Query QPS | 2,158 | 1,973 | -8.6% |
| P50 | 457.9us | 486.4us | +6.2% |
| P95 | 523.6us | 648.8us | +23.9% |
| P99 | 579.5us | 814.3us | **+40.5%** |
| P99/P50 | 1.27x | 1.67x | 尾延迟不稳定 |
| Recall@10 | 82.50% | 82.50% | 一致 |

### 关键发现

1. **Recall 82.5% 偏低** -- 行业标准 HNSW 在 ef=100, 10K 数据集上通常能达到 95%+。当前 82.5% 说明 `searchLayer` 的浮点容差启发式（`relativeTolerance = 0.01`）或 `selectNeighborsHeuristic` 的剪枝过于激进，导致搜索路径过早收敛。
2. **尾延迟不稳定** -- P99 在两次运行间波动 40%（579us vs 814us），P99/P50 从 1.27x 飙到 1.67x。主要原因是 GC 压力：10K 插入产生 **3.6 GB 累计分配、21.3M 次分配**，GC 暂停直接体现在尾延迟上。
3. **存储膨胀 0.84x** -- 压缩后比原始数据还大 19%。128 维 float32 向量本身高熵，压缩效果差是正常的，但 "compression ratio" 应该标注为 1.19x（膨胀）而非 0.84x。
4. **Build 吞吐 ~470 vec/s** -- 对于 10K 规模尚可接受，但 21M allocs 暗示每次插入 ~2100 allocs，heap 压力很大。

---

## 二、Vego Collection Benchmark (Storage + HNSW + Metadata)

### 2.1 插入性能

| Benchmark | ns/op | MB/op | allocs/op | 吞吐 |
|-----------|-------|-------|-----------|------|
| Insert (单条, D128) | 1,171,440 | 0.42 | 1,108 | **854 doc/s** |
| InsertBatch/10 | 12,179,145 | 4.24 | 11,411 | 821 doc/s |
| InsertBatch/50 | 57,406,548 | 21.3 | 56,018 | 871 doc/s |
| InsertBatch/100 | 131,676,343 | 44.0 | 117,452 | 760 doc/s |
| InsertBatch/500 | 540,874,722 | 217.7 | 554,882 | 924 doc/s |

- **批量插入无加速效应** -- Batch 500 的单文档吞吐（924 doc/s）与单条插入（854 doc/s）几乎持平，说明瓶颈在 HNSW 图构建（每条必须走 searchLayer + 连接剪枝），Storage 层 I/O 不是瓶颈。
- **每条插入 ~1100 allocs** -- 大量小对象分配，主要来自 HNSW searchLayer 中的 PriorityQueue/MaxHeap Item 分配和 map 初始化。

### 2.2 维度对插入的影响

| 维度 | ns/op | 相对 D64 |
|------|-------|----------|
| 64 | 685,679 | 1.0x |
| 128 | 1,119,957 | 1.6x |
| 256 | 2,251,239 | 3.3x |
| 512 | 3,954,597 | 5.8x |
| 768 | 5,563,285 | 8.1x |
| 1024 | 6,910,765 | 10.1x |

维度从 64 到 1024（16x），延迟增长约 10x，接近线性。符合预期：距离计算 O(D)，搜索路径长度不变。

### 2.3 搜索性能

| Benchmark | ns/op | MB/op | allocs/op |
|-----------|-------|-------|-----------|
| Search K=1 | 1,071,986 | 3.84 | 5,248 |
| Search K=5 | 4,578,903 | 18.3 | 23,389 |
| Search K=10 | 9,208,524 | 36.4 | 46,070 |
| Search K=20 | 17,811,144 | 72.4 | 91,442 |
| Search K=50 | 44,155,046 | 180.7 | 227,563 |
| Search K=100 | 89,351,433 | 361.3 | 454,374 |
| SearchWithFilter (K=20) | 18,214,378 | 72.5 | 91,460 |
| SearchBatch (10 queries) | 34,285,462 | 359.9 | 460,360 |

- **Search K=10 约 9.2ms** -- 对于 1K 文档的小集合，这个延迟偏高。参考 Phase 1 目标 "Search(k=10) 10 万文档 < 100ms"。
- **Search 与 K 严格线性** -- K=1 为 1.07ms，K=100 为 89.4ms，每增加 1 个 K 约增加 0.89ms。说明搜索开销主要不在 HNSW 遍历（ef 固定），而在从 Storage 读取文档的过程中。**每个搜索结果约 5K allocs 和 3.6MB 分配**，这是 Storage 层逐文档读取 + 反序列化的代价。
- **SearchWithFilter 与 Search K=20 相当** -- Filter 几乎没有额外开销。

### 2.4 搜索规模扩展

| 数据集规模 | 准备时间 | Search ns/op | 相对 1K |
|-----------|----------|------------|---------|
| 1K | 860ms | 9,401,888 | 1.0x |
| 10K | 15.7s | 51,044,486 | 5.4x |
| 100K | 2m51s | 573,038,666 | 60.9x |

- **100K 搜索 573ms** -- 超过 Phase 1 目标 "< 100ms" 的 5.7 倍。主要瓶颈是 Storage 层读取（100K 文档占用 3.6 GB 分配、5M allocs）。
- **从 1K 到 100K 延迟增长 61x** -- 不是 HNSW 的问题（HNSW 搜索复杂度是 O(log N)），而是 Storage 层读取候选文档的开销随数据集增长而线性扩大。
- **100K 准备时间 2m51s** -- 约 584 doc/s，插入吞吐随规模增长而下降（HNSW 图变大，searchLayer 探索范围增加）。

### 2.5 维度对搜索的影响

| 维度 | Search ns/op (1K docs) | 相对 D64 |
|------|------------------------|----------|
| 64 | 6,867,795 | 1.0x |
| 128 | 9,341,048 | 1.4x |
| 256 | 13,251,875 | 1.9x |
| 512 | 21,793,227 | 3.2x |
| 768 | 29,067,230 | 4.2x |
| 1024 | SIGQUIT (timeout) | -- |

D1024 搜索超时崩溃，说明高维搜索的 HNSW 遍历 + Storage 读取组合开销在 benchmark 迭代中累积超限。

---

## 三、Storage Benchmark (Column + Encoding)

### 3.1 编码性能对比

| 编码器 | Encode (ns/op) | Decode (ns/op) | B/op (Encode) |
|--------|----------------|----------------|---------------|
| RLE | 7,020 | 13,126 | 4,976 |
| Zstd | 9,181 | 21,775 | 130,946 |
| BSS (Float32) | 16,327 | 19,849 | 112,853 |
| Dictionary | 56,925 | 17,851 | 64,848 |
| BitPacking | 35,813 | 38,747 | 32,864 |

RLE 编码最快（7us），BitPacking 解码最慢（38.7us）。

### 3.2 E2E 编码轮回

| 数据模式 | ns/op |
|----------|-------|
| RLE | 12,699 |
| Dictionary | 17,916 |
| BitPacking | 18,277 |
| Random | 50,105 |

RLE 在有规律数据上表现最优，Random 数据退化到 50us。

### 3.3 Async IO

| 模式 | ns/op |
|------|-------|
| Sync | 1,452,994 |
| Async | 1,422,018 |

**Async 几乎无收益**（仅快 2%），且并发扩展呈线性退化：

| Concurrency | ns/op | 相对 1 |
|-------------|-------|--------|
| 1 | 2,590,983 | 1.0x |
| 2 | 5,545,395 | 2.1x |
| 4 | 11,619,277 | 4.5x |
| 8 | 22,558,967 | 8.7x |
| 16 | 45,310,463 | 17.5x |

并发度翻倍，耗时翻倍 -- 完全无并行收益，疑似共享锁或顺序 I/O 瓶颈。验证了 phase1_extra.md 中 "Writer 异步优化未完成" 的判断。

### 3.4 列数扩展 (Async IO)

| Columns | ns/op | 相对 1 |
|---------|-------|--------|
| 1 | 111,964 | 1.0x |
| 5 | 266,849 | 2.4x |
| 10 | 411,678 | 3.7x |
| 20 | 674,207 | 6.0x |
| 50 | 1,437,153 | 12.8x |

列数扩展接近线性，略有超线性增长（50 列时 12.8x 而非 50x），说明每列的固定开销（header 解析、内存分配）占比较大。

### 3.5 其他 Storage 基准

| Benchmark | ns/op | B/op |
|-----------|-------|------|
| WriteInt32Array | 7,464 | 3,488 |
| ReadInt32Array | 5,273 | 8,328 |
| WriteVectorArray | 278,109 | 15,131,706 |
| FileRoundtrip | 6,690,770 | 6,014,669 |
| Float32 Reader | 4,532,790 | 12,139,357 |
| Float64 Reader | 4,830,751 | 16,001,218 |
| Dict High Cardinality | 333,512 | 552,162 |

WriteVectorArray 的 15MB/op 分配量显著偏高，可能是向量数据的多次拷贝所致。

---

## 四、Phase 1 目标对照

| ROADMAP 目标 | 基线数据 | 判定 |
|-------------|---------|------|
| Search(k=10) 10 万文档 < 100ms | 573ms | **未达标 (5.7x)** |
| go test -race 零竞态 | 已通过 | 达标 |
| 代码覆盖率 > 60% | 81.3% | 达标 |
| Write/Read 基准 | 已记录 | 达标（有数据） |

---

## 五、核心瓶颈总结

| # | 瓶颈 | 影响 | 根因 | 优化方向 |
|---|------|------|------|----------|
| 1 | 搜索热路径分配风暴 | Search K=10 单次 46K allocs / 36MB | Storage 层逐文档 Get() 反序列化 | 延迟加载；Search 只返回 ID+Distance，按需读文档 |
| 2 | Recall 偏低 (82.5%) | 搜索质量不达标 | searchLayer 浮点容差过宽 / selectNeighborsHeuristic 剪枝过激进 | 去掉 relativeTolerance 或降低阈值 |
| 3 | 100K 搜索远超目标 | 573ms vs 目标 100ms | Storage 层读取候选文档开销随数据集线性增长 | 减少 Search 结果的文档加载量 |
| 4 | Async IO 完全无效 | 并发列读取退化为串行 | Writer 异步优化未实现 | 多列并行编码 + 顺序写入 |
| 5 | GC 压力致尾延迟不稳 | P99/P50 从 1.27x 到 1.67x | 10K 插入产生 3.6GB / 21M allocs | 对象池化 PriorityQueue Items，预分配 visited map |
| 6 | 批量插入无加速 | Batch 500 与单条吞吐持平 | HNSW 图构建为串行瓶颈 | 批量预构建 + 延迟连接 |
