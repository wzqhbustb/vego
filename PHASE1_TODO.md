# Phase 1: 存储引擎加固 — 剩余任务

## 概述

Phase 1 的核心高价值任务（RowIndex O(1)、BlockCache、Deletion Vector、Compact）已全部完成。以下是剩余的收尾工作，按优先级排序。

---

## P0 — 质量底线

### 1. `go test -race` 全量通过

- **现状**：有 race test 文件（`collection_compact_race_test.go`、`collection_p1_test.go`），但全量 `-race` 运行未验证通过
- **目标**：`go test -race ./...` 零竞态报告
- **范围**：排查 vego/、index/、storage/ 三个包的并发安全
- **优先级理由**：并发安全是生产可用的最低门槛，所有后续工作建立在这个基础上

### 2. 测试覆盖率补齐

- **现状**：总体 67.1%，vego 包单独目标 > 70% 尚未确认
- **目标**：vego 包覆盖率 ≥ 70%
- **方法**：针对 collection.go、storage.go 的边界 case 和错误路径补充测试
- **优先级理由**：差距不大，快速搞定，为后续改动提供安全网

---

## P1 — 技术债清理

### 3. Null 编码统一

- **现状**：仅 Zstd 支持 null，RLE / BitPacking / BSS / Dictionary 遇到 null 返回 `ErrNullNotSupported`，回退 Zstd
- **目标**：所有编码器统一支持 null（通过 null bitmap 分离或编码器内部处理）
- **影响文件**：`storage/encoding/rle.go`、`bitpacking.go`、`bss.go`、`dictionary.go`
- **优先级理由**：影响 4 个编码器的压缩效率，是 Roadmap 标注的 "最复杂" 任务，越早处理越好

### 4. 页面级 Min/Max 统计

- **现状**：`storage/encoding/statistics.go` 仅有 BitWidth 和 MaxLength，无数值列的 Min/Max
- **目标**：写入时收集每页的 Min/Max 统计，存入页面元数据
- **依赖**：无
- **优先级理由**：工作量小（~1-2 天），是 Phase 3 Zone Map 谓词下推的前置条件，早做不亏

---

## P2 — 功能补全

### 5. Delta 编码实现

- **现状**：`storage/encoding/factory.go` 有 stub（`EnableDeltaEncoding: false`），无实际编解码器
- **目标**：实现变长整数 Delta 编码，适用于时间戳、自增 ID 等单调递增数据
- **优先级理由**：对特定场景（时间序列）有显著压缩提升，但不阻塞其他功能

### 6. Writer 异步优化

- **现状**：异步 I/O 基础设施已有，Column Reader 支持异步，但 Column Writer / PageWriter 仍为同步
- **目标**：支持多列并行编码 + 顺序写入，提升大批量写入吞吐
- **优先级理由**：工程量较大，且当前同步写入性能（~330 MB/s）在目标场景下够用，可最后做

---

## P3 — 性能验证

### 7. 性能基线正式建立

- **现状**：benchmark 框架和部分测试已有，但未系统性跑通
- **目标**：
  - [ ] 写入 100MB 向量数据 < 5s ✓ 验证
  - [ ] 读取 100MB 向量数据 < 2s ✓ 验证
  - [ ] Search(k=10) 10 万文档 < 100ms ✓ 验证
- **方法**：运行 `make bench-all`，记录基线数据，纳入 CI 回归检测
- **优先级理由**：验证性工作，放在功能改动完成后做，避免重复跑

---

## 完成标准

全部任务完成后，Phase 1 可正式关闭：

- [ ] `go test -race ./...` 零竞态
- [ ] vego 包测试覆盖率 ≥ 70%
- [ ] 所有编码器支持 null（不回退 Zstd）
- [ ] 页面元数据包含 Min/Max 统计
- [ ] Delta 编码可用
- [ ] Writer 支持并行编码
- [ ] 性能基线数据已记录并纳入 CI

---

## 执行顺序

```
  P0: Race 修复 → 覆盖率补齐
       │
  P1: Null 统一 → Min/Max 统计
       │
  P2: Delta 编码 → Writer 异步
       │
  P3: 性能基线验证
       │
  ✅ Phase 1 关闭 → 进入 Phase 2
```
