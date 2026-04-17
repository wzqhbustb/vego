Phase 1 完成状态判定
  逐项验证结果
   # │ PHASE1_TODO 任务           │ 优先级 │ 状态     │ 验证详情
  ───┼────────────────────────────┼────────┼──────────┼─────────────────────────────────────────────────────────────────────
   1 │ go test -race ./... 零竞态 │ P0     │ 已通过   │ vego/ 包通过（~185s, PASS）；index/ 包通过（~104s, PASS）；
     │                            │        │          │ storage/ 各包全部通过。零竞态。
   2 │ vego 包覆盖率 >= 70%       │ P0     │ 已通过   │ 实测 81.3%，远超 70% 目标
   3 │ Null 编码统一              │ P1     │ 已完成   │ RLE、BitPacking、BSS、Dictionary 四个编码器全部原生支持
     │                            │        │          │ null，不再回退 Zstd
   4 │ 页面级 Min/Max 统计        │ P1     │ 进行中   │ 列级 Min/Max 已实现（storage/format/statistics.go），含类型支持 +
     │                            │        │          │ Merge + Writer 集成。逐页 Min/Max 待实现（Page 结构体增加字段）。
   5 │ Delta 编码                 │ P2     │ 未完成   │ 仍是 stub：EnableDeltaEncoding: false，isMonotonic 返回 false +
     │                            │        │          │ TODO，NewDeltaEncoder() 被注释掉
   6 │ Writer 异步优化            │ P2     │ 未完成   │ Writer 明确标注 "NOT safe for concurrent use"，编码路径全部同步
   7 │ 性能基线建立               │ P3     │ 部分完成 │ Makefile 目标和 benchmark 测试文件存在，但 bench_results/
     │                            │        │          │ 目录不存在，没有实际跑过并记录基线数据

  ROADMAP_CN 中 Phase 1 完成标准对照
   ROADMAP 完成标准                                            │ 状态
  ─────────────────────────────────────────────────────────────┼────────────────────────────────────────────────────────────
   文件版本管理                                                │ 已完成 — V1.2 格式，兼容性测试通过
   Get() O(1)                                                  │ 已完成 — RowIndex + BlockCache
   Search(k=10) 10万文档 < 100ms                               │ 未验证 — 无基线数据
   所有编码器往返测试                                          │ 已完成
   go test -race 无竞态                                        │ 已通过
   基准测试目标（Write/Read）                                  │ 未验证
   代码覆盖率 > 60%                                            │ 已完成 — 81.3%
   Deletion Vector 框架                                        │ 已完成
   Compact 实现                                                │ 已完成
  ────────
  结论：Phase 1 尚未完成，但核心高价值工作已经做完
  按 PHASE1_TODO 的 7 项完成标准：4 项已完成，3 项为 Phase 1 剩余任务。

  必须完成才能关闭 Phase 1（5 项）
  1. 性能基线至少跑一次 — 不需要完美的 CI 集成，但至少执行一次 make bench-all 并记录
     结果，验证 RowIndex + BlockCache 的实际收益。
  2. 逐页 Min/Max — 在 Page 结构体中增加 Min/Max 字段，写入时收集，为后续 Zone Map
     页面跳过打基础。
  3. Delta 编码 — 实现变长整数 Delta 编解码器，启用 factory 中的开关。
  4. Writer 异步优化 — 多列并行编码 + 顺序写入，提升大批量写入吞吐。

  建议的关闭路径
  1. 跑一次 make bench-all，记录基线数据            ← 接下来立即执行
  2. 实现 Page 级 Min/Max 统计                       ← Phase 1 内完成
  3. 实现 Delta 编码                                ← Phase 1 内完成
  4. Writer 异步优化                                ← Phase 1 内完成
  5. Phase 1 关闭 → 进入 Memory Service 开发
