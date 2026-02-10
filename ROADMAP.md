# Lance in Go RoadMap V2

## Overview

| Phase | Goal | Timeline | Key Deliverables |
|-------|------|----------|------------------|
| Phase 0 | Architecture Hardening | 4-6 weeks | Foundation, benchmarks, error handling |
| Phase 1 | MVP | 6-8 weeks | Block Cache, async I/O, Table abstraction |
| Phase 2 | Beta | 8-10 weeks | CMO, projection pushdown, Zone Map |
| Phase 3 | V1.0 Performance | 10-12 weeks | MiniBlock, prefetch, SIMD |
| Phase 4 | V1.5 Extreme | 12-16 weeks | io_uring, vectorized execution |
| Phase 5 | V2.0 Enterprise | 20-24 weeks | WAL, MVCC, indexing, partitioning |

**Note**: The current Go implementation serves as an excellent lightweight alternative. The ultimate goal is to achieve production-grade performance comparable to LanceDB (Rust implementation), requiring significant engineering effort to port its core innovations (MiniBlock, Blob Layout, R/D Levels).

---

## Phase 0: Architecture Hardening

### Goal
Solidify the foundation, establish benchmarks, and ensure subsequent development doesn't require rework.

### Key Tasks
- **Delta Encoding Implementation**: Variable-length integer encoding for time-series data
- **End-to-End Integration Tests**: Full path coverage from Write → Read
- **Performance Baseline Establishment**: Comprehensive benchmark suite
- **Error Classification System**: `lance/errors` package with structured error handling
- **Page-Level Statistics (Min/Max)**: Foundation for Phase 2 Zone Map
- **Nullable Encoding Unified Handling**: Currently only Zstd supports null; unify null handling across all encoders
- **File Version Management**: Prepare for format evolution

### Steps
1. Error classification system ✅
2. End-to-end integration tests ✅
3. Performance baseline tests ✅
4. Performance optimization:
   - Index Build Performance (HNSW)
   - Query Performance (HNSW)
5. File version management mechanism
6. Page-level statistics framework
7. Delta encoding framework
8. Nullable unified handling (most complex) - Requires modification of all encoders

### Definition of Done
- [ ] All encoders pass round-trip tests (encode → decode → data integrity)
- [ ] `go test -race` shows no race conditions
- [ ] Benchmark targets: Write 100MB vector data < 5s, Read < 2s
- [ ] Code test coverage > 60%

### Risks & Mitigation
- **Risk**: Discovery of architectural flaws requiring refactoring
- **Mitigation**: Small-scale refactoring now costs less than post-production discovery

---

## Phase 1: MVP (Minimum Viable Product)

### Goal
Enable the system to handle real-world data with basic CRUD and query capabilities.

### Key Tasks
- **Block Cache Implementation**: 64KB blocks, LRU eviction, thread-safe
- **Writer Async Optimization**: Parallel encoding with guaranteed sequential writes
- **Accumulation Buffer**: Avoid small Pages (< 4KB)
- **Basic Monitoring**: I/O count, cache hit rate, encoding latency
- **Request Coalescing**: Merge adjacent I/O requests
- **Table Abstraction Layer**: Higher-level API for users
- **Manifest Basic Version**: File metadata management (foundation for Phase 5 MVCC)
- **Column Pruning (Basic)**: Read only required columns
- **Performance Optimization Based on Benchmarks**:
  - Async I/O memory overhead
  - Multi-reader concurrency degradation (current: 4x concurrency = 4x slowdown!)
    ```
    Concurrency 1:  2.3 ms
    Concurrency 4:  9.2 ms  (4x degradation!)
    Concurrency 16: 38 ms   (16x degradation!)
    ```

### Definition of Done
- [ ] Single file 1GB vector data read/write without OOM
- [ ] Repeated query performance improved 5x+ (cache hit)
- [ ] Write 1M vectors (768-dim) < 30s
- [ ] Provide high-level APIs: `lance.Open()` / `lance.Write()` / `lance.Read()`

---

## Phase 2: Beta (Production-Ready)

### Goal
Production-grade reliability, observability, and query optimization for confident deployment.

### Key Tasks
- **CMO (Column Metadata Offset) Table**: O(1) column lookup, supporting 1000+ columns
- **Projection Pushdown**: Read only required columns
- **Page Skipping (Zone Map)**: Min/Max statistics to skip irrelevant pages
- **Error Recovery**: File corruption detection, partial reads
- **Comprehensive Monitoring**: Prometheus metrics export
- **Configuration System**: Tunable cache size, compression levels
- **Streaming Reads**: Large files without loading entirely into memory
- **Parallel Column Reading**: Multi-column parallel loading (3-4x performance gain)

### Definition of Done
- [ ] 1000-column file open time < 100ms (vs current O(n) scan)
- [ ] Single-column query I/O reduced by 90%
- [ ] File corruption localization to specific Page, support partial recovery
- [ ] Prometheus exporter with observable key metrics

---

## Phase 3: V1.0 (Performance Edition)

### Goal
Achieve performance approaching 80% of Rust Lance.

### Key Tasks
- **MiniBlock Architecture Refactoring**: Page internal block structure
- **Intelligent Prefetch**: Sequential prefetch + strided prefetch (columnar)
- **String Compression Optimization**: Snappy as FSST alternative (pragmatic choice)
- **Encoder SIMD Optimization**: BitPacking and other critical paths
- **Memory Pool Optimization**: Reduce GC pressure, fine-grained object pooling
- **Adaptive Compression Level**: Auto-select compression based on data characteristics
- **Batch Decoding Optimization**: Process multiple values per operation

### Definition of Done
- [ ] Compression ratio: integers > 70%, strings > 60% (Snappy)
- [ ] Sequential scan performance improved 3x (vs MVP)
- [ ] Decoding overhead < 5% of raw read cost
- [ ] Single file support for 100GB+ datasets

---

## Phase 4: V1.5 (Extreme Edition)

### Goal
Outperform competitors, become the fastest Go columnar storage.

### Key Tasks
- **io_uring Support (Linux only)**: Ultimate I/O performance
- **Vectorized Execution**: SIMD computation based on Arrow
- **FSST Final Implementation**: If time permits, pure Go implementation or CGO binding
- **Adaptive Encoding Optimization**: ML-based optimal encoding selection

### Definition of Done
- [ ] TPC-H query performance approaching 50% of DuckDB
- [ ] Vector search performance reaching 80% of Milvus/Lance

---

## Phase 5: V2.0 (Enterprise Edition) - Long Term

### Goal
Evolve from "storage engine" to "database system".

### Key Tasks (Prioritized)

#### Tier 1: Data Safety (Required)
- **WAL (Write-Ahead Logging)**: Crash recovery
- **Checksums**: Per-Page CRC, per-file integrity verification
- **Backup/Snapshots**: Point-in-time recovery

#### Tier 2: Transaction MVCC
- **Snapshot Isolation**: Read historical versions
- **Optimistic Concurrency Control**: Write-write conflict detection
- **Multi-Version Concurrency Control**
- **Out of Scope**: Two-phase commit, distributed transactions

#### Tier 3: Indexing System
- **BTree Index**: Scalar fields
- **Bloom Filter**: Existence queries
- **Vector Index HNSW**: External integration (already implemented)

#### Tier 4: Distributed
- **Data Partitioning**
- **Partition Pruning**
- **Parallel Query Execution**

#### Tier 4: Query Engine (Pending Planning)
- **Expression System (Basic)**: Simple filtering
- **Row-Level Filtering**: Execute filters on RecordBatch

### Definition of Done
- [ ] 100% data recovery after crash
- [ ] Support concurrent reads and writes (snapshot reads)
- [ ] Scalar query performance improved 100x (BTree)

---

## Architecture Decision Records (ADR)

### ADR 1: Abandon FSST, Adopt Snappy
**Context**: FSST implementation complexity requires 2-3 weeks dedicated effort  
**Decision**: Use Snappy for v1.0, re-evaluate FSST for v1.5+  
**Impact**: String compression ratio drops from 70% to 60%, development time saved: 2 weeks

### ADR 2: MiniBlock Must Support Backward Compatibility
**Context**: Once file format is released, long-term maintenance is required  
**Decision**: Reader supports both old and new formats; Writer defaults to new format  
**Impact**: Increased Reader code complexity, but avoids painful user data migration

### ADR 3: Optimistic Concurrency Control for Transactions
**Context**: Lance is primarily used for analytics with rare write-write conflicts  
**Decision**: Abandon pessimistic locks, adopt MVCC + optimistic conflict detection  
**Impact**: Extremely high read performance; write conflicts return errors for application-level retry

### ADR 4: Prioritize Block Cache Over OS Page Cache
**Context**: Go has weak control over OS Page Cache  
**Decision**: User-space Block Cache for precise memory and prefetch control  
**Impact**: Slightly higher memory usage, but more predictable performance

### ADR 5: Async I/O Strategy Adjustment
**Context**: Current AsyncIO implementation performs worse than synchronous I/O  
**Decision**: Default to Sync I/O for Phase 1, Async I/O as experimental feature  
**Impact**: API must support both modes; users can explicitly choose

### ADR 6: Compression Strategy
**Context**: Small file compression overhead > benefits  
**Decision**: < 1MB files use Plain encoding, > 1MB use ZSTD  
**Impact**: Slightly lower compression ratio, significantly faster speed

---

## Additional TODOs

### Testing
- [ ] Cover more test cases
- [ ] Fuzz testing for encoding/decoding
- [ ] Chaos testing for fault tolerance
- [ ] Performance regression testing in CI

### Documentation
- [ ] API reference documentation
- [ ] Performance tuning guide
- [ ] Deployment and operations guide
- [ ] Migration guide from other formats (Parquet, etc.)

### Tooling
- [ ] Lance file inspector/dumper
- [ ] Format conversion tools
- [ ] Benchmark comparison tool
- [ ] Visual profiler integration

---

## Contributing to the Roadmap

This roadmap is a living document. We welcome:
- Performance benchmark results from different environments
- Suggestions for priority adjustments
- Proposals for new features or ADRs
- Feedback on feasibility of specific phases

Please open an issue to discuss any roadmap changes before submitting PRs.

