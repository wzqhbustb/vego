# Vego Roadmap

## Overview

| Phase | Goal | Timeline | Key Deliverables |
|-------|------|----------|------------------|
| Phase 0 ✅ | Unified API & Foundation | 1-2 weeks | User-friendly API, basic integration tests |
| Phase 1 ✅ | Storage Engine Hardening | 4-6 weeks | Row Index, Block Cache, Deletion Vector (framework), Get() O(1) |
| **Phase 2** | MVP | 10-12 weeks | CRUD operations, Agent Memory, Architecture Refactoring, Delete/Update hardening, I/O Scheduler, Blob descriptor + inline tier |
| Phase 3 | Beta | 8-10 weeks | CMO, Zone Map, IVF-PQ Index, Blob tiered storage (Pack/Dedicated + GC), Production-ready |
| Phase 4 | V1.0 Performance | 10-12 weeks | MiniBlock, prefetch, IVF-HNSW-PQ, Late Materialization |
| Phase 5 | V1.5 Cloud Native | 12-16 weeks | Object Store, Multi-modal optimization, Cloud storage support |
| Phase 6 | V2.0 Enterprise | 20-24 weeks | WAL, MVCC (simplified), Scalar indexes, Point-in-time recovery |

**Current Focus**: Phase 2 expanded scope — core MVP delivered (Agent Memory ✅, Architecture Refactoring ✅ at v0.1.5, Delete/Update hardening ✅). Remaining work: I/O Scheduler (critical), Tombstone with grace period, Blob descriptor + inline tier, Phase 1 storage carry-overs. Pack/Dedicated blob tiers and Pack GC moved to Phase 3.

**Status Legend**: ✅ done · 🔄 in progress · ⚠️ partial / needs follow-up · ❌ not started · ~ deferred

> **Note**: Phase 0 (Unified API) and Phase 1 (Storage Engine Hardening) are complete. Architecture refactoring (Phase 2) merged to `main` at tag `v0.1.5`. Phase 2 timeline extended from 6-8w → 10-12w to absorb I/O Scheduler, Tombstone, and the minimal Blob foundation; full Blob tiered storage moves to Phase 3. Several non-critical tasks (Backup/Restore, advanced observability, structured errors) were deferred to Phase 6. See Phase 6 "Tier 6" for deferred tasks.

---

## Phase 0: Unified API & Foundation ✅ COMPLETE

### Goal
Create a unified, intuitive API that combines HNSW vector search with columnar storage, making Vego accessible to users without requiring deep knowledge of the underlying components.

### Vision
```go
// The Vego API should be this simple:
db, _ := vego.Open("./mydb", vego.WithDimension(768))
coll, _ := db.Collection("documents")

// Insert with auto-generated embeddings
coll.Insert(&vego.Document{
    ID:       vego.DocumentID(),
    Vector:   embedding,  // Your 768-dim vector
    Metadata: map[string]any{"title": "Hello", "author": "Alice"},
})

// Vector search with metadata filter
results, _ := coll.Search(queryVector, 10,
    vego.WithFilter(vego.MetadataFilter{
        Field: "author", Operator: "eq", Value: "Alice",
    }),
)
```

### Key Tasks

#### 1. Unified DB/Collection API ✅
- [x] `vego.Open()` - Open or create database
- [x] `db.Collection()` - Get or create collection
- [x] `db.DropCollection()` - Remove collection
- [x] `db.Collections()` - List collections
- [x] `db.Close()` - Graceful shutdown

#### 2. Document-Centric Collection API ✅
- [x] `coll.Insert(doc)` - Insert single document
- [x] `coll.InsertBatch(docs)` - Batch insert
- [x] `coll.Get(id)` - Retrieve by ID
- [x] `coll.Delete(id)` - Delete document
- [x] `coll.Update(doc)` - Update document
- [x] `coll.Upsert(doc)` - Insert or update

#### 3. Vector Search API ✅
- [x] `coll.Search(query, k, opts...)` - Vector similarity search
- [x] `coll.SearchWithFilter(query, k, filter)` - Search with metadata filter
- [x] `coll.SearchBatch(queries, k, opts...)` - Batch search

#### 4. Configuration API ✅
- [x] `vego.WithDimension(d)` - Set vector dimension
- [x] `vego.WithAdaptive(bool)` - Enable adaptive tuning
- [x] `vego.WithExpectedSize(n)` - Expected dataset size
- [x] `vego.WithDistanceFunc(fn)` - Distance metric (L2/Cosine/InnerProduct)
- [x] `vego.WithM(m)` - HNSW M parameter
- [x] `vego.WithEfConstruction(ef)` - HNSW build quality

#### 5. Persistence API 🔄
- [x] `coll.Save()` - Persist collection to disk
- [x] `coll.Close()` - Auto-save on close
- [x] `coll.Load()` - Reload from disk (verify on init)
- [~] `db.Backup(path)` - Full database backup (deferred to Phase 6)
- [~] `db.Restore(path)` - Restore from backup (deferred to Phase 6)

#### 6. Performance & Observability 📊
- [x] `coll.Stats()` - Collection statistics
- [~] `db.Stats()` - Database-wide statistics (deferred to Phase 6)
- [~] Query latency metrics (deferred to Phase 6)
- [~] Index build progress callback (deferred to Phase 6)

#### 7. Error Handling & Reliability 🔧
- [~] Structured error types (deferred to Phase 6)
- [~] Partial failure handling in batch operations (deferred to Phase 6)
- [~] Auto-retry for transient failures (deferred to Phase 6)
- [x] Corruption detection on load (basic validation exists)

### Definition of Done
- [x] User can perform all CRUD operations without touching `index` or `storage` packages directly
- [x] Examples demonstrate real-world use cases (RAG, semantic search, recommendations, batch insert, persistence)
- [x] API documentation with usage patterns
- [~] Unit test coverage > 70% for vego package (target moved to Phase 1)
- [x] Integration tests for full workflows (e2e_test.go covers core workflows)

### API Design Principles

1. **Simplicity First**: Common operations should be one-liners
2. **Sensible Defaults**: Adaptive configuration works out of the box
3. **Progressive Disclosure**: Simple for beginners, powerful for experts
4. **Consistency**: Similar patterns across DB, Collection, and Query APIs
5. **Fail Fast**: Validation at API boundary, clear error messages

---

## Phase 1: Storage Engine Hardening ✅ COMPLETE

### Goal
Solidify the storage foundation, establish benchmarks, and ensure subsequent development doesn't require rework.

### Key Tasks

#### Week 1-2: File Format Foundation
- [x] **Header/Footer Version Fields** (`storage/format/header.go:18`, `footer.go:17`)
  - `Version uint16` in Header + redundant `Version` in Footer for validation
  - Magic number + flag-based feature detection (`FlagVersioned`)
- [x] **`VersionPolicy` Structured Versioning** (`storage/format/version.go`)
  - `V1_0`, `V1_1`, `V1_2` with FeatureFlags bitmap
  - `Encoded()` / `String()` / `HasFeature()` / `CanRead()`
- [x] **`VersionChecker` Runtime Compatibility** (`storage/format/version.go:276`)
  - Major version must match; minor version backward compatible
  - `CheckReadCompatibility()` returns structured `VersionError` with migration hints
- [x] **Legacy Version Mapping** (`storage/format/version.go:192`)
  - `NormalizeVersion()`: maps old `version=1` → `V1.0` (0x0100)
  - `version_legacy_test.go`: validates backward compatibility for all version pairs
- [x] **Format Version Metadata**
  - Footer stores explicit version string (`vego.format.version`)
  - Foundation for forward/backward compatibility (full strategy in ADR 4, refined during Phase 2)

#### Week 2-4: Memory Index & Caching (Critical Path)
- [x] **RowIndex Memory Mapping** (`vego/storage.go`, `catalog.IDMapping`)
  - `idToHash` (docID → internal ID) + reverse mapping for O(1) lookup
  - Built from `vectors.lance` footer RowIndex metadata on startup
  - In-memory only (<1M docs); rebuild cost negligible vs persistence complexity
- [x] **`Get()` O(1) Path** (`vego/storage.go:245`)
  - `bufferIndex` check (hot path: most recent writes)
  - `tryReadByRowIndex()` → direct page/offset seek for persisted data
  - Falls back to full scan only for legacy files without RowIndex
- [x] **BlockCache Implementation** (`storage/format/blockcache.go`)
  - 64KB blocks, sharded LRU (default 64 shards), thread-safe
  - `Get`/`Put`/`Invalidate`/`Stats` API
  - Used by column readers, footer readers, and RowIndex loaders
- [ ] ~~**DocumentCache**~~ (not implemented)
  - Phase 1 planned per-document LRU cache (default 10K docs)
  - **Decision**: BlockCache provides sufficient caching; DocumentCache deferred indefinitely
  - Search results currently read from BlockCache-decoded pages
- [x] **GetBatch Optimization** (`vego/storage.go`)
  - Batch loading to reduce I/O round trips for Search results

#### Week 4-6: Storage Engine Hardening
- [x] **Deletion Vector In-Memory** (`index/deletion_vector.go`)
  - `RoaringBitmap`-based row-level deletion markers
  - Thread-safe `MarkDeleted()` / `IsDeleted()` / `Count()` / `Union()`
- [x] **DV Persistence** (`index/deletion_vector_persist.go`)
  - Serialize to `.del` sidecar files (varint-encoded RoaringBitmap)
  - Deserialize on load; merge with in-memory DV
- [x] **`SearchWithDV()` API** (`index/hnsw.go:161`)
  - Greedy search returns candidates; post-filter via `isDeleted` callback
  - Caller controls over-fetch (`k*2` for high deletion rates)
- [x] **End-to-End Integration Tests**
  - `index/search_with_dv_test.go`: DV correctness under concurrent insert/delete
  - `vego/e2e_test.go`: full CRUD → Search pipeline
  - `collection_compact_*_test.go`: compaction correctness across 9 strategies
- [x] **Performance Baseline** (`bench_results/baseline.txt`)
  - Write throughput, Read latency, Search latency, Build time
  - 4x concurrency degradation documented (9.2ms vs 2.3ms)
- [ ] **Writer Async Optimization** (deferred)
  - Multi-column parallel encoding + guaranteed sequential writes
  - Current ~330 MB/s sufficient for target scenarios

#### Week 5-6: Storage Foundation (Non-blocking)
- [x] **Error Classification System** (`core/errors` package)
  - Structured errors with context (`core.IO()`, `core.Validation()`)
  - `Unwrap()` support for `errors.Is()` chain inspection
  - Stack-trace-like context accumulation
- [x] **NullBitmap Unified Design** (`storage/encoding/nullbitmap.go`)
  - Shared null bitmap abstraction across all encoders
  - `Encode()` / `Decode()` / `IsNull()` / `SetNull()` API
- [x] **Encoder Null Support** (all encoders)
  - RLE (`rle.go` / `rle_decoder.go`)
  - BitPacking (`bitpacking.go` / `bitpacking_decoder.go`)
  - BSS (`bss.go` / `bss_decoder.go`)
  - Dictionary (`dictionary.go` / `dictionary_decoder.go`)
  - Zstd (`zstd.go` / `zstd_decoder.go`)
- [ ] **Delta Encoding** (deferred to Phase 2)
  - Variable-length integer delta for timestamps, auto-increment IDs
  - `EnableDeltaEncoding` switch in `factory.go` reserved
- [ ] **Page-Level Min/Max Statistics** (deferred to Phase 2)
  - `MinValue`/`MaxValue` fields in `format.Page`
  - Foundation for Phase 3 Zone Map page skipping

#### Deletion Vector Framework (Cross-cutting)
- **Design Rationale**: Following Lance's design, logical deletion instead of physical deletion to support incremental updates without full rewrite
- **Implementation Details**:
  - [x] In-memory: `RoaringBitmap` with `sync.RWMutex`
  - [x] Persistence: `.del` sidecar with varint encoding
  - [x] HNSW integration: `SearchWithDV()` post-filter
  - [x] Compaction: `Compact()` rebuilds graph excluding DV-marked nodes
- **Benefits**:
  - ✅ Fast soft-delete (O(1) bitmap mark)
  - ✅ Background compaction amortizes cleanup cost
  - ✅ Foundation for MVCC (snapshot isolation via DV versioning)
- **Trade-offs**:
  - ❌ Slightly higher memory (bitmap overhead, ~1 bit per row)
  - ❌ Search needs DV filtering (minimal: bitmap check is O(1))
- **API**:
  ```go
  type DeletionVector interface {
      Contains(rowID uint32) bool
      Set(rowID uint32)
      Count() int
      Serialize() ([]byte, error)
      Deserialize([]byte) error
  }
  ```

### Detailed Task Inventory

| # | Task | Status | Key Files |
|---|------|--------|-----------|
| 1 | Header/Footer version fields | ✅ | `storage/format/header.go`, `footer.go` |
| 2 | `VersionPolicy` + `VersionChecker` | ✅ | `storage/format/version.go` |
| 3 | Legacy version mapping | ✅ | `storage/format/version_legacy_test.go` |
| 4 | RowIndex memory mapping | ✅ | `vego/storage.go`, `catalog/` |
| 5 | `Get()` O(1) via RowIndex | ✅ | `vego/storage.go:245` |
| 6 | BlockCache (64KB, LRU, sharded) | ✅ | `storage/format/blockcache.go` |
| 7 | Deletion Vector in-memory | ✅ | `index/deletion_vector.go` |
| 9 | DV persistence (.del sidecar) | ✅ | `index/deletion_vector_persist.go` |
| 10 | `SearchWithDV()` API | ✅ | `index/hnsw.go:161` |
| 11 | End-to-end integration tests | ✅ | `index/search_with_dv_test.go`, `vego/e2e_test.go` |
| 12 | Performance baseline | ✅ | `bench_results/baseline.txt` |
| 13 | Error classification system | ✅ | `core/errors` |
| 14 | NullBitmap + all encoder null support | ✅ | `storage/encoding/nullbitmap.go` |
| 15 | DocumentCache (standalone) | ❌ not impl | — (BlockCache sufficient) |
| 17 | Writer Async Optimization | ❌ deferred | — |
| 18 | Delta Encoding | ❌ deferred | `factory.go` (reserved) |
| 19 | Page-level Min/Max Statistics | ❌ deferred | `format.Page` (reserved) |

### Definition of Done
- [x] File version management: Can detect and handle format version mismatches
- [x] Get() operation is O(1) average case (via Row Index + Cache)
- [ ] Search(k=10) with 100K docs completes in < 100ms (vs current 10+ seconds)
- [x] All encoders pass round-trip tests (encode → decode → data integrity)
- [x] `go test -race` shows no race conditions
- [x] Benchmark targets: Write/Read/Search baseline established
- [x] Code test coverage > 60% (actual: 79.4%, 2026-05)
- [x] Deletion Vector framework: Can mark rows as deleted and filter during search
- [x] Compact: Can rebuild index and reclaim deleted space

### Dependencies
- Week 1-2 (File Version) must complete before any disk format changes
- Week 2-4 (Row Index + Cache) can start once File Version is stable
- Week 4-6 (Block Cache) depends on Row Index for cache key management
- Week 5-6 tasks are non-blocking and can proceed in parallel

---

## Phase 2: MVP (Minimum Viable Product) ⭐ CURRENT PRIORITY

**Timeline**: 10-12 weeks (revised from 6-8w to absorb I/O Scheduler + Tombstone + Blob foundation + Phase 1 carry-overs)

### Goal
Enable the system to handle real-world data with basic CRUD and query capabilities. Following Lance's design: separate vector storage (in-page) from multimodal storage (external), enable lazy loading for large objects.

Phase 2 delivers the **minimum** blob foundation (descriptor format + inline tier only). Pack files, Dedicated files, and Pack GC are scoped into Phase 3 to keep Phase 2 deliverable.

### Key Tasks

#### HNSW Index Hardening with Deletion Vector ✅
- **Deletion Vector Integration ✅**: Replace physical deletion with logical deletion using DV
  - HNSW nodes are marked deleted via DV, not removed from graph
  - Search results filtered by DV (O(1) check per result)
  - Background compaction reclaims space periodically
- **Tombstone Mechanism ❌**: Soft-delete with configurable grace period and recovery
  - **Current State**: DV bitmap provides instant soft-delete only. No grace period, no recovery window.
  - **Design Complete**: Full design spec documented below (API, persistence, expiry loop, grace configuration)
  - **Not Implemented**: No code exists (`index/tombstone.go`, `index/tombstone_persist.go` not present)
- **Orphan Prevention ✅**: Update uses DV to mark old version, inserts new version
- **Index Compaction ✅**: Background rebuild removes DV-marked nodes and optimizes graph
  - Blocking compaction implemented (auto-trigger + manual)
  - Lightweight/background optimization for Phase 4+
  - **Implementation Strategy**: See [COMPACTION.md](COMPACTION.md) for detailed design of 9 compaction strategies
  - **Phase 2 (Current)**: Blocking compaction (simple, reliable)
    - Compact blocks all reads/writes during rebuild
    - Suitable for maintenance windows and batch processing
  - **Future Optimization**: Lightweight locking (Phase 4+) or Background dual-write (Phase 5+)
    - Enables zero-downtime compaction for online services
    - Higher engineering complexity (4-5x effort)

#### I/O Scheduler Refactoring (Critical) 🔄
- **Problem**: Current 4x concurrency = 4x performance degradation
- **Target**: 4x concurrency degradation < 20% (vs current 300%+)
- **Status**: Skeleton implemented (`vfs/scheduler.go` + `vfs/async.go` + `vfs/executor.go` + `storage/column/reader.go` `NewReaderWithAsyncIO`). **Production paths not wired** — `vego/storage.go` still uses synchronous `column.NewReader()` / `column.NewReaderWithCache()`; `NewReaderWithAsyncIO()` is test-only.

**Design Rationale**: The current synchronous I/O model serializes reads per goroutine, causing excessive context switching and OS page cache thrashing under concurrency. A user-space I/O scheduler (inspired by Lance's design) centralizes I/O decision-making to maximize throughput.

**Architecture**: Two-level scheduler (global admission + per-file dispatch). See subtasks below.

##### Subtask 1: Core Scheduler Interface & Types 🔄
- [x] `IORequest` / `IORange` structs defined (`vfs/request.go`)
- [x] `Scheduler` struct with `Submit()` / `SubmitBatch()` (`vfs/scheduler.go`)
- [x] `Executor` worker pool (`vfs/executor.go`)
- [x] `AsyncIO` facade (`vfs/async.go`)
- [ ] `GetScheduler()` singleton + `NewScheduler(cfg)` factory
- [ ] Full `Config` struct: `MaxInFlight`, `EnableCoalescing`, `CoalesceWindow`, `WorkersPerFile`, `FileIdleTimeout`

##### Subtask 2: Request Coalescing Engine ❌
- [ ] `Coalescer` struct: groups adjacent/overlapping `IORange` requests within a time window
- [ ] Algorithm: sort by `(FileID, Offset)`, merge contiguous ranges where `gap < threshold`
- [ ] `CoalesceWindow`: configurable batching window (default 100µs)
- [ ] Unit tests: adjacent merge, gap threshold, multi-file isolation, window timeout

##### Subtask 3: Priority Queue with Row-Number Ordering 🔄
- [x] `priorityQueue` backed by `container/heap` (`vfs/scheduler.go:267`)
- [x] Basic priority ordering (`Priority` field)
- [ ] Priority tiers enum: `PriorityHigh` / `PriorityNormal` / `PriorityLow`
- [ ] Row-number tiebreaking within same tier for sequential scan favoring
- [ ] Starvation prevention (age-based priority boost)

##### Subtask 4: Backpressure Mechanism 🔄
- [x] Queue-capacity blocking: `Submit()` blocks when `queue.Len() >= maxQueueSize`
- [ ] `MaxInFlight` byte limit (default 64MB) — currently only queue-slot limit exists
- [ ] `ErrBackpressure` / `ErrOverloaded` non-blocking mode
- [ ] `InFlight() int64` monitoring API
- [ ] Adaptive threshold auto-tuning

##### Subtask 5: Per-File Scheduling ❌
- [ ] `FileScheduler` struct: one per active file
- [ ] Bounded worker pool per file (default 2–4 workers)
- [ ] `SubmitToFile(fileID, req)` routing from global scheduler
- [ ] Head-of-line blocking isolation per file
- [ ] Idle timeout cleanup (default 30s)

##### Subtask 6: Storage Layer Integration 🔄
- [x] `ColumnReader` supports `NewReaderWithAsyncIO()` (`storage/column/reader.go`)
- [x] Async read path in `readPage()` / `readPagesBatch()` when `useAsync=true`
- [ ] **Production wiring**: `vego/storage.go` must create readers with AsyncIO instead of `NewReader()` / `NewReaderWithCache()`
- [ ] Batch API for multi-page reads
- [ ] Backward-compatible fallback when scheduler is nil

##### Subtask 7: Metrics & Observability 🔄
- [x] Basic counters: `Submitted`, `Completed`, `Errors` (`SchedulerStats` partial)
- [ ] `SchedulerStats` full struct: `CoalescedRequests`, `AvgQueueDepth`, `AvgWaitLatency`, `InFlightBytes`, `BackpressureEvents`
- [ ] Prometheus-format export (foundation for Phase 3 monitoring)

##### Definition of Done (I/O Scheduler)
- [ ] Production read paths wired to AsyncIO (`vego/storage.go`)
- [ ] 4x concurrency search latency degradation < 20% (vs baseline 1x concurrency)
- [ ] 16x concurrency latency degradation < 50%
- [ ] Coalescing reduces I/O syscalls by > 40% under sequential scan
- [ ] Backpressure prevents OOM under 1000+ concurrent queries
- [ ] All existing storage tests pass with scheduler enabled

#### Phase 1 Carry-over Tasks (Storage Engine Finishing)

These tasks don't affect MVP core functionality but improve storage engine completeness. Moved from Phase 1 to Phase 2.

##### Carry-Over 1: Per-Page Min/Max Statistics ❌
- [ ] Add `MinValue any` + `MaxValue any` fields to `format.Page` struct
- [ ] Column-type-aware comparison in `PageWriter`:
  - Numeric columns: track min/max during page write via `Compare()` loop
  - String columns: track lexicographic min/max
  - Vector column: skip (min/max not meaningful for high-dim vectors)
- [ ] `UpdateStats(val any)` method on PageWriter — called per-value during encoding
- [ ] `Page.Stats() PageStats` — returns `{Min, Max, NullCount, RowCount}`
- [ ] Serialize min/max as footer metadata (not inline in page body, to avoid breaking page layout)
- [ ] `PageSkipper` interface (used by Phase 3 Zone Map):
  ```go
  type PageSkipper interface {
      CanSkip(page PageStats, predicate Predicate) bool
  }
  ```
- [ ] Unit tests: verify min/max correctness per encoder type, null handling

##### Carry-Over 2: Delta Encoding Implementation ❌
- [ ] `DeltaEncoder` in `storage/encoding/delta.go`:
  - Input: sorted/almost-sorted int64/uint64 sequences
  - Algorithm: store first value as absolute, subsequent values as `delta = val[i] - val[i-1]` using varint encoding
  - `Encode(values []int64) ([]byte, error)`
- [ ] `DeltaDecoder` in `storage/encoding/delta_decoder.go`:
  - `Decode(data []byte, out []int64) error` — reconstruct original values
  - Support partial reads: seek to position N by summing deltas up to N
- [ ] Integration with `storage/encoding/factory.go`:
  - `EnableDeltaEncoding` switch (already reserved in factory.go)
  - Auto-detect eligibility: column is `int64/uint64/timestamp` type AND sorted in ascending order
  - Fallback to plain/ZSTD when data is not sorted
- [ ] Best-case compression ratio target: > 80% for timestamps, > 60% for auto-increment IDs
- [ ] Unit tests: round-trip, edge cases (all same value, negative deltas, overflow), partial reads

##### Carry-Over 3: Writer Async Optimization ❌
- [ ] Current state: `ColumnWriter` / `PageWriter` encode synchronously, one column at a time
- [ ] Target: encode multiple columns in parallel, then write pages sequentially (deterministic file layout)
- [ ] Implementation plan:
  - [ ] `AsyncColumnWriter`: spawn goroutine per column for encode phase
  - [ ] `sync.WaitGroup` to collect all encoded page buffers
  - [ ] Sequential write of completed pages (ensures deterministic page ordering)
  - [ ] Configurable worker pool: `NumWriteWorkers` (default `runtime.GOMAXPROCS(0)`)
- [ ] Memory budget: limit total in-flight encoded pages to `MaxWriteBufferBytes` (default 128MB)
- [ ] Expected throughput: 800–1200 MB/s (estimated 2.5–3.5x over current ~330 MB/s; actual gain depends on column count and encoding mix due to Amdahl's law — single wide column sees minimal benefit, 10+ narrow columns benefit most)
- [ ] Integration: add `WithAsyncWrite(bool)` option to WriterConfig, default off for safety
- [ ] Benchmark: compare `BenchmarkWrite*` before/after, measure wall-clock time and CPU utilization

#### Agent Memory System ✅
- **Goal**: Embedded vector-searchable memory for AI agents, built on Vego's HNSW + columnar storage
- **Ingest Pipeline ✅**: Unified `Ingest()` entry point with two modes:
  - **ModeNormal**: Messages → LLM fact extraction → Reconcile against existing memories
  - **ModeRaw**: Messages → content-hash dedup → sequential storage per session
- **Reconciliation ✅**: Compare extracted facts against existing memories via vector + keyword search, LLM decides ADD/UPDATE/DELETE/NOOP for each fact
- **Hybrid Search ✅**: 10-stage pipeline combining:
  - HNSW vector search + BM25 keyword search + RRF fusion
  - Signal boosts (pinned, recency, dual-channel)
  - Second-hop associative recall
  - Gap-stop truncation + pagination
- **Supporting Infrastructure ✅**:
  - In-memory BM25 inverted index (English + CJK tokenization)
  - Temporal normalization (Chinese/English relative dates → absolute → relative display)
  - Content-hash deduplication for session messages
  - Near-duplicate detection for reconciliation
  - Schema migration system
- **Architecture**: `memory/` (L5) → `vego/` (L4) only, no direct import of `index/` or `storage/`

#### Architecture Refactoring ✅
- **Goal**: Establish a clean 5-layer dependency architecture
- **Completed Steps**:
  - Step 0: Promote `storage/arrow/` → `core/`, `storage/errors/` → `core/`
  - Step 1: Promote `storage/io/` → `vfs/`
  - Step 2: Isolate `index/` (remove illegal storage imports)
  - Step 3: Clean up `memory/` → `vego/` (remove direct `index/` dependency)
- **I/O Layer Fixes** (completed during Step 1 / Step 3):
  - [x] **`FilePool` Handle Reuse** (`vfs/file_pool.go`)
    - `sync.RWMutex` + reference counting for OS file handles
    - Prevents `too many open files` under concurrent column reads
  - [x] **Partial Read Fix** (`storage/format/footer.go`, `manifest.go`, `page.go`)
    - Replaced bare `Read()` with `io.ReadFull()` for fixed-size structures
    - Prevents corrupted reads under high I/O pressure
- **Result**: `core/` (L1) → `vfs/` (L2) → `index/` (L3-A) + `storage/` (L3-B) → `vego/` (L4) → `memory/` (L5)
- **Details**: See [ARCHITECTURE.md](ARCHITECTURE.md) for full specification

#### Blob Storage Foundation (Minimal — Inline Tier Only) ❌
- **Goal (Phase 2 scope)**: Establish the blob descriptor format and a working inline tier (< 64KB) so the column type, API surface, and on-disk layout are settled before Phase 3 layers in Pack and Dedicated tiers.
- **Status**: Not implemented
- **Deferred to Phase 3**: Pack file manager (64KB–4MB), Dedicated blob files (> 4MB), full `BlobStorage` registry routing, `take_blobs()` streaming API, Pack GC, large-blob boundary tests. See Phase 3 "Blob Storage: Tiered Implementation".

**Design Rationale**: Vectors and large binary objects have fundamentally different access patterns. Vectors are small (~3KB for 768-dim), compute-heavy, and loaded eagerly. Multimodal data is large (KB to GB), I/O-heavy, and should be loaded lazily. Following ADR 10, blob storage is separate from vector/columnar storage. Phase 2 only locks down the descriptor + inline path so user-facing types are stable; Phase 3 expands tiers without breaking the API.

##### Subtask 1: Blob Descriptor Format ❌
- [ ] `BlobDescriptor` struct in `storage/format/blob.go`:
  ```go
  type BlobDescriptor struct {
      Kind     uint8   // 0=inline (Phase 2), 1=pack (Phase 3), 2=dedicated (Phase 3)
      Position uint64  // byte offset within the target file
      Size     uint64  // blob size in bytes
      FileID   uint32  // 0 for inline (lives in column page); reserved for pack/.blob in Phase 3
  }
  ```
- [ ] Serialized size: 21 bytes per descriptor (compact enough for inline storage in page metadata)
- [ ] `Encode()/Decode()` methods for binary I/O
- [ ] **Forward-compatibility**:
  - `Encode()` writes a 1-byte format version prefix (`0x01`) before the 21-byte payload; `Decode()` checks it and rejects unknown versions. This lets Phase 3 extend the descriptor layout without breaking Phase 2 parsing.
  - Encoder writes `Kind=0` only; decoder rejects `Kind=1/2` with a clear "Phase 3 required" error so existing readers fail loudly when upgraded data shows up

##### Subtask 2: Inline Blob Storage (< 64KB) ❌
- [ ] `InlineBlobWriter` in `storage/format/blob_inline.go`:
  - Store blob bytes directly in a column page as a variable-length binary array
  - `Write(blobs [][]byte) ([]BlobDescriptor, error)` — returns ALL descriptors with Kind=0
  - Max blob size: 64KB (configured via `MaxInlineBlobSize`); reject larger blobs with `ErrBlobTooLargeForPhase2` (Phase 3 will route them to Pack/Dedicated)
- [ ] `InlineBlobReader`:
  - `Read(desc BlobDescriptor) ([]byte, error)` — read blob from page using Position + Size
  - No extra file I/O: blob lives in the page already loaded by column reader
- [ ] Trade-off: inline blobs increase page size → fewer rows per page → higher memory. Best for small thumbnails, short text, icons.

##### Subtask 3: Minimal Column + Collection Wiring (Inline Only) ❌
- [ ] `BlobColumnWriter` / `BlobColumnReader` for inline blobs only:
  - Column type: `core.BlobType` (new Arrow extension type)
  - Stores `[]BlobDescriptor` per row (21 bytes × N), payloads colocated in the column page
- [ ] `BlobHandle` type:
  ```go
  type BlobHandle struct {
      desc  BlobDescriptor
      store BlobStorage
  }
  func (h BlobHandle) Read() ([]byte, error)
  func (h BlobHandle) Size() int64
  // ReadCloser/Range omitted in Phase 2 — added with Pack/Dedicated tiers in Phase 3
  func (h BlobHandle) Close() error  // no-op in Phase 2; Phase 3 Pack/Dedicated release file handles
  ```
- [ ] `coll.Insert()` accepts blob fields ≤ `MaxInlineBlobSize`; larger blobs return `ErrBlobTooLargeForPhase2` until Phase 3 lands
- [ ] `coll.Get()` returns a `BlobHandle` for blob fields (callers invoke `.Read()` to materialize bytes)

##### Subtask 4: Testing (Inline Tier) ❌
- [ ] Round-trip tests: write → read → verify SHA256 for sizes {1B, 1KB, 16KB, 64KB-1, exactly 64KB}
- [ ] Boundary test: blob > 64KB returns `ErrBlobTooLargeForPhase2` (Phase 3 will replace this with auto-tier routing)
- [ ] Compatibility test: descriptors with `Kind=1` or `Kind=2` are rejected by Phase 2 readers with a Phase 3 hint
- [ ] Concurrent writes: many small inline blobs across pages, verify no cross-row corruption

#### Storage Engine Enhancements 🔄

##### Enhancement 1: Accumulation Buffer ❌
- **Problem**: Small pages (< 4KB) cause I/O amplification and poor compression ratios
- **Target**: Minimum 64KB pages for all column types
- **Current State**: No `WriteBuffer` implementation in the storage format layer. The only buffer is `DocumentStorage.writeBuffer` in `vego/storage.go`, which is a document-level in-memory buffer for batching writes — not the page-level accumulation buffer described below.
- [ ] `WriteBuffer` in `storage/format/write_buffer.go`:
  - Accumulates values until buffer reaches `MinPageSize` (default 64KB) or `MaxPageRows` (default 65535)
  - `Append(val any) (flushed bool, page *Page, err error)` — returns nil page until buffer is full
  - `Flush() (*Page, error)` — force flush remaining buffered data
- [ ] Column-type-aware size estimation:
  - Fixed-width types (int32, float64): known `sizeof(val)` × count
  - Variable-width types (string, binary): running total of `len(val)`
  - Compressed types: estimate post-compression size (conservatively 50% of raw)
- [ ] `Flush()` called on: buffer threshold reached, collection close, or explicit `coll.Sync()`
- [ ] Benchmark: compare page count and write throughput before/after, target page count reduction > 60%

##### Enhancement 2: Basic Monitoring ⚠️
- **Current**: Stats interface partially implemented
- [ ] `StorageMetrics` struct in `vego/metrics.go` (no external dependencies — keeps `vego` package dependency-free):
  ```go
  type StorageMetrics struct {
      IOCount       atomic.Int64    // total I/O operations
      IOBytes       atomic.Int64    // total bytes read
      CacheHits     atomic.Int64    // BlockCache hits
      CacheMisses   atomic.Int64    // BlockCache misses
      CacheHitRate  float64         // computed: hits / (hits + misses)
      EncodeLatency LatencyHistogram // encoding time distribution (custom, Phase 3 adds Prometheus adapter)
      ReadLatency   LatencyHistogram // read time distribution
      ActiveReaders atomic.Int32    // current concurrent readers
  }
  // LatencyHistogram is a simple bucketed histogram ([]int64 buckets + total count).
  // Phase 3 exports it to Prometheus; Phase 2 only exposes it via Metrics() snapshot.
  type LatencyHistogram struct { ... }
  ```
- [ ] Integration points:
  - `vfs.ReadAt()` wrapper: increment `IOCount`, `IOBytes`
  - `BlockCache.Get()`: increment `CacheHits` or `CacheMisses`
  - Encoder `Encode()` calls: time via histogram
- [ ] `coll.Metrics() StorageMetrics` — snapshot for applications
- [ ] `WithMetrics(enabled bool)` option: zero overhead when disabled (default: **disabled** to honor zero-overhead promise; production deployments opt in)
- [ ] Prometheus exporter (Phase 3): `GET /metrics` endpoint serving Prometheus-format data

##### Enhancement 3: Manifest System ⚠️
- **Goal**: File-level metadata management, foundation for Phase 5 MVCC
- **Current State**: `storage/format/manifest.go` already has `ManifestManager` with MVCC version tracking (`CreateVersion`/`CommitVersion`/`GetVersion`/`GetLatestVersion`). What's missing: file-registry-level `ManifestEntry` (per-file CRC, row ranges, file types) as designed below.
- [ ] `Manifest` struct in `storage/manifest.go`:
  ```go
  type Manifest struct {
      Version     uint32              // manifest format version
      Files       []ManifestEntry     // all files in this collection
      SequenceNum uint64              // monotonically increasing
      CreatedAt   time.Time
      UpdatedAt   time.Time
  }
  type ManifestEntry struct {
      FilePath   string              // relative path within collection dir
      FileType   FileType            // data / index / del / blob / pack
      Size       int64               // file size in bytes
      Checksum   uint32              // CRC32 of file content
      RowCount   uint32              // number of rows in this file
      MinRowID   uint32              // row range for pruning
      MaxRowID   uint32
      CreatedAt  time.Time
  }
  ```
- [ ] `FileType` enum: `{DataFile, IndexFile, DelFile, TombstoneFile, PackFile, BlobFile}`
- [ ] Manifest persistence: JSON for human readability (manifest.json) + binary for performance (manifest.bin)
- [ ] Atomic updates: write to temp file → rename (prevents corruption)
- [ ] `Manifest.Load(path string)` — read and validate (CRC check per entry)
- [ ] `Manifest.Add(entry ManifestEntry)` / `Manifest.Remove(filePath string)`
- [ ] Integration strategy — **Writer-owned (方案 A)**: `PageWriter` / `ColumnWriter` call `Manifest.Add()` internally when they create or replace a data file, and `Manifest.Remove()` on compact/reclaim. This keeps the manifest always correct at the cost of a storage→manifest dependency. (Alternative: return file lists to the `vego` layer and let it manage the manifest — simpler dependency graph but racy if a writer crashes between file creation and manifest update. Starting with 方案 A for correctness; revisit if the dependency becomes problematic.)
- [ ] Unit tests: CRUD, simultaneous read/write, corruption detection, version compatibility

##### Enhancement 4: Column Pruning (Basic) ❌
- **Goal**: Read only required columns, reducing I/O for queries that touch a subset of columns
- [ ] `Schema` struct in `core/schema.go`:
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
- [ ] `ReaderOptions.WithColumns(names []string)` — specify which columns to load
- [ ] `ColumnReader` integration:
  - Parse footer → get column offsets
  - Skip reading pages for unrequested columns entirely
  - Must still read RowIndex (always needed for row resolution)
- [ ] Search integration: `coll.Search(query, k, WithColumns("id", "title"))` — skip vector column on read-back
- [ ] `ForEach` / `GetAllValidDocuments`: column pruning to avoid loading vectors into memory
- [ ] Performance target: single-column query I/O reduced by > 70% on 10-column files
- [ ] Unit tests: verify unrequested columns have zero I/O, verify result correctness

#### Known Bottlenecks & Solution Paths

**Current Problem**: Multi-reader concurrency causes severe degradation due to OS page cache thrashing and lack of coordinated I/O:

```
Concurrency 1:   2.3 ms
Concurrency 4:   9.2 ms  (4x degradation!)
Concurrency 16: 38.0 ms  (16x degradation!)
```

##### Bottleneck 1: flush() Full Rewrite — O(n) ❌
- **Location**: `vego/storage.go:661` — reads ALL existing docs, appends buffer, rewrites entire file
- **Impact**: Write latency grows linearly with collection size; 1M docs → multi-second flush
- **Solution Path**:
  1. Append-only segment files: flush writes buffer as new segment (O(buffer_size))
  2. Background merge: compact segments when count exceeds threshold
  3. Manifest tracks active segments (depends on Enhancement 3: Manifest System)
- **Dependency**: Manifest System (Enhancement 3)
- **Acceptance**: flush() cost = O(buffer_size); 1M vectors (768-dim) write < 30s

##### Bottleneck 2: GetBatch Sequential I/O ❌
- **Location**: `vego/storage.go:365` — calls Get() in a loop sequentially
- **Impact**: GetBatch(k=10) costs ~10x single Get() instead of near-1x with batched I/O
- **Solution Path**:
  1. Batch RowIndex lookup: collect all rowIDs
  2. Sort by file offset for sequential disk access
  3. Single scan pass to materialize all documents
- **Acceptance**: GetBatch(k=10) < 2x single Get() latency

##### Bottleneck 3: ForEach / GetAllValidDocuments Full Memory Load ❌
- **Location**: `vego/storage.go:541` (GetAllValidDocuments), `storage.go:597` (ForEach)
- **Impact**: Loads entire file into memory; 1GB file = 1GB+ RSS spike
- **Solution Path**:
  1. Multi-batch file format: Writer produces multiple RecordBatches per file
  2. `ReadNextBatch()` iterator: reader streams one batch at a time
  3. Column pruning (Enhancement 4): skip vector column when only metadata needed
- **Dependency**: Column Pruning (Enhancement 4), Writer multi-batch support
- **Acceptance**: 1GB file ForEach RSS < 100MB

##### Bottleneck 4: Concurrency Degradation 300%+ ⚠️
- **Location**: OS page cache thrashing — concurrent `vfs.File.ReadAt()` calls cause kernel contention
- **Impact**: 4x concurrency = 4x latency; 16x = 16x latency (should be sub-linear)
- **Solution Path**: I/O Scheduler integration at `vego/` layer (see I/O Scheduler Refactoring above)
  - Route all storage reads through the scheduler's coalescing + priority queue
  - Existing infra: `vfs/scheduler.go` + `vfs/async.go` + `storage/column/reader.go` async path
  - Missing: wiring in `vego/storage.go` read paths
- **Acceptance**: 4x concurrency degradation < 20%; 16x < 50%

##### Bottleneck 5: Cache Effectiveness Unquantified ⚠️
- **Location**: `storage/format/blockcache.go` — Stats() exists but not exposed to user API
- **Impact**: Cannot tune cache size without visibility; repeated queries may not benefit from cache
- **Solution Path**:
  1. Expose `BlockCache.Stats()` through `coll.Metrics()` (ties into Enhancement 2: Basic Monitoring)
  2. Add hit-rate tracking per collection
  3. Auto-tune cache sizing based on observed hit rate
- **Acceptance**: Repeated query < 20% of cold-cache latency (i.e. 5x+ improvement); hit-rate visible via `coll.Metrics()`

##### Performance Implementation Tasks

###### Task 1: Async I/O Memory Budget ⚠️
- [ ] `ReadAheadConfig`: `{MaxReadAheadBytes int64; MaxReadAheadPages int}`
- [ ] Cap total in-flight read-ahead to `MaxReadAheadBytes` (default 32MB)
- [ ] `ActiveReadAhead() int64` — current read-ahead memory usage for monitoring
- [ ] Spill-to-sync: when budget exhausted, new reads fall back to synchronous path
- [ ] Unit tests: budget enforcement, concurrent reader memory tracking

###### Task 2: BlockCache Tuning 🔄
- [ ] Auto-tune cache shard count based on `GOMAXPROCS` (current: hardcoded 64 shards; keep 64 as the fallback when auto-tune is disabled or computes an unreasonable value)
- [ ] Adaptive cache sizing: `MaxCacheSize` as % of available system memory (default 25%)
- [ ] Cache prefetch on sequential access patterns: detect forward-scan → preload next block
- [ ] `WarmCache(column string, rowRange RowRange)` — explicit preload for known hot ranges
- [ ] Benchmark: compare cache hit rate with varying shard counts and sizes

###### Task 3: Goroutine Pool for Search ⚠️
- [ ] `SearchWorkerPool` in `index/hnsw.go`:
  - Bounded goroutine pool for concurrent search graph traversal
  - Default workers: `min(GOMAXPROCS, 8)` — prevent oversubscription
  - `Submit(query) → channel` — worker returns result via channel
- [ ] Anticipate I/O Scheduler integration: search workers submit reads to scheduler, not to OS directly
- [ ] Benchmark: 4x/8x/16x concurrent search with and without worker pool

###### Task 4: Benchmark Suite ⚠️
- [ ] CI benchmark regression detection:
  - `bench_results/baseline.txt` — reference numbers
  - `make bench-compare` — compare current vs baseline, flag >10% regressions
- [ ] Key metrics tracked:
  - Write: MB/s for 768-dim vectors (1K, 10K, 100K batch sizes)
  - Read: Get() latency (cold cache vs warm cache)
  - Search: k=10 latency at 10K, 100K, 1M scale
  - Concurrency: 1x/4x/8x/16x search throughput
  - Memory: RSS at idle, during write, during concurrent search
- [ ] Target: maintain benchmark history in `bench_results/history/` for trend analysis

### Implementation Priority & Dependency Graph

#### Priority 1 — Blocks Phase 3 (must complete before Phase 2 closes)

| Task | Location | Blocks |
|------|----------|--------|
| Column Pruning (Basic) | `storage/column/reader.go` | Phase 3 ForEach Streaming, Projection Pushdown, Parallel Column Reading |
| Per-Page Min/Max Statistics | `storage/format/page.go` | Phase 3 Zone Map (Page Skipping) |
| I/O Scheduler Storage Integration | `vego/storage.go` ← `vfs/scheduler.go` | Phase 3 Parallel Column Reading, concurrent read scalability |

#### Priority 2 — MVP Completeness (core Phase 2 deliverables)

| Task | Rationale | Depends on |
|------|-----------|------------|
| Tombstone Mechanism | Soft-delete recovery for production safety | — |
| flush() Append-Only Optimization | Solves 1M write target (Bottleneck 1) | Manifest System |
| Manifest System (file registry) | Foundation for segments, flush optimization, Phase 5 MVCC | — |
| GetBatch Batch I/O | Solves search result materialization perf (Bottleneck 2) | — |
| Blob Descriptor + Inline Tier | Locks API surface for Phase 3 Pack/Dedicated tiers | — |
| Accumulation Buffer | Reduces I/O amplification from small pages | — |

#### Priority 3 — Can absorb into Phase 3 if time-constrained

| Task | Phase 3 Entry Point |
|------|---------------------|
| Delta Encoding | Phase 3 Storage Optimization |
| Writer Async Optimization | Phase 4 Performance |
| Unified Monitoring Aggregation | Phase 3 Prometheus Exporter |
| BlockCache Auto-Tune | Phase 3 Configuration System |

#### Recommended Execution Order

```
Wave 1 (parallel): Column Pruning ‖ Per-Page Min/Max ‖ Accumulation Buffer
Wave 2 (parallel): Manifest System ‖ Tombstone Mechanism
Wave 3:            I/O Scheduler Storage Integration
Wave 4:            flush() Append-Only (needs Manifest from Wave 2)
Wave 5 (parallel): GetBatch Batch I/O ‖ Blob Inline Tier
```

### Definition of Done

**Already delivered**:
- [x] **Delete operation uses Deletion Vector** ✅ (`MarkDeleted()` + DV implemented)
- [x] **Update operation uses DV + Insert** ✅ (no orphan nodes)
- [x] **Index compaction reduces size after bulk deletes** ✅ (>30% space reclaim, `Compact()` implemented)
- [x] **Agent Memory**: Ingest + Reconcile + Hybrid Search pipeline ✅
- [x] **Architecture Refactoring**: 5-layer dependency structure enforced ✅

---

#### P0 — Phase 2 does not close without these

If time runs short, cut P1 items first; P0 items gate the Phase 2 milestone.

| # | Deliverable | Criterion | Status |
|---|-------------|-----------|--------|
| P0-1 | **I/O Scheduler** | 4x concurrency latency degradation < 20% (vs current 300%+) | ❌ |
| P0-2 | **I/O Scheduler** | 16x concurrency latency degradation < 50% | ❌ |
| P0-3 | **I/O Scheduler** | Coalescing reduces I/O syscalls > 40% under sequential scan | ❌ |
| P0-4 | **I/O Scheduler** | Backpressure prevents OOM under 1000+ concurrent queries | ❌ |
| P0-5 | **I/O Scheduler** | All existing storage tests pass with scheduler enabled | ❌ |
| P0-6 | **Tombstone Mechanism** | grace>0 lifecycle works (mark → grace → expire→DV → undelete inside window succeeds, outside window fails); grace=0 short-circuits to DV with no goroutine cost | ❌ |
| P0-7 | **Blob Storage (Phase 2 minimum)** | Descriptor format frozen + inline tier (< 64KB) round-trips with SHA256; > 64KB returns `ErrBlobTooLargeForPhase2`; Pack/Dedicated explicitly deferred to Phase 3 | ❌ |
| P0-8 | **Manifest System** | Per-collection `manifest.json` + `manifest.bin` with CRC; atomic temp-rename writes; CRUD APIs covered by tests | ❌ |
| P0-9 | **Column Pruning (Basic)** | `WithColumns([...])` reduces single-column query I/O > 70% on 10-column files; `Search`/`ForEach`/`GetAllValidDocuments` honor it | ❌ |
| P0-10 | **Accumulation Buffer** | Page count reduction > 60% on the write benchmark; minimum 64KB pages enforced for all column types | ❌ |
| P0-11 | **1GB file scalability** | Single file 1GB vector data read/write without OOM → *Bottleneck 3 (ForEach streaming) + Column Pruning* | 🔄 |
| P0-12 | **Write throughput** | Write 1M vectors (768-dim) < 30s → *Bottleneck 1 (flush append-only)* | 🔄 |
| P0-13 | **Cache effectiveness** | Repeated query < 20% of cold-cache latency (i.e. 5x+ improvement) → *Bottleneck 5 (cache visibility + tuning)* | 🔄 |
| P0-14 | **Per-Page Min/Max Statistics** | Numeric + string column min/max stored in footer metadata; null-aware; `PageSkipper` interface defined (blocks Phase 3 Zone Map) | ❌ |

#### P1 — Ship if ready; Phase 3 can absorb if not

These improve storage engine completeness but don't block the MVP milestone. Phase 3 has natural entry points for each.

| # | Deliverable | Criterion | Phase 3 entry point | Status |
|---|-------------|-----------|---------------------|--------|
| P1-1 | **Delta Encoding** | Round-trip correctness for sorted int64/uint64; compression > 80% on timestamp benchmark; auto-detect in `factory.go` | Phase 3 Storage Optimization | ❌ |
| P1-2 | **Writer Async Optimization** | `WithAsyncWrite(true)` reaches 800–1200 MB/s (estimated) on `BenchmarkWrite*`; deterministic page ordering preserved | Phase 4 Performance | ❌ |
| P1-3 | **Storage Metrics (basic)** | `coll.Metrics()` snapshot + opt-in via `WithMetrics(true)` (default off); zero overhead when disabled | Phase 3 Prometheus exporter | ❌ |

---

## Phase 3: Beta (Production-Ready)

### Goal
Production-grade reliability, observability, and query optimization for confident deployment. Following Lance: separate vector indexes (ANN) from multimodal storage.

### Key Tasks

#### Storage Optimization
- **CMO (Column Metadata Offset) Table**: O(1) column lookup, supporting 1000+ columns
- **Projection Pushdown**: Read only required columns
- **Page Skipping (Zone Map)**: Min/Max statistics to skip irrelevant pages
- **Error Recovery**: File corruption detection, partial reads
- **Comprehensive Monitoring**: Prometheus metrics export
- **Configuration System**: Tunable cache size, compression levels
- **Streaming Reads**: Large files without loading entirely into memory
- **ForEach Streaming Support**: Solve `ForEach`/`GetAllValidDocuments` full-load memory bottleneck
  - Multi-batch file format + `ReadNextBatch` API (replacing single-batch full load)
  - Page-level cache (cache decoded pages, replacing disk-block-level BlockCache)
  - Column pruning reads (load only metadata columns, skip Vector column)
  - Prerequisite: Phase 2 Column Pruning (Basic)
- **Parallel Column Reading**: Multi-column parallel loading (3-4x performance gain)

#### Vector Index: IVF-PQ (New - Critical)
- **Motivation**: HNSW memory usage O(N), unsuitable for >10M vectors. IVF-PQ uses O(√N) memory with acceptable recall loss.
- **Components**:
  - **IVF (Inverted File Index)**: K-means clustering into partitions (nlist = 4*√N)
  - **PQ (Product Quantization)**: Split vector into m sub-vectors, each quantized to k centroids (typically m=16, k=256)
  - **Coarse Quantizer**: Center points for partition assignment
  - **Codebook**: PQ centroids stored per partition
- **Search Process**:
  1. Find nearest nprobe partitions using coarse quantizer
  2. Load PQ codes for candidates in selected partitions
  3. Asymmetric Distance Computation (ADC) on compressed codes
  4. Rerank top-k using original vectors
- **API**:
  ```go
  index := NewIVFPQIndex(Config{
      Dimension: 768,
      Nlist: 256,      // Number of partitions
      M: 16,           // Sub-quantizers
      Nbits: 8,        // Bits per code (k=256)
      Metric: Cosine,
  })
  ```
- **Memory Saving**: 100M vectors (768d) = 300GB raw → ~5GB with IVF-PQ (60x reduction)

#### Blob Storage: Tiered Implementation (Builds on Phase 2)
**Phase 2 ships**: `BlobDescriptor` format and Inline tier (< 64KB) only.
**Phase 3 ships**: Pack tier (64KB–4MB), Dedicated tier (> 4MB), unified `BlobStorage` registry, `take_blobs()` streaming, and Pack GC during compaction.

- **Pack File Manager (64KB–4MB)**:
  - Append-only `.pack_NNNN` sidecar files, auto-roll at `MaxPackFileSize` (default 1GB)
  - `PackWriter.Write(blob) (BlobDescriptor, error)` — descriptor with `Kind=1`
  - `PackReader.Read(desc) / ReadCloser(desc)` — random and streaming access
- **Dedicated File Support**: > 4MB blobs stored as individual `.blob` files
  - Multipart write for blobs > 100MB; SHA256 in descriptor footer for integrity
  - `DedicatedReader.ReadRange(desc, offset, length)` — HTTP Range-style partial reads
  - Lifecycle: deleted only when parent document is hard-deleted (after tombstone expiry)
- **BlobStorage Interface & Registry**: routes inline / pack / dedicated based on size
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
  - Default routing: `size ≤ MaxInlineSize (64KB)` → inline; `< MinDedicatedSize (4MB)` → pack; else dedicated
- **Pack GC**: when compaction removes rows referencing pack blobs, orphaned entries are reclaimed by rewriting the pack file (skipping unreferenced ranges)
- **take_blobs() API**: Lazy loading for large objects
  ```go
  func (c *Collection) TakeBlobs(column string, ids []string) ([]BlobFile, error)
  type BlobFile interface {
      io.ReadSeeker
      io.Closer
      Size() int64
  }
  ```
- **Use Case**: Video frame extraction without loading entire file
  ```go
  blobs, _ := coll.TakeBlobs("video", []string{"vid001"})
  defer blobs[0].Close()
  
  // Seek to specific offset, stream read
  blobs[0].Seek(1024*1024, io.SeekStart)  // Skip to 1MB
  chunk := make([]byte, 4096)
  blobs[0].Read(chunk)  // Read 4KB chunk
  ```
- **Boundary tests** (carried over from Phase 2's deferred test plan): exactly 4MB pack/dedicated boundary, 500MB streaming, concurrent pack writes, GC correctness
- **Integration with PyTorch**: `LanceDataset` equivalent for Go ML frameworks

#### Late Materialization (New)
- **Concept**: Filter on lightweight columns first, load heavy blobs only for matching rows
- **Implementation**:
  1. Search vector column → get candidate row IDs
  2. Apply metadata filters → filtered row IDs  
  3. Load blob columns only for final results
- **Benefit**: 10x+ I/O reduction for filtered queries

### Definition of Done
- [ ] 1000-column file open time < 100ms (vs current O(n) scan)
- [ ] Single-column query I/O reduced by 90%
- [ ] File corruption localization to specific Page, support partial recovery
- [ ] Prometheus exporter with observable key metrics
- [ ] IVF-PQ index: 10M vectors search < 50ms with 95%+ recall
- [ ] Blob storage: Support all 3 tiers (inline/pack/dedicated), lazy loading works
- [ ] Late materialization: Filter-then-load reduces I/O by 5x+

---

## Phase 4: V1.0 (Performance Edition)

### Goal
Achieve performance approaching 80% of Rust Lance. Focus on algorithmic optimization over hardware-specific acceleration (Go limitations).

### Key Tasks
- **MiniBlock Architecture Refactoring**: Page internal block structure
- **Intelligent Prefetch**: Sequential prefetch + strided prefetch (columnar)
- **String Compression Optimization**: Snappy as FSST alternative (pragmatic choice)
- **Memory Pool Optimization**: Reduce GC pressure, fine-grained object pooling
- **Adaptive Compression Level**: Auto-select compression based on data characteristics
- **Batch Decoding Optimization**: Process multiple values per operation

#### Vector Index: IVF-HNSW-PQ (New)
- **Hybrid Index**: Combine IVF (partitioning) + HNSW (per-partition graph) + PQ (compression)
- **Benefits**:
  - IVF reduces search space from N to N/nlist
  - HNSW within partition provides fast exact search
  - PQ reduces memory by 20-50x
- **Use Case**: Billion-scale vector search (e.g., 1B vectors = ~100GB with PQ vs 4TB raw)
- **Architecture**:
  ```
  Level 1: IVF (256-4096 partitions)
    └─ Level 2: HNSW graph per partition (small, fits in cache)
          └─ Level 3: PQ codes for storage, original vectors for reranking
  ```

#### Late Materialization Enhancement (New)
- **Predicate Pushdown on Blobs**: Filter using blob metadata (size, type) before loading
- **Partial Blob Read**: Read only header/range of large files (e.g., video thumbnail)
- **Async Blob Prefetch**: Predictive loading of blobs based on access patterns

#### Multimodal Query Optimization (New)
- **Unified Search API**: Combine vector search + metadata filter + blob existence check
  ```go
  results, _ := coll.MultimodalSearch(queryVector, 10,
      WithFilter("category = 'video'"),
      WithBlobCheck("thumbnail"),  // Only return if thumbnail exists
  )
  ```

### Definition of Done
- [ ] Compression ratio: integers > 70%, strings > 60% (Snappy)
- [ ] Sequential scan performance improved 3x (vs MVP)
- [ ] Decoding overhead < 5% of raw read cost
- [ ] Single file support for 100GB+ datasets
- [ ] IVF-HNSW-PQ: 100M vectors search < 20ms with 90%+ recall
- [ ] Late materialization: 10x I/O reduction for filtered multimodal queries

---

## Phase 5: V1.5 (Cloud Native Edition)

### Goal
Extend Vego from local embedded storage to cloud-native multimodal vector database.

### Rationale for Scope Change
- **Removed io_uring**: Go ecosystem immature, Linux-only, complexity outweighs benefit
- **Removed SIMD**: Go's SIMD support limited; focus on algorithmic optimization instead
- **Focus Shift**: Cloud storage integration is more valuable for production deployments

### Key Tasks
- **Object Store Abstraction**: Unified interface for local/S3/GCS/Azure
  ```go
  type ObjectStore interface {
      Get(path string, range Range) ([]byte, error)
      Put(path string, data []byte) error
      List(prefix string) ([]ObjectMeta, error)
      Delete(path string) error
  }
  ```
- **Cloud Blob Storage**: Store large multimodal data in object storage (S3)
  - Hot data: Local cache (LRU)
  - Warm data: S3 standard
  - Cold data: S3 Glacier (via lifecycle policy)
- **Streaming Upload/Download**: Multipart upload for large files, resumable downloads
- **Credential Management**: IAM role, access key, environment variable support
- **Caching Strategy**: Tiered cache (local SSD → distributed cache → object store)

#### Multimodal Optimization (New)
- **Video Streaming**: HTTP Range request support for browser-based playback
- **Image Thumbnails**: On-the-fly resizing with caching
- **Content-Type Detection**: MIME type inference from blob content
- **Presigned URLs**: Temporary access to private blobs

### Definition of Done
- [ ] S3/GCS/Azure blob storage support
- [ ] 100MB file upload < 5s on standard broadband
- [ ] Multimodal streaming: Video seek latency < 100ms
- [ ] Vector search performance reaching 80% of Milvus/Lance (local), 60% (cloud)

---

## Phase 6: V2.0 (Enterprise Edition) - Long Term

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

#### Tier 3: Indexing System (Expanded)
- **BTree Index**: Scalar fields for range queries
- **Bloom Filter**: Existence queries, negative lookup acceleration
- **Inverted Index**: Full-text search on text fields (Phase 6 Extension)
- **Vector Indexes**: HNSW (in-memory), IVF-PQ (disk-based), IVF-HNSW-PQ (hybrid)

#### Tier 4: Distributed (Deferred to Post-V2.0)
> **Decision**: Distributed features deferred as they conflict with Vego's "embedded storage" positioning. Focus on single-node performance and reliability.

- ~~Data Partitioning~~ (Post-V2.0)
- ~~Partition Pruning~~ (Post-V2.0)  
- ~~Parallel Query Execution~~ (Post-V2.0)
- **Single-Node Parallelism**: Multi-core query execution within single node (kept)

#### Tier 5: Query Engine (Pending Planning)
- **Expression System (Basic)**: Simple filtering
- **Row-Level Filtering**: Execute filters on RecordBatch

#### Tier 6: Phase 0 Deferred Tasks (Moved from Phase 0)
The following tasks were intentionally deferred from Phase 0 to focus on core performance:

- **Database Backup/Restore**: `db.Backup(path)`, `db.Restore(path)` for disaster recovery
- **Advanced Observability**: `db.Stats()`, query latency metrics, index build progress callbacks
- **Enhanced Error Handling**: Structured error types, partial failure handling in batch operations, auto-retry for transient failures
- **Testing Coverage**: Unit test coverage > 70% for vego package

### Definition of Done
- [ ] 100% data recovery after crash
- [ ] Support concurrent reads and writes (snapshot reads)
- [ ] Scalar query performance improved 100x (BTree)

---

## Architecture Decision Records (ADR)

### ADR 1: API-First Design
**Context**: Users should not need to understand HNSW or Lance internals to use Vego  
**Decision**: Build unified `vego` package as primary API, `index` and `storage` as internal implementation  
**Impact**: Simpler user experience, more maintainable codebase, easier testing

### ADR 2: Document-Centric Model
**Context**: Vector databases naturally fit document-oriented patterns  
**Decision**: Primary API uses Document (ID + Vector + Metadata), not raw vectors  
**Impact**: More intuitive for users, enables metadata filtering, aligns with use cases

### ADR 3: Abandon FSST, Adopt Snappy
**Context**: FSST implementation complexity requires 2-3 weeks dedicated effort  
**Decision**: Use Snappy for v1.0, re-evaluate FSST for v1.5+  
**Impact**: String compression ratio drops from 70% to 60%, development time saved: 2 weeks

### ADR 4: MiniBlock Must Support Backward Compatibility
**Context**: Once file format is released, long-term maintenance is required  
**Decision**: Reader supports both old and new formats; Writer defaults to new format  
**Impact**: Increased Reader code complexity, but avoids painful user data migration

### ADR 5: Optimistic Concurrency Control for Transactions
**Context**: Lance is primarily used for analytics with rare write-write conflicts  
**Decision**: Abandon pessimistic locks, adopt MVCC + optimistic conflict detection  
**Impact**: Extremely high read performance; write conflicts return errors for application-level retry

### ADR 6: Prioritize Block Cache Over OS Page Cache
**Context**: Go has weak control over OS Page Cache  
**Decision**: User-space Block Cache for precise memory and prefetch control  
**Impact**: Slightly higher memory usage, but more predictable performance

### ADR 7: Async I/O Strategy Adjustment
**Context**: Current AsyncIO implementation performs worse than synchronous I/O  
**Decision**: Default to Sync I/O for Phase 1, Async I/O as experimental feature  
**Impact**: API must support both modes; users can explicitly choose

### ADR 8: Compression Strategy
**Context**: Small file compression overhead > benefits  
**Decision**: < 1MB files use Plain encoding, > 1MB use ZSTD  
**Impact**: Slightly lower compression ratio, significantly faster speed

### ADR 9: Deletion Vector over Physical Delete
**Context**: HNSW doesn't support efficient deletion; physical rebuild is expensive  
**Decision**: Adopt Lance-style Deletion Vector (DV) for logical deletion  
**Trade-offs**: 
- ✅ Fast soft-delete (O(1) bitmap mark)
- ✅ Background compaction amortizes cleanup cost
- ✅ Enables MVCC foundation
- ❌ Slightly higher memory (bitmap overhead)
- ❌ Search needs DV filtering (minimal overhead)

### ADR 10: Separate Vector and Multimodal Storage
**Context**: Vectors (small, compute-heavy) and multimodal data (large, I/O-heavy) have different access patterns  
**Decision**: 
- Vectors: In-page columnar storage with ANN indexes
- Multimodal: External storage with descriptor-based lazy loading  
**Impact**: 
- ✅ Vector search not blocked by large blob I/O
- ✅ Multimodal data can be streamed/paged
- ✅ Independent scaling (hot vectors in memory, cold blobs on disk/S3)

### ADR 11: Abandon io_uring and SIMD (Phase 5 Scope Change)
**Context**: Phase 5 originally planned io_uring (Linux-only) and SIMD (Go limitations)  
**Decision**: Remove both; focus on Object Store and cloud integration  
**Rationale**:
- io_uring: Go support immature (requires CGO or experimental runtime); complexity outweighs 10-15% perf gain
- SIMD: Go's `simd` package experimental; pure Go algorithmic optimization (cache locality, prefetch) provides 80% of benefit with 20% effort
- Cloud storage: More impactful for production use cases than local I/O micro-optimization  
**Impact**: Reduced complexity, faster delivery, broader platform support

### ADR 12: 5-Layer Architecture Refactoring (Phase 2)
**Context**: The original codebase had tangled dependencies — `index/` imported `storage/`, `memory/` imported `index/` directly, shared types were buried in `storage/arrow/`  
**Decision**: Refactor into strict 5-layer architecture: `core/` (L1) → `vfs/` (L2) → `index/` (L3-A) + `storage/` (L3-B) → `vego/` (L4) → `memory/` (L5)  
**Key Moves**:
- `storage/arrow/` → `core/` (shared types like RecordBatch)
- `storage/errors/` → `core/` (shared error types)
- `storage/io/` → `vfs/` (file I/O abstraction)
- `index/` isolated: no storage imports, Marshal/Unmarshal via `core.RecordBatch`
- `memory/` uses only `vego/` re-exports (distance functions etc.)  
**Impact**: Clean dependency graph, each layer testable in isolation, safe parallel development across layers. See [ARCHITECTURE.md](ARCHITECTURE.md) for full specification.

---

## Additional TODOs

### Testing
- [ ] Cover more test cases
- [ ] Fuzz testing for encoding/decoding
- [ ] Chaos testing for fault tolerance
- [ ] Performance regression testing in CI

### Documentation
- [x] API reference documentation (examples/README.md)
- [ ] Performance tuning guide
- [ ] Deployment and operations guide
- [ ] Migration guide from other formats (Parquet, etc.)

### Tooling
- [ ] Vego file inspector/dumper
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
