# Vego Roadmap

## Overview

| Phase | Goal | Timeline | Key Deliverables |
|-------|------|----------|------------------|
| Phase 0 ✅ | Unified API & Foundation | 1-2 weeks | User-friendly API, basic integration tests |
| Phase 1 ✅ | Storage Engine Hardening | 4-6 weeks | Row Index, Block Cache, Deletion Vector (framework), Get() O(1) |
| **Phase 2** | MVP | 6-8 weeks | CRUD operations, Agent Memory, Architecture Refactoring, Delete/Update hardening |
| Phase 3 | Beta | 8-10 weeks | CMO, Zone Map, IVF-PQ Index, Blob tiered storage, Production-ready |
| Phase 4 | V1.0 Performance | 10-12 weeks | MiniBlock, prefetch, IVF-HNSW-PQ, Late Materialization |
| Phase 5 | V1.5 Cloud Native | 12-16 weeks | Object Store, Multi-modal optimization, Cloud storage support |
| Phase 6 | V2.0 Enterprise | 20-24 weeks | WAL, MVCC (simplified), Scalar indexes, Point-in-time recovery |

**Current Focus**: Phase 2 wrapping up — Agent Memory ✅, architecture refactoring ✅ (v0.1.5 released), Delete/Update hardening ✅. Preparing for Phase 3 (IVF-PQ, Zone Map, Blob storage).

> **Note**: Phase 0 (Unified API) and Phase 1 (Storage Engine Hardening) are complete. Architecture refactoring (Phase 2) merged to `main` at tag `v0.1.5`. Several non-critical tasks (Backup/Restore, advanced observability, structured errors) were deferred to Phase 6. See Phase 6 "Tier 6" for deferred tasks.

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
- [x] Code test coverage > 60% (actual: 81.3%)
- [x] Deletion Vector framework: Can mark rows as deleted and filter during search
- [x] Compact: Can rebuild index and reclaim deleted space

### Dependencies
- Week 1-2 (File Version) must complete before any disk format changes
- Week 2-4 (Row Index + Cache) can start once File Version is stable
- Week 4-6 (Block Cache) depends on Row Index for cache key management
- Week 5-6 tasks are non-blocking and can proceed in parallel

---

## Phase 2: MVP (Minimum Viable Product) ⭐ CURRENT PRIORITY

### Goal
Enable the system to handle real-world data with basic CRUD and query capabilities. Following Lance's design: separate vector storage (in-page) from multimodal storage (external), enable lazy loading for large objects.

### Key Tasks

#### HNSW Index Hardening with Deletion Vector ✅
- **Deletion Vector Integration ✅**: Replace physical deletion with logical deletion using DV
  - HNSW nodes are marked deleted via DV, not removed from graph
  - Search results filtered by DV (O(1) check per result)
  - Background compaction reclaims space periodically
- **Tombstone Mechanism ⚠️**: Soft-delete for documents with grace period (current DV bitmap provides instant soft-delete; dedicated Tombstone with grace period/recovery not implemented)
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

#### I/O Scheduler Refactoring (Critical) ❌
- **Problem**: Current 4x concurrency = 4x performance degradation
- **Solution**: Implement Lance-style I/O scheduler with:
  - **Request Coalescing ❌**: Merge adjacent/small I/O requests
  - **Priority Queue ❌**: Row-number based priority for sequential scan optimization
  - **Backpressure ❌**: Limit in-flight I/O to prevent memory blowup
  - **Per-file Scheduling ❌**: Independent queues per file to avoid head-of-line blocking
- **Status**: Not implemented, postponed to Phase 3 or as standalone optimization
- **API**:
  ```go
  type IOScheduler interface {
      Submit(requests []IORange, priority int) Future<[]bytes>
      Coalesce(requests []IORange) []IORange
  }
  ```

#### Phase 1 Carry-over Tasks (Storage Engine Finishing)
The following tasks were moved from Phase 1 to Phase 2. They don't affect MVP core functionality but improve storage engine completeness:
- **Per-Page Min/Max Statistics**: Add `MinValue`/`MaxValue` fields to `format.Page`, collected during PageWriter writes, providing fine-grained statistics for Phase 3 Zone Map page skipping
- **Delta Encoding Implementation**: Variable-length integer Delta encoder/decoder for timestamps, auto-increment IDs and other monotonically increasing data; enable `EnableDeltaEncoding` switch in `factory.go`
- **Writer Async Optimization**: Column Writer / PageWriter currently synchronous; implement multi-column parallel encoding + sequential writes to improve bulk write throughput (current ~330 MB/s is sufficient for target scenarios, but as a performance-focused optimization)

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

#### Blob Storage Foundation (New) ❌
- **Goal**: Support multimodal data (images, videos, audio) following Lance Blob v2 design
- **Storage Strategy** (3-tier, similar to Lance):
  - **Inline ❌**: < 64KB blobs stored directly in Page
  - **Pack ❌**: 64KB ~ 4MB blobs stored in `.pack` sidecar files (1GB max per file)
  - **Dedicated ❌**: > 4MB blobs stored in individual `.blob` files
- **Descriptor Format**: `struct { kind uint8; position uint64; size uint64; fileID uint32 }`
- **API Preview**:
- **Status**: Not implemented, planned for Phase 3 or standalone feature module
  ```go
  type BlobStorage interface {
      Write(data []byte) (BlobDescriptor, error)
      Read(desc BlobDescriptor) (io.ReadCloser, error)
  }
  ```

#### Storage Engine Enhancements 🔄
- **Accumulation Buffer 🔄**: Avoid small Pages (< 4KB) (Write Buffer partially implemented)
- **Basic Monitoring ⚠️**: I/O count, cache hit rate, encoding latency (Stats interface partially implemented)
- **Request Coalescing ❌**: Merge adjacent I/O requests (depends on I/O Scheduler)
- **Table Abstraction Layer ⚠️**: Higher-level API for users (Collection API basic version available)
- **Manifest Basic Version ❌**: File metadata management (foundation for Phase 5 MVCC)
- **Column Pruning (Basic) ❌**: Read only required columns

#### Performance Optimization
  - Async I/O memory overhead
  - Multi-reader concurrency degradation (current: 4x concurrency = 4x slowdown!)
    ```
    Concurrency 1:  2.3 ms
    Concurrency 4:  9.2 ms  (4x degradation!)
    Concurrency 16: 38 ms   (16x degradation!)
    ```

### Definition of Done
- [ ] Single file 1GB vector data read/write without OOM 🔄
- [ ] Repeated query performance improved 5x+ (cache hit) 🔄
- [ ] Write 1M vectors (768-dim) < 30s 🔄
- [ ] I/O Scheduler: 4x concurrency performance degradation < 20% (vs current 300%) ❌
- [x] **Delete operation uses Deletion Vector** ✅ (`MarkDeleted()` + DV implemented)
- [x] **Update operation uses DV + Insert** ✅ (no orphan nodes)
- [ ] Blob Storage: Support inline (<64KB) and pack (64KB-4MB) storage ❌
- [x] **Index compaction reduces size after bulk deletes** ✅ (>30% space reclaim, `Compact()` implemented)
- [x] **Agent Memory**: Ingest + Reconcile + Hybrid Search pipeline ✅
- [x] **Architecture Refactoring**: 5-layer dependency structure enforced ✅

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

#### Blob Storage: Tiered Implementation (New)
- **Dedicated File Support**: >4MB blobs stored as individual `.blob` files
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
