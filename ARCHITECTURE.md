# Vego Architecture

This document describes the layered architecture of Vego, including design decisions, package boundaries, dependency rules, and evolution strategy.

> **Note:** This document describes the **target** layered architecture. The current codebase is actively evolving toward this design.

---

## 1. Layered Overview

```
┌─────────────────────────────────────────────────────────────────┐
│  Layer 5: Application Service          memory/                  │
│  Agent Memory, Hybrid Search, LLM Fact Extraction, State Machine│
└─────────────────────────────────────────────────────────────────┘
                              │ depends on
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Layer 4: API / Orchestration          vego/                    │
│  DB, Collection, Document, Query, Config                        │
│  Role: coordinate index engine and storage engine               │
└─────────────────────────────────────────────────────────────────┘
                              │
               ┌──────────────┴──────────────┐
               ▼                             ▼
┌──────────────────────────┐  ┌──────────────────────────────────┐
│  Layer 3-A: Index Engine │  │  Layer 3-B: Storage Engine       │
│  index/                  │  │  storage/                        │
│                          │  │                                  │
│  HNSW graph build/search │  │  storage/catalog/  metadata mgmt │
│  Distance functions      │  │  storage/column/   columnar R/W  │
│  DeletionVector filtering│  │  storage/encoding/ codec/compress│
│  Adaptive parameters     │  │  storage/format/   file structure│
│                          │  │                                  │
│  Does NOT know how data  │  │                                  │
│  is stored. Only cares   │  │                                  │
│  about vectors and graph │  │                                  │
│  structure.              │  │                                  │
└──────────────────────────┘  └──────────────────────────────────┘
               │                             │
               └──────────────┬──────────────┘
                              │ shared dependency
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Layer 2: I/O Layer                    vfs/                     │
│  File read/write, sync/async I/O, file handle management        │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Layer 1: Foundation                   core/                    │
│  Schema, Array, Buffer, RecordBatch, Bitmap, Builder            │
└─────────────────────────────────────────────────────────────────┘
```

---

## 2. Package Structure

```
vego/
├── memory/              # Layer 5: Agent Memory Service
├── vego/                # Layer 4: API / Orchestration
├── index/               # Layer 3-A: Index Engine (HNSW)
├── storage/             # Layer 3-B: Storage Engine
│   ├── catalog/         #   Metadata management (Snapshot, IDMapping, DeletionStore)
│   ├── column/          #   Columnar read/write (ColumnWriter, ColumnReader)
│   ├── encoding/        #   Adaptive encoding (ZSTD, RLE, BitPacking, BSS, Dictionary)
│   └── format/          #   File structure (Header, Footer, PageIndex, BlockCache)
├── vfs/                 # Layer 2: I/O operations (sync/async file access)
└── core/                # Layer 1: In-memory format (Schema, Array, RecordBatch)
```

---

## 3. Import Dependency Rules

```
memory/  ──→  vego/  ──→  index/            ──→  core/
    └────────→  index/   (current state; will be removed in Step 2)
                     ──→  storage/catalog/   ──→  core/, vfs/
                     ──→  storage/column/    ──→  storage/encoding/, storage/format/,
                                                  core/, vfs/
                                                  storage/encoding/ ──→ core/
                                                  storage/format/   ──→ core/
                                                  vfs/              ──→ (stdlib)
                                                  core/             ──→ (stdlib)
```

**Critical constraints:**

- `index/` does NOT import `storage/` — the index engine has no knowledge of persistence.
- `storage/catalog/` does NOT import `index/` — the metadata layer has no knowledge of index algorithms.
- `vego/` is the ONLY package that imports both `index/` and `storage/` — it is the orchestration layer.
- `storage/column/` → `storage/encoding/` → `storage/format/` → `core/` is a unidirectional dependency chain.
- `core/` and `vfs/` are independent top-level packages — shared infrastructure for both index and storage engines.

**Current deviation — `memory/` → `index/`:**

The `memory/` package currently imports `index/` directly. This dependency is indirect in nature — `memory/` uses `index/` types that should be re-exported through `vego/`. Elimination path: ensure `memory/` only references public types exposed by `vego/`, never internal `index/` types directly. This will be resolved as part of Step 2.

---

## 4. Layer Details

### 4.1 Layer 1: Foundation (core/)

> **Current location:** `storage/arrow/`. Will be promoted to top-level `core/` in Migration Step 0.

Pure in-memory data representation. Zero external dependencies.

Provides: Schema, Field, Array (Int32Array, Float32Array, FixedSizeListArray, etc.), Buffer, RecordBatch, Bitmap, Builder.

Design: Custom Arrow implementation without CGO. Zero-copy semantics propagate upward through the entire stack.

### 4.2 Layer 2: I/O Layer (vfs/)

> **Current location:** `storage/io/`. Will be promoted to top-level `vfs/` in Migration Step 0.

Shared infrastructure for disk access. Provides synchronous and asynchronous file operations.

> **Current implementation:** `storage/io/file_pool.go`. Will be promoted to `vfs/` as-is in Step 0, then fixed in Step 3.

```go
package vfs

type FileHandle interface {
    ReadAt(p []byte, off int64) (n int, err error)
    WriteAt(p []byte, off int64) (n int, err error)
    Sync() error
    Close() error
}

type FilePool struct {
    // Current implementation exists in storage/io/file_pool.go.
    // Planned fixes (Step 3):
    //   - Replace sync.Mutex with sync.RWMutex
    //   - Remove duplicate Get/GetFile methods
    //   - Fix partial read handling
}
```

### 4.3 Layer 3-A: Index Engine (index/)

Pure in-memory graph structure and search algorithms. The index engine does NOT know how to persist itself — it only exposes serialization methods that produce RecordBatches.

```go
package index

// HNSWIndex is the concrete type. No interface is defined here.
// "Accept interfaces, return structs" — interfaces are defined
// by consumers (vego/) when needed, not by providers (index/).
type HNSWIndex struct { ... }

func NewHNSW(config Config) *HNSWIndex

// Core operations — pure memory
func (h *HNSWIndex) Add(id int, vector []float32) error
func (h *HNSWIndex) Search(query []float32, k int, filter func(int) bool) []SearchResult
func (h *HNSWIndex) Delete(id int)

// Serialization — produces RecordBatches without knowing where they go.
// Current implementation returns a single batch; streaming via callback
// will be introduced when datasets exceed ~1M vectors (see note below).
func (h *HNSWIndex) MarshalNodes() (*core.RecordBatch, error)
func (h *HNSWIndex) MarshalConnections() (*core.RecordBatch, error)
func (h *HNSWIndex) MarshalMetadata() (*core.RecordBatch, error)
func (h *HNSWIndex) UnmarshalNodes(batch *core.RecordBatch) error
func (h *HNSWIndex) UnmarshalConnections(batch *core.RecordBatch) error
func UnmarshalMetadata(batch *core.RecordBatch) (*MetadataResult, error)
```

**Why callback-based streaming:**

- Memory-controlled: 1M vectors x 768 dims x 4 bytes = ~3GB would be a single RecordBatch without streaming.
- Aligns with columnar page boundaries: each batch maps to one or more Pages.
- Writer can process incrementally without buffering all batches.
- No Go version dependency (works on Go 1.21+; iter.Seq2 requires 1.23+).

**Why no interface at this layer:**

- Only one implementation exists today (HNSW).
- When IVF-PQ is added, the consuming layer (vego/) will define:

```go
// vego/searcher.go — defined by consumer
type Searcher interface {
    Search(query []float32, k int, filter func(int) bool) []SearchResult
}
```

This follows Go's "interfaces belong to consumers" principle.

### 4.4 Layer 3-B: Storage Engine (storage/)

Reorganized into 4 sub-packages with clear responsibilities.

#### storage/catalog/ — Metadata Management (new)

Extracted from the current `vego/storage.go`. Manages snapshot, ID mappings, and deletion vectors.

```go
package catalog

// Snapshot is the single source of truth for collection state.
// It is the ONLY commit point during flush — atomic update of this
// file guarantees crash safety.
type Snapshot struct {
    Version      int64
    NumRows      int64
    Schema       *core.Schema
    DataFile     string            // vectors.lance path
    IndexFiles   []string          // HNSW file paths
    DeletionFile string            // .dv file path
    CreatedAt    time.Time
    Metadata     map[string]string
}

// Transaction protocol for crash-safe writes.
func (s *Snapshot) BeginTransaction() string          // returns temp dir path
func (s *Snapshot) CommitTransaction(txnDir string) error  // atomic commit
func (s *Snapshot) AbortTransaction(txnDir string)    // cleanup on failure
func (s *Snapshot) RecoverFromCrash() error           // cleanup orphan txn dirs on startup

// IDMapping manages docID <-> nodeID <-> rowID mapping.
type IDMapping interface {
    DocToNode(docID string) (nodeID int, ok bool)
    NodeToDoc(nodeID int) (docID string, ok bool)
    NodeToRow(nodeID int) (rowID uint32, ok bool)
    Put(docID string, nodeID int)
    Delete(docID string)
    AllMappings() map[string]int
    io.WriterTo    // WriteTo(w io.Writer) (int64, error)
    io.ReaderFrom  // ReadFrom(r io.Reader) (int64, error)
}

// DeletionStore manages soft-delete state in memory.
// Persistence is controlled by the API layer (Option A).
// DeletionStore does NOT decide when or where to save —
// it only provides serialization capability.
type DeletionStore interface {
    MarkDeleted(rowID uint32)
    IsDeleted(rowID uint32) bool
    Count() int
    io.WriterTo    // WriteTo(w io.Writer) (int64, error) — full snapshot
    io.ReaderFrom  // ReadFrom(r io.Reader) (int64, error)
    // FlushDelta writes only the changes since last flush (optional optimization).
    // If not implemented (returns nil), API layer falls back to WriteTo.
    // Designed for Phase 6 WAL integration.
    FlushDelta(w io.Writer) error
}
```

**Why this sub-package exists:**

1. Current metadata management is scattered across `vego/storage.go`, mixed with columnar I/O.
2. Provides a natural extension point for Phase 6 WAL + MVCC.
3. The underlying format (JSON today) can be replaced with SQLite/BoltDB by changing only this package.

#### storage/column/ — Columnar Read/Write (existing, interface-refined)

```go
package column

type ColumnWriter interface {
    WriteRecordBatch(batch *core.RecordBatch) error
    Close() error
}

type ColumnReader interface {
    ReadRecordBatch() (*core.RecordBatch, error)
    ReadRow(rowIndex int) (*core.RecordBatch, error)  // O(1) via RowIndex
    Close() error
}
```

#### storage/encoding/ — Adaptive Encoding (existing, unchanged)

Adaptive codec selection: ZSTD, RLE, BitPacking, BSS, Dictionary.

#### storage/format/ — File Structure (existing, unchanged)

Header, Footer, PageIndex, Version, BlockCache definitions.

### 4.5 Layer 4: API / Orchestration (vego/)

Transforms from "both orchestration and implementation" to **pure orchestration**. This is the only layer that imports both `index/` and `storage/`.

```go
package vego

type Collection struct {
    index    *index.HNSWIndex           // Index engine (concrete type)
    snapshot *catalog.Snapshot          // Collection state metadata
    ids      catalog.IDMapping          // ID mapping
    dv       catalog.DeletionStore      // Deletion management
    writer   column.ColumnWriter        // Data write
    reader   column.ColumnReader        // Data read
    buffer   *WriteBuffer              // Write buffer (orchestration strategy)
    config   *Config
    dirty    bool                       // Tracks uncommitted changes
}
```

#### Orchestration: Insert

```go
func (c *Collection) Insert(doc *Document) error {
    // 1. Validate
    if err := doc.Validate(c.config.Dimension); err != nil {
        return err
    }
    // 2. Index engine: add vector
    nodeID := c.index.Add(doc.Vector)
    // 3. Metadata: record mapping
    c.ids.Put(doc.ID, nodeID)
    // 4. Write buffer: accumulate data
    c.buffer.Append(doc)
    c.dirty = true
    // 5. Auto flush
    if c.buffer.ShouldFlush() {
        return c.flush()
    }
    return nil
}
```

#### Orchestration: Search

```go
func (c *Collection) Search(query []float32, k int) []SearchResult {
    // 1. Index engine: ANN search (with DV filter injected)
    candidates := c.index.Search(query, k*2, c.dv.IsDeleted)
    // 2. Map: nodeID -> docID
    // 3. Storage engine: fetch document data
    // 4. Return results
}
```

#### Orchestration: Delete

```go
func (c *Collection) Delete(docID string) error {
    nodeID, ok := c.ids.DocToNode(docID)
    if !ok {
        return ErrDocumentNotFound
    }
    rowID, _ := c.ids.NodeToRow(nodeID)
    c.dv.MarkDeleted(rowID)     // Memory operation
    c.ids.Delete(docID)          // Memory operation
    c.dirty = true               // Mark dirty

    // Does NOT immediately persist. Triggered by:
    //   - Explicit collection.Save()
    //   - Auto-compact threshold
    //   - DB.Close()
    return nil
}
```

#### Orchestration: Flush (Transactional)

```go
func (c *Collection) flush() error {
    txnDir := c.snapshot.BeginTransaction()

    // 1. Write data to txnDir
    dataPath := txnDir + "/vectors.lance"
    if err := c.writeData(dataPath); err != nil {
        c.snapshot.AbortTransaction(txnDir)
        return err
    }

    // 2. Write index to txnDir (streaming)
    indexPath := txnDir + "/index/"
    if err := c.writeIndex(indexPath); err != nil {
        c.snapshot.AbortTransaction(txnDir)
        return err
    }

    // 3. Write metadata to txnDir
    if err := c.writeMetadata(txnDir); err != nil {
        c.snapshot.AbortTransaction(txnDir)
        return err
    }

    // 4. Atomic commit: update snapshot (the ONLY commit point)
    if err := c.snapshot.CommitTransaction(txnDir); err != nil {
        return err
    }

    c.dirty = false
    return nil
}
```

**Crash safety guarantees:**

- Crash during step 1/2/3: txnDir is orphaned. `RecoverFromCrash()` cleans it up on next startup.
- Crash during step 4 (before snapshot rename): same as above.
- Crash during step 4 (after snapshot rename): new data is in place, consistent.
- **snapshot.json is the last file renamed** — it is the sole commit point.

#### Responsibility Migration from Current Code

| Current location | New location |
|---|---|
| writeBuffer + flush strategy | Stays in API layer (`vego/buffer.go`) |
| metaStore (ID mapping) | `storage/catalog/id_mapping.go` |
| deletionVector management | `storage/catalog/deletion_store.go` |
| cachedRowIndex | Internal to `storage/column/reader.go` |
| version (format version) | `storage/format/version.go` |
| blockCache | `storage/format/blockcache.go` (unchanged) |
| columnar R/W calls | `storage/column/` exposes interface directly |

### 4.6 Layer 5: Application Service (memory/)

Unchanged. Depends on Layer 4 API. Provides Agent Memory functionality.

Key sub-modules:

| Sub-module | Responsibility |
|---|---|
| Ingest pipeline | Message → LLM fact extraction → embedding → storage |
| Reconcile | Deduplication and conflict resolution (ADD/UPDATE/DELETE/NOOP) |
| Hybrid search | HNSW vector search + BM25 text search + RRF score fusion |
| Temporal normalization | Relative time expressions → absolute timestamps |
| Embedding | Configurable concurrency: serial / parallel workers / batch |
| LLM client | JSON mode three-state control, multi-format response parsing |

---

## 5. Key Design Decisions

### Decision 1: Keep `storage/` naming, reorganize internally

**Choice:** Retain `storage/` package path; add `catalog/` sub-package internally. Promote shared infrastructure (`storage/arrow/` → `core/`, `storage/io/` → `vfs/`) to top-level — these are not part of the storage engine.

**Rationale:**
- Go module paths, once recorded in `go.sum`, have inertia.
- `storage/` is more semantically accurate than `store/` (avoids confusion with "shop").
- The benefit of renaming is too small to justify a breaking change.
- `core/` and `vfs/` are general-purpose infrastructure used by both `index/` and `storage/`. They were incorrectly nested under `storage/` — promoting them is a correction, not a rename of the storage engine.

### Decision 2: No premature interface for index engine

**Choice:** API layer holds `*index.HNSWIndex` directly. No interface defined in `index/`.

**Rationale:**
- Go idiom: "Accept interfaces, return structs."
- Only one implementation exists. Defining an interface now would be speculative.
- When IVF-PQ is added, the consumer (`vego/`) defines the interface it needs.

### Decision 3: Callback-based streaming serialization

**Choice:** `MarshalNodes(batchSize int, emit func(*RecordBatch) error) error`

**Rationale:**
- Avoids multi-GB single RecordBatch for large datasets.
- Aligns with columnar page boundaries naturally.
- Writer processes incrementally without buffering.
- No Go version dependency (callback works on any Go version; `iter.Seq2` requires 1.23+).
- `batchSize` should be auto-calculated by the API layer (target: single batch < 64MB based on dimension), with override available for extreme cases.

### Decision 4: Catalog manages transactional flush

**Choice:** `catalog.Snapshot` owns the transaction protocol (Begin → Write All → Commit).

**Rationale:**
- All file writes go to a temp directory first.
- snapshot.json atomic rename is the sole commit point.
- On crash, orphan txnDirs are cleaned up by `RecoverFromCrash()`.
- This mirrors Lance's `_transactions/*.txn` + atomic manifest write pattern, simplified for single-file scenarios.

### Decision 5: DeletionStore — API layer controls persistence

**Choice:** Option A. DeletionStore manages only in-memory state + serialization. The API layer decides when and where to persist.

**Rationale:**
- Aligns with "API layer is pure orchestration" principle.
- DeletionStore cannot know the dirty state of other components.
- Save timing is coupled with data flush and index flush — must be unified by the orchestration layer.
- `io.WriterTo` / `io.ReaderFrom` interfaces compose well with Go stdlib (bufio, compress, etc.).

---

## 6. Concurrency Model

```
Lock Hierarchy (must acquire in this order):

  DB Level (outermost)
  ├── db.mu (RWMutex)
  │   ├── RLock: Collections()
  │   └── Lock:  Collection(), DropCollection(), Close()
  │
  Collection Level (middle)
  ├── c.mu (RWMutex)
  │   ├── RLock: Get(), Search(), Count(), Stats()
  │   └── Lock:  Insert(), Delete(), Update(), Save(), flush()
  │
  Index Level (HNSW internal)
  ├── h.globalLock (RWMutex)
  │   ├── RLock: Search, SearchWithDV, Len
  │   └── Lock:  Add (blocks all concurrent Search/Len)
  ├── h.mu (Mutex) — protects RNG only (acquired inside Add)
  │
  Storage Level (innermost)
  └── Catalog, ColumnWriter/Reader have their own internal locks
```

Rule: Always acquire locks in order DB → Collection → Index → Storage. Never hold an inner lock while requesting an outer lock.

---

## 7. Roadmap Alignment

This layered architecture directly supports the planned evolution:

| Roadmap Phase | Affected Layer | Scope |
|---|---|---|
| Phase 2: I/O Scheduler | Layer 2 (vfs/) | Rewrite I/O package |
| Phase 2: Blob Storage | Layer 3-B (format/ + column/) | Add Blob page type |
| Phase 3: Zone Map | Layer 3-B (encoding/) | Compute min/max during encoding, store in PageHeader |
| Phase 3: IVF-PQ | Layer 3-A (index/) | New index implementation |
| Phase 4: Prefetch / MiniBlock | Layer 2 (vfs/) + Layer 3-B (format/) | Smart prefetch + sub-page structure |
| Phase 5: Cloud-native | Layer 2 (vfs/) | Add S3/GCS backend (transparent to upper layers) |
| Phase 6: WAL | Layer 3-B (catalog/) | Add WAL file management alongside Snapshot |
| Phase 6: MVCC | Layer 3-B (catalog/) + Layer 4 (vego/) | Multi-version Snapshot, snapshot reads |
| Phase 6: Scalar Index | Layer 3-A (index/) | Add BTree/Bloom index implementations |

**Key insight:** `storage/catalog/` provides a clean extension point for Phase 6 — evolving from "JSON file management" to "WAL + MVCC version management" without affecting other layers.

---

## 8. Migration Strategy

The refactoring is executed in 4 incremental steps. Each step keeps all tests green and can be merged independently.

### Step 0: Promote `core/` and `vfs/` to top-level packages

- Move `storage/arrow/` → top-level `core/`. Package name changes from `arrow` to `core`.
- Move `storage/io/` → top-level `vfs/`. Package name changes from `io` to `vfs`.
- Update all type references: `arrow.Schema` → `core.Schema`, `arrow.Array` → `core.Array`, etc.
- Both `index/` and `storage/` depend on these packages, so they must not live under `storage/`.
- **Impact:** ~4 packages, ~15 files. Compiler catches all missed references.
- **Risk: Low** — pure refactoring (package rename + type reference update), no behavior change.

### Step 1: Extract `storage/catalog/`

- Extract Snapshot (collection state metadata), IDMapping, and DeletionStore from `vego/storage.go` into `storage/catalog/`.
- Move metadataStore logic (version, schema, file paths) into `catalog.Snapshot`.
- `vego/storage.go` calls catalog interfaces instead of managing state directly.
- **Risk: Low** — pure refactoring, no behavior change.

### Step 2: Index engine drops persistence ✅ Completed

> **Prerequisite:** Step 0 must be completed first — `index/storage.go` currently imports `storage/arrow/` and `storage/column/`, which must become `core/` before removal.

- Add `MarshalNodes` / `MarshalConnections` / `MarshalMetadata` to `*index.HNSWIndex`, plus `UnmarshalNodes` / `UnmarshalConnections` / `UnmarshalMetadata`.
- Remove `index/storage.go` (direct Lance file writes + illegal imports of `storage/column/` and `storage/encoding/`).
- Migrate schema construction logic (`SchemaForNodes`, `SchemaForConnections`, `SchemaForMetadata`) to depend only on `core/`.
- API layer (`vego/index_persist.go`) takes over persistence orchestration using `storage/column/`.
- **Status:** Completed. `index/` has zero `storage/` dependencies (only `core/`), all tests pass.

### Step 3: I/O layer fixes + column interface

- FilePool: replace Mutex with RWMutex.
- Remove duplicate Get/GetFile methods.
- Fix partial read handling.
- Formalize `ColumnWriter` / `ColumnReader` interfaces in `storage/column/`.
- **Risk: Low** — targeted fixes.

---

## 9. Disk Layout

After the refactoring, the on-disk layout for a single collection:

```
collection_path/
├── snapshot.json          # The ONLY commit point (atomic rename)
│   ├── version
│   ├── data_file: "vectors.lance"
│   ├── index_files: ["index/nodes.lance", "index/connections.lance"]
│   ├── deletion_file: "deletions.dv"
│   └── metadata
├── vectors.lance          # Lance columnar format (data)
│   ├── Header (8KB)
│   ├── Pages (compressed columns: id_hash, vector, timestamp, metadata)
│   └── Footer (PageIndexList)
├── mappings.json          # docID <-> nodeID bidirectional mapping
├── deletions.dv           # Deletion vector (bitset)
├── index/                 # HNSW index (Lance format)
│   ├── nodes.lance        # id + vector + level
│   └── connections.lance  # node_id + layer + neighbor_id
└── .txn_*/                # Temporary transaction dirs (cleaned on recovery)
```

**HNSW persistence format — performance gate:**

> Cold-start loading of 1M nodes must complete within 5 seconds. If Lance columnar format cannot meet this threshold (due to row-by-row adjacency list reconstruction causing excessive random I/O), the design allows fallback to a custom binary format (flat adjacency arrays + mmap). The `MarshalNodes`/`UnmarshalNodes` interface is format-agnostic by design — it does not mandate Lance.

---

## 10. Design Principles Summary

1. **Single-direction dependencies.** Upper layers depend on lower layers, never the reverse. No circular imports.
2. **Index engine is persistence-agnostic.** It produces/consumes RecordBatches. It never touches the filesystem.
3. **API layer is pure orchestration.** It coordinates index + storage + catalog. It does not implement algorithms or formats.
4. **Interfaces belong to consumers.** Providers export concrete types. Consumers define the interfaces they need.
5. **Crash safety through atomic snapshot.** All writes go to temp dirs first. Snapshot file rename is the sole commit point.
6. **Streaming over buffering (future).** Large data will be processed via callbacks (emit functions) once datasets reach million-scale. Current implementation uses single-batch for simplicity.
7. **Defer abstraction until needed.** Three similar lines of code is better than a premature interface. Add abstractions when the second implementation arrives.
