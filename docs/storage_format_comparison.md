# Why LanceDB Uses Page-Based Storage Instead of LSM Tree

> A technical analysis of storage format design decisions for vector databases

## Executive Summary

LanceDB (and by extension, Vego) adopts a **page-based columnar storage format with Deletion Vectors** rather than the widely-used LSM Tree architecture. This document explains the technical rationale behind this decision, comparing the trade-offs between LSM Tree and Lance's approach for AI/vector workloads.

---

## 1. Architectural Comparison

| Dimension | LSM Tree | Lance Page-Based |
|-----------|----------|------------------|
| **Write Pattern** | High-frequency random writes | Batch-oriented, append-heavy |
| **Read Pattern** | Point lookups | Columnar scans, ANN search, range queries |
| **Data Layout** | Row-oriented (SSTables) | Column-oriented (Pages) |
| **Update Strategy** | Multi-version + compaction | Copy-on-Write + Deletion Vector |
| **Compaction** | Frequent, foreground/background | Infrequent, scheduled |
| **Space Amplification** | 1.1x - 3x | Controllable (post-DV compaction) |
| **Write Amplification** | 10x - 100x (compaction) | ~1x (append + DV marking) |
| **Read Amplification** | O(log N) levels | O(1) direct page access |

---

## 2. Workload Characteristics

### 2.1 LSM Tree: Optimized for OLTP

LSM Trees excel in scenarios with:
- High-throughput random key-value writes
- Frequent updates to small records
- Read-heavy with strong locality
- Need for fast point lookups

```
Typical LSM Flow:
Write → WAL → MemTable → Immutable → Flush → L0 SSTable 
     → Compaction → L1/L2/L3 SSTables
```

### 2.2 Vector Databases: AI/Analytical Workloads

Vector databases like LanceDB face different challenges:
- **Bulk ingestion**: Import millions of vectors at once (e.g., embedding datasets)
- **Approximate search**: ANN queries don't need exact point lookups
- **Columnar access**: Project only required dimensions for filtering
- **Append-mostly**: Updates are rare; deletions use soft-delete patterns

```
Lance Flow:
Batch Write → Column Pages → Deletion Vector (for updates)
     → Background Compaction (scheduled, infrequent)
```

---

## 3. The Vector Data Problem

### 3.1 Size Matters

High-dimensional vectors are large:

| Dimension | Float32 Size | Typical Use Case |
|-----------|-------------|------------------|
| 384 | 1.5 KB | Sentence embeddings (all-MiniLM) |
| 768 | 3 KB | BERT-style embeddings |
| 1536 | 6 KB | OpenAI text-embedding-3-large |
| 1024×768 (image) | 3 MB | Vision transformer patches |

**LSM Tree Challenge**: 
- Frequent compaction rewriting 3KB-6KB entries = massive I/O amplification
- HNSW/IVF indexes don't naturally fit SSTable structure
- Multiple versions of large vectors quickly exhaust storage

**Lance Solution**:
- Columnar pages: Vectors stored contiguously for SIMD efficiency
- Deletion Vector: Bitmask marks deleted rows (bytes vs. rewriting gigabytes)
- Immutable pages: New data appended to new pages, old pages untouched

### 3.2 ANN Index Integration

```go
// LSM + HNSW: Awkward fit
// Each compaction changes row IDs → requires HNSW rebuild

// Lance + HNSW: Natural fit
// Page locations stable → HNSW stores (pageID, offset) references
// Deletion Vector filters results without rebuilding index
```

---

## 4. Multimodal Storage Requirements

Modern AI applications handle diverse data types:

| Data Type | Size | Access Pattern | Storage Strategy |
|-----------|------|---------------|------------------|
| Embeddings | KB | Random access (ANN) | Inline in Page |
| Metadata | B-KB | Filter/scan | Columnar Page |
| Images | 100KB-10MB | Lazy loading | Pack file |
| Videos | 10MB-1GB | Streaming | Dedicated Blob file |

### 4.1 Lance's Tiered Blob Storage

Lance supports "**logically unified, physically separated**" storage:

```go
// Three-tier blob storage (from Vego roadmap)
type BlobStorage interface {
    // Tier 1: Inline (< 64KB) - stored directly in page
    // Tier 2: Packed (64KB - 4MB) - stored in .pack sidecar files
    // Tier 3: Dedicated (> 4MB) - stored in separate .blob files
}

// Lazy loading API
func (c *Collection) TakeBlobs(column string, ids []string) ([]BlobFile, error)
```

**Why LSM Can't Do This**:
- SSTables are self-contained; splitting data across files breaks assumptions
- LSM compaction expects to rewrite entire records atomically
- No native support for "read metadata first, load blob lazily"

---

## 5. Performance Trade-offs

### 5.1 Write Performance

| Scenario | LSM Tree | Lance |
|----------|----------|-------|
| Sequential bulk load | Good | Excellent |
| Random single writes | Excellent | Good |
| Large value updates | Poor (high WA) | Good (DV marking) |
| Vector updates | Very Poor | Good |

**Write Amplification (WA)**:
- LSM: 10-100x (repeated compaction rewrites)
- Lance: ~1x (append + occasional DV compaction)

### 5.2 Read Performance

| Query Type | LSM Tree | Lance |
|------------|----------|-------|
| Point lookup | Excellent | Good |
| Range scan | Good | Excellent (columnar) |
| Full scan | Poor | Excellent (vectorized) |
| ANN search | Poor (needs extra index) | Excellent (native HNSW/IVF) |
| Projection pushdown | Not applicable | Excellent (read only needed columns) |

### 5.3 Space Efficiency

```
LSM Space Amplification Formula:
SA = (size of all versions) / (size of live data)
Typical: 1.1x - 3x depending on compaction strategy

Lance Space Amplification:
SA = (live data + deletion vector + uncompacted pages) / (live data)
Typical: 1.0x - 1.3x (compaction reclaims DV-marked space)
```

---

## 6. Lance's Update Strategy: Copy-on-Write + Deletion Vector

### 6.1 The Deletion Vector (DV)

A bit-packed structure marking logically deleted rows:

```go
type DeletionVector interface {
    Contains(rowID uint32) bool    // O(1) check
    Set(rowID uint32)              // O(1) mark deleted
    Count() int                    // Deleted count
    Serialize() ([]byte, error)   // Persist to .del file
    Deserialize([]byte) error
}
```

### 6.2 Update Flow

```
Update Operation:
┌─────────────────────────────────────────────────────────┐
│ 1. Read old document → get oldRowID                     │
│    (Page location is stable, no need to update index)   │
├─────────────────────────────────────────────────────────┤
│ 2. Mark oldRowID deleted in DV                          │
│    (O(1) bitmap operation, no page rewrite)             │
├─────────────────────────────────────────────────────────┤
│ 3. Append new document → get newRowID                   │
│    (Append to new page, update HNSW with new point)     │
├─────────────────────────────────────────────────────────┤
│ 4. Search automatically filters via DV                  │
│    (HNSW returns candidates, filtered by DV bitmask)    │
└─────────────────────────────────────────────────────────┘
```

### 6.3 Compaction Strategy

Unlike LSM's forced compaction:

| Trigger | LSM | Lance |
|---------|-----|-------|
| Frequency | Continuous/Level-based | Scheduled/On-demand |
| Scope | Entire levels | Selected fragments |
| I/O Pattern | Massive sequential reads/writes | Targeted page rewrites |
| User Impact | Latency spikes | Background, throttled |

```
Lance Compaction:
- Trigger: DV deletion ratio > 30% (configurable)
- Action: Rewrite pages excluding deleted rows
- Benefit: Reclaim space without reindexing
```

---

## 7. When to Use Which?

### Choose LSM Tree when:
- ✅ High-throughput key-value writes (e.g., caching, sessions)
- ✅ Frequent small updates to records
- ✅ Strong consistency requirements per key
- ✅ Point lookup dominant workload

### Choose Lance when:
- ✅ Vector/analytical workloads (embeddings, feature stores)
- ✅ Batch ingestion with occasional updates
- ✅ Columnar projection and filtering needed
- ✅ Multimodal data (images, videos alongside vectors)
- ✅ ANN search is primary query pattern

---

## 8. Summary

| Factor | Winner | Rationale |
|--------|--------|-----------|
| **Vector storage** | Lance | Columnar pages + DV avoid rewrite amplification |
| **Update efficiency** | Lance | DV marking vs. full record rewrite |
| **Bulk load** | Tie | Both optimized for sequential writes |
| **Point lookup** | LSM | SSTable bloom filters + level skipping |
| **ANN search** | Lance | Native HNSW/IVF with stable page references |
| **Multimodal** | Lance | Tiered blob storage with lazy loading |
| **Operational simplicity** | Lance | Fewer tuning knobs, predictable compaction |

### Key Insight

> LSM Tree optimizes for **write-heavy, key-value, OLTP** workloads.
> Lance optimizes for **read-heavy, columnar, AI/Analytics** workloads.

For vector databases, the choice is clear: the overhead of LSM compaction on large vector data outweighs its benefits, while Lance's page-based columnar format with Deletion Vectors provides the right balance of write efficiency, read performance, and storage economics for AI applications.

---

## References

1. [Lance Format Specification](https://lancedb.github.io/lance/format.html)
2. [RocksDB Wiki: LSM Overview](https://github.com/facebook/rocksdb/wiki/RocksDB-Overview)
3. [DuckDB Storage Format](https://duckdb.org/docs/internals/storage.html)
4. [Vego Roadmap: Deletion Vector & Blob Storage](../ROADMAP.md)

---

*Last updated: 2026-02-26*
