# Introducing Vego: A Lightweight Vector Search Engine Written in Pure Go

> *TL;DR: I built Vego — an embeddable vector search engine with zero CGO dependencies, a self-developed columnar storage engine, and HNSW-based similarity search. It's designed for AI agents, local RAG, and edge devices where deploying a full vector database is overkill.*

---

## The Problem

When I started building AI agent workflows, I hit a recurring pain point: **every time I needed vector search, I had to deploy a separate vector database.**

For a local RAG pipeline with a few thousand documents, spinning up Milvus or Weaviate felt like driving a semi-truck to the grocery store. I wanted something I could `go get`, call a few functions, and have production-grade vector search — all within a single binary, no Docker, no external services.

That's why I built **Vego**.

---

## What is Vego?

[Vego](https://github.com/wzqhbustb/vego) is a lightweight, embeddable vector search engine written in pure Go. Think of it as **SQLite for vector search** — it runs inside your application process, stores data to local files, and requires zero infrastructure.

Key characteristics:

- **Pure Go, zero CGO** — cross-compile anywhere Go runs, single binary
- **Self-developed columnar storage** — Lance-compatible format with adaptive encoding
- **HNSW search** — millisecond-level approximate nearest neighbor queries
- **Document-oriented API** — not just vectors, but documents with metadata and filtering
- **Production-grade** — crash-safe writes, auto-compaction, deletion vectors

```bash
go get github.com/wzqhbustb/vego
```

That's all you need. No Dockerfile. No `docker-compose.yml`. No cluster configuration.

---

## A Quick Taste

Here's how Vego feels in practice:

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/wzqhbustb/vego/vego"
)

func main() {
    // Open a database (creates directory if needed)
    db, err := vego.Open("./my_db", vego.WithDimension(128))
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Get or create a collection
    coll, _ := db.Collection("documents")

    // Insert a document with metadata
    ctx := context.Background()
    doc := &vego.Document{
        ID:     "doc-001",
        Vector: embedding, // your 128-dim embedding
        Metadata: map[string]interface{}{
            "title":  "Introduction to AI",
            "author": "Alice",
            "tags":   []string{"ai", "ml"},
        },
    }
    coll.InsertContext(ctx, doc)

    // Search — returns ranked results with full documents
    results, _ := coll.SearchContext(ctx, queryVector, 10)
    for _, r := range results {
        fmt.Printf("%s (distance: %.4f)\n", r.Document.ID, r.Distance)
    }
}
```

The API is intentionally simple: **Open → Collection → Insert → Search**. If you've used MongoDB or Redis, this will feel familiar.

For advanced users, there's also a low-level Index API that gives direct access to the HNSW graph:

```go
index := hnsw.NewHNSW(hnsw.Config{
    Dimension:    128,
    Adaptive:     true,       // auto-tune M and efConstruction
    ExpectedSize: 10000,
    DistanceFunc: hnsw.CosineDistance,
})

id, _ := index.Add(vector)
results, _ := index.Search(query, 10, 0)
```

---

## Architecture: Three Layers

Vego is built as a clean three-layer system:

```
┌──────────────────────────────────────────────────┐
│            Collection API  (vego/)               │
│     Documents · Metadata · Search · Filtering    │
└────────────────┬─────────────────┬───────────────┘
                 │                 │
    ┌────────────▼──────┐  ┌──────▼───────────────┐
    │   HNSW Index      │  │  Columnar Storage    │
    │   (index/)        │  │  (storage/)          │
    │                   │  │                      │
    │  Multi-layer      │  │  Arrow arrays        │
    │  graph search     │  │  Adaptive encoding   │
    │  Deletion vector  │  │  Lance file format   │
    │  Adaptive params  │  │  Async I/O           │
    └───────────────────┘  └──────────────────────┘
```

### Layer 1: Collection API

The top layer provides a document-oriented interface. You work with `Document` objects that carry an ID, a vector, and arbitrary metadata. The API handles all the plumbing — mapping document IDs to HNSW node IDs to storage row IDs, managing the write buffer, flushing to disk.

It also supports:
- `context.Context` for timeout and cancellation
- Metadata filtering on search results
- Batch operations for bulk inserts
- Auto-compaction when deletion rate exceeds a threshold (default 30%)

### Layer 2: HNSW Index

The index layer implements the [HNSW algorithm](https://arxiv.org/abs/1603.09320) — a multi-layer navigable small world graph for approximate nearest neighbor search. Key design choices:

- **Adaptive configuration**: Set `Adaptive: true` and `ExpectedSize`, and Vego automatically tunes `M`, `efConstruction`, and `maxLevel` based on your data characteristics.
- **Soft deletion**: Uses [RoaringBitmap](https://roaringbitmap.org/) to mark deleted nodes without modifying the graph structure. This keeps deletion O(1) and avoids expensive graph reconstruction.
- **Three distance metrics**: L2 (Euclidean), Cosine, and Inner Product.

### Layer 3: Columnar Storage Engine

This is the part I'm most proud of — a **self-developed Lance-compatible columnar storage engine** with zero CGO dependencies.

Why build a custom storage engine instead of using Apache Arrow's C library? Three reasons:

1. **Zero CGO** — Arrow's Go binding requires CGO, which breaks cross-compilation and complicates deployment
2. **Vector-specific optimizations** — I can tune page sizes, encoding selection, and I/O patterns for vector workloads
3. **Smaller binary** — no C toolchain overhead

The storage engine has its own sub-architecture:

```
Arrow Arrays → Adaptive Encoding → Lance Pages → File I/O
     │              │                    │            │
  Zero-copy    5 codecs auto-     Versioned      Async scheduler
  columnar     selected by        format         with priorities
  format       data statistics    (V1.0-V1.2)
```

---

## The Storage Engine: A Deeper Look

Since the storage layer is the most technically interesting part (and takes up ~30,000 lines of the codebase), let me go deeper.

### Adaptive Encoding

Vego doesn't use a fixed compression codec. Instead, it analyzes data statistics at write time and selects the optimal encoding:

| Encoding | When Selected | Best For |
|----------|--------------|----------|
| **RLE** | Run ratio < 0.1 | Timestamps, sequential IDs |
| **Dictionary** | Cardinality < 10% | Category labels, tags |
| **BitPacking** | Values fit in ≤16 bits | Small integers |
| **BSS** (Byte Stream Split) | Float32 arrays | Vector embeddings |
| **Zstd** | Default fallback | General purpose |

This means you don't have to think about compression — Vego picks the right codec for each column automatically.

### O(1) Document Retrieval with RowIndex

Early versions of Vego had a painful performance cliff: retrieving a single document required scanning the entire column file — O(N) complexity. For 10,000 documents, a simple `Get("doc-id")` took ~2 seconds.

I solved this with **RowIndex**, a hash-based index that maps document ID hashes to file row offsets:

| Dataset Size | Before (O(N) scan) | After (O(1) RowIndex) | Speedup |
|-------------|--------------------|-----------------------|---------|
| 100 docs | ~20ms | ~0.2ms | **100x** |
| 1,000 docs | ~200ms | ~1ms | **200x** |
| 10,000 docs | ~2s | ~5ms | **400x** |

The RowIndex is stored as a special page in the Lance file, so it persists across restarts with zero overhead. V1.2 format files have it enabled by default, and legacy V1.0 files are auto-upgraded on flush.

### Three-Level Cache

To minimize disk I/O, Vego implements a three-level cache hierarchy:

1. **L1 — Write Buffer**: In-memory buffer for documents not yet flushed (~1,000 docs capacity). Provides strong consistency for read-after-write.
2. **L2 — Document Cache**: Deserialized document objects (~10,000 docs). Version-keyed to avoid invalidation overhead.
3. **L3 — Block Cache**: LRU page cache (default 64MB). Uses 64 shard locks to reduce contention by ~64x under concurrent access.

```go
// Share a BlockCache across multiple collections
cache := format.NewBlockCache(64 * 1024 * 1024) // 64 MB

db, _ := vego.Open("./mydb",
    vego.WithDimension(768),
    vego.WithBlockCache(cache),
)
```

### Crash Safety

Every write follows the **temp file → fsync → rename** pattern:

1. Write to a temporary file
2. `fsync` to ensure data hits disk
3. Atomically rename to the final path

If the process crashes mid-write, the incomplete temp file is automatically cleaned up on the next startup. No WAL needed for basic crash safety.

---

## Performance

All benchmarks run on Apple M3 Max, macOS ARM64, Go 1.23.

### HNSW Search

| Dataset | Dims | Recall | P99 Latency | QPS |
|---------|------|--------|-------------|-----|
| 10K vectors | 128 | **95.9%** | 975µs | ~1,000 |
| 100K vectors | 128 | **75.4%** | 3.17ms | 419 |
| 10K vectors | 768 | **74.6%** | 4.67ms | 255 |

Sub-millisecond P99 on 10K datasets. For 100K vectors, recall is actively being improved.

### Storage Layer

| Operation | Throughput |
|-----------|-----------|
| Column write | ~330 MB/s |
| Column read | ~250 MB/s |
| Float32 access (zero-copy) | 1.3 ns/op |
| Get() with RowIndex (10K docs) | ~1ms |

### Encoding Speed

| Codec | Encode | Decode |
|-------|--------|--------|
| RLE | 10 µs | 39 µs |
| Zstd | 23 µs | 62 µs |
| BitPacking | 50 µs | 88 µs |
| BSS | 48 µs | 48 µs |

---

## Use Case: Local RAG

The use case I'm most excited about is **local RAG** (Retrieval-Augmented Generation). Combine Vego with a local LLM (via Ollama or llama.cpp), and you get a fully private, fully offline knowledge base:

```go
// Index your knowledge base
kb, _ := db.Collection("knowledge_base")

for _, doc := range documents {
    kb.InsertContext(ctx, &vego.Document{
        ID:     doc.ID,
        Vector: embed(doc.Content),  // your embedding model
        Metadata: map[string]interface{}{
            "content":  doc.Content,
            "source":   doc.Source,
            "category": doc.Category,
        },
    })
}

// At query time: retrieve → augment → generate
results, _ := kb.SearchContext(ctx, embed(userQuery), 5)

// Feed retrieved documents as context to your LLM
context := buildPrompt(results)
answer := llm.Generate(context + userQuery)
```

No API keys. No data leaving your machine. No vector database to manage.

Other use cases that work well:
- **AI Agent memory** — give agents persistent, searchable long-term memory
- **Edge/IoT** — semantic matching on resource-constrained devices
- **Microservice embedding** — add vector search to any Go service without infrastructure changes

---

## Honest Limitations

I believe in being upfront about what Vego can't do (yet):

1. **Recall at scale**: 75% recall on 100K datasets — actively improving, but not yet competitive with mature solutions at that scale.
2. **Memory allocation**: Search operations allocate significant memory under heavy load. `sync.Pool` helps for high-concurrency scenarios.
3. **No incremental persistence**: `Save()` performs a full export. Incremental save is under investigation.
4. **Async I/O**: Current implementation shows linear latency increase with concurrency. Synchronous I/O (default) is recommended for now.
5. **No WAL/MVCC**: Crash-safe but not transactional. Fine for embedded use; not for multi-writer scenarios.
6. **Limited distance functions**: L2, Cosine, InnerProduct only. Hamming and Jaccard are in the backlog.

If you need 99%+ recall on millions of vectors, distributed deployment, or strong transactional guarantees — use Milvus, Weaviate, or Qdrant. Vego targets a different niche: **lightweight, embedded, zero-dependency vector search**.

---

## What's Next

Vego is currently at the end of Phase 0 (API foundation) heading into Phase 1 (storage hardening). The roadmap includes:

- **Near-term**: Full deletion vector integration, compaction strategy finalization, I/O scheduler optimization
- **Medium-term**: Zone Map filtering, IVF-PQ for larger datasets, blob storage
- **Long-term**: Cloud-native object store support, multi-modal vectors, WAL/MVCC for enterprise workloads

The full 6-phase roadmap is in [ROADMAP.md](https://github.com/wzqhbustb/vego/blob/main/ROADMAP.md).

---

## Try It

```bash
go get github.com/wzqhbustb/vego
```

Check out the [examples](https://github.com/wzqhbustb/vego/tree/main/examples) directory for runnable code — from basic usage to a complete RAG demo.

If you find it useful, a ⭐ on [GitHub](https://github.com/wzqhbustb/vego) would mean a lot. Issues, feedback, and contributions are all welcome.

---

*Vego is licensed under Apache 2.0. Built with ❤️ in Go.*
