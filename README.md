# Vego

[![Go Reference](https://pkg.go.dev/badge/github.com/wzqhbustb/vego.svg)](https://pkg.go.dev/github.com/wzqhbustb/vego)
[![Go Report Card](https://goreportcard.com/badge/github.com/wzqhbustb/vego)](https://goreportcard.com/report/github.com/wzqhbustb/vego)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

**Vego** is a lightweight vector search engine for AI agents and embedded applications, written in pure Go with zero CGO dependencies.

---

## 🎯 Why Vego?

### Core Advantages

1. **🚀 Ultra Lightweight**
   - Pure Go implementation, zero CGO dependencies
   - Single binary deployment
   - Minimal memory footprint (ideal for edge devices)

2. **⚡ High Performance**
   - HNSW algorithm with millisecond-level latency
   - >95% recall rate (compared to brute-force search)
   - Concurrent read/write support

3. **💾 Built-in Storage Engine**
   - Self-developed **Lance-compatible** columnar storage format
   - Adaptive encoding (ZSTD, BitPacking, RLE, BSS) with intelligent auto-selection
   - Zero-copy Arrow implementation for optimal memory efficiency
   - One-click save/load complete index
   - [📖 Learn more about the storage engine](STORAGE.md)

4. **🔧 Simple & Easy to Use**
   - Clean API design
   - Supports L2, Cosine, InnerProduct distance metrics
   - Automatic memory management, no complex configuration

---

## 📦 Use Cases

### 1. AI Agent Local Memory
Provide long-term memory capabilities for AI agents without external databases.

### 2. Edge Device Embedded Search
Ideal for IoT, mobile, and edge computing scenarios:
- Local semantic matching for smart home devices
- Anomaly detection for industrial equipment
- Image retrieval for drones

### 3. Microservice Embedded Vector Retrieval
Use directly within services without deploying separate vector databases:
- Candidate set retrieval for recommendation systems
- Document similarity detection
- Log anomaly detection

### 4. RAG (Retrieval-Augmented Generation)
Provide knowledge base retrieval capabilities for local LLMs:
- Local RAG combined with Ollama/Llama.cpp
- Intelligent Q&A for private documents
- Semantic search for code repositories

---

## 🚀 Quick Start

### Installation

```bash
go get github.com/wzqhbustb/vego
```

### Basic Example

```go
package main

import (
    "fmt"
    hnsw "github.com/wzqhbustb/vego/index"
)

func main() {
    // 1. Create index
    config := hnsw.Config{
        Dimension:      128,        // Vector dimension
        M:              16,         // Connections (affects recall and memory)
        EfConstruction: 200,        // Build parameter (higher = more accurate)
        DistanceFunc:   hnsw.L2Distance,
    }
    index := hnsw.NewHNSW(config)

    // 2. Add vectors
    vector := make([]float32, 128)
    // ... fill vector data ...
    id, err := index.Add(vector)
    if err != nil {
        panic(err)
    }
    fmt.Printf("Added vector with ID: %d\n", id)

    // 3. Search nearest neighbors
    query := make([]float32, 128)
    // ... fill query vector ...
    results, err := index.Search(query, 10, 0) // Return Top-10
    if err != nil {
        panic(err)
    }

    for _, r := range results {
        fmt.Printf("ID: %d, Distance: %.4f\n", r.ID, r.Distance)
    }
}
```

### Persistence Example

```go
// Save index to disk
err := index.SaveToLance("./my_index")
if err != nil {
    panic(err)
}

// Load index from disk
loadedIndex, err := hnsw.LoadHNSWFromLance("./my_index")
if err != nil {
    panic(err)
}

// Continue using loaded index
results, _ := loadedIndex.Search(query, 10, 0)
```

---

## 📖 API Documentation

### Creating an Index

```go
config := hnsw.Config{
    Dimension:      128,                      // Required: vector dimension
    M:              16,                       // Optional: max connections per layer (default 16)
    EfConstruction: 200,                      // Optional: search scope during build (default 200)
    DistanceFunc:   hnsw.CosineDistance,      // Optional: distance function
    Seed:           42,                       // Optional: random seed
}
index := hnsw.NewHNSW(config)
```

**Distance Function Options:**
- `hnsw.L2Distance` - Euclidean distance (default, for general use)
- `hnsw.CosineDistance` - Cosine distance (for text embeddings)
- `hnsw.InnerProductDistance` - Inner product distance (for semantic search)

### Adding Vectors

```go
id, err := index.Add(vector []float32) (int, error)
```

- Auto-incrementing IDs assigned
- Vectors are deep-copied; modifying original array won't affect index
- Thread-safe, supports concurrent additions

### Searching

```go
results, err := index.Search(
    query []float32,  // Query vector
    k int,            // Number of results to return
    ef int,           // Search scope (0 means use efConstruction)
) ([]SearchResult, error)
```

**Return Value:**
```go
type SearchResult struct {
    ID       int
    Distance float32
}
```

---

## 🏗️ Architecture

### HNSW Implementation

```
Layer 3: [Entry Point] ---------------------> [Node A]
            ↓                                    ↓
Layer 2: [Entry Point] -----> [Node B] -----> [Node A]
            ↓                     ↓              ↓
Layer 1: [Entry Point] -> [C] -> [B] -> [D] -> [A]
            ↓              ↓      ↓      ↓       ↓
Layer 0: [EP]->[C]->[E]->[F]->[B]->[G]->[D]->[H]->[A]  (Full Graph)
```

- **Multi-layer Graph Structure**: Fast coarse search in upper layers, precise search in lower layers
- **Probabilistic Layer Assignment**: Exponential distribution determines node levels, ensuring O(log N) query efficiency
- **Heuristic Edge Selection**: Considers both distance and neighbor diversity

### Storage Engine Architecture

Vego's storage layer is built on a **5-tier columnar architecture** designed specifically for vector workloads:

```
Application (HNSW Index)
    ↓
Column API (Read/Write)
    ↓
Arrow Subsystem (Zero-Copy Memory)     ← 1.2 ns/op access, no CGO
    ↓
Encoding Layer (Adaptive Compression)  ← Auto-selects ZSTD/RLE/BitPacking
    ↓
Format Layer (Lance-compatible)        ← 0.77-0.84x compression ratio
    ↓
I/O Layer (Sync/Async)                 ← 330 MB/s write, 250 MB/s read
```

**Key Design Decisions:**
- **Self-Developed Arrow**: Custom implementation without CGO dependencies
- **Adaptive Encoding**: Intelligent encoder selection based on data statistics
- **Dual I/O Modes**: Synchronous (production-ready) and Asynchronous (experimental)

👉 **[Full storage engine docs →](STORAGE.md)**

### Storage Engine Highlights

Vego features a **custom-built columnar storage engine** specifically designed for vector workloads:

```
┌─────────────────────────────────────────────────────────────┐
│                    Lance-compatible Format                   │
├─────────────────────────────────────────────────────────────┤
│  nodes.lance       →  ID + Vector (FixedSizeList) + Level   │
│  connections.lance →  NodeID + Layer + NeighborID          │
│  metadata.lance    →  M, Dimension, EntryPoint, etc.       │
└─────────────────────────────────────────────────────────────┘
```

**Key Features:**

| Feature | Performance | Benefit |
|---------|-------------|---------|
| **Zero-Copy Arrow** | 1.2 ns/op access | No CGO, minimal GC pressure |
| **Adaptive Encoding** | Auto-selects ZSTD/RLE/BitPacking | Optimal compression ratio |
| **Columnar Layout** | 0.77-0.84x compression | Efficient vector storage |
| **Dual I/O Modes** | 330 MB/s write, 250 MB/s read | Sync (stable) / Async (concurrent) |

**Supported Encodings:**
- **ZSTD**: General-purpose, high compression (23μs encode / 62μs decode)
- **BitPacking**: Narrow integers (up to 16-bit)
- **RLE**: Run-length encoding for sequential data
- **BSS**: Byte-stream split for Float32 vectors
- **Dictionary**: Low-cardinality data

👉 **[Read the full storage engine documentation →](STORAGE.md)**

---

## 📊 Performance Benchmarks

Test Environment: Intel Core i9-13950HX, Linux amd64, Go 1.23
---

### Storage Layer Performance

#### Memory Access (Arrow Layer)

| Operation | Latency | Allocations | Throughput |
|-----------|---------|-------------|------------|
| Int32 Array Access | 2.2 ns/op | 0 | ~450M ops/s |
| Float32 Array Access | 1.3 ns/op | 0 | ~770M ops/s |
| Buffer View (Zero-Copy) | 1.2 ns/op | 0 | ~830M ops/s |
| RecordBatch Creation | 220 μs | 21 allocs | - |

#### Encoding Performance

Encoding transforms Arrow arrays into compressed binary formats. Smaller is better for latency.

| Encoding | Encode | Decode | Compression | Best For |
|----------|--------|--------|-------------|----------|
| **RLE** | 10 μs | 39 μs | High | Sequential data (timestamps) |
| **ZSTD** | 23 μs | 62 μs | Very High | General purpose |
| **BitPacking** | 50 μs | 88 μs | Medium | Integer arrays |
| **BSS** | 48 μs | 48 μs | Medium | Float32 vectors |
| **Dictionary** | 92 μs | 50 μs | High* | Low cardinality data |

\* *Dictionary encoding degrades to ~650 μs for high cardinality data (>10K unique values)*

#### File I/O Throughput

Columnar file format (`.lance`) read/write performance:

| Operation | Rows | Columns | Throughput | Latency |
|-----------|------|---------|------------|---------|
| **Write** | 10K | 1 | ~330 MB/s | 12 μs |
| **Write** | 100K | 10 | ~280 MB/s | 1.4 ms |
| **Read** | 10K | 1 | ~250 MB/s | 16 μs |
| **Read** | 100K | 10 | ~220 MB/s | 1.8 ms |
| **Roundtrip** | 10K | 5 | - | 890 μs |

#### Concurrency & Async I/O

The storage layer supports both synchronous and asynchronous I/O:

| Mode | Concurrency | Latency (1 col) | Memory | Use Case |
|------|-------------|-----------------|--------|----------|
| **Sync** | 1 | 1.9 ms | 1.2 MB | Simple sequential access |
| **Async** | 1 | 2.0 ms | 2.4 MB | - |
| **Async** | 8 | 23 ms | 27 MB | Parallel column reads |
| **Async** | 16 | 35 ms | 52 MB | High parallelism |

> **⚠️ Note**: Current async I/O implementation shows linear latency increase with concurrency due to scheduling overhead. For most workloads, synchronous I/O is recommended until this is optimized.

---

### Comparative Summary

| Feature | Vego | Typical Vector DB (Milvus/Qdrant) |
|---------|------|-----------------------------------|
| **Memory Overhead** | ~200MB per 1M vectors (128-dim) | 500MB-1GB+ |
| **Query Latency (100K)** | <0.2ms P99 | 1-5ms |
| **Cold Start (Load)** | <2s (100K vectors) | 5-30s |
| **Binary Size** | Single static binary (~10MB) | 100MB+ with dependencies |
| **CGO Dependency** | None | Often required (FAISS, RocksDB) |

---

## 🗺️ Roadmap

Vego is actively evolving. For the detailed development roadmap including:

- **Phase 0**: Architecture Hardening (Foundation, benchmarks, error handling)
- **Phase 1**: MVP (Block Cache, async I/O, Table abstraction)
- **Phase 2**: Beta (CMO, projection pushdown, Zone Map)
- **Phase 3**: V1.0 Performance (MiniBlock, prefetch, SIMD)
- **Phase 4**: V1.5 Extreme (io_uring, vectorized execution)
- **Phase 5**: V2.0 Enterprise (WAL, MVCC, indexing, partitioning)

👉 See [ROADMAP.md](ROADMAP.md) for full details, timelines, and Architecture Decision Records (ADRs).

### Current Status

| Feature | Status | Milestone |
|---------|--------|-----------|
| HNSW index with configurable parameters | ✅ Available | v0.1 |
| Lance-compatible columnar storage | ✅ Available | v0.1 |
| ZSTD, BitPacking, RLE encoding | ✅ Available | v0.1 |
| Quantization support (PQ/SQ) | 🚧 Planned | v0.5 |
| Distributed index | 📋 Backlog | v1.0 |

---

## 🤲 Contributing

Contributions are welcome! Whether it's code, bug reports, or suggestions.

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Environment

```bash
# Clone repository
git clone https://github.com/wzqhbustb/vego.git
cd vego

# Run tests
go test ./...

# Run benchmarks
go test -bench=. ./index/

# Run recall test
go test -run TestRecall -v ./index/
```

---

## 📄 License

This project is licensed under the [Apache 2.0 License](LICENSE).

---

## 📚 References & Papers

This project is built on top of cutting-edge research in vector search and columnar storage. The following papers have significantly influenced our implementation:

### Small-World Network Theory

| Paper | Authors | Year | Contribution |
|-------|---------|------|--------------|
| **[Collective dynamics of 'small-world' networks](https://doi.org/10.1038/30918)** | Watts & Strogatz | 1998 | Foundational paper on small-world networks; provides the theoretical basis for understanding NSW/HNSW algorithms |
| **[Navigable Networks as Nash Equilibria of Navigation Games](https://doi.org/10.1038/s41467-017-01294-3)** | Papadopoulos et al. | 2018 | Explains why certain network structures exhibit good navigability properties |

### NSW (Navigable Small World) Algorithm

| Paper | Authors | Year | Contribution |
|-------|---------|------|--------------|
| **[Approximate nearest neighbor algorithm based on navigable small world graphs](https://doi.org/10.1016/j.ins.2013.08.017)** | Malkov et al. | 2014 | The predecessor to HNSW; introduces approximate nearest neighbor search based on navigable small-world graphs |
| **[Scalable Distributed Algorithm for Approximate Nearest Neighbor Search Problem in High Dimensional General Metric Spaces](https://doi.org/10.1109/TPDS.2015.2505333)** | Malkov et al. | 2016 | Early improvements to NSW, focusing on scalability and distributed processing |

### HNSW (Hierarchical NSW) Algorithm

| Paper | Authors | Year | Contribution |
|-------|---------|------|--------------|
| **[Efficient and robust approximate nearest neighbor search using Hierarchical Navigable Small World graphs](https://arxiv.org/abs/1603.09320)** | Malkov & Yashunin | 2016 | **Core algorithm** used by Vego; extends NSW with hierarchical structure for O(log N) query complexity |
| **[A Comprehensive Survey and Experimental Comparison of Graph-Based Approximate Nearest Neighbor Search](https://doi.org/10.14778/3397230)** | Li et al. | 2020 | Comparative analysis of various graph-based ANN algorithms including HNSW; guided our implementation decisions |

### Large-Scale Vector Search

| Paper | Authors | Year | Contribution |
|-------|---------|------|--------------|
| **[Billion-scale similarity search with GPUs](https://arxiv.org/abs/1702.08734)** | Johnson et al. | 2017 | Facebook AI's FAISS library; provides comprehensive comparison of methods including HNSW |
| **[DiskANN: Fast Accurate Billion-point Nearest Neighbor Search on a Single Node](https://doi.org/10.14778/3424573)** | Subramanya et al. | 2019 | Microsoft's improved approach for billion-scale data on SSDs; informs our future roadmap for large-scale support |

### Columnar Storage & Vector Quantization

| Paper | Authors | Year | Contribution |
|-------|---------|------|--------------|
| **[Apache Arrow: Cross-Language Development Platform for In-Memory Analytics](https://doi.org/10.14778/3397230)** | Apache Arrow Team | 2016 | Foundation for our storage layer's in-memory representation |
| **[Lance: Efficient Random Access in Columnar Storage through Adaptive Structural Encodings](https://github.com/lancedb/lance)** | Lance Team | 2022 | Influenced our Lance-compatible columnar storage format design |
| **[Lance v2: A New Columnar Container Format](https://lancedb.github.io/lance/format.html)** | Lance Team | 2023 | Latest columnar container format improvements |
| **[Product Quantization for Nearest Neighbor Search](https://doi.org/10.1109/TPAMI.2010.57)** | Jégou et al. | 2011 | Foundation for future quantization support (PQ) to reduce memory footprint |

---

## 🙏 Acknowledgments

- **HNSW Algorithm**: Yury A. Malkov and Dmitry A. Yashunin for the groundbreaking HNSW algorithm
- **Apache Arrow Project**: For the standardized columnar memory format
- **Lance**: For the modern columnar storage format designed for ML/AI workloads
- **Go Community**: For the excellent ecosystem and tools

---

## 📮 Contact

- Project Homepage: https://github.com/wzqhbustb/vego
- Issue Tracker: [GitHub Issues](https://github.com/wzqhbustb/vego/issues)
- Discussions: [GitHub Discussions](https://github.com/wzqhbustb/vego/discussions)

---

<p align="center">
  <b>If you find this project helpful, please give it a ⭐️ Star!</b>
</p>
