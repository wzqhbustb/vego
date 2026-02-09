# Vego

[![Go Reference](https://pkg.go.dev/badge/github.com/wzqhbkjdx/vego.svg)](https://pkg.go.dev/github.com/wzqhbkjdx/vego)
[![Go Report Card](https://goreportcard.com/badge/github.com/wzqhbkjdx/vego)](https://goreportcard.com/report/github.com/wzqhbkjdx/vego)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

**Vego** is a lightweight vector search engine for AI agents and embedded applications, written in pure Go with zero CGO dependencies.

```go
// Get started in 5 minutes: create a vector index
index := vego.NewHNSW(vego.Config{
    Dimension: 128,
    M:         16,
})

// Add vectors
id, _ := index.Add([]float32{0.1, 0.2, ...})

// Search nearest neighbors
results, _ := index.Search(query, 10)
```

---

## 🎯 Why Vego?

| Feature | Vego | Milvus | Weaviate | FAISS |
|------|------|--------|----------|-------|
| **Deployment** | Embedded Library | Standalone Service | Standalone Service | Python/C++ Lib |
| **Go Native** | ✅ Pure Go | ❌ Go+CGO | ✅ Go | ❌ C++ |
| **Binary Size** | ~500KB | 100MB+ | 50MB+ | 10MB+ |
| **External Dependencies** | Zero CGO | Many | Many | Python/C++ |
| **Startup Time** | Milliseconds | Seconds | Seconds | - |
| **Persistence** | Built-in | Requires Config | Requires Config | DIY |

### Core Advantages

1. **🚀 Ultra Lightweight**
   - Pure Go implementation, zero CGO dependencies
   - Single binary deployment
   - Minimal memory footprint (ideal for edge devices)

2. **⚡ High Performance**
   - HNSW algorithm with millisecond-level latency
   - >95% recall rate (compared to brute-force search)
   - Concurrent read/write support

3. **💾 Built-in Persistence**
   - Self-developed Lance-like columnar storage format
   - Supports ZSTD, BitPacking, and other compression algorithms
   - One-click save/load complete index

4. **🔧 Simple & Easy to Use**
   - Clean API design
   - Supports L2, Cosine, InnerProduct distance metrics
   - Automatic memory management, no complex configuration

---

## 📦 Use Cases

### 1. AI Agent Local Memory
Provide long-term memory capabilities for AI agents without external databases:

```go
// Agent memory system
memory := vego.NewHNSW(vego.Config{Dimension: 1536})

// Save conversation history
embedding := embed("User loves golang programming")
memory.Add(embedding)

// Retrieve relevant memories
query := embed("What are the user's programming preferences?")
memories, _ := memory.Search(query, 3)
```

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
go get github.com/wzqhbkjdx/vego
```

### Basic Example

```go
package main

import (
    "fmt"
    "github.com/wzqhbkjdx/vego/index"
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

### Parameter Tuning Guide

| Parameter | Small Dataset (<1M) | Large Dataset (>1M) | High Recall | Low Latency |
|-----------|---------------------|---------------------|-------------|-------------|
| M | 16 | 32 | 32-64 | 8-16 |
| EfConstruction | 100-200 | 200-400 | 400+ | 50-100 |
| ef (search) | 50-100 | 100-200 | 200+ | 20-50 |

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

### Storage Format

Self-developed **Lance-compatible** columnar storage:

```
nodes.lance       # Node data: ID + Vector + Level
connections.lance # Edge data: NodeID + Layer + NeighborID
metadata.lance    # Metadata: M, Dimension, EntryPoint, etc.
```

**Encoding Support:**
- ZSTD compression
- BitPacking integer compression
- RLE run-length encoding
- Dictionary encoding

---

## 📊 Performance Benchmarks

Test Environment: Apple M3 Pro, 18GB RAM

### Index Build Performance

| Dataset Size | Dimension | Build Time | Memory Usage |
|-----------|------|----------|----------|
| 10,000 | 128 | 0.12s | 2.1MB |
| 100,000 | 128 | 1.8s | 18MB |
| 1,000,000 | 128 | 25s | 175MB |

### Query Performance (Single-threaded)

| Dataset Size | Top-K | Latency (P99) | Recall |
|-----------|-------|-----------|--------|
| 100,000 | 10 | 0.8ms | 96.5% |
| 100,000 | 100 | 2.1ms | 98.2% |
| 1,000,000 | 10 | 1.5ms | 95.1% |
| 1,000,000 | 100 | 4.2ms | 97.8% |

### Concurrent Performance

```bash
BenchmarkHNSWInsert-12      100000    15230 ns/op    // Concurrent insert
BenchmarkHNSWSearch-12      100000     8920 ns/op    // Concurrent search
```

---

## 🔌 Integration Examples

### Building Local RAG with Ollama

```go
package main

import (
    "context"
    "github.com/ollama/ollama/api"
    "github.com/wzqhbkjdx/vego/index"
)

type RAGSystem struct {
    index   *hnsw.HNSWIndex
    client  *api.Client
}

func (r *RAGSystem) AddDocument(ctx context.Context, text string) error {
    // Generate embedding using Ollama
    embedding, err := r.getEmbedding(ctx, text)
    if err != nil {
        return err
    }
    _, err = r.index.Add(embedding)
    return err
}

func (r *RAGSystem) Query(ctx context.Context, question string) (string, error) {
    // 1. Vectorize the question
    queryVec, _ := r.getEmbedding(ctx, question)
    
    // 2. Retrieve relevant documents
    results, _ := r.index.Search(queryVec, 3, 0)
    
    // 3. Construct prompt and query LLM
    context := r.buildContext(results)
    return r.askLLM(ctx, question, context)
}
```

### As AI Agent Memory System

```go
type AgentMemory struct {
    shortTerm []string
    longTerm  *hnsw.HNSWIndex
}

func (m *AgentMemory) Remember(ctx string, embedding []float32) {
    // Long-term memory stored in vector index
    m.longTerm.Add(embedding)
}

func (m *AgentMemory) Recall(query []float32, topK int) []string {
    // Semantic retrieval of relevant memories
    results, _ := m.longTerm.Search(query, topK, 0)
    return m.fetchMemories(results)
}
```

---

## 🤝 Comparison with Other Projects

### vs FAISS
- **FAISS**: Meta's C++ library with Python bindings, powerful but heavy dependencies
- **Vego**: Go-native, no CGO, better suited for Go ecosystem and embedded scenarios

### vs Milvus/Zilliz
- **Milvus**: Enterprise-grade distributed vector database, feature-complete but complex deployment
- **Vego**: Lightweight embedded library, ideal for edge and simple scenarios

### vs Weaviate
- **Weaviate**: Go-implemented vector database requiring standalone deployment
- **Vego**: Pure library form, no service dependencies, faster startup

### vs chroma-go
- **chroma-go**: Go client for Chroma vector database
- **Vego**: No external service dependencies, completely self-contained

---

## 🛠️ Roadmap

- [x] HNSW core algorithm
- [x] Multiple distance function support
- [x] Persistence storage
- [x] Concurrent safety
- [ ] Vector deletion/update
- [ ] Incremental save
- [ ] Metadata filtering (Filter Search)
- [ ] SIMD acceleration (AVX/NEON)
- [ ] Quantization support (PQ/SQ)
- [ ] Distributed index

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
git clone https://github.com/wzqhbkjdx/vego.git
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

## 🙏 Acknowledgments

- HNSW original paper: [Efficient and robust approximate nearest neighbor search using Hierarchical Navigable Small World graphs](https://arxiv.org/abs/1603.09320)
- Apache Arrow project
- Lance storage format design

---

## 📮 Contact

- Project Homepage: https://github.com/wzqhbkjdx/vego
- Issue Tracker: [GitHub Issues](https://github.com/wzqhbkjdx/vego/issues)
- Discussions: [GitHub Discussions](https://github.com/wzqhbkjdx/vego/discussions)

---

<p align="center">
  <b>If you find this project helpful, please give it a ⭐️ Star!</b>
</p>
