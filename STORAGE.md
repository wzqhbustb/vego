# Vego Storage Layer

A high-performance, columnar storage engine designed for vector search workloads.

[![Go Reference](https://pkg.go.dev/badge/github.com/wzqhbustb/vego.svg)](https://pkg.go.dev/github.com/wzqhbustb/vego/storage)

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Design Principles](#design-principles)
- [Core Components](#core-components)
  - [Arrow Subsystem](#arrow-subsystem)
  - [Column Storage Engine](#column-storage-engine)
  - [Encoding & Compression](#encoding--compression)
  - [File Format](#file-format)
  - [Async I/O System](#async-io-system)
- [Performance Characteristics](#performance-characteristics)
- [Usage Guide](#usage-guide)
- [Roadmap](#roadmap)
- [References](#references)

---

## Overview

Vego Storage is a self-developed, Lance-compatible columnar storage engine optimized for AI/ML workloads. It provides:

- **Zero-Copy Memory Management**: Custom Arrow implementation without CGO
- **Adaptive Compression**: Intelligent encoder selection based on data characteristics
- **Dual I/O Modes**: Both synchronous and asynchronous I/O support
- **HNSW Integration**: Native support for vector index persistence

### Why Self-Developed?

| Aspect | Apache Arrow Go | Vego Storage |
|--------|-----------------|--------------|
| CGO Dependency | Required | **None** |
| Binary Size | ~50MB | **~10MB** |
| Memory Control | Limited | **Full** |
| Vector Optimizations | Generic | **Specialized** |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Application Layer                        │
│                    (HNSW Index, Vector Search)                   │
└──────────────────────┬──────────────────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────────────────┐
│                      Column API                                  │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────────┐ │
│  │   Writer    │    │   Reader    │    │   RecordBatch       │ │
│  │  (Encode)   │    │  (Decode)   │    │   (In-Memory)       │ │
│  └──────┬──────┘    └──────┬──────┘    └─────────────────────┘ │
└─────────┼──────────────────┼────────────────────────────────────┘
          │                  │
┌─────────▼──────────────────▼────────────────────────────────────┐
│                      Arrow Subsystem                             │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────────┐ │
│  │    Array    │    │   Schema    │    │   DataType          │ │
│  │  (Memory)   │    │  (Metadata) │    │   (Type System)     │ │
│  └──────┬──────┘    └─────────────┘    └─────────────────────┘ │
└─────────┼────────────────────────────────────────────────────────┘
          │
┌─────────▼────────────────────────────────────────────────────────┐
│                     Encoding Layer                               │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────────────┐   │
│  │   Zstd   │ │   RLE    │ │    BSS   │ │   BitPacking     │   │
│  └──────────┘ └──────────┘ └──────────┘ └──────────────────┘   │
│                    EncoderFactory                                │
│              (Adaptive Selection)                                │
└──────────────────────────────────────────────────────────────────┘
          │
┌─────────▼────────────────────────────────────────────────────────┐
│                     Format Layer                                 │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │  Lance-compatible Columnar Format                           │ │
│  │  ┌─────────┬─────────┬─────────┬─────────┬─────────┐        │ │
│  │  │ Header  │ Page 1  │ Page 2  │  ...    │ Footer  │        │ │
│  │  │ (8KB)   │(1MB)    │(1MB)    │         │ (32KB)  │        │ │
│  │  └─────────┴─────────┴─────────┴─────────┴─────────┘        │ │
│  └─────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────┘
          │
┌─────────▼────────────────────────────────────────────────────────┐
│                      I/O Layer                                   │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐       │
│  │  Sync I/O    │    │  Async I/O   │    │  FilePool    │       │
│  │  (Default)   │    │  (Optional)  │    │  (Manager)   │       │
│  └──────────────┘    └──────────────┘    └──────────────┘       │
└──────────────────────────────────────────────────────────────────┘
```

---

## Design Principles

### 1. Columnar Storage

Data is stored column-by-column rather than row-by-row:

```
Row-based (Traditional):
┌─────────────────────────────────────────┐
│ ID │ Vector[128]       │ Level │ ...  │
├─────────────────────────────────────────┤
│ 0  │ [0.1, 0.2, ...]   │ 3     │      │
│ 1  │ [0.3, 0.4, ...]   │ 2     │      │
└─────────────────────────────────────────┘

Columnar (Vego):
┌──────────────────┐ ┌──────────────────┐ ┌──────────┐
│       ID         │ │     Vector       │ │  Level   │
├──────────────────┤ ├──────────────────┤ ├──────────┤
│ 0                │ │ 0.1, 0.2, 0.3... │ │ 3        │
│ 1                │ │ 0.3, 0.4, 0.5... │ │ 2        │
└──────────────────┘ └──────────────────┘ └──────────┘
```

**Benefits**:
- Better compression (same-type data is contiguous)
- Vectorized operations (SIMD-friendly)
- Projection pushdown (read only needed columns)

### 2. Zero-Copy Memory

```go
// Arrow Buffer wraps Go slice without copying
type Buffer struct {
    data []byte  // Points to original memory
    refCount int
}

// Zero-copy view
func (b *Buffer) Int32() []int32 {
    return unsafe.Slice((*int32)(unsafe.Pointer(&b.data[0])), len(b.data)/4)
}
```

### 3. Adaptive Encoding

Storage layer automatically selects the best encoding based on data statistics:

```
Integer Data → Analyze Cardinality & Run Ratio
                ↓
        ┌───────┴───────┐
   Low Cardinality    High Run Ratio
        ↓                  ↓
   Dictionary         RLE
   Encoding           Encoding
        ↓                  ↓
   Else → BitPacking (if narrow) → ZSTD (default)
```

---

## Core Components

### Arrow Subsystem

#### Type System

```go
// Primitive Types
Int32, Int64, Float32, Float64, String, Binary

// Nested Types
FixedSizeList  // For vectors: FixedSizeList<Float32>[128]
List           // For variable-length arrays
Struct         // For complex records
```

#### Specialized for Vectors

```go
// Create a 128-dimensional float32 vector type
vectorType := core.VectorType(128)
// Result: fixed_size_list<float32>[128]

// Memory layout for 1000 vectors:
// - 1000 * 128 * 4 bytes = 512KB contiguous memory
// - Cache-line aligned (64 bytes)
// - No pointer chasing, pure sequential access
```

#### Key Arrays

| Array Type | Use Case | Memory Layout |
|------------|----------|---------------|
| `Int32Array` | Node IDs, Levels | Contiguous int32 slice |
| `Float32Array` | Vector components | Contiguous float32 slice |
| `FixedSizeListArray` | Complete vectors | Flattened + offset calculation |

### Column Storage Engine

#### Writer

```go
// File layout with header reservation
// ┌─────────────────────────────────────────────┐
// │ Header (8KB reserved)                       │
// │   - Schema                                  │
// │   - Version                                 │
// │   - NumRows (updated on close)              │
// ├─────────────────────────────────────────────┤
// │ Column 0, Page 0                            │
// │ Column 0, Page 1                            │
// │ ...                                         │
// │ Column N, Page M                            │
// ├─────────────────────────────────────────────┤
// │ Footer (32KB)                               │
// │   - PageIndex (offset, size, encoding)      │
// │   - Statistics                              │
// └─────────────────────────────────────────────┘
```

**Features**:
- **Header Padding**: 8KB reserved space allows metadata updates without rewriting
- **Page-based**: 1MB default pages support streaming for large columns
- **Footer Index**: Fast random access to any page

#### Reader

```go
type Reader struct {
    // Sync Mode (default)
    file *os.File
    
    // Async Mode (optional)
    asyncIO *io.AsyncIO
    useAsync bool
}

// Automatic mode selection
func (r *Reader) ReadRecordBatch() (*core.RecordBatch, error) {
    if r.useAsync {
        return r.readColumnsAsync()  // Concurrent column reads
    }
    return r.readColumnsSync()       // Sequential reads
}
```

### Encoding & Compression

#### Supported Encodings

| Encoding | Encode | Decode | Compression | Best For |
|----------|--------|--------|-------------|----------|
| **ZSTD** | 23 μs | 62 μs | Very High | General purpose |
| **RLE** | 10 μs | 39 μs | High | Sequential/timestamps |
| **BitPacking** | 50 μs | 88 μs | Medium | Narrow integers |
| **BSS** | 48 μs | 48 μs | Medium | Float32 vectors |
| **Dictionary** | 92 μs | 50 μs | High* | Low cardinality |

*Dictionary degrades to ~650 μs for high cardinality (>10K unique values)

#### Encoder Selection Logic

```go
func (f *EncoderFactory) SelectEncoder(dtype DataType, stats *Statistics) Encoder {
    // Small data: ZSTD
    if stats.NumValues < 100 {
        return NewZstdEncoder(level)
    }
    
    switch dtype.ID() {
    case core.INT32, core.INT64:
        return f.selectIntegerEncoder(stats)
    case core.FLOAT32, core.FLOAT64:
        return f.selectFloatEncoder(stats)
    case core.FIXED_SIZE_LIST:
        return f.selectFixedSizeListEncoder(stats)
    }
}

func (f *EncoderFactory) selectIntegerEncoder(stats *Statistics) Encoder {
    // Priority 1: RLE for sequential data
    if stats.GetRunRatio() < 0.1 {
        return NewRLEEncoder()
    }
    
    // Priority 2: Dictionary for low cardinality
    if stats.GetCardinalityRatio() < 0.1 {
        return NewDictionaryEncoder()
    }
    
    // Priority 3: BitPacking for narrow integers
    if stats.GetMaxBitWidth() <= 16 {
        return NewBitPackingEncoder(uint8(stats.GetMaxBitWidth()))
    }
    
    // Default: ZSTD
    return NewZstdEncoder(f.compressionLevel)
}
```

### File Format

#### Page Structure

```go
type Page struct {
    Type             PageType     // DATA, DICT, INDEX
    Encoding         EncodingType // ZSTD, RLE, etc.
    ColumnIndex      int32
    NumValues        int32
    UncompressedSize int32
    CompressedSize   int32
    Checksum         uint32       // CRC32
    Data             []byte
}
```

#### PageIndex Structure

```go
type PageIndex struct {
    ColumnIndex int32        // Column number
    PageNum     int32        // Page sequence
    Offset      int64        // File offset
    Size        int32        // Compressed size
    NumValues   int32        // Number of values
    Encoding    EncodingType // For decoder selection
}
```

### Async I/O System

#### Architecture

```
Application
    │
    ▼
┌─────────────┐
│  Scheduler  │──► Priority Queue (sorted by offset)
│             │──► Request Merging (adjacent offsets)
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  Executor   │──► Worker Pool (4-8 goroutines)
│             │──► Submit to FilePool
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  FilePool   │──► Reference-counted file handles
│             │──► Automatic cleanup
└─────────────┘
```

**Current Status**:
- ✅ **Sync I/O**: Production-ready, optimal for most workloads
- ⚠️ **Async I/O**: Latency increases linearly with concurrency (known issue)

---

## Performance Characteristics

### Memory Performance

| Operation | Latency | Throughput | Allocations |
|-----------|---------|------------|-------------|
| Float32 Array Access | 1.3 ns/op | ~770M ops/s | 0 |
| Buffer View (Zero-Copy) | 1.2 ns/op | ~830M ops/s | 0 |
| RecordBatch Creation | 220 μs | - | 21 |

### File I/O Throughput

| Operation | Rows | Columns | Throughput | Latency |
|-----------|------|---------|------------|---------|
| **Write** | 10K | 1 | ~330 MB/s | 12 μs |
| **Write** | 100K | 10 | ~280 MB/s | 1.4 ms |
| **Read** | 10K | 1 | ~250 MB/s | 16 μs |
| **Read** | 100K | 10 | ~220 MB/s | 1.8 ms |
| **Roundtrip** | 10K | 5 | - | 890 μs |

### Compression Efficiency

| Data Type | Original Size | Compressed | Ratio |
|-----------|---------------|------------|-------|
| 10K vectors (128-dim) | ~6.9 MB | 5.79 MB | 0.84x |
| 100K vectors (128-dim) | ~82 MB | 63.39 MB | 0.77x |
| 10K vectors (1536-dim) | ~76 MB | 59.51 MB | 0.78x |

### Concurrency Performance

| Mode | Concurrency | Latency | Memory | Status |
|------|-------------|---------|--------|--------|
| Sync | 1 | 1.9 ms | 1.2 MB | ✅ Production-ready |
| Async | 1 | 2.0 ms | 2.4 MB | ✅ Available |
| Async | 8 | 23 ms | 27 MB | ⚠️ Known issue |
| Async | 16 | 35 ms | 52 MB | ⚠️ Not recommended |

**Recommendation**: Use Sync I/O for production workloads until Async I/O is optimized.

---

## Usage Guide

### Basic Usage

#### Writing Data

```go
package main

import (
    "github.com/wzqhbustb/vego/core"
    "github.com/wzqhbustb/vego/storage/column"
    "github.com/wzqhbustb/vego/storage/encoding"
)

func main() {
    // 1. Define schema
    schema := core.NewSchema([]core.Field{
        core.NewField("id", core.PrimInt32(), false),
        core.NewField("vector", core.VectorType(128), false),
        core.NewField("level", core.PrimInt32(), false),
    }, nil)
    
    // 2. Create writer with encoding factory
    factory := encoding.NewEncoderFactory(3) // compression level 3
    writer, err := column.NewWriter("data.lance", schema, factory)
    if err != nil {
        panic(err)
    }
    defer writer.Close()
    
    // 3. Prepare data arrays
    ids := core.NewInt32Array([]int32{0, 1, 2, 3, 4}, nil)
    vectors := core.NewFloat32Array(
        // Flattened: 5 vectors × 128 dimensions
        make([]float32, 5*128), 
        nil,
    )
    levels := core.NewInt32Array([]int32{3, 2, 4, 1, 3}, nil)
    
    // 4. Create RecordBatch
    batch, err := core.NewRecordBatch(schema, 5, []core.Array{ids, vectors, levels})
    if err != nil {
        panic(err)
    }
    
    // 5. Write to file
    if err := writer.WriteRecordBatch(batch); err != nil {
        panic(err)
    }
}
```

#### Reading Data

```go
// Synchronous read (recommended)
reader, err := column.NewReader("data.lance")
if err != nil {
    panic(err)
}
defer reader.Close()

batch, err := reader.ReadRecordBatch()
if err != nil {
    panic(err)
}

// Access columns
idColumn := batch.Column(0).(*core.Int32Array)
vectorColumn := batch.Column(1).(*core.FixedSizeListArray)
levelColumn := batch.Column(2).(*core.Int32Array)

// Read values
for i := 0; i < batch.NumRows(); i++ {
    id := idColumn.Value(i)
    level := levelColumn.Value(i)
    vector := vectorColumn.ValueSlice(i).([]float32)
    
    fmt.Printf("ID: %d, Level: %d, Vector: %v...\n", id, level, vector[:5])
}
```

### HNSW Integration

#### Saving Index

```go
// Save HNSW index to disk
func (h *HNSWIndex) SaveToLance(baseDir string) error {
    // Saves three files:
    // - nodes.lance: ID, vector, level
    // - connections.lance: node_id, layer, neighbor_id
    // - metadata.lance: M, dimension, entryPoint, etc.
    
    if err := h.saveNodes(filepath.Join(baseDir, "nodes.lance")); err != nil {
        return fmt.Errorf("save nodes failed: %w", err)
    }
    
    if err := h.saveConnections(filepath.Join(baseDir, "connections.lance")); err != nil {
        return fmt.Errorf("save connections failed: %w", err)
    }
    
    if err := h.saveMetadata(filepath.Join(baseDir, "metadata.lance")); err != nil {
        return fmt.Errorf("save metadata failed: %w", err)
    }
    
    return nil
}
```

#### Loading Index

```go
// Load HNSW index from disk
func LoadHNSWFromLance(baseDir string) (*HNSWIndex, error) {
    // 1. Load metadata to get configuration
    metadata, err := loadMetadata(filepath.Join(baseDir, "metadata.lance"))
    if err != nil {
        return nil, err
    }
    
    // 2. Create HNSW with original config
    config := Config{
        M:              int(metadata[0]),
        EfConstruction: int(metadata[3]),
        Dimension:      int(metadata[4]),
        DistanceFunc:   L2Distance,
    }
    hnsw := NewHNSW(config)
    
    // 3. Restore state
    hnsw.entryPoint = metadata[5]
    hnsw.maxLevel = metadata[6]
    
    // 4. Load nodes and connections
    if err := hnsw.loadNodes(filepath.Join(baseDir, "nodes.lance")); err != nil {
        return nil, err
    }
    
    if err := hnsw.loadConnections(filepath.Join(baseDir, "connections.lance")); err != nil {
        return nil, err
    }
    
    return hnsw, nil
}
```

### Advanced: Custom Encoding

```go
// Create encoder factory with custom configuration
config := &encoding.EncoderConfig{
    BitPackingMaxBitWidth: 16,
    RLEThreshold:          0.5,
    DictionaryThreshold:   0.5,
    DictionaryMaxSize:     1 << 20,
    BSSEntropyThreshold:   4.0,
    SmallDataThreshold:    100,
}

factory := encoding.NewEncoderFactoryWithConfig(3, config)
writer, err := column.NewWriter("data.lance", schema, factory)
```

### Async I/O (Experimental)

```go
// Create AsyncIO instance
asyncIO, err := lanceio.New(&lanceio.Config{
    Workers:      4,
    QueueSize:    1000,
    SchedulerCap: 10000,
})
if err != nil {
    panic(err)
}
defer asyncIO.Close()

// Create reader with async support
reader, err := column.NewReaderWithAsyncIO("data.lance", asyncIO)
if err != nil {
    panic(err)
}
defer reader.Close()

// Read will use async I/O internally
batch, err := reader.ReadRecordBatch()
```

---

## Roadmap

### Phase 0: Foundation (Current - v0.1)

✅ **Completed**:
- [x] Lance-compatible columnar format
- [x] ZSTD, BitPacking, RLE, BSS, Dictionary encoding
- [x] Arrow subsystem (Int32, Int64, Float32, Float64, FixedSizeList)
- [x] Sync I/O (production-ready)
- [x] Async I/O (basic implementation)
- [x] HNSW index persistence

### Phase 1: MVP (v0.2 - v0.3)

🚧 **In Progress**:

| Feature | Priority | Description |
|---------|----------|-------------|
| Block Cache | P0 | LRU cache for frequently accessed pages |
| Async I/O Optimization | P0 | Fix linear latency growth with concurrency |
| Table Abstraction | P1 | Multi-file dataset management |
| Statistics Collection | P1 | Min/Max/Null count per page |

**Target Metrics**:
- Async I/O latency: O(1) vs concurrency (currently O(n))
- Cache hit rate: >80% for hot data
- Memory limit: Configurable cache size

### Phase 2: Beta (v0.4 - v0.5)

📋 **Planned**:

| Feature | Priority | Description |
|---------|----------|-------------|
| CMO (Cache Miss Optimization) | P1 | Prefetch based on access patterns |
| Projection Pushdown | P1 | Read only needed columns |
| Zone Map | P2 | Min/Max indexing for range queries |
| Quantization (PQ/SQ) | P2 | In-memory vector compression |

### Phase 3: Performance (v1.0)

🎯 **Goals**:

| Feature | Target | Impact |
|---------|--------|--------|
| MiniBlock | 4KB blocks | Better cache locality |
| SIMD Acceleration | NEON/AVX | 4-8x encoding speed |
| Prefetch | Sequential read +50% | Reduced I/O wait |
| Mmap Support | Zero-copy read | Reduced memory usage |

### Phase 4: Extreme (v1.5)

🚀 **Advanced**:

- **io_uring** (Linux): Kernel-bypass I/O
- **Vectorized Execution**: Batch processing
- **Column Grouping**: Row-group like optimization
- **Bloom Filter**: Fast negative lookup

### Phase 5: Enterprise (v2.0)

🏢 **Production Features**:

- **WAL (Write-Ahead Log)**: Crash recovery
- **MVCC**: Multi-version concurrency control
- **Secondary Indexing**: B-tree, inverted index
- **Partitioning**: Horizontal sharding
- **Replication**: Multi-node support

---

## References

### Papers & Influences

| Paper | Authors | Year | Influence |
|-------|---------|------|-----------|
| [Apache Arrow](https://doi.org/10.14778/3397230) | Apache Team | 2016 | Memory format foundation |
| [Lance Format](https://github.com/lancedb/lance) | Lance Team | 2022 | Columnar storage design |
| [MonetDB/X100](https://doi.org/10.14778/1135885) | Boncz et al. | 2005 | Vectorized execution |
| [C-Store](https://doi.org/10.14778/1168135) | Stonebraker et al. | 2005 | Columnar storage concepts |

### Related Projects

- [Apache Arrow](https://arrow.apache.org/): Cross-language development platform
- [Lance](https://lancedb.github.io/lance/): Modern columnar format for ML
- [Parquet](https://parquet.apache.org/): Apache columnar format
- [ORC](https://orc.apache.org/): Optimized Row Columnar format

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for details on how to contribute to the storage layer.

### Development Setup

```bash
# Run storage tests
cd storage
go test ./...

# Run benchmarks
go test -bench=. ./...

# Run encoding benchmarks specifically
go test -bench=BenchmarkEncode ./encoding/
```

---

## License

Vego Storage is licensed under the [Apache 2.0 License](../LICENSE).
