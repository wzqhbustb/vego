# Vego Examples

This directory contains runnable examples demonstrating various features of the Vego vector search engine.

## Prerequisites

- Go 1.23 or later
- Vego package installed

## Quick Start

Each example is a standalone Go program. Navigate to any example directory and run:

```bash
cd basic_usage
go run main.go
```

## Examples Overview

### 1. Basic Usage (`basic_usage/`)

Demonstrates the fundamental operations of HNSW index:
- Creating an index with adaptive configuration
- Adding vectors
- Searching nearest neighbors
- Tuning EF parameter

```bash
cd basic_usage
go run main.go
```

**Output:**
```
=== Vego Basic Usage Demo ===
✓ Created HNSW index with adaptive configuration
Adding 1000 random vectors...
✓ Added 1000 vectors in ...
Searching for 10 nearest neighbors...
```

### 2. Persistence (`persistence/`)

Shows how to save and load HNSW index to/from disk:
- Save index to Lance format
- Load index from disk
- Continue using loaded index

```bash
cd persistence
go run main.go
```

**Key Points:**
- Index is persisted to `/tmp/vego_persistence_demo/`
- Demonstrates save/load performance
- Shows that loaded index maintains search capability

### 3. Storage Demo (`storage_demo/`)

Low-level demonstration of the columnar storage API:
- Define Arrow schema
- Build RecordBatch from arrays
- Write to Lance format with compression
- Read back and access data

```bash
cd storage_demo
go run main.go
```

**Key Points:**
- Shows compression ratios
- Demonstrates Arrow array builders
- Direct storage API usage (without HNSW)

### 4. Batch Insert (`batch_insert/`)

Compares different insertion strategies:
- Single insert performance
- Optimized configuration for batch workloads
- Search performance after bulk load

```bash
cd batch_insert
go run main.go
```

**Output:**
```
Test 1: Single Insert (one by one)
  Rate: XXX vectors/sec

Test 2: Optimized Configuration
  Rate: YYY vectors/sec
  Improvement: Z% faster
```

### 5. Search Comparison (`search_comparison/`)

Compares different distance functions and parameters:
- L2 (Euclidean) Distance
- Cosine Distance
- Inner Product
- Different EF values and their impact

```bash
cd search_comparison
go run main.go
```

**Key Points:**
- Shows how different distance metrics work
- Demonstrates EF parameter trade-offs
- Guides on choosing the right configuration

## Common Patterns

### Creating an Index

```go
config := hnsw.Config{
    Dimension:    128,      // Your embedding dimension
    Adaptive:     true,     // Auto-tune parameters
    ExpectedSize: 10000,    // Expected number of vectors
}
index := hnsw.NewHNSW(config)
```

### Adding Vectors

```go
vector := make([]float32, 128)
// Fill with your embedding data
id, err := index.Add(vector)
```

### Searching

```go
// Search for 10 nearest neighbors
results, err := index.Search(query, 10, 0)
// EF=0 means use default (usually 200)

// Search with higher recall (slower)
results, err := index.Search(query, 10, 400)
```

### Persistence

```go
// Save
err := index.SaveToLance("./my_index")

// Load
loadedIndex, err := hnsw.LoadHNSWFromLance("./my_index")
```

## Performance Tips

1. **Use Adaptive Configuration**: Let Vego auto-tune parameters based on your data
2. **Set ExpectedSize**: Helps with memory pre-allocation
3. **Choose Right Distance Function**:
   - L2: General purpose, magnitude matters
   - Cosine: Direction matters (common for text)
   - Inner Product: Specific use cases
4. **Tune EF Parameter**:
   - Lower for speed (50-100)
   - Higher for recall (200-400)
5. **Batch Operations**: Build index offline, load for serving

## Troubleshooting

### "dimension must be positive" panic
Make sure `Dimension` is set correctly in the config.

### Slow search
- Try reducing EF parameter
- Check if index is in memory (loaded from disk)
- Consider building index with larger M for better graph quality

### High memory usage
- Reduce M parameter (fewer connections per node)
- Use smaller EF during construction
- Process data in batches

## More Information

- [Main README](../README.md)
- [Storage Documentation](../STORAGE.md)
- [Architecture Documentation](../ARCHITECTURE.md)
