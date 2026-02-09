# Vego Performance Analysis Report

**Date**: 2024-02  
**Test Command**: `make bench-quick` (index module)  
**Hardware**: Intel Core i9-13950HX, Linux amd64  
**Test Configuration**: 1,000 vectors, 128-dim, M=16, EfConstruction=200

---

## Executive Summary

Current performance shows **significant degradation** compared to expected targets:

| Metric | Current | Expected | Gap | Status |
|--------|---------|----------|-----|--------|
| Build Throughput | 225 vec/s | 10K-50K vec/s | **44-222x slower** | 🔴 Critical |
| Build Time (1K) | 4.45s | ~0.1s | **44x slower** | 🔴 Critical |
| Compression Ratio | 0.71x | 2-4x | **Storage bloat** | 🔴 Critical |
| Query P99 Latency | 1.37ms | <0.1ms | **13x slower** | 🟡 High |
| Query QPS | 1,943 | ~20,000 | **10x slower** | 🟡 High |

**Positive**: Recall@10 = 100.00% ✅  
**Conclusion**: Algorithm correctness is verified, but engineering implementation has severe performance issues.

---

## Test Result Comparison

### Stability Analysis (Two Runs)

| Metric | Run 1 | Run 2 | Variance | Stability |
|--------|-------|-------|----------|-----------|
| Build Time | 4.524s | 4.448s | -1.7% | ✅ Stable |
| Build Throughput | 221 vec/s | 225 vec/s | +1.8% | ✅ Stable |
| Memory Usage | 3.63 MB | 3.43 MB | -5.5% | ✅ Normal |
| Save Time | 34.9 ms | 40.3 ms | +15% | ✅ Normal |
| Load Time | 1.84 ms | 1.88 ms | +2% | ✅ Stable |
| Query QPS | 2,220 | 1,943 | -12.5% | ✅ Normal |
| P99 Latency | 1.34 ms | 1.37 ms | +2.2% | ✅ Stable |

**Observation**: Performance is highly reproducible. Issues are systematic, not random fluctuations.

---

## Critical Issues

### Issue 1: Index Build Performance (Critical 🔴)

**Measured Data**:
```
4.45s to build 1,000 vectors = 225 vec/s
```

**Expected Performance**:
- Reasonable target: 10,000-50,000 vec/s
- Current gap: **44-222x slower than expected**

**Memory Allocation Analysis**:
```
6,698,375,296 B/op = 6.7 GB allocations
13,952,249 allocs   = 14 million allocations

Per vector: 6.7 MB allocated
Per operation: 14 allocations
```

**Root Causes**:

1. **EfConstruction=200 is too high for small datasets**
   ```go
   // Current configuration
   EfConstruction: 200  // Searches 200 candidates per insertion
   
   // For 1K data, this means examining 20% of all nodes
   // Theoretically, efConstruction should scale with dataset size
   ```

2. **Connection slice frequent resizing**
   ```go
   // node.go - connections are [][]int
   // Each AddConnection may trigger slice growth
   // No pre-allocation of capacity
   ```

3. **No SIMD optimization for distance calculation**
   - Pure Go implementation without AVX2/SSE
   - Each insertion requires hundreds of distance calculations
   - Each distance calculation: 128-dim × 4 bytes = 512 bytes read

4. **Global lock contention**
   ```go
   // hnsw.go:91-94
   h.globalLock.Lock()  // Serializes ALL insertions
   nodeID := len(h.nodes)
   h.nodes = append(h.nodes, newNode)
   h.globalLock.Unlock()
   ```

### Issue 2: Compression Ratio Anomaly (Critical 🔴)

**Measured**:
```
Raw data:    1000 × 128 × 4 bytes = 512 KB
Stored:      0.68 MB = 696 KB
Ratio:       0.73x (36% larger than raw)
```

**Expected**:
```
Compression ratio: 2-4x (storage should be 128-256 KB)
```

**Root Causes**:

1. **Small file overhead**
   ```
   Lance file structure overhead:
   - Header (8KB reserved)
   - Footer (32KB fixed)
   - Page metadata (~4KB per column)
   
   Fixed overhead: ~44KB
   
   For 512KB data: expected 8% overhead
   Actual overhead: 36% → indicates other issues
   ```

2. **Inappropriate encoder for small data**
   ```go
   // storage.go:13-15
   func defaultEncoderFactory() *encoding.EncoderFactory {
       return encoding.NewEncoderFactory(3) // Compression level 3
   }
   ```
   - May be using Plain encoding for small batches
   - Compression metadata overhead exceeds savings

3. **Vector data characteristics**
   - Random float32 vectors have high entropy
   - ZSTD performs poorly on random floats
   - Quantization (PQ/SQ) needed for effective compression

### Issue 3: Query Latency (High 🟡)

**Measured**:
```
P99 Latency: 1.37 ms (1K dataset)
```

**Expected**:
```
P99 Latency: <0.1 ms (1K dataset)
```

**Root Causes**:

1. **Ef=100 too large for 1K dataset**
   - Searches 10% of all nodes
   - Brute force would be faster for this size

2. **Unfriendly memory access pattern**
   - Linked list traversal, non-contiguous memory
   - No prefetching
   - Cache miss on each hop

---

## Code-Level Root Cause Analysis

### Memory Allocation Hotspots

Based on 14 million allocations, likely hotspots:

```go
// 1. insert.go:62-68 - candidates slice creation per insertion
candidatesForPrune := make([]SearchResult, len(neighborConnections))

// 2. search.go:174-181 - result array per search
resultArray := make([]SearchResult, results.Len())

// 3. node.go:53 - GetConnections returns copy
func (n *Node) GetConnections(level int) []int {
    result := make([]int, len(n.connections[level]))  // Allocates every call
    copy(result, n.connections[level])
    return result
}

// 4. distance.go - temporary slices (if any)
```

### Lock Contention Analysis

```go
// hnsw.go:78-106 Add function
func (h *HNSWIndex) Add(vector []float32) (int, error) {
    // ...
    h.globalLock.Lock()  // 🔴 Bottleneck 1: Serializes all insertions
    nodeID := len(h.nodes)
    newNode := NewNode(nodeID, vectorCopy, level)
    h.nodes = append(h.nodes, newNode)
    h.globalLock.Unlock()

    if nodeID == 0 {
        return nodeID, nil
    }

    h.insert(newNode)  // 🔴 Bottleneck 2: Multiple lock acquisitions inside
    return nodeID, nil
}
```

**Impact**: Even read operations (Search) contend with writes:
```go
// search.go
h.globalLock.RLock()  // Contends with Add
ep := h.entryPoint
maxLvl := h.maxLevel
h.globalLock.RUnlock()
```

---

## Optimization Recommendations

### P0: Critical (Immediate Impact)

#### 1. Dynamic EfConstruction Adjustment

```go
func optimalEfConstruction(n int) int {
    // Scale efConstruction with dataset size
    switch {
    case n < 1000:
        return 50
    case n < 10000:
        return 100
    case n < 100000:
        return 150
    default:
        return 200
    }
}

// Usage in NewHNSW
if config.EfConstruction <= 0 {
    config.EfConstruction = optimalEfConstruction(expectedSize)
}
```

**Expected Gain**: 1K dataset build time: 4.5s → ~1s (4.5x speedup)

#### 2. Pre-allocate Connection Slices

```go
// node.go:16-27
func NewNode(id int, vector []float32, level int) *Node {
    connections := make([][]int, level+1)
    for i := range connections {
        // Pre-allocate with capacity M
        connections[i] = make([]int, 0, 16) // M=16
    }
    return &Node{
        id:          id,
        vector:      vector,
        level:       level,
        connections: connections,
    }
}
```

**Expected Gain**: Reduce allocations by 50%

#### 3. Zero-Copy GetConnections

```go
// node.go - Add new method
func (n *Node) GetConnectionsReadOnly(level int) []int {
    n.mu.RLock()
    defer n.mu.RUnlock()
    return n.connections[level] // Return reference, caller must not modify
}

// Use in hot paths where read-only access is sufficient
```

**Expected Gain**: Reduce allocations by 30%

### P1: High Priority (Significant Improvement)

#### 4. SIMD-Optimized Distance Calculation

```go
// distance_simd.go
import "github.com/klauspost/cpuid/v2"

func L2Distance(a, b []float32) float32 {
    if cpuid.CPU.Supports(cpuid.AVX2) && len(a) >= 32 {
        return l2DistanceAVX2(a, b)
    }
    return l2DistanceScalar(a, b)
}

//go:noescape
func l2DistanceAVX2(a, b []float32) float32
```

**Expected Gain**: 4-8x speedup in distance calculations

#### 5. Batch Insert API

```go
// hnsw.go - New method
func (h *HNSWIndex) AddBatch(vectors [][]float32) ([]int, error) {
    // 1. Allocate all IDs at once
    // 2. Parallel level generation
    // 3. Batch insertion with reduced lock contention
    // 4. Parallel neighbor search within batch
}
```

**Expected Gain**: 5-10x speedup for bulk loading

### P2: Medium Term (Architecture)

#### 6. Vector Quantization (PQ/SQ)

```go
type QuantizedIndex struct {
    codebook []float32  // 256 × dimension
    codes    []uint8    // N × 1 byte per vector
}

func (q *QuantizedIndex) Search(query []float32, k int) ([]int, error) {
    // Search in quantized space
    // 4x memory reduction, 2-4x speedup
}
```

#### 7. Hierarchical Locking

```go
type HNSWIndex struct {
    entryPoint atomic.Int32
    maxLevel   atomic.Int32
    nodes      []atomic.Pointer[Node]  // Lock-free reads
    // Or: sync.RWMutex per level
}
```

---

## Validation & Testing

### Micro-Benchmarks

```go
// Benchmark distance calculation
func BenchmarkL2Distance(b *testing.B) {
    a := make([]float32, 128)
    vec := make([]float32, 128)
    rand.Read((*[512]byte)(unsafe.Pointer(&a[0]))[:])
    rand.Read((*[512]byte)(unsafe.Pointer(&vec[0]))[:])
    
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        L2Distance(a, vec)
    }
}

// Benchmark node operations
func BenchmarkNodeAddConnection(b *testing.B) {
    node := NewNode(0, make([]float32, 128), 3)
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        node.AddConnection(0, i%100)
    }
}

// Benchmark memory allocations
func BenchmarkHNSWInsertSingle(b *testing.B) {
    config := Config{Dimension: 128, M: 16}
    for i := 0; i < b.N; i++ {
        b.StopTimer()
        index := NewHNSW(config)
        vec := make([]float32, 128)
        rand.Read((*[512]byte)(unsafe.Pointer(&vec[0]))[:])
        b.StartTimer()
        index.Add(vec)
    }
}
```

### Profiling Commands

```bash
# CPU profiling
go test -bench=BenchmarkHNSW_E2E_1K -cpuprofile=cpu.prof ./index/
go tool pprof -http=:8080 cpu.prof

# Memory profiling
go test -bench=BenchmarkHNSW_E2E_1K -memprofile=mem.prof ./index/
go tool pprof -http=:8080 mem.prof

# Execution tracing
go test -bench=BenchmarkHNSW_E2E_1K -trace=trace.out ./index/
go tool trace trace.out

# Race detection
go test -race -bench=BenchmarkHNSW_E2E_1K ./index/

# Allocation tracking
go test -bench=BenchmarkHNSW_E2E_1K -benchmem ./index/
```

---

## Performance Targets (Post-Optimization)

### Short Term (P0 Optimizations)

| Metric | Current | Target | Improvement |
|--------|---------|--------|-------------|
| Build Time (1K) | 4.45s | <1s | 4.5x |
| Allocations | 14M | <5M | 3x |
| Build QPS | 225 | >1,000 | 4.5x |

### Medium Term (P1 Optimizations)

| Metric | Current | Target | Improvement |
|--------|---------|--------|-------------|
| Build QPS | 225 | >10,000 | 44x |
| Query P99 | 1.37ms | <0.2ms | 7x |
| Compression | 0.71x | >1.5x | 2x |

### Long Term (P2 Optimizations)

| Metric | Current | Target | Improvement |
|--------|---------|--------|-------------|
| Build QPS | 225 | >50,000 | 222x |
| Query QPS | 1,943 | >20,000 | 10x |
| Memory | 3.5MB | <2MB | 2x |
| Compression | 0.71x | >2.0x | 3x |

---

## Action Items

### Immediate (This Week)

- [ ] Profile current implementation (`go test -cpuprofile`)
- [ ] Implement dynamic `efConstruction` adjustment
- [ ] Pre-allocate connection slice capacity
- [ ] Add `GetConnectionsReadOnly` zero-copy method

### Short Term (This Month)

- [ ] Implement SIMD distance calculation (AVX2)
- [ ] Add batch insert API
- [ ] Optimize small file storage overhead
- [ ] Add memory pool for temporary slices

### Medium Term (Next Quarter)

- [ ] Implement vector quantization (SQ8)
- [ ] Refactor locking strategy (hierarchical or lock-free)
- [ ] Add Block Cache (from Roadmap Phase 1)
- [ ] Complete full ROADMAP.md Phase 0

---

## Appendix: Raw Benchmark Output

```
Build Performance:
  Time:             4.447912831s
  Throughput:       224.82 vectors/sec
  Memory Usage:     3.43 MB

Persistence Performance:
  Save Time:        40.340493ms
  Load Time:        1.883007ms
  Storage Size:     0.68 MB
  Compression:      0.71x

Query Performance:
  Total Time:       257.358297ms
  Throughput:       1942.82 queries/sec
  Avg Latency:      514.716µs
  P50 Latency:      503.705µs
  P95 Latency:      1.138257ms
  P99 Latency:      1.372234ms
  Recall@10:        1.0000 (100.00%)

Benchmark Stats:
  4769638577 ns/op        (4.77s per operation)
  6698375296 B/op         (6.7 GB allocated)
  13952249 allocs/op      (14M allocations)
```

---

*Report generated for Vego project. For questions or updates, refer to [ROADMAP.md](ROADMAP.md).*
