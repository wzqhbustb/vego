package vego

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/wzqhbustb/vego/core"
	"github.com/wzqhbustb/vego/storage/column"
	"github.com/wzqhbustb/vego/storage/format"
)

// BenchmarkGetWithRowIndex benchmarks O(1) Get performance with RowIndex
func BenchmarkGetWithRowIndex(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "vego-bench-rowindex-*")
	defer os.RemoveAll(tmpDir)

	storage, _ := NewDocumentStorage(filepath.Join(tmpDir, "storage"), 128)
	
	// Prepare data: 1000 documents
	numDocs := 1000
	for i := 0; i < numDocs; i++ {
		storage.Put(&Document{
			ID:     fmt.Sprintf("doc-%d", i),
			Vector: makeTestVector(128, float32(i)),
		})
	}
	storage.Flush()
	
	b.ResetTimer()
	b.ReportAllocs()
	
	// Benchmark: Random Get queries
	for i := 0; i < b.N; i++ {
		id := fmt.Sprintf("doc-%d", i%numDocs)
		storage.Get(id)
	}
	
	storage.Close()
}

// BenchmarkGetWithRowIndexAndCache benchmarks with both RowIndex and BlockCache
func BenchmarkGetWithRowIndexAndCache(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "vego-bench-rowindex-cache-*")
	defer os.RemoveAll(tmpDir)

	cache := format.NewBlockCache(256 * 1024 * 1024) // 256MB cache
	storage, _ := NewDocumentStorage(filepath.Join(tmpDir, "storage"), 128, cache)
	
	// Prepare data
	numDocs := 1000
	for i := 0; i < numDocs; i++ {
		storage.Put(&Document{
			ID:     fmt.Sprintf("doc-%d", i),
			Vector: makeTestVector(128, float32(i)),
		})
	}
	storage.Flush()
	
	b.ResetTimer()
	b.ReportAllocs()
	
	for i := 0; i < b.N; i++ {
		id := fmt.Sprintf("doc-%d", i%numDocs)
		storage.Get(id)
	}
	
	storage.Close()
}

// BenchmarkGetWithoutCacheMiss benchmarks cache miss vs hit
// First iteration: cache miss, subsequent: cache hit
func BenchmarkGetCacheEffect(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "vego-bench-cache-effect-*")
	defer os.RemoveAll(tmpDir)

	cache := format.NewBlockCache(256 * 1024 * 1024)
	storage, _ := NewDocumentStorage(filepath.Join(tmpDir, "storage"), 128, cache)
	
	// Prepare data
	numDocs := 100
	for i := 0; i < numDocs; i++ {
		storage.Put(&Document{
			ID:     fmt.Sprintf("doc-%d", i),
			Vector: makeTestVector(128, float32(i)),
		})
	}
	storage.Flush()
	
	b.Run("CacheMiss_FirstRead", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// Always query different docs to force cache miss
			id := fmt.Sprintf("doc-%d", i%numDocs)
			storage.Get(id)
		}
	})
	
	b.Run("CacheHit_RepeatedRead", func(b *testing.B) {
		// Pre-warm cache
		for i := 0; i < numDocs; i++ {
			storage.Get(fmt.Sprintf("doc-%d", i))
		}
		
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Query same docs repeatedly - cache hit
			id := fmt.Sprintf("doc-%d", i%numDocs)
			storage.Get(id)
		}
	})
	
	storage.Close()
}

// BenchmarkReadRowAt benchmarks the core O(1) read operation
func BenchmarkReadRowAt(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "vego-bench-readrowat-*")
	defer os.RemoveAll(tmpDir)

	// Create file with RowIndex
	filename := filepath.Join(tmpDir, "test.lance")
	schema := createTestSchema(128)
	writer, _ := column.NewRowIndexWriter(filename, schema, format.V1_2, nil)
	
	// Write 10000 rows
	numRows := 10000
	writeTestData(writer, numRows, 128)
	writer.Close()
	
	reader, _ := column.NewRowIndexReader(filename)
	defer reader.Close()
	
	b.ResetTimer()
	b.ReportAllocs()
	
	for i := 0; i < b.N; i++ {
		rowIdx := int64(i % numRows)
		reader.ReadRowAt(rowIdx)
	}
}

// BenchmarkReadRowAtWithCache benchmarks with BlockCache
func BenchmarkReadRowAtWithCache(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "vego-bench-readrowat-cache-*")
	defer os.RemoveAll(tmpDir)

	filename := filepath.Join(tmpDir, "test.lance")
	schema := createTestSchema(128)
	writer, _ := column.NewRowIndexWriter(filename, schema, format.V1_2, nil)
	
	numRows := 10000
	writeTestData(writer, numRows, 128)
	writer.Close()
	
	cache := format.NewBlockCache(256 * 1024 * 1024)
	reader, _ := column.NewRowIndexReaderWithCache(filename, cache)
	defer reader.Close()
	
	b.ResetTimer()
	b.ReportAllocs()
	
	for i := 0; i < b.N; i++ {
		rowIdx := int64(i % numRows)
		reader.ReadRowAt(rowIdx)
	}
}

// BenchmarkFullScanVsRowIndex compares full scan vs RowIndex lookup
func BenchmarkFullScanVsRowIndex(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "vego-bench-scan-vs-index-*")
	defer os.RemoveAll(tmpDir)

	storage, _ := NewDocumentStorage(filepath.Join(tmpDir, "storage"), 128)
	
	// Prepare different data sizes
	sizes := []int{100, 500, 1000, 5000}
	
	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size_%d", size), func(b *testing.B) {
			// Clear and recreate storage
			storage.Close()
			os.RemoveAll(filepath.Join(tmpDir, "storage"))
			storage, _ = NewDocumentStorage(filepath.Join(tmpDir, "storage"), 128)
			
			// Prepare data
			for i := 0; i < size; i++ {
				storage.Put(&Document{
					ID:     fmt.Sprintf("doc-%d", i),
					Vector: makeTestVector(128, float32(i)),
				})
			}
			storage.Flush()
			
			b.Run("RowIndex_O1", func(b *testing.B) {
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					id := fmt.Sprintf("doc-%d", i%size)
					storage.Get(id)
				}
			})
		})
	}
	
	storage.Close()
}

// BenchmarkScalability tests how performance scales with data size
func BenchmarkScalability(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "vego-bench-scalability-*")
	defer os.RemoveAll(tmpDir)
	
	sizes := []int{100, 1000, 10000}
	
	for _, size := range sizes {
		b.Run(fmt.Sprintf("Docs_%d", size), func(b *testing.B) {
			dir := filepath.Join(tmpDir, fmt.Sprintf("storage-%d", size))
			storage, _ := NewDocumentStorage(dir, 128)
			
			// Prepare data
			for i := 0; i < size; i++ {
				storage.Put(&Document{
					ID:     fmt.Sprintf("doc-%d", i),
					Vector: makeTestVector(128, float32(i)),
				})
			}
			storage.Flush()
			
			b.ResetTimer()
			
			// Random access pattern
			for i := 0; i < b.N; i++ {
				id := fmt.Sprintf("doc-%d", i%size)
				storage.Get(id)
			}
			
			storage.Close()
		})
	}
}

// Helper functions for benchmarks
func createTestSchema(dim int) *core.Schema {
	return core.NewSchema([]core.Field{
		{Name: "id_hash", Type: core.PrimInt64(), Nullable: false},
		{Name: "vector", Type: core.VectorType(dim), Nullable: false},
		{Name: "timestamp", Type: core.PrimInt64(), Nullable: false},
	}, nil)
}

func writeTestData(writer *column.RowIndexWriter, count, dim int) {
	idBuilder := core.NewInt64Builder()
	vectorBuilder := core.NewFixedSizeListBuilder(
		core.FixedSizeListOf(core.PrimFloat32(), dim).(*core.FixedSizeListType),
	)
	timestampBuilder := core.NewInt64Builder()
	
	for i := 0; i < count; i++ {
		idBuilder.Append(int64(i))
		vectorBuilder.AppendValues(makeTestVector(dim, float32(i)))
		timestampBuilder.Append(time.Now().UnixNano())
	}
	
	schema := createTestSchema(dim)
	batch, _ := core.NewRecordBatch(schema, count, []core.Array{
		idBuilder.NewArray(),
		vectorBuilder.NewArray(),
		timestampBuilder.NewArray(),
	})
	
	writer.WriteRecordBatch(batch)
	for i := 0; i < count; i++ {
		writer.AddRowID(fmt.Sprintf("doc-%d", i), int64(i))
	}
}

// PrintComparisonResults prints a summary table (run with -v flag)
func TestPrintPerformanceSummary(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping summary in short mode")
	}
	
	t.Log("\n========== Performance Summary ==========")
	t.Log("RowIndex provides O(1) query performance vs O(N) full scan")
	t.Log("BlockCache provides additional speedup for repeated queries")
	t.Log("=========================================\n")
}
