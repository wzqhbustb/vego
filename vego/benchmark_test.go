package vego

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// setupBenchmarkCollection creates a collection for benchmarking
func setupBenchmarkCollection(b *testing.B, dimension int) (*Collection, func()) {
	b.Helper()
	tmpDir := filepath.Join(os.TempDir(), "vego_benchmark", fmt.Sprintf("bench_%d", time.Now().UnixNano()))
	os.RemoveAll(tmpDir)

	config := &Config{
		Dimension:      dimension,
		M:              16,
		EfConstruction: 200,
	}

	coll, err := NewCollection("benchmark", tmpDir, config)
	if err != nil {
		b.Fatalf("Failed to create collection: %v", err)
	}

	cleanup := func() {
		coll.Close()
		os.RemoveAll(tmpDir)
	}

	return coll, cleanup
}

// generateRandomVector generates a random vector for testing
func generateRandomVector(dimension int, seed int) []float32 {
	vec := make([]float32, dimension)
	// Simple pseudo-random based on seed
	for i := range vec {
		vec[i] = float32((i+seed)%100) * 0.01
	}
	return vec
}

// ==================== 插入性能基准测试 ====================

// BenchmarkInsert benchmarks single document insertion
func BenchmarkInsert(b *testing.B) {
	coll, cleanup := setupBenchmarkCollection(b, 128)
	defer cleanup()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		doc := &Document{
			ID:       fmt.Sprintf("doc_%d", i),
			Vector:   generateRandomVector(128, i),
			Metadata: map[string]interface{}{"index": i},
		}
		if err := coll.Insert(doc); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkInsertBatch benchmarks batch insertion with different batch sizes
func BenchmarkInsertBatch(b *testing.B) {
	batchSizes := []int{10, 50, 100, 500}

	for _, size := range batchSizes {
		b.Run(fmt.Sprintf("BatchSize_%d", size), func(b *testing.B) {
			coll, cleanup := setupBenchmarkCollection(b, 128)
			defer cleanup()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				docs := make([]*Document, size)
				for j := 0; j < size; j++ {
					docs[j] = &Document{
						ID:       fmt.Sprintf("batch_%d_doc_%d", i, j),
						Vector:   generateRandomVector(128, i*size+j),
						Metadata: map[string]interface{}{"batch": i, "index": j},
					}
				}
				if err := coll.InsertBatch(docs); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(size), "docs/op")
		})
	}
}

// BenchmarkInsertDifferentDimensions benchmarks insertion with different vector dimensions
func BenchmarkInsertDifferentDimensions(b *testing.B) {
	dimensions := []int{64, 128, 256, 512, 768, 1024}

	for _, dim := range dimensions {
		b.Run(fmt.Sprintf("Dim_%d", dim), func(b *testing.B) {
			coll, cleanup := setupBenchmarkCollection(b, dim)
			defer cleanup()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				doc := &Document{
					ID:     fmt.Sprintf("dim_%d_doc_%d", dim, i),
					Vector: generateRandomVector(dim, i),
				}
				if err := coll.Insert(doc); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// ==================== 搜索性能基准测试 ====================

// BenchmarkSearch benchmarks single search operation
func BenchmarkSearch(b *testing.B) {
	coll, cleanup := setupBenchmarkCollection(b, 128)
	defer cleanup()

	// Prepare data
	for i := 0; i < 1000; i++ {
		doc := &Document{
			ID:     fmt.Sprintf("search_doc_%d", i),
			Vector: generateRandomVector(128, i),
		}
		coll.Insert(doc)
	}

	query := generateRandomVector(128, 9999)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := coll.Search(query, 10)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSearchK benchmarks search with different k values
func BenchmarkSearchK(b *testing.B) {
	kValues := []int{1, 5, 10, 20, 50, 100}

	for _, k := range kValues {
		b.Run(fmt.Sprintf("K_%d", k), func(b *testing.B) {
			coll, cleanup := setupBenchmarkCollection(b, 128)
			defer cleanup()

			// Prepare data
			for i := 0; i < 1000; i++ {
				doc := &Document{
					ID:     fmt.Sprintf("search_doc_%d", i),
					Vector: generateRandomVector(128, i),
				}
				coll.Insert(doc)
			}

			query := generateRandomVector(128, 9999)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := coll.Search(query, k)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkSearchWithFilter benchmarks filtered search
func BenchmarkSearchWithFilter(b *testing.B) {
	coll, cleanup := setupBenchmarkCollection(b, 128)
	defer cleanup()

	// Prepare data with metadata
	for i := 0; i < 1000; i++ {
		category := "A"
		if i%2 == 0 {
			category = "B"
		}
		doc := &Document{
			ID:     fmt.Sprintf("filter_doc_%d", i),
			Vector: generateRandomVector(128, i),
			Metadata: map[string]interface{}{
				"category": category,
				"index":    i,
			},
		}
		coll.Insert(doc)
	}

	query := generateRandomVector(128, 9999)
	filter := &MetadataFilter{
		Field:    "category",
		Operator: "eq",
		Value:    "A",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := coll.SearchWithFilter(query, 10, filter)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSearchBatch benchmarks batch search
func BenchmarkSearchBatch(b *testing.B) {
	coll, cleanup := setupBenchmarkCollection(b, 128)
	defer cleanup()

	// Prepare data
	for i := 0; i < 1000; i++ {
		doc := &Document{
			ID:     fmt.Sprintf("batch_search_doc_%d", i),
			Vector: generateRandomVector(128, i),
		}
		coll.Insert(doc)
	}

	// Prepare queries
	queries := make([][]float32, 10)
	for i := range queries {
		queries[i] = generateRandomVector(128, 10000+i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := coll.SearchBatch(queries, 10)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// ==================== 不同规模搜索基准测试 ====================

// BenchmarkSearch1K benchmarks search on 1K documents
func BenchmarkSearch1K(b *testing.B) {
	benchmarkSearchWithSize(b, 1000, 128)
}

// BenchmarkSearch10K benchmarks search on 10K documents
func BenchmarkSearch10K(b *testing.B) {
	benchmarkSearchWithSize(b, 10000, 128)
}

// BenchmarkSearch100K benchmarks search on 100K documents
func BenchmarkSearch100K(b *testing.B) {
	benchmarkSearchWithSize(b, 100000, 128)
}

// benchmarkSearchWithSize helper function for different dataset sizes
func benchmarkSearchWithSize(b *testing.B, size, dimension int) {
	coll, cleanup := setupBenchmarkCollection(b, dimension)
	defer cleanup()

	b.Logf("Preparing %d documents...", size)
	start := time.Now()

	// Prepare data in batches for efficiency
	batchSize := 100
	for batch := 0; batch < size/batchSize; batch++ {
		docs := make([]*Document, batchSize)
		for i := 0; i < batchSize; i++ {
			docs[i] = &Document{
				ID:     fmt.Sprintf("scale_doc_%d", batch*batchSize+i),
				Vector: generateRandomVector(dimension, batch*batchSize+i),
			}
		}
		if err := coll.InsertBatch(docs); err != nil {
			b.Fatal(err)
		}
	}

	b.Logf("Prepared %d documents in %v", size, time.Since(start))

	query := generateRandomVector(dimension, 999999)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := coll.Search(query, 10)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSearchDifferentDimensions benchmarks search performance with different dimensions
func BenchmarkSearchDifferentDimensions(b *testing.B) {
	dimensions := []int{64, 128, 256, 512, 768, 1024}

	for _, dim := range dimensions {
		b.Run(fmt.Sprintf("Dim_%d", dim), func(b *testing.B) {
			benchmarkSearchWithSize(b, 1000, dim)
		})
	}
}

// ==================== 获取性能基准测试 ====================

// BenchmarkGet benchmarks single document retrieval
func BenchmarkGet(b *testing.B) {
	coll, cleanup := setupBenchmarkCollection(b, 128)
	defer cleanup()

	// Prepare data
	for i := 0; i < 1000; i++ {
		doc := &Document{
			ID:     fmt.Sprintf("get_doc_%d", i),
			Vector: generateRandomVector(128, i),
		}
		coll.Insert(doc)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := coll.Get(fmt.Sprintf("get_doc_%d", i%1000))
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkGetBatch benchmarks batch retrieval with different batch sizes
// Note: GetBatch is not yet implemented, this serves as a placeholder
func BenchmarkGetBatch(b *testing.B) {
	b.Skip("GetBatch not yet implemented")
	/*
	batchSizes := []int{10, 50, 100, 500}

	for _, size := range batchSizes {
		b.Run(fmt.Sprintf("BatchSize_%d", size), func(b *testing.B) {
			coll, cleanup := setupBenchmarkCollection(b, 128)
			defer cleanup()

			// Prepare data
			for i := 0; i < 1000; i++ {
				doc := &Document{
					ID:     fmt.Sprintf("getbatch_doc_%d", i),
					Vector: generateRandomVector(128, i),
				}
				coll.Insert(doc)
			}

			// Prepare ID batches
			ids := make([]string, size)
			for i := 0; i < size; i++ {
				ids[i] = fmt.Sprintf("getbatch_doc_%d", i)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := coll.GetBatch(ids)
				if err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(size), "docs/op")
		})
	}
	*/
}

// ==================== 更新和删除性能基准测试 ====================

// BenchmarkUpdate benchmarks document update
func BenchmarkUpdate(b *testing.B) {
	coll, cleanup := setupBenchmarkCollection(b, 128)
	defer cleanup()

	// Prepare data
	for i := 0; i < 1000; i++ {
		doc := &Document{
			ID:     fmt.Sprintf("update_doc_%d", i),
			Vector: generateRandomVector(128, i),
			Metadata: map[string]interface{}{
				"version": 1,
			},
		}
		coll.Insert(doc)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		doc, err := coll.Get(fmt.Sprintf("update_doc_%d", i%1000))
		if err != nil {
			b.Fatal(err)
		}
		doc.Metadata["version"] = doc.Metadata["version"].(int) + 1
		if err := coll.Update(doc); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDelete benchmarks document deletion
func BenchmarkDelete(b *testing.B) {
	// Prepare more data since we delete many
	sizes := []int{100, 500, 1000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size_%d", size), func(b *testing.B) {
			// Each iteration needs fresh data
			b.StopTimer()
			coll, cleanup := setupBenchmarkCollection(b, 128)
			defer cleanup()

			// Prepare data
			for i := 0; i < size; i++ {
				doc := &Document{
					ID:     fmt.Sprintf("delete_doc_%d", i),
					Vector: generateRandomVector(128, i),
				}
				coll.Insert(doc)
			}

			b.StartTimer()
			for i := 0; i < b.N && i < size; i++ {
				if err := coll.Delete(fmt.Sprintf("delete_doc_%d", i)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// ==================== 综合性能基准测试 ====================

// BenchmarkMixedWorkload benchmarks mixed read/write workload
func BenchmarkMixedWorkload(b *testing.B) {
	readRatios := []float64{0.9, 0.7, 0.5, 0.3, 0.1}

	for _, readRatio := range readRatios {
		b.Run(fmt.Sprintf("ReadRatio_%.0f%%", readRatio*100), func(b *testing.B) {
			coll, cleanup := setupBenchmarkCollection(b, 128)
			defer cleanup()

			// Prepare data
			for i := 0; i < 1000; i++ {
				doc := &Document{
					ID:     fmt.Sprintf("mixed_doc_%d", i),
					Vector: generateRandomVector(128, i),
				}
				coll.Insert(doc)
			}

			query := generateRandomVector(128, 9999)
			insertCounter := 1000

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if float64(i%100)/100 < readRatio {
					// Read operation: search
					_, err := coll.Search(query, 10)
					if err != nil {
						b.Fatal(err)
					}
				} else {
					// Write operation: insert
					doc := &Document{
						ID:     fmt.Sprintf("mixed_doc_%d", insertCounter),
						Vector: generateRandomVector(128, insertCounter),
					}
					if err := coll.Insert(doc); err != nil {
						b.Fatal(err)
					}
					insertCounter++
				}
			}
		})
	}
}

// BenchmarkCollectionMemoryUsage benchmarks memory usage at different scales
func BenchmarkCollectionMemoryUsage(b *testing.B) {
	sizes := []int{1000, 5000, 10000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size_%d", size), func(b *testing.B) {
			coll, cleanup := setupBenchmarkCollection(b, 128)
			defer cleanup()

			b.Logf("Inserting %d documents for memory benchmark...", size)

			batchSize := 100
			for batch := 0; batch < size/batchSize; batch++ {
				docs := make([]*Document, batchSize)
				for i := 0; i < batchSize; i++ {
					docs[i] = &Document{
						ID:       fmt.Sprintf("mem_doc_%d", batch*batchSize+i),
						Vector:   generateRandomVector(128, batch*batchSize+i),
						Metadata: map[string]interface{}{"index": batch*batchSize + i},
					}
				}
				if err := coll.InsertBatch(docs); err != nil {
					b.Fatal(err)
				}
			}

			stats := coll.Stats()
			b.ReportMetric(float64(stats.Count), "documents")

			// Perform searches to measure query performance at scale
			query := generateRandomVector(128, 999999)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := coll.Search(query, 10)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkSave benchmarks collection save performance
func BenchmarkSave(b *testing.B) {
	sizes := []int{100, 500, 1000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size_%d", size), func(b *testing.B) {
			coll, cleanup := setupBenchmarkCollection(b, 128)
			defer cleanup()

			// Prepare data
			for i := 0; i < size; i++ {
				doc := &Document{
					ID:     fmt.Sprintf("save_doc_%d", i),
					Vector: generateRandomVector(128, i),
				}
				coll.Insert(doc)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := coll.Save(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
