package hnsw

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// BenchmarkConfig defines the configuration for benchmark tests
type BenchmarkConfig struct {
	DatasetSize      int
	Dimension        int
	M                int
	EfConstruction   int
	QueryEf          int
	TopK             int
	NumQueries       int
	DistanceFunc     DistanceFunc
	DistanceFuncName string // Added: explicit distance function name
	Concurrency      int    // Added: for concurrent tests
	UseAdaptive      bool   // Added: use adaptive configuration based on Dimension and DatasetSize
}

// BenchmarkResult stores the results of a benchmark run
type BenchmarkResult struct {
	Config          BenchmarkConfig
	BuildTime       time.Duration
	BuildQPS        float64
	SaveTime        time.Duration
	LoadTime        time.Duration
	QueryTime       time.Duration
	QueryQPS        float64
	AvgQueryLatency time.Duration
	P50Latency      time.Duration
	P95Latency      time.Duration
	P99Latency      time.Duration
	MemoryUsageMB   float64
	TotalAllocMB    float64 // Added: total allocation
	StorageSizeMB   float64
	Recall          float64
}

// generateRandomVectors generates random float32 vectors with uniform distribution
func generateRandomVectors(n, dim int, seed int64) [][]float32 {
	rng := rand.New(rand.NewSource(seed))
	vectors := make([][]float32, n)
	for i := 0; i < n; i++ {
		vectors[i] = make([]float32, dim)
		for j := 0; j < dim; j++ {
			vectors[i][j] = rng.Float32()*2 - 1 // Range [-1, 1]
		}
		// Normalize for better numerical stability
		norm := float32(0)
		for j := 0; j < dim; j++ {
			norm += vectors[i][j] * vectors[i][j]
		}
		norm = float32(math.Sqrt(float64(norm)))
		if norm > 1e-6 {
			for j := 0; j < dim; j++ {
				vectors[i][j] /= norm
			}
		}
	}
	return vectors
}

// generateGaussianVectors generates random vectors with Gaussian distribution (more realistic)
func generateGaussianVectors(n, dim int, seed int64) [][]float32 {
	rng := rand.New(rand.NewSource(seed))
	vectors := make([][]float32, n)
	for i := 0; i < n; i++ {
		vectors[i] = make([]float32, dim)
		for j := 0; j < dim; j++ {
			// Box-Muller transform for Gaussian distribution
			u1 := rng.Float32()
			u2 := rng.Float32()
			if u1 < 1e-10 {
				u1 = 1e-10 // Avoid log(0)
			}
			vectors[i][j] = float32(math.Sqrt(-2*math.Log(float64(u1))) *
				math.Cos(2*math.Pi*float64(u2)))
		}
		// Normalize
		norm := float32(0)
		for j := 0; j < dim; j++ {
			norm += vectors[i][j] * vectors[i][j]
		}
		norm = float32(math.Sqrt(float64(norm)))
		if norm > 1e-6 {
			for j := 0; j < dim; j++ {
				vectors[i][j] /= norm
			}
		}
	}
	return vectors
}

// bruteForceSearch2 performs exhaustive search for ground truth
func bruteForceSearch2(index *HNSWIndex, query []float32, k int) []SearchResult {
	index.globalLock.RLock()
	defer index.globalLock.RUnlock()

	results := make([]SearchResult, 0, len(index.nodes))
	for id, node := range index.nodes {
		if node != nil {
			dist := index.distFunc(query, node.vector)
			results = append(results, SearchResult{ID: id, Distance: dist})
		}
	}

	// Sort by distance (ascending)
	sort.Slice(results, func(i, j int) bool {
		return results[i].Distance < results[j].Distance
	})

	if len(results) > k {
		results = results[:k]
	}
	return results
}

// computeGroundTruthParallel computes ground truth in parallel for better performance
func computeGroundTruthParallel(index *HNSWIndex, queries [][]float32, k int) [][]SearchResult {
	results := make([][]SearchResult, len(queries))

	// Use worker pool to avoid goroutine explosion
	numWorkers := runtime.GOMAXPROCS(0)
	jobs := make(chan int, len(queries))
	var wg sync.WaitGroup

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				results[i] = bruteForceSearch2(index, queries[i], k)
			}
		}()
	}

	for i := range queries {
		jobs <- i
	}
	close(jobs)
	wg.Wait()

	return results
}

// calculateRecall2 calculates the recall@k
func calculateRecall2(groundTruth, results []SearchResult) float64 {
	if len(groundTruth) == 0 || len(results) == 0 {
		return 0
	}

	gtSet := make(map[int]bool)
	for _, r := range groundTruth {
		gtSet[r.ID] = true
	}

	hits := 0
	for _, r := range results {
		if gtSet[r.ID] {
			hits++
		}
	}

	return float64(hits) / float64(len(groundTruth))
}

// getMemoryUsageMB returns current and total memory usage in MB
func getMemoryUsageMB() (alloc, totalAlloc float64) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return float64(m.Alloc) / (1024 * 1024),
		float64(m.TotalAlloc) / (1024 * 1024)
}

// getStorageSize calculates total storage size in bytes
func getStorageSize(path string) (int64, error) {
	var totalSize int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			totalSize += info.Size()
		}
		return nil
	})
	return totalSize, err
}

// runBenchmark executes a complete benchmark with the given configuration
func runBenchmark(b *testing.B, config BenchmarkConfig) *BenchmarkResult {
	b.Helper()

	result := &BenchmarkResult{Config: config}
	tempDir := b.TempDir()
	storagePath := filepath.Join(tempDir, "index")

	// Generate test data
	b.Logf("Generating %d vectors of dimension %d...", config.DatasetSize, config.Dimension)
	vectors := generateRandomVectors(config.DatasetSize, config.Dimension, 42)
	queryVectors := generateRandomVectors(config.NumQueries, config.Dimension, 123)

	// Phase 1: Build index
	var indexConfig Config
	if config.UseAdaptive {
		b.Logf("Building index with adaptive config (Dimension=%d, ExpectedSize=%d)...", config.Dimension, config.DatasetSize)
		indexConfig = Config{
			Dimension:    config.Dimension,
			Adaptive:     true,
			ExpectedSize: config.DatasetSize,
			DistanceFunc: config.DistanceFunc,
			Seed:         42,
		}
		// Log actual values after adaptive calculation (NewHNSW will compute these)
	} else {
		b.Logf("Building index with M=%d, EfConstruction=%d...", config.M, config.EfConstruction)
		indexConfig = Config{
			Dimension:      config.Dimension,
			M:              config.M,
			EfConstruction: config.EfConstruction,
			DistanceFunc:   config.DistanceFunc,
			Seed:           42,
		}
	}

	runtime.GC()
	time.Sleep(10 * time.Millisecond) // Let GC settle
	memBefore, totalBefore := getMemoryUsageMB()

	index := NewHNSW(indexConfig)

	// 更新 result.Config 为实际使用的参数（从索引读取）
	result.Config.M = index.M
	result.Config.EfConstruction = index.efConstruction

	buildStart := time.Now()

	for i, vec := range vectors {
		_, err := index.Add(vec)
		if err != nil {
			b.Fatalf("Failed to add vector %d: %v", i, err)
		}
		if (i+1)%1000 == 0 {
			b.Logf("  Added %d/%d vectors...", i+1, config.DatasetSize)
		}
	}

	result.BuildTime = time.Since(buildStart)
	result.BuildQPS = float64(config.DatasetSize) / result.BuildTime.Seconds()
	memAfter, totalAfter := getMemoryUsageMB()
	result.MemoryUsageMB = memAfter - memBefore
	result.TotalAllocMB = totalAfter - totalBefore

	b.Logf("Build completed in %v (%.2f vectors/sec, %.2f MB memory, %.2f MB total alloc)",
		result.BuildTime, result.BuildQPS, result.MemoryUsageMB, result.TotalAllocMB)

	// Phase 2: Save to storage
	b.Logf("Saving index to storage...")
	saveStart := time.Now()
	if err := index.SaveToLance(storagePath); err != nil {
		b.Fatalf("Failed to save index: %v", err)
	}
	result.SaveTime = time.Since(saveStart)

	storageSize, err := getStorageSize(storagePath)
	if err != nil {
		b.Fatalf("Failed to get storage size: %v", err)
	}
	result.StorageSizeMB = float64(storageSize) / (1024 * 1024)

	b.Logf("Save completed in %v (%.2f MB on disk)", result.SaveTime, result.StorageSizeMB)

	// Clear memory to test load from cold start
	index = nil
	runtime.GC()
	time.Sleep(10 * time.Millisecond)

	// Phase 3: Load from storage
	b.Logf("Loading index from storage...")
	loadStart := time.Now()
	loadedIndex, err := LoadHNSWFromLance(storagePath)
	if err != nil {
		b.Fatalf("Failed to load index: %v", err)
	}
	result.LoadTime = time.Since(loadStart)
	b.Logf("Load completed in %v", result.LoadTime)

	// Verify loaded index
	if len(loadedIndex.nodes) != config.DatasetSize {
		b.Fatalf("Loaded index has %d nodes, expected %d", len(loadedIndex.nodes), config.DatasetSize)
	}

	// Phase 4: Compute ground truth for recall (use subset for large datasets)
	numGroundTruth := config.NumQueries
	if numGroundTruth > 100 {
		numGroundTruth = 100
	}
	// For very large datasets, use even fewer ground truth queries
	if config.DatasetSize > 50000 {
		numGroundTruth = 50
	}

	b.Logf("Computing ground truth for %d queries (parallel)...", numGroundTruth)
	gtStart := time.Now()
	groundTruths := computeGroundTruthParallel(loadedIndex, queryVectors[:numGroundTruth], config.TopK)
	b.Logf("Ground truth computed in %v", time.Since(gtStart))

	// Phase 5: Query performance
	b.Logf("Running %d queries with ef=%d, k=%d...", config.NumQueries, config.QueryEf, config.TopK)

	// Warm up: ensure CPU caches are hot and GC doesn't skew results
	warmupQueries := 10
	if config.DatasetSize > 50000 {
		warmupQueries = 50
	}
	b.Logf("Warming up with %d queries...", warmupQueries)
	for i := 0; i < warmupQueries && i < config.NumQueries; i++ {
		_, _ = loadedIndex.Search(queryVectors[i], config.TopK, config.QueryEf)
	}
	runtime.GC() // Final GC before measurement

	// Measure query performance
	latencies := make([]time.Duration, config.NumQueries)
	totalRecall := 0.0

	if config.Concurrency > 1 {
		// Concurrent benchmark
		b.Logf("Running concurrent queries with %d workers...", config.Concurrency)
		queriesPerWorker := config.NumQueries / config.Concurrency
		var wg sync.WaitGroup

		queryStart := time.Now()
		for w := 0; w < config.Concurrency; w++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				startIdx := workerID * queriesPerWorker
				endIdx := startIdx + queriesPerWorker
				if workerID == config.Concurrency-1 {
					endIdx = config.NumQueries // Last worker takes remainder
				}

				for i := startIdx; i < endIdx; i++ {
					start := time.Now()
					results, err := loadedIndex.Search(queryVectors[i], config.TopK, config.QueryEf)
					latencies[i] = time.Since(start)

					if err != nil {
						b.Errorf("Query %d failed: %v", i, err)
						return
					}

					// Calculate recall for subset
					if i < numGroundTruth {
						recall := calculateRecall2(groundTruths[i], results)
						totalRecall += recall
					}
				}
			}(w)
		}
		wg.Wait()
		result.QueryTime = time.Since(queryStart)
	} else {
		// Sequential benchmark
		queryStart := time.Now()
		for i := 0; i < config.NumQueries; i++ {
			start := time.Now()
			results, err := loadedIndex.Search(queryVectors[i], config.TopK, config.QueryEf)
			latencies[i] = time.Since(start)

			if err != nil {
				b.Fatalf("Query %d failed: %v", i, err)
			}

			// Calculate recall for subset
			if i < numGroundTruth {
				recall := calculateRecall2(groundTruths[i], results)
				totalRecall += recall
			}

			if (i+1)%100 == 0 {
				b.Logf("  Completed %d/%d queries...", i+1, config.NumQueries)
			}
		}
		result.QueryTime = time.Since(queryStart)
	}

	result.QueryQPS = float64(config.NumQueries) / result.QueryTime.Seconds()
	result.Recall = totalRecall / float64(numGroundTruth)

	// Calculate latency percentiles
	sortedLatencies := make([]time.Duration, len(latencies))
	copy(sortedLatencies, latencies)
	sort.Slice(sortedLatencies, func(i, j int) bool {
		return sortedLatencies[i] < sortedLatencies[j]
	})

	result.AvgQueryLatency = result.QueryTime / time.Duration(config.NumQueries)
	result.P50Latency = sortedLatencies[len(sortedLatencies)*50/100]
	result.P95Latency = sortedLatencies[len(sortedLatencies)*95/100]
	result.P99Latency = sortedLatencies[len(sortedLatencies)*99/100]

	b.Logf("Query completed: %.2f qps, Recall@%d: %.4f (%.2f%%)",
		result.QueryQPS, config.TopK, result.Recall, result.Recall*100)
	b.Logf("Latency - Avg: %v, P50: %v, P95: %v, P99: %v",
		result.AvgQueryLatency, result.P50Latency, result.P95Latency, result.P99Latency)

	return result
}

// Benchmark suite for different scales
func BenchmarkHNSW_E2E_1K_D128(b *testing.B) {
	config := BenchmarkConfig{
		DatasetSize:      1000,
		Dimension:        128,
		M:                16,
		EfConstruction:   200,
		QueryEf:          100,
		TopK:             10,
		NumQueries:       500,
		DistanceFunc:     L2Distance,
		DistanceFuncName: "L2",
		Concurrency:      1,
	}
	result := runBenchmark(b, config)
	printBenchmarkResult(b, result)
}

func BenchmarkHNSW_E2E_10K_D128(b *testing.B) {
	config := BenchmarkConfig{
		DatasetSize:      10000,
		Dimension:        128,
		M:                16,
		EfConstruction:   200,
		QueryEf:          100,
		TopK:             10,
		NumQueries:       1000,
		DistanceFunc:     L2Distance,
		DistanceFuncName: "L2",
		Concurrency:      1,
	}
	result := runBenchmark(b, config)
	printBenchmarkResult(b, result)
}

func BenchmarkHNSW_E2E_10K_D128Adaptive(b *testing.B) {
	config := BenchmarkConfig{
		DatasetSize:      10000,
		Dimension:        128,
		M:                0,
		EfConstruction:   0,
		QueryEf:          100,
		TopK:             10,
		NumQueries:       1000,
		DistanceFunc:     L2Distance,
		DistanceFuncName: "L2",
		Concurrency:      1,
		UseAdaptive:      true,
	}
	result := runBenchmark(b, config)
	printBenchmarkResult(b, result)
}

// go test -v -bench=^BenchmarkHNSW_E2E_100K_D128$ -benchtime=1x -timeout=20m
func BenchmarkHNSW_E2E_100K_D128(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping large benchmark in short mode")
	}

	config := BenchmarkConfig{
		DatasetSize:      100000,
		Dimension:        128,
		M:                0,
		EfConstruction:   0,
		QueryEf:          300,
		TopK:             10,
		NumQueries:       1000,
		DistanceFunc:     L2Distance,
		DistanceFuncName: "L2",
		Concurrency:      1,
		UseAdaptive:      true, // Enable adaptive config for large dataset
	}
	result := runBenchmark(b, config)
	printBenchmarkResult(b, result)
}

// Concurrent benchmarks
func BenchmarkHNSW_E2E_10K_D128_Concurrent(b *testing.B) {
	baseConfig := BenchmarkConfig{
		DatasetSize:      10000,
		Dimension:        128,
		M:                0,
		EfConstruction:   0,
		QueryEf:          100,
		TopK:             10,
		NumQueries:       1000,
		DistanceFunc:     L2Distance,
		DistanceFuncName: "L2",
		UseAdaptive:      true, // Enable adaptive config for better recall
	}

	for _, concurrency := range []int{1, 2, 4, 8, 16} {
		config := baseConfig
		config.Concurrency = concurrency
		b.Run(fmt.Sprintf("C%d", concurrency), func(b *testing.B) {
			result := runBenchmark(b, config)
			printBenchmarkResult(b, result)
		})
	}
}

// Dimension benchmarks
// go test -v -bench=^BenchmarkHNSW_E2E_10K_D256$ -benchtime=1x -timeout=20m
func BenchmarkHNSW_E2E_10K_D256(b *testing.B) {
	config := BenchmarkConfig{
		DatasetSize:      10000,
		Dimension:        256,
		M:                16,
		EfConstruction:   200,
		QueryEf:          100,
		TopK:             10,
		NumQueries:       1000,
		DistanceFunc:     L2Distance,
		DistanceFuncName: "L2",
		Concurrency:      1,
	}
	result := runBenchmark(b, config)
	printBenchmarkResult(b, result)
}

func BenchmarkHNSW_E2E_10K_D512(b *testing.B) {
	config := BenchmarkConfig{
		DatasetSize:      10000,
		Dimension:        512,
		M:                16,
		EfConstruction:   200,
		QueryEf:          100,
		TopK:             10,
		NumQueries:       1000,
		DistanceFunc:     L2Distance,
		DistanceFuncName: "L2",
		Concurrency:      1,
	}
	result := runBenchmark(b, config)
	printBenchmarkResult(b, result)
}

// go test -v -bench=^BenchmarkHNSW_E2E_10K_D768$ -benchtime=1x -timeout=20m
func BenchmarkHNSW_E2E_10K_D768(b *testing.B) {
	config := BenchmarkConfig{
		DatasetSize:      10000,
		Dimension:        768,
		M:                0,
		EfConstruction:   0,
		QueryEf:          100,
		TopK:             10,
		NumQueries:       1000,
		DistanceFunc:     L2Distance,
		DistanceFuncName: "L2",
		Concurrency:      1,
		UseAdaptive:      true, // Enable adaptive config for high dimension (BERT)
	}
	result := runBenchmark(b, config)
	printBenchmarkResult(b, result)
}

func BenchmarkHNSW_E2E_10K_D1536(b *testing.B) {
	config := BenchmarkConfig{
		DatasetSize:      10000,
		Dimension:        1536,
		M:                0,
		EfConstruction:   0,
		QueryEf:          100,
		TopK:             10,
		NumQueries:       1000,
		DistanceFunc:     L2Distance,
		DistanceFuncName: "L2",
		Concurrency:      1,
		UseAdaptive:      true, // Enable adaptive config for high dimension
	}
	result := runBenchmark(b, config)
	printBenchmarkResult(b, result)
}

// Distance function benchmarks
func BenchmarkHNSW_E2E_10K_Cosine(b *testing.B) {
	config := BenchmarkConfig{
		DatasetSize:      10000,
		Dimension:        128,
		M:                16,
		EfConstruction:   200,
		QueryEf:          100,
		TopK:             10,
		NumQueries:       1000,
		DistanceFunc:     CosineDistance,
		DistanceFuncName: "Cosine",
		Concurrency:      1,
	}
	result := runBenchmark(b, config)
	printBenchmarkResult(b, result)
}

func BenchmarkHNSW_E2E_10K_InnerProduct(b *testing.B) {
	config := BenchmarkConfig{
		DatasetSize:      10000,
		Dimension:        128,
		M:                16,
		EfConstruction:   200,
		QueryEf:          100,
		TopK:             10,
		NumQueries:       1000,
		DistanceFunc:     InnerProductDistance,
		DistanceFuncName: "InnerProduct",
		Concurrency:      1,
	}
	result := runBenchmark(b, config)
	printBenchmarkResult(b, result)
}

// Parameter tuning benchmarks - M values
func BenchmarkHNSW_E2E_10K_M8(b *testing.B) {
	config := BenchmarkConfig{
		DatasetSize:      10000,
		Dimension:        128,
		M:                8,
		EfConstruction:   200,
		QueryEf:          100,
		TopK:             10,
		NumQueries:       1000,
		DistanceFunc:     L2Distance,
		DistanceFuncName: "L2",
		Concurrency:      1,
	}
	result := runBenchmark(b, config)
	printBenchmarkResult(b, result)
}

func BenchmarkHNSW_E2E_10K_M32(b *testing.B) {
	config := BenchmarkConfig{
		DatasetSize:      10000,
		Dimension:        128,
		M:                32,
		EfConstruction:   200,
		QueryEf:          100,
		TopK:             10,
		NumQueries:       1000,
		DistanceFunc:     L2Distance,
		DistanceFuncName: "L2",
		Concurrency:      1,
	}
	result := runBenchmark(b, config)
	printBenchmarkResult(b, result)
}

func BenchmarkHNSW_E2E_10K_M64(b *testing.B) {
	config := BenchmarkConfig{
		DatasetSize:      10000,
		Dimension:        128,
		M:                64,
		EfConstruction:   200,
		QueryEf:          100,
		TopK:             10,
		NumQueries:       1000,
		DistanceFunc:     L2Distance,
		DistanceFuncName: "L2",
		Concurrency:      1,
	}
	result := runBenchmark(b, config)
	printBenchmarkResult(b, result)
}

// Parameter tuning benchmarks - Query Ef values
func BenchmarkHNSW_E2E_10K_Ef50(b *testing.B) {
	config := BenchmarkConfig{
		DatasetSize:      10000,
		Dimension:        128,
		M:                16,
		EfConstruction:   200,
		QueryEf:          50,
		TopK:             10,
		NumQueries:       1000,
		DistanceFunc:     L2Distance,
		DistanceFuncName: "L2",
		Concurrency:      1,
	}
	result := runBenchmark(b, config)
	printBenchmarkResult(b, result)
}

func BenchmarkHNSW_E2E_10K_Ef200(b *testing.B) {
	config := BenchmarkConfig{
		DatasetSize:      10000,
		Dimension:        128,
		M:                16,
		EfConstruction:   200,
		QueryEf:          200,
		TopK:             10,
		NumQueries:       1000,
		DistanceFunc:     L2Distance,
		DistanceFuncName: "L2",
		Concurrency:      1,
	}
	result := runBenchmark(b, config)
	printBenchmarkResult(b, result)
}

func BenchmarkHNSW_E2E_10K_Ef400(b *testing.B) {
	config := BenchmarkConfig{
		DatasetSize:      10000,
		Dimension:        128,
		M:                16,
		EfConstruction:   200,
		QueryEf:          400,
		TopK:             10,
		NumQueries:       1000,
		DistanceFunc:     L2Distance,
		DistanceFuncName: "L2",
		Concurrency:      1,
	}
	result := runBenchmark(b, config)
	printBenchmarkResult(b, result)
}

// TopK benchmarks
func BenchmarkHNSW_E2E_10K_Top1(b *testing.B) {
	config := BenchmarkConfig{
		DatasetSize:      10000,
		Dimension:        128,
		M:                16,
		EfConstruction:   200,
		QueryEf:          100,
		TopK:             1,
		NumQueries:       1000,
		DistanceFunc:     L2Distance,
		DistanceFuncName: "L2",
		Concurrency:      1,
	}
	result := runBenchmark(b, config)
	printBenchmarkResult(b, result)
}

func BenchmarkHNSW_E2E_10K_Top50(b *testing.B) {
	config := BenchmarkConfig{
		DatasetSize:      10000,
		Dimension:        128,
		M:                16,
		EfConstruction:   200,
		QueryEf:          100,
		TopK:             50,
		NumQueries:       1000,
		DistanceFunc:     L2Distance,
		DistanceFuncName: "L2",
		Concurrency:      1,
	}
	result := runBenchmark(b, config)
	printBenchmarkResult(b, result)
}

func BenchmarkHNSW_E2E_10K_Top100(b *testing.B) {
	config := BenchmarkConfig{
		DatasetSize:      10000,
		Dimension:        128,
		M:                16,
		EfConstruction:   200,
		QueryEf:          100,
		TopK:             100,
		NumQueries:       1000,
		DistanceFunc:     L2Distance,
		DistanceFuncName: "L2",
		Concurrency:      1,
	}
	result := runBenchmark(b, config)
	printBenchmarkResult(b, result)
}

// printBenchmarkResult prints the benchmark results in a formatted table
func printBenchmarkResult(b *testing.B, result *BenchmarkResult) {
	b.Helper()

	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("BENCHMARK RESULTS")
	fmt.Println(strings.Repeat("=", 80))

	fmt.Println("\nConfiguration:")
	fmt.Printf("  Dataset Size:     %d vectors\n", result.Config.DatasetSize)
	fmt.Printf("  Dimension:        %d\n", result.Config.Dimension)
	fmt.Printf("  M:                %d\n", result.Config.M)
	fmt.Printf("  EfConstruction:   %d\n", result.Config.EfConstruction)
	fmt.Printf("  Query Ef:         %d\n", result.Config.QueryEf)
	fmt.Printf("  Top-K:            %d\n", result.Config.TopK)
	fmt.Printf("  Num Queries:      %d\n", result.Config.NumQueries)
	fmt.Printf("  Distance Func:    %s\n", result.Config.DistanceFuncName)
	if result.Config.Concurrency > 1 {
		fmt.Printf("  Concurrency:      %d\n", result.Config.Concurrency)
	}

	fmt.Println("\nBuild Performance:")
	fmt.Printf("  Time:             %v\n", result.BuildTime)
	fmt.Printf("  Throughput:       %.2f vectors/sec\n", result.BuildQPS)
	fmt.Printf("  Memory Usage:     %.2f MB (%.2f MB total alloc)\n",
		result.MemoryUsageMB, result.TotalAllocMB)

	fmt.Println("\nPersistence Performance:")
	fmt.Printf("  Save Time:        %v\n", result.SaveTime)
	fmt.Printf("  Load Time:        %v\n", result.LoadTime)
	fmt.Printf("  Storage Size:     %.2f MB\n", result.StorageSizeMB)
	compressionRatio := float64(result.Config.DatasetSize*result.Config.Dimension*4) / (result.StorageSizeMB * 1024 * 1024)
	fmt.Printf("  Compression:      %.2fx\n", compressionRatio)

	fmt.Println("\nQuery Performance:")
	fmt.Printf("  Total Time:       %v\n", result.QueryTime)
	fmt.Printf("  Throughput:       %.2f queries/sec\n", result.QueryQPS)
	if result.Config.Concurrency > 1 {
		fmt.Printf("  Per-Thread QPS:   %.2f queries/sec\n",
			result.QueryQPS/float64(result.Config.Concurrency))
	}
	fmt.Printf("  Avg Latency:      %v\n", result.AvgQueryLatency)
	fmt.Printf("  P50 Latency:      %v\n", result.P50Latency)
	fmt.Printf("  P95 Latency:      %v\n", result.P95Latency)
	fmt.Printf("  P99 Latency:      %v\n", result.P99Latency)
	fmt.Printf("  P99/P50 Ratio:    %.2fx\n",
		float64(result.P99Latency)/float64(result.P50Latency))
	fmt.Printf("  Recall@%d:        %.4f (%.2f%%)\n",
		result.Config.TopK, result.Recall, result.Recall*100)

	fmt.Println(strings.Repeat("=", 80))
}
