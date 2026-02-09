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
	"testing"
	"time"
)

// BenchmarkConfig defines the configuration for benchmark tests
type BenchmarkConfig struct {
	DatasetSize    int
	Dimension      int
	M              int
	EfConstruction int
	QueryEf        int
	TopK           int
	NumQueries     int
	DistanceFunc   DistanceFunc
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
	StorageSizeMB   float64
	Recall          float64
}

// generateRandomVectors generates random float32 vectors
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

// getMemoryUsageMB returns current memory usage in MB
func getMemoryUsageMB() float64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return float64(m.Alloc) / (1024 * 1024)
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
	b.Logf("Building index with M=%d, EfConstruction=%d...", config.M, config.EfConstruction)
	indexConfig := Config{
		Dimension:      config.Dimension,
		M:              config.M,
		EfConstruction: config.EfConstruction,
		DistanceFunc:   config.DistanceFunc,
		Seed:           42,
	}

	runtime.GC()
	memBefore := getMemoryUsageMB()

	index := NewHNSW(indexConfig)
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
	result.MemoryUsageMB = getMemoryUsageMB() - memBefore

	b.Logf("Build completed in %v (%.2f vectors/sec, %.2f MB memory)",
		result.BuildTime, result.BuildQPS, result.MemoryUsageMB)

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
	b.Logf("Computing ground truth for %d queries...", numGroundTruth)
	groundTruths := make([][]SearchResult, numGroundTruth)
	for i := 0; i < numGroundTruth; i++ {
		groundTruths[i] = bruteForceSearch2(loadedIndex, queryVectors[i], config.TopK)
	}

	// Phase 5: Query performance
	b.Logf("Running %d queries with ef=%d, k=%d...", config.NumQueries, config.QueryEf, config.TopK)

	// Warm up
	for i := 0; i < 10 && i < config.NumQueries; i++ {
		_, _ = loadedIndex.Search(queryVectors[i], config.TopK, config.QueryEf)
	}

	// Measure query performance
	latencies := make([]time.Duration, config.NumQueries)
	totalRecall := 0.0

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
		DatasetSize:    1000,
		Dimension:      128,
		M:              16,
		EfConstruction: 200,
		QueryEf:        100,
		TopK:           10,
		NumQueries:     500,
		DistanceFunc:   L2Distance,
	}
	result := runBenchmark(b, config)
	printBenchmarkResult(b, result)
}

func BenchmarkHNSW_E2E_10K_D128(b *testing.B) {
	config := BenchmarkConfig{
		DatasetSize:    10000,
		Dimension:      128,
		M:              16,
		EfConstruction: 200,
		QueryEf:        100,
		TopK:           10,
		NumQueries:     1000,
		DistanceFunc:   L2Distance,
	}
	result := runBenchmark(b, config)
	printBenchmarkResult(b, result)
}

func BenchmarkHNSW_E2E_100K_D128(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping large benchmark in short mode")
	}
	config := BenchmarkConfig{
		DatasetSize:    100000,
		Dimension:      128,
		M:              16,
		EfConstruction: 200,
		QueryEf:        100,
		TopK:           10,
		NumQueries:     1000,
		DistanceFunc:   L2Distance,
	}
	result := runBenchmark(b, config)
	printBenchmarkResult(b, result)
}

// Dimension benchmarks
func BenchmarkHNSW_E2E_10K_D256(b *testing.B) {
	config := BenchmarkConfig{
		DatasetSize:    10000,
		Dimension:      256,
		M:              16,
		EfConstruction: 200,
		QueryEf:        100,
		TopK:           10,
		NumQueries:     1000,
		DistanceFunc:   L2Distance,
	}
	result := runBenchmark(b, config)
	printBenchmarkResult(b, result)
}

func BenchmarkHNSW_E2E_10K_D512(b *testing.B) {
	config := BenchmarkConfig{
		DatasetSize:    10000,
		Dimension:      512,
		M:              16,
		EfConstruction: 200,
		QueryEf:        100,
		TopK:           10,
		NumQueries:     1000,
		DistanceFunc:   L2Distance,
	}
	result := runBenchmark(b, config)
	printBenchmarkResult(b, result)
}

func BenchmarkHNSW_E2E_10K_D768(b *testing.B) {
	config := BenchmarkConfig{
		DatasetSize:    10000,
		Dimension:      768,
		M:              16,
		EfConstruction: 200,
		QueryEf:        100,
		TopK:           10,
		NumQueries:     1000,
		DistanceFunc:   L2Distance,
	}
	result := runBenchmark(b, config)
	printBenchmarkResult(b, result)
}

func BenchmarkHNSW_E2E_10K_D1536(b *testing.B) {
	config := BenchmarkConfig{
		DatasetSize:    10000,
		Dimension:      1536,
		M:              16,
		EfConstruction: 200,
		QueryEf:        100,
		TopK:           10,
		NumQueries:     1000,
		DistanceFunc:   L2Distance,
	}
	result := runBenchmark(b, config)
	printBenchmarkResult(b, result)
}

// Distance function benchmarks
func BenchmarkHNSW_E2E_10K_Cosine(b *testing.B) {
	config := BenchmarkConfig{
		DatasetSize:    10000,
		Dimension:      128,
		M:              16,
		EfConstruction: 200,
		QueryEf:        100,
		TopK:           10,
		NumQueries:     1000,
		DistanceFunc:   CosineDistance,
	}
	result := runBenchmark(b, config)
	printBenchmarkResult(b, result)
}

func BenchmarkHNSW_E2E_10K_InnerProduct(b *testing.B) {
	config := BenchmarkConfig{
		DatasetSize:    10000,
		Dimension:      128,
		M:              16,
		EfConstruction: 200,
		QueryEf:        100,
		TopK:           10,
		NumQueries:     1000,
		DistanceFunc:   InnerProductDistance,
	}
	result := runBenchmark(b, config)
	printBenchmarkResult(b, result)
}

// Parameter tuning benchmarks - M values
func BenchmarkHNSW_E2E_10K_M8(b *testing.B) {
	config := BenchmarkConfig{
		DatasetSize:    10000,
		Dimension:      128,
		M:              8,
		EfConstruction: 200,
		QueryEf:        100,
		TopK:           10,
		NumQueries:     1000,
		DistanceFunc:   L2Distance,
	}
	result := runBenchmark(b, config)
	printBenchmarkResult(b, result)
}

func BenchmarkHNSW_E2E_10K_M32(b *testing.B) {
	config := BenchmarkConfig{
		DatasetSize:    10000,
		Dimension:      128,
		M:              32,
		EfConstruction: 200,
		QueryEf:        100,
		TopK:           10,
		NumQueries:     1000,
		DistanceFunc:   L2Distance,
	}
	result := runBenchmark(b, config)
	printBenchmarkResult(b, result)
}

func BenchmarkHNSW_E2E_10K_M64(b *testing.B) {
	config := BenchmarkConfig{
		DatasetSize:    10000,
		Dimension:      128,
		M:              64,
		EfConstruction: 200,
		QueryEf:        100,
		TopK:           10,
		NumQueries:     1000,
		DistanceFunc:   L2Distance,
	}
	result := runBenchmark(b, config)
	printBenchmarkResult(b, result)
}

// Parameter tuning benchmarks - Query Ef values
func BenchmarkHNSW_E2E_10K_Ef50(b *testing.B) {
	config := BenchmarkConfig{
		DatasetSize:    10000,
		Dimension:      128,
		M:              16,
		EfConstruction: 200,
		QueryEf:        50,
		TopK:           10,
		NumQueries:     1000,
		DistanceFunc:   L2Distance,
	}
	result := runBenchmark(b, config)
	printBenchmarkResult(b, result)
}

func BenchmarkHNSW_E2E_10K_Ef200(b *testing.B) {
	config := BenchmarkConfig{
		DatasetSize:    10000,
		Dimension:      128,
		M:              16,
		EfConstruction: 200,
		QueryEf:        200,
		TopK:           10,
		NumQueries:     1000,
		DistanceFunc:   L2Distance,
	}
	result := runBenchmark(b, config)
	printBenchmarkResult(b, result)
}

func BenchmarkHNSW_E2E_10K_Ef400(b *testing.B) {
	config := BenchmarkConfig{
		DatasetSize:    10000,
		Dimension:      128,
		M:              16,
		EfConstruction: 200,
		QueryEf:        400,
		TopK:           10,
		NumQueries:     1000,
		DistanceFunc:   L2Distance,
	}
	result := runBenchmark(b, config)
	printBenchmarkResult(b, result)
}

// TopK benchmarks
func BenchmarkHNSW_E2E_10K_Top1(b *testing.B) {
	config := BenchmarkConfig{
		DatasetSize:    10000,
		Dimension:      128,
		M:              16,
		EfConstruction: 200,
		QueryEf:        100,
		TopK:           1,
		NumQueries:     1000,
		DistanceFunc:   L2Distance,
	}
	result := runBenchmark(b, config)
	printBenchmarkResult(b, result)
}

func BenchmarkHNSW_E2E_10K_Top50(b *testing.B) {
	config := BenchmarkConfig{
		DatasetSize:    10000,
		Dimension:      128,
		M:              16,
		EfConstruction: 200,
		QueryEf:        100,
		TopK:           50,
		NumQueries:     1000,
		DistanceFunc:   L2Distance,
	}
	result := runBenchmark(b, config)
	printBenchmarkResult(b, result)
}

func BenchmarkHNSW_E2E_10K_Top100(b *testing.B) {
	config := BenchmarkConfig{
		DatasetSize:    10000,
		Dimension:      128,
		M:              16,
		EfConstruction: 200,
		QueryEf:        100,
		TopK:           100,
		NumQueries:     1000,
		DistanceFunc:   L2Distance,
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

	distName := "L2"
	if result.Config.DistanceFunc != nil {
		switch fmt.Sprintf("%p", result.Config.DistanceFunc) {
		case fmt.Sprintf("%p", CosineDistance):
			distName = "Cosine"
		case fmt.Sprintf("%p", InnerProductDistance):
			distName = "InnerProduct"
		}
	}
	fmt.Printf("  Distance Func:    %s\n", distName)

	fmt.Println("\nBuild Performance:")
	fmt.Printf("  Time:             %v\n", result.BuildTime)
	fmt.Printf("  Throughput:       %.2f vectors/sec\n", result.BuildQPS)
	fmt.Printf("  Memory Usage:     %.2f MB\n", result.MemoryUsageMB)

	fmt.Println("\nPersistence Performance:")
	fmt.Printf("  Save Time:        %v\n", result.SaveTime)
	fmt.Printf("  Load Time:        %v\n", result.LoadTime)
	fmt.Printf("  Storage Size:     %.2f MB\n", result.StorageSizeMB)
	compressionRatio := float64(result.Config.DatasetSize*result.Config.Dimension*4) / (result.StorageSizeMB * 1024 * 1024)
	fmt.Printf("  Compression:      %.2fx\n", compressionRatio)

	fmt.Println("\nQuery Performance:")
	fmt.Printf("  Total Time:       %v\n", result.QueryTime)
	fmt.Printf("  Throughput:       %.2f queries/sec\n", result.QueryQPS)
	fmt.Printf("  Avg Latency:      %v\n", result.AvgQueryLatency)
	fmt.Printf("  P50 Latency:      %v\n", result.P50Latency)
	fmt.Printf("  P95 Latency:      %v\n", result.P95Latency)
	fmt.Printf("  P99 Latency:      %v\n", result.P99Latency)
	fmt.Printf("  Recall@%d:        %.4f (%.2f%%)\n", result.Config.TopK, result.Recall, result.Recall*100)

	fmt.Println(strings.Repeat("=", 80))
}
