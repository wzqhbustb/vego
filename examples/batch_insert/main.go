// Batch Insert Example
// This example compares single insert vs batch insert performance.
//
// Run: go run main.go
package main

import (
	"fmt"
	"math/rand"
	"time"

	hnsw "github.com/wzqhbustb/vego/index"
)

func main() {
	fmt.Println("=== Vego Batch Insert Demo ===")
	fmt.Println()

	vectorCount := 5000
	dimension := 128

	// Generate random vectors first (shared between tests)
	fmt.Printf("Generating %d random vectors (dimension %d)...\n", vectorCount, dimension)
	vectors := make([][]float32, vectorCount)
	for i := 0; i < vectorCount; i++ {
		vec := make([]float32, dimension)
		for j := range vec {
			vec[j] = rand.Float32()
		}
		vectors[i] = vec
	}
	fmt.Println("✓ Vectors generated")
	fmt.Println()

	// Test 1: Single insert
	fmt.Println("Test 1: Single Insert (one by one)")
	config1 := hnsw.Config{
		Dimension:    dimension,
		Adaptive:     true,
		ExpectedSize: vectorCount,
	}
	index1 := hnsw.NewHNSW(config1)

	start := time.Now()
	for i, vec := range vectors {
		_, err := index1.Add(vec)
		if err != nil {
			panic(err)
		}
		if (i+1)%1000 == 0 {
			fmt.Printf("  → Inserted %d vectors...\n", i+1)
		}
	}
	elapsed1 := time.Since(start)

	fmt.Printf("✓ Single insert completed\n")
	fmt.Printf("  - Total time: %v\n", elapsed1)
	fmt.Printf("  - Vectors: %d\n", index1.Len())
	fmt.Printf("  - Rate: %.2f vectors/sec\n", float64(vectorCount)/elapsed1.Seconds())
	fmt.Println()

	// Test 2: Simulate batch (using the same Add API, but without locking overhead per call)
	// Note: HNSW index doesn't have a native batch API, so we demonstrate
	// the difference between creating index with different configurations
	fmt.Println("Test 2: Optimized Configuration for Batch Workload")
	
	// For batch workloads, we can pre-allocate by setting expected size
	config2 := hnsw.Config{
		Dimension:      dimension,
		Adaptive:       true,
		ExpectedSize:   vectorCount,
		// Pre-tune parameters for known workload
		M:              16,
		EfConstruction: 200,
	}
	index2 := hnsw.NewHNSW(config2)

	start = time.Now()
	for i, vec := range vectors {
		_, err := index2.Add(vec)
		if err != nil {
			panic(err)
		}
		if (i+1)%1000 == 0 {
			fmt.Printf("  → Inserted %d vectors...\n", i+1)
		}
	}
	elapsed2 := time.Since(start)

	fmt.Printf("✓ Optimized insert completed\n")
	fmt.Printf("  - Total time: %v\n", elapsed2)
	fmt.Printf("  - Vectors: %d\n", index2.Len())
	fmt.Printf("  - Rate: %.2f vectors/sec\n", float64(vectorCount)/elapsed2.Seconds())
	fmt.Println()

	// Compare search performance
	fmt.Println("Test 3: Search Performance Comparison")
	query := make([]float32, dimension)
	for j := range query {
		query[j] = rand.Float32()
	}

	// Warm up
	index1.Search(query, 10, 0)
	index2.Search(query, 10, 0)

	// Benchmark index1
	start = time.Now()
	for i := 0; i < 100; i++ {
		index1.Search(query, 10, 0)
	}
	searchTime1 := time.Since(start) / 100

	// Benchmark index2
	start = time.Now()
	for i := 0; i < 100; i++ {
		index2.Search(query, 10, 0)
	}
	searchTime2 := time.Since(start) / 100

	fmt.Printf("Average search latency:\n")
	fmt.Printf("  - Index 1: %v\n", searchTime1)
	fmt.Printf("  - Index 2: %v\n", searchTime2)
	fmt.Println()

	// Summary
	fmt.Println("=== Summary ===")
	fmt.Printf("Single insert:  %v (%.2f vec/sec)\n", elapsed1, float64(vectorCount)/elapsed1.Seconds())
	fmt.Printf("Optimized:      %v (%.2f vec/sec)\n", elapsed2, float64(vectorCount)/elapsed2.Seconds())
	if elapsed2 < elapsed1 {
		fmt.Printf("Improvement:    %.1f%% faster\n", 100.0*(float64(elapsed1)-float64(elapsed2))/float64(elapsed1))
	}
	fmt.Println()
	fmt.Println("Tips for batch insertion:")
	fmt.Println("  1. Set ExpectedSize accurately for better memory allocation")
	fmt.Println("  2. Use Adaptive mode to auto-tune parameters")
	fmt.Println("  3. Consider building index offline and loading for production")
	fmt.Println()
	fmt.Println("=== Demo completed! ===")
}
