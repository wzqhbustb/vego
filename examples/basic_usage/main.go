// Basic Usage Example
// This example demonstrates the basic usage of HNSW index for vector search.
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
	fmt.Println("=== Vego Basic Usage Demo ===")
	fmt.Println()

	// Step 1: Create index with adaptive configuration
	// Adaptive mode automatically tunes M and EfConstruction based on dimension and expected size
	config := hnsw.Config{
		Dimension:    128,    // Vector dimension (e.g., embedding size)
		Adaptive:     true,   // Enable adaptive parameter tuning
		ExpectedSize: 10000,  // Expected number of vectors
	}
	index := hnsw.NewHNSW(config)
	fmt.Printf("✓ Created HNSW index with adaptive configuration\n")
	fmt.Printf("  - Dimension: %d\n", config.Dimension)
	fmt.Printf("  - Adaptive: %v\n", config.Adaptive)
	fmt.Printf("  - ExpectedSize: %d\n", config.ExpectedSize)
	fmt.Println()

	// Step 2: Add random vectors
	fmt.Println("Adding 1000 random vectors...")
	start := time.Now()
	
	for i := 0; i < 1000; i++ {
		vec := make([]float32, 128)
		for j := range vec {
			vec[j] = rand.Float32()
		}
		id, err := index.Add(vec)
		if err != nil {
			panic(err)
		}
		if (i+1)%200 == 0 {
			fmt.Printf("  → Added %d vectors, latest ID: %d\n", i+1, id)
		}
	}
	
	elapsed := time.Since(start)
	fmt.Printf("✓ Added 1000 vectors in %v (%.2f ms/vector)\n", elapsed, float64(elapsed.Milliseconds())/1000)
	fmt.Printf("  - Total nodes in index: %d\n", index.Len())
	fmt.Println()

	// Step 3: Search nearest neighbors
	fmt.Println("Searching for 10 nearest neighbors...")
	query := make([]float32, 128)
	for j := range query {
		query[j] = rand.Float32()
	}

	start = time.Now()
	results, err := index.Search(query, 10, 0) // Return Top-10, 0 = use default EF
	if err != nil {
		panic(err)
	}
	elapsed = time.Since(start)

	fmt.Printf("✓ Search completed in %v\n", elapsed)
	fmt.Printf("  - Query returned %d neighbors:\n", len(results))
	for i, r := range results {
		fmt.Printf("    %d. ID: %d, Distance: %.4f\n", i+1, r.ID, r.Distance)
	}
	fmt.Println()

	// Step 4: Search with different EF values
	fmt.Println("Searching with different EF values...")
	fmt.Println("  (Higher EF = better recall but slower)")
	
	for _, ef := range []int{50, 100, 200} {
		start = time.Now()
		results, _ := index.Search(query, 10, ef)
		elapsed = time.Since(start)
		fmt.Printf("  EF=%d: %d results in %v\n", ef, len(results), elapsed)
	}
	fmt.Println()

	fmt.Println("=== Demo completed successfully! ===")
}
