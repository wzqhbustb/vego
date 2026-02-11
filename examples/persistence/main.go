// Persistence Example
// This example demonstrates how to save and load HNSW index to/from disk.
//
// Run: go run main.go
package main

import (
	"fmt"
	"math/rand"
	"os"
	"time"

	hnsw "github.com/wzqhbustb/vego/index"
)

func main() {
	fmt.Println("=== Vego Persistence Demo ===")
	fmt.Println()

	// Create a temporary directory for the demo
	tmpDir := "/tmp/vego_persistence_demo"
	os.MkdirAll(tmpDir, 0755)
	defer os.RemoveAll(tmpDir)
	fmt.Printf("Working directory: %s\n", tmpDir)
	fmt.Println()

	// Step 1: Create and populate index
	fmt.Println("Step 1: Creating and populating index...")
	config := hnsw.Config{
		Dimension:    128,
		Adaptive:     true,
		ExpectedSize: 5000,
	}
	index := hnsw.NewHNSW(config)

	// Add some vectors
	for i := 0; i < 500; i++ {
		vec := make([]float32, 128)
		for j := range vec {
			vec[j] = rand.Float32()
		}
		index.Add(vec)
	}
	fmt.Printf("✓ Created index with %d vectors\n", index.Len())
	fmt.Println()

	// Step 2: Save index to disk
	fmt.Println("Step 2: Saving index to disk...")
	savePath := tmpDir + "/my_index"
	
	start := time.Now()
	err := index.SaveToLance(savePath)
	if err != nil {
		panic(err)
	}
	elapsed := time.Since(start)
	
	fmt.Printf("✓ Index saved to %s\n", savePath)
	fmt.Printf("  - Save time: %v\n", elapsed)
	
	// Check file size
	if info, err := os.Stat(savePath); err == nil {
		fmt.Printf("  - File size: %.2f MB\n", float64(info.Size())/(1024*1024))
	}
	fmt.Println()

	// Step 3: Load index from disk
	fmt.Println("Step 3: Loading index from disk...")
	
	start = time.Now()
	loadedIndex, err := hnsw.LoadHNSWFromLance(savePath)
	if err != nil {
		panic(err)
	}
	elapsed = time.Since(start)
	
	fmt.Printf("✓ Index loaded from %s\n", savePath)
	fmt.Printf("  - Load time: %v\n", elapsed)
	fmt.Printf("  - Loaded %d vectors\n", loadedIndex.Len())
	fmt.Println()

	// Step 4: Verify loaded index works
	fmt.Println("Step 4: Verifying loaded index...")
	query := make([]float32, 128)
	for j := range query {
		query[j] = rand.Float32()
	}

	// Search on original index
	results1, _ := index.Search(query, 5, 0)
	fmt.Printf("Original index search:\n")
	for i, r := range results1 {
		fmt.Printf("  %d. ID: %d, Distance: %.4f\n", i+1, r.ID, r.Distance)
	}

	// Search on loaded index
	results2, _ := loadedIndex.Search(query, 5, 0)
	fmt.Printf("Loaded index search:\n")
	for i, r := range results2 {
		fmt.Printf("  %d. ID: %d, Distance: %.4f\n", i+1, r.ID, r.Distance)
	}
	fmt.Println()

	// Step 5: Continue using loaded index
	fmt.Println("Step 5: Adding more vectors to loaded index...")
	for i := 0; i < 100; i++ {
		vec := make([]float32, 128)
		for j := range vec {
			vec[j] = rand.Float32()
		}
		loadedIndex.Add(vec)
	}
	fmt.Printf("✓ Added 100 more vectors, total: %d\n", loadedIndex.Len())
	fmt.Println()

	// Save again
	fmt.Println("Step 6: Saving updated index...")
	err = loadedIndex.SaveToLance(savePath)
	if err != nil {
		panic(err)
	}
	fmt.Printf("✓ Updated index saved\n")
	fmt.Println()

	fmt.Println("=== Demo completed successfully! ===")
	fmt.Println("The index has been persisted and can be loaded in future runs.")
}
