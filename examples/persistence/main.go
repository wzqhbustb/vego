// Persistence Example
// This example demonstrates how to save and load a Vego collection to/from disk.
//
// Run: go run main.go
package main

import (
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/wzqhbustb/vego/vego"
)

func main() {
	fmt.Println("=== Vego Persistence Demo ===")
	fmt.Println()

	// Create a temporary directory for the demo
	tmpDir := "/tmp/vego_persistence_demo"
	os.RemoveAll(tmpDir)
	os.MkdirAll(tmpDir, 0755)
	defer os.RemoveAll(tmpDir)
	fmt.Printf("Working directory: %s\n", tmpDir)
	fmt.Println()

	// Step 1: Open database and create collection
	fmt.Println("Step 1: Opening database and creating collection...")
	db, err := vego.Open(tmpDir)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	coll, err := db.Collection("embeddings")
	if err != nil {
		panic(err)
	}

	// Add some documents with vectors
	for i := 0; i < 500; i++ {
		vec := make([]float32, 128)
		for j := range vec {
			vec[j] = rand.Float32()
		}
		doc := &vego.Document{
			ID:       fmt.Sprintf("doc-%d", i),
			Vector:   vec,
			Metadata: map[string]interface{}{"index": i},
		}
		if err := coll.Insert(doc); err != nil {
			panic(err)
		}
	}
	fmt.Printf("✓ Created collection with %d documents\n", coll.Count())
	fmt.Println()

	// Step 2: Save collection to disk (auto-saved on Close, but explicit here)
	fmt.Println("Step 2: Saving collection to disk...")
	start := time.Now()
	if err := coll.Save(); err != nil {
		panic(err)
	}
	elapsed := time.Since(start)
	fmt.Printf("✓ Collection saved\n")
	fmt.Printf("  - Save time: %v\n", elapsed)
	fmt.Println()

	// Step 3: Close and reopen database to test persistence
	fmt.Println("Step 3: Closing and reopening database...")
	if err := db.Close(); err != nil {
		panic(err)
	}

	start = time.Now()
	db2, err := vego.Open(tmpDir)
	if err != nil {
		panic(err)
	}
	defer db2.Close()
	elapsed = time.Since(start)

	coll2, err := db2.Collection("embeddings")
	if err != nil {
		panic(err)
	}

	fmt.Printf("✓ Database reopened\n")
	fmt.Printf("  - Open time: %v\n", elapsed)
	fmt.Printf("  - Loaded %d documents\n", coll2.Count())
	fmt.Println()

	// Step 4: Verify loaded collection works
	fmt.Println("Step 4: Verifying loaded collection...")
	query := make([]float32, 128)
	for j := range query {
		query[j] = rand.Float32()
	}

	results, err := coll2.Search(query, 5)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Search results from loaded collection:\n")
	for i, r := range results {
		fmt.Printf("  %d. ID: %s, Distance: %.4f\n", i+1, r.Document.ID, r.Distance)
	}
	fmt.Println()

	// Step 5: Continue using loaded collection
	fmt.Println("Step 5: Adding more documents to loaded collection...")
	for i := 0; i < 100; i++ {
		vec := make([]float32, 128)
		for j := range vec {
			vec[j] = rand.Float32()
		}
		doc := &vego.Document{
			ID:       fmt.Sprintf("new-doc-%d", i),
			Vector:   vec,
			Metadata: map[string]interface{}{"batch": "second"},
		}
		if err := coll2.Insert(doc); err != nil {
			panic(err)
		}
	}
	fmt.Printf("✓ Added 100 more documents, total: %d\n", coll2.Count())
	fmt.Println()

	fmt.Println("=== Demo completed successfully! ===")
	fmt.Println("The collection has been persisted and can be loaded in future runs.")
}
