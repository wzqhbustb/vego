// Batch Operations Example
// This example demonstrates batch operations for improved performance
// when dealing with large numbers of documents.
//
// Run: go run main.go
package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/wzqhbustb/vego/vego"
)

func main() {
	fmt.Println("=== Vego Collection API - Batch Operations Demo ===")
	fmt.Println()

	// Create temporary directory
	tmpDir, _ := os.MkdirTemp("", "vego_batch_demo")
	defer os.RemoveAll(tmpDir)

	// Initialize database
	db, err := vego.Open(tmpDir, vego.WithDimension(128))
	if err != nil {
		panic(err)
	}
	defer db.Close()

	coll, err := db.Collection("documents")
	if err != nil {
		panic(err)
	}
	fmt.Println("✓ Database initialized")
	fmt.Println()

	// Demo 1: Batch Insert
	fmt.Println("═══════════════════════════════════════════════════════════")
	fmt.Println("Demo 1: Batch Insert Performance")
	fmt.Println("═══════════════════════════════════════════════════════════")

	// Generate 1000 documents
	docCount := 1000
	docs := make([]*vego.Document, docCount)
	for i := 0; i < docCount; i++ {
		docs[i] = &vego.Document{
			ID:       fmt.Sprintf("doc-%04d", i),
			Vector:   generateVector(128, i),
			Metadata: map[string]interface{}{
				"index": i,
				"type":  "batch_doc",
				"tags":  []string{"batch", fmt.Sprintf("tag-%d", i%10)},
			},
		}
	}

	// Batch insert
	ctx := context.Background()
	start := time.Now()
	if err := coll.InsertBatchContext(ctx, docs); err != nil {
		panic(err)
	}
	batchDuration := time.Since(start)

	fmt.Printf("✓ Batch inserted %d documents in %v\n", docCount, batchDuration)
	fmt.Printf("  Throughput: %.2f docs/sec\n", float64(docCount)/batchDuration.Seconds())
	fmt.Println()

	// Compare with individual inserts (on smaller set)
	fmt.Println("Comparing with individual inserts (100 documents)...")
	singleDocs := make([]*vego.Document, 100)
	for i := 0; i < 100; i++ {
		singleDocs[i] = &vego.Document{
			ID:       fmt.Sprintf("single-%04d", i),
			Vector:   generateVector(128, i+10000),
			Metadata: map[string]interface{}{"type": "single_doc"},
		}
	}

	start = time.Now()
	for _, doc := range singleDocs {
		if err := coll.InsertContext(ctx, doc); err != nil {
			panic(err)
		}
	}
	singleDuration := time.Since(start)

	fmt.Printf("  Individual inserts: %v (%.2f docs/sec)\n", singleDuration, float64(100)/singleDuration.Seconds())
	fmt.Printf("  Batch insert is %.1fx faster\n", float64(singleDuration)/float64(batchDuration)*10)
	fmt.Println()

	// Demo 2: Batch Get
	fmt.Println("═══════════════════════════════════════════════════════════")
	fmt.Println("Demo 2: Batch Get Performance")
	fmt.Println("═══════════════════════════════════════════════════════════")

	// Prepare IDs to retrieve
	ids := []string{
		"doc-0001", "doc-0050", "doc-0100", "doc-0200", "doc-0500",
		"doc-0999", "non-existent-1", "non-existent-2",
	}

	start = time.Now()
	results, err := coll.GetBatch(ids)
	if err != nil {
		panic(err)
	}
	duration := time.Since(start)

	fmt.Printf("✓ Batch retrieved %d documents in %v\n", len(results), duration)
	fmt.Println("Retrieved documents:")
	for id, doc := range results {
		fmt.Printf("  • %s (index: %v)\n", id, doc.Metadata["index"])
	}
	fmt.Printf("Missing IDs were skipped: %d requested, %d found\n", len(ids), len(results))
	fmt.Println()

	// Demo 3: Batch Delete
	fmt.Println("═══════════════════════════════════════════════════════════")
	fmt.Println("Demo 3: Batch Delete")
	fmt.Println("═══════════════════════════════════════════════════════════")

	// Get current count
	stats := coll.Stats()
	fmt.Printf("Documents before delete: %d\n", stats.Count)

	// Delete batch
	deleteIds := []string{
		"doc-0001", "doc-0002", "doc-0003", "doc-0004", "doc-0005",
		"non-existent", // Will be silently skipped
	}

	if err := coll.DeleteBatch(deleteIds); err != nil {
		panic(err)
	}

	stats = coll.Stats()
	fmt.Printf("Documents after delete: %d\n", stats.Count)
	fmt.Printf("✓ Deleted %d documents\n", len(deleteIds)-1) // -1 for non-existent
	fmt.Println()

	// Verify deletion
	_, err = coll.Get("doc-0001")
	if err != nil {
		if vego.IsNotFound(err) {
			fmt.Println("✓ Confirmed: doc-0001 was deleted")
		}
	}
	fmt.Println()

	// Demo 4: Context Cancellation
	fmt.Println("═══════════════════════════════════════════════════════════")
	fmt.Println("Demo 4: Context Cancellation")
	fmt.Println("═══════════════════════════════════════════════════════════")

	// Create a context that cancels immediately
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel right away

	// Try to insert with cancelled context
	cancelDocs := []*vego.Document{
		{ID: "cancel-1", Vector: generateVector(128, 9999)},
	}

	err = coll.InsertBatchContext(ctx, cancelDocs)
	if err == context.Canceled {
		fmt.Println("✓ Batch insert was cancelled as expected")
	}
	fmt.Println()

	// Demo 5: Chunked Batch Insert
	fmt.Println("═══════════════════════════════════════════════════════════")
	fmt.Println("Demo 5: Chunked Batch Insert (for very large datasets)")
	fmt.Println("═══════════════════════════════════════════════════════════")

	// When dealing with very large datasets, it's better to insert in chunks
	totalDocs := 5000
	chunkSize := 500
	chunks := totalDocs / chunkSize

	fmt.Printf("Inserting %d documents in %d chunks (chunk size: %d)...\n", totalDocs, chunks, chunkSize)

	start = time.Now()
	for chunk := 0; chunk < chunks; chunk++ {
		chunkDocs := make([]*vego.Document, chunkSize)
		for i := 0; i < chunkSize; i++ {
			docID := chunk*chunkSize + i + 2000 // Offset to avoid conflicts
			chunkDocs[i] = &vego.Document{
				ID:     fmt.Sprintf("chunk-doc-%d", docID),
				Vector: generateVector(128, docID),
				Metadata: map[string]interface{}{
					"chunk": chunk,
					"index": docID,
				},
			}
		}

		if err := coll.InsertBatch(chunkDocs); err != nil {
			panic(err)
		}
		fmt.Printf("  ✓ Chunk %d/%d inserted\n", chunk+1, chunks)
	}
	duration = time.Since(start)

	fmt.Printf("\n✓ Inserted %d documents in %v\n", totalDocs, duration)
	fmt.Printf("  Throughput: %.2f docs/sec\n", float64(totalDocs)/duration.Seconds())

	stats = coll.Stats()
	fmt.Printf("  Total documents in collection: %d\n", stats.Count)
	fmt.Println()

	// Demo 6: Batch Search
	fmt.Println("═══════════════════════════════════════════════════════════")
	fmt.Println("Demo 6: Batch Search (Parallel Search)")
	fmt.Println("═══════════════════════════════════════════════════════════")

	// Create multiple query vectors
	queries := [][]float32{
		generateVector(128, 1),
		generateVector(128, 100),
		generateVector(128, 500),
		generateVector(128, 1000),
	}

	start = time.Now()
	batchResults, err := coll.SearchBatch(queries, 5)
	if err != nil {
		panic(err)
	}
	duration = time.Since(start)

	fmt.Printf("✓ Executed %d searches in parallel in %v\n", len(queries), duration)
	for i, results := range batchResults {
		fmt.Printf("  Query %d: %d results\n", i+1, len(results))
	}
	fmt.Println()

	// Demo 7: Error Handling
	fmt.Println("═══════════════════════════════════════════════════════════")
	fmt.Println("Demo 7: Error Handling in Batch Operations")
	fmt.Println("═══════════════════════════════════════════════════════════")

	// Try to insert duplicate IDs
	duplicateDocs := []*vego.Document{
		{ID: "doc-0010", Vector: generateVector(128, 1)}, // Already exists
		{ID: "new-doc-1", Vector: generateVector(128, 2)},
	}

	err = coll.InsertBatch(duplicateDocs)
	if err != nil {
		if vego.IsDuplicate(err) {
			fmt.Println("✓ Detected duplicate ID error")
		}
	}
	fmt.Println()

	// Summary
	fmt.Println("=== Batch Operations Demo completed! ===")
	fmt.Println()
	fmt.Println("Key takeaways:")
	fmt.Println("  • Batch insert is significantly faster than individual inserts")
	fmt.Println("  • GetBatch retrieves multiple documents efficiently")
	fmt.Println("  • DeleteBatch removes multiple documents")
	fmt.Println("  • Context cancellation works for all batch operations")
	fmt.Println("  • For large datasets, use chunked batch inserts")
	fmt.Println("  • SearchBatch performs multiple searches in parallel")
	fmt.Println("  • Proper error handling with IsDuplicate, IsNotFound, etc.")
}

// generateVector creates a deterministic random vector
func generateVector(dim int, seed int) []float32 {
	vec := make([]float32, dim)
	r := rand.New(rand.NewSource(int64(seed)))
	for i := range vec {
		vec[i] = r.Float32()
	}
	return vec
}
