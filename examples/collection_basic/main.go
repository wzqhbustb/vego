// Collection API Basic Usage Example
// This example demonstrates the high-level Collection API for document-oriented vector search.
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
	fmt.Println("=== Vego Collection API - Basic Usage Demo ===")
	fmt.Println()

	// Create temporary directory for database
	tmpDir, _ := os.MkdirTemp("", "vego_collection_demo")
	defer os.RemoveAll(tmpDir)

	// Step 1: Open database
	// The Collection API provides a document-oriented interface with metadata support
	db, err := vego.Open(tmpDir, vego.WithDimension(128))
	if err != nil {
		panic(err)
	}
	defer db.Close()
	fmt.Printf("✓ Opened database at: %s\n", tmpDir)
	fmt.Println()

	// Step 2: Get or create collection
	// Collections are like tables in a database - they group related documents
	coll, err := db.Collection("documents")
	if err != nil {
		panic(err)
	}
	fmt.Printf("✓ Created/Opened collection: 'documents'\n")
	fmt.Println()

	// Step 3: Insert documents with metadata
	// Documents can have IDs, vectors, and arbitrary metadata
	fmt.Println("Inserting documents with metadata...")
	ctx := context.Background()

	documents := []*vego.Document{
		{
			ID:     "doc-001",
			Vector: generateVector(128, 1),
			Metadata: map[string]interface{}{
				"title":    "Introduction to Machine Learning",
				"author":   "Alice Smith",
				"category": "AI",
				"tags":     []string{"ml", "tutorial", "basics"},
				"views":    1250,
			},
		},
		{
			ID:     "doc-002",
			Vector: generateVector(128, 2),
			Metadata: map[string]interface{}{
				"title":    "Deep Learning Fundamentals",
				"author":   "Bob Johnson",
				"category": "AI",
				"tags":     []string{"dl", "neural-networks", "advanced"},
				"views":    890,
			},
		},
		{
			ID:     "doc-003",
			Vector: generateVector(128, 3),
			Metadata: map[string]interface{}{
				"title":    "Natural Language Processing",
				"author":   "Alice Smith",
				"category": "NLP",
				"tags":     []string{"nlp", "transformers", "bert"},
				"views":    2100,
			},
		},
		{
			ID:     "doc-004",
			Vector: generateVector(128, 4),
			Metadata: map[string]interface{}{
				"title":    "Computer Vision Basics",
				"author":   "Carol White",
				"category": "CV",
				"tags":     []string{"cv", "cnn", "image-processing"},
				"views":    1500,
			},
		},
		{
			ID:     "doc-005",
			Vector: generateVector(128, 5),
			Metadata: map[string]interface{}{
				"title":    "Reinforcement Learning",
				"author":   "Bob Johnson",
				"category": "AI",
				"tags":     []string{"rl", "q-learning", "advanced"},
				"views":    650,
			},
		},
	}

	for _, doc := range documents {
		if err := coll.InsertContext(ctx, doc); err != nil {
			panic(err)
		}
		fmt.Printf("  → Inserted: %s - %s\n", doc.ID, doc.Metadata["title"])
	}
	fmt.Printf("✓ Inserted %d documents\n", len(documents))
	fmt.Println()

	// Step 4: Retrieve a document by ID
	fmt.Println("Retrieving document by ID...")
	doc, err := coll.Get("doc-001")
	if err != nil {
		panic(err)
	}
	fmt.Printf("✓ Retrieved: %s\n", doc.ID)
	fmt.Printf("  - Title: %s\n", doc.Metadata["title"])
	fmt.Printf("  - Author: %s\n", doc.Metadata["author"])
	fmt.Printf("  - Category: %s\n", doc.Metadata["category"])
	fmt.Printf("  - Tags: %v\n", doc.Metadata["tags"])
	fmt.Println()

	// Step 5: Search similar documents
	fmt.Println("Searching for similar documents...")
	query := generateVector(128, 1) // Similar to doc-001

	start := time.Now()
	results, err := coll.SearchContext(ctx, query, 3)
	if err != nil {
		panic(err)
	}
	elapsed := time.Since(start)

	fmt.Printf("✓ Search completed in %v\n", elapsed)
	fmt.Printf("  - Top 3 similar documents:\n")
	for i, r := range results {
		fmt.Printf("    %d. %s (distance: %.4f) - %s\n",
			i+1,
			r.Document.ID,
			r.Distance,
			r.Document.Metadata["title"])
	}
	fmt.Println()

	// Step 6: Update a document
	fmt.Println("Updating document metadata...")
	doc, _ = coll.Get("doc-001")
	doc.Metadata["views"] = doc.Metadata["views"].(int) + 1
	doc.Metadata["last_viewed"] = time.Now().Format("2006-01-02")

	if err := coll.Update(doc); err != nil {
		panic(err)
	}
	fmt.Printf("✓ Updated: %s\n", doc.ID)
	fmt.Printf("  - New view count: %d\n", doc.Metadata["views"])
	fmt.Println()

	// Step 7: Get collection stats
	fmt.Println("Collection statistics:")
	stats := coll.Stats()
	fmt.Printf("  - Name: %s\n", stats.Name)
	fmt.Printf("  - Document count: %d\n", stats.Count)
	fmt.Printf("  - Vector dimension: %d\n", stats.Dimension)
	fmt.Printf("  - Index nodes: %d\n", stats.IndexNodes)
	fmt.Println()

	// Step 8: Delete a document
	fmt.Println("Deleting a document...")
	if err := coll.Delete("doc-005"); err != nil {
		panic(err)
	}
	fmt.Printf("✓ Deleted: doc-005\n")

	// Verify deletion
	_, err = coll.Get("doc-005")
	if err != nil {
		if vego.IsNotFound(err) {
			fmt.Printf("  ✓ Confirmed: doc-005 not found (as expected)\n")
		}
	}
	fmt.Println()

	// Step 9: Save collection
	fmt.Println("Saving collection to disk...")
	if err := coll.Save(); err != nil {
		panic(err)
	}
	fmt.Printf("✓ Collection saved successfully\n")
	fmt.Println()

	fmt.Println("=== Demo completed successfully! ===")
	fmt.Println()
	fmt.Println("Key features demonstrated:")
	fmt.Println("  • Document-oriented API with metadata")
	fmt.Println("  • Insert/Get/Update/Delete operations")
	fmt.Println("  • Vector similarity search")
	fmt.Println("  • Context support for cancellation/timeouts")
	fmt.Println("  • Structured error handling (IsNotFound)")
	fmt.Println("  • Persistence with Save()")
}

// generateVector creates a deterministic random vector for demo purposes
func generateVector(dim int, seed int) []float32 {
	vec := make([]float32, dim)
	r := rand.New(rand.NewSource(int64(seed)))
	for i := range vec {
		vec[i] = r.Float32()
	}
	return vec
}
