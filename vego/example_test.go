package vego_test

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/wzqhbustb/vego/vego"
)

// Example demonstrates basic usage of the Vego API
func Example() {
	// Create temporary directory for test
	tmpDir := filepath.Join(os.TempDir(), "vego_example")
	defer os.RemoveAll(tmpDir)

	// Open database
	db, err := vego.Open(tmpDir,
		vego.WithDimension(128),
		vego.WithAdaptive(true),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Get or create collection
	coll, err := db.Collection("documents")
	if err != nil {
		log.Fatal(err)
	}

	// Create and insert documents
	doc1 := &vego.Document{
		ID:     vego.DocumentID(),
		Vector: make([]float32, 128),
		Metadata: map[string]interface{}{
			"title":  "Document 1",
			"author": "Alice",
		},
	}
	// Fill vector with sample data
	for i := range doc1.Vector {
		doc1.Vector[i] = float32(i) * 0.01
	}

	if err := coll.Insert(doc1); err != nil {
		log.Fatal(err)
	}

	// Batch insert
	docs := []*vego.Document{
		{
			ID:     vego.DocumentID(),
			Vector: make([]float32, 128),
			Metadata: map[string]interface{}{
				"title":  "Document 2",
				"author": "Bob",
			},
		},
		{
			ID:     vego.DocumentID(),
			Vector: make([]float32, 128),
			Metadata: map[string]interface{}{
				"title":  "Document 3",
				"author": "Alice",
			},
		},
	}
	for _, doc := range docs {
		for i := range doc.Vector {
			doc.Vector[i] = float32(i) * 0.02
		}
	}

	if err := coll.InsertBatch(docs); err != nil {
		log.Fatal(err)
	}

	// Get collection stats
	stats := coll.Stats()
	fmt.Printf("Collection has %d documents\n", stats.Count)

	// Search
	query := make([]float32, 128)
	for i := range query {
		query[i] = float32(i) * 0.015
	}

	results, err := coll.Search(query, 2)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Found %d results\n", len(results))

	// Search with filter
	filter := &vego.MetadataFilter{
		Field:    "author",
		Operator: "eq",
		Value:    "Alice",
	}
	filteredResults, err := coll.SearchWithFilter(query, 10, filter)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Found %d filtered results\n", len(filteredResults))

	// Update document
	doc1.Metadata["updated"] = true
	if err := coll.Update(doc1); err != nil {
		log.Fatal(err)
	}

	// Upsert (will update if exists, insert if not)
	newDoc := &vego.Document{
		ID:     vego.DocumentID(),
		Vector: make([]float32, 128),
		Metadata: map[string]interface{}{
			"title": "Document 4",
		},
	}
	if err := coll.Upsert(newDoc); err != nil {
		log.Fatal(err)
	}

	// Save collection
	if err := coll.Save(); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Example completed successfully")
	// Output:
	// Collection has 3 documents
	// Found 2 results
	// Found 2 filtered results
	// Example completed successfully
}

// ExampleSearchBatch demonstrates batch search
func Example_searchBatch() {
	tmpDir := filepath.Join(os.TempDir(), "vego_batch_example")
	defer os.RemoveAll(tmpDir)

	db, _ := vego.Open(tmpDir, vego.WithDimension(64))
	defer db.Close()

	coll, _ := db.Collection("test")

	// Insert some documents
	for i := 0; i < 100; i++ {
		doc := &vego.Document{
			ID:     vego.DocumentID(),
			Vector: make([]float32, 64),
		}
		for j := range doc.Vector {
			doc.Vector[j] = float32(i*j) * 0.001
		}
		coll.Insert(doc)
	}

	// Batch search
	queries := [][]float32{
		make([]float32, 64),
		make([]float32, 64),
		make([]float32, 64),
	}
	for _, q := range queries {
		for i := range q {
			q[i] = float32(i) * 0.01
		}
	}

	results, err := coll.SearchBatch(queries, 5)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Batch search returned results for %d queries\n", len(results))
	// Output:
	// Batch search returned results for 3 queries
}
