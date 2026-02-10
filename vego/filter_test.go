package vego

import (
	"os"
	"path/filepath"
	"testing"
)

func TestMetadataFilter(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "vego_filter_test")
	defer os.RemoveAll(tmpDir)

	db, err := Open(tmpDir, WithDimension(64))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	coll, err := db.Collection("test")
	if err != nil {
		t.Fatal(err)
	}

	// Insert test documents
	doc1 := &Document{
		ID:     DocumentID(),
		Vector: make([]float32, 64),
		Metadata: map[string]interface{}{
			"author": "Alice",
			"type":   "article",
		},
	}
	doc2 := &Document{
		ID:     DocumentID(),
		Vector: make([]float32, 64),
		Metadata: map[string]interface{}{
			"author": "Bob",
			"type":   "article",
		},
	}
	doc3 := &Document{
		ID:     DocumentID(),
		Vector: make([]float32, 64),
		Metadata: map[string]interface{}{
			"author": "Alice",
			"type":   "blog",
		},
	}

	if err := coll.Insert(doc1); err != nil {
		t.Fatal(err)
	}
	if err := coll.Insert(doc2); err != nil {
		t.Fatal(err)
	}
	if err := coll.Insert(doc3); err != nil {
		t.Fatal(err)
	}

	// Test retrieval
	retrieved, err := coll.Get(doc1.ID)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Retrieved doc1 metadata: %+v", retrieved.Metadata)

	// Test search without filter
	query := make([]float32, 64)
	allResults, err := coll.Search(query, 10)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Search returned %d results", len(allResults))
	for i, r := range allResults {
		t.Logf("  Result %d: author=%v, distance=%.4f", i, r.Document.Metadata["author"], r.Distance)
	}

	// Test filter
	filter := &MetadataFilter{
		Field:    "author",
		Operator: "eq",
		Value:    "Alice",
	}

	filteredResults, err := coll.SearchWithFilter(query, 10, filter)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Filtered search returned %d results", len(filteredResults))
	for i, r := range filteredResults {
		t.Logf("  Filtered result %d: author=%v", i, r.Document.Metadata["author"])
	}

	if len(filteredResults) != 2 {
		t.Errorf("Expected 2 filtered results, got %d", len(filteredResults))
	}

	// Verify all results have author="Alice"
	for _, r := range filteredResults {
		if r.Document.Metadata["author"] != "Alice" {
			t.Errorf("Expected author=Alice, got %v", r.Document.Metadata["author"])
		}
	}
}
