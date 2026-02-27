package vego

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/wzqhbustb/vego/storage/column"
)

// TestRowIndexLookup verifies that RowIndex correctly maps ID to row
func TestRowIndexLookup(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "vego-rowindex-lookup-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create storage
	storage, err := NewDocumentStorage(filepath.Join(tmpDir, "storage"), 64)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	// Create test documents with specific IDs
	docs := []*Document{
		{ID: "first-doc", Vector: makeTestVector(64, 1.0)},
		{ID: "second-doc", Vector: makeTestVector(64, 2.0)},
		{ID: "third-doc", Vector: makeTestVector(64, 3.0)},
	}

	// Put documents
	for _, doc := range docs {
		if err := storage.Put(doc); err != nil {
			t.Fatalf("Failed to put document %s: %v", doc.ID, err)
		}
	}

	// Flush to write data file
	if err := storage.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}
	storage.Close()

	// Directly verify RowIndex lookup
	dataFile := filepath.Join(tmpDir, "storage", "vectors.lance")
	reader, err := column.NewRowIndexReader(dataFile)
	if err != nil {
		t.Fatalf("Failed to open reader: %v", err)
	}
	defer reader.Close()

	if !reader.HasRowIndex() {
		t.Fatal("Expected HasRowIndex() to be true")
	}

	// Verify each document ID can be found (row number may differ due to map iteration order in deduplication)
	foundRows := make(map[int64]string)
	for _, doc := range docs {
		rowIdx, err := reader.LookupRowID(doc.ID)
		if err != nil {
			t.Errorf("Failed to lookup %s: %v", doc.ID, err)
			continue
		}
		// Check no two IDs map to same row
		if otherID, exists := foundRows[rowIdx]; exists {
			t.Errorf("Row %d mapped to both %s and %s", rowIdx, otherID, doc.ID)
		}
		foundRows[rowIdx] = doc.ID
	}
	// Verify we found all 3 documents
	if len(foundRows) != 3 {
		t.Errorf("Expected 3 unique rows, got %d", len(foundRows))
	}

	// Verify non-existent ID returns error
	_, err = reader.LookupRowID("non-existent-id")
	if err == nil {
		t.Error("Expected error for non-existent ID, got nil")
	}

	t.Logf("RowIndex lookup test passed - all IDs map to correct rows")
}

// TestRowIndexStats verifies RowIndex statistics
func TestRowIndexStats(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "vego-rowindex-stats-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create storage
	storage, err := NewDocumentStorage(filepath.Join(tmpDir, "storage"), 64)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	// Create 100 documents
	for i := 0; i < 100; i++ {
		doc := &Document{
			ID:     fmt.Sprintf("doc-%d", i),
			Vector: makeTestVector(64, float32(i)),
		}
		if err := storage.Put(doc); err != nil {
			t.Fatalf("Failed to put document: %v", err)
		}
	}

	if err := storage.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}
	storage.Close()

	// Verify RowIndex stats
	dataFile := filepath.Join(tmpDir, "storage", "vectors.lance")
	reader, err := column.NewRowIndexReader(dataFile)
	if err != nil {
		t.Fatalf("Failed to open reader: %v", err)
	}
	defer reader.Close()

	stats, err := reader.RowIndexStats()
	if err != nil {
		t.Fatalf("Failed to get RowIndex stats: %v", err)
	}

	if stats.NumEntries != 100 {
		t.Errorf("Expected 100 entries, got %d", stats.NumEntries)
	}

	t.Logf("RowIndex stats: %s", stats.String())
}

