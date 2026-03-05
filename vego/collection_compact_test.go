package vego

import (
	"fmt"
	"os"
	"testing"
)

// flushCollection helper to flush documents to disk
func flushCollection(t *testing.T, coll *Collection) {
	t.Helper()
	if err := coll.Save(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}
}

// TestCompactBasic tests basic compaction functionality
func TestCompactBasic(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "compact_basic")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	coll, err := NewCollection("test", tmpDir, &Config{
		Dimension: 128,
	})
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}
	defer coll.Close()

	// Insert and flush documents
	docs := []*Document{
		{ID: "doc-001", Vector: randomVector(128, 1), Metadata: map[string]interface{}{"type": "test"}},
		{ID: "doc-002", Vector: randomVector(128, 2), Metadata: map[string]interface{}{"type": "test"}},
		{ID: "doc-003", Vector: randomVector(128, 3), Metadata: map[string]interface{}{"type": "test"}},
	}
	for _, doc := range docs {
		if err := coll.Insert(doc); err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}
	flushCollection(t, coll)

	// Compact (no deletions, should be no-op essentially)
	if err := coll.Compact(); err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	// Verify all documents still accessible
	if count := coll.Count(); count != 3 {
		t.Errorf("Expected count 3, got %d", count)
	}
	for _, id := range []string{"doc-001", "doc-002", "doc-003"} {
		if _, err := coll.Get(id); err != nil {
			t.Errorf("Failed to get %s: %v", id, err)
		}
	}

	t.Log("Basic compact test passed")
}

// TestCompactWithDeletions tests compaction removes deleted documents
func TestCompactWithDeletions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "compact_deletions")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	coll, err := NewCollection("test", tmpDir, &Config{
		Dimension: 10,
	})
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}
	defer coll.Close()

	// Insert and flush
	docs := []*Document{
		{ID: "doc-001", Vector: []float32{1, 0, 0, 0, 0, 0, 0, 0, 0, 0}},
		{ID: "doc-002", Vector: []float32{0, 1, 0, 0, 0, 0, 0, 0, 0, 0}},
		{ID: "doc-003", Vector: []float32{0, 0, 1, 0, 0, 0, 0, 0, 0, 0}},
		{ID: "doc-004", Vector: []float32{0, 0, 0, 1, 0, 0, 0, 0, 0, 0}},
		{ID: "doc-005", Vector: []float32{0, 0, 0, 0, 1, 0, 0, 0, 0, 0}},
	}
	for _, doc := range docs {
		if err := coll.Insert(doc); err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}
	flushCollection(t, coll)

	// Delete 2 documents
	if err := coll.Delete("doc-002"); err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}
	if err := coll.Delete("doc-004"); err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	// Check stats before compact
	stats := coll.Stats()
	if stats.DeletedCount != 2 {
		t.Errorf("Expected 2 deleted, got %d", stats.DeletedCount)
	}

	// Compact
	if err := coll.Compact(); err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	// Verify count
	if count := coll.Count(); count != 3 {
		t.Errorf("Expected count 3, got %d", count)
	}

	// Verify deleted documents gone
	if _, err := coll.Get("doc-002"); err == nil {
		t.Error("doc-002 should not exist")
	}
	if _, err := coll.Get("doc-004"); err == nil {
		t.Error("doc-004 should not exist")
	}

	// Verify remaining exist
	for _, id := range []string{"doc-001", "doc-003", "doc-005"} {
		if _, err := coll.Get(id); err != nil {
			t.Errorf("Failed to get %s: %v", id, err)
		}
	}

	// Verify deletion stats cleared
	stats = coll.Stats()
	if stats.DeletedCount != 0 {
		t.Errorf("Expected 0 deleted after compact, got %d", stats.DeletedCount)
	}

	t.Log("Compact with deletions test passed")
}

// TestCompactSearchAfterCompaction tests search works after compaction
func TestCompactSearchAfterCompaction(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "compact_search")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	coll, err := NewCollection("test", tmpDir, &Config{
		Dimension: 10,
	})
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}
	defer coll.Close()

	// Insert and flush
	docs := []*Document{
		{ID: "doc-001", Vector: []float32{1, 0, 0, 0, 0, 0, 0, 0, 0, 0}},
		{ID: "doc-002", Vector: []float32{0, 1, 0, 0, 0, 0, 0, 0, 0, 0}},
		{ID: "doc-003", Vector: []float32{0, 0, 1, 0, 0, 0, 0, 0, 0, 0}},
	}
	for _, doc := range docs {
		if err := coll.Insert(doc); err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}
	flushCollection(t, coll)

	// Delete one
	if err := coll.Delete("doc-002"); err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	// Compact
	if err := coll.Compact(); err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	// Search
	query := []float32{0.5, 0.5, 0.5, 0, 0, 0, 0, 0, 0, 0}
	results, err := coll.Search(query, 3)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	// Verify deleted document not in results
	for _, r := range results {
		if r.Document.ID == "doc-002" {
			t.Error("Found deleted document in search results")
		}
	}

	t.Log("Search after compaction test passed")
}

// TestCompactPersistence tests compacted data persists after reopen
func TestCompactPersistence(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "compact_persist")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create and populate
	coll1, err := NewCollection("test", tmpDir, &Config{
		Dimension: 128,
	})
	if err != nil {
		t.Fatalf("Failed to create: %v", err)
	}

	for i := 1; i <= 5; i++ {
		doc := &Document{
			ID:     fmt.Sprintf("doc-%03d", i),
			Vector: randomVector(128, i),
		}
		if err := coll1.Insert(doc); err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}
	flushCollection(t, coll1)

	// Delete and compact
	if err := coll1.Delete("doc-002"); err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}
	if err := coll1.Delete("doc-004"); err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}
	if err := coll1.Compact(); err != nil {
		t.Fatalf("Compact failed: %v", err)
	}
	if err := coll1.Save(); err != nil {
		t.Fatalf("Save failed: %v", err)
	}
	coll1.Close()

	// Reopen
	coll2, err := NewCollection("test", tmpDir, &Config{
		Dimension: 128,
	})
	if err != nil {
		t.Fatalf("Failed to reopen: %v", err)
	}
	defer coll2.Close()

	// Verify
	if count := coll2.Count(); count != 3 {
		t.Errorf("Expected count 3 after reopen, got %d", count)
	}
	if _, err := coll2.Get("doc-002"); err == nil {
		t.Error("doc-002 should not exist")
	}
	for _, id := range []string{"doc-001", "doc-003", "doc-005"} {
		if _, err := coll2.Get(id); err != nil {
			t.Errorf("Failed to get %s: %v", id, err)
		}
	}

	t.Log("Compact persistence test passed")
}

// TestCompactEmptyCollection tests compacting empty collection
func TestCompactEmptyCollection(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "compact_empty")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	coll, err := NewCollection("test", tmpDir, &Config{
		Dimension: 128,
	})
	if err != nil {
		t.Fatalf("Failed to create: %v", err)
	}
	defer coll.Close()

	// Compact empty
	if err := coll.Compact(); err != nil {
		t.Fatalf("Compact on empty failed: %v", err)
	}

	// Insert, delete all, compact
	doc := &Document{ID: "doc-001", Vector: randomVector(128, 1)}
	if err := coll.Insert(doc); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	flushCollection(t, coll)
	if err := coll.Delete("doc-001"); err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}
	if err := coll.Compact(); err != nil {
		t.Fatalf("Compact after delete all failed: %v", err)
	}

	if count := coll.Count(); count != 0 {
		t.Errorf("Expected count 0, got %d", count)
	}

	t.Log("Compact empty collection test passed")
}

// TestCompactAfterUpdate tests compaction after updates
func TestCompactAfterUpdate(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "compact_update")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	coll, err := NewCollection("test", tmpDir, &Config{
		Dimension: 128,
	})
	if err != nil {
		t.Fatalf("Failed to create: %v", err)
	}
	defer coll.Close()

	// Insert original
	original := &Document{
		ID:       "doc-001",
		Vector:   randomVector(128, 1),
		Metadata: map[string]interface{}{"version": "v1"},
	}
	if err := coll.Insert(original); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	flushCollection(t, coll)

	// Update
	updated := &Document{
		ID:       "doc-001",
		Vector:   randomVector(128, 99),
		Metadata: map[string]interface{}{"version": "v2"},
	}
	if err := coll.Update(updated); err != nil {
		t.Fatalf("Failed to update: %v", err)
	}

	// Verify update
	doc, err := coll.Get("doc-001")
	if err != nil {
		t.Fatalf("Failed to get: %v", err)
	}
	if doc.Metadata["version"] != "v2" {
		t.Error("Update not reflected")
	}

	// Compact
	if err := coll.Compact(); err != nil {
		t.Fatalf("Compact failed: %v", err)
	}

	// Verify still correct
	doc, err = coll.Get("doc-001")
	if err != nil {
		t.Fatalf("Failed to get after compact: %v", err)
	}
	if doc.Metadata["version"] != "v2" {
		t.Error("Version wrong after compact")
	}

	t.Log("Compact after update test passed")
}
