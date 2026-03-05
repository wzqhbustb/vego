package vego

import (
	"context"
	"fmt"
	"os"
	"testing"
)

// TestCollectionDVDelete tests logical deletion using DeletionVector
func TestCollectionDVDelete(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "collection_dv_delete")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create collection
	coll, err := NewCollection("test", tmpDir, &Config{
		Dimension: 128,
	})
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}
	defer coll.Close()

	ctx := context.Background()

	// Insert test documents
	docs := []*Document{
		{ID: "doc-001", Vector: randomVector(128, 1), Metadata: map[string]interface{}{"type": "test"}},
		{ID: "doc-002", Vector: randomVector(128, 2), Metadata: map[string]interface{}{"type": "test"}},
		{ID: "doc-003", Vector: randomVector(128, 3), Metadata: map[string]interface{}{"type": "test"}},
	}

	for _, doc := range docs {
		if err := coll.InsertContext(ctx, doc); err != nil {
			t.Fatalf("Failed to insert document: %v", err)
		}
	}

	// Verify initial count
	if count := coll.Count(); count != 3 {
		t.Errorf("Expected count 3, got %d", count)
	}

	// Delete a document
	if err := coll.DeleteContext(ctx, "doc-002"); err != nil {
		t.Fatalf("Failed to delete document: %v", err)
	}

	// Verify document is marked as deleted (Get should fail)
	_, err = coll.GetContext(ctx, "doc-002")
	if err == nil {
		t.Error("Expected error when getting deleted document, got nil")
	}

	// Verify other documents still exist
	doc1, err := coll.GetContext(ctx, "doc-001")
	if err != nil {
		t.Errorf("Failed to get existing document: %v", err)
	}
	if doc1 == nil || doc1.ID != "doc-001" {
		t.Error("Retrieved wrong document")
	}

	doc3, err := coll.GetContext(ctx, "doc-003")
	if err != nil {
		t.Errorf("Failed to get existing document: %v", err)
	}
	if doc3 == nil || doc3.ID != "doc-003" {
		t.Error("Retrieved wrong document")
	}

	// Verify deletion persists after reload
	if err := coll.Save(); err != nil {
		t.Fatalf("Failed to save collection: %v", err)
	}

	// Reopen collection
	coll.Close()
	coll2, err := NewCollection("test", tmpDir, &Config{
		Dimension: 128,
	})
	if err != nil {
		t.Fatalf("Failed to reopen collection: %v", err)
	}
	defer coll2.Close()

	// Verify deleted document is still deleted
	_, err = coll2.GetContext(ctx, "doc-002")
	if err == nil {
		t.Error("Expected error when getting deleted document after reload, got nil")
	}

	// Verify other documents still exist after reload
	doc1, err = coll2.GetContext(ctx, "doc-001")
	if err != nil {
		t.Errorf("Failed to get document after reload: %v", err)
	}
	if doc1 == nil || doc1.ID != "doc-001" {
		t.Error("Retrieved wrong document after reload")
	}

	t.Log("Collection Delete test passed - logical deletion working correctly")
}

// TestCollectionDVUpdate tests Update using DV (logical delete + insert)
func TestCollectionDVUpdate(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "collection_dv_update")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create collection
	coll, err := NewCollection("test", tmpDir, &Config{
		Dimension: 128,
	})
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}
	defer coll.Close()

	ctx := context.Background()

	// Insert a document
	originalDoc := &Document{
		ID:       "doc-001",
		Vector:   randomVector(128, 1),
		Metadata: map[string]interface{}{"version": "v1", "data": "original"},
	}

	if err := coll.InsertContext(ctx, originalDoc); err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// Update the document
	updatedDoc := &Document{
		ID:       "doc-001",
		Vector:   randomVector(128, 99), // Different vector
		Metadata: map[string]interface{}{"version": "v2", "data": "updated"},
	}

	if err := coll.UpdateContext(ctx, updatedDoc); err != nil {
		t.Fatalf("Failed to update document: %v", err)
	}

	// Verify the updated document is retrieved
	retrieved, err := coll.GetContext(ctx, "doc-001")
	if err != nil {
		t.Fatalf("Failed to get updated document: %v", err)
	}

	// Check metadata is updated
	if retrieved.Metadata["version"] != "v2" {
		t.Errorf("Expected version v2, got %v", retrieved.Metadata["version"])
	}
	if retrieved.Metadata["data"] != "updated" {
		t.Errorf("Expected data 'updated', got %v", retrieved.Metadata["data"])
	}

	// Verify update persists after reload
	if err := coll.Save(); err != nil {
		t.Fatalf("Failed to save collection: %v", err)
	}

	coll.Close()
	coll2, err := NewCollection("test", tmpDir, &Config{
		Dimension: 128,
	})
	if err != nil {
		t.Fatalf("Failed to reopen collection: %v", err)
	}
	defer coll2.Close()

	retrieved2, err := coll2.GetContext(ctx, "doc-001")
	if err != nil {
		t.Fatalf("Failed to get document after reload: %v", err)
	}
	if retrieved2.Metadata["version"] != "v2" {
		t.Errorf("Expected version v2 after reload, got %v", retrieved2.Metadata["version"])
	}

	t.Log("Collection Update test passed - DV-based update working correctly")
}

// TestCollectionDVSearch tests that search filters out deleted documents
func TestCollectionDVSearch(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "collection_dv_search")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create collection with small dimension for predictable search
	coll, err := NewCollection("test", tmpDir, &Config{
		Dimension: 10,
	})
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}
	defer coll.Close()

	ctx := context.Background()

	// Insert documents with specific vectors for predictable results
	// Using basis vectors for predictable nearest neighbor
	docs := []*Document{
		{ID: "doc-001", Vector: []float32{1, 0, 0, 0, 0, 0, 0, 0, 0, 0}, Metadata: map[string]interface{}{"name": "doc1"}},
		{ID: "doc-002", Vector: []float32{0, 1, 0, 0, 0, 0, 0, 0, 0, 0}, Metadata: map[string]interface{}{"name": "doc2"}},
		{ID: "doc-003", Vector: []float32{0, 0, 1, 0, 0, 0, 0, 0, 0, 0}, Metadata: map[string]interface{}{"name": "doc3"}},
		{ID: "doc-004", Vector: []float32{0, 0, 0, 1, 0, 0, 0, 0, 0, 0}, Metadata: map[string]interface{}{"name": "doc4"}},
	}

	for _, doc := range docs {
		if err := coll.InsertContext(ctx, doc); err != nil {
			t.Fatalf("Failed to insert document: %v", err)
		}
	}

	// Search before deletion - should find all 4 documents
	query := []float32{0.5, 0.5, 0.5, 0.5, 0, 0, 0, 0, 0, 0}
	results, err := coll.SearchContext(ctx, query, 4)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if len(results) != 4 {
		t.Errorf("Expected 4 results before deletion, got %d", len(results))
	}

	// Delete doc-002
	if err := coll.DeleteContext(ctx, "doc-002"); err != nil {
		t.Fatalf("Failed to delete document: %v", err)
	}

	// Search after deletion - deleted doc should not appear
	results, err = coll.SearchContext(ctx, query, 4)
	if err != nil {
		t.Fatalf("Search failed after deletion: %v", err)
	}

	// Check that doc-002 is not in results
	for _, r := range results {
		if r.Document.ID == "doc-002" {
			t.Error("Found deleted document in search results")
		}
	}

	// Delete another document
	if err := coll.DeleteContext(ctx, "doc-004"); err != nil {
		t.Fatalf("Failed to delete document: %v", err)
	}

	// Search again
	results, err = coll.SearchContext(ctx, query, 4)
	if err != nil {
		t.Fatalf("Search failed after second deletion: %v", err)
	}

	// Check that neither deleted doc appears
	for _, r := range results {
		if r.Document.ID == "doc-002" || r.Document.ID == "doc-004" {
			t.Error("Found deleted document in search results after second deletion")
		}
	}

	// Verify we can still find the non-deleted documents
	foundDoc1, foundDoc3 := false, false
	for _, r := range results {
		if r.Document.ID == "doc-001" {
			foundDoc1 = true
		}
		if r.Document.ID == "doc-003" {
			foundDoc3 = true
		}
	}
	if !foundDoc1 {
		t.Error("Could not find doc-001 in search results")
	}
	if !foundDoc3 {
		t.Error("Could not find doc-003 in search results")
	}

	t.Log("Search with deletes test passed - DV filtering working correctly")
}

// TestCollectionDVDeleteNonExistent tests deleting a non-existent document
func TestCollectionDVDeleteNonExistent(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "collection_dv_delete_missing")
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

	ctx := context.Background()

	// Try to delete non-existent document
	err = coll.DeleteContext(ctx, "non-existent")
	if err == nil {
		t.Error("Expected error when deleting non-existent document, got nil")
	}

	t.Log("Delete non-existent document test passed")
}

// TestCollectionDVUpdateNonExistent tests updating a non-existent document
func TestCollectionDVUpdateNonExistent(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "collection_dv_update_missing")
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

	ctx := context.Background()

	// Try to update non-existent document
	doc := &Document{
		ID:     "non-existent",
		Vector: randomVector(128, 1),
	}

	err = coll.UpdateContext(ctx, doc)
	if err == nil {
		t.Error("Expected error when updating non-existent document, got nil")
	}

	t.Log("Update non-existent document test passed")
}

// TestCollectionDVDeleteBatch tests batch deletion
func TestCollectionDVDeleteBatch(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "collection_dv_delete_batch")
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

	ctx := context.Background()

	// Insert documents
	for i := 1; i <= 5; i++ {
		doc := &Document{
			ID:     fmt.Sprintf("doc-%03d", i),
			Vector: randomVector(128, i),
		}
		if err := coll.InsertContext(ctx, doc); err != nil {
			t.Fatalf("Failed to insert document: %v", err)
		}
	}

	// Delete batch
	idsToDelete := []string{"doc-001", "doc-003", "doc-005"}
	if err := coll.DeleteBatchContext(ctx, idsToDelete); err != nil {
		t.Fatalf("Failed to delete batch: %v", err)
	}

	// Verify deleted documents are gone
	for _, id := range idsToDelete {
		_, err := coll.GetContext(ctx, id)
		if err == nil {
			t.Errorf("Expected error when getting deleted document %s", id)
		}
	}

	// Verify other documents still exist
	for _, id := range []string{"doc-002", "doc-004"} {
		_, err := coll.GetContext(ctx, id)
		if err != nil {
			t.Errorf("Failed to get existing document %s: %v", id, err)
		}
	}

	t.Log("Delete batch test passed")
}

// Helper function for random vector generation
func randomVector(dim, seed int) []float32 {
	vec := make([]float32, dim)
	for i := range vec {
		vec[i] = float32(seed*i%100) / 100.0
	}
	return vec
}
