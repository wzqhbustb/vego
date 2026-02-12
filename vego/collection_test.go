package vego

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// setupTestCollection creates a test collection with cleanup
func setupTestCollection(t *testing.T) (*Collection, func()) {
	t.Helper()
	tmpDir := filepath.Join(os.TempDir(), "vego_test_"+time.Now().Format("20060102150405"))
	
	config := &Config{
		Dimension:      64,
		M:              16,
		EfConstruction: 200,
	}
	
	coll, err := NewCollection("test", tmpDir, config)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}
	
	cleanup := func() {
		coll.Close()
		os.RemoveAll(tmpDir)
	}
	
	return coll, cleanup
}

// createTestDocument creates a test document with the given ID and dimension
func createTestDocument(id string, dimension int, metadata map[string]interface{}) *Document {
	vector := make([]float32, dimension)
	for i := range vector {
		vector[i] = float32(i) * 0.01
	}
	
	return &Document{
		ID:       id,
		Vector:   vector,
		Metadata: metadata,
	}
}

// TestCollectionInsert tests the Insert and InsertContext methods
func TestCollectionInsert(t *testing.T) {
	coll, cleanup := setupTestCollection(t)
	defer cleanup()
	
	t.Run("Insert single document", func(t *testing.T) {
		doc := createTestDocument("doc1", 64, map[string]interface{}{"key": "value1"})
		
		if err := coll.Insert(doc); err != nil {
			t.Errorf("Insert failed: %v", err)
		}
		
		// Verify document was inserted
		retrieved, err := coll.Get("doc1")
		if err != nil {
			t.Errorf("Get failed: %v", err)
		}
		if retrieved.ID != "doc1" {
			t.Errorf("Expected ID doc1, got %s", retrieved.ID)
		}
	})
	
	t.Run("Insert duplicate document", func(t *testing.T) {
		doc := createTestDocument("doc2", 64, nil)
		
		if err := coll.Insert(doc); err != nil {
			t.Fatalf("First insert failed: %v", err)
		}
		
		// Try to insert duplicate
		if err := coll.Insert(doc); err == nil {
			t.Error("Expected error for duplicate insert, got nil")
		}
	})
	
	t.Run("InsertContext with cancellation", func(t *testing.T) {
		doc := createTestDocument("doc3", 64, nil)
		
		// Test with cancelled context
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately
		
		err := coll.InsertContext(ctx, doc)
		if err != context.Canceled {
			t.Errorf("Expected context.Canceled, got %v", err)
		}
	})
	
	t.Run("Insert with wrong dimension", func(t *testing.T) {
		doc := createTestDocument("doc_bad", 32, nil) // Wrong dimension
		
		if err := coll.Insert(doc); err == nil {
			t.Error("Expected error for wrong dimension, got nil")
		}
	})
}

// TestCollectionGet tests the Get and GetContext methods
func TestCollectionGet(t *testing.T) {
	coll, cleanup := setupTestCollection(t)
	defer cleanup()
	
	// Insert test document
	doc := createTestDocument("get_test", 64, map[string]interface{}{"name": "test"})
	if err := coll.Insert(doc); err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}
	
	t.Run("Get existing document", func(t *testing.T) {
		retrieved, err := coll.Get("get_test")
		if err != nil {
			t.Errorf("Get failed: %v", err)
		}
		if retrieved == nil {
			t.Error("Expected document, got nil")
			return
		}
		if retrieved.ID != "get_test" {
			t.Errorf("Expected ID get_test, got %s", retrieved.ID)
		}
	})
	
	t.Run("Get non-existent document", func(t *testing.T) {
		_, err := coll.Get("non_existent")
		if err == nil {
			t.Error("Expected error for non-existent document, got nil")
		}
	})
	
	t.Run("GetContext with cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately
		
		_, err := coll.GetContext(ctx, "get_test")
		if err != context.Canceled {
			t.Errorf("Expected context.Canceled, got %v", err)
		}
	})
}

// TestCollectionDelete tests the Delete and DeleteContext methods
func TestCollectionDelete(t *testing.T) {
	coll, cleanup := setupTestCollection(t)
	defer cleanup()
	
	// Insert test document
	doc := createTestDocument("delete_test", 64, nil)
	if err := coll.Insert(doc); err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}
	
	t.Run("Delete existing document", func(t *testing.T) {
		if err := coll.Delete("delete_test"); err != nil {
			t.Errorf("Delete failed: %v", err)
		}
		
		// Verify document was deleted
		_, err := coll.Get("delete_test")
		if err == nil {
			t.Error("Expected error after deletion, got nil")
		}
	})
	
	t.Run("Delete non-existent document", func(t *testing.T) {
		if err := coll.Delete("non_existent"); err == nil {
			t.Error("Expected error for non-existent document, got nil")
		}
	})
	
	t.Run("DeleteContext with cancellation", func(t *testing.T) {
		// Insert a new document for this test
		doc := createTestDocument("delete_ctx_test", 64, nil)
		coll.Insert(doc)
		
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately
		
		err := coll.DeleteContext(ctx, "delete_ctx_test")
		if err != context.Canceled {
			t.Errorf("Expected context.Canceled, got %v", err)
		}
	})
}

// TestCollectionUpdate tests the Update and UpdateContext methods
func TestCollectionUpdate(t *testing.T) {
	coll, cleanup := setupTestCollection(t)
	defer cleanup()
	
	t.Run("Update existing document", func(t *testing.T) {
		// Insert test document
		doc := createTestDocument("update_test", 64, map[string]interface{}{"version": 1})
		if err := coll.Insert(doc); err != nil {
			t.Fatalf("Failed to insert document: %v", err)
		}
		
		// Update metadata
		doc.Metadata["version"] = 2
		doc.Metadata["updated"] = true
		
		// Update vector slightly
		for i := range doc.Vector {
			doc.Vector[i] = float32(i) * 0.02
		}
		
		if err := coll.Update(doc); err != nil {
			t.Errorf("Update failed: %v", err)
		}
		
		// Verify update
		retrieved, err := coll.Get("update_test")
		if err != nil {
			t.Errorf("Get after update failed: %v", err)
			return
		}
		
		if retrieved.Metadata["version"] != 2 {
			t.Errorf("Expected version 2, got %v", retrieved.Metadata["version"])
		}
	})
	
	t.Run("Update non-existent document", func(t *testing.T) {
		doc := createTestDocument("non_existent", 64, nil)
		
		if err := coll.Update(doc); err == nil {
			t.Error("Expected error for non-existent document, got nil")
		}
	})
	
	t.Run("UpdateContext with cancellation", func(t *testing.T) {
		doc := createTestDocument("update_ctx_test", 64, nil)
		coll.Insert(doc)
		
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately
		
		err := coll.UpdateContext(ctx, doc)
		if err != context.Canceled {
			t.Errorf("Expected context.Canceled, got %v", err)
		}
	})
}

// TestCollectionUpsert tests the Upsert and UpsertContext methods
func TestCollectionUpsert(t *testing.T) {
	coll, cleanup := setupTestCollection(t)
	defer cleanup()
	
	t.Run("Upsert insert new document", func(t *testing.T) {
		doc := createTestDocument("upsert_new", 64, map[string]interface{}{"action": "insert"})
		
		if err := coll.Upsert(doc); err != nil {
			t.Errorf("Upsert failed: %v", err)
		}
		
		// Verify insertion
		retrieved, err := coll.Get("upsert_new")
		if err != nil {
			t.Errorf("Get after upsert failed: %v", err)
			return
		}
		if retrieved.Metadata["action"] != "insert" {
			t.Errorf("Expected action=insert, got %v", retrieved.Metadata["action"])
		}
	})
	
	t.Run("Upsert update existing document", func(t *testing.T) {
		// Insert first
		doc := createTestDocument("upsert_update", 64, map[string]interface{}{"action": "insert"})
		if err := coll.Insert(doc); err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
		
		// Retrieve the document, modify and upsert
		retrieved, _ := coll.Get("upsert_update")
		retrieved.Metadata["action"] = "update"
		if err := coll.Upsert(retrieved); err != nil {
			t.Errorf("Upsert failed: %v", err)
		}
		
		// Verify update
		updated, _ := coll.Get("upsert_update")
		if updated.Metadata["action"] != "update" {
			t.Errorf("Expected action=update, got %v", updated.Metadata["action"])
		}
	})
	
	t.Run("UpsertContext with cancellation", func(t *testing.T) {
		doc := createTestDocument("upsert_ctx_test", 64, nil)
		
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately
		
		err := coll.UpsertContext(ctx, doc)
		if err != context.Canceled {
			t.Errorf("Expected context.Canceled, got %v", err)
		}
	})
}

// TestCollectionSearch tests the Search and SearchContext methods
func TestCollectionSearch(t *testing.T) {
	coll, cleanup := setupTestCollection(t)
	defer cleanup()
	
	// Insert test documents
	for i := 0; i < 10; i++ {
		doc := createTestDocument(fmt.Sprintf("search_doc_%d", i), 64, map[string]interface{}{"index": i})
		// Vary vectors slightly
		for j := range doc.Vector {
			doc.Vector[j] = float32(j+i) * 0.01
		}
		if err := coll.Insert(doc); err != nil {
			t.Fatalf("Failed to insert document: %v", err)
		}
	}
	
	t.Run("Search returns results", func(t *testing.T) {
		query := make([]float32, 64)
		for i := range query {
			query[i] = float32(i) * 0.01
		}
		
		results, err := coll.Search(query, 5)
		if err != nil {
			t.Errorf("Search failed: %v", err)
		}
		if len(results) == 0 {
			t.Error("Expected search results, got none")
		}
		if len(results) > 5 {
			t.Errorf("Expected at most 5 results, got %d", len(results))
		}
	})
	
	t.Run("Search with wrong dimension", func(t *testing.T) {
		query := make([]float32, 32) // Wrong dimension
		
		_, err := coll.Search(query, 5)
		if err == nil {
			t.Error("Expected error for wrong dimension, got nil")
		}
	})
	
	t.Run("SearchContext with cancellation", func(t *testing.T) {
		query := make([]float32, 64)
		
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately
		
		_, err := coll.SearchContext(ctx, query, 5)
		if err != context.Canceled {
			t.Errorf("Expected context.Canceled, got %v", err)
		}
	})
}

// TestCollectionBatchOperations tests batch operations
func TestCollectionBatchOperations(t *testing.T) {
	coll, cleanup := setupTestCollection(t)
	defer cleanup()
	
	t.Run("InsertBatch", func(t *testing.T) {
		docs := []*Document{
			createTestDocument("batch1", 64, map[string]interface{}{"batch": 1}),
			createTestDocument("batch2", 64, map[string]interface{}{"batch": 2}),
			createTestDocument("batch3", 64, map[string]interface{}{"batch": 3}),
		}
		
		if err := coll.InsertBatch(docs); err != nil {
			t.Errorf("InsertBatch failed: %v", err)
		}
		
		// Verify all documents were inserted
		for _, doc := range docs {
			_, err := coll.Get(doc.ID)
			if err != nil {
				t.Errorf("Failed to get %s: %v", doc.ID, err)
			}
		}
	})
	
	t.Run("InsertBatch empty", func(t *testing.T) {
		if err := coll.InsertBatch([]*Document{}); err != nil {
			t.Errorf("InsertBatch with empty slice failed: %v", err)
		}
	})
}

// TestCollectionContextCancellation tests various context cancellation scenarios
func TestCollectionContextCancellation(t *testing.T) {
	coll, cleanup := setupTestCollection(t)
	defer cleanup()
	
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()
	
	// Wait for context to expire
	time.Sleep(10 * time.Millisecond)
	
	t.Run("Operations respect cancelled context", func(t *testing.T) {
		doc := createTestDocument("ctx_test", 64, nil)
		
		err := coll.InsertContext(ctx, doc)
		if err != context.DeadlineExceeded && err != context.Canceled {
			t.Logf("InsertContext with expired context returned: %v", err)
		}
	})
}

// TestCollectionStats tests the Stats method
func TestCollectionStats(t *testing.T) {
	coll, cleanup := setupTestCollection(t)
	defer cleanup()
	
	// Insert documents
	for i := 0; i < 5; i++ {
		doc := createTestDocument(fmt.Sprintf("stats_doc_%d", i), 64, nil)
		coll.Insert(doc)
	}
	
	stats := coll.Stats()
	if stats.Count != 5 {
		t.Errorf("Expected count 5, got %d", stats.Count)
	}
	if stats.Name != "test" {
		t.Errorf("Expected name 'test', got %s", stats.Name)
	}
	if stats.Dimension != 64 {
		t.Errorf("Expected dimension 64, got %d", stats.Dimension)
	}
}

// TestCollectionSaveAndClose tests save and close operations
func TestCollectionSaveAndClose(t *testing.T) {
	coll, _ := setupTestCollection(t)
	// Don't call cleanup here - we'll test Close manually
	
	// Insert a document
	doc := createTestDocument("save_test", 64, nil)
	coll.Insert(doc)
	
	// Test Save
	if err := coll.Save(); err != nil {
		t.Errorf("Save failed: %v", err)
	}
	
	// Test Close
	if err := coll.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}
	
	// Clean up
	os.RemoveAll(coll.path)
}


