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

// TestCollectionForEach tests the ForEach method
func TestCollectionForEach(t *testing.T) {
	coll, cleanup := setupTestCollection(t)
	defer cleanup()

	// Insert 3 test documents into shared collection
	docs := []*Document{
		createTestDocument("foreach_1", 64, map[string]interface{}{"idx": 1}),
		createTestDocument("foreach_2", 64, map[string]interface{}{"idx": 2}),
		createTestDocument("foreach_3", 64, map[string]interface{}{"idx": 3}),
	}
	for _, doc := range docs {
		if err := coll.Insert(doc); err != nil {
			t.Fatalf("Failed to insert document: %v", err)
		}
	}

	t.Run("Iterate all documents", func(t *testing.T) {
		var count int
		var ids []string
		err := coll.ForEach(func(doc *Document) bool {
			count++
			ids = append(ids, doc.ID)
			return true
		})
		if err != nil {
			t.Fatalf("ForEach failed: %v", err)
		}
		if count != 3 {
			t.Errorf("Expected 3 documents, got %d", count)
		}
		if len(ids) != 3 {
			t.Errorf("Expected 3 IDs, got %d", len(ids))
		}
	})

	t.Run("Early stop", func(t *testing.T) {
		var count int
		err := coll.ForEach(func(doc *Document) bool {
			count++
			return false // stop after first
		})
		if err != nil {
			t.Fatalf("ForEach failed: %v", err)
		}
		if count != 1 {
			t.Errorf("Expected 1 document after early stop, got %d", count)
		}
	})

	t.Run("Buffer deduplicates with disk", func(t *testing.T) {
		// Use independent collection to avoid state pollution
		tmpDir := t.TempDir()
		cfg := &Config{Dimension: 64, M: 16, EfConstruction: 200}
		c, err := NewCollection("dedup", tmpDir, cfg)
		if err != nil {
			t.Fatalf("Failed to create collection: %v", err)
		}
		defer c.Close()

		// Insert v1 and flush to disk
		docV1 := createTestDocument("dedup_doc", 64, map[string]interface{}{"version": 1})
		if err := c.Insert(docV1); err != nil {
			t.Fatalf("Failed to insert v1: %v", err)
		}
		if err := c.Save(); err != nil {
			t.Fatalf("Failed to save: %v", err)
		}

		// Update to v2 — new version stays in writeBuffer, old version on disk
		docV2 := createTestDocument("dedup_doc", 64, map[string]interface{}{"version": 2})
		if err := c.Update(docV2); err != nil {
			t.Fatalf("Failed to update to v2: %v", err)
		}

		var count int
		var versions []int
		err = c.ForEach(func(doc *Document) bool {
			count++
			if v, ok := doc.Metadata["version"].(int); ok {
				versions = append(versions, v)
			}
			return true
		})
		if err != nil {
			t.Fatalf("ForEach failed: %v", err)
		}
		if count != 1 {
			t.Errorf("Expected 1 document (deduplicated), got %d", count)
		}
		if len(versions) != 1 || versions[0] != 2 {
			t.Errorf("Expected version 2 (from buffer), got %v", versions)
		}
	})

	t.Run("Context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		err := coll.ForEachContext(ctx, func(doc *Document) bool {
			return true
		})
		if err != context.Canceled {
			t.Errorf("Expected context.Canceled, got %v", err)
		}
	})

	t.Run("Context cancellation during iteration", func(t *testing.T) {
		tmpDir := t.TempDir()
		cfg := &Config{Dimension: 64, M: 16, EfConstruction: 200}
		c, err := NewCollection("cancel_iter", tmpDir, cfg)
		if err != nil {
			t.Fatalf("Failed to create collection: %v", err)
		}
		defer c.Close()

		for i := 0; i < 5; i++ {
			doc := createTestDocument(fmt.Sprintf("cancel_%d", i), 64, nil)
			if err := c.Insert(doc); err != nil {
				t.Fatalf("Failed to insert: %v", err)
			}
		}
		if err := c.Save(); err != nil {
			t.Fatalf("Failed to save: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		var count int
		err = c.ForEachContext(ctx, func(doc *Document) bool {
			count++
			if count == 2 {
				cancel() // cancel after processing 2 docs
			}
			return true
		})
		if err != context.Canceled {
			t.Errorf("Expected context.Canceled, got %v (count=%d)", err, count)
		}
		if count < 2 {
			t.Errorf("Expected at least 2 docs processed before cancel, got %d", count)
		}
	})

	t.Run("Deleted documents are excluded", func(t *testing.T) {
		// Use independent collection to avoid state pollution
		tmpDir := t.TempDir()
		cfg := &Config{Dimension: 64, M: 16, EfConstruction: 200}
		c, err := NewCollection("del", tmpDir, cfg)
		if err != nil {
			t.Fatalf("Failed to create collection: %v", err)
		}
		defer c.Close()

		for i := 1; i <= 3; i++ {
			doc := createTestDocument(fmt.Sprintf("del_%d", i), 64, nil)
			if err := c.Insert(doc); err != nil {
				t.Fatalf("Failed to insert: %v", err)
			}
		}

		if err := c.Delete("del_2"); err != nil {
			t.Fatalf("Failed to delete document: %v", err)
		}

		var ids []string
		err = c.ForEach(func(doc *Document) bool {
			ids = append(ids, doc.ID)
			return true
		})
		if err != nil {
			t.Fatalf("ForEach failed: %v", err)
		}
		for _, id := range ids {
			if id == "del_2" {
				t.Error("Deleted document del_2 should not appear in ForEach")
			}
		}
		if len(ids) != 2 {
			t.Errorf("Expected 2 documents after delete, got %d", len(ids))
		}
	})

	t.Run("Empty collection", func(t *testing.T) {
		tmpDir := t.TempDir()
		cfg := &Config{Dimension: 64, M: 16, EfConstruction: 200}
		c, err := NewCollection("empty", tmpDir, cfg)
		if err != nil {
			t.Fatalf("Failed to create collection: %v", err)
		}
		defer c.Close()

		var count int
		err = c.ForEach(func(doc *Document) bool {
			count++
			return true
		})
		if err != nil {
			t.Fatalf("ForEach on empty collection failed: %v", err)
		}
		if count != 0 {
			t.Errorf("Expected 0 documents in empty collection, got %d", count)
		}
	})

	t.Run("Persisted documents only", func(t *testing.T) {
		tmpDir := t.TempDir()
		cfg := &Config{Dimension: 64, M: 16, EfConstruction: 200}
		c, err := NewCollection("persisted", tmpDir, cfg)
		if err != nil {
			t.Fatalf("Failed to create collection: %v", err)
		}
		defer c.Close()

		for i := 1; i <= 3; i++ {
			doc := createTestDocument(fmt.Sprintf("persisted_%d", i), 64, map[string]interface{}{"idx": i})
			if err := c.Insert(doc); err != nil {
				t.Fatalf("Failed to insert: %v", err)
			}
		}
		if err := c.Save(); err != nil {
			t.Fatalf("Failed to save: %v", err)
		}

		var count int
		var ids []string
		err = c.ForEach(func(doc *Document) bool {
			count++
			ids = append(ids, doc.ID)
			return true
		})
		if err != nil {
			t.Fatalf("ForEach failed: %v", err)
		}
		if count != 3 {
			t.Errorf("Expected 3 persisted documents, got %d", count)
		}
		if len(ids) != 3 {
			t.Errorf("Expected 3 IDs, got %d", len(ids))
		}
	})

	t.Run("Concurrent read during write", func(t *testing.T) {
		tmpDir := t.TempDir()
		cfg := &Config{Dimension: 64, M: 16, EfConstruction: 200, AutoCompact: false}
		c, err := NewCollection("concurrent", tmpDir, cfg)
		if err != nil {
			t.Fatalf("Failed to create collection: %v", err)
		}
		defer c.Close()

		writerDone := make(chan struct{})
		go func() {
			defer close(writerDone)
			for i := 0; i < 100; i++ {
				doc := createTestDocument(fmt.Sprintf("concurrent_%d", i), 64, nil)
				if err := c.Insert(doc); err != nil {
					return
				}
				if i%10 == 0 && i > 0 {
					_ = c.Delete(fmt.Sprintf("concurrent_%d", i-10))
				}
			}
		}()

		readerDone := make(chan struct{})
		go func() {
			defer close(readerDone)
			for i := 0; i < 50; i++ {
				_ = c.ForEach(func(doc *Document) bool {
					return true
				})
			}
		}()

		select {
		case <-writerDone:
		case <-time.After(10 * time.Second):
			t.Fatal("Writer goroutine deadlock or timeout")
		}

		select {
		case <-readerDone:
		case <-time.After(10 * time.Second):
			t.Fatal("Reader goroutine deadlock or timeout")
		}
	})

	t.Run("Closed storage", func(t *testing.T) {
		tmpDir := t.TempDir()
		cfg := &Config{Dimension: 64, M: 16, EfConstruction: 200}
		c, err := NewCollection("closed", tmpDir, cfg)
		if err != nil {
			t.Fatalf("Failed to create collection: %v", err)
		}
		// Insert and save a document so Close() doesn't fail on empty HNSW
		doc := createTestDocument("closed_doc", 64, nil)
		if err := c.Insert(doc); err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
		if err := c.Save(); err != nil {
			t.Fatalf("Failed to save: %v", err)
		}
		if err := c.Close(); err != nil {
			t.Fatalf("Failed to close collection: %v", err)
		}

		err = c.ForEach(func(doc *Document) bool {
			return true
		})
		if err == nil {
			t.Error("Expected error for closed storage, got nil")
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
	
	t.Run("InsertBatchContext with cancellation", func(t *testing.T) {
		docs := []*Document{
			createTestDocument("ctx_batch1", 64, nil),
			createTestDocument("ctx_batch2", 64, nil),
		}
		
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately
		
		err := coll.InsertBatchContext(ctx, docs)
		if err != context.Canceled {
			t.Errorf("Expected context.Canceled, got %v", err)
		}
	})
	
	t.Run("GetBatch", func(t *testing.T) {
		// First insert some documents
		docs := []*Document{
			createTestDocument("getbatch1", 64, map[string]interface{}{"key": "val1"}),
			createTestDocument("getbatch2", 64, map[string]interface{}{"key": "val2"}),
			createTestDocument("getbatch3", 64, map[string]interface{}{"key": "val3"}),
		}
		coll.InsertBatch(docs)
		
		// Get batch
		results, err := coll.GetBatch([]string{"getbatch1", "getbatch2", "non_existent"})
		if err != nil {
			t.Errorf("GetBatch failed: %v", err)
		}
		if len(results) != 2 {
			t.Errorf("Expected 2 results, got %d", len(results))
		}
		if _, ok := results["getbatch1"]; !ok {
			t.Error("Expected getbatch1 in results")
		}
		if _, ok := results["getbatch2"]; !ok {
			t.Error("Expected getbatch2 in results")
		}
	})
	
	t.Run("GetBatchContext with cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately
		
		_, err := coll.GetBatchContext(ctx, []string{"getbatch1"})
		if err != context.Canceled {
			t.Errorf("Expected context.Canceled, got %v", err)
		}
	})
	
	t.Run("DeleteBatch", func(t *testing.T) {
		// Insert documents to delete
		docs := []*Document{
			createTestDocument("delbatch1", 64, nil),
			createTestDocument("delbatch2", 64, nil),
			createTestDocument("delbatch3", 64, nil),
		}
		coll.InsertBatch(docs)
		
		// Delete batch
		if err := coll.DeleteBatch([]string{"delbatch1", "delbatch2", "non_existent"}); err != nil {
			t.Errorf("DeleteBatch failed: %v", err)
		}
		
		// Verify deletion
		_, err1 := coll.Get("delbatch1")
		_, err2 := coll.Get("delbatch2")
		if err1 == nil || err2 == nil {
			t.Error("Expected documents to be deleted")
		}
		
		// Verify delbatch3 still exists
		_, err3 := coll.Get("delbatch3")
		if err3 != nil {
			t.Error("Expected delbatch3 to still exist")
		}
	})
	
	t.Run("DeleteBatchContext with cancellation", func(t *testing.T) {
		// Insert documents to delete
		docs := []*Document{
			createTestDocument("delctx1", 64, nil),
			createTestDocument("delctx2", 64, nil),
		}
		coll.InsertBatch(docs)
		
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately
		
		err := coll.DeleteBatchContext(ctx, []string{"delctx1", "delctx2"})
		if err != context.Canceled {
			t.Errorf("Expected context.Canceled, got %v", err)
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


