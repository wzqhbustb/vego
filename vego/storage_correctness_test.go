package vego

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestDocumentStorageRowIndexCorrectness verifies RowIndex path correctness
func TestDocumentStorageRowIndexCorrectness(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vego-rowindex-correctness-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	storage, err := NewDocumentStorage(filepath.Join(tmpDir, "storage"), 64)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	// Test 1: 多文档场景（100个文档）
	t.Run("MultipleDocuments", func(t *testing.T) {
		numDocs := 100
		docs := make([]*Document, numDocs)
		
		// Create 100 documents
		for i := 0; i < numDocs; i++ {
			docs[i] = &Document{
				ID:     fmt.Sprintf("doc-%d", i),
				Vector: makeTestVector(64, float32(i)),
				Metadata: map[string]interface{}{
					"index": i,
					"name":  fmt.Sprintf("name-%d", i),
				},
				Timestamp: time.Now().Add(time.Duration(i) * time.Second),
			}
		}
		
		// Put all documents
		for _, doc := range docs {
			if err := storage.Put(doc); err != nil {
				t.Fatalf("Failed to put document %s: %v", doc.ID, err)
			}
		}
		
		if err := storage.Flush(); err != nil {
			t.Fatalf("Failed to flush: %v", err)
		}
		
		// Random sampling verification (check 20 random documents)
		rand.Seed(time.Now().UnixNano())
		for i := 0; i < 20; i++ {
			idx := rand.Intn(numDocs)
			expected := docs[idx]
			got, err := storage.Get(expected.ID)
			
			if err != nil {
				t.Errorf("Failed to get document %s: %v", expected.ID, err)
				continue
			}
			
			if got.ID != expected.ID {
				t.Errorf("ID mismatch for doc-%d: got %s, want %s", idx, got.ID, expected.ID)
			}
			
			// Verify vector values
			expectedValue := float32(idx)
			if len(got.Vector) > 0 && got.Vector[0] != expectedValue {
				t.Errorf("Vector mismatch for doc-%d: first value %v, want %v", 
					idx, got.Vector[0], expectedValue)
			}
			
			if got.Metadata["index"] != expected.Metadata["index"] {
				t.Errorf("Metadata mismatch for doc-%d", idx)
			}
		}
		
		t.Logf("Multiple documents test passed - %d docs written, 20 random samples verified", numDocs)
	})

	// Test 2: 更新场景
	t.Run("UpdateDocument", func(t *testing.T) {
		// Write v1
		v1 := makeTestVector(64, 1.0)
		docV1 := &Document{
			ID:        "update-test-doc",
			Vector:    v1,
			Metadata:  map[string]interface{}{"version": 1},
			Timestamp: time.Now(),
		}
		
		if err := storage.Put(docV1); err != nil {
			t.Fatalf("Failed to put v1: %v", err)
		}
		if err := storage.Flush(); err != nil {
			t.Fatalf("Failed to flush v1: %v", err)
		}
		
		// Update to v2 (same ID, different data)
		time.Sleep(10 * time.Millisecond) // Ensure different timestamp
		v2 := makeTestVector(64, 2.0)
		docV2 := &Document{
			ID:        "update-test-doc",
			Vector:    v2,
			Metadata:  map[string]interface{}{"version": 2},
			Timestamp: time.Now(),
		}
		
		if err := storage.Put(docV2); err != nil {
			t.Fatalf("Failed to put v2: %v", err)
		}
		if err := storage.Flush(); err != nil {
			t.Fatalf("Failed to flush v2: %v", err)
		}
		
		// Verify reads v2, not v1
		got, err := storage.Get("update-test-doc")
		if err != nil {
			t.Fatalf("Failed to get updated document: %v", err)
		}
		
		if len(got.Vector) > 0 && got.Vector[0] != 2.0 {
			t.Errorf("Expected v2 vector (first value = 2.0), got %v", got.Vector[0])
		}
		
		if got.Metadata["version"] != 2 {
			t.Errorf("Expected version 2 metadata, got %v", got.Metadata["version"])
		}
		
		t.Logf("Update document test passed - correctly read v2 after update")
	})

	// Test 3: 删除场景
	t.Run("DeleteDocument", func(t *testing.T) {
		doc := &Document{
			ID:     "delete-test-doc",
			Vector: makeTestVector(64, 3.0),
		}
		
		// Put and flush
		if err := storage.Put(doc); err != nil {
			t.Fatalf("Failed to put document: %v", err)
		}
		if err := storage.Flush(); err != nil {
			t.Fatalf("Failed to flush: %v", err)
		}
		
		// Verify exists
		_, err := storage.Get("delete-test-doc")
		if err != nil {
			t.Fatalf("Document should exist before deletion: %v", err)
		}
		
		// Delete and flush
		if err := storage.Delete("delete-test-doc"); err != nil {
			t.Fatalf("Failed to delete document: %v", err)
		}
		if err := storage.Flush(); err != nil {
			t.Fatalf("Failed to flush after delete: %v", err)
		}
		
		// Verify deleted
		_, err = storage.Get("delete-test-doc")
		if err != ErrDocumentNotFound {
			t.Errorf("Expected ErrDocumentNotFound after delete, got %v", err)
		}
		
		t.Logf("Delete document test passed - document correctly removed")
	})

	// Test 4: 查询不存在 ID
	t.Run("NonExistentID", func(t *testing.T) {
		// Put some documents first
		for i := 0; i < 5; i++ {
			doc := &Document{
				ID:     fmt.Sprintf("existing-%d", i),
				Vector: makeTestVector(64, float32(i)),
			}
			if err := storage.Put(doc); err != nil {
				t.Fatalf("Failed to put document: %v", err)
			}
		}
		if err := storage.Flush(); err != nil {
			t.Fatalf("Failed to flush: %v", err)
		}
		
		// Query non-existent ID
		nonExistentIDs := []string{
			"does-not-exist",
			"",
			"existing-999", // Similar pattern but doesn't exist
		}
		
		for _, id := range nonExistentIDs {
			_, err := storage.Get(id)
			if err != ErrDocumentNotFound {
				t.Errorf("ID %q: Expected ErrDocumentNotFound, got %v", id, err)
			}
		}
		
		t.Logf("Non-existent ID test passed - all queries returned ErrDocumentNotFound")
	})

	// Test 5: Buffer 未 Flush 场景
	t.Run("UnflushedBuffer", func(t *testing.T) {
		doc := &Document{
			ID:     "buffer-test-doc",
			Vector: makeTestVector(64, 5.0),
		}
		
		// Put but don't flush
		if err := storage.Put(doc); err != nil {
			t.Fatalf("Failed to put document: %v", err)
		}
		
		// Should still be able to get from buffer (not via RowIndex)
		got, err := storage.Get("buffer-test-doc")
		if err != nil {
			t.Fatalf("Should get document from buffer: %v", err)
		}
		if got.ID != "buffer-test-doc" {
			t.Errorf("ID mismatch: got %s", got.ID)
		}
		
		t.Logf("Buffer test passed - document read from buffer before flush")
	})

	storage.Close()
}
