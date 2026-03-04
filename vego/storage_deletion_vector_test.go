// Copyright 2024 Vego Authors
// Licensed under the Apache License, Version 2.0

package vego

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/wzqhbustb/vego/index"
)

func TestDocumentStorageMarkDeleted(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "dv_storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	storage, err := NewDocumentStorage(tmpDir, 128)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()

	// Create and store a document
	doc := &Document{
		ID:     "doc-001",
		Vector: make([]float32, 128),
		Metadata: map[string]interface{}{
			"title": "Test Document",
		},
	}

	if err := storage.Put(doc); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Flush to persist
	if err := storage.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Verify document exists
	if _, err := storage.Get("doc-001"); err != nil {
		t.Fatalf("Get failed before delete: %v", err)
	}

	// Mark as deleted
	if err := storage.MarkDeleted("doc-001"); err != nil {
		t.Fatalf("MarkDeleted failed: %v", err)
	}

	// Verify document is marked as deleted
	if !storage.IsDeleted("doc-001") {
		t.Error("Document should be marked as deleted")
	}

	// Verify deletion stats
	deletedCount, totalCount, rate := storage.GetDeletionStats()
	if deletedCount != 1 {
		t.Errorf("Expected deletedCount=1, got %d", deletedCount)
	}
	if totalCount != 1 {
		t.Errorf("Expected totalCount=1, got %d", totalCount)
	}
	if rate != 1.0 {
		t.Errorf("Expected deletionRate=1.0, got %f", rate)
	}

	// Verify Stats includes deletion info
	stats := storage.Stats()
	if stats.DeletedCount != 1 {
		t.Errorf("Expected Stats.DeletedCount=1, got %d", stats.DeletedCount)
	}
	if stats.DeletionRate != 1.0 {
		t.Errorf("Expected Stats.DeletionRate=1.0, got %f", stats.DeletionRate)
	}
}

func TestDocumentStorageMarkDeletedNonExistent(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "dv_storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	storage, err := NewDocumentStorage(tmpDir, 128)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()

	// Try to delete non-existent document
	err = storage.MarkDeleted("non-existent")
	if err == nil {
		t.Error("MarkDeleted should return error for non-existent document")
	}
	if !IsNotFound(err) {
		t.Errorf("Expected ErrDocumentNotFound, got %v", err)
	}
}

func TestDocumentStorageDeletionPersistence(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "dv_storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create storage and add documents
	storage, err := NewDocumentStorage(tmpDir, 128)
	if err != nil {
		t.Fatal(err)
	}

	// Add multiple documents
	for i := 0; i < 5; i++ {
		doc := &Document{
			ID:     fmt.Sprintf("doc-%03d", i),
			Vector: make([]float32, 128),
		}
		if err := storage.Put(doc); err != nil {
			t.Fatal(err)
		}
	}

	if err := storage.Flush(); err != nil {
		t.Fatal(err)
	}

	// Mark some as deleted
	storage.MarkDeleted("doc-001")
	storage.MarkDeleted("doc-003")

	// Save and close
	if err := storage.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen storage
	storage2, err := NewDocumentStorage(tmpDir, 128)
	if err != nil {
		t.Fatal(err)
	}
	defer storage2.Close()

	// Verify deletions are persisted
	if !storage2.IsDeleted("doc-001") {
		t.Error("doc-001 should be marked as deleted after reopen")
	}
	if !storage2.IsDeleted("doc-003") {
		t.Error("doc-003 should be marked as deleted after reopen")
	}
	if storage2.IsDeleted("doc-000") {
		t.Error("doc-000 should not be marked as deleted")
	}
	if storage2.IsDeleted("doc-002") {
		t.Error("doc-002 should not be marked as deleted")
	}
}

func TestDocumentStorageGetAllValidDocuments(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "dv_storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	storage, err := NewDocumentStorage(tmpDir, 128)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()

	// Add documents
	docs := []*Document{
		{ID: "doc-001", Vector: make([]float32, 128), Metadata: map[string]interface{}{"name": "doc1"}},
		{ID: "doc-002", Vector: make([]float32, 128), Metadata: map[string]interface{}{"name": "doc2"}},
		{ID: "doc-003", Vector: make([]float32, 128), Metadata: map[string]interface{}{"name": "doc3"}},
		{ID: "doc-004", Vector: make([]float32, 128), Metadata: map[string]interface{}{"name": "doc4"}},
	}

	for _, doc := range docs {
		if err := storage.Put(doc); err != nil {
			t.Fatal(err)
		}
	}

	if err := storage.Flush(); err != nil {
		t.Fatal(err)
	}

	// Mark some as deleted
	storage.MarkDeleted("doc-001")
	storage.MarkDeleted("doc-003")

	// Get valid documents
	validDocs, err := storage.GetAllValidDocuments()
	if err != nil {
		t.Fatalf("GetAllValidDocuments failed: %v", err)
	}

	// Should have 2 valid documents
	if len(validDocs) != 2 {
		t.Errorf("Expected 2 valid documents, got %d", len(validDocs))
	}

	// Verify which documents are valid
	validIDs := make(map[string]bool)
	for _, doc := range validDocs {
		validIDs[doc.ID] = true
	}

	if !validIDs["doc-002"] {
		t.Error("doc-002 should be in valid documents")
	}
	if !validIDs["doc-004"] {
		t.Error("doc-004 should be in valid documents")
	}
	if validIDs["doc-001"] {
		t.Error("doc-001 should not be in valid documents")
	}
	if validIDs["doc-003"] {
		t.Error("doc-003 should not be in valid documents")
	}
}

func TestDocumentStorageClearDeletionVector(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "dv_storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	storage, err := NewDocumentStorage(tmpDir, 128)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()

	// Add and delete a document
	doc := &Document{ID: "doc-001", Vector: make([]float32, 128)}
	if err := storage.Put(doc); err != nil {
		t.Fatal(err)
	}
	if err := storage.Flush(); err != nil {
		t.Fatal(err)
	}

	storage.MarkDeleted("doc-001")
	if !storage.IsDeleted("doc-001") {
		t.Error("Document should be deleted before clear")
	}

	// Clear deletion vector
	storage.ClearDeletionVector()

	// Verify deletion is cleared
	if storage.IsDeleted("doc-001") {
		t.Error("Document should not be deleted after clear")
	}

	// Stats should reflect the clear
	stats := storage.Stats()
	if stats.DeletedCount != 0 {
		t.Errorf("Expected DeletedCount=0 after clear, got %d", stats.DeletedCount)
	}
}

func TestDocumentStorageEmptyDeletionVectorFile(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "dv_storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create storage with a document
	storage, err := NewDocumentStorage(tmpDir, 128)
	if err != nil {
		t.Fatal(err)
	}

	doc := &Document{ID: "doc-001", Vector: make([]float32, 128)}
	if err := storage.Put(doc); err != nil {
		t.Fatal(err)
	}
	if err := storage.Flush(); err != nil {
		t.Fatal(err)
	}
	storage.Close()

	// Verify no DV file exists (since no deletions)
	dvPath := hnsw.GetDeletionVectorPath(filepath.Join(tmpDir, dataFileName))
	if hnsw.FileExists(dvPath) {
		t.Error("DV file should not exist when there are no deletions")
	}

	// Reopen and verify
	storage2, err := NewDocumentStorage(tmpDir, 128)
	if err != nil {
		t.Fatal(err)
	}
	defer storage2.Close()

	if !storage2.deletionVector.IsEmpty() {
		t.Error("DV should be empty when no DV file exists")
	}
}

func TestDocumentStorageIsDeletedByRowID(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "dv_storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	storage, err := NewDocumentStorage(tmpDir, 128)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()

	// Add documents
	for i := 0; i < 3; i++ {
		doc := &Document{
			ID:     fmt.Sprintf("doc-%03d", i),
			Vector: make([]float32, 128),
		}
		if err := storage.Put(doc); err != nil {
			t.Fatal(err)
		}
	}
	if err := storage.Flush(); err != nil {
		t.Fatal(err)
	}

	// Mark doc-001 (rowID 1) as deleted
	storage.MarkDeleted("doc-001")

	// Test IsDeletedByRowID
	if !storage.IsDeletedByRowID(1) {
		t.Error("RowID 1 should be deleted")
	}
	if storage.IsDeletedByRowID(0) {
		t.Error("RowID 0 should not be deleted")
	}
	if storage.IsDeletedByRowID(2) {
		t.Error("RowID 2 should not be deleted")
	}
	if storage.IsDeletedByRowID(999) {
		t.Error("Non-existent RowID should not be deleted")
	}
}

func TestDocumentStorageDeletionWithBuffer(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "dv_storage_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	storage, err := NewDocumentStorage(tmpDir, 128)
	if err != nil {
		t.Fatal(err)
	}
	defer storage.Close()

	// Add document but don't flush (keep in buffer)
	doc := &Document{ID: "doc-001", Vector: make([]float32, 128)}
	if err := storage.Put(doc); err != nil {
		t.Fatal(err)
	}

	// MarkDeleted should work for buffered document (removes from buffer directly)
	if err := storage.MarkDeleted("doc-001"); err != nil {
		t.Fatalf("MarkDeleted should work for buffered document: %v", err)
	}

	// Document should be gone (not findable)
	_, err = storage.Get("doc-001")
	if err == nil {
		t.Error("Document should not be found after deletion from buffer")
	}

	// Add another document, flush, then delete
	doc2 := &Document{ID: "doc-002", Vector: make([]float32, 128)}
	if err := storage.Put(doc2); err != nil {
		t.Fatal(err)
	}

	// After flush, MarkDeleted should use DV
	if err := storage.Flush(); err != nil {
		t.Fatal(err)
	}

	if err := storage.MarkDeleted("doc-002"); err != nil {
		t.Fatalf("MarkDeleted should work after flush: %v", err)
	}

	// Verify doc-002 is marked as deleted via DV
	if !storage.IsDeleted("doc-002") {
		t.Error("doc-002 should be marked as deleted")
	}
}

// BenchmarkMarkDeleted benchmarks the MarkDeleted operation
func BenchmarkMarkDeleted(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "dv_bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	storage, err := NewDocumentStorage(tmpDir, 128)
	if err != nil {
		b.Fatal(err)
	}
	defer storage.Close()

	// Add documents
	for i := 0; i < 1000; i++ {
		doc := &Document{
			ID:     fmt.Sprintf("doc-%04d", i),
			Vector: make([]float32, 128),
		}
		storage.Put(doc)
	}
	storage.Flush()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		storage.MarkDeleted(fmt.Sprintf("doc-%04d", i%1000))
	}
}

// BenchmarkIsDeleted benchmarks the IsDeleted operation
func BenchmarkIsDeleted(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "dv_bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	storage, err := NewDocumentStorage(tmpDir, 128)
	if err != nil {
		b.Fatal(err)
	}
	defer storage.Close()

	// Add documents
	for i := 0; i < 1000; i++ {
		doc := &Document{
			ID:     fmt.Sprintf("doc-%04d", i),
			Vector: make([]float32, 128),
		}
		storage.Put(doc)
	}
	storage.Flush()

	// Mark half as deleted
	for i := 0; i < 500; i++ {
		storage.MarkDeleted(fmt.Sprintf("doc-%04d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		storage.IsDeleted(fmt.Sprintf("doc-%04d", i%1000))
	}
}
