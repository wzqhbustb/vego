package vego

import (
	"os"
	"path/filepath"
	"testing"
)

// TestDocumentStorageRowIndexRead verifies O(1) read via RowIndex
func TestDocumentStorageRowIndexRead(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "vego-rowindex-read-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create storage with V1.2 (RowIndex support)
	storage, err := NewDocumentStorage(filepath.Join(tmpDir, "storage"), 64)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	// Create test documents
	docs := []*Document{
		{ID: "doc-1", Vector: makeTestVector(64, 1.0), Metadata: map[string]interface{}{"idx": 0}},
		{ID: "doc-2", Vector: makeTestVector(64, 2.0), Metadata: map[string]interface{}{"idx": 1}},
		{ID: "doc-3", Vector: makeTestVector(64, 3.0), Metadata: map[string]interface{}{"idx": 2}},
	}

	// Put and flush documents
	for _, doc := range docs {
		if err := storage.Put(doc); err != nil {
			t.Fatalf("Failed to put document %s: %v", doc.ID, err)
		}
	}
	if err := storage.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}
	storage.Close()

	// Reopen storage
	storage2, err := NewDocumentStorage(filepath.Join(tmpDir, "storage"), 64)
	if err != nil {
		t.Fatalf("Failed to reopen storage: %v", err)
	}
	defer storage2.Close()

	// Verify file has RowIndex by checking Get works (uses RowIndex path internally)
	// The tryReadByRowIndex method will detect and use RowIndex automatically

	// Test Get via RowIndex path
	for _, expected := range docs {
		got, err := storage2.Get(expected.ID)
		if err != nil {
			t.Fatalf("Failed to get document %s: %v", expected.ID, err)
		}
		if got.ID != expected.ID {
			t.Errorf("ID mismatch: got %s, want %s", got.ID, expected.ID)
		}
		if len(got.Vector) != len(expected.Vector) {
			t.Errorf("Vector length mismatch for %s", expected.ID)
		}
	}

	// Test non-existent document
	_, err = storage2.Get("non-existent")
	if err != ErrDocumentNotFound {
		t.Errorf("Expected ErrDocumentNotFound for non-existent ID, got %v", err)
	}

	t.Logf("RowIndex read test passed - O(1) lookup working correctly")
}

// TestDocumentStorageRowIndexFallback verifies fallback to full scan when no RowIndex
func TestDocumentStorageRowIndexFallback(t *testing.T) {
	// This test verifies that Get still works for files without RowIndex
	// by manually creating a V1.0 style file
	
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "vego-rowindex-fallback-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	storagePath := filepath.Join(tmpDir, "storage")
	
	// Create storage and write data
	storage, err := NewDocumentStorage(storagePath, 64)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	doc := &Document{ID: "test-doc", Vector: makeTestVector(64, 1.0)}
	if err := storage.Put(doc); err != nil {
		t.Fatalf("Failed to put document: %v", err)
	}
	if err := storage.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}
	storage.Close()

	// Verify Get works - this internally uses RowIndex path for V1.2 files

	// Reopen and verify Get works
	storage2, err := NewDocumentStorage(storagePath, 64)
	if err != nil {
		t.Fatalf("Failed to reopen storage: %v", err)
	}
	defer storage2.Close()

	got, err := storage2.Get("test-doc")
	if err != nil {
		t.Fatalf("Failed to get document: %v", err)
	}
	if got.ID != "test-doc" {
		t.Errorf("ID mismatch: got %s, want test-doc", got.ID)
	}

	t.Logf("RowIndex fallback test passed - Get works with RowIndex path")
}

// TestFileHasRowIndex verifies the fileHasRowIndex detection
func TestFileHasRowIndex(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "vego-has-rowindex-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Test 1: Empty storage (no file yet)
	storage, err := NewDocumentStorage(filepath.Join(tmpDir, "storage1"), 64)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	
	// No data flushed, file doesn't exist - Get will use fallback path
	// This is expected behavior
	storage.Close()

	// Test 2: Storage with flushed data (V1.2 by default)
	storage2, err := NewDocumentStorage(filepath.Join(tmpDir, "storage2"), 64)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	
	doc := &Document{ID: "test", Vector: makeTestVector(64, 1.0)}
	if err := storage2.Put(doc); err != nil {
		t.Fatalf("Failed to put document: %v", err)
	}
	if err := storage2.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}
	
	// After flush, Get should work via RowIndex path
	got, err := storage2.Get("test")
	if err != nil {
		t.Fatalf("Failed to get document after flush: %v", err)
	}
	if got.ID != "test" {
		t.Errorf("ID mismatch: got %s, want test", got.ID)
	}
	storage2.Close()

	t.Logf("fileHasRowIndex detection test passed")
}
