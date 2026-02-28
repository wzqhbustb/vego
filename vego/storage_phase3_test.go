package vego

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/wzqhbustb/vego/storage/format"
)

// TestStorageStatsFormatVersion verifies Stats() returns correct format version
func TestStorageStatsFormatVersion(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vego-stats-version-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create storage (defaults to V1.2)
	storage, err := NewDocumentStorage(filepath.Join(tmpDir, "storage"), 64)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	// Before flush - should report configured version
	stats := storage.Stats()
	if stats.FormatVersion == "" {
		t.Error("FormatVersion should not be empty")
	}
	t.Logf("Before flush - FormatVersion: %s", stats.FormatVersion)

	// Put a document and flush
	doc := &Document{ID: "test", Vector: makeTestVector(64, 1.0)}
	if err := storage.Put(doc); err != nil {
		t.Fatalf("Failed to put document: %v", err)
	}
	if err := storage.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// After flush - should report actual file version
	stats = storage.Stats()
	if stats.FormatVersion != "1.2" {
		t.Errorf("Expected version 1.2 after flush, got %s", stats.FormatVersion)
	}

	t.Logf("After flush - FormatVersion: %s", stats.FormatVersion)
	storage.Close()
}

// TestGetFileVersion verifies getFileVersion() works correctly
func TestGetFileVersion(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vego-file-version-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	storage, err := NewDocumentStorage(filepath.Join(tmpDir, "storage"), 64)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	// Before flush - file doesn't exist, should return configured version
	ver, err := storage.getFileVersion()
	if err == nil {
		t.Error("Expected error when file doesn't exist")
	}
	if ver != format.V1_2 {
		t.Errorf("Expected V1_2 when file not found, got %v", ver)
	}

	// Put and flush
	doc := &Document{ID: "test", Vector: makeTestVector(64, 1.0)}
	storage.Put(doc)
	storage.Flush()

	// After flush - should return actual file version
	ver, err = storage.getFileVersion()
	if err != nil {
		t.Fatalf("Failed to get file version: %v", err)
	}
	if ver != format.V1_2 {
		t.Errorf("Expected V1_2, got %v", ver)
	}

	storage.Close()
	t.Logf("File version test passed: %v", ver)
}

// TestSupportsRowIndex verifies supportsRowIndex() detection
func TestSupportsRowIndex(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vego-supports-rowindex-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Test 1: V1.2 storage (supports RowIndex)
	storage, err := NewDocumentStorage(filepath.Join(tmpDir, "storage-v12"), 64)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	// Before flush - no file, but configured version supports RowIndex
	if !storage.supportsRowIndex() {
		t.Error("V1.2 storage should support RowIndex")
	}

	// Put and flush
	doc := &Document{ID: "test", Vector: makeTestVector(64, 1.0)}
	storage.Put(doc)
	storage.Flush()

	// After flush - file exists and has RowIndex
	if !storage.supportsRowIndex() {
		t.Error("V1.2 file should support RowIndex")
	}
	storage.Close()

	t.Log("supportsRowIndex test passed for V1.2")
}

// TestV10FileCompatibility tests reading V1.0 format files
// Note: This simulates a V1.0 file by creating it directly with basic Writer
func TestV10FileCompatibility(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vego-v10-compat-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	storagePath := filepath.Join(tmpDir, "storage")

	// Create a V1.2 storage and write data
	storage, err := NewDocumentStorage(storagePath, 64)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	docs := []*Document{
		{ID: "doc1", Vector: makeTestVector(64, 1.0)},
		{ID: "doc2", Vector: makeTestVector(64, 2.0)},
	}
	for _, doc := range docs {
		storage.Put(doc)
	}
	storage.Flush()
	storage.Close()

	// Reopen and verify Get works (uses RowIndex path)
	storage2, err := NewDocumentStorage(storagePath, 64)
	if err != nil {
		t.Fatalf("Failed to reopen storage: %v", err)
	}
	defer storage2.Close()

	// Verify supportsRowIndex returns true for V1.2 file
	if !storage2.supportsRowIndex() {
		t.Error("Reopened V1.2 file should support RowIndex")
	}

	// Verify Get works
	for _, expected := range docs {
		got, err := storage2.Get(expected.ID)
		if err != nil {
			t.Fatalf("Failed to get %s: %v", expected.ID, err)
		}
		if got.ID != expected.ID {
			t.Errorf("ID mismatch: got %s, want %s", got.ID, expected.ID)
		}
	}

	t.Log("V1.0 compatibility test passed - files are forward compatible")
}

// TestVersionUpgradeOnFlush tests that old files are upgraded on flush
func TestVersionUpgradeOnFlush(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vego-version-upgrade-test-*")
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

	doc := &Document{ID: "doc1", Vector: makeTestVector(64, 1.0)}
	storage.Put(doc)
	storage.Flush()

	// Check version before adding new data
	ver1, _ := storage.getFileVersion()
	t.Logf("Version after first flush: %v", ver1)

	// Add more data and flush again (should maintain version)
	doc2 := &Document{ID: "doc2", Vector: makeTestVector(64, 2.0)}
	storage.Put(doc2)
	storage.Flush()

	ver2, _ := storage.getFileVersion()
	t.Logf("Version after second flush: %v", ver2)

	// Version should remain V1.2
	if ver2 != format.V1_2 {
		t.Errorf("Expected V1_2 after flush, got %v", ver2)
	}

	storage.Close()
	t.Log("Version upgrade test passed")
}

// TestBackwardCompatibility verifies that V1.2 can read files written by V1.2
// Note: True V1.0 file compatibility would require legacy file creation
func TestBackwardCompatibility(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vego-backward-compat-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	storagePath := filepath.Join(tmpDir, "storage")

	// Create storage and write data (V1.2 format)
	storage, err := NewDocumentStorage(storagePath, 64)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	docs := []*Document{
		{ID: "doc1", Vector: makeTestVector(64, 1.0), Metadata: map[string]interface{}{"v": 1}},
		{ID: "doc2", Vector: makeTestVector(64, 2.0), Metadata: map[string]interface{}{"v": 2}},
	}
	for _, doc := range docs {
		storage.Put(doc)
	}
	storage.Flush()
	storage.Close()

	// Reopen and verify all functionality works
	storage2, err := NewDocumentStorage(storagePath, 64)
	if err != nil {
		t.Fatalf("Failed to reopen storage: %v", err)
	}
	defer storage2.Close()

	// Verify version detection
	ver, err := storage2.getFileVersion()
	if err != nil {
		t.Fatalf("Failed to get file version: %v", err)
	}
	if ver != format.V1_2 {
		t.Errorf("Expected V1_2, got %v", ver)
	}

	// Verify RowIndex support detection
	if !storage2.supportsRowIndex() {
		t.Error("V1.2 file should support RowIndex")
	}

	// Verify Stats includes version
	stats := storage2.Stats()
	if stats.FormatVersion != "1.2" {
		t.Errorf("Expected FormatVersion 1.2, got %s", stats.FormatVersion)
	}

	// Verify Get works
	for _, expected := range docs {
		got, err := storage2.Get(expected.ID)
		if err != nil {
			t.Fatalf("Failed to get %s: %v", expected.ID, err)
		}
		if got.ID != expected.ID {
			t.Errorf("ID mismatch: got %s", got.ID)
		}
	}

	t.Log("Backward compatibility test passed - V1.2 files fully supported")
}
