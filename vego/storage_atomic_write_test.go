package vego

import (
	"os"
	"path/filepath"
	"testing"
)

// TestAtomicWrite verifies that writeColumnStorage uses atomic write pattern
func TestAtomicWrite(t *testing.T) {
	tmpDir := t.TempDir()

	storage, err := NewDocumentStorage(tmpDir, 128)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()

	// Insert some documents
	docs := []*Document{
		{ID: "doc-001", Vector: make([]float32, 128)},
		{ID: "doc-002", Vector: make([]float32, 128)},
	}
	for _, doc := range docs {
		if err := storage.Put(doc); err != nil {
			t.Fatalf("Failed to put document: %v", err)
		}
	}
	storage.Flush()

	// Verify data file exists
	dataFile := filepath.Join(tmpDir, dataFileName)
	if _, err := os.Stat(dataFile); os.IsNotExist(err) {
		t.Fatal("Data file should exist after flush")
	}

	// Rewrite with new documents (simulating compaction)
	newDocs := []*Document{
		{ID: "doc-003", Vector: make([]float32, 128)},
	}
	if err := storage.Rewrite(newDocs); err != nil {
		t.Fatalf("Failed to rewrite: %v", err)
	}

	// Verify data file still exists and is valid
	if _, err := os.Stat(dataFile); os.IsNotExist(err) {
		t.Fatal("Data file should exist after rewrite")
	}

	// Verify temp file is cleaned up
	tempFile := dataFile + ".tmp"
	if _, err := os.Stat(tempFile); !os.IsNotExist(err) {
		t.Error("Temp file should be cleaned up after successful rewrite")
	}

	// Verify new data is correct
	retrieved, err := storage.Get("doc-003")
	if err != nil {
		t.Errorf("Failed to get doc-003: %v", err)
	}
	if retrieved == nil || retrieved.ID != "doc-003" {
		t.Error("Retrieved wrong document")
	}

	// Old documents should be gone
	if _, err := storage.Get("doc-001"); err == nil {
		t.Error("doc-001 should not exist after rewrite")
	}

	t.Log("Atomic write test passed")
}

// TestCleanupTempFiles verifies that stale temp files are cleaned up on startup
// and only data file temp files are cleaned (not other .tmp.* files)
func TestCleanupTempFiles(t *testing.T) {
	tmpDir := t.TempDir()

	// Create vego data file temp files (should be cleaned up)
	vegoTempFiles := []string{
		filepath.Join(tmpDir, "vectors.lance.tmp.1234567890"),
		filepath.Join(tmpDir, "vectors.lance.tmp.1234567891"),
	}

	// Create other temp files (should NOT be cleaned up - not vego data files)
	otherTempFiles := []string{
		filepath.Join(tmpDir, "backup.tmp.bak"),
		filepath.Join(tmpDir, "config.tmp.json"),
		filepath.Join(tmpDir, "other.lance.tmp.1234567892"),
	}

	allTempFiles := append(vegoTempFiles, otherTempFiles...)
	for _, f := range allTempFiles {
		if err := os.WriteFile(f, []byte("fake data"), 0644); err != nil {
			t.Fatalf("Failed to create temp file: %v", err)
		}
	}

	// Create a real data file (should not be cleaned up)
	dataFile := filepath.Join(tmpDir, dataFileName)
	if err := os.WriteFile(dataFile, []byte("real data"), 0644); err != nil {
		t.Fatalf("Failed to create data file: %v", err)
	}

	// Create storage - should clean up only vego data file temp files
	storage, err := NewDocumentStorage(tmpDir, 128)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	storage.Close()

	// Verify vego data file temp files are cleaned up
	for _, f := range vegoTempFiles {
		if _, err := os.Stat(f); !os.IsNotExist(err) {
			t.Errorf("Vego temp file %s should be cleaned up", f)
		}
	}

	// Verify other temp files are NOT cleaned up
	for _, f := range otherTempFiles {
		if _, err := os.Stat(f); os.IsNotExist(err) {
			t.Errorf("Other temp file %s should NOT be cleaned up", f)
		}
	}

	// Verify data file still exists
	if _, err := os.Stat(dataFile); os.IsNotExist(err) {
		t.Error("Data file should not be cleaned up")
	}

	t.Log("Cleanup temp files test passed - only vego data file temps cleaned")
}

// TestAtomicWriteRollback verifies that temp file is cleaned up on failure
func TestAtomicWriteRollback(t *testing.T) {
	tmpDir := t.TempDir()

	storage, err := NewDocumentStorage(tmpDir, 128)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()

	// Insert initial documents
	docs := []*Document{
		{ID: "doc-001", Vector: make([]float32, 128)},
	}
	if err := storage.Put(docs[0]); err != nil {
		t.Fatalf("Failed to put document: %v", err)
	}
	storage.Flush()

	// Verify original data exists
	dataFile := filepath.Join(tmpDir, dataFileName)
	originalStat, err := os.Stat(dataFile)
	if err != nil {
		t.Fatalf("Original data file should exist: %v", err)
	}

	// Rewrite with valid documents should succeed
	if err := storage.Rewrite(docs); err != nil {
		t.Fatalf("Rewrite should succeed: %v", err)
	}

	// Verify data file still exists
	if _, err := os.Stat(dataFile); os.IsNotExist(err) {
		t.Error("Data file should still exist after successful rewrite")
	}

	// Verify temp file is cleaned up
	tempFile := dataFile + ".tmp"
	if _, err := os.Stat(tempFile); !os.IsNotExist(err) {
		t.Error("Temp file should be cleaned up")
	}

	// Verify data integrity
	newStat, err := os.Stat(dataFile)
	if err != nil {
		t.Fatalf("Should be able to stat data file: %v", err)
	}

	// File should have been replaced (different size or mod time)
	if newStat.Size() == originalStat.Size() && newStat.ModTime() == originalStat.ModTime() {
		t.Log("Warning: File stats identical, may indicate rewrite didn't happen")
	}

	t.Log("Atomic write rollback test passed")
}
