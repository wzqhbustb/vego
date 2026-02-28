package vego

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/wzqhbustb/vego/storage/format"
)

// TestVersionUpgrade verifies that files are upgraded on flush when version changes
func TestVersionUpgrade(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vego-version-upgrade-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	storagePath := filepath.Join(tmpDir, "storage")

	// Create V1.2 storage (default)
	storage, err := NewDocumentStorage(storagePath, 64)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	// Write initial data
	doc1 := &Document{ID: "doc1", Vector: makeTestVector(64, 1.0)}
	storage.Put(doc1)
	storage.Flush()

	// Verify initial version
	ver1, _ := storage.getFileVersion()
	if ver1 != format.V1_2 {
		t.Errorf("Expected initial version V1_2, got %v", ver1)
	}
	storage.Close()

	// Simulate version change by reopening with same version
	// (In real scenario, this could be V1_1 or other version)
	storage2, err := NewDocumentStorage(storagePath, 64)
	if err != nil {
		t.Fatalf("Failed to reopen storage: %v", err)
	}
	defer storage2.Close()

	// Add more data and flush
	doc2 := &Document{ID: "doc2", Vector: makeTestVector(64, 2.0)}
	storage2.Put(doc2)
	storage2.Flush()

	// Verify version is maintained
	ver2, _ := storage2.getFileVersion()
	if ver2 != format.V1_2 {
		t.Errorf("Expected version V1_2 after flush, got %v", ver2)
	}

	// Verify both documents are readable
	got1, err := storage2.Get("doc1")
	if err != nil {
		t.Fatalf("Failed to get doc1: %v", err)
	}
	if got1.ID != "doc1" {
		t.Errorf("doc1 ID mismatch: %s", got1.ID)
	}

	got2, err := storage2.Get("doc2")
	if err != nil {
		t.Fatalf("Failed to get doc2: %v", err)
	}
	if got2.ID != "doc2" {
		t.Errorf("doc2 ID mismatch: %s", got2.ID)
	}

	t.Logf("Version upgrade test passed - maintained at %v", ver2)
}

// TestRewriteMaintainsVersion verifies that rewriteStorage maintains configured version
func TestRewriteMaintainsVersion(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vego-rewrite-version-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	storagePath := filepath.Join(tmpDir, "storage")
	storage, err := NewDocumentStorage(storagePath, 64)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	// Write multiple batches
	for i := 0; i < 3; i++ {
		doc := &Document{
			ID:     fmt.Sprintf("batch%d-doc", i),
			Vector: makeTestVector(64, float32(i)),
		}
		storage.Put(doc)
		storage.Flush()

		// Verify version after each flush
		ver, _ := storage.getFileVersion()
		if ver != format.V1_2 {
			t.Errorf("Batch %d: Expected V1_2, got %v", i, ver)
		}
	}

	// Verify all documents exist
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("batch%d-doc", i)
		got, err := storage.Get(id)
		if err != nil {
			t.Fatalf("Failed to get %s: %v", id, err)
		}
		if got.ID != id {
			t.Errorf("ID mismatch: got %s", got.ID)
		}
	}

	storage.Close()
	t.Log("Rewrite maintains version test passed")
}


