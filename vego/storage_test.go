package vego

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/wzqhbustb/vego/storage/format"
)

// TestDocumentStorageWithoutCache tests storage works without BlockCache
func TestDocumentStorageWithoutCache(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "vego-storage-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create storage without cache
	storage, err := NewDocumentStorage(filepath.Join(tmpDir, "storage"), 128)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()

	// Verify blockCache is nil
	if storage.blockCache != nil {
		t.Error("Expected blockCache to be nil when not provided")
	}

	// Create test documents
	docs := []*Document{
		{
			ID:     "doc1",
			Vector: makeTestVector(128, 1.0),
			Metadata: map[string]interface{}{
				"title": "Document 1",
			},
		},
		{
			ID:     "doc2",
			Vector: makeTestVector(128, 2.0),
			Metadata: map[string]interface{}{
				"title": "Document 2",
			},
		},
	}

	// Put documents
	for _, doc := range docs {
		if err := storage.Put(doc); err != nil {
			t.Fatalf("Failed to put document %s: %v", doc.ID, err)
		}
	}

	// Flush to ensure data is written
	if err := storage.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// Get documents back
	for _, expected := range docs {
		got, err := storage.Get(expected.ID)
		if err != nil {
			t.Fatalf("Failed to get document %s: %v", expected.ID, err)
		}

		if got.ID != expected.ID {
			t.Errorf("ID mismatch: got %s, want %s", got.ID, expected.ID)
		}

		if len(got.Vector) != len(expected.Vector) {
			t.Errorf("Vector length mismatch: got %d, want %d", len(got.Vector), len(expected.Vector))
		}

		if got.Metadata["title"] != expected.Metadata["title"] {
			t.Errorf("Metadata mismatch: got %v, want %v", got.Metadata["title"], expected.Metadata["title"])
		}
	}

	// Verify stats
	stats := storage.Stats()
	if stats.DocumentCount != 2 {
		t.Errorf("Expected 2 documents, got %d", stats.DocumentCount)
	}
}

// TestDocumentStorageWithCache tests storage works with BlockCache
func TestDocumentStorageWithCache(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "vego-storage-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create BlockCache
	cache := format.NewBlockCache(64*1024*1024) // 64 MB
	if cache == nil {
		t.Fatal("Failed to create BlockCache")
	}

	// Create storage with cache
	storage, err := NewDocumentStorage(filepath.Join(tmpDir, "storage"), 128, cache)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()

	// Verify blockCache is set
	if storage.blockCache == nil {
		t.Error("Expected blockCache to be set when provided")
	}

	if storage.blockCache != cache {
		t.Error("Expected blockCache to be the same instance as provided")
	}

	// Create test documents
	docs := []*Document{
		{
			ID:     "doc1",
			Vector: makeTestVector(128, 1.0),
			Metadata: map[string]interface{}{
				"title": "Document 1",
			},
		},
		{
			ID:     "doc2",
			Vector: makeTestVector(128, 2.0),
			Metadata: map[string]interface{}{
				"title": "Document 2",
			},
		},
	}

	// Put documents
	for _, doc := range docs {
		if err := storage.Put(doc); err != nil {
			t.Fatalf("Failed to put document %s: %v", doc.ID, err)
		}
	}

	// Flush to ensure data is written
	if err := storage.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// Get documents back (should use cache on second read)
	for _, expected := range docs {
		got, err := storage.Get(expected.ID)
		if err != nil {
			t.Fatalf("Failed to get document %s: %v", expected.ID, err)
		}

		if got.ID != expected.ID {
			t.Errorf("ID mismatch: got %s, want %s", got.ID, expected.ID)
		}

		if len(got.Vector) != len(expected.Vector) {
			t.Errorf("Vector length mismatch: got %d, want %d", len(got.Vector), len(expected.Vector))
		}
	}

	// Verify stats
	stats := storage.Stats()
	if stats.DocumentCount != 2 {
		t.Errorf("Expected 2 documents, got %d", stats.DocumentCount)
	}
}

// TestDocumentStorageWithNilCache tests that nil cache is handled correctly
func TestDocumentStorageWithNilCache(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "vego-storage-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create storage with nil cache (should behave like no cache)
	storage, err := NewDocumentStorage(filepath.Join(tmpDir, "storage"), 128, nil)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()

	// Verify blockCache is nil
	if storage.blockCache != nil {
		t.Error("Expected blockCache to be nil when nil is provided")
	}

	// Basic functionality test
	doc := &Document{
		ID:     "test-doc",
		Vector: makeTestVector(128, 1.0),
	}

	if err := storage.Put(doc); err != nil {
		t.Fatalf("Failed to put document: %v", err)
	}

	if err := storage.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	got, err := storage.Get("test-doc")
	if err != nil {
		t.Fatalf("Failed to get document: %v", err)
	}

	if got.ID != "test-doc" {
		t.Errorf("ID mismatch: got %s, want test-doc", got.ID)
	}
}

// TestDocumentStorageSharedCache tests that multiple storages can share the same cache
func TestDocumentStorageSharedCache(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "vego-storage-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create shared BlockCache
	sharedCache := format.NewBlockCache(64 * 1024 * 1024)

	// Create multiple storages with the same cache
	storage1, err := NewDocumentStorage(filepath.Join(tmpDir, "storage1"), 64, sharedCache)
	if err != nil {
		t.Fatalf("Failed to create storage1: %v", err)
	}
	defer storage1.Close()

	storage2, err := NewDocumentStorage(filepath.Join(tmpDir, "storage2"), 64, sharedCache)
	if err != nil {
		t.Fatalf("Failed to create storage2: %v", err)
	}
	defer storage2.Close()

	// Verify both storages use the same cache instance
	if storage1.blockCache != sharedCache {
		t.Error("storage1 should use the shared cache")
	}

	if storage2.blockCache != sharedCache {
		t.Error("storage2 should use the shared cache")
	}

	if storage1.blockCache != storage2.blockCache {
		t.Error("Both storages should share the same cache instance")
	}

	// Add documents to both storages
	doc1 := &Document{
		ID:     "doc1",
		Vector: makeTestVector(64, 1.0),
	}
	doc2 := &Document{
		ID:     "doc2",
		Vector: makeTestVector(64, 2.0),
	}

	if err := storage1.Put(doc1); err != nil {
		t.Fatalf("Failed to put doc1: %v", err)
	}
	if err := storage2.Put(doc2); err != nil {
		t.Fatalf("Failed to put doc2: %v", err)
	}

	if err := storage1.Flush(); err != nil {
		t.Fatalf("Failed to flush storage1: %v", err)
	}
	if err := storage2.Flush(); err != nil {
		t.Fatalf("Failed to flush storage2: %v", err)
	}

	// Verify both documents can be retrieved
	got1, err := storage1.Get("doc1")
	if err != nil {
		t.Fatalf("Failed to get doc1: %v", err)
	}
	if got1.ID != "doc1" {
		t.Errorf("Expected doc1, got %s", got1.ID)
	}

	got2, err := storage2.Get("doc2")
	if err != nil {
		t.Fatalf("Failed to get doc2: %v", err)
	}
	if got2.ID != "doc2" {
		t.Errorf("Expected doc2, got %s", got2.ID)
	}
}

// makeTestVector creates a test vector with given dimension and fill value
func makeTestVector(dimension int, fillValue float32) []float32 {
	vec := make([]float32, dimension)
	for i := range vec {
		vec[i] = fillValue
	}
	return vec
}

// TestDocumentStorageCacheInvalidation tests that cache is invalidated after flush
func TestDocumentStorageCacheInvalidation(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "vego-storage-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create BlockCache
	cache := format.NewBlockCache(64 * 1024 * 1024)

	// Create storage with cache
	storage, err := NewDocumentStorage(filepath.Join(tmpDir, "storage"), 64, cache)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()

	// Step 1: Write initial document
	doc1 := &Document{
		ID:     "doc1",
		Vector: makeTestVector(64, 1.0),
		Metadata: map[string]interface{}{
			"version": "v1",
		},
	}
	if err := storage.Put(doc1); err != nil {
		t.Fatalf("Failed to put doc1: %v", err)
	}
	if err := storage.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// Step 2: Read document (this populates the cache)
	got, err := storage.Get("doc1")
	if err != nil {
		t.Fatalf("Failed to get doc1: %v", err)
	}
	if got.Metadata["version"] != "v1" {
		t.Errorf("Expected version v1, got %v", got.Metadata["version"])
	}

	// Step 3: Update the document
	doc1Updated := &Document{
		ID:     "doc1",
		Vector: makeTestVector(64, 2.0),
		Metadata: map[string]interface{}{
			"version": "v2",
		},
	}
	if err := storage.Put(doc1Updated); err != nil {
		t.Fatalf("Failed to update doc1: %v", err)
	}
	if err := storage.Flush(); err != nil {
		t.Fatalf("Failed to flush after update: %v", err)
	}

	// Step 4: Read updated document (should get fresh data, not stale cache)
	got, err = storage.Get("doc1")
	if err != nil {
		t.Fatalf("Failed to get updated doc1: %v", err)
	}

	// Verify we got the updated version
	if got.Metadata["version"] != "v2" {
		t.Errorf("Expected version v2 after update, got %v", got.Metadata["version"])
	}

	// Verify vector was also updated
	for i, v := range got.Vector {
		if v != 2.0 {
			t.Errorf("Vector[%d] = %f, expected 2.0", i, v)
			break
		}
	}
}

// TestDocumentStorageCacheInvalidationOnDelete tests cache invalidation after delete
func TestDocumentStorageCacheInvalidationOnDelete(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "vego-storage-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create BlockCache
	cache := format.NewBlockCache(64 * 1024 * 1024)

	// Create storage with cache
	storage, err := NewDocumentStorage(filepath.Join(tmpDir, "storage"), 64, cache)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()

	// Write two documents
	docs := []*Document{
		{ID: "doc1", Vector: makeTestVector(64, 1.0)},
		{ID: "doc2", Vector: makeTestVector(64, 2.0)},
	}
	for _, doc := range docs {
		if err := storage.Put(doc); err != nil {
			t.Fatalf("Failed to put %s: %v", doc.ID, err)
		}
	}
	if err := storage.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// Read to populate cache
	_, err = storage.Get("doc1")
	if err != nil {
		t.Fatalf("Failed to get doc1: %v", err)
	}

	// Delete one document
	if err := storage.Delete("doc1"); err != nil {
		t.Fatalf("Failed to delete doc1: %v", err)
	}

	// Verify doc1 is deleted
	_, err = storage.Get("doc1")
	if err != ErrDocumentNotFound {
		t.Errorf("Expected ErrDocumentNotFound, got %v", err)
	}

	// Verify doc2 still exists
	got, err := storage.Get("doc2")
	if err != nil {
		t.Fatalf("Failed to get doc2: %v", err)
	}
	if got.ID != "doc2" {
		t.Errorf("Expected doc2, got %s", got.ID)
	}
}
