package vego

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/wzqhbustb/vego/storage/column"
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

// TestDocumentStorageRowIndexWrite verifies that V1.2 format writes RowIndex correctly
func TestDocumentStorageRowIndexWrite(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "vego-storage-rowindex-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create storage (defaults to V1.2)
	storage, err := NewDocumentStorage(filepath.Join(tmpDir, "storage"), 64)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	// Verify default version is V1.2
	if storage.version != format.V1_2 {
		t.Errorf("Expected default version V1_2, got %v", storage.version)
	}

	// Create test documents
	docs := []*Document{
		{ID: "doc1", Vector: makeTestVector(64, 1.0), Metadata: map[string]interface{}{"key": "value1"}},
		{ID: "doc2", Vector: makeTestVector(64, 2.0), Metadata: map[string]interface{}{"key": "value2"}},
		{ID: "doc3", Vector: makeTestVector(64, 3.0), Metadata: map[string]interface{}{"key": "value3"}},
	}

	// Put documents
	for _, doc := range docs {
		if err := storage.Put(doc); err != nil {
			t.Fatalf("Failed to put document %s: %v", doc.ID, err)
		}
	}

	// Flush to write data file with RowIndex
	if err := storage.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// Close storage
	if err := storage.Close(); err != nil {
		t.Fatalf("Failed to close storage: %v", err)
	}

	// Verify RowIndex was written correctly by checking file format
	dataFile := filepath.Join(tmpDir, "storage", "vectors.lance")
	reader, err := column.NewRowIndexReader(dataFile)
	if err != nil {
		t.Fatalf("Failed to open reader: %v", err)
	}
	defer reader.Close()

	// Verify Footer has RowIndex
	if !reader.HasRowIndex() {
		t.Error("Expected HasRowIndex() to be true for V1.2 file")
	}

	// Verify file version is V1.2
	version := reader.GetVersion()
	if version != format.V1_2 {
		t.Errorf("Expected version V1_2, got %v", version)
	}

	// Reopen storage and verify data can be read (RowIndex written correctly)
	storage2, err := NewDocumentStorage(filepath.Join(tmpDir, "storage"), 64)
	if err != nil {
		t.Fatalf("Failed to reopen storage: %v", err)
	}
	defer storage2.Close()

	// Verify all documents can be retrieved
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

	t.Logf("RowIndex write test passed - Footer.HasRowIndex=true, Version=V1.2, documents verified")
}

// TestDocumentStorageVersionConfiguration tests that version can be configured
func TestDocumentStorageVersionConfiguration(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "vego-storage-version-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Test default version is V1.2
	storage, err := NewDocumentStorage(filepath.Join(tmpDir, "storage-v12"), 64)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	if storage.version != format.V1_2 {
		t.Errorf("Expected default version V1_2, got %v", storage.version)
	}
	storage.Close()

	t.Logf("Version configuration test passed - default version is V1.2")
}

// TestPutBatchBufferDedup verifies that PutBatch removes existing entries from
// the writeBuffer before appending updates, preventing duplicate IDs.
func TestPutBatchBufferDedup(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vego-putbatch-dedup-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	storage, err := NewDocumentStorage(filepath.Join(tmpDir, "storage"), 64)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()

	// Case 1: PutBatch updates a document already in writeBuffer
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
	if storage.bufferSize != 1 {
		t.Fatalf("Expected bufferSize=1, got %d", storage.bufferSize)
	}

	doc1Updated := &Document{
		ID:     "doc1",
		Vector: makeTestVector(64, 2.0),
		Metadata: map[string]interface{}{
			"version": "v2",
		},
	}
	if err := storage.PutBatch([]*Document{doc1Updated}); err != nil {
		t.Fatalf("Failed to PutBatch update doc1: %v", err)
	}
	if storage.bufferSize != 1 {
		t.Errorf("Expected bufferSize=1 after update, got %d", storage.bufferSize)
	}

	// Verify only the latest version is in buffer
	var count int
	var version string
	for _, d := range storage.writeBuffer {
		if d.ID == "doc1" {
			count++
			version = d.Metadata["version"].(string)
		}
	}
	if count != 1 {
		t.Errorf("Expected 1 doc1 in buffer, got %d", count)
	}
	if version != "v2" {
		t.Errorf("Expected version v2 in buffer, got %s", version)
	}

	// Case 2: PutBatch with duplicate IDs inside the batch itself
	docA1 := &Document{
		ID:     "dupA",
		Vector: makeTestVector(64, 1.0),
		Metadata: map[string]interface{}{
			"version": "v1",
		},
	}
	docA2 := &Document{
		ID:     "dupA",
		Vector: makeTestVector(64, 2.0),
		Metadata: map[string]interface{}{
			"version": "v2",
		},
	}
	if err := storage.PutBatch([]*Document{docA1, docA2}); err != nil {
		t.Fatalf("Failed to PutBatch duplicate IDs: %v", err)
	}

	// After Case 1 buffer had 1 doc (doc1). Now we added 2 docs but they
	// should dedup against each other, so bufferSize should be 2 (doc1 + dupA_v2).
	if storage.bufferSize != 2 {
		t.Errorf("Expected bufferSize=2 after dedup batch, got %d", storage.bufferSize)
	}

	var dupCount int
	var dupVersion string
	for _, d := range storage.writeBuffer {
		if d.ID == "dupA" {
			dupCount++
			dupVersion = d.Metadata["version"].(string)
		}
	}
	if dupCount != 1 {
		t.Errorf("Expected 1 dupA in buffer, got %d", dupCount)
	}
	if dupVersion != "v2" {
		t.Errorf("Expected dupA version v2, got %s", dupVersion)
	}
}
