package vego

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// ==================== P1: 持久化测试 ====================

// TestCollectionPersistence tests collection data persistence across reopening
func TestCollectionPersistence(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "vego_persist_p1_test")
	os.RemoveAll(tmpDir)

	config := &Config{
		Dimension:      64,
		M:              16,
		EfConstruction: 200,
	}

	// Phase 1: Create collection and insert data
	coll, err := NewCollection("test", tmpDir, config)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// Insert documents with various metadata
	docs := []*Document{
		{ID: "doc1", Vector: make([]float32, 64), Metadata: map[string]interface{}{"author": "Alice", "views": 100}},
		{ID: "doc2", Vector: make([]float32, 64), Metadata: map[string]interface{}{"author": "Bob", "views": 200}},
		{ID: "doc3", Vector: make([]float32, 64), Metadata: map[string]interface{}{"author": "Charlie", "views": 300}},
	}
	for _, doc := range docs {
		if err := coll.Insert(doc); err != nil {
			t.Fatalf("Failed to insert %s: %v", doc.ID, err)
		}
	}

	// Save and close
	if err := coll.Save(); err != nil {
		t.Fatalf("Failed to save: %v", err)
	}
	if err := coll.Close(); err != nil {
		t.Fatalf("Failed to close: %v", err)
	}

	// Phase 2: Reopen collection and verify data
	coll2, err := NewCollection("test", tmpDir, config)
	if err != nil {
		t.Fatalf("Failed to reopen collection: %v", err)
	}
	defer coll2.Close()
	defer os.RemoveAll(tmpDir)

	// Verify document count
	stats := coll2.Stats()
	if stats.Count != 3 {
		t.Errorf("Expected 3 documents after reopen, got %d", stats.Count)
	}

	// Verify all documents are retrievable with correct metadata
	for _, expected := range docs {
		retrieved, err := coll2.Get(expected.ID)
		if err != nil {
			t.Errorf("Failed to get %s after reopen: %v", expected.ID, err)
			continue
		}
		if retrieved.Metadata["author"] != expected.Metadata["author"] {
			t.Errorf("Expected author %v, got %v", expected.Metadata["author"], retrieved.Metadata["author"])
		}
	}

	// Verify search works on reloaded index
	query := make([]float32, 64)
	results, err := coll2.Search(query, 10)
	if err != nil {
		t.Errorf("Search failed after reopen: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("Expected 3 search results, got %d", len(results))
	}
}

// TestCollectionReload tests that mappings are correctly reloaded
func TestCollectionReload(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "vego_reload_test")
	os.RemoveAll(tmpDir)

	config := &Config{Dimension: 64}

	// Create and populate collection
	coll, _ := NewCollection("test", tmpDir, config)

	// Insert, update, delete to create complex state
	for i := 0; i < 10; i++ {
		doc := &Document{
			ID:     fmt.Sprintf("doc%d", i),
			Vector: make([]float32, 64),
			Metadata: map[string]interface{}{"index": i},
		}
		coll.Insert(doc)
	}

	// Update some documents
	for i := 0; i < 5; i++ {
		doc, _ := coll.Get(fmt.Sprintf("doc%d", i))
		doc.Metadata["updated"] = true
		coll.Update(doc)
	}

	// Delete some documents
	for i := 5; i < 8; i++ {
		coll.Delete(fmt.Sprintf("doc%d", i))
	}

	expectedCount := coll.Stats().Count
	coll.Save()
	coll.Close()

	// Reload
	coll2, err := NewCollection("test", tmpDir, config)
	if err != nil {
		t.Fatalf("Failed to reload: %v", err)
	}
	defer coll2.Close()
	defer os.RemoveAll(tmpDir)

	// Verify count matches
	if coll2.Stats().Count != expectedCount {
		t.Errorf("Expected %d documents after reload, got %d", expectedCount, coll2.Stats().Count)
	}

	// Verify updated documents
	for i := 0; i < 5; i++ {
		doc, err := coll2.Get(fmt.Sprintf("doc%d", i))
		if err != nil {
			t.Errorf("Failed to get updated doc%d: %v", i, err)
			continue
		}
		if doc.Metadata["updated"] != true {
			t.Errorf("Expected doc%d to have updated=true", i)
		}
	}

	// Verify deleted documents are gone
	for i := 5; i < 8; i++ {
		_, err := coll2.Get(fmt.Sprintf("doc%d", i))
		if err == nil {
			t.Errorf("Expected doc%d to be deleted", i)
		}
	}
}

// TestCollectionSaveConsistency tests that Save maintains data consistency
func TestCollectionSaveConsistency(t *testing.T) {
	coll, cleanup := setupTestCollection(t)
	defer cleanup()

	// Insert initial data
	for i := 0; i < 5; i++ {
		doc := createTestDocument(fmt.Sprintf("consistency_doc_%d", i), 64, map[string]interface{}{"batch": 1})
		coll.Insert(doc)
	}

	// Save
	if err := coll.Save(); err != nil {
		t.Fatalf("First save failed: %v", err)
	}

	// Add more data
	for i := 5; i < 10; i++ {
		doc := createTestDocument(fmt.Sprintf("consistency_doc_%d", i), 64, map[string]interface{}{"batch": 2})
		coll.Insert(doc)
	}

	// Save again
	if err := coll.Save(); err != nil {
		t.Fatalf("Second save failed: %v", err)
	}

	// Verify all data is intact
	stats := coll.Stats()
	if stats.Count != 10 {
		t.Errorf("Expected 10 documents, got %d", stats.Count)
	}

	// Verify search works
	query := make([]float32, 64)
	results, err := coll.Search(query, 10)
	if err != nil {
		t.Errorf("Search after save failed: %v", err)
	}
	if len(results) != 10 {
		t.Errorf("Expected 10 search results, got %d", len(results))
	}
}

// ==================== P1: 并发测试 ====================

// TestCollectionConcurrentInsert tests concurrent insertions
func TestCollectionConcurrentInsert(t *testing.T) {
	coll, cleanup := setupTestCollection(t)
	defer cleanup()

	const numGoroutines = 10
	const numDocsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	start := make(chan struct{}) // Signal to start simultaneously

	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()
			<-start // Wait for signal

			for j := 0; j < numDocsPerGoroutine; j++ {
				docID := fmt.Sprintf("goroutine_%d_doc_%d", goroutineID, j)
				doc := createTestDocument(docID, 64, map[string]interface{}{
					"goroutine": goroutineID,
					"index":     j,
				})

				if err := coll.Insert(doc); err != nil {
					t.Errorf("Insert failed for %s: %v", docID, err)
				}
			}
		}(i)
	}

	close(start) // Start all goroutines simultaneously
	wg.Wait()

	// Verify total count
	stats := coll.Stats()
	expected := numGoroutines * numDocsPerGoroutine
	if stats.Count != expected {
		t.Errorf("Expected %d documents, got %d", expected, stats.Count)
	}

	// Verify each goroutine's documents
	for i := 0; i < numGoroutines; i++ {
		for j := 0; j < numDocsPerGoroutine; j++ {
			docID := fmt.Sprintf("goroutine_%d_doc_%d", i, j)
			_, err := coll.Get(docID)
			if err != nil {
				t.Errorf("Failed to get %s: %v", docID, err)
			}
		}
	}
}

// TestCollectionConcurrentReadWrite tests concurrent reads and writes
func TestCollectionConcurrentReadWrite(t *testing.T) {
	coll, cleanup := setupTestCollection(t)
	defer cleanup()

	// Insert initial data
	for i := 0; i < 100; i++ {
		doc := createTestDocument(fmt.Sprintf("initial_%d", i), 64, nil)
		coll.Insert(doc)
	}

	const numReaders = 5
	const numWriters = 3
	const duration = 2 * time.Second

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Writers
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			counter := 0
			for {
				select {
				case <-stop:
					return
				default:
					docID := fmt.Sprintf("writer_%d_doc_%d", writerID, counter)
					doc := createTestDocument(docID, 64, map[string]interface{}{"writer": writerID})
					coll.Insert(doc)
					counter++
				}
			}
		}(i)
	}

	// Readers
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			query := make([]float32, 64)
			for {
				select {
				case <-stop:
					return
				default:
					// Perform search
					_, err := coll.Search(query, 10)
					if err != nil {
						t.Errorf("Reader %d search failed: %v", readerID, err)
					}

					// Random get
					_, _ = coll.Get(fmt.Sprintf("initial_%d", readerID%100))
				}
			}
		}(i)
	}

	// Let it run
	time.Sleep(duration)
	close(stop)
	wg.Wait()

	// Verify data integrity
	stats := coll.Stats()
	if stats.Count < 100 {
		t.Errorf("Expected at least 100 documents, got %d", stats.Count)
	}

	// Verify all initial documents are still there
	for i := 0; i < 100; i++ {
		_, err := coll.Get(fmt.Sprintf("initial_%d", i))
		if err != nil {
			t.Errorf("Initial doc %d missing after concurrent access: %v", i, err)
		}
	}
}

// TestCollectionConcurrentSearch tests concurrent searches
func TestCollectionConcurrentSearch(t *testing.T) {
	coll, cleanup := setupTestCollection(t)
	defer cleanup()

	// Insert test data
	for i := 0; i < 1000; i++ {
		doc := createTestDocument(fmt.Sprintf("search_doc_%d", i), 64, map[string]interface{}{"index": i})
		coll.Insert(doc)
	}

	const numGoroutines = 20
	const searchesPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()

			for j := 0; j < searchesPerGoroutine; j++ {
				// Vary query slightly per goroutine
				query := make([]float32, 64)
				query[0] = float32(goroutineID) * 0.01
				query[1] = float32(j) * 0.01

				results, err := coll.Search(query, 10)
				if err != nil {
					t.Errorf("Search failed in goroutine %d: %v", goroutineID, err)
					continue
				}

				if len(results) == 0 {
					t.Errorf("Search returned no results in goroutine %d", goroutineID)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify collection is still intact
	stats := coll.Stats()
	if stats.Count != 1000 {
		t.Errorf("Expected 1000 documents after concurrent searches, got %d", stats.Count)
	}
}

// TestCollectionRaceCondition runs race detector friendly tests
func TestCollectionRaceCondition(t *testing.T) {
	coll, cleanup := setupTestCollection(t)
	defer cleanup()

	// Insert initial data
	for i := 0; i < 50; i++ {
		doc := createTestDocument(fmt.Sprintf("race_doc_%d", i), 64, map[string]interface{}{"initial": true})
		coll.Insert(doc)
	}

	var wg sync.WaitGroup

	// Concurrent Insert
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			doc := createTestDocument(fmt.Sprintf("concurrent_insert_%d", i), 64, nil)
			coll.Insert(doc)
		}
	}()

	// Concurrent Get
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			_, _ = coll.Get(fmt.Sprintf("race_doc_%d", i%50))
		}
	}()

	// Concurrent Search
	wg.Add(1)
	go func() {
		defer wg.Done()
		query := make([]float32, 64)
		for i := 0; i < 20; i++ {
			_, _ = coll.Search(query, 10)
		}
	}()

	// Concurrent Stats
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 30; i++ {
			_ = coll.Stats()
		}
	}()

	// Concurrent Update
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			doc, err := coll.Get(fmt.Sprintf("race_doc_%d", i%50))
			if err == nil {
				doc.Metadata["updated"] = true
				coll.Update(doc)
			}
		}
	}()

	wg.Wait()

	// Final verification
	stats := coll.Stats()
	if stats.Count < 50 {
		t.Errorf("Expected at least 50 documents, got %d", stats.Count)
	}
}

// TestCollectionEmptyOperations tests operations on empty collection
func TestCollectionEmptyOperations(t *testing.T) {
	coll, cleanup := setupTestCollection(t)
	defer cleanup()

	// Search on empty collection - should return error
	query := make([]float32, 64)
	_, err := coll.Search(query, 10)
	if err == nil {
		t.Error("Expected error searching empty collection")
	}

	// Get non-existent
	_, err = coll.Get("non_existent")
	if err == nil {
		t.Error("Expected error getting non-existent document")
	}

	// Stats on empty
	stats := coll.Stats()
	if stats.Count != 0 {
		t.Errorf("Expected 0 count on empty collection, got %d", stats.Count)
	}

	// Delete on empty
	err = coll.Delete("non_existent")
	if err == nil {
		t.Error("Expected error deleting from empty collection")
	}
}

// TestCollectionLargeMetadata tests documents with large metadata
func TestCollectionLargeMetadata(t *testing.T) {
	coll, cleanup := setupTestCollection(t)
	defer cleanup()

	// Create document with large metadata
	largeMeta := make(map[string]interface{})
	for i := 0; i < 1000; i++ {
		largeMeta[fmt.Sprintf("key_%d", i)] = fmt.Sprintf("value_%d", i)
	}

	doc := &Document{
		ID:       "large_meta_doc",
		Vector:   make([]float32, 64),
		Metadata: largeMeta,
	}

	if err := coll.Insert(doc); err != nil {
		t.Fatalf("Failed to insert large metadata doc: %v", err)
	}

	// Retrieve and verify
	retrieved, err := coll.Get("large_meta_doc")
	if err != nil {
		t.Fatalf("Failed to get large metadata doc: %v", err)
	}

	if len(retrieved.Metadata) != 1000 {
		t.Errorf("Expected 1000 metadata keys, got %d", len(retrieved.Metadata))
	}

	// Verify a few random keys
	for _, i := range []int{0, 100, 500, 999} {
		key := fmt.Sprintf("key_%d", i)
		expectedVal := fmt.Sprintf("value_%d", i)
		if retrieved.Metadata[key] != expectedVal {
			t.Errorf("Metadata mismatch for %s", key)
		}
	}
}

// TestCollectionSpecialCharactersID tests IDs with special characters
func TestCollectionSpecialCharactersID(t *testing.T) {
	coll, cleanup := setupTestCollection(t)
	defer cleanup()

	specialIDs := []string{
		"with-dash",
		"with_underscore",
		"with.dot",
		"with:colon",
		"with/slash",
		"with space",
		"with\ttab",
		"with\nnewline",
		"mixed-_.:/ chars",
		"中文测试",
		"日本語テスト",
		"🎉emoji",
		"very/long/path/like/id",
		"a",
		"123",
	}

	for _, id := range specialIDs {
		doc := &Document{
			ID:       id,
			Vector:   make([]float32, 64),
			Metadata: map[string]interface{}{"original_id": id},
		}

		if err := coll.Insert(doc); err != nil {
			t.Errorf("Failed to insert doc with ID %q: %v", id, err)
			continue
		}

		// Retrieve and verify
		retrieved, err := coll.Get(id)
		if err != nil {
			t.Errorf("Failed to get doc with ID %q: %v", id, err)
			continue
		}

		if retrieved.ID != id {
			t.Errorf("ID mismatch: expected %q, got %q", id, retrieved.ID)
		}

		if retrieved.Metadata["original_id"] != id {
			t.Errorf("Metadata mismatch for ID %q", id)
		}
	}

	// Verify count
	stats := coll.Stats()
	if stats.Count != len(specialIDs) {
		t.Errorf("Expected %d documents, got %d", len(specialIDs), stats.Count)
	}
}

// TestCollectionMaxDimension tests with large dimensions
func TestCollectionMaxDimension(t *testing.T) {
	// Test various large dimensions
	dimensions := []int{256, 512, 768, 1024, 1536}

	for _, dim := range dimensions {
		t.Run(fmt.Sprintf("Dimension_%d", dim), func(t *testing.T) {
			tmpDir := filepath.Join(os.TempDir(), fmt.Sprintf("vego_dim_%d_test", dim))
			os.RemoveAll(tmpDir)

			config := &Config{
				Dimension:      dim,
				M:              16,
				EfConstruction: 200,
			}

			coll, err := NewCollection("test", tmpDir, config)
			if err != nil {
				t.Fatalf("Failed to create collection with dimension %d: %v", dim, err)
			}
			defer coll.Close()
			defer os.RemoveAll(tmpDir)

			// Insert documents
			for i := 0; i < 10; i++ {
				doc := &Document{
					ID:     fmt.Sprintf("doc_%d", i),
					Vector: make([]float32, dim),
				}
				// Fill with some pattern
				for j := range doc.Vector {
					doc.Vector[j] = float32(j%100) * 0.01
				}

				if err := coll.Insert(doc); err != nil {
					t.Fatalf("Failed to insert doc with dimension %d: %v", dim, err)
				}
			}

			// Search
			query := make([]float32, dim)
			results, err := coll.Search(query, 5)
			if err != nil {
				t.Errorf("Search failed with dimension %d: %v", dim, err)
			}

			if len(results) != 5 {
				t.Errorf("Expected 5 results with dimension %d, got %d", dim, len(results))
			}

			t.Logf("Successfully tested dimension %d", dim)
		})
	}
}
