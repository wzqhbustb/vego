package vego

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	hnsw "github.com/wzqhbustb/vego/index"
)

// setupTestDB creates a test database with cleanup
func setupTestDB(t *testing.T, opts ...Option) (*DB, func()) {
	t.Helper()
	tmpDir := filepath.Join(os.TempDir(), "vego_db_test_"+time.Now().Format("20060102150405"))
	
	db, err := Open(tmpDir, opts...)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	
	cleanup := func() {
		db.Close()
		os.RemoveAll(tmpDir)
	}
	
	return db, cleanup
}

// TestOpen tests opening/creating a database
func TestOpen(t *testing.T) {
	t.Run("Create new database", func(t *testing.T) {
		tmpDir := filepath.Join(os.TempDir(), "vego_open_test")
		os.RemoveAll(tmpDir)
		
		db, err := Open(tmpDir)
		if err != nil {
			t.Errorf("Open failed: %v", err)
		}
		if db == nil {
			t.Error("Expected non-nil DB")
			return
		}
		
		// Verify directory was created
		if _, err := os.Stat(tmpDir); os.IsNotExist(err) {
			t.Error("Database directory was not created")
		}
		
		db.Close()
		os.RemoveAll(tmpDir)
	})
	
	t.Run("Open existing database", func(t *testing.T) {
		tmpDir := filepath.Join(os.TempDir(), "vego_open_existing")
		os.RemoveAll(tmpDir)
		
		// Create first
		db1, _ := Open(tmpDir)
		db1.Close()
		
		// Reopen
		db2, err := Open(tmpDir)
		if err != nil {
			t.Errorf("Open existing failed: %v", err)
		}
		db2.Close()
		os.RemoveAll(tmpDir)
	})
}

// TestOpenWithOptions tests opening with options
func TestOpenWithOptions(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "vego_options_test")
	os.RemoveAll(tmpDir)
	
	db, err := Open(tmpDir,
		WithDimension(256),
		WithM(32),
		WithEfConstruction(300),
		WithAdaptive(false),
		WithExpectedSize(50000),
		WithDistanceFunc(hnsw.CosineDistance),
	)
	if err != nil {
		t.Fatalf("Open with options failed: %v", err)
	}
	defer db.Close()
	defer os.RemoveAll(tmpDir)
	
	// Verify config was applied
	if db.config.Dimension != 256 {
		t.Errorf("Expected Dimension 256, got %d", db.config.Dimension)
	}
	if db.config.M != 32 {
		t.Errorf("Expected M 32, got %d", db.config.M)
	}
	if db.config.EfConstruction != 300 {
		t.Errorf("Expected EfConstruction 300, got %d", db.config.EfConstruction)
	}
	if db.config.Adaptive != false {
		t.Errorf("Expected Adaptive false, got %v", db.config.Adaptive)
	}
	if db.config.ExpectedSize != 50000 {
		t.Errorf("Expected ExpectedSize 50000, got %d", db.config.ExpectedSize)
	}
	// Verify distance function is set by testing behavior
	vec1 := []float32{1, 0}
	vec2 := []float32{0, 1}
	result := db.config.DistanceFunc(vec1, vec2)
	// Cosine distance between perpendicular vectors should be ~1
	if result < 0.9 || result > 1.1 {
		t.Error("Expected CosineDistance behavior")
	}
}

// TestDBClose tests closing the database
func TestDBClose(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()
	
	// Create a collection
	coll, _ := db.Collection("test")
	coll.Insert(&Document{ID: "doc1", Vector: make([]float32, 128)})
	
	// Close
	if err := db.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}
	
	// Verify closed flag
	if !db.closed {
		t.Error("Expected DB to be marked as closed")
	}
}

// TestDBCollection tests getting/creating collections
func TestDBCollection(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()
	
	t.Run("Create new collection", func(t *testing.T) {
		coll, err := db.Collection("test1")
		if err != nil {
			t.Errorf("Collection failed: %v", err)
		}
		if coll == nil {
			t.Error("Expected non-nil collection")
		}
		if coll.name != "test1" {
			t.Errorf("Expected name test1, got %s", coll.name)
		}
	})
	
	t.Run("Get existing collection", func(t *testing.T) {
		// Create first
		db.Collection("test2")
		
		// Get again
		coll, err := db.Collection("test2")
		if err != nil {
			t.Errorf("Get existing collection failed: %v", err)
		}
		if coll.name != "test2" {
			t.Errorf("Expected name test2, got %s", coll.name)
		}
	})
	
	t.Run("Multiple collections", func(t *testing.T) {
		db.Collection("coll1")
		db.Collection("coll2")
		db.Collection("coll3")
		
		names := db.Collections()
		if len(names) < 3 {
			t.Errorf("Expected at least 3 collections, got %d", len(names))
		}
	})
}

// TestDBDropCollection tests dropping collections
func TestDBDropCollection(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()
	
	// Create collection
	db.Collection("to_drop")
	
	// Drop it
	if err := db.DropCollection("to_drop"); err != nil {
		t.Errorf("DropCollection failed: %v", err)
	}
	
	// Verify it's gone
	names := db.Collections()
	for _, name := range names {
		if name == "to_drop" {
			t.Error("Collection should have been dropped")
		}
	}
	
	// Drop non-existent should error
	if err := db.DropCollection("non_existent"); err == nil {
		t.Error("Expected error for non-existent collection")
	}
}

// TestDBCollections tests listing collections
func TestDBCollections(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()
	
	// Initially empty (no collections yet)
	names := db.Collections()
	initialCount := len(names)
	
	// Create some collections
	db.Collection("coll_a")
	db.Collection("coll_b")
	
	names = db.Collections()
	if len(names) != initialCount+2 {
		t.Errorf("Expected %d collections, got %d", initialCount+2, len(names))
	}
	
	// Check names are present
	foundA, foundB := false, false
	for _, name := range names {
		if name == "coll_a" {
			foundA = true
		}
		if name == "coll_b" {
			foundB = true
		}
	}
	if !foundA {
		t.Error("Expected coll_a in list")
	}
	if !foundB {
		t.Error("Expected coll_b in list")
	}
}

// TestDBPersistence tests database persistence
func TestDBPersistence(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "vego_persist_test")
	os.RemoveAll(tmpDir)
	
	// Create database and add data
	db1, _ := Open(tmpDir, WithDimension(64))
	coll1, _ := db1.Collection("my_collection")
	
	docs := []*Document{
		{ID: "doc1", Vector: make([]float32, 64), Metadata: map[string]interface{}{"key": "value1"}},
		{ID: "doc2", Vector: make([]float32, 64), Metadata: map[string]interface{}{"key": "value2"}},
		{ID: "doc3", Vector: make([]float32, 64), Metadata: map[string]interface{}{"key": "value3"}},
	}
	for _, doc := range docs {
		coll1.Insert(doc)
	}
	
	// Save and close
	coll1.Save()
	db1.Close()
	
	// Reopen database
	db2, err := Open(tmpDir, WithDimension(64))
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer db2.Close()
	defer os.RemoveAll(tmpDir)
	
	// Get collection and verify data
	coll2, _ := db2.Collection("my_collection")
	
	// Verify collection still has the documents
	stats := coll2.Stats()
	if stats.Count != 3 {
		t.Errorf("Expected 3 documents after reopen, got %d", stats.Count)
	}
	
	// Verify we can get the documents
	for _, id := range []string{"doc1", "doc2", "doc3"} {
		_, err := coll2.Get(id)
		if err != nil {
			t.Errorf("Failed to get %s after reopen: %v", id, err)
		}
	}
}

// TestDBMultipleCollections tests multiple collections in one DB
func TestDBMultipleCollections(t *testing.T) {
	db, cleanup := setupTestDB(t, WithDimension(64))
	defer cleanup()
	
	// Create multiple collections
	coll1, _ := db.Collection("users")
	coll2, _ := db.Collection("products")
	coll3, _ := db.Collection("orders")
	
	// Insert into each
	coll1.Insert(&Document{ID: "user1", Vector: make([]float32, 64)})
	coll2.Insert(&Document{ID: "product1", Vector: make([]float32, 64)})
	coll3.Insert(&Document{ID: "order1", Vector: make([]float32, 64)})
	
	// Verify each collection has its own data
	if stats := coll1.Stats(); stats.Count != 1 {
		t.Errorf("Expected coll1 to have 1 doc, got %d", stats.Count)
	}
	if stats := coll2.Stats(); stats.Count != 1 {
		t.Errorf("Expected coll2 to have 1 doc, got %d", stats.Count)
	}
	if stats := coll3.Stats(); stats.Count != 1 {
		t.Errorf("Expected coll3 to have 1 doc, got %d", stats.Count)
	}
	
	// Verify documents are isolated
	_, err := coll1.Get("product1") // Should not exist in users
	if err == nil {
		t.Error("Expected error getting product1 from users collection")
	}
}

// TestOpenInvalidPath tests opening with invalid path
func TestOpenInvalidPath(t *testing.T) {
	t.Run("Invalid path", func(t *testing.T) {
		// Try to create in a non-existent parent directory that can't be created
		// This test may be platform-specific
		_, err := Open("/invalid_path_that_does_not_exist/db")
		// On Unix systems, this should fail with permission denied
		// But we can't guarantee this test works on all platforms
		if err != nil {
			t.Logf("Got expected error for invalid path: %v", err)
		}
	})
}

// TestDBCollectionClosed tests operations on closed database
func TestDBCollectionClosed(t *testing.T) {
	db, _ := setupTestDB(t)
	db.Close()
	
	// Operations on closed DB should not panic
	// Some operations might return errors, which is acceptable
	_, err := db.Collection("test")
	// The behavior depends on implementation, we just ensure it doesn't panic
	t.Logf("Collection on closed DB returned: %v", err)
}
