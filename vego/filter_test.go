package vego

import (
	"os"
	"path/filepath"
	"testing"
)

// setupFilterTest creates a test collection with sample data for filter testing
func setupFilterTest(t *testing.T) (*Collection, func()) {
	t.Helper()
	tmpDir := filepath.Join(os.TempDir(), "vego_filter_test_"+t.Name())
	os.RemoveAll(tmpDir)
	
	db, err := Open(tmpDir, WithDimension(64))
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	
	coll, err := db.Collection("test")
	if err != nil {
		t.Fatalf("Failed to get collection: %v", err)
	}
	
	// Insert test documents
	docs := []*Document{
		{
			ID:     "doc1",
			Vector: make([]float32, 64),
			Metadata: map[string]interface{}{
				"author": "Alice",
				"type":   "article",
				"views":  100,
				"rating": 4.5,
				"tags":   []string{"tech", "go"},
			},
		},
		{
			ID:     "doc2",
			Vector: make([]float32, 64),
			Metadata: map[string]interface{}{
				"author": "Bob",
				"type":   "article",
				"views":  200,
				"rating": 3.5,
				"tags":   []string{"tech", "python"},
			},
		},
		{
			ID:     "doc3",
			Vector: make([]float32, 64),
			Metadata: map[string]interface{}{
				"author": "Alice",
				"type":   "blog",
				"views":  50,
				"rating": 5.0,
				"tags":   []string{"life"},
			},
		},
		{
			ID:     "doc4",
			Vector: make([]float32, 64),
			Metadata: map[string]interface{}{
				"author": "Charlie",
				"type":   "video",
				"views":  500,
				"rating": 2.0,
			},
		},
	}
	
	for _, doc := range docs {
		if err := coll.Insert(doc); err != nil {
			t.Fatalf("Failed to insert %s: %v", doc.ID, err)
		}
	}
	
	cleanup := func() {
		db.Close()
		os.RemoveAll(tmpDir)
	}
	
	return coll, cleanup
}

// TestMetadataFilter tests the basic filter functionality
func TestMetadataFilter(t *testing.T) {
	coll, cleanup := setupFilterTest(t)
	defer cleanup()
	
	query := make([]float32, 64)
	
	// Test filter
	filter := &MetadataFilter{
		Field:    "author",
		Operator: "eq",
		Value:    "Alice",
	}
	
	filteredResults, err := coll.SearchWithFilter(query, 10, filter)
	if err != nil {
		t.Fatal(err)
	}
	
	if len(filteredResults) != 2 {
		t.Errorf("Expected 2 filtered results, got %d", len(filteredResults))
	}
	
	// Verify all results have author="Alice"
	for _, r := range filteredResults {
		if r.Document.Metadata["author"] != "Alice" {
			t.Errorf("Expected author=Alice, got %v", r.Document.Metadata["author"])
		}
	}
}

// TestMetadataFilterEq tests equal operator
func TestMetadataFilterEq(t *testing.T) {
	coll, cleanup := setupFilterTest(t)
	defer cleanup()
	
	query := make([]float32, 64)
	
	filter := &MetadataFilter{
		Field:    "type",
		Operator: "eq",
		Value:    "article",
	}
	
	results, err := coll.SearchWithFilter(query, 10, filter)
	if err != nil {
		t.Fatal(err)
	}
	
	if len(results) != 2 {
		t.Errorf("Expected 2 articles, got %d", len(results))
	}
	
	for _, r := range results {
		if r.Document.Metadata["type"] != "article" {
			t.Errorf("Expected type=article, got %v", r.Document.Metadata["type"])
		}
	}
}

// TestMetadataFilterNe tests not equal operator
func TestMetadataFilterNe(t *testing.T) {
	coll, cleanup := setupFilterTest(t)
	defer cleanup()
	
	query := make([]float32, 64)
	
	filter := &MetadataFilter{
		Field:    "author",
		Operator: "ne",
		Value:    "Alice",
	}
	
	results, err := coll.SearchWithFilter(query, 10, filter)
	if err != nil {
		t.Fatal(err)
	}
	
	if len(results) != 2 {
		t.Errorf("Expected 2 non-Alice results, got %d", len(results))
	}
	
	for _, r := range results {
		if r.Document.Metadata["author"] == "Alice" {
			t.Error("Should not have Alice in results")
		}
	}
}

// TestMetadataFilterGt tests greater than operator
func TestMetadataFilterGt(t *testing.T) {
	coll, cleanup := setupFilterTest(t)
	defer cleanup()
	
	query := make([]float32, 64)
	
	filter := &MetadataFilter{
		Field:    "views",
		Operator: "gt",
		Value:    100,
	}
	
	results, err := coll.SearchWithFilter(query, 10, filter)
	if err != nil {
		t.Fatal(err)
	}
	
	if len(results) != 2 {
		t.Errorf("Expected 2 results with views > 100, got %d", len(results))
	}
	
	for _, r := range results {
		views := r.Document.Metadata["views"].(int)
		if views <= 100 {
			t.Errorf("Expected views > 100, got %d", views)
		}
	}
}

// TestMetadataFilterGte tests greater than or equal operator
func TestMetadataFilterGte(t *testing.T) {
	coll, cleanup := setupFilterTest(t)
	defer cleanup()
	
	query := make([]float32, 64)
	
	filter := &MetadataFilter{
		Field:    "views",
		Operator: "gte",
		Value:    100,
	}
	
	results, err := coll.SearchWithFilter(query, 10, filter)
	if err != nil {
		t.Fatal(err)
	}
	
	if len(results) != 3 {
		t.Errorf("Expected 3 results with views >= 100, got %d", len(results))
	}
}

// TestMetadataFilterLt tests less than operator
func TestMetadataFilterLt(t *testing.T) {
	coll, cleanup := setupFilterTest(t)
	defer cleanup()
	
	query := make([]float32, 64)
	
	filter := &MetadataFilter{
		Field:    "views",
		Operator: "lt",
		Value:    100,
	}
	
	results, err := coll.SearchWithFilter(query, 10, filter)
	if err != nil {
		t.Fatal(err)
	}
	
	if len(results) != 1 {
		t.Errorf("Expected 1 result with views < 100, got %d", len(results))
	}
	
	if results[0].Document.ID != "doc3" {
		t.Errorf("Expected doc3, got %s", results[0].Document.ID)
	}
}

// TestMetadataFilterLte tests less than or equal operator
func TestMetadataFilterLte(t *testing.T) {
	coll, cleanup := setupFilterTest(t)
	defer cleanup()
	
	query := make([]float32, 64)
	
	filter := &MetadataFilter{
		Field:    "views",
		Operator: "lte",
		Value:    100,
	}
	
	results, err := coll.SearchWithFilter(query, 10, filter)
	if err != nil {
		t.Fatal(err)
	}
	
	if len(results) != 2 {
		t.Errorf("Expected 2 results with views <= 100, got %d", len(results))
	}
}

// TestMetadataFilterContains tests contains operator
func TestMetadataFilterContains(t *testing.T) {
	coll, cleanup := setupFilterTest(t)
	defer cleanup()
	
	query := make([]float32, 64)
	
	filter := &MetadataFilter{
		Field:    "author",
		Operator: "contains",
		Value:    "Ali",
	}
	
	results, err := coll.SearchWithFilter(query, 10, filter)
	if err != nil {
		t.Fatal(err)
	}
	
	if len(results) != 2 {
		t.Errorf("Expected 2 results containing 'Ali', got %d", len(results))
	}
	
	for _, r := range results {
		author := r.Document.Metadata["author"].(string)
		if !containsSubstring(author, "Ali") {
			t.Errorf("Expected author containing 'Ali', got %s", author)
		}
	}
}

// TestAndFilter tests AND filter combination
func TestAndFilter(t *testing.T) {
	coll, cleanup := setupFilterTest(t)
	defer cleanup()
	
	query := make([]float32, 64)
	
	filter := &AndFilter{
		Filters: []Filter{
			&MetadataFilter{
				Field:    "author",
				Operator: "eq",
				Value:    "Alice",
			},
			&MetadataFilter{
				Field:    "type",
				Operator: "eq",
				Value:    "article",
			},
		},
	}
	
	results, err := coll.SearchWithFilter(query, 10, filter)
	if err != nil {
		t.Fatal(err)
	}
	
	if len(results) != 1 {
		t.Errorf("Expected 1 result matching both conditions, got %d", len(results))
	}
	
	if results[0].Document.ID != "doc1" {
		t.Errorf("Expected doc1, got %s", results[0].Document.ID)
	}
}

// TestOrFilter tests OR filter combination
func TestOrFilter(t *testing.T) {
	coll, cleanup := setupFilterTest(t)
	defer cleanup()
	
	query := make([]float32, 64)
	
	filter := &OrFilter{
		Filters: []Filter{
			&MetadataFilter{
				Field:    "author",
				Operator: "eq",
				Value:    "Bob",
			},
			&MetadataFilter{
				Field:    "type",
				Operator: "eq",
				Value:    "video",
			},
		},
	}
	
	results, err := coll.SearchWithFilter(query, 10, filter)
	if err != nil {
		t.Fatal(err)
	}
	
	if len(results) != 2 {
		t.Errorf("Expected 2 results matching either condition, got %d", len(results))
	}
}

// TestNestedFilter tests nested filter combinations
func TestNestedFilter(t *testing.T) {
	coll, cleanup := setupFilterTest(t)
	defer cleanup()
	
	query := make([]float32, 64)
	
	// (author = "Alice" OR author = "Bob") AND type = "article"
	filter := &AndFilter{
		Filters: []Filter{
			&OrFilter{
				Filters: []Filter{
					&MetadataFilter{
						Field:    "author",
						Operator: "eq",
						Value:    "Alice",
					},
					&MetadataFilter{
						Field:    "author",
						Operator: "eq",
						Value:    "Bob",
					},
				},
			},
			&MetadataFilter{
				Field:    "type",
				Operator: "eq",
				Value:    "article",
			},
		},
	}
	
	results, err := coll.SearchWithFilter(query, 10, filter)
	if err != nil {
		t.Fatal(err)
	}
	
	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}
	
	for _, r := range results {
		author := r.Document.Metadata["author"].(string)
		if author != "Alice" && author != "Bob" {
			t.Errorf("Expected Alice or Bob, got %s", author)
		}
	}
}

// TestFilterMissingField tests filter on missing field
func TestFilterMissingField(t *testing.T) {
	coll, cleanup := setupFilterTest(t)
	defer cleanup()
	
	query := make([]float32, 64)
	
	filter := &MetadataFilter{
		Field:    "non_existent_field",
		Operator: "eq",
		Value:    "value",
	}
	
	results, err := coll.SearchWithFilter(query, 10, filter)
	if err != nil {
		t.Fatal(err)
	}
	
	if len(results) != 0 {
		t.Errorf("Expected 0 results for non-existent field, got %d", len(results))
	}
}

// TestFilterTypeMismatch tests filter with type mismatch
func TestFilterTypeMismatch(t *testing.T) {
	coll, cleanup := setupFilterTest(t)
	defer cleanup()
	
	query := make([]float32, 64)
	
	// Try to compare string field with int value
	filter := &MetadataFilter{
		Field:    "author",
		Operator: "gt",
		Value:    100,
	}
	
	results, err := coll.SearchWithFilter(query, 10, filter)
	if err != nil {
		t.Fatal(err)
	}
	
	// Should return 0 results due to type mismatch
	if len(results) != 0 {
		t.Errorf("Expected 0 results for type mismatch, got %d", len(results))
	}
}

// TestFilterNilMetadata tests filter on document with nil metadata
func TestFilterNilMetadata(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "vego_filter_nil_test")
	os.RemoveAll(tmpDir)
	
	db, _ := Open(tmpDir, WithDimension(64))
	defer db.Close()
	defer os.RemoveAll(tmpDir)
	
	coll, _ := db.Collection("test")
	
	// Insert document with nil metadata
	doc := &Document{
		ID:       "nil_meta",
		Vector:   make([]float32, 64),
		Metadata: nil,
	}
	coll.Insert(doc)
	
	// Also insert normal document
	doc2 := &Document{
		ID:     "normal",
		Vector: make([]float32, 64),
		Metadata: map[string]interface{}{
			"author": "Alice",
		},
	}
	coll.Insert(doc2)
	
	query := make([]float32, 64)
	filter := &MetadataFilter{
		Field:    "author",
		Operator: "eq",
		Value:    "Alice",
	}
	
	results, err := coll.SearchWithFilter(query, 10, filter)
	if err != nil {
		t.Fatal(err)
	}
	
	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}
	
	if results[0].Document.ID != "normal" {
		t.Errorf("Expected normal doc, got %s", results[0].Document.ID)
	}
}

// TestFilterEmptyMetadata tests filter on document with empty metadata
func TestFilterEmptyMetadata(t *testing.T) {
	tmpDir := filepath.Join(os.TempDir(), "vego_filter_empty_test")
	os.RemoveAll(tmpDir)
	
	db, _ := Open(tmpDir, WithDimension(64))
	defer db.Close()
	defer os.RemoveAll(tmpDir)
	
	coll, _ := db.Collection("test")
	
	// Insert document with empty metadata
	doc := &Document{
		ID:       "empty_meta",
		Vector:   make([]float32, 64),
		Metadata: map[string]interface{}{},
	}
	coll.Insert(doc)
	
	query := make([]float32, 64)
	filter := &MetadataFilter{
		Field:    "author",
		Operator: "eq",
		Value:    "Alice",
	}
	
	results, err := coll.SearchWithFilter(query, 10, filter)
	if err != nil {
		t.Fatal(err)
	}
	
	if len(results) != 0 {
		t.Errorf("Expected 0 results for empty metadata, got %d", len(results))
	}
}

// Helper function
func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
