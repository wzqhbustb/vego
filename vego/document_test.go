package vego

import (
	"testing"
)

// TestDocumentID tests ID generation
func TestDocumentID(t *testing.T) {
	t.Run("Generate unique IDs", func(t *testing.T) {
		id1 := DocumentID()
		id2 := DocumentID()
		
		if id1 == "" {
			t.Error("Expected non-empty ID")
		}
		if id2 == "" {
			t.Error("Expected non-empty ID")
		}
		if id1 == id2 {
			t.Error("Expected unique IDs")
		}
	})
	
	t.Run("ID format", func(t *testing.T) {
		id := DocumentID()
		// UUID v4 format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx (36 chars)
		if len(id) != 36 {
			t.Errorf("Expected ID length 36, got %d", len(id))
		}
	})
}

// TestDocumentValidate tests document validation
func TestDocumentValidate(t *testing.T) {
	t.Run("Valid document", func(t *testing.T) {
		doc := &Document{
			ID:     "test",
			Vector: make([]float32, 128),
		}
		
		err := doc.Validate(128)
		if err != nil {
			t.Errorf("Expected valid document, got error: %v", err)
		}
	})
	
	t.Run("Empty ID", func(t *testing.T) {
		doc := &Document{
			ID:     "",
			Vector: make([]float32, 128),
		}
		
		err := doc.Validate(128)
		if err == nil {
			t.Error("Expected error for empty ID")
		}
	})
	
	t.Run("Wrong dimension", func(t *testing.T) {
		doc := &Document{
			ID:     "test",
			Vector: make([]float32, 64), // Wrong dimension
		}
		
		err := doc.Validate(128)
		if err == nil {
			t.Error("Expected error for wrong dimension")
		}
	})
	
	t.Run("Nil vector", func(t *testing.T) {
		doc := &Document{
			ID:     "test",
			Vector: nil,
		}
		
		err := doc.Validate(128)
		if err == nil {
			t.Error("Expected error for nil vector")
		}
	})
	
	t.Run("Empty vector", func(t *testing.T) {
		doc := &Document{
			ID:     "test",
			Vector: []float32{},
		}
		
		err := doc.Validate(128)
		if err == nil {
			t.Error("Expected error for empty vector")
		}
	})
}

// TestDocumentValidateEmptyID tests empty ID validation
func TestDocumentValidateEmptyID(t *testing.T) {
	doc := &Document{
		ID:     "",
		Vector: make([]float32, 64),
	}
	
	err := doc.Validate(64)
	if err == nil {
		t.Error("Expected validation error for empty ID")
	}
}

// TestDocumentValidateWrongDimension tests wrong dimension validation
func TestDocumentValidateWrongDimension(t *testing.T) {
	testCases := []struct {
		name      string
		vectorLen int
		dimension int
		wantErr   bool
	}{
		{"Exact match", 128, 128, false},
		{"Too short", 64, 128, true},
		{"Too long", 256, 128, true},
		{"Zero dimension", 0, 0, true},
	}
	
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			doc := &Document{
				ID:     "test",
				Vector: make([]float32, tc.vectorLen),
			}
			
			err := doc.Validate(tc.dimension)
			if tc.wantErr && err == nil {
				t.Errorf("Expected error for %s", tc.name)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}

// TestDocumentValidateNilVector tests nil vector validation
func TestDocumentValidateNilVector(t *testing.T) {
	doc := &Document{
		ID:     "test",
		Vector: nil,
	}
	
	err := doc.Validate(64)
	if err == nil {
		t.Error("Expected validation error for nil vector")
	}
}

// TestDocumentClone tests document cloning
func TestDocumentClone(t *testing.T) {
	t.Run("Basic clone", func(t *testing.T) {
		original := &Document{
			ID:     "test",
			Vector: []float32{1.0, 2.0, 3.0, 4.0},
			Metadata: map[string]interface{}{
				"key1": "value1",
				"key2": 42,
			},
		}
		
		clone := original.Clone()
		
		// Verify values are copied
		if clone.ID != original.ID {
			t.Error("Clone ID mismatch")
		}
		if len(clone.Vector) != len(original.Vector) {
			t.Error("Clone vector length mismatch")
		}
		if len(clone.Metadata) != len(original.Metadata) {
			t.Error("Clone metadata length mismatch")
		}
	})
	
	t.Run("Deep copy vector", func(t *testing.T) {
		original := &Document{
			ID:     "test",
			Vector: []float32{1.0, 2.0, 3.0},
		}
		
		clone := original.Clone()
		
		// Modify original vector
		original.Vector[0] = 999.0
		
		// Clone should not be affected
		if clone.Vector[0] != 1.0 {
			t.Error("Clone vector was affected by original modification")
		}
	})
	
	t.Run("Deep copy metadata", func(t *testing.T) {
		original := &Document{
			ID:     "test",
			Vector: make([]float32, 3),
			Metadata: map[string]interface{}{
				"key": "value",
			},
		}
		
		clone := original.Clone()
		
		// Modify original metadata
		original.Metadata["key"] = "modified"
		original.Metadata["new"] = "added"
		
		// Clone should not be affected
		if clone.Metadata["key"] != "value" {
			t.Error("Clone metadata was affected by original modification")
		}
		if _, exists := clone.Metadata["new"]; exists {
			t.Error("Clone metadata has new key that was added after clone")
		}
	})
}

// TestDocumentCloneDeepCopy tests deep copy behavior
func TestDocumentCloneDeepCopy(t *testing.T) {
	original := &Document{
		ID:     "deep_test",
		Vector: []float32{0.1, 0.2, 0.3, 0.4, 0.5},
		Metadata: map[string]interface{}{
			"nested": map[string]interface{}{
				"inner": "value",
			},
			"slice": []int{1, 2, 3},
		},
	}
	
	clone := original.Clone()
	
	// Test vector independence
	original.Vector[0] = 999.0
	if clone.Vector[0] != 0.1 {
		t.Error("Vector not deeply copied")
	}
	
	// Test metadata independence at top level
	original.Metadata["new_key"] = "new_value"
	if _, exists := clone.Metadata["new_key"]; exists {
		t.Error("Metadata not deeply copied")
	}
}

// TestDocumentWithNilMetadata tests document with nil metadata
func TestDocumentWithNilMetadata(t *testing.T) {
	doc := &Document{
		ID:       "test",
		Vector:   make([]float32, 64),
		Metadata: nil,
	}
	
	// Validation should pass
	err := doc.Validate(64)
	if err != nil {
		t.Errorf("Validation failed: %v", err)
	}
	
	// Clone should handle nil metadata
	clone := doc.Clone()
	if clone.Metadata != nil {
		t.Error("Clone should have nil metadata for nil original")
	}
}

// TestDocumentWithEmptyMetadata tests document with empty metadata
func TestDocumentWithEmptyMetadata(t *testing.T) {
	doc := &Document{
		ID:       "test",
		Vector:   make([]float32, 64),
		Metadata: map[string]interface{}{},
	}
	
	err := doc.Validate(64)
	if err != nil {
		t.Errorf("Validation failed: %v", err)
	}
	
	clone := doc.Clone()
	if len(clone.Metadata) != 0 {
		t.Error("Clone should have empty metadata")
	}
}

// TestDocumentValidationEdgeCases tests edge cases
func TestDocumentValidationEdgeCases(t *testing.T) {
	t.Run("Large dimension", func(t *testing.T) {
		doc := &Document{
			ID:     "large",
			Vector: make([]float32, 1536), // OpenAI embedding dimension
		}
		
		err := doc.Validate(1536)
		if err != nil {
			t.Errorf("Large dimension validation failed: %v", err)
		}
	})
	
	t.Run("Single dimension", func(t *testing.T) {
		doc := &Document{
			ID:     "single",
			Vector: []float32{1.0},
		}
		
		err := doc.Validate(1)
		if err != nil {
			t.Errorf("Single dimension validation failed: %v", err)
		}
	})
	
	t.Run("Special characters in ID", func(t *testing.T) {
		doc := &Document{
			ID:     "special-_.:/ chars",
			Vector: make([]float32, 64),
		}
		
		err := doc.Validate(64)
		if err != nil {
			t.Errorf("Special characters ID validation failed: %v", err)
		}
	})
	
	t.Run("Unicode ID", func(t *testing.T) {
		doc := &Document{
			ID:     "中文测试文档",
			Vector: make([]float32, 64),
		}
		
		err := doc.Validate(64)
		if err != nil {
			t.Errorf("Unicode ID validation failed: %v", err)
		}
	})
}
