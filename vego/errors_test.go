package vego

import (
	"errors"
	"testing"
)

// TestSentinelErrors tests sentinel error variables
func TestSentinelErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"ErrDocumentNotFound", ErrDocumentNotFound, "document not found"},
		{"ErrDuplicateID", ErrDuplicateID, "document already exists"},
		{"ErrDimensionMismatch", ErrDimensionMismatch, "vector dimension mismatch"},
		{"ErrCollectionNotFound", ErrCollectionNotFound, "collection not found"},
		{"ErrCollectionClosed", ErrCollectionClosed, "collection is closed"},
		{"ErrInvalidFilter", ErrInvalidFilter, "invalid filter expression"},
		{"ErrIndexCorrupted", ErrIndexCorrupted, "index corrupted"},
		{"ErrStorageCorrupted", ErrStorageCorrupted, "storage corrupted"},
		{"ErrValidationFailed", ErrValidationFailed, "validation failed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.err == nil {
				t.Error("Expected non-nil error")
			}
			if tt.err.Error() != tt.want {
				t.Errorf("Expected %q, got %q", tt.want, tt.err.Error())
			}
		})
	}
}

// TestErrorStruct tests the Error struct
func TestErrorStruct(t *testing.T) {
	t.Run("Error with DocID", func(t *testing.T) {
		err := &Error{
			Op:    "Get",
			Coll:  "test_coll",
			DocID: "doc123",
			Err:   ErrDocumentNotFound,
		}

		want := "vego: Get on collection test_coll (doc doc123) failed: document not found"
		if err.Error() != want {
			t.Errorf("Expected %q, got %q", want, err.Error())
		}
	})

	t.Run("Error without DocID", func(t *testing.T) {
		err := &Error{
			Op:   "Search",
			Coll: "test_coll",
			Err:  ErrDimensionMismatch,
		}

		want := "vego: Search on collection test_coll failed: vector dimension mismatch"
		if err.Error() != want {
			t.Errorf("Expected %q, got %q", want, err.Error())
		}
	})

	t.Run("Error without Coll", func(t *testing.T) {
		err := &Error{
			Op:  "Validate",
			Err: ErrValidationFailed,
		}

		want := "vego: Validate failed: validation failed"
		if err.Error() != want {
			t.Errorf("Expected %q, got %q", want, err.Error())
		}
	})
}

// TestErrorUnwrap tests error unwrapping
func TestErrorUnwrap(t *testing.T) {
	inner := ErrDocumentNotFound
	err := &Error{
		Op:    "Get",
		Coll:  "test",
		DocID: "doc1",
		Err:   inner,
	}

	unwrapped := err.Unwrap()
	if unwrapped != inner {
		t.Error("Unwrap should return the underlying error")
	}

	// Test errors.Is
	if !errors.Is(err, ErrDocumentNotFound) {
		t.Error("errors.Is should work with wrapped errors")
	}
}

// TestIsNotFound tests IsNotFound helper
func TestIsNotFound(t *testing.T) {
	t.Run("Direct error", func(t *testing.T) {
		if !IsNotFound(ErrDocumentNotFound) {
			t.Error("IsNotFound should return true for ErrDocumentNotFound")
		}
	})

	t.Run("Wrapped error", func(t *testing.T) {
		err := &Error{
			Op:    "Get",
			Coll:  "test",
			DocID: "doc1",
			Err:   ErrDocumentNotFound,
		}
		if !IsNotFound(err) {
			t.Error("IsNotFound should work with wrapped errors")
		}
	})

	t.Run("Different error", func(t *testing.T) {
		if IsNotFound(ErrDuplicateID) {
			t.Error("IsNotFound should return false for other errors")
		}
	})

	t.Run("Nil error", func(t *testing.T) {
		if IsNotFound(nil) {
			t.Error("IsNotFound should return false for nil")
		}
	})
}

// TestIsDuplicate tests IsDuplicate helper
func TestIsDuplicate(t *testing.T) {
	t.Run("Direct error", func(t *testing.T) {
		if !IsDuplicate(ErrDuplicateID) {
			t.Error("IsDuplicate should return true for ErrDuplicateID")
		}
	})

	t.Run("Wrapped error", func(t *testing.T) {
		err := wrapError("Insert", "test", "doc1", ErrDuplicateID)
		if !IsDuplicate(err) {
			t.Error("IsDuplicate should work with wrapped errors")
		}
	})

	t.Run("Different error", func(t *testing.T) {
		if IsDuplicate(ErrDocumentNotFound) {
			t.Error("IsDuplicate should return false for other errors")
		}
	})
}

// TestIsDimensionMismatch tests IsDimensionMismatch helper
func TestIsDimensionMismatch(t *testing.T) {
	t.Run("Direct error", func(t *testing.T) {
		if !IsDimensionMismatch(ErrDimensionMismatch) {
			t.Error("IsDimensionMismatch should return true")
		}
	})

	t.Run("Wrapped error", func(t *testing.T) {
		err := wrapError("Search", "test", "", ErrDimensionMismatch)
		if !IsDimensionMismatch(err) {
			t.Error("IsDimensionMismatch should work with wrapped errors")
		}
	})
}

// TestIsCollectionClosed tests IsCollectionClosed helper
func TestIsCollectionClosed(t *testing.T) {
	t.Run("Direct error", func(t *testing.T) {
		if !IsCollectionClosed(ErrCollectionClosed) {
			t.Error("IsCollectionClosed should return true")
		}
	})

	t.Run("Different error", func(t *testing.T) {
		if IsCollectionClosed(ErrDocumentNotFound) {
			t.Error("IsCollectionClosed should return false for other errors")
		}
	})
}

// TestIsValidationFailed tests IsValidationFailed helper
func TestIsValidationFailed(t *testing.T) {
	t.Run("Direct error", func(t *testing.T) {
		if !IsValidationFailed(ErrValidationFailed) {
			t.Error("IsValidationFailed should return true")
		}
	})

	t.Run("Wrapped error", func(t *testing.T) {
		err := wrapError("Insert", "test", "doc1", ErrValidationFailed)
		if !IsValidationFailed(err) {
			t.Error("IsValidationFailed should work with wrapped errors")
		}
	})
}

// TestWrapError tests wrapError helper
func TestWrapError(t *testing.T) {
	t.Run("Wrap error", func(t *testing.T) {
		inner := ErrDocumentNotFound
		err := wrapError("Get", "coll1", "doc123", inner)

		vegoErr, ok := err.(*Error)
		if !ok {
			t.Fatal("Expected *Error type")
		}

		if vegoErr.Op != "Get" {
			t.Errorf("Expected Op=Get, got %s", vegoErr.Op)
		}
		if vegoErr.Coll != "coll1" {
			t.Errorf("Expected Coll=coll1, got %s", vegoErr.Coll)
		}
		if vegoErr.DocID != "doc123" {
			t.Errorf("Expected DocID=doc123, got %s", vegoErr.DocID)
		}
		if vegoErr.Err != inner {
			t.Error("Expected underlying error to be preserved")
		}
	})

	t.Run("Wrap nil returns nil", func(t *testing.T) {
		err := wrapError("Get", "test", "", nil)
		if err != nil {
			t.Error("wrapError with nil should return nil")
		}
	})
}

// TestErrorsIntegration tests error handling in actual operations
func TestErrorsIntegration(t *testing.T) {
	coll, cleanup := setupTestCollection(t)
	defer cleanup()

	// Insert a document
	doc := &Document{
		ID:     "error_test_doc",
		Vector: make([]float32, 64),
	}
	if err := coll.Insert(doc); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	t.Run("Duplicate insert returns ErrDuplicateID", func(t *testing.T) {
		err := coll.Insert(doc)
		if err == nil {
			t.Fatal("Expected error for duplicate insert")
		}
		if !IsDuplicate(err) {
			t.Errorf("Expected IsDuplicate to return true, got false. Error: %v", err)
		}
	})

	t.Run("Get non-existent returns ErrDocumentNotFound", func(t *testing.T) {
		_, err := coll.Get("non_existent_doc")
		if err == nil {
			t.Fatal("Expected error for non-existent doc")
		}
		if !IsNotFound(err) {
			t.Errorf("Expected IsNotFound to return true, got false. Error: %v", err)
		}
	})

	t.Run("Update non-existent returns ErrDocumentNotFound", func(t *testing.T) {
		doc := &Document{
			ID:     "non_existent",
			Vector: make([]float32, 64),
		}
		err := coll.Update(doc)
		if err == nil {
			t.Fatal("Expected error for update of non-existent doc")
		}
		if !IsNotFound(err) {
			t.Errorf("Expected IsNotFound to return true. Error: %v", err)
		}
	})

	t.Run("Delete non-existent returns ErrDocumentNotFound", func(t *testing.T) {
		err := coll.Delete("non_existent_doc")
		if err == nil {
			t.Fatal("Expected error for delete of non-existent doc")
		}
		if !IsNotFound(err) {
			t.Errorf("Expected IsNotFound to return true. Error: %v", err)
		}
	})

	t.Run("Search with wrong dimension returns ErrDimensionMismatch", func(t *testing.T) {
		query := make([]float32, 32) // Wrong dimension
		_, err := coll.Search(query, 10)
		if err == nil {
			t.Fatal("Expected error for wrong dimension")
		}
		if !IsDimensionMismatch(err) {
			t.Errorf("Expected IsDimensionMismatch to return true. Error: %v", err)
		}
	})
}
