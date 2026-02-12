package vego

import (
	"errors"
	"fmt"
)

// Sentinel errors for common cases
var (
	// ErrDocumentNotFound is returned when a document does not exist
	ErrDocumentNotFound = errors.New("document not found")

	// ErrDuplicateID is returned when inserting a document with an existing ID
	ErrDuplicateID = errors.New("document already exists")

	// ErrDimensionMismatch is returned when vector dimension doesn't match collection
	ErrDimensionMismatch = errors.New("vector dimension mismatch")

	// ErrCollectionNotFound is returned when a collection does not exist
	ErrCollectionNotFound = errors.New("collection not found")

	// ErrCollectionClosed is returned when operating on a closed collection
	ErrCollectionClosed = errors.New("collection is closed")

	// ErrInvalidFilter is returned when filter expression is invalid
	ErrInvalidFilter = errors.New("invalid filter expression")

	// ErrIndexCorrupted is returned when index data is corrupted
	ErrIndexCorrupted = errors.New("index corrupted")

	// ErrStorageCorrupted is returned when storage data is corrupted
	ErrStorageCorrupted = errors.New("storage corrupted")

	// ErrValidationFailed is returned when document validation fails
	ErrValidationFailed = errors.New("validation failed")
)

// Error provides structured error information
type Error struct {
	Op    string // Operation: "Get", "Insert", "Search", etc.
	Coll  string // Collection name (if applicable)
	DocID string // Document ID (if applicable)
	Err   error  // Underlying error
}

// Error returns a formatted error string
func (e *Error) Error() string {
	if e.DocID != "" {
		return fmt.Sprintf("vego: %s on collection %s (doc %s) failed: %v", e.Op, e.Coll, e.DocID, e.Err)
	}
	if e.Coll != "" {
		return fmt.Sprintf("vego: %s on collection %s failed: %v", e.Op, e.Coll, e.Err)
	}
	return fmt.Sprintf("vego: %s failed: %v", e.Op, e.Err)
}

// Unwrap returns the underlying error for errors.Is/As support
func (e *Error) Unwrap() error { return e.Err }

// Helper functions for error checking

// IsNotFound checks if an error is ErrDocumentNotFound
func IsNotFound(err error) bool {
	return errors.Is(err, ErrDocumentNotFound)
}

// IsDuplicate checks if an error is ErrDuplicateID
func IsDuplicate(err error) bool {
	return errors.Is(err, ErrDuplicateID)
}

// IsDimensionMismatch checks if an error is ErrDimensionMismatch
func IsDimensionMismatch(err error) bool {
	return errors.Is(err, ErrDimensionMismatch)
}

// IsCollectionClosed checks if an error is ErrCollectionClosed
func IsCollectionClosed(err error) bool {
	return errors.Is(err, ErrCollectionClosed)
}

// IsValidationFailed checks if an error is ErrValidationFailed
func IsValidationFailed(err error) bool {
	return errors.Is(err, ErrValidationFailed)
}

// wrapError creates a new Error with the given operation and collection
func wrapError(op, coll, docID string, err error) error {
	if err == nil {
		return nil
	}
	return &Error{
		Op:    op,
		Coll:  coll,
		DocID: docID,
		Err:   err,
	}
}
