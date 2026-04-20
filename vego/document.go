package vego

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Document represents a document with vector embedding and metadata
type Document struct {
	ID        string                 `json:"id"`
	Vector    []float32              `json:"vector"`
	Metadata  map[string]interface{} `json:"metadata"`
	Timestamp time.Time              `json:"timestamp"`
}

// DocumentID generates a unique document ID using UUID v4
func DocumentID() string {
	return uuid.New().String()
}

// Validate checks if document is valid
func (d *Document) Validate(dimension int) error {
	if d.ID == "" {
		return fmt.Errorf("document ID is required")
	}
	if dimension <= 0 {
		return fmt.Errorf("invalid dimension: %d", dimension)
	}
	if len(d.Vector) != dimension {
		return fmt.Errorf("vector dimension mismatch: expected %d, got %d", dimension, len(d.Vector))
	}
	return nil
}

// Clone creates a copy of the document. Vector is deep-copied.
// Metadata is shallow-copied at the map level: the map itself is duplicated,
// but values inside it (slices, maps, pointers) are shared with the original.
func (d *Document) Clone() *Document {
	clone := &Document{
		ID:        d.ID,
		Vector:    make([]float32, len(d.Vector)),
		Metadata:  nil,
		Timestamp: d.Timestamp,
	}
	copy(clone.Vector, d.Vector)

	// Only copy metadata if it's not nil
	if d.Metadata != nil {
		clone.Metadata = make(map[string]interface{}, len(d.Metadata))
		for k, v := range d.Metadata {
			clone.Metadata[k] = v
		}
	}

	return clone
}
