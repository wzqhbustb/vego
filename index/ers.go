package hnsw

import "errors"

var (
	// ErrDimensionMismatch is returned when vector dimensions do not match
	ErrDimensionMismatch = errors.New("vector dimension mismatch")

	// ErrEmptyIndex is returned when the index is empty
	ErrEmptyIndex = errors.New("index is empty")

	// ErrInvalidParameter is returned when a parameter is invalid
	ErrInvalidParameter = errors.New("invalid parameter")
)
