// Copyright 2024 Vego Authors
// Licensed under the Apache License, Version 2.0

package hnsw

import (
	"sync"

	"github.com/RoaringBitmap/roaring"
)

// DeletionVector marks deleted rows using a bitmap.
// It provides O(1) deletion checking and memory-efficient storage
// for sparse deletion patterns using RoaringBitmap.
type DeletionVector struct {
	deleted *roaring.Bitmap
	mu      sync.RWMutex
}

// NewDeletionVector creates a new empty DeletionVector.
func NewDeletionVector() *DeletionVector {
	return &DeletionVector{
		deleted: roaring.NewBitmap(),
	}
}

// NewDeletionVectorFromBitmap creates a DeletionVector from an existing bitmap.
// The bitmap is cloned to avoid shared state.
func NewDeletionVectorFromBitmap(bitmap *roaring.Bitmap) *DeletionVector {
	if bitmap == nil {
		return NewDeletionVector()
	}
	return &DeletionVector{
		deleted: bitmap.Clone(),
	}
}

// MarkDeleted marks a row ID as deleted.
// This operation is thread-safe and O(1).
func (dv *DeletionVector) MarkDeleted(rowID uint32) {
	dv.mu.Lock()
	defer dv.mu.Unlock()
	dv.deleted.Add(rowID)
}

// UnmarkDeleted unmarks a row ID (for rollback/undelete scenarios).
// Returns true if the row was previously marked as deleted.
func (dv *DeletionVector) UnmarkDeleted(rowID uint32) bool {
	dv.mu.Lock()
	defer dv.mu.Unlock()
	if !dv.deleted.Contains(rowID) {
		return false
	}
	dv.deleted.Remove(rowID)
	return true
}

// IsDeleted checks if a row is deleted.
// This operation is thread-safe and O(1).
func (dv *DeletionVector) IsDeleted(rowID uint32) bool {
	dv.mu.RLock()
	defer dv.mu.RUnlock()
	return dv.deleted.Contains(rowID)
}

// Count returns the number of deleted rows.
func (dv *DeletionVector) Count() int {
	dv.mu.RLock()
	defer dv.mu.RUnlock()
	return int(dv.deleted.GetCardinality())
}

// CountUint64 returns the number of deleted rows as uint64.
func (dv *DeletionVector) CountUint64() uint64 {
	dv.mu.RLock()
	defer dv.mu.RUnlock()
	return dv.deleted.GetCardinality()
}

// IsEmpty returns true if no rows are marked as deleted.
func (dv *DeletionVector) IsEmpty() bool {
	dv.mu.RLock()
	defer dv.mu.RUnlock()
	return dv.deleted.IsEmpty()
}

// DeletedRows returns a clone of the internal bitmap containing all deleted row IDs.
// The returned bitmap is safe to modify without affecting the DeletionVector.
func (dv *DeletionVector) DeletedRows() *roaring.Bitmap {
	dv.mu.RLock()
	defer dv.mu.RUnlock()
	return dv.deleted.Clone()
}

// Clear removes all deletion marks.
func (dv *DeletionVector) Clear() {
	dv.mu.Lock()
	defer dv.mu.Unlock()
	dv.deleted.Clear()
}

// Merge combines another DeletionVector into this one.
// After merge, this DeletionVector contains all deletions from both.
func (dv *DeletionVector) Merge(other *DeletionVector) {
	if other == nil {
		return
	}

	dv.mu.Lock()
	defer dv.mu.Unlock()

	other.mu.RLock()
	defer other.mu.RUnlock()

	dv.deleted.Or(other.deleted)
}

// GetMaxRowID returns the maximum row ID that has been marked as deleted.
// Returns 0 and false if no rows are deleted.
func (dv *DeletionVector) GetMaxRowID() (uint32, bool) {
	dv.mu.RLock()
	defer dv.mu.RUnlock()

	if dv.deleted.IsEmpty() {
		return 0, false
	}

	// Get the maximum value in the bitmap
	iterator := dv.deleted.ReverseIterator()
	if iterator.HasNext() {
		return iterator.Next(), true
	}
	return 0, false
}

// GetDeletedRows returns a slice of all deleted row IDs.
// Note: This may allocate significant memory for large deletion sets.
// Consider using IterDeletedRows for large datasets.
func (dv *DeletionVector) GetDeletedRows() []uint32 {
	dv.mu.RLock()
	defer dv.mu.RUnlock()

	count := dv.deleted.GetCardinality()
	if count == 0 {
		return []uint32{}
	}

	rows := make([]uint32, 0, count)
	iterator := dv.deleted.Iterator()
	for iterator.HasNext() {
		rows = append(rows, iterator.Next())
	}
	return rows
}

// IterDeletedRows provides an iterator callback for all deleted rows.
// More memory-efficient than GetDeletedRows for large datasets.
func (dv *DeletionVector) IterDeletedRows(fn func(rowID uint32) bool) {
	dv.mu.RLock()
	defer dv.mu.RUnlock()

	iterator := dv.deleted.Iterator()
	for iterator.HasNext() {
		if !fn(iterator.Next()) {
			break
		}
	}
}

// Clone creates a deep copy of the DeletionVector.
func (dv *DeletionVector) Clone() *DeletionVector {
	dv.mu.RLock()
	defer dv.mu.RUnlock()

	return &DeletionVector{
		deleted: dv.deleted.Clone(),
	}
}

// AndNot removes all deletions that are present in other (set subtraction).
// This is useful for clearing deletions after compaction.
func (dv *DeletionVector) AndNot(other *DeletionVector) {
	if other == nil {
		return
	}

	dv.mu.Lock()
	defer dv.mu.Unlock()

	other.mu.RLock()
	defer other.mu.RUnlock()

	dv.deleted.AndNot(other.deleted)
}
