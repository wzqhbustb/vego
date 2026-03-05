// Copyright 2024 Vego Authors
// Licensed under the Apache License, Version 2.0

package hnsw

import (
	"sync"
	"testing"

	"github.com/RoaringBitmap/roaring"
)

func TestNewDeletionVector(t *testing.T) {
	dv := NewDeletionVector()
	if dv == nil {
		t.Fatal("NewDeletionVector returned nil")
	}
	if !dv.IsEmpty() {
		t.Error("new DeletionVector should be empty")
	}
	if dv.Count() != 0 {
		t.Errorf("expected count 0, got %d", dv.Count())
	}
}

func TestNewDeletionVectorFromBitmap(t *testing.T) {
	// Test with nil bitmap
	dv := NewDeletionVectorFromBitmap(nil)
	if !dv.IsEmpty() {
		t.Error("DV from nil bitmap should be empty")
	}

	// Test with existing bitmap
	bitmap := roaring.NewBitmap()
	bitmap.Add(1)
	bitmap.Add(5)
	bitmap.Add(10)
	dv = NewDeletionVectorFromBitmap(bitmap)

	if dv.Count() != 3 {
		t.Errorf("expected count 3, got %d", dv.Count())
	}
	if !dv.IsDeleted(1) || !dv.IsDeleted(5) || !dv.IsDeleted(10) {
		t.Error("DV should contain rows 1, 5, 10")
	}

	// Verify bitmap is cloned (modifying original doesn't affect DV)
	bitmap.Add(20)
	if dv.IsDeleted(20) {
		t.Error("DV should be independent of original bitmap")
	}
}

func TestMarkDeleted(t *testing.T) {
	dv := NewDeletionVector()

	// Mark some rows as deleted
	dv.MarkDeleted(0)
	dv.MarkDeleted(5)
	dv.MarkDeleted(100)

	if dv.Count() != 3 {
		t.Errorf("expected count 3, got %d", dv.Count())
	}
	if !dv.IsDeleted(0) || !dv.IsDeleted(5) || !dv.IsDeleted(100) {
		t.Error("marked rows should be deleted")
	}
	if dv.IsDeleted(1) || dv.IsDeleted(99) {
		t.Error("unmarked rows should not be deleted")
	}
}

func TestUnmarkDeleted(t *testing.T) {
	dv := NewDeletionVector()

	// Mark and then unmark
	dv.MarkDeleted(5)
	if !dv.IsDeleted(5) {
		t.Error("row 5 should be deleted")
	}

	removed := dv.UnmarkDeleted(5)
	if !removed {
		t.Error("UnmarkDeleted should return true for deleted row")
	}
	if dv.IsDeleted(5) {
		t.Error("row 5 should not be deleted after unmark")
	}

	// Unmark non-deleted row
	removed = dv.UnmarkDeleted(5)
	if removed {
		t.Error("UnmarkDeleted should return false for non-deleted row")
	}
}

func TestIsDeleted(t *testing.T) {
	dv := NewDeletionVector()

	// Test empty DV
	if dv.IsDeleted(0) {
		t.Error("empty DV should not have any deleted rows")
	}

	// Test after marking
	dv.MarkDeleted(42)
	if !dv.IsDeleted(42) {
		t.Error("row 42 should be deleted")
	}
	if dv.IsDeleted(41) || dv.IsDeleted(43) {
		t.Error("neighbors should not be deleted")
	}
}

func TestCount(t *testing.T) {
	dv := NewDeletionVector()

	if dv.Count() != 0 {
		t.Errorf("empty DV count should be 0, got %d", dv.Count())
	}

	// Add some deletions
	for i := uint32(0); i < 100; i++ {
		dv.MarkDeleted(i)
	}

	if dv.Count() != 100 {
		t.Errorf("expected count 100, got %d", dv.Count())
	}

	// Test CountUint64
	if dv.CountUint64() != 100 {
		t.Errorf("expected uint64 count 100, got %d", dv.CountUint64())
	}
}

func TestIsEmpty(t *testing.T) {
	dv := NewDeletionVector()

	if !dv.IsEmpty() {
		t.Error("new DV should be empty")
	}

	dv.MarkDeleted(0)
	if dv.IsEmpty() {
		t.Error("DV with deletions should not be empty")
	}

	dv.UnmarkDeleted(0)
	if !dv.IsEmpty() {
		t.Error("DV after clearing all deletions should be empty")
	}
}

func TestClear(t *testing.T) {
	dv := NewDeletionVector()

	// Add deletions
	for i := uint32(0); i < 10; i++ {
		dv.MarkDeleted(i)
	}

	if dv.Count() != 10 {
		t.Errorf("expected count 10, got %d", dv.Count())
	}

	// Clear all
	dv.Clear()

	if !dv.IsEmpty() {
		t.Error("DV should be empty after Clear")
	}
	if dv.Count() != 0 {
		t.Errorf("count should be 0 after Clear, got %d", dv.Count())
	}

	// Verify can add after clear
	dv.MarkDeleted(100)
	if !dv.IsDeleted(100) {
		t.Error("should be able to mark deleted after Clear")
	}
}

func TestDeletedRows(t *testing.T) {
	dv := NewDeletionVector()

	// Empty DV
	bitmap := dv.DeletedRows()
	if bitmap.GetCardinality() != 0 {
		t.Error("empty DV should return empty bitmap")
	}

	// Add deletions
	dv.MarkDeleted(1)
	dv.MarkDeleted(3)
	dv.MarkDeleted(5)

	bitmap = dv.DeletedRows()
	if bitmap.GetCardinality() != 3 {
		t.Errorf("expected bitmap cardinality 3, got %d", bitmap.GetCardinality())
	}

	// Verify it's a clone
	bitmap.Add(10)
	if dv.IsDeleted(10) {
		t.Error("modifying returned bitmap should not affect DV")
	}
}

func TestGetDeletedRows(t *testing.T) {
	dv := NewDeletionVector()

	// Empty DV
	rows := dv.GetDeletedRows()
	if len(rows) != 0 {
		t.Errorf("expected empty slice, got %d elements", len(rows))
	}

	// Add deletions
	dv.MarkDeleted(5)
	dv.MarkDeleted(10)
	dv.MarkDeleted(15)

	rows = dv.GetDeletedRows()
	if len(rows) != 3 {
		t.Errorf("expected 3 rows, got %d", len(rows))
	}

	// Verify contents
	expected := map[uint32]bool{5: true, 10: true, 15: true}
	for _, row := range rows {
		if !expected[row] {
			t.Errorf("unexpected row %d", row)
		}
	}
}

func TestIterDeletedRows(t *testing.T) {
	dv := NewDeletionVector()

	// Empty DV
	count := 0
	dv.IterDeletedRows(func(rowID uint32) bool {
		count++
		return true
	})
	if count != 0 {
		t.Errorf("expected 0 iterations, got %d", count)
	}

	// Add deletions
	dv.MarkDeleted(1)
	dv.MarkDeleted(2)
	dv.MarkDeleted(3)

	// Test full iteration
	count = 0
	dv.IterDeletedRows(func(rowID uint32) bool {
		count++
		return true
	})
	if count != 3 {
		t.Errorf("expected 3 iterations, got %d", count)
	}

	// Test early termination
	count = 0
	dv.IterDeletedRows(func(rowID uint32) bool {
		count++
		return count < 2 // Stop after 2
	})
	if count != 2 {
		t.Errorf("expected early termination at 2, got %d", count)
	}
}

func TestMerge(t *testing.T) {
	dv1 := NewDeletionVector()
	dv1.MarkDeleted(1)
	dv1.MarkDeleted(2)

	dv2 := NewDeletionVector()
	dv2.MarkDeleted(2) // Overlap
	dv2.MarkDeleted(3)

	// Merge dv2 into dv1
	dv1.Merge(dv2)

	if dv1.Count() != 3 {
		t.Errorf("expected count 3 after merge, got %d", dv1.Count())
	}

	// Verify all deletions
	if !dv1.IsDeleted(1) || !dv1.IsDeleted(2) || !dv1.IsDeleted(3) {
		t.Error("merged DV should contain rows 1, 2, 3")
	}

	// dv2 should be unchanged
	if dv2.Count() != 2 {
		t.Error("source DV should be unchanged after merge")
	}

	// Test merge with nil
	dv1.Merge(nil) // Should not panic
	if dv1.Count() != 3 {
		t.Error("merge with nil should not change DV")
	}
}

func TestAndNot(t *testing.T) {
	dv1 := NewDeletionVector()
	dv1.MarkDeleted(1)
	dv1.MarkDeleted(2)
	dv1.MarkDeleted(3)

	dv2 := NewDeletionVector()
	dv2.MarkDeleted(2)

	// Remove dv2's deletions from dv1
	dv1.AndNot(dv2)

	if dv1.Count() != 2 {
		t.Errorf("expected count 2, got %d", dv1.Count())
	}
	if dv1.IsDeleted(2) {
		t.Error("row 2 should be removed after AndNot")
	}
	if !dv1.IsDeleted(1) || !dv1.IsDeleted(3) {
		t.Error("rows 1 and 3 should still be deleted")
	}

	// Test with nil
	dv1.AndNot(nil) // Should not panic
	if dv1.Count() != 2 {
		t.Error("AndNot with nil should not change DV")
	}
}

func TestClone(t *testing.T) {
	dv1 := NewDeletionVector()
	dv1.MarkDeleted(1)
	dv1.MarkDeleted(2)

	dv2 := dv1.Clone()

	// Verify clone has same data
	if dv2.Count() != 2 {
		t.Errorf("cloned DV should have count 2, got %d", dv2.Count())
	}

	// Verify independence
	dv2.MarkDeleted(3)
	if dv1.IsDeleted(3) {
		t.Error("modifying clone should not affect original")
	}

	dv1.MarkDeleted(4)
	if dv2.IsDeleted(4) {
		t.Error("modifying original should not affect clone")
	}
}

func TestGetMaxRowID(t *testing.T) {
	dv := NewDeletionVector()

	// Empty DV
	maxID, ok := dv.GetMaxRowID()
	if ok {
		t.Error("empty DV should return ok=false")
	}

	// Add deletions
	dv.MarkDeleted(5)
	dv.MarkDeleted(10)
	dv.MarkDeleted(3)

	maxID, ok = dv.GetMaxRowID()
	if !ok {
		t.Error("should return ok=true for non-empty DV")
	}
	if maxID != 10 {
		t.Errorf("expected max row ID 10, got %d", maxID)
	}
}

func TestConcurrentAccess(t *testing.T) {
	dv := NewDeletionVector()
	const numGoroutines = 100
	const numOperations = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 2)

	// Concurrent writes
	for i := 0; i < numGoroutines; i++ {
		go func(base int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				dv.MarkDeleted(uint32(base*numOperations + j))
			}
		}(i)
	}

	// Concurrent reads
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				_ = dv.Count()
				_ = dv.IsDeleted(uint32(j))
			}
		}()
	}

	wg.Wait()

	// Verify count
	expectedCount := numGoroutines * numOperations
	if dv.Count() != expectedCount {
		t.Errorf("expected count %d after concurrent operations, got %d", expectedCount, dv.Count())
	}
}

func TestLargeDeletions(t *testing.T) {
	dv := NewDeletionVector()

	// Add 10000 deletions
	for i := uint32(0); i < 10000; i++ {
		dv.MarkDeleted(i)
	}

	if dv.Count() != 10000 {
		t.Errorf("expected count 10000, got %d", dv.Count())
	}

	// Spot check
	if !dv.IsDeleted(0) || !dv.IsDeleted(5000) || !dv.IsDeleted(9999) {
		t.Error("spot checks failed")
	}
	if dv.IsDeleted(10000) {
		t.Error("row 10000 should not be deleted")
	}
}

func TestSparseDeletions(t *testing.T) {
	dv := NewDeletionVector()

	// Add sparse deletions (every 1000th row)
	for i := uint32(0); i < 1000000; i += 1000 {
		dv.MarkDeleted(i)
	}

	expectedCount := 1000 // 0, 1000, 2000, ..., 999000
	if dv.Count() != expectedCount {
		t.Errorf("expected count %d for sparse deletions, got %d", expectedCount, dv.Count())
	}

	// Verify sparse pattern
	if !dv.IsDeleted(0) || !dv.IsDeleted(500000) || !dv.IsDeleted(999000) {
		t.Error("sparse deletions should be present")
	}
	if dv.IsDeleted(1) || dv.IsDeleted(500001) {
		t.Error("non-marked rows should not be deleted")
	}
}

func BenchmarkMarkDeleted(b *testing.B) {
	dv := NewDeletionVector()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dv.MarkDeleted(uint32(i))
	}
}

func BenchmarkIsDeleted(b *testing.B) {
	dv := NewDeletionVector()
	// Mark 10000 rows as deleted
	for i := uint32(0); i < 10000; i++ {
		dv.MarkDeleted(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dv.IsDeleted(uint32(i % 20000))
	}
}

func BenchmarkCount(b *testing.B) {
	dv := NewDeletionVector()
	for i := uint32(0); i < 10000; i++ {
		dv.MarkDeleted(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = dv.Count()
	}
}
