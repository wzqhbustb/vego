// Copyright 2024 Vego Authors
// Licensed under the Apache License, Version 2.0

package format

import (
	"bytes"
	"testing"
)

// TestRowIndex_BasicOperations tests basic insert and lookup
func TestRowIndex_BasicOperations(t *testing.T) {
	ri := NewRowIndex(100)

	// Test initial state
	if ri.Magic != RowIndexMagic {
		t.Errorf("Magic = 0x%08X, want 0x%08X", ri.Magic, RowIndexMagic)
	}
	if ri.NumEntries != 0 {
		t.Errorf("NumEntries = %d, want 0", ri.NumEntries)
	}
	if ri.BucketCount < 16 {
		t.Errorf("BucketCount = %d, want >= 16", ri.BucketCount)
	}

	// Insert some entries
	ids := []string{"id1", "id2", "id3", "id4", "id5"}
	for i, id := range ids {
		err := ri.Insert(id, int64(i*10))
		if err != nil {
			t.Errorf("Insert(%s) failed: %v", id, err)
		}
	}

	if ri.NumEntries != int32(len(ids)) {
		t.Errorf("NumEntries = %d, want %d", ri.NumEntries, len(ids))
	}

	// Lookup entries
	for i, id := range ids {
		rowIdx := ri.Lookup(id)
		if rowIdx != int64(i*10) {
			t.Errorf("Lookup(%s) = %d, want %d", id, rowIdx, i*10)
		}
	}

	// Lookup non-existent entry
	rowIdx := ri.Lookup("nonexistent")
	if rowIdx != -1 {
		t.Errorf("Lookup(nonexistent) = %d, want -1", rowIdx)
	}
}

// TestRowIndex_Update tests updating existing entries
func TestRowIndex_Update(t *testing.T) {
	ri := NewRowIndex(10)

	// Insert
	err := ri.Insert("id1", 100)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Update
	err = ri.Insert("id1", 200)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	// Verify update
	rowIdx := ri.Lookup("id1")
	if rowIdx != 200 {
		t.Errorf("Lookup(id1) = %d, want 200", rowIdx)
	}

	// Should still have only 1 entry
	if ri.NumEntries != 1 {
		t.Errorf("NumEntries = %d, want 1", ri.NumEntries)
	}
}

// TestRowIndex_HashCollision tests collision handling
func TestRowIndex_HashCollision(t *testing.T) {
	ri := NewRowIndex(10)

	// Insert many entries to force collisions
	for i := 0; i < 50; i++ {
		id := string(rune('a' + i%26)) + string(rune('a'+i/26))
		err := ri.Insert(id, int64(i))
		if err != nil {
			t.Errorf("Insert(%s) failed: %v", id, err)
		}
	}

	// Verify all entries can be found
	for i := 0; i < 50; i++ {
		id := string(rune('a' + i%26)) + string(rune('a'+i/26))
		rowIdx := ri.Lookup(id)
		if rowIdx != int64(i) {
			t.Errorf("Lookup(%s) = %d, want %d", id, rowIdx, i)
		}
	}
}

// TestRowIndex_Rehash tests automatic rehashing
func TestRowIndex_Rehash(t *testing.T) {
	ri := NewRowIndex(10)
	initialBuckets := ri.BucketCount

	// Insert many entries to trigger rehash
	for i := 0; i < 100; i++ {
		id := string(rune('a'+i%26)) + string(rune('a'+i/26)) + string(rune('a'+i/676))
		err := ri.Insert(id, int64(i))
		if err != nil {
			t.Errorf("Insert(%s) failed: %v", id, err)
		}
	}

	// Bucket count should have increased
	if ri.BucketCount <= initialBuckets {
		t.Errorf("BucketCount = %d, want > %d (rehash should have occurred)", ri.BucketCount, initialBuckets)
	}

	// Verify all entries still accessible after rehash
	for i := 0; i < 100; i++ {
		id := string(rune('a'+i%26)) + string(rune('a'+i/26)) + string(rune('a'+i/676))
		rowIdx := ri.Lookup(id)
		if rowIdx != int64(i) {
			t.Errorf("Lookup(%s) = %d, want %d after rehash", id, rowIdx, i)
		}
	}
}

// TestRowIndex_Serialization tests WriteTo and ReadFrom
func TestRowIndex_Serialization(t *testing.T) {
	// Create and populate RowIndex
	ri := NewRowIndex(50)
	for i := 0; i < 20; i++ {
		id := string(rune('a' + i))
		err := ri.Insert(id, int64(i*100))
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	// Serialize
	var buf bytes.Buffer
	n, err := ri.WriteTo(&buf)
	if err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}

	// Deserialize
	ri2 := &RowIndex{}
	n2, err := ri2.ReadFrom(&buf)
	if err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}

	if n != n2 {
		t.Errorf("Bytes written = %d, bytes read = %d", n, n2)
	}

	// Verify magic
	if ri2.Magic != RowIndexMagic {
		t.Errorf("Magic = 0x%08X, want 0x%08X", ri2.Magic, RowIndexMagic)
	}

	// Verify entries
	for i := 0; i < 20; i++ {
		id := string(rune('a' + i))
		rowIdx := ri2.Lookup(id)
		if rowIdx != int64(i*100) {
			t.Errorf("Lookup(%s) = %d, want %d after deserialization", id, rowIdx, i*100)
		}
	}
}

// TestRowIndex_ToFromPage tests conversion to/from Page
func TestRowIndex_ToFromPage(t *testing.T) {
	ri := NewRowIndex(20)
	for i := 0; i < 10; i++ {
		id := string(rune('a' + i))
		err := ri.Insert(id, int64(i*10))
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	// Convert to Page
	page, err := ri.ToPage()
	if err != nil {
		t.Fatalf("ToPage failed: %v", err)
	}

	// Verify Page properties
	if page.Type != PageTypeIndex {
		t.Errorf("Page.Type = %v, want PageTypeIndex", page.Type)
	}
	if page.NumValues != ri.NumEntries {
		t.Errorf("Page.NumValues = %d, want %d", page.NumValues, ri.NumEntries)
	}

	// Convert from Page
	ri2, err := RowIndexFromPage(page)
	if err != nil {
		t.Fatalf("RowIndexFromPage failed: %v", err)
	}

	// Verify entries
	for i := 0; i < 10; i++ {
		id := string(rune('a' + i))
		rowIdx := ri2.Lookup(id)
		if rowIdx != int64(i*10) {
			t.Errorf("Lookup(%s) = %d, want %d", id, rowIdx, i*10)
		}
	}
}

// TestRowIndex_Validate tests validation
func TestRowIndex_Validate(t *testing.T) {
	// Valid RowIndex
	ri := NewRowIndex(10)
	ri.Insert("id1", 100)
	if err := ri.Validate(); err != nil {
		t.Errorf("Validate failed for valid RowIndex: %v", err)
	}

	// Invalid magic
	riBad := &RowIndex{Magic: 0x12345678}
	if err := riBad.Validate(); err == nil {
		t.Error("Validate should fail for invalid magic")
	}

	// Invalid num entries
	riBad2 := &RowIndex{Magic: RowIndexMagic, NumEntries: -1, BucketCount: 16}
	if err := riBad2.Validate(); err == nil {
		t.Error("Validate should fail for negative num entries")
	}

	// Invalid bucket count (not power of 2)
	riBad3 := &RowIndex{Magic: RowIndexMagic, NumEntries: 0, BucketCount: 15}
	if err := riBad3.Validate(); err == nil {
		t.Error("Validate should fail for non-power-of-2 bucket count")
	}
}

// TestRowIndex_Stats tests statistics
func TestRowIndex_Stats(t *testing.T) {
	ri := NewRowIndex(50)
	for i := 0; i < 30; i++ {
		id := string(rune('a' + i%26)) + string(rune('a'+i/26))
		ri.Insert(id, int64(i))
	}

	stats := ri.Stats()

	if stats.NumEntries != 30 {
		t.Errorf("NumEntries = %d, want 30", stats.NumEntries)
	}
	if stats.BucketCount <= 0 {
		t.Errorf("BucketCount = %d, want > 0", stats.BucketCount)
	}
	if stats.LoadFactor <= 0 || stats.LoadFactor > 1 {
		t.Errorf("LoadFactor = %.2f, want (0, 1]", stats.LoadFactor)
	}
	if stats.MemoryUsage <= 0 {
		t.Errorf("MemoryUsage = %d, want > 0", stats.MemoryUsage)
	}
}

// TestRowIndex_EncodedSize tests size calculation
func TestRowIndex_EncodedSize(t *testing.T) {
	ri := NewRowIndex(20)
	for i := 0; i < 5; i++ {
		ri.Insert(string(rune('a'+i)), int64(i))
	}

	size := ri.EncodedSize()

	// Minimum size: magic(4) + numEntries(4) + bucketCount(4) + hashTable + entries + checksum(4)
	expected := 4 + 4 + 4 + len(ri.HashTable)*8 + len(ri.Entries)*16 + 4

	if size != expected {
		t.Errorf("EncodedSize = %d, want %d", size, expected)
	}
}

// TestRowIndex_LargeDataset tests with a large number of entries
func TestRowIndex_LargeDataset(t *testing.T) {
	ri := NewRowIndex(10000)

	const numEntries = 5000
	// Insert entries
	for i := 0; i < numEntries; i++ {
		id := string(rune('a'+i%26)) + string(rune('a'+i/26%26)) + string(rune('a'+i/676%26))
		err := ri.Insert(id, int64(i))
		if err != nil {
			t.Errorf("Insert(%s) failed at iteration %d: %v", id, i, err)
		}
	}

	// Verify all entries
	for i := 0; i < numEntries; i++ {
		id := string(rune('a'+i%26)) + string(rune('a'+i/26%26)) + string(rune('a'+i/676%26))
		rowIdx := ri.Lookup(id)
		if rowIdx != int64(i) {
			t.Errorf("Lookup(%s) = %d, want %d", id, rowIdx, i)
		}
	}

	// Check stats
	stats := ri.Stats()
	if stats.NumEntries != numEntries {
		t.Errorf("NumEntries = %d, want %d", stats.NumEntries, numEntries)
	}
}

// TestRowIndex_EmptyLookup tests lookup on empty index
func TestRowIndex_EmptyLookup(t *testing.T) {
	ri := NewRowIndex(10)

	rowIdx := ri.Lookup("nonexistent")
	if rowIdx != -1 {
		t.Errorf("Lookup on empty index = %d, want -1", rowIdx)
	}
}

// BenchmarkRowIndex_Insert benchmarks insert operations
func BenchmarkRowIndex_Insert(b *testing.B) {
	ri := NewRowIndex(int32(b.N))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		id := string(rune('a'+i%26)) + string(rune('a'+i/26%26)) + string(rune('a'+i/676%26))
		ri.Insert(id, int64(i))
	}
}

// BenchmarkRowIndex_Lookup benchmarks lookup operations
func BenchmarkRowIndex_Lookup(b *testing.B) {
	ri := NewRowIndex(1000)
	for i := 0; i < 500; i++ {
		id := string(rune('a' + i%26))
		ri.Insert(id, int64(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ri.Lookup("a")
	}
}

// BenchmarkRowIndex_Serialization benchmarks serialization
func BenchmarkRowIndex_Serialization(b *testing.B) {
	ri := NewRowIndex(100)
	for i := 0; i < 50; i++ {
		ri.Insert(string(rune('a'+i)), int64(i))
	}

	buf := new(bytes.Buffer)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buf.Reset()
		ri.WriteTo(buf)
	}
}
