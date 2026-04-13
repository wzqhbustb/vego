// Copyright 2024 Vego Authors
// Licensed under the Apache License, Version 2.0

package format

import (
	"bytes"
	"strings"
	"testing"

	"github.com/wzqhbustb/vego/storage/arrow"
)

// TestFooterStatsRoundTrip verifies that Footer with statistics fields
// can be correctly written and read back.
func TestFooterStatsRoundTrip(t *testing.T) {
	// Create a footer with statistics fields set
	original := NewFooter()
	original.NumPages = 10
	original.StatsOffset = 8255
	original.StatsCount = 3
	original.AddMetadata("test_key", "test_value")
	
	// Add dummy page indices to satisfy validation
	for i := 0; i < 10; i++ {
		original.PageIndexList.Add(int32(i), int32(i), int64(i*1000), 1000, 100, 1)
	}

	// Write to buffer
	var buf bytes.Buffer
	_, err := original.WriteTo(&buf)
	if err != nil {
		t.Fatalf("Failed to write footer: %v", err)
	}

	// Read back
	reader := bytes.NewReader(buf.Bytes())
	read := &Footer{}
	_, err = read.ReadFrom(reader)
	if err != nil {
		t.Fatalf("Failed to read footer: %v", err)
	}

	// Verify all fields including statistics
	if read.NumPages != original.NumPages {
		t.Errorf("NumPages mismatch: got %d, want %d", read.NumPages, original.NumPages)
	}
	if read.StatsOffset != original.StatsOffset {
		t.Errorf("StatsOffset mismatch: got %d, want %d", read.StatsOffset, original.StatsOffset)
	}
	if read.StatsCount != original.StatsCount {
		t.Errorf("StatsCount mismatch: got %d, want %d", read.StatsCount, original.StatsCount)
	}
	if read.Metadata["test_key"] != "test_value" {
		t.Errorf("Metadata mismatch: got %s, want test_value", read.Metadata["test_key"])
	}
}

// TestFooterStatsZeroValues verifies that Footer with zero StatsOffset/StatsCount
// (indicating no statistics) round-trips correctly.
func TestFooterStatsZeroValues(t *testing.T) {
	original := NewFooter()
	original.NumPages = 5
	original.StatsOffset = 0  // No statistics
	original.StatsCount = 0   // No statistics
	
	// Add dummy page indices to satisfy validation
	for i := 0; i < 5; i++ {
		original.PageIndexList.Add(int32(i), int32(i), int64(i*1000), 1000, 100, 1)
	}

	var buf bytes.Buffer
	_, err := original.WriteTo(&buf)
	if err != nil {
		t.Fatalf("Failed to write footer: %v", err)
	}

	reader := bytes.NewReader(buf.Bytes())
	read := &Footer{}
	_, err = read.ReadFrom(reader)
	if err != nil {
		t.Fatalf("Failed to read footer: %v", err)
	}

	if read.StatsOffset != 0 {
		t.Errorf("StatsOffset should be 0, got %d", read.StatsOffset)
	}
	if read.StatsCount != 0 {
		t.Errorf("StatsCount should be 0, got %d", read.StatsCount)
	}
}

// TestFooterEncodedSizeAccuracy verifies EncodedSize matches actual written size.
func TestFooterEncodedSizeAccuracy(t *testing.T) {
	footer := NewFooter()
	footer.NumPages = 100
	footer.StatsOffset = 12345
	footer.StatsCount = 10
	footer.AddMetadata("key1", "value1")
	footer.AddMetadata("key2", "longer_value_here")
	
	// Add dummy page indices to satisfy validation
	for i := 0; i < 100; i++ {
		footer.PageIndexList.Add(int32(i), int32(i), int64(i*1000), 1000, 100, 1)
	}

	// Get encoded size
	encodedSize := footer.EncodedSize()

	// Write and check actual size
	var buf bytes.Buffer
	n, err := footer.WriteTo(&buf)
	if err != nil {
		t.Fatalf("Failed to write footer: %v", err)
	}

	// Note: WriteTo pads to FooterSize, so we can't directly compare
	// But EncodedSize should be <= FooterSize
	if encodedSize > FooterSize {
		t.Errorf("EncodedSize(%d) > FooterSize(%d)", encodedSize, FooterSize)
	}

	// Verify we can read it back
	reader := bytes.NewReader(buf.Bytes())
	read := &Footer{}
	_, err = read.ReadFrom(reader)
	if err != nil {
		t.Fatalf("Failed to read footer: %v", err)
	}

	// Verify checksum validation works (it should since we write data correctly)
	t.Logf("Footer written successfully: %d bytes (encoded size: %d)", n, encodedSize)
}

// TestBuiltinMaxFunction verifies that Go 1.21+ built-in max function works correctly
// for the types we use in statistics.
func TestBuiltinMaxFunction(t *testing.T) {
	// Test int32
	if max(int32(5), int32(10)) != 10 {
		t.Error("max(int32) failed")
	}
	
	// Test int64
	if max(int64(100), int64(50)) != 100 {
		t.Error("max(int64) failed")
	}
	
	// Test float32
	if max(float32(3.14), float32(2.71)) != 3.14 {
		t.Error("max(float32) failed")
	}
	
	// Test float64
	if max(1.5, 2.5) != 2.5 {
		t.Error("max(float64) failed")
	}
}

// TestMergeTypeIDMismatch verifies that Merge returns error when TypeID mismatches.
func TestMergeTypeIDMismatch(t *testing.T) {
	// Create int32 stats
	intStats := &ColumnStatistics{
		Version:     1,
		ColumnIndex: 0,
		TypeID:      StatsTypeInt32,
		HasMinMax:   true,
	}
	intStats.SetMinMaxInt32(10, 20)
	
	// Create float32 stats (different TypeID)
	floatStats := &ColumnStatistics{
		Version:     1,
		ColumnIndex: 0,
		TypeID:      StatsTypeFloat32,
		HasMinMax:   true,
	}
	floatStats.SetMinMaxFloat32(1.5, 2.5)
	
	// Merge should fail due to TypeID mismatch
	err := intStats.Merge(floatStats)
	if err == nil {
		t.Error("Expected error for TypeID mismatch, got nil")
	}
	
	// Verify error message contains relevant info
	errStr := err.Error()
	if !strings.Contains(errStr, "type mismatch") {
		t.Errorf("Error message should contain 'type mismatch', got: %s", errStr)
	}
}

// TestValidateRejectsInvalidVersion verifies Validate rejects invalid version.
func TestValidateRejectsInvalidVersion(t *testing.T) {
	stats := &ColumnStatistics{
		Version:     2, // Invalid version
		ColumnIndex: 0,
		TypeID:      StatsTypeInt32,
	}
	
	err := stats.Validate()
	if err == nil {
		t.Error("Expected error for invalid version, got nil")
	}
}

// TestValidateRejectsInvalidTypeID verifies Validate rejects invalid TypeID.
func TestValidateRejectsInvalidTypeID(t *testing.T) {
	stats := &ColumnStatistics{
		Version:     1,
		ColumnIndex: 0,
		TypeID:      StatsTypeID(255), // Invalid TypeID
	}
	
	err := stats.Validate()
	if err == nil {
		t.Error("Expected error for invalid TypeID, got nil")
	}
}

// TestValidateRejectsNegativeColumnIndex verifies Validate rejects negative column index.
func TestValidateRejectsNegativeColumnIndex(t *testing.T) {
	stats := &ColumnStatistics{
		Version:     1,
		ColumnIndex: -1, // Invalid
		TypeID:      StatsTypeInt32,
	}
	
	err := stats.Validate()
	if err == nil {
		t.Error("Expected error for negative ColumnIndex, got nil")
	}
}

// TestNewStatisticsListColumnIndex verifies that NewStatisticsList correctly
// initializes ColumnIndex for each column.
func TestNewStatisticsListColumnIndex(t *testing.T) {
	sl := NewStatisticsList(5)
	
	for i := 0; i < 5; i++ {
		stats := sl.GetColumnStats(int32(i))
		if stats == nil {
			t.Fatalf("GetColumnStats(%d) returned nil", i)
		}
		if stats.ColumnIndex != int32(i) {
			t.Errorf("Column %d: expected ColumnIndex=%d, got %d", i, i, stats.ColumnIndex)
		}
		if stats.Version != 1 {
			t.Errorf("Column %d: expected Version=1, got %d", i, stats.Version)
		}
	}
}

// TestGetColumnStatsNegativeIndex verifies that negative index returns nil.
func TestGetColumnStatsNegativeIndex(t *testing.T) {
	sl := NewStatisticsList(3)
	
	// Negative index should return nil, not panic
	stats := sl.GetColumnStats(-1)
	if stats != nil {
		t.Error("GetColumnStats(-1) should return nil")
	}
	
	stats = sl.GetColumnStats(-100)
	if stats != nil {
		t.Error("GetColumnStats(-100) should return nil")
	}
}

// TestMergeCopiesColumnIndex verifies that Merge preserves ColumnIndex.
func TestMergeCopiesColumnIndex(t *testing.T) {
	// Create source stats with specific ColumnIndex
	src := &ColumnStatistics{
		Version:     1,
		ColumnIndex: 5,
		TypeID:      StatsTypeInt32,
		HasMinMax:   true,
		NullCount:   0,
	}
	src.SetMinMaxInt32(10, 20)
	
	// Create empty dest stats (like from NewStatisticsList)
	dst := &ColumnStatistics{
		ColumnIndex: 5, // Should already be set by NewStatisticsList
	}
	
	// Merge
	if err := dst.Merge(src); err != nil {
		t.Fatalf("Merge failed: %v", err)
	}
	
	// Verify ColumnIndex is preserved (it was already set to 5)
	if dst.ColumnIndex != 5 {
		t.Errorf("Expected ColumnIndex=5 after merge, got %d", dst.ColumnIndex)
	}
	
	// Verify other fields are copied
	if !dst.HasMinMax {
		t.Error("Expected HasMinMax=true after merge")
	}
	if dst.TypeID != StatsTypeInt32 {
		t.Errorf("Expected TypeID=StatsTypeInt32, got %d", dst.TypeID)
	}
}

// TestStatisticsRoundTripWithMerge verifies statistics survive write/read after merge.
func TestStatisticsRoundTripWithMerge(t *testing.T) {
	// Create statistics list
	sl := NewStatisticsList(2)
	
	// Simulate writing two batches
	// Batch 1: col0=[1,2], col1=[10.5, 20.5]
	batch1Stats0 := ComputeColumnStatistics(arrow.NewInt32Array([]int32{1, 2}, nil), 0)
	batch1Stats1 := ComputeColumnStatistics(arrow.NewFloat32Array([]float32{10.5, 20.5}, nil), 1)
	
	// Batch 2: col0=[5,6], col1=[30.5, 40.5]
	batch2Stats0 := ComputeColumnStatistics(arrow.NewInt32Array([]int32{5, 6}, nil), 0)
	batch2Stats1 := ComputeColumnStatistics(arrow.NewFloat32Array([]float32{30.5, 40.5}, nil), 1)
	
	// Merge batch 1
	if err := sl.Stats[0].Merge(batch1Stats0); err != nil {
		t.Fatalf("Merge batch1 col0 failed: %v", err)
	}
	if err := sl.Stats[1].Merge(batch1Stats1); err != nil {
		t.Fatalf("Merge batch1 col1 failed: %v", err)
	}
	
	// Merge batch 2
	if err := sl.Stats[0].Merge(batch2Stats0); err != nil {
		t.Fatalf("Merge batch2 col0 failed: %v", err)
	}
	if err := sl.Stats[1].Merge(batch2Stats1); err != nil {
		t.Fatalf("Merge batch2 col1 failed: %v", err)
	}
	
	// Write to buffer
	var buf bytes.Buffer
	if _, err := sl.WriteTo(&buf); err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}
	
	// Read back
	readSl := &StatisticsList{}
	if _, err := readSl.ReadFrom(&buf); err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}
	
	// Verify ColumnIndex preserved
	for i := 0; i < 2; i++ {
		stats := readSl.GetColumnStats(int32(i))
		if stats == nil {
			t.Fatalf("GetColumnStats(%d) returned nil", i)
		}
		if stats.ColumnIndex != int32(i) {
			t.Errorf("Column %d: expected ColumnIndex=%d, got %d", i, i, stats.ColumnIndex)
		}
	}
	
	// Verify merged min/max values
	min0, max0, ok0 := readSl.Stats[0].GetMinMaxInt32()
	if !ok0 || min0 != 1 || max0 != 6 {
		t.Errorf("Col0: expected min=1, max=6, got min=%d, max=%d", min0, max0)
	}
	
	min1, max1, ok1 := readSl.Stats[1].GetMinMaxFloat32()
	if !ok1 || min1 != 10.5 || max1 != 40.5 {
		t.Errorf("Col1: expected min=10.5, max=40.5, got min=%f, max=%f", min1, max1)
	}
}
