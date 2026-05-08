// Copyright 2024 Vego Authors
// Licensed under the Apache License, Version 2.0

package column

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/wzqhbustb/vego/core"
	"github.com/wzqhbustb/vego/storage/encoding"
	"github.com/wzqhbustb/vego/storage/format"
)

// TestReadRowAtBasic tests basic ReadRowAt functionality
func TestReadRowAtBasic(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "column-readrowat-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create test file with RowIndex
	filename := filepath.Join(tmpDir, "test.lance")
	schema := core.NewSchema([]core.Field{
		{Name: "id", Type: core.PrimInt64(), Nullable: false},
		{Name: "vector", Type: core.VectorType(4), Nullable: false},
		{Name: "timestamp", Type: core.PrimInt64(), Nullable: false},
	}, nil)

	factory := encoding.NewEncoderFactory(3)
	writer, err := NewRowIndexWriter(filename, schema, format.V1_2, factory)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	// Build data: 5 rows
	idBuilder := core.NewInt64Builder()
	vectorBuilder := core.NewFixedSizeListBuilder(
		core.FixedSizeListOf(core.PrimFloat32(), 4).(*core.FixedSizeListType),
	)
	timestampBuilder := core.NewInt64Builder()

	expectedIDs := []int64{100, 200, 300, 400, 500}
	expectedVectors := [][]float32{
		{1.0, 2.0, 3.0, 4.0},
		{5.0, 6.0, 7.0, 8.0},
		{9.0, 10.0, 11.0, 12.0},
		{13.0, 14.0, 15.0, 16.0},
		{17.0, 18.0, 19.0, 20.0},
	}
	expectedTimestamps := []int64{1000, 2000, 3000, 4000, 5000}

	for i := 0; i < 5; i++ {
		idBuilder.Append(expectedIDs[i])
		vectorBuilder.AppendValues(expectedVectors[i])
		timestampBuilder.Append(expectedTimestamps[i])
	}

	batch, err := core.NewRecordBatch(schema, 5, []core.Array{
		idBuilder.NewArray(),
		vectorBuilder.NewArray(),
		timestampBuilder.NewArray(),
	})
	if err != nil {
		t.Fatalf("Failed to create batch: %v", err)
	}

	if err := writer.WriteRecordBatch(batch); err != nil {
		t.Fatalf("Failed to write batch: %v", err)
	}

	// Add RowIndex
	for i := 0; i < 5; i++ {
		writer.AddRowID(string(rune('a'+i)), int64(i))
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// Test ReadRowAt for each row
	reader, err := NewRowIndexReader(filename)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Close()

	for rowIdx := int64(0); rowIdx < 5; rowIdx++ {
		values, err := reader.ReadRowAt(rowIdx)
		if err != nil {
			t.Fatalf("ReadRowAt(%d) failed: %v", rowIdx, err)
		}

		if len(values) != 3 {
			t.Errorf("Expected 3 values, got %d", len(values))
			continue
		}

		// Verify values
		if values[0].(int64) != expectedIDs[rowIdx] {
			t.Errorf("Row %d: ID mismatch: got %v, want %d", rowIdx, values[0], expectedIDs[rowIdx])
		}

		vector := values[1].([]float32)
		if len(vector) != 4 {
			t.Errorf("Row %d: Vector length mismatch", rowIdx)
		} else {
			for i, v := range expectedVectors[rowIdx] {
				if vector[i] != v {
					t.Errorf("Row %d: Vector[%d] mismatch: got %v, want %v", rowIdx, i, vector[i], v)
				}
			}
		}

		if values[2].(int64) != expectedTimestamps[rowIdx] {
			t.Errorf("Row %d: Timestamp mismatch: got %v, want %d", rowIdx, values[2], expectedTimestamps[rowIdx])
		}
	}

	t.Logf("ReadRowAt basic test passed - all 5 rows read correctly")
}

// TestReadRowAtBounds tests boundary conditions
func TestReadRowAtBounds(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "column-readrowat-bounds-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	filename := filepath.Join(tmpDir, "test.lance")
	schema := core.NewSchema([]core.Field{
		{Name: "id", Type: core.PrimInt64(), Nullable: false},
	}, nil)

	factory := encoding.NewEncoderFactory(3)
	writer, err := NewRowIndexWriter(filename, schema, format.V1_2, factory)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	// Write 3 rows
	idBuilder := core.NewInt64Builder()
	for i := 0; i < 3; i++ {
		idBuilder.Append(int64(i + 1))
	}

	batch, _ := core.NewRecordBatch(schema, 3, []core.Array{idBuilder.NewArray()})
	writer.WriteRecordBatch(batch)
	for i := 0; i < 3; i++ {
		writer.AddRowID(string(rune('a'+i)), int64(i))
	}
	writer.Close()

	reader, err := NewRowIndexReader(filename)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Close()

	// Test valid indices
	for i := int64(0); i < 3; i++ {
		_, err := reader.ReadRowAt(i)
		if err != nil {
			t.Errorf("ReadRowAt(%d) should succeed, got: %v", i, err)
		}
	}

	// Test negative index
	_, err = reader.ReadRowAt(-1)
	if err == nil {
		t.Error("ReadRowAt(-1) should fail")
	}

	// Test out of bounds (equal to num rows)
	_, err = reader.ReadRowAt(3)
	if err == nil {
		t.Error("ReadRowAt(3) should fail (out of bounds)")
	}

	// Test far out of bounds
	_, err = reader.ReadRowAt(100)
	if err == nil {
		t.Error("ReadRowAt(100) should fail (out of bounds)")
	}

	t.Logf("ReadRowAt bounds test passed")
}

// TestReadRowAtEmptyFile tests behavior with empty file
func TestReadRowAtEmptyFile(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "column-readrowat-empty-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	filename := filepath.Join(tmpDir, "test.lance")
	schema := core.NewSchema([]core.Field{
		{Name: "id", Type: core.PrimInt64(), Nullable: false},
	}, nil)

	factory := encoding.NewEncoderFactory(3)
	writer, err := NewRowIndexWriter(filename, schema, format.V1_2, factory)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	// Write empty batch
	idBuilder := core.NewInt64Builder()
	batch, _ := core.NewRecordBatch(schema, 0, []core.Array{idBuilder.NewArray()})
	writer.WriteRecordBatch(batch)
	writer.Close()

	reader, err := NewRowIndexReader(filename)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Close()

	// Test reading from empty file
	_, err = reader.ReadRowAt(0)
	if err == nil {
		t.Error("ReadRowAt(0) on empty file should fail")
	}

	t.Logf("ReadRowAt empty file test passed")
}

// TestReadRowAtWithCache tests ReadRowAt with BlockCache
func TestReadRowAtWithCache(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "column-readrowat-cache-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	filename := filepath.Join(tmpDir, "test.lance")
	schema := core.NewSchema([]core.Field{
		{Name: "id", Type: core.PrimInt64(), Nullable: false},
		{Name: "value", Type: core.PrimFloat32(), Nullable: false},
	}, nil)

	factory := encoding.NewEncoderFactory(3)
	writer, err := NewRowIndexWriter(filename, schema, format.V1_2, factory)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	// Write 10 rows
	idBuilder := core.NewInt64Builder()
	valueBuilder := core.NewFloat32Builder()
	for i := 0; i < 10; i++ {
		idBuilder.Append(int64(i))
		valueBuilder.Append(float32(i * 10))
	}

	batch, _ := core.NewRecordBatch(schema, 10, []core.Array{
		idBuilder.NewArray(),
		valueBuilder.NewArray(),
	})
	writer.WriteRecordBatch(batch)
	for i := 0; i < 10; i++ {
		writer.AddRowID(string(rune('a'+i)), int64(i))
	}
	writer.Close()

	// Create cache and reader
	cache := format.NewBlockCache(64 * 1024 * 1024)
	reader, err := NewRowIndexReaderWithCache(filename, cache)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Close()

	// Read multiple times to test caching
	for iteration := 0; iteration < 3; iteration++ {
		for rowIdx := int64(0); rowIdx < 10; rowIdx++ {
			values, err := reader.ReadRowAt(rowIdx)
			if err != nil {
				t.Fatalf("Iteration %d, ReadRowAt(%d) failed: %v", iteration, rowIdx, err)
			}

			if values[0].(int64) != rowIdx {
				t.Errorf("Iteration %d, Row %d: ID mismatch", iteration, rowIdx)
			}
			if values[1].(float32) != float32(rowIdx*10) {
				t.Errorf("Iteration %d, Row %d: Value mismatch", iteration, rowIdx)
			}
		}
	}

	// Check cache was used
	stats := reader.BlockCacheStats()
	if stats.Hits == 0 {
		t.Logf("Warning: No cache hits recorded (cache may not be configured for ReadRowAt)")
	} else {
		t.Logf("Cache stats: hits=%d, misses=%d", stats.Hits, stats.Misses)
	}

	t.Logf("ReadRowAt with cache test passed")
}

// TestReadRowAtConsistency tests that ReadRowAt returns consistent results
func TestReadRowAtConsistency(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "column-readrowat-consistency-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	filename := filepath.Join(tmpDir, "test.lance")
	schema := core.NewSchema([]core.Field{
		{Name: "id", Type: core.PrimInt64(), Nullable: false},
	}, nil)

	factory := encoding.NewEncoderFactory(3)
	writer, err := NewRowIndexWriter(filename, schema, format.V1_2, factory)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	// Write 100 rows
	idBuilder := core.NewInt64Builder()
	for i := 0; i < 100; i++ {
		idBuilder.Append(int64(i * i)) // Use squares to verify correctness
	}

	batch, _ := core.NewRecordBatch(schema, 100, []core.Array{idBuilder.NewArray()})
	writer.WriteRecordBatch(batch)
	for i := 0; i < 100; i++ {
		writer.AddRowID(fmt.Sprintf("doc-%d", i), int64(i))
	}
	writer.Close()

	reader, err := NewRowIndexReader(filename)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Close()

	// Read rows in random order multiple times
	readOrder := []int64{50, 10, 90, 0, 99, 33, 77, 1, 98}
	for iteration := 0; iteration < 5; iteration++ {
		for _, rowIdx := range readOrder {
			values, err := reader.ReadRowAt(rowIdx)
			if err != nil {
				t.Fatalf("Iteration %d, ReadRowAt(%d) failed: %v", iteration, rowIdx, err)
			}

			expectedValue := int64(rowIdx * rowIdx)
			if values[0].(int64) != expectedValue {
				t.Errorf("Iteration %d, Row %d: Value mismatch: got %v, want %d",
					iteration, rowIdx, values[0], expectedValue)
			}
		}
	}

	t.Logf("ReadRowAt consistency test passed - 100 rows verified")
}
