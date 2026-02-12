// Copyright 2024 Vego Authors
// Licensed under the Apache License, Version 2.0

package column

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/wzqhbustb/vego/storage/arrow"
	"github.com/wzqhbustb/vego/storage/format"
)

// TestRowIndexWriterReader tests the full RowIndex write/read cycle
func TestRowIndexWriterReader(t *testing.T) {
	// Create a temporary directory for test files
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_rowindex.lance")

	// Create schema with only supported types
	fields := []arrow.Field{
		arrow.NewField("id", arrow.PrimInt64(), false),
		arrow.NewField("embedding", arrow.VectorType(128), false),
	}
	schema := arrow.NewSchema(fields, nil)

	// Write file with RowIndex (V1.1)
	t.Run("write_v1.1_with_rowindex", func(t *testing.T) {
		writer, err := NewRowIndexWriter(filename, schema, format.V1_1, nil)
		if err != nil {
			t.Fatalf("Failed to create writer: %v", err)
		}

		// Create record batch using builders
		builder := arrow.NewRecordBatchBuilder(schema)

		// Build id column
		idBuilder := builder.Field(0).(*arrow.Int64Builder)
		idBuilder.Append(1)
		idBuilder.Append(2)
		idBuilder.Append(3)

		// Build embedding column (vector)
		embBuilder := builder.Field(1).(*arrow.FixedSizeListBuilder)
		for i := 0; i < 3; i++ {
			embBuilder.AppendValues(make([]float32, 128))
		}

		batch, err := builder.NewBatch()
		if err != nil {
			t.Fatalf("Failed to create record batch: %v", err)
		}

		// Write batch
		if err := writer.WriteRecordBatch(batch); err != nil {
			t.Fatalf("Failed to write record batch: %v", err)
		}

		// Add row index mappings
		if err := writer.AddRowID("doc1", 0); err != nil {
			t.Errorf("AddRowID(doc1) failed: %v", err)
		}
		if err := writer.AddRowID("doc2", 1); err != nil {
			t.Errorf("AddRowID(doc2) failed: %v", err)
		}
		if err := writer.AddRowID("doc3", 2); err != nil {
			t.Errorf("AddRowID(doc3) failed: %v", err)
		}

		// Close writer
		if err := writer.Close(); err != nil {
			t.Fatalf("Failed to close writer: %v", err)
		}

		// Verify file exists
		if _, err := os.Stat(filename); os.IsNotExist(err) {
			t.Errorf("File was not created: %s", filename)
		}
	})

	// Read file and verify RowIndex
	t.Run("read_v1.1_with_rowindex", func(t *testing.T) {
		reader, err := NewRowIndexReader(filename)
		if err != nil {
			t.Fatalf("Failed to create reader: %v", err)
		}
		defer reader.Close()

		// Check version
		version := reader.GetVersion()
		if version.MajorVersion != 1 || version.MinorVersion != 1 {
			t.Errorf("Version = %s, want V1.1", version.String())
		}

		// Check RowIndex presence
		if !reader.HasRowIndex() {
			t.Error("HasRowIndex() = false, want true")
		}

		// Lookup rows
		tests := []struct {
			docID     string
			wantIndex int64
		}{
			{"doc1", 0},
			{"doc2", 1},
			{"doc3", 2},
		}

		for _, tt := range tests {
			rowIdx, err := reader.LookupRowID(tt.docID)
			if err != nil {
				t.Errorf("LookupRowID(%s) failed: %v", tt.docID, err)
				continue
			}
			if rowIdx != tt.wantIndex {
				t.Errorf("LookupRowID(%s) = %d, want %d", tt.docID, rowIdx, tt.wantIndex)
			}
		}

		// Lookup non-existent ID
		_, err = reader.LookupRowID("nonexistent")
		if err == nil {
			t.Error("LookupRowID(nonexistent) should fail")
		}
	})
}

// TestRowIndexWriterV10 tests that V1.0 files don't have RowIndex
func TestRowIndexWriterV10(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_v10.lance")

	fields := []arrow.Field{
		arrow.NewField("id", arrow.PrimInt64(), false),
	}
	schema := arrow.NewSchema(fields, nil)

	// Write V1.0 file (no RowIndex)
	writer, err := NewRowIndexWriter(filename, schema, format.V1_0, nil)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	builder := arrow.NewRecordBatchBuilder(schema)
	idBuilder := builder.Field(0).(*arrow.Int64Builder)
	idBuilder.Append(1)
	idBuilder.Append(2)

	batch, err := builder.NewBatch()
	if err != nil {
		t.Fatalf("Failed to create record batch: %v", err)
	}

	if err := writer.WriteRecordBatch(batch); err != nil {
		t.Fatalf("Failed to write batch: %v", err)
	}

	// AddRowID should be no-op for V1.0
	if err := writer.AddRowID("doc1", 0); err != nil {
		t.Errorf("AddRowID for V1.0 failed: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// Read and verify no RowIndex
	reader, err := NewRowIndexReader(filename)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Close()

	// Check version
	if reader.GetVersion().String() != "1.0" {
		t.Errorf("Version = %s, want 1.0", reader.GetVersion().String())
	}

	// Check no RowIndex
	if reader.HasRowIndex() {
		t.Error("HasRowIndex() = true for V1.0 file, want false")
	}

	// LookupRowID should fail
	_, err = reader.LookupRowID("doc1")
	if err == nil {
		t.Error("LookupRowID should fail for V1.0 file")
	}
}

// TestRowIndexWriterV12 tests V1.2 files with both RowIndex and BlockCache
func TestRowIndexWriterV12(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_v12.lance")

	fields := []arrow.Field{
		arrow.NewField("id", arrow.PrimInt64(), false),
		arrow.NewField("vector", arrow.VectorType(64), false),
	}
	schema := arrow.NewSchema(fields, nil)

	writer, err := NewRowIndexWriter(filename, schema, format.V1_2, nil)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	builder := arrow.NewRecordBatchBuilder(schema)

	idBuilder := builder.Field(0).(*arrow.Int64Builder)
	for i := 1; i <= 5; i++ {
		idBuilder.Append(int64(i))
	}

	vecBuilder := builder.Field(1).(*arrow.FixedSizeListBuilder)
	for i := 0; i < 5; i++ {
		vecBuilder.AppendValues(make([]float32, 64))
	}

	batch, err := builder.NewBatch()
	if err != nil {
		t.Fatalf("Failed to create record batch: %v", err)
	}

	if err := writer.WriteRecordBatch(batch); err != nil {
		t.Fatalf("Failed to write batch: %v", err)
	}

	// Add row index mappings
	for i := 1; i <= 5; i++ {
		docID := string(rune('a' - 1 + i))
		if err := writer.AddRowID(docID, int64(i-1)); err != nil {
			t.Errorf("AddRowID(%s) failed: %v", docID, err)
		}
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// Read and verify
	reader, err := NewRowIndexReader(filename)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Close()

	// Check version
	if reader.GetVersion().String() != "1.2" {
		t.Errorf("Version = %s, want 1.2", reader.GetVersion().String())
	}

	// Check has RowIndex
	if !reader.HasRowIndex() {
		t.Error("HasRowIndex() = false for V1.2 file, want true")
	}

	// Verify all lookups
	for i := 1; i <= 5; i++ {
		docID := string(rune('a' - 1 + i))
		rowIdx, err := reader.LookupRowID(docID)
		if err != nil {
			t.Errorf("LookupRowID(%s) failed: %v", docID, err)
			continue
		}
		if rowIdx != int64(i-1) {
			t.Errorf("LookupRowID(%s) = %d, want %d", docID, rowIdx, i-1)
		}
	}
}

// TestRowIndexStats tests RowIndex statistics
func TestRowIndexStats(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_stats.lance")

	fields := []arrow.Field{
		arrow.NewField("id", arrow.PrimInt64(), false),
	}
	schema := arrow.NewSchema(fields, nil)

	writer, err := NewRowIndexWriter(filename, schema, format.V1_1, nil)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	builder := arrow.NewRecordBatchBuilder(schema)
	idBuilder := builder.Field(0).(*arrow.Int64Builder)
	for i := 0; i < 10; i++ {
		idBuilder.Append(int64(i))
	}

	batch, err := builder.NewBatch()
	if err != nil {
		t.Fatalf("Failed to create record batch: %v", err)
	}

	if err := writer.WriteRecordBatch(batch); err != nil {
		t.Fatalf("Failed to write batch: %v", err)
	}

	// Add 10 entries
	for i := 0; i < 10; i++ {
		docID := string(rune('a' + i))
		if err := writer.AddRowID(docID, int64(i)); err != nil {
			t.Errorf("AddRowID(%s) failed: %v", docID, err)
		}
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// Read and get stats
	reader, err := NewRowIndexReader(filename)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Close()

	stats, err := reader.RowIndexStats()
	if err != nil {
		t.Fatalf("RowIndexStats() failed: %v", err)
	}

	if stats.NumEntries != 10 {
		t.Errorf("NumEntries = %d, want 10", stats.NumEntries)
	}
	if stats.BucketCount <= 0 {
		t.Errorf("BucketCount = %d, want > 0", stats.BucketCount)
	}
	if stats.MemoryUsage <= 0 {
		t.Errorf("MemoryUsage = %d, want > 0", stats.MemoryUsage)
	}
}

// TestRowIndexUpdate tests updating existing entries
func TestRowIndexUpdate(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_update.lance")

	fields := []arrow.Field{
		arrow.NewField("id", arrow.PrimInt64(), false),
	}
	schema := arrow.NewSchema(fields, nil)

	writer, err := NewRowIndexWriter(filename, schema, format.V1_1, nil)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	builder := arrow.NewRecordBatchBuilder(schema)
	idBuilder := builder.Field(0).(*arrow.Int64Builder)
	idBuilder.Append(1)

	batch, err := builder.NewBatch()
	if err != nil {
		t.Fatalf("Failed to create record batch: %v", err)
	}

	if err := writer.WriteRecordBatch(batch); err != nil {
		t.Fatalf("Failed to write batch: %v", err)
	}

	// Insert then update
	if err := writer.AddRowID("doc1", 0); err != nil {
		t.Fatalf("AddRowID(doc1, 0) failed: %v", err)
	}
	if err := writer.AddRowID("doc1", 100); err != nil {
		t.Fatalf("AddRowID(doc1, 100) update failed: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// Read and verify updated value
	reader, err := NewRowIndexReader(filename)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Close()

	rowIdx, err := reader.LookupRowID("doc1")
	if err != nil {
		t.Fatalf("LookupRowID(doc1) failed: %v", err)
	}

	if rowIdx != 100 {
		t.Errorf("LookupRowID(doc1) = %d, want 100 (updated value)", rowIdx)
	}
}
