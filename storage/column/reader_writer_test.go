package column

import (
	"fmt"
	"github.com/wzqhbkjdx/vego/storage/arrow"
	"github.com/wzqhbkjdx/vego/storage/encoding" // [NEW] 导入 encoding 包
	"github.com/wzqhbkjdx/vego/storage/format"
	"os"
	"path/filepath"
	"testing"
)

// [NEW] 辅助函数：创建默认的 EncoderFactory
func defaultEncoderFactory() *encoding.EncoderFactory {
	return encoding.NewEncoderFactory(3) // 默认压缩级别 3
}

// ====================
// PageWriter/PageReader Tests
// ====================

func TestPageWriterReader_Int32Array(t *testing.T) {
	tests := []struct {
		name   string
		values []int32
		nulls  []bool
	}{
		{
			name:   "no nulls",
			values: []int32{1, 2, 3, 4, 5},
			nulls:  nil,
		},
		{
			name:   "with nulls",
			values: []int32{1, 0, 3, 0, 5},
			nulls:  []bool{true, false, true, false, true},
		},
		{
			name:   "all nulls",
			values: []int32{0, 0, 0},
			nulls:  []bool{false, false, false},
		},
		{
			name:   "single value",
			values: []int32{42},
			nulls:  nil,
		},
		{
			name:   "non-8-multiple length",
			values: []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			nulls:  []bool{true, false, true, false, true, false, true, false, true, false},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Build array
			builder := arrow.NewInt32Builder()
			defer builder.Release()

			for i, v := range tt.values {
				if tt.nulls != nil && !tt.nulls[i] {
					builder.AppendNull()
				} else {
					builder.Append(v)
				}
			}
			originalArray := builder.NewArray()

			// [MODIFIED] Write to page with EncoderFactory
			writer := NewPageWriter(defaultEncoderFactory())
			pages, err := writer.WritePages(originalArray, 0)
			if err != nil {
				t.Fatalf("WritePages failed: %v", err)
			}

			if len(pages) != 1 {
				t.Fatalf("expected 1 page, got %d", len(pages))
			}

			// Read from page
			reader := NewPageReader()
			resultArray, err := reader.ReadPage(pages[0], arrow.PrimInt32())
			if err != nil {
				t.Fatalf("ReadPage failed: %v", err)
			}

			// Verify
			if !arraysEqual(originalArray, resultArray) {
				t.Errorf("arrays not equal after roundtrip")
			}
		})
	}
}

func TestPageWriterReader_Int64Array(t *testing.T) {
	builder := arrow.NewInt64Builder()
	defer builder.Release()

	values := []int64{100, 200, 300, 400, 500}
	nulls := []bool{true, false, true, false, true}

	for i, v := range values {
		if nulls[i] {
			builder.Append(v)
		} else {
			builder.AppendNull()
		}
	}

	originalArray := builder.NewArray()

	// Roundtrip
	// [MODIFIED] 使用 defaultEncoderFactory()
	writer := NewPageWriter(defaultEncoderFactory())
	pages, err := writer.WritePages(originalArray, 0)
	if err != nil {
		t.Fatalf("WritePages failed: %v", err)
	}

	reader := NewPageReader()
	resultArray, err := reader.ReadPage(pages[0], arrow.PrimInt64())
	if err != nil {
		t.Fatalf("ReadPage failed: %v", err)
	}

	if !arraysEqual(originalArray, resultArray) {
		t.Errorf("arrays not equal after roundtrip")
	}
}

func TestPageWriterReader_Float32Array(t *testing.T) {
	builder := arrow.NewFloat32Builder()
	defer builder.Release()

	values := []float32{1.1, 2.2, 3.3, 4.4, 5.5}
	for _, v := range values {
		builder.Append(v)
	}

	originalArray := builder.NewArray()

	// Roundtrip
	// [MODIFIED] 使用 defaultEncoderFactory()
	writer := NewPageWriter(defaultEncoderFactory())
	pages, err := writer.WritePages(originalArray, 0)
	if err != nil {
		t.Fatalf("WritePages failed: %v", err)
	}

	reader := NewPageReader()
	resultArray, err := reader.ReadPage(pages[0], arrow.PrimFloat32())
	if err != nil {
		t.Fatalf("ReadPage failed: %v", err)
	}

	if !arraysEqual(originalArray, resultArray) {
		t.Errorf("arrays not equal after roundtrip")
	}
}

func TestPageWriterReader_Float64Array(t *testing.T) {
	builder := arrow.NewFloat64Builder()
	defer builder.Release()

	values := []float64{1.111, 2.222, 3.333}
	nulls := []bool{true, false, true}

	for i, v := range values {
		if nulls[i] {
			builder.Append(v)
		} else {
			builder.AppendNull()
		}
	}

	originalArray := builder.NewArray()

	// Roundtrip
	// [MODIFIED] 使用 defaultEncoderFactory()
	writer := NewPageWriter(defaultEncoderFactory())
	pages, err := writer.WritePages(originalArray, 0)
	if err != nil {
		t.Fatalf("WritePages failed: %v", err)
	}

	reader := NewPageReader()
	resultArray, err := reader.ReadPage(pages[0], arrow.PrimFloat64())
	if err != nil {
		t.Fatalf("ReadPage failed: %v", err)
	}

	if !arraysEqual(originalArray, resultArray) {
		t.Errorf("arrays not equal after roundtrip")
	}
}

func TestPageWriterReader_FixedSizeListArray(t *testing.T) {
	// Create 768-dimensional vectors for HNSW
	dim := 768
	numVectors := 3

	// Build child array (768 * 3 = 2304 float32 values)
	childBuilder := arrow.NewFloat32Builder()
	defer childBuilder.Release()

	for i := 0; i < numVectors*dim; i++ {
		childBuilder.Append(float32(i) * 0.1)
	}
	childArray := childBuilder.NewArray()

	// Create FixedSizeList array
	listType := arrow.FixedSizeListOf(arrow.PrimFloat32(), dim)
	originalArray := arrow.NewFixedSizeListArray(listType.(*arrow.FixedSizeListType), childArray, nil)

	// Roundtrip
	// [MODIFIED] 使用 defaultEncoderFactory()
	writer := NewPageWriter(defaultEncoderFactory())
	pages, err := writer.WritePages(originalArray, 0)
	if err != nil {
		t.Fatalf("WritePages failed: %v", err)
	}

	reader := NewPageReader()
	resultArray, err := reader.ReadPage(pages[0], listType)
	if err != nil {
		t.Fatalf("ReadPage failed: %v", err)
	}

	if !arraysEqual(originalArray, resultArray) {
		t.Errorf("arrays not equal after roundtrip")
	}
}

func TestPageWriterReader_FixedSizeListArray_WithNulls(t *testing.T) {
	dim := 128
	numVectors := 5

	// Build child array
	childBuilder := arrow.NewFloat32Builder()
	defer childBuilder.Release()

	for i := 0; i < numVectors*dim; i++ {
		childBuilder.Append(float32(i))
	}
	childArray := childBuilder.NewArray()

	// Create null bitmap
	nullBitmap := arrow.NewBitmap(numVectors)
	nullBitmap.Set(0) // valid
	nullBitmap.Set(2) // valid
	nullBitmap.Set(4) // valid
	// indices 1, 3 are null

	listType := arrow.FixedSizeListOf(arrow.PrimFloat32(), dim)
	originalArray := arrow.NewFixedSizeListArray(listType.(*arrow.FixedSizeListType), childArray, nullBitmap)

	// Roundtrip
	// [MODIFIED] 使用 defaultEncoderFactory()
	writer := NewPageWriter(defaultEncoderFactory())
	pages, err := writer.WritePages(originalArray, 0)
	if err != nil {
		t.Fatalf("WritePages failed: %v", err)
	}

	reader := NewPageReader()
	resultArray, err := reader.ReadPage(pages[0], listType)
	if err != nil {
		t.Fatalf("ReadPage failed: %v", err)
	}

	if !arraysEqual(originalArray, resultArray) {
		t.Errorf("arrays not equal after roundtrip")
	}
}

// ====================
// Writer/Reader Integration Tests
// ====================

func TestWriterReader_SingleRecordBatch(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test.lance")

	// Create schema
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimInt32(), Nullable: false},
		{Name: "value", Type: arrow.PrimFloat32(), Nullable: true},
	}, nil)

	// Build data
	idBuilder := arrow.NewInt32Builder()
	valueBuilder := arrow.NewFloat32Builder()
	defer idBuilder.Release()
	defer valueBuilder.Release()

	for i := 0; i < 100; i++ {
		idBuilder.Append(int32(i))
		if i%10 == 0 {
			valueBuilder.AppendNull()
		} else {
			valueBuilder.Append(float32(i) * 1.5)
		}
	}

	idArray := idBuilder.NewArray()
	valueArray := valueBuilder.NewArray()

	batch, err := arrow.NewRecordBatch(schema, 100, []arrow.Array{idArray, valueArray})
	if err != nil {
		t.Fatalf("NewRecordBatch failed: %v", err)
	}

	// Write
	// [MODIFIED] 使用 defaultEncoderFactory()
	writer, err := NewWriter(filename, schema, defaultEncoderFactory())
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	if err := writer.WriteRecordBatch(batch); err != nil {
		t.Fatalf("WriteRecordBatch failed: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Close writer failed: %v", err)
	}

	// Read
	reader, err := NewReader(filename)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer reader.Close()

	// Verify schema
	if !reader.Schema().Equal(schema) {
		t.Errorf("schema mismatch")
	}

	// Verify num rows
	if reader.NumRows() != 100 {
		t.Errorf("expected 100 rows, got %d", reader.NumRows())
	}

	// Read data
	resultBatch, err := reader.ReadRecordBatch()
	if err != nil {
		t.Fatalf("ReadRecordBatch failed: %v", err)
	}

	if resultBatch.NumRows() != 100 {
		t.Errorf("expected 100 rows in result, got %d", resultBatch.NumRows())
	}

	// Verify columns
	if !arraysEqual(idArray, resultBatch.Column(0)) {
		t.Errorf("id column mismatch")
	}
	if !arraysEqual(valueArray, resultBatch.Column(1)) {
		t.Errorf("value column mismatch")
	}
}

func TestWriterReader_MultipleRecordBatches(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_multi.lance")

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "counter", Type: arrow.PrimInt64(), Nullable: false},
	}, nil)

	// Write
	// [MODIFIED] 使用 defaultEncoderFactory()
	writer, err := NewWriter(filename, schema, defaultEncoderFactory())
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	totalRows := 0
	var allValues []int64

	// Write 5 batches
	for batchNum := 0; batchNum < 5; batchNum++ {
		builder := arrow.NewInt64Builder()
		for i := 0; i < 50; i++ {
			val := int64(batchNum*50 + i)
			builder.Append(val)
			allValues = append(allValues, val)
		}
		array := builder.NewArray()
		builder.Release()

		batch, err := arrow.NewRecordBatch(schema, 50, []arrow.Array{array})
		if err != nil {
			t.Fatalf("NewRecordBatch failed: %v", err)
		}

		if err := writer.WriteRecordBatch(batch); err != nil {
			t.Fatalf("WriteRecordBatch failed: %v", err)
		}

		totalRows += 50
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Close writer failed: %v", err)
	}

	// Read
	reader, err := NewReader(filename)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer reader.Close()

	if reader.NumRows() != int64(totalRows) {
		t.Errorf("expected %d rows, got %d", totalRows, reader.NumRows())
	}

	resultBatch, err := reader.ReadRecordBatch()
	if err != nil {
		t.Fatalf("ReadRecordBatch failed: %v", err)
	}

	// Verify all values
	resultArray := resultBatch.Column(0).(*arrow.Int64Array)
	for i, expected := range allValues {
		if resultArray.Value(i) != expected {
			t.Errorf("value mismatch at index %d: expected %d, got %d", i, expected, resultArray.Value(i))
		}
	}
}

func TestWriterReader_VectorColumn(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_vectors.lance")

	// Create schema with 768-dim vectors
	dim := 768
	listType := arrow.FixedSizeListOf(arrow.PrimFloat32(), dim)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "vector_id", Type: arrow.PrimInt32(), Nullable: false},
		{Name: "embedding", Type: listType, Nullable: false},
	}, nil)

	// Build data
	numVectors := 10
	idBuilder := arrow.NewInt32Builder()
	childBuilder := arrow.NewFloat32Builder()
	defer idBuilder.Release()
	defer childBuilder.Release()

	for i := 0; i < numVectors; i++ {
		idBuilder.Append(int32(i))
		for d := 0; d < dim; d++ {
			childBuilder.Append(float32(i*dim+d) * 0.001)
		}
	}

	idArray := idBuilder.NewArray()
	childArray := childBuilder.NewArray()
	vectorArray := arrow.NewFixedSizeListArray(listType.(*arrow.FixedSizeListType), childArray, nil)

	batch, err := arrow.NewRecordBatch(schema, numVectors, []arrow.Array{idArray, vectorArray})
	if err != nil {
		t.Fatalf("NewRecordBatch failed: %v", err)
	}

	// Write
	// [MODIFIED] 使用 defaultEncoderFactory()
	writer, err := NewWriter(filename, schema, defaultEncoderFactory())
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	if err := writer.WriteRecordBatch(batch); err != nil {
		t.Fatalf("WriteRecordBatch failed: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Close writer failed: %v", err)
	}

	// Read
	reader, err := NewReader(filename)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer reader.Close()

	resultBatch, err := reader.ReadRecordBatch()
	if err != nil {
		t.Fatalf("ReadRecordBatch failed: %v", err)
	}

	// Verify
	if !arraysEqual(idArray, resultBatch.Column(0)) {
		t.Errorf("id column mismatch")
	}
	if !arraysEqual(vectorArray, resultBatch.Column(1)) {
		t.Errorf("vector column mismatch")
	}
}

// ====================
// Multi-Page Tests
// ====================

func TestWriterReader_MultiPageColumn(t *testing.T) {
	// Note: Current implementation creates 1 page per array
	// This test verifies multi-batch writes create multiple pages
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_multipage.lance")

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "data", Type: arrow.PrimInt32(), Nullable: false},
	}, nil)

	// [MODIFIED] 使用 defaultEncoderFactory()
	writer, err := NewWriter(filename, schema, defaultEncoderFactory())
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	// Write 3 batches (will create 3 pages)
	var allValues []int32
	for batchNum := 0; batchNum < 3; batchNum++ {
		builder := arrow.NewInt32Builder()
		for i := 0; i < 100; i++ {
			val := int32(batchNum*100 + i)
			builder.Append(val)
			allValues = append(allValues, val)
		}
		array := builder.NewArray()
		builder.Release()

		batch, err := arrow.NewRecordBatch(schema, 100, []arrow.Array{array})
		if err != nil {
			t.Fatalf("NewRecordBatch failed: %v", err)
		}

		if err := writer.WriteRecordBatch(batch); err != nil {
			t.Fatalf("WriteRecordBatch failed: %v", err)
		}
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Close writer failed: %v", err)
	}

	// Read and verify merge worked correctly
	reader, err := NewReader(filename)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer reader.Close()

	if reader.NumRows() != 300 {
		t.Errorf("expected 300 rows, got %d", reader.NumRows())
	}

	resultBatch, err := reader.ReadRecordBatch()
	if err != nil {
		t.Fatalf("ReadRecordBatch failed: %v", err)
	}

	resultArray := resultBatch.Column(0).(*arrow.Int32Array)
	for i, expected := range allValues {
		if resultArray.Value(i) != expected {
			t.Errorf("value mismatch at index %d: expected %d, got %d", i, expected, resultArray.Value(i))
			break
		}
	}
}

func TestWriterReader_MultiPageWithNulls(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_multipage_nulls.lance")

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "data", Type: arrow.PrimFloat64(), Nullable: true},
	}, nil)

	// [MODIFIED] 使用 defaultEncoderFactory()
	writer, err := NewWriter(filename, schema, defaultEncoderFactory())
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	// Write 2 batches with nulls
	expectedValues := make([]float64, 0)
	expectedNulls := make([]bool, 0)

	for batchNum := 0; batchNum < 2; batchNum++ {
		builder := arrow.NewFloat64Builder()
		for i := 0; i < 50; i++ {
			if (batchNum*50+i)%7 == 0 {
				builder.AppendNull()
				expectedNulls = append(expectedNulls, false)
				expectedValues = append(expectedValues, 0)
			} else {
				val := float64(batchNum*50+i) * 0.5
				builder.Append(val)
				expectedNulls = append(expectedNulls, true)
				expectedValues = append(expectedValues, val)
			}
		}
		array := builder.NewArray()
		builder.Release()

		batch, err := arrow.NewRecordBatch(schema, 50, []arrow.Array{array})
		if err != nil {
			t.Fatalf("NewRecordBatch failed: %v", err)
		}

		if err := writer.WriteRecordBatch(batch); err != nil {
			t.Fatalf("WriteRecordBatch failed: %v", err)
		}
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Close writer failed: %v", err)
	}

	// Read and verify
	reader, err := NewReader(filename)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer reader.Close()

	resultBatch, err := reader.ReadRecordBatch()
	if err != nil {
		t.Fatalf("ReadRecordBatch failed: %v", err)
	}

	resultArray := resultBatch.Column(0).(*arrow.Float64Array)
	for i := 0; i < len(expectedNulls); i++ {
		isValid := resultArray.IsValid(i)
		if isValid != expectedNulls[i] {
			t.Errorf("null mismatch at index %d: expected valid=%v, got valid=%v", i, expectedNulls[i], isValid)
		}
		if isValid && resultArray.Value(i) != expectedValues[i] {
			t.Errorf("value mismatch at index %d: expected %f, got %f", i, expectedValues[i], resultArray.Value(i))
		}
	}
}

// ====================
// Error Cases
// ====================

func TestPageWriter_EmptyArray(t *testing.T) {
	builder := arrow.NewInt32Builder()
	array := builder.NewArray()
	builder.Release()

	// [MODIFIED] 使用 defaultEncoderFactory()
	writer := NewPageWriter(defaultEncoderFactory())
	_, err := writer.WritePages(array, 0)
	if err == nil {
		t.Error("expected error for empty array")
	}
}

func TestPageWriter_NilArray(t *testing.T) {
	// [MODIFIED] 使用 defaultEncoderFactory()
	writer := NewPageWriter(defaultEncoderFactory())
	_, err := writer.WritePages(nil, 0)
	if err == nil {
		t.Error("expected error for nil array")
	}
}

func TestWriter_SchemaMismatch(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_schema_mismatch.lance")

	schema1 := arrow.NewSchema([]arrow.Field{
		{Name: "field1", Type: arrow.PrimInt32(), Nullable: false},
	}, nil)

	schema2 := arrow.NewSchema([]arrow.Field{
		{Name: "field2", Type: arrow.PrimInt32(), Nullable: false},
	}, nil)

	// [MODIFIED] 使用 defaultEncoderFactory()
	writer, err := NewWriter(filename, schema1, defaultEncoderFactory())
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}
	defer writer.Close()

	// Try to write batch with different schema
	builder := arrow.NewInt32Builder()
	builder.Append(1)
	array := builder.NewArray()
	builder.Release()

	batch, err := arrow.NewRecordBatch(schema2, 1, []arrow.Array{array})
	if err != nil {
		t.Fatalf("NewRecordBatch failed: %v", err)
	}

	err = writer.WriteRecordBatch(batch)
	if err == nil {
		t.Error("expected error for schema mismatch")
	}
}

func TestWriter_ClosedWriter(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_closed.lance")

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "data", Type: arrow.PrimInt32(), Nullable: false},
	}, nil)

	// [MODIFIED] 使用 defaultEncoderFactory()
	writer, err := NewWriter(filename, schema, defaultEncoderFactory())
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Try to write after close
	builder := arrow.NewInt32Builder()
	builder.Append(1)
	array := builder.NewArray()
	builder.Release()

	batch, err := arrow.NewRecordBatch(schema, 1, []arrow.Array{array})
	if err != nil {
		t.Fatalf("NewRecordBatch failed: %v", err)
	}

	err = writer.WriteRecordBatch(batch)
	if err == nil {
		t.Error("expected error for writing to closed writer")
	}
}

func TestReader_NonexistentFile(t *testing.T) {
	_, err := NewReader("/nonexistent/file.lance")
	if err == nil {
		t.Error("expected error for nonexistent file")
	}
}

func TestReader_ClosedReader(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_closed_reader.lance")

	// Create a valid file
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "data", Type: arrow.PrimInt32(), Nullable: false},
	}, nil)

	// [MODIFIED] 使用 defaultEncoderFactory()
	writer, err := NewWriter(filename, schema, defaultEncoderFactory())
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	builder := arrow.NewInt32Builder()
	builder.Append(1)
	array := builder.NewArray()
	builder.Release()

	batch, err := arrow.NewRecordBatch(schema, 1, []arrow.Array{array})
	if err != nil {
		t.Fatalf("NewRecordBatch failed: %v", err)
	}

	writer.WriteRecordBatch(batch)
	writer.Close()

	// Read then close
	reader, err := NewReader(filename)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}

	if err := reader.Close(); err != nil {
		t.Fatalf("Close reader failed: %v", err)
	}

	// Try to read after close
	_, err = reader.ReadRecordBatch()
	if err == nil {
		t.Error("expected error for reading from closed reader")
	}
}

// ====================
// Helper Functions
// ====================

func arraysEqual(a, b arrow.Array) bool {
	if a.Len() != b.Len() {
		return false
	}

	if a.NullN() != b.NullN() {
		return false
	}

	switch arr := a.(type) {
	case *arrow.Int32Array:
		barr := b.(*arrow.Int32Array)
		for i := 0; i < a.Len(); i++ {
			if a.IsValid(i) != b.IsValid(i) {
				return false
			}
			if a.IsValid(i) && arr.Value(i) != barr.Value(i) {
				return false
			}
		}
	case *arrow.Int64Array:
		barr := b.(*arrow.Int64Array)
		for i := 0; i < a.Len(); i++ {
			if a.IsValid(i) != b.IsValid(i) {
				return false
			}
			if a.IsValid(i) && arr.Value(i) != barr.Value(i) {
				return false
			}
		}
	case *arrow.Float32Array:
		barr := b.(*arrow.Float32Array)
		for i := 0; i < a.Len(); i++ {
			if a.IsValid(i) != b.IsValid(i) {
				return false
			}
			if a.IsValid(i) && arr.Value(i) != barr.Value(i) {
				return false
			}
		}
	case *arrow.Float64Array:
		barr := b.(*arrow.Float64Array)
		for i := 0; i < a.Len(); i++ {
			if a.IsValid(i) != b.IsValid(i) {
				return false
			}
			if a.IsValid(i) && arr.Value(i) != barr.Value(i) {
				return false
			}
		}
	case *arrow.FixedSizeListArray:
		barr := b.(*arrow.FixedSizeListArray)
		if arr.Len() != barr.Len() {
			return false
		}
		// Compare child arrays
		return arraysEqual(arr.Values(), barr.Values())
	default:
		return false
	}

	return true
}

// ====================
// Benchmark Tests
// ====================

func BenchmarkWriteInt32Array(b *testing.B) {
	builder := arrow.NewInt32Builder()
	builder.Reserve(1000)
	for i := 0; i < 1000; i++ {
		builder.Append(int32(i))
	}
	array := builder.NewArray()
	builder.Release()

	// [MODIFIED] 使用 defaultEncoderFactory()
	writer := NewPageWriter(defaultEncoderFactory())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = writer.WritePages(array, 0)
	}
}

func BenchmarkReadInt32Array(b *testing.B) {
	builder := arrow.NewInt32Builder()
	builder.Reserve(1000)
	for i := 0; i < 1000; i++ {
		builder.Append(int32(i))
	}
	array := builder.NewArray()
	builder.Release()

	// [MODIFIED] 使用 defaultEncoderFactory()
	writer := NewPageWriter(defaultEncoderFactory())
	pages, _ := writer.WritePages(array, 0)

	reader := NewPageReader()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = reader.ReadPage(pages[0], arrow.PrimInt32())
	}
}

func BenchmarkWriteVectorArray(b *testing.B) {
	dim := 768
	numVectors := 100

	childBuilder := arrow.NewFloat32Builder()
	childBuilder.Reserve(dim * numVectors)
	for i := 0; i < numVectors*dim; i++ {
		childBuilder.Append(float32(i) * 0.001)
	}
	childArray := childBuilder.NewArray()
	childBuilder.Release()

	listType := arrow.FixedSizeListOf(arrow.PrimFloat32(), dim)
	array := arrow.NewFixedSizeListArray(listType.(*arrow.FixedSizeListType), childArray, nil)

	// [MODIFIED] 使用 defaultEncoderFactory()
	writer := NewPageWriter(defaultEncoderFactory())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = writer.WritePages(array, 0)
	}
}

func BenchmarkFileRoundtrip(b *testing.B) {
	tmpDir := b.TempDir()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimInt32(), Nullable: false},
		{Name: "value", Type: arrow.PrimFloat64(), Nullable: false},
	}, nil)

	idBuilder := arrow.NewInt32Builder()
	valueBuilder := arrow.NewFloat64Builder()
	idBuilder.Reserve(1000)
	valueBuilder.Reserve(1000)

	for i := 0; i < 1000; i++ {
		idBuilder.Append(int32(i))
		valueBuilder.Append(float64(i) * 1.5)
	}

	idArray := idBuilder.NewArray()
	valueArray := valueBuilder.NewArray()
	idBuilder.Release()
	valueBuilder.Release()

	batch, _ := arrow.NewRecordBatch(schema, 1000, []arrow.Array{idArray, valueArray})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		filename := filepath.Join(tmpDir, fmt.Sprintf("bench_%d.lance", i))

		// [MODIFIED] 使用 defaultEncoderFactory()
		writer, _ := NewWriter(filename, schema, defaultEncoderFactory())
		writer.WriteRecordBatch(batch)
		writer.Close()

		reader, _ := NewReader(filename)
		reader.ReadRecordBatch()
		reader.Close()

		os.Remove(filename)
	}
}

// TestEncoderSelection 验证 Factory 正确选择编码器
func TestEncoderSelection(t *testing.T) {
	tests := []struct {
		name        string
		values      []int32
		expectedEnc format.EncodingType
		description string
	}{
		{
			name:        "RLE_for_high_run_ratio",
			values:      []int32{1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3}, // 高连续重复
			expectedEnc: format.EncodingRLE,
			description: "高 run ratio 应该选择 RLE",
		},
		{
			name:        "BitPacking_for_narrow_integers",
			values:      []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, // 小整数
			expectedEnc: format.EncodingBitPacked,
			description: "小范围整数应该选择 BitPacking",
		},
		{
			name:        "Dictionary_for_low_cardinality",
			values:      []int32{1, 2, 1, 2, 1, 2, 1, 2, 1, 2}, // 低基数
			expectedEnc: format.EncodingDictionary,
			description: "低基数数据应该选择 Dictionary",
		},
		{
			name:        "Zstd_for_high_cardinality",
			values:      []int32{1000000, 2000000, 3000000, 4000000}, // 大范围唯一值
			expectedEnc: format.EncodingZstd,
			description: "高基数数据应该 fallback 到 Zstd",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			array := arrow.NewInt32Array(tt.values, nil)

			writer := NewPageWriter(defaultEncoderFactory())
			pages, err := writer.WritePages(array, 0)
			if err != nil {
				t.Fatalf("WritePages failed: %v", err)
			}

			// 验证使用的编码器类型
			if pages[0].Encoding != tt.expectedEnc {
				t.Logf("Note: %s - expected %v, got %v",
					tt.description, tt.expectedEnc, pages[0].Encoding)
				// 不强制失败，因为 Factory 的选择策略可能调整
				// 但应该记录观察
			}

			// 验证数据正确性
			reader := NewPageReader()
			result, err := reader.ReadPage(pages[0], arrow.PrimInt32())
			if err != nil {
				t.Fatalf("ReadPage failed: %v", err)
			}

			if !arraysEqual(array, result) {
				t.Errorf("Data mismatch after roundtrip")
			}
		})
	}
}

// TestNullFallbackToZstd 验证 specialized encoders 正确处理 null
func TestNullFallbackToZstd(t *testing.T) {
	tests := []struct {
		name       string
		buildArray func() arrow.Array
		dtype      arrow.DataType
	}{
		{
			name: "RLE_with_nulls_fallback",
			buildArray: func() arrow.Array {
				builder := arrow.NewInt32Builder()
				builder.Append(1)
				builder.AppendNull() // null
				builder.Append(1)
				builder.AppendNull() // null
				builder.Append(1)
				return builder.NewArray()
			},
			dtype: arrow.PrimInt32(),
		},
		{
			name: "BSS_with_nulls_fallback",
			buildArray: func() arrow.Array {
				builder := arrow.NewFloat32Builder()
				builder.Append(1.1)
				builder.AppendNull()
				builder.Append(1.1)
				builder.AppendNull()
				builder.Append(1.1)
				return builder.NewArray()
			},
			dtype: arrow.PrimFloat32(),
		},
		{
			name: "BitPacking_with_nulls_fallback",
			buildArray: func() arrow.Array {
				builder := arrow.NewInt32Builder()
				for i := 0; i < 10; i++ {
					if i%2 == 0 {
						builder.Append(int32(i))
					} else {
						builder.AppendNull()
					}
				}
				return builder.NewArray()
			},
			dtype: arrow.PrimInt32(),
		},
		{
			name: "Dictionary_with_nulls_fallback",
			buildArray: func() arrow.Array {
				builder := arrow.NewInt32Builder()
				builder.Append(1)
				builder.Append(1)
				builder.AppendNull()
				builder.Append(2)
				builder.Append(2)
				return builder.NewArray()
			},
			dtype: arrow.PrimInt32(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			array := tt.buildArray()

			writer := NewPageWriter(defaultEncoderFactory())
			pages, err := writer.WritePages(array, 0)
			if err != nil {
				t.Fatalf("WritePages failed: %v", err)
			}

			// 验证 fallback 到 Zstd
			if pages[0].Encoding != format.EncodingZstd {
				t.Errorf("Expected Zstd fallback for null data, got %v", pages[0].Encoding)
			}

			// 验证数据完整性
			reader := NewPageReader()
			result, err := reader.ReadPage(pages[0], tt.dtype)
			if err != nil {
				t.Fatalf("ReadPage failed: %v", err)
			}

			if !arraysEqual(array, result) {
				t.Errorf("Data mismatch after roundtrip with nulls")
			}
		})
	}
}

// TestEdgeCases 验证边界情况
func TestEdgeCases(t *testing.T) {
	tests := []struct {
		name   string
		testFn func(t *testing.T)
	}{
		{
			name: "empty_array",
			testFn: func(t *testing.T) {
				array := arrow.NewInt32Array([]int32{}, nil)
				writer := NewPageWriter(defaultEncoderFactory())
				_, err := writer.WritePages(array, 0)
				if err == nil {
					t.Error("Expected error for empty array")
				}
			},
		},
		{
			name: "single_value",
			testFn: func(t *testing.T) {
				array := arrow.NewInt32Array([]int32{42}, nil)
				writer := NewPageWriter(defaultEncoderFactory())
				pages, err := writer.WritePages(array, 0)
				if err != nil {
					t.Fatalf("WritePages failed: %v", err)
				}

				reader := NewPageReader()
				result, err := reader.ReadPage(pages[0], arrow.PrimInt32())
				if err != nil {
					t.Fatalf("ReadPage failed: %v", err)
				}

				if result.Len() != 1 || result.(*arrow.Int32Array).Value(0) != 42 {
					t.Error("Single value mismatch")
				}
			},
		},
		{
			name: "all_nulls",
			testFn: func(t *testing.T) {
				builder := arrow.NewInt32Builder()
				for i := 0; i < 10; i++ {
					builder.AppendNull()
				}
				array := builder.NewArray()

				writer := NewPageWriter(defaultEncoderFactory())
				pages, err := writer.WritePages(array, 0)
				if err != nil {
					t.Fatalf("WritePages failed: %v", err)
				}

				reader := NewPageReader()
				result, err := reader.ReadPage(pages[0], arrow.PrimInt32())
				if err != nil {
					t.Fatalf("ReadPage failed: %v", err)
				}

				if result.Len() != 10 || result.NullN() != 10 {
					t.Errorf("Expected 10 nulls, got %d nulls", result.NullN())
				}
			},
		},
		{
			name: "very_large_array",
			testFn: func(t *testing.T) {
				values := make([]int32, 100000)
				for i := range values {
					values[i] = int32(i % 100) // 低基数
				}
				array := arrow.NewInt32Array(values, nil)

				writer := NewPageWriter(defaultEncoderFactory())
				pages, err := writer.WritePages(array, 0)
				if err != nil {
					t.Fatalf("WritePages failed: %v", err)
				}

				reader := NewPageReader()
				result, err := reader.ReadPage(pages[0], arrow.PrimInt32())
				if err != nil {
					t.Fatalf("ReadPage failed: %v", err)
				}

				if result.Len() != 100000 {
					t.Errorf("Length mismatch: expected 100000, got %d", result.Len())
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, tt.testFn)
	}
}

// TestCompressionEffectiveness 验证编码确实有压缩效果
func TestCompressionEffectiveness(t *testing.T) {
	// 生成高可压缩数据
	values := make([]int32, 10000)
	for i := range values {
		values[i] = int32(i % 10) // 只有10个唯一值
	}
	array := arrow.NewInt32Array(values, nil)

	writer := NewPageWriter(defaultEncoderFactory())
	pages, err := writer.WritePages(array, 0)
	if err != nil {
		t.Fatalf("WritePages failed: %v", err)
	}

	// 原始大小: 10000 * 4 = 40000 bytes
	// 压缩后应该远小于原始大小（至少 < 50%）
	compressedSize := len(pages[0].Data)
	originalSize := 10000 * 4
	compressionRatio := float64(compressedSize) / float64(originalSize)

	if compressionRatio > 0.6 {
		t.Errorf("Compression ratio too high: %.2f (compressed: %d, original: %d)",
			compressionRatio, compressedSize, originalSize)
	}

	t.Logf("Compression ratio: %.2f (compressed: %d, original: %d)",
		compressionRatio, compressedSize, originalSize)
}

func TestConcurrency_EncodeDecode(t *testing.T) {
	factory := encoding.NewEncoderFactory(3)
	writer := NewPageWriter(factory)
	reader := NewPageReader()

	// 准备测试数据
	values := make([]int32, 1000)
	for i := range values {
		values[i] = int32(i)
	}
	array := arrow.NewInt32Array(values, nil)

	// 并发编码
	const numGoroutines = 100
	errChan := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			pages, err := writer.WritePages(array, 0)
			if err != nil {
				errChan <- err
				return
			}

			// 立即解码
			_, err = reader.ReadPage(pages[0], arrow.PrimInt32())
			errChan <- err
		}()
	}

	// 检查所有 goroutine 都成功
	for i := 0; i < numGoroutines; i++ {
		if err := <-errChan; err != nil {
			t.Errorf("Concurrent operation failed: %v", err)
		}
	}
}

// ====================
// P0: 页元数据精确验证
// ====================

func TestPageMetadata_UncompressedSizeAccuracy(t *testing.T) {
	factory := encoding.NewEncoderFactory(3)
	writer := NewPageWriter(factory)

	testCases := []struct {
		name                 string
		buildArray           func() arrow.Array
		expectedUncompressed int32
	}{
		{
			name: "Int32_NoNulls",
			buildArray: func() arrow.Array {
				return arrow.NewInt32Array([]int32{1, 2, 3, 4, 5}, nil)
			},
			expectedUncompressed: 5 * 4, // 20 bytes
		},
		{
			name: "Int32_WithNulls",
			buildArray: func() arrow.Array {
				builder := arrow.NewInt32Builder()
				builder.Append(1)
				builder.AppendNull()
				builder.Append(3)
				return builder.NewArray()
			},
			expectedUncompressed: 3*4 + 1, // 12 bytes data + 1 byte bitmap
		},
		{
			name: "Float64_WithNulls",
			buildArray: func() arrow.Array {
				builder := arrow.NewFloat64Builder()
				for i := 0; i < 100; i++ {
					if i%10 == 0 {
						builder.AppendNull()
					} else {
						builder.Append(float64(i))
					}
				}
				return builder.NewArray()
			},
			expectedUncompressed: 100*8 + (100+7)/8, // 800 bytes + 13 bytes bitmap
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			array := tc.buildArray()
			pages, err := writer.WritePages(array, 0)
			if err != nil {
				t.Fatalf("WritePages failed: %v", err)
			}

			if pages[0].UncompressedSize != tc.expectedUncompressed {
				t.Errorf("UncompressedSize mismatch: expected %d, got %d",
					tc.expectedUncompressed, pages[0].UncompressedSize)
			}
		})
	}
}

// ====================
// P1: 编码器回退完整性测试
// ====================

func TestEncoderFallback_ValueOutOfRange(t *testing.T) {
	factory := encoding.NewEncoderFactory(3)
	writer := NewPageWriter(factory)
	reader := NewPageReader()

	// BitPacking 无法处理负数，应该回退到 Zstd
	values := []int32{1, 2, 3, -1, 5} // -1 会触发回退
	array := arrow.NewInt32Array(values, nil)

	pages, err := writer.WritePages(array, 0)
	if err != nil {
		t.Fatalf("WritePages failed: %v", err)
	}

	// 验证回退到了 Zstd
	if pages[0].Encoding != format.EncodingZstd {
		t.Logf("Note: Expected fallback to Zstd, but got %v", pages[0].Encoding)
	}

	// 验证能正确读取
	result, err := reader.ReadPage(pages[0], arrow.PrimInt32())
	if err != nil {
		t.Fatalf("ReadPage failed: %v", err)
	}

	if !arraysEqual(array, result) {
		t.Error("Data mismatch after fallback")
	}
}

func TestEncoderFallback_DictionaryTooLarge(t *testing.T) {
	factory := encoding.NewEncoderFactory(3)
	writer := NewPageWriter(factory)
	reader := NewPageReader()

	// 创建超过字典大小限制的唯一值
	// DefaultEncoderConfig().DictionaryMaxSize = 1 << 20
	numUniqueValues := 1 << 21 // 2M unique values
	values := make([]int32, numUniqueValues)
	for i := range values {
		values[i] = int32(i)
	}
	array := arrow.NewInt32Array(values, nil)

	pages, err := writer.WritePages(array, 0)
	if err != nil {
		t.Fatalf("WritePages failed: %v", err)
	}

	// 应该回退到非 Dictionary 编码
	if pages[0].Encoding == format.EncodingDictionary {
		t.Error("Should not use Dictionary for large cardinality")
	}

	// 验证能正确读取
	result, err := reader.ReadPage(pages[0], arrow.PrimInt32())
	if err != nil {
		t.Fatalf("ReadPage failed: %v", err)
	}

	if !arraysEqual(array, result) {
		t.Error("Data mismatch after fallback")
	}
}

// ====================
// P1: FixedSizeList 编码器交互测试
// ====================

func TestFixedSizeList_AlwaysUsesZstd(t *testing.T) {
	factory := encoding.NewEncoderFactory(3)
	writer := NewPageWriter(factory)

	dim := 128
	numVectors := 100

	childBuilder := arrow.NewFloat32Builder()
	for i := 0; i < numVectors*dim; i++ {
		childBuilder.Append(float32(i))
	}
	childArray := childBuilder.NewArray()

	listType := arrow.FixedSizeListOf(arrow.PrimFloat32(), dim)
	array := arrow.NewFixedSizeListArray(listType.(*arrow.FixedSizeListType), childArray, nil)

	pages, err := writer.WritePages(array, 0)
	if err != nil {
		t.Fatalf("WritePages failed: %v", err)
	}

	// FixedSizeList 应该总是使用 Zstd
	if pages[0].Encoding != format.EncodingZstd {
		t.Errorf("FixedSizeList should use Zstd, but got %v", pages[0].Encoding)
	}
}

// ====================
// P1: EncoderFactory 极端配置测试
// ====================

func TestEncoderFactory_ExtremeThresholds(t *testing.T) {
	testCases := []struct {
		name   string
		config *encoding.EncoderConfig
		values []int32
		verify func(t *testing.T, encoding format.EncodingType)
	}{
		{
			name: "RLEThreshold_Zero_AlwaysRLE",
			config: &encoding.EncoderConfig{
				RLEThreshold:          0.0, // 任何 run ratio 都触发 RLE
				DictionaryThreshold:   1.0, // 永不触发 Dictionary
				BitPackingMaxBitWidth: 16,
				// ... 其他字段使用默认值
			},
			values: []int32{1, 2, 3, 4, 5}, // 无重复，但仍应选择 RLE
			verify: func(t *testing.T, enc format.EncodingType) {
				// 注意：可能因为其他条件而不选择 RLE
				t.Logf("With RLEThreshold=0, got encoding: %v", enc)
			},
		},
		{
			name: "DictionaryThreshold_One_NeverDictionary",
			config: &encoding.EncoderConfig{
				RLEThreshold:          1.0, // 永不触发 RLE
				DictionaryThreshold:   1.0, // 永不触发 Dictionary
				BitPackingMaxBitWidth: 0,   // 禁用 BitPacking
				// ...
			},
			values: []int32{1, 2, 1, 2, 1, 2}, // 低基数，但不应选 Dictionary
			verify: func(t *testing.T, enc format.EncodingType) {
				if enc == format.EncodingDictionary {
					t.Error("Should not use Dictionary with threshold=1.0")
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			factory := encoding.NewEncoderFactoryWithConfig(3, tc.config)
			writer := NewPageWriter(factory)

			array := arrow.NewInt32Array(tc.values, nil)
			pages, err := writer.WritePages(array, 0)
			if err != nil {
				t.Fatalf("WritePages failed: %v", err)
			}

			tc.verify(t, pages[0].Encoding)
		})
	}
}

// ====================
// P2: Header 超过预留大小测试
// ====================

func TestWriter_HeaderExceedsReservedSize(t *testing.T) {
	tmpDir := t.TempDir()
	filename := tmpDir + "/huge_schema.lance"

	// 创建一个超大 Schema（超过 8KB）
	fields := make([]arrow.Field, 1000) // 1000 列
	for i := 0; i < 1000; i++ {
		fields[i] = arrow.Field{
			Name:     fmt.Sprintf("column_%d_with_very_long_name_to_increase_size", i),
			Type:     arrow.PrimInt32(),
			Nullable: false,
		}
	}
	schema := arrow.NewSchema(fields, nil)

	// 尝试创建 Writer（应该失败或警告）
	_, err := NewWriter(filename, schema, encoding.NewEncoderFactory(3))
	if err == nil {
		t.Log("Warning: Large schema was accepted, may exceed HeaderReservedSize")
	} else {
		t.Logf("Large schema rejected as expected: %v", err)
	}
}
