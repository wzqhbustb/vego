package column

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/wzqhbustb/vego/storage/arrow"
	"github.com/wzqhbustb/vego/storage/encoding"
	lerrors "github.com/wzqhbustb/vego/storage/errors"
	"github.com/wzqhbustb/vego/storage/format"
)

// ====================
// Test 6: readPagesSync - 同步多页读取
// ====================

func TestReadPagesSync(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "multi_page.lance")

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "data", Type: arrow.PrimInt32(), Nullable: false},
	}, nil)

	writer, err := NewWriter(filename, schema, encoding.NewEncoderFactory(3))
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	builder := arrow.NewInt32Builder()
	for i := 0; i < 100000; i++ {
		builder.Append(int32(i))
	}
	array := builder.NewArray()

	batch, _ := arrow.NewRecordBatch(schema, 100000, []arrow.Array{array})
	if err := writer.WriteRecordBatch(batch); err != nil {
		t.Fatalf("WriteRecordBatch failed: %v", err)
	}
	writer.Close()

	reader, err := NewReader(filename)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer reader.Close()

	result, err := reader.ReadRecordBatch()
	if err != nil {
		t.Fatalf("ReadRecordBatch failed: %v", err)
	}

	if result.NumRows() != 100000 {
		t.Errorf("Expected 100000 rows, got %d", result.NumRows())
	}

	resultArray := result.Column(0).(*arrow.Int32Array)
	for i := 0; i < 1000; i++ {
		if resultArray.Value(i) != int32(i) {
			t.Errorf("Data mismatch at %d: expected %d, got %d", i, i, resultArray.Value(i))
			break
		}
	}
}

func TestReadPagesSync_MultipleColumns(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "multi_col_multi_page.lance")

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimInt32(), Nullable: false},
		{Name: "value", Type: arrow.PrimFloat64(), Nullable: true},
	}, nil)

	writer, err := NewWriter(filename, schema, encoding.NewEncoderFactory(3))
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	for batchNum := 0; batchNum < 3; batchNum++ {
		idBuilder := arrow.NewInt32Builder()
		valueBuilder := arrow.NewFloat64Builder()

		for i := 0; i < 50000; i++ {
			idBuilder.Append(int32(batchNum*50000 + i))
			if i%10 == 0 {
				valueBuilder.AppendNull()
			} else {
				valueBuilder.Append(float64(i) * 1.5)
			}
		}

		batch, _ := arrow.NewRecordBatch(schema, 50000, []arrow.Array{
			idBuilder.NewArray(),
			valueBuilder.NewArray(),
		})
		if err := writer.WriteRecordBatch(batch); err != nil {
			t.Fatalf("WriteRecordBatch failed: %v", err)
		}
	}
	writer.Close()

	reader, err := NewReader(filename)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer reader.Close()

	if reader.NumRows() != 150000 {
		t.Errorf("Expected 150000 rows, got %d", reader.NumRows())
	}

	result, err := reader.ReadRecordBatch()
	if err != nil {
		t.Fatalf("ReadRecordBatch failed: %v", err)
	}

	valueCol := result.Column(1).(*arrow.Float64Array)
	expectedNulls := 150000 / 10
	if valueCol.NullN() != expectedNulls {
		t.Errorf("Expected %d nulls, got %d", expectedNulls, valueCol.NullN())
	}
}

// ====================
// Test 7: ColumnError - 验证结构化错误
// ====================

func TestColumnError_Structured(t *testing.T) {
	tests := []struct {
		name        string
		operation   string
		column      string
		message     string
		expectError bool
	}{
		{
			name:        "基本错误",
			operation:   "validate",
			column:      "test_col",
			message:     "array is nil",
			expectError: true,
		},
		{
			name:        "空列名",
			operation:   "read",
			column:      "",
			message:     "column not found",
			expectError: true,
		},
		{
			name:        "长操作名",
			operation:   "very_long_operation_name",
			column:      "col_with_long_name",
			message:     "something went wrong",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := newColumnError(tt.operation, tt.column, tt.message)

			if err == nil && tt.expectError {
				t.Error("Expected error but got nil")
				return
			}

			if !lerrors.Is(err, lerrors.ErrInvalidArgument) {
				t.Errorf("Expected error code ErrInvalidArgument")
			}

			errStr := err.Error()
			if tt.column != "" && !contains(errStr, tt.column) {
				t.Errorf("Error should contain column '%s': %s", tt.column, errStr)
			}
			if !contains(errStr, tt.operation) {
				t.Errorf("Error should contain operation '%s': %s", tt.operation, errStr)
			}
		})
	}
}

func TestColumnError_Context(t *testing.T) {
	err := newColumnError("write_column", "embedding", "unsupported data type")

	wrappedErr := lerrors.New(lerrors.ErrColumnNotFound).
		Op("batch_write").
		Context("batch_id", 123).
		Wrap(err).
		Build()

	if !lerrors.Is(wrappedErr, lerrors.ErrColumnNotFound) {
		t.Error("Wrapped error should preserve error code")
	}

	if !lerrors.Is(wrappedErr, lerrors.ErrInvalidArgument) {
		t.Error("Wrapped error should preserve original error code in chain")
	}
}

// ====================
// Test 8: DefaultSerializationOptions
// ====================

func TestDefaultSerializationOptions(t *testing.T) {
	opts := DefaultSerializationOptions()

	if opts.PageSize != format.DefaultPageSize {
		t.Errorf("Expected PageSize %d, got %d", format.DefaultPageSize, opts.PageSize)
	}

	if opts.Encoding != format.EncodingPlain {
		t.Errorf("Expected Encoding %v, got %v", format.EncodingPlain, opts.Encoding)
	}

	if opts.PageSize <= 0 {
		t.Error("PageSize should be positive")
	}

	if opts.PageSize > 100*1024*1024 {
		t.Error("PageSize seems unreasonably large")
	}
}

// ====================
// Test 9: 边界条件
// ====================

func TestBoundary_EmptyFile(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "empty.lance")

	file, err := os.Create(filename)
	if err != nil {
		t.Fatalf("Failed to create empty file: %v", err)
	}
	file.Close()

	_, err = NewReader(filename)
	if err == nil {
		t.Error("Expected error for empty file")
	}
}

func TestBoundary_TruncatedFile(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "truncated.lance")

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimInt32(), Nullable: false},
	}, nil)

	writer, _ := NewWriter(filename, schema, encoding.NewEncoderFactory(3))
	builder := arrow.NewInt32Builder()
	builder.Append(1)
	array := builder.NewArray()
	batch, _ := arrow.NewRecordBatch(schema, 1, []arrow.Array{array})
	writer.WriteRecordBatch(batch)
	writer.Close()

	fileInfo, _ := os.Stat(filename)
	file, _ := os.OpenFile(filename, os.O_WRONLY, 0644)
	file.Truncate(fileInfo.Size() - 100)
	file.Close()

	_, err := NewReader(filename)
	if err == nil {
		t.Error("Expected error for truncated file")
	}
}

func TestBoundary_CorruptedFooter(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "corrupted_footer.lance")

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimInt32(), Nullable: false},
	}, nil)

	writer, _ := NewWriter(filename, schema, encoding.NewEncoderFactory(3))
	builder := arrow.NewInt32Builder()
	builder.Append(1)
	array := builder.NewArray()
	batch, _ := arrow.NewRecordBatch(schema, 1, []arrow.Array{array})
	writer.WriteRecordBatch(batch)
	writer.Close()

	data, _ := os.ReadFile(filename)
	// Corrupt the version field in footer (offset 0 from footer start, i.e., last 32KB of file)
	// Footer starts at: fileSize - FooterSize (32768)
	// Version is at the beginning of footer (first 2 bytes)
	if len(data) > 32768 {
		footerStart := len(data) - 32768
		// Corrupt the version field (first 2 bytes of footer)
		data[footerStart] = 0xFF
		data[footerStart+1] = 0xFF
	}
	os.WriteFile(filename, data, 0644)

	_, err := NewReader(filename)
	if err == nil {
		t.Error("Expected error for corrupted footer checksum")
	}
}

func TestBoundary_NonExistentFile(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "does_not_exist.lance")

	os.Remove(filename)

	_, err := NewReader(filename)
	if err == nil {
		t.Error("Expected error for non-existent file")
	}
}

func TestBoundary_ZeroLengthArray(t *testing.T) {
	factory := encoding.NewEncoderFactory(3)
	writer := NewPageWriter(factory)

	builder := arrow.NewInt32Builder()
	array := builder.NewArray()

	_, err := writer.WritePages(array, 0)
	if err == nil {
		t.Error("Expected error for zero-length array")
	}
}

func TestBoundary_VeryLargePageSize(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "large_page.lance")

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimInt32(), Nullable: false},
	}, nil)

	writer, err := NewWriter(filename, schema, encoding.NewEncoderFactory(3))
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	builder := arrow.NewInt32Builder()
	for i := 0; i < 10; i++ {
		builder.Append(int32(i))
	}
	array := builder.NewArray()

	batch, _ := arrow.NewRecordBatch(schema, 10, []arrow.Array{array})
	if err := writer.WriteRecordBatch(batch); err != nil {
		t.Fatalf("WriteRecordBatch failed: %v", err)
	}
	writer.Close()

	reader, err := NewReader(filename)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer reader.Close()

	result, err := reader.ReadRecordBatch()
	if err != nil {
		t.Fatalf("ReadRecordBatch failed: %v", err)
	}

	if result.NumRows() != 10 {
		t.Errorf("Expected 10 rows, got %d", result.NumRows())
	}
}

func contains(s, substr string) bool {
	return bytes.Contains([]byte(s), []byte(substr))
}
