package column

import (
	"fmt"
	"github.com/wzqhbkjdx/vego/storage/arrow"
	lerrors "github.com/wzqhbkjdx/vego/storage/errors"
	"github.com/wzqhbkjdx/vego/storage/format"
)

// ColumnMetadata contains metadata about a column in the file
type ColumnMetadata struct {
	Index     int32               // Column index in schema
	Name      string              // Column name
	DataType  arrow.DataType      // Column data type
	NumPages  int32               // Number of pages for this column
	NumValues int64               // Total number of values
	Encoding  format.EncodingType // Encoding type used
}

// PageMetadata contains metadata about a single page
type PageMetadata struct {
	ColumnIndex      int32 // Column this page belongs to
	PageNum          int32 // Page number within column (0-based)
	Offset           int64 // Byte offset in file
	CompressedSize   int32 // Size of compressed data
	UncompressedSize int32 // Size of uncompressed data
	NumValues        int32 // Number of values in this page
}

// SerializationOptions controls how data is serialized
type SerializationOptions struct {
	PageSize int32               // Target page size in bytes (default: 1MB)
	Encoding format.EncodingType // Encoding to use (default: Plain)
}

// DefaultSerializationOptions returns default serialization options
func DefaultSerializationOptions() SerializationOptions {
	return SerializationOptions{
		PageSize: format.DefaultPageSize,
		Encoding: format.EncodingPlain,
	}
}

// Error types
type ColumnError struct {
	Op      string // Operation that failed
	Column  string // Column name or index
	Message string // Error message
}

func (e *ColumnError) Error() string {
	if e.Column != "" {
		return fmt.Sprintf("column %s: %s: %s", e.Column, e.Op, e.Message)
	}
	return fmt.Sprintf("column: %s: %s", e.Op, e.Message)
}

// newColumnError 创建列错误（使用新的错误系统）
func newColumnError(op, column, message string) error {
	return lerrors.New(lerrors.ErrInvalidArgument).
		Op(op).
		Context("column", column).
		Context("message", message).
		Build()
}

// validateArray validates that an array is suitable for serialization
func validateArray(array arrow.Array, field arrow.Field) error {
	if array == nil {
		return newColumnError("validate", field.Name, "array is nil")
	}

	if array.Len() == 0 {
		return newColumnError("validate", field.Name, "array is empty")
	}

	if array.DataType().ID() != field.Type.ID() {
		return lerrors.TypeMismatch("validate_array", field.Name,
			field.Type.Name(), array.DataType().Name())
	}

	return nil
}

// calculateNumPages calculates how many pages are needed for an array
func calculateNumPages(array arrow.Array, pageSize int32) int32 {
	// Estimate bytes per value (rough approximation)
	bytesPerValue := estimateBytesPerValue(array.DataType())
	totalBytes := int64(array.Len()) * int64(bytesPerValue)

	numPages := totalBytes / int64(pageSize)
	if totalBytes%int64(pageSize) != 0 {
		numPages++
	}

	if numPages == 0 {
		numPages = 1
	}

	return int32(numPages)
}

// estimateBytesPerValue estimates bytes per value for a data type
func estimateBytesPerValue(dt arrow.DataType) int32 {
	switch t := dt.(type) {
	case *arrow.Int32Type:
		return 4
	case *arrow.Int64Type:
		return 8
	case *arrow.Float32Type:
		return 4
	case *arrow.Float64Type:
		return 8
	case *arrow.FixedSizeListType:
		elemSize := estimateBytesPerValue(t.Elem())
		return elemSize * int32(t.Size())
	case *arrow.StringType:
		return 16 // Average estimate
	case *arrow.BinaryType:
		return 16 // Average estimate
	default:
		return 8 // Default estimate
	}
}

// splitArrayIntoRanges splits an array into multiple ranges for pagination
func splitArrayIntoRanges(arrayLen int, pageSize int32, bytesPerValue int32) []struct{ Start, End int } {
	valuesPerPage := int(pageSize) / int(bytesPerValue)
	if valuesPerPage == 0 {
		valuesPerPage = 1
	}

	var ranges []struct{ Start, End int }
	for offset := 0; offset < arrayLen; {
		end := offset + valuesPerPage
		if end > arrayLen {
			end = arrayLen
		}
		ranges = append(ranges, struct{ Start, End int }{offset, end})
		offset = end
	}

	return ranges
}
