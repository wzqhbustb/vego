package encoding

import (
	lerrors "github.com/wzqhbustb/vego/storage/errors"
	"github.com/wzqhbustb/vego/storage/arrow"
	"unsafe"
)

// ArrayToBytes extracts raw bytes from an Arrow array's value buffer.
// This is used by encoders that need byte-level access (like Zstd).
// For FixedSizeListArray, it recursively extracts bytes from the child array.
// Note: This does NOT include the null bitmap.
func ArrayToBytes(array arrow.Array) ([]byte, error) {
	switch arr := array.(type) {
	case *arrow.Int32Array:
		return int32SliceToBytes(arr.Values()), nil
	case *arrow.Int64Array:
		return int64SliceToBytes(arr.Values()), nil
	case *arrow.Float32Array:
		return float32SliceToBytes(arr.Values()), nil
	case *arrow.Float64Array:
		return float64SliceToBytes(arr.Values()), nil
	case *arrow.FixedSizeListArray:
		// For FixedSizeListArray, recursively get bytes from child array
		return ArrayToBytes(arr.Values())
	default:
		return nil, lerrors.New(lerrors.ErrUnsupportedType).
			Op("array_to_bytes").
			Context("array_type", "unknown").
			Build()
	}
}

// int32SliceToBytes converts []int32 to []byte without copy (unsafe but efficient).
// For production, consider using safe copy if memory aliasing is a concern.
func int32SliceToBytes(values []int32) []byte {
	if len(values) == 0 {
		return []byte{}
	}
	// Calculate byte length: len(values) * 4
	byteLen := len(values) * 4
	// Use unsafe.Pointer to convert without copy
	// Note: This is Go unsafe, but efficient for encoding scenarios
	header := *(*sliceHeader)(unsafe.Pointer(&values))
	header.Len = byteLen
	header.Cap = byteLen
	return *(*[]byte)(unsafe.Pointer(&header))
}

// sliceHeader is a simplified version of reflect.SliceHeader
type sliceHeader struct {
	Data unsafe.Pointer
	Len  int
	Cap  int
}

// Similar functions for other types...
func int64SliceToBytes(values []int64) []byte {
	if len(values) == 0 {
		return []byte{}
	}
	byteLen := len(values) * 8
	header := *(*sliceHeader)(unsafe.Pointer(&values))
	header.Len = byteLen
	header.Cap = byteLen
	return *(*[]byte)(unsafe.Pointer(&header))
}

func float32SliceToBytes(values []float32) []byte {
	if len(values) == 0 {
		return []byte{}
	}
	byteLen := len(values) * 4
	header := *(*sliceHeader)(unsafe.Pointer(&values))
	header.Len = byteLen
	header.Cap = byteLen
	return *(*[]byte)(unsafe.Pointer(&header))
}

func float64SliceToBytes(values []float64) []byte {
	if len(values) == 0 {
		return []byte{}
	}
	byteLen := len(values) * 8
	header := *(*sliceHeader)(unsafe.Pointer(&values))
	header.Len = byteLen
	header.Cap = byteLen
	return *(*[]byte)(unsafe.Pointer(&header))
}

// GetNullBitmap extracts the null bitmap from an array if it has nulls.
// Returns nil if the array has no nulls.
// For FixedSizeListArray, returns the list-level null bitmap (not the child's).
func GetNullBitmap(array arrow.Array) *arrow.Bitmap {
	if array.NullN() == 0 {
		return nil
	}
	return array.Data().NullBitmap()
}

// HasNulls is a helper to check if array has any nulls.
func HasNulls(array arrow.Array) bool {
	return array.NullN() > 0
}

// GetValueCount returns the number of values in the array.
// For FixedSizeListArray, returns the number of lists (not total elements).
func GetValueCount(array arrow.Array) int {
	return array.Len()
}

// GetFixedSizeListValueSize returns the list size for FixedSizeListArray.
// Returns 0 for other types.
func GetFixedSizeListValueSize(array arrow.Array) int {
	if arr, ok := array.(*arrow.FixedSizeListArray); ok {
		return arr.ListSize()
	}
	return 0
}
