package encoding

import (
	"github.com/wzqhbustb/vego/core"
	"github.com/wzqhbustb/vego/storage/format"
)

// Encoder defines the interface for encoding data.
// V2: Encode method now receives core.Array directly for type-safe, zero-copy encoding.
type Encoder interface {
	// Encode compresses an Arrow array.
	// The encoder can use type assertion to access typed data (e.g., *core.Float32Array).
	// Returns error if encoding fails or if the array type is not supported.
	Encode(array core.Array) (*EncodedData, error)

	// Type returns the encoding type
	Type() format.EncodingType

	// EstimateSize estimates the compressed size for the given array.
	// This is more accurate than the old byte-based estimation.
	EstimateSize(array core.Array) int

	// SupportsType checks if this encoder supports the given data type.
	// Optional: can be used by EncoderFactory for validation.
	SupportsType(dtype core.DataType) bool
}

// Decoder defines the interface for decoding data.
// V2: Decode method reconstructs core.Array directly with null support.
type Decoder interface {
	// Decode decompresses data and reconstructs an Arrow array of the specified type.
	// The dtype parameter tells the decoder what type of array to create.
	// If nullBitmap is provided and numValues > 0, the decoded values are expanded to include nulls.
	Decode(data []byte, dtype core.DataType, nullBitmap []byte, numValues int) (core.Array, error)
}

// EncodedData represents the result of encoding data.
// V2: Added support for null values through NullBitmap.
type EncodedData struct {
	Data       []byte              // Encoded non-null values
	Metadata   []byte              // Additional metadata if any
	Type       format.EncodingType // Encoding type
	NullBitmap []byte              // Bitmap where bit=1 means value exists, bit=0 means null (optional)
	NumValues  int                 // Total number of values (including nulls)
	NullCount  int                 // Number of null values
}

// HasNulls returns true if the encoded data contains null values.
func (e *EncodedData) HasNulls() bool {
	return e.NullCount > 0 && e.NullBitmap != nil
}

// Validate checks if the encoded data is valid.
func (e *EncodedData) Validate() error {
	if e.NumValues < 0 {
		return ErrInvalidData
	}
	if e.NullCount < 0 || e.NullCount > e.NumValues {
		return ErrInvalidData
	}
	// Zstd 编码特殊处理：null 信息嵌入在 Data 中，不需要单独的 NullBitmap
	// 对于其他编码，如果 NullCount > 0 则需要 NullBitmap
	if e.Type != format.EncodingZstd && e.NullCount > 0 && e.NullBitmap == nil {
		return ErrInvalidData
	}
	if e.NullBitmap != nil {
		expectedSize := BitmapSize(e.NumValues)
		if len(e.NullBitmap) != expectedSize {
			return ErrInvalidData
		}
	}
	return nil
}
