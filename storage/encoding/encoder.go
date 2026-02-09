package encoding

import (
	"github.com/wzqhbkjdx/vego/storage/arrow"
	"github.com/wzqhbkjdx/vego/storage/format"
)

// Encoder defines the interface for encoding data.
// V2: Encode method now receives arrow.Array directly for type-safe, zero-copy encoding.
type Encoder interface {
	// Encode compresses an Arrow array.
	// The encoder can use type assertion to access typed data (e.g., *arrow.Float32Array).
	// Returns error if encoding fails or if the array type is not supported.
	Encode(array arrow.Array) (*EncodedData, error)

	// Type returns the encoding type
	Type() format.EncodingType

	// EstimateSize estimates the compressed size for the given array.
	// This is more accurate than the old byte-based estimation.
	EstimateSize(array arrow.Array) int

	// SupportsType checks if this encoder supports the given data type.
	// Optional: can be used by EncoderFactory for validation.
	SupportsType(dtype arrow.DataType) bool
}

// Decoder defines the interface for decoding data.
// V2: Decode method reconstructs arrow.Array directly.
type Decoder interface {
	// Decode decompresses data and reconstructs an Arrow array of the specified type.
	// The dtype parameter tells the decoder what type of array to create.
	Decode(data []byte, dtype arrow.DataType) (arrow.Array, error)
}

// EncodedData represents the result of encoding data.
type EncodedData struct {
	Data     []byte              // Encoded data bytes
	Metadata []byte              // Additional metadata if any
	Type     format.EncodingType // Encoding type
}
