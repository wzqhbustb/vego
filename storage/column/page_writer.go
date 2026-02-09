package column

import (
	"github.com/wzqhbkjdx/vego/storage/arrow"
	"github.com/wzqhbkjdx/vego/storage/encoding"
	lerrors "github.com/wzqhbkjdx/vego/storage/errors"
	"github.com/wzqhbkjdx/vego/storage/format"
)

// PageWriter handles serialization of Array data to Pages with intelligent encoding
type PageWriter struct {
	factory *encoding.EncoderFactory
}

// NewPageWriter creates a new page writer with the given encoder factory.
// If factory is nil, a default factory with compression level 3 is used.
func NewPageWriter(factory *encoding.EncoderFactory) *PageWriter {
	if factory == nil {
		factory = encoding.NewEncoderFactory(3) // 默认压缩级别 3
	}
	return &PageWriter{
		factory: factory,
	}
}

// WritePages converts an Array into Pages with intelligent encoding.
// Currently returns a single page, but the signature allows for future pagination support.
// The encoder is selected based on data statistics (cardinality, entropy, run ratio).
// If the selected encoder fails (e.g., doesn't support nulls), it automatically
// falls back to Zstd compression.
func (w *PageWriter) WritePages(array arrow.Array, columnIndex int32) ([]*format.Page, error) {
	if array == nil || array.Len() == 0 {
		return nil, lerrors.New(lerrors.ErrInvalidArgument).
			Op("write_pages").
			Context("message", "cannot write empty array").
			Build()
	}

	// Special handling for FixedSizeListArray - always use Zstd
	// FixedSizeListArray is a container type, and individual encoders (BSS, RLE, etc.)
	// don't know how to handle it. We could extract and encode the child array,
	// but for simplicity and safety, we use Zstd which handles any data type.
	if _, isFixedSizeList := array.(*arrow.FixedSizeListArray); isFixedSizeList {
		return w.writeWithZstd(array, columnIndex)
	}

	// Step 1: Compute statistics for encoder selection
	stats := encoding.ComputeStatistics(array)

	// Step 2: Select best encoder based on statistics
	encoder := w.factory.SelectEncoder(array.DataType(), stats)
	if encoder == nil {
		return nil, lerrors.New(lerrors.ErrUnsupportedType).
			Op("select_encoder").
			Context("data_type", array.DataType().Name()).
			Context("message", "failed to select encoder").
			Build()
	}

	// Step 3: Encode the array with automatic fallback
	encodedData, err := w.encodeWithFallback(array, encoder)
	if err != nil {
		return nil, lerrors.EncodeFailed(encoder.Type().String(), array.DataType().Name(), err)
	}

	// Step 4: Calculate uncompressed size for statistics
	uncompressedSize := w.calculateUncompressedSize(array)

	// Step 5: Create and populate page
	page := format.NewPage(columnIndex, format.PageTypeData, encodedData.Type)
	page.NumValues = int32(array.Len())
	page.SetData(encodedData.Data, int32(uncompressedSize))

	return []*format.Page{page}, nil
}

// writeWithZstd writes the array using Zstd compression.
// Used for FixedSizeListArray and as fallback for other types.
func (w *PageWriter) writeWithZstd(array arrow.Array, columnIndex int32) ([]*format.Page, error) {
	zstdEncoder := encoding.NewZstdEncoder(w.factory.GetCompressionLevel())
	encodedData, err := zstdEncoder.Encode(array)
	if err != nil {
		return nil, lerrors.EncodeFailed("zstd", array.DataType().Name(), err)
	}

	uncompressedSize := w.calculateUncompressedSize(array)
	page := format.NewPage(columnIndex, format.PageTypeData, encodedData.Type)
	page.NumValues = int32(array.Len())
	page.SetData(encodedData.Data, int32(uncompressedSize))

	return []*format.Page{page}, nil
}

// encodeWithFallback attempts to encode with the given encoder and falls back to Zstd if needed.
// This handles cases where specialized encoders don't support null values or certain data patterns.
func (w *PageWriter) encodeWithFallback(array arrow.Array, encoder encoding.Encoder) (*encoding.EncodedData, error) {
	encodedData, err := encoder.Encode(array)
	if err != nil {
		// If specialized encoder fails due to null or type issues, fallback to Zstd
		if err == encoding.ErrNullNotSupported || err == encoding.ErrUnsupportedType {
			// TODO: Add logging here
			// log.Warnf("Encoder %v failed (%v), falling back to Zstd", encoder.Type(), err)
			zstdEncoder := encoding.NewZstdEncoder(w.factory.GetCompressionLevel())
			encodedData, err = zstdEncoder.Encode(array)
			if err != nil {
				return nil, lerrors.EncodeFailed("zstd_fallback", array.DataType().Name(), err)
			}
			return encodedData, nil
		}
		return nil, err
	}
	return encodedData, nil
}

// calculateUncompressedSize computes the raw size of the array data including nulls.
// This is an approximate value for statistics purposes.
func (w *PageWriter) calculateUncompressedSize(array arrow.Array) int {
	// Base size: number of values * size per value
	valueSize := encoding.GetValueSize(array.DataType().ID())
	size := array.Len() * valueSize

	// Add null bitmap size if present
	if array.NullN() > 0 {
		bitmapSize := (array.Len() + 7) / 8
		size += bitmapSize
	}

	return size
}

// EstimatePageSize estimates the encoded size for an array without actually encoding.
// This is useful for buffer pre-allocation and planning page splits.
// Note: This is a best-effort estimate. Actual encoding may fall back to Zstd
// if the selected encoder doesn't support the data pattern (e.g., null values).
func (w *PageWriter) EstimatePageSize(array arrow.Array) (int, error) {
	if array == nil || array.Len() == 0 {
		return 0, lerrors.New(lerrors.ErrInvalidArgument).
			Op("estimate_page_size").
			Context("message", "cannot estimate empty array").
			Build()
	}

	// For FixedSizeListArray, estimate with Zstd directly
	if _, isFixedSizeList := array.(*arrow.FixedSizeListArray); isFixedSizeList {
		zstdEncoder := encoding.NewZstdEncoder(w.factory.GetCompressionLevel())
		return zstdEncoder.EstimateSize(array), nil
	}

	stats := encoding.ComputeStatistics(array)
	encoder := w.factory.SelectEncoder(array.DataType(), stats)
	if encoder == nil {
		return 0, lerrors.New(lerrors.ErrUnsupportedType).
			Op("select_encoder_estimate").
			Context("data_type", array.DataType().Name()).
			Context("message", "failed to select encoder for estimation").
			Build()
	}

	// If the encoder doesn't support nulls but array has nulls, estimate with Zstd instead
	if array.NullN() > 0 && !w.encoderSupportsNulls(encoder) {
		zstdEncoder := encoding.NewZstdEncoder(w.factory.GetCompressionLevel())
		return zstdEncoder.EstimateSize(array), nil
	}

	return encoder.EstimateSize(array), nil
}

// encoderSupportsNulls checks if an encoder can handle null values.
// Currently only Zstd supports null values. All specialized encoders
// (RLE, BitPacking, BSS, Dictionary) reject arrays with nulls.
func (w *PageWriter) encoderSupportsNulls(encoder encoding.Encoder) bool {
	switch encoder.Type() {
	case format.EncodingZstd:
		return true
	case format.EncodingRLE, format.EncodingBitPacked,
		format.EncodingBSSEncoding, format.EncodingDictionary:
		return false
	default:
		return false
	}
}
