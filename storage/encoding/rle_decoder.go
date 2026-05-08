package encoding

import (
	"encoding/binary"

	"github.com/wzqhbustb/vego/core"
)

type RLEDecoder struct{}

func NewRLEDecoder() *RLEDecoder {
	return &RLEDecoder{}
}

// Decode decodes RLE-encoded data.
// If nullBitmap is provided, expands the values to include nulls.
// numValues is the total number of values (including nulls).
func (d *RLEDecoder) Decode(data []byte, dtype core.DataType, nullBitmap []byte, numValues int) (core.Array, error) {
	// Convert []byte to *core.Bitmap if needed
	var bitmap *core.Bitmap
	if nullBitmap != nil && numValues > 0 {
		bitmap = core.NewBitmapFromBytes(nullBitmap, numValues)
	}

	// Handle all-null case (no data, only nulls)
	if len(data) == 0 && bitmap != nil && numValues > 0 {
		switch dtype.ID() {
		case core.INT32:
			return core.NewInt32Array(make([]int32, numValues), bitmap), nil
		case core.INT64:
			return core.NewInt64Array(make([]int64, numValues), bitmap), nil
		default:
			return nil, core.New(core.ErrUnsupportedType).
				Op("rle_decode").
				Build()
		}
	}

	if len(data) < 4 {
		return nil, core.New(core.ErrCorruptedFile).
			Op("rle_decode").
			Context("reason", "data too short for header").
			Context("min_required", 4).
			Context("actual", len(data)).
			Build()
	}

	numRuns := binary.LittleEndian.Uint32(data[0:4])
	offset := 4

	switch dtype.ID() {
	case core.INT32:
		return d.decodeInt32(data[offset:], int(numRuns), nullBitmap, numValues)
	case core.INT64:
		return d.decodeInt64(data[offset:], int(numRuns), nullBitmap, numValues)
	default:
		return nil, core.New(core.ErrUnsupportedType).
			Op("rle_decode").
			Build()
	}
}

func (d *RLEDecoder) decodeInt32(data []byte, numRuns int, nullBitmap []byte, numValues int) (core.Array, error) {
	// Format: [(value:int32, count:uint32)...]
	runSize := 8 // 4 bytes value + 4 bytes count
	expectedSize := numRuns * runSize
	if len(data) < expectedSize {
		return nil, core.New(core.ErrCorruptedFile).
			Op("rle_decode_int32").
			Context("reason", "insufficient data for runs").
			Context("expected", expectedSize).
			Context("actual", len(data)).
			Build()
	}

	// Calculate total non-null values from runs
	totalNonNull := 0
	for i := 0; i < numRuns; i++ {
		count := binary.LittleEndian.Uint32(data[i*runSize+4 : i*runSize+8])
		totalNonNull += int(count)
	}

	// Expand runs to get non-null values
	nonNullValues := make([]int32, totalNonNull)
	idx := 0
	for i := 0; i < numRuns; i++ {
		value := int32(binary.LittleEndian.Uint32(data[i*runSize : i*runSize+4]))
		count := int(binary.LittleEndian.Uint32(data[i*runSize+4 : i*runSize+8]))
		for j := 0; j < count; j++ {
			nonNullValues[idx] = value
			idx++
		}
	}

	// If no null bitmap, return directly
	if nullBitmap == nil || numValues == 0 {
		return core.NewInt32Array(nonNullValues, nil), nil
	}

	// Expand with nulls: insert values back to their original positions
	values, err := ExpandInt32(nonNullValues, nullBitmap, numValues)
	if err != nil {
		return nil, core.New(core.ErrCorruptedFile).
			Op("rle_decode_int32").
			Wrap(err).
			Build()
	}
	bitmap := core.NewBitmapFromBytes(nullBitmap, numValues)
	return core.NewInt32Array(values, bitmap), nil
}

func (d *RLEDecoder) decodeInt64(data []byte, numRuns int, nullBitmap []byte, numValues int) (core.Array, error) {
	// Format: [(value:int64, count:uint32)...]
	runSize := 12 // 8 bytes value + 4 bytes count
	expectedSize := numRuns * runSize
	if len(data) < expectedSize {
		return nil, core.New(core.ErrCorruptedFile).
			Op("rle_decode_int64").
			Context("reason", "insufficient data for runs").
			Context("expected", expectedSize).
			Context("actual", len(data)).
			Build()
	}

	// Calculate total non-null values
	totalNonNull := 0
	for i := 0; i < numRuns; i++ {
		count := binary.LittleEndian.Uint32(data[i*runSize+8 : i*runSize+12])
		totalNonNull += int(count)
	}

	// Expand runs
	nonNullValues := make([]int64, totalNonNull)
	idx := 0
	for i := 0; i < numRuns; i++ {
		value := int64(binary.LittleEndian.Uint64(data[i*runSize : i*runSize+8]))
		count := int(binary.LittleEndian.Uint32(data[i*runSize+8 : i*runSize+12]))
		for j := 0; j < count; j++ {
			nonNullValues[idx] = value
			idx++
		}
	}

	// If no null bitmap, return directly
	if nullBitmap == nil || numValues == 0 {
		return core.NewInt64Array(nonNullValues, nil), nil
	}

	// Expand with nulls
	values, err := ExpandInt64(nonNullValues, nullBitmap, numValues)
	if err != nil {
		return nil, core.New(core.ErrCorruptedFile).
			Op("rle_decode_int64").
			Wrap(err).
			Build()
	}
	bitmap := core.NewBitmapFromBytes(nullBitmap, numValues)
	return core.NewInt64Array(values, bitmap), nil
}
