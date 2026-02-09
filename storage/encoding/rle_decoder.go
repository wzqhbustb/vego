package encoding

import (
	"encoding/binary"

	lerrors "github.com/wzqhbkjdx/vego/storage/errors"
	"github.com/wzqhbkjdx/vego/storage/arrow"
)

type RLEDecoder struct{}

func NewRLEDecoder() *RLEDecoder {
	return &RLEDecoder{}
}

func (d *RLEDecoder) Decode(data []byte, dtype arrow.DataType) (arrow.Array, error) {
	if len(data) < 4 {
		return nil, lerrors.New(lerrors.ErrCorruptedFile).
			Op("rle_decode").
			Context("reason", "data too short for header").
			Context("min_required", 4).
			Context("actual", len(data)).
			Build()
	}

	numRuns := binary.LittleEndian.Uint32(data[0:4])
	offset := 4

	switch dtype.ID() {
	case arrow.INT32:
		return d.decodeInt32(data[offset:], int(numRuns))
	case arrow.INT64:
		return d.decodeInt64(data[offset:], int(numRuns))
	default:
		return nil, lerrors.New(lerrors.ErrUnsupportedType).
			Op("rle_decode").
			Build()
	}
}

func (d *RLEDecoder) decodeInt32(data []byte, numRuns int) (arrow.Array, error) {
	// Format: [(value:int32, count:uint32)...]
	runSize := 8 // 4 bytes value + 4 bytes count
	expectedSize := numRuns * runSize
	if len(data) < expectedSize {
		return nil, lerrors.New(lerrors.ErrCorruptedFile).
			Op("rle_decode_int32").
			Context("reason", "insufficient data for runs").
			Context("expected", expectedSize).
			Context("actual", len(data)).
			Build()
	}

	// First pass: calculate total values
	totalValues := 0
	for i := 0; i < numRuns; i++ {
		count := binary.LittleEndian.Uint32(data[i*runSize+4 : i*runSize+8])
		totalValues += int(count)
	}

	// Second pass: expand runs
	values := make([]int32, totalValues)
	idx := 0
	for i := 0; i < numRuns; i++ {
		value := int32(binary.LittleEndian.Uint32(data[i*runSize : i*runSize+4]))
		count := int(binary.LittleEndian.Uint32(data[i*runSize+4 : i*runSize+8]))
		for j := 0; j < count; j++ {
			values[idx] = value
			idx++
		}
	}

	return arrow.NewInt32Array(values, nil), nil
}

func (d *RLEDecoder) decodeInt64(data []byte, numRuns int) (arrow.Array, error) {
	// Format: [(value:int64, count:uint32)...]
	runSize := 12 // 8 bytes value + 4 bytes count
	expectedSize := numRuns * runSize
	if len(data) < expectedSize {
		return nil, lerrors.New(lerrors.ErrCorruptedFile).
			Op("rle_decode_int64").
			Context("reason", "insufficient data for runs").
			Context("expected", expectedSize).
			Context("actual", len(data)).
			Build()
	}

	totalValues := 0
	for i := 0; i < numRuns; i++ {
		count := binary.LittleEndian.Uint32(data[i*runSize+8 : i*runSize+12])
		totalValues += int(count)
	}

	values := make([]int64, totalValues)
	idx := 0
	for i := 0; i < numRuns; i++ {
		value := int64(binary.LittleEndian.Uint64(data[i*runSize : i*runSize+8]))
		count := int(binary.LittleEndian.Uint32(data[i*runSize+8 : i*runSize+12]))
		for j := 0; j < count; j++ {
			values[idx] = value
			idx++
		}
	}

	return arrow.NewInt64Array(values, nil), nil
}
