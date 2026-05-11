package encoding

import (
	"encoding/binary"
	"math"

	"github.com/wzqhbustb/vego/core"
)

type BSSDecoder struct{}

func NewBSSDecoder() *BSSDecoder {
	return &BSSDecoder{}
}

func (d *BSSDecoder) Decode(data []byte, dtype core.DataType, nullBitmap []byte, numValues int) (core.Array, error) {
	if len(data) < 4 {
		return nil, core.New(core.ErrCorruptedFile).
			Op("bss_decode").
			Context("reason", "data too short for header").
			Context("min_required", 4).
			Context("actual", len(data)).
			Build()
	}

	// Read packedNumValues (when null support is added, this may differ from numValues)
	packedNumValues := int(binary.LittleEndian.Uint32(data[0:4]))
	headerSize := 4

	switch dtype.ID() {
	case core.FLOAT32:
		return d.decodeFloat32(data[headerSize:], packedNumValues, nullBitmap, numValues)
	case core.FLOAT64:
		return d.decodeFloat64(data[headerSize:], packedNumValues, nullBitmap, numValues)
	default:
		return nil, core.New(core.ErrUnsupportedType).
			Op("bss_decode").
			Build()
	}
}

func (d *BSSDecoder) decodeFloat32(data []byte, packedNumValues int, nullBitmap []byte, numValues int) (core.Array, error) {
	// Format: [stream0...][stream1...][stream2...][stream3...]
	// Each stream has packedNumValues bytes
	expectedSize := packedNumValues * 4
	if len(data) < expectedSize {
		return nil, core.New(core.ErrCorruptedFile).
			Op("bss_decode_float32").
			Context("reason", "insufficient data").
			Context("expected", expectedSize).
			Context("actual", len(data)).
			Build()
	}

	values := make([]float32, packedNumValues)

	// Reconstruct from byte streams
	for i := 0; i < packedNumValues; i++ {
		bits := uint32(data[i]) |
			uint32(data[packedNumValues+i])<<8 |
			uint32(data[packedNumValues*2+i])<<16 |
			uint32(data[packedNumValues*3+i])<<24
		values[i] = math.Float32frombits(bits)
	}

	if nullBitmap != nil && numValues > 0 {
		expandedValues, err := ExpandFloat32(values, nullBitmap, numValues)
		if err != nil {
			return nil, core.New(core.ErrCorruptedFile).
				Op("bss_decode_float32").
				Wrap(err).
				Build()
		}
		bitmap := core.NewBitmapFromBytes(nullBitmap, numValues)
		return core.NewFloat32Array(expandedValues, bitmap), nil
	}

	return core.NewFloat32Array(values, nil), nil
}

func (d *BSSDecoder) decodeFloat64(data []byte, packedNumValues int, nullBitmap []byte, numValues int) (core.Array, error) {
	expectedSize := packedNumValues * 8
	if len(data) < expectedSize {
		return nil, core.New(core.ErrCorruptedFile).
			Op("bss_decode_float64").
			Context("reason", "insufficient data").
			Context("expected", expectedSize).
			Context("actual", len(data)).
			Build()
	}

	values := make([]float64, packedNumValues)

	for i := 0; i < packedNumValues; i++ {
		bits := uint64(data[i]) |
			uint64(data[packedNumValues+i])<<8 |
			uint64(data[packedNumValues*2+i])<<16 |
			uint64(data[packedNumValues*3+i])<<24 |
			uint64(data[packedNumValues*4+i])<<32 |
			uint64(data[packedNumValues*5+i])<<40 |
			uint64(data[packedNumValues*6+i])<<48 |
			uint64(data[packedNumValues*7+i])<<56
		values[i] = math.Float64frombits(bits)
	}

	if nullBitmap != nil && numValues > 0 {
		expandedValues, err := ExpandFloat64(values, nullBitmap, numValues)
		if err != nil {
			return nil, core.New(core.ErrCorruptedFile).
				Op("bss_decode_float64").
				Wrap(err).
				Build()
		}
		bitmap := core.NewBitmapFromBytes(nullBitmap, numValues)
		return core.NewFloat64Array(expandedValues, bitmap), nil
	}

	return core.NewFloat64Array(values, nil), nil
}
