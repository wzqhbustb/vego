package encoding

import (
	"encoding/binary"
	"math"

	lerrors "github.com/wzqhbustb/vego/storage/errors"
	"github.com/wzqhbustb/vego/storage/arrow"
)

type BSSDecoder struct{}

func NewBSSDecoder() *BSSDecoder {
	return &BSSDecoder{}
}

func (d *BSSDecoder) Decode(data []byte, dtype arrow.DataType) (arrow.Array, error) {
	if len(data) < 4 {
		return nil, lerrors.New(lerrors.ErrCorruptedFile).
			Op("bss_decode").
			Context("reason", "data too short for header").
			Context("min_required", 4).
			Context("actual", len(data)).
			Build()
	}

	// Read numValues
	numValues := binary.LittleEndian.Uint32(data[0:4])
	headerSize := 4

	switch dtype.ID() {
	case arrow.FLOAT32:
		return d.decodeFloat32(data[headerSize:], int(numValues))
	case arrow.FLOAT64:
		return d.decodeFloat64(data[headerSize:], int(numValues))
	default:
		return nil, lerrors.New(lerrors.ErrUnsupportedType).
			Op("bss_decode").
			Build()
	}
}

func (d *BSSDecoder) decodeFloat32(data []byte, numValues int) (arrow.Array, error) {
	// Format: [stream0...][stream1...][stream2...][stream3...]
	// Each stream has numValues bytes
	expectedSize := numValues * 4
	if len(data) < expectedSize {
		return nil, lerrors.New(lerrors.ErrCorruptedFile).
			Op("bss_decode_float32").
			Context("reason", "insufficient data").
			Context("expected", expectedSize).
			Context("actual", len(data)).
			Build()
	}

	values := make([]float32, numValues)

	// Reconstruct from byte streams
	for i := 0; i < numValues; i++ {
		bits := uint32(data[i]) |
			uint32(data[numValues+i])<<8 |
			uint32(data[numValues*2+i])<<16 |
			uint32(data[numValues*3+i])<<24
		values[i] = math.Float32frombits(bits)
	}

	return arrow.NewFloat32Array(values, nil), nil
}

func (d *BSSDecoder) decodeFloat64(data []byte, numValues int) (arrow.Array, error) {
	// Format: [stream0...]...[stream7...]
	expectedSize := numValues * 8
	if len(data) < expectedSize {
		return nil, lerrors.New(lerrors.ErrCorruptedFile).
			Op("bss_decode_float64").
			Context("reason", "insufficient data").
			Context("expected", expectedSize).
			Context("actual", len(data)).
			Build()
	}

	values := make([]float64, numValues)

	for i := 0; i < numValues; i++ {
		bits := uint64(data[i]) |
			uint64(data[numValues+i])<<8 |
			uint64(data[numValues*2+i])<<16 |
			uint64(data[numValues*3+i])<<24 |
			uint64(data[numValues*4+i])<<32 |
			uint64(data[numValues*5+i])<<40 |
			uint64(data[numValues*6+i])<<48 |
			uint64(data[numValues*7+i])<<56
		values[i] = math.Float64frombits(bits)
	}

	return arrow.NewFloat64Array(values, nil), nil
}
