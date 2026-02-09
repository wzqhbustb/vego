package encoding

import (
	"encoding/binary"
	"math"

	lerrors "github.com/wzqhbkjdx/vego/storage/errors"
	"github.com/wzqhbkjdx/vego/storage/arrow"
)

type DictionaryDecoder struct{}

func NewDictionaryDecoder() *DictionaryDecoder {
	return &DictionaryDecoder{}
}

func (d *DictionaryDecoder) Decode(data []byte, dtype arrow.DataType) (arrow.Array, error) {
	if len(data) < 10 {
		return nil, lerrors.New(lerrors.ErrCorruptedFile).
			Op("dictionary_decode").
			Context("reason", "data too short for header").
			Context("min_required", 10).
			Context("actual", len(data)).
			Build()
	}

	valueSize := int(data[0])
	numEntries := binary.LittleEndian.Uint32(data[1:5])
	numValues := binary.LittleEndian.Uint32(data[5:9])
	indexSize := int(data[9])

	headerSize := 10
	offset := headerSize

	switch dtype.ID() {
	case arrow.INT32:
		if valueSize != 4 {
			return nil, lerrors.New(lerrors.ErrCorruptedFile).
				Op("dictionary_decode_int32").
				Context("reason", "unexpected value size").
				Context("expected", 4).
				Context("actual", valueSize).
				Build()
		}
		return d.decodeInt32(data[offset:], int(numEntries), int(numValues), indexSize)
	case arrow.INT64:
		if valueSize != 8 {
			return nil, lerrors.New(lerrors.ErrCorruptedFile).
				Op("dictionary_decode_int64").
				Context("reason", "unexpected value size").
				Context("expected", 8).
				Context("actual", valueSize).
				Build()
		}
		return d.decodeInt64(data[offset:], int(numEntries), int(numValues), indexSize)
	case arrow.FLOAT32:
		if valueSize != 4 {
			return nil, lerrors.New(lerrors.ErrCorruptedFile).
				Op("dictionary_decode_float32").
				Context("reason", "unexpected value size").
				Context("expected", 4).
				Context("actual", valueSize).
				Build()
		}
		return d.decodeFloat32(data[offset:], int(numEntries), int(numValues), indexSize)
	case arrow.FLOAT64:
		if valueSize != 8 {
			return nil, lerrors.New(lerrors.ErrCorruptedFile).
				Op("dictionary_decode_float64").
				Context("reason", "unexpected value size").
				Context("expected", 8).
				Context("actual", valueSize).
				Build()
		}
		return d.decodeFloat64(data[offset:], int(numEntries), int(numValues), indexSize)
	default:
		return nil, lerrors.New(lerrors.ErrUnsupportedType).
			Op("dictionary_decode").
			Build()
	}
}

func (d *DictionaryDecoder) decodeInt32(data []byte, numEntries, numValues, indexSize int) (arrow.Array, error) {
	// Read dictionary
	dictSize := numEntries * 4
	if len(data) < dictSize {
		return nil, lerrors.New(lerrors.ErrCorruptedFile).
			Op("dictionary_decode_int32").
			Context("reason", "insufficient data for dictionary").
			Context("expected", dictSize).
			Context("actual", len(data)).
			Build()
	}

	dict := make([]int32, numEntries)
	for i := 0; i < numEntries; i++ {
		dict[i] = int32(binary.LittleEndian.Uint32(data[i*4:]))
	}

	// Read indices
	offset := dictSize
	indexArraySize := numValues * indexSize
	if len(data) < dictSize+indexArraySize {
		return nil, lerrors.New(lerrors.ErrCorruptedFile).
			Op("dictionary_decode_int32").
			Context("reason", "insufficient data for indices").
			Context("expected", dictSize+indexArraySize).
			Context("actual", len(data)).
			Build()
	}

	// Expand values using indices
	values := make([]int32, numValues)
	for i := 0; i < numValues; i++ {
		var idx int
		if indexSize == 2 {
			idx = int(binary.LittleEndian.Uint16(data[offset+i*2:]))
		} else {
			idx = int(binary.LittleEndian.Uint32(data[offset+i*4:]))
		}
		if idx >= numEntries {
			return nil, lerrors.New(lerrors.ErrCorruptedFile).
				Op("dictionary_decode_int32").
				Context("reason", "index out of range").
				Context("index", idx).
				Context("num_entries", numEntries).
				Build()
		}
		values[i] = dict[idx]
	}

	return arrow.NewInt32Array(values, nil), nil
}

func (d *DictionaryDecoder) decodeInt64(data []byte, numEntries, numValues, indexSize int) (arrow.Array, error) {
	dictSize := numEntries * 8
	if len(data) < dictSize {
		return nil, lerrors.New(lerrors.ErrCorruptedFile).
			Op("dictionary_decode_int64").
			Context("reason", "insufficient data for dictionary").
			Context("expected", dictSize).
			Context("actual", len(data)).
			Build()
	}

	dict := make([]int64, numEntries)
	for i := 0; i < numEntries; i++ {
		dict[i] = int64(binary.LittleEndian.Uint64(data[i*8:]))
	}

	offset := dictSize
	values := make([]int64, numValues)
	for i := 0; i < numValues; i++ {
		var idx int
		if indexSize == 2 {
			idx = int(binary.LittleEndian.Uint16(data[offset+i*2:]))
		} else {
			idx = int(binary.LittleEndian.Uint32(data[offset+i*4:]))
		}
		if idx >= numEntries {
			return nil, lerrors.New(lerrors.ErrCorruptedFile).
				Op("dictionary_decode_int64").
				Context("reason", "index out of range").
				Context("index", idx).
				Context("num_entries", numEntries).
				Build()
		}
		values[i] = dict[idx]
	}

	return arrow.NewInt64Array(values, nil), nil
}

func (d *DictionaryDecoder) decodeFloat32(data []byte, numEntries, numValues, indexSize int) (arrow.Array, error) {
	dictSize := numEntries * 4
	if len(data) < dictSize {
		return nil, lerrors.New(lerrors.ErrCorruptedFile).
			Op("dictionary_decode_float32").
			Context("reason", "insufficient data for dictionary").
			Context("expected", dictSize).
			Context("actual", len(data)).
			Build()
	}

	dict := make([]float32, numEntries)
	for i := 0; i < numEntries; i++ {
		bits := binary.LittleEndian.Uint32(data[i*4:])
		dict[i] = math.Float32frombits(bits)
	}

	offset := dictSize
	values := make([]float32, numValues)
	for i := 0; i < numValues; i++ {
		var idx int
		if indexSize == 2 {
			idx = int(binary.LittleEndian.Uint16(data[offset+i*2:]))
		} else {
			idx = int(binary.LittleEndian.Uint32(data[offset+i*4:]))
		}
		if idx >= numEntries {
			return nil, lerrors.New(lerrors.ErrCorruptedFile).
				Op("dictionary_decode_float32").
				Context("reason", "index out of range").
				Context("index", idx).
				Context("num_entries", numEntries).
				Build()
		}
		values[i] = dict[idx]
	}

	return arrow.NewFloat32Array(values, nil), nil
}

func (d *DictionaryDecoder) decodeFloat64(data []byte, numEntries, numValues, indexSize int) (arrow.Array, error) {
	dictSize := numEntries * 8
	if len(data) < dictSize {
		return nil, lerrors.New(lerrors.ErrCorruptedFile).
			Op("dictionary_decode_float64").
			Context("reason", "insufficient data for dictionary").
			Context("expected", dictSize).
			Context("actual", len(data)).
			Build()
	}

	dict := make([]float64, numEntries)
	for i := 0; i < numEntries; i++ {
		bits := binary.LittleEndian.Uint64(data[i*8:])
		dict[i] = math.Float64frombits(bits)
	}

	offset := dictSize
	values := make([]float64, numValues)
	for i := 0; i < numValues; i++ {
		var idx int
		if indexSize == 2 {
			idx = int(binary.LittleEndian.Uint16(data[offset+i*2:]))
		} else {
			idx = int(binary.LittleEndian.Uint32(data[offset+i*4:]))
		}
		if idx >= numEntries {
			return nil, lerrors.New(lerrors.ErrCorruptedFile).
				Op("dictionary_decode_float64").
				Context("reason", "index out of range").
				Context("index", idx).
				Context("num_entries", numEntries).
				Build()
		}
		values[i] = dict[idx]
	}

	return arrow.NewFloat64Array(values, nil), nil
}
