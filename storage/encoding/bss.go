package encoding

import (
	"bytes"
	"encoding/binary"
	"math"

	lerrors "github.com/wzqhbustb/vego/storage/errors"
	"github.com/wzqhbustb/vego/storage/arrow"
	"github.com/wzqhbustb/vego/storage/format"
)

type BSSEncoder struct{}

func NewBSSEncoder() *BSSEncoder {
	return &BSSEncoder{}
}

func (e *BSSEncoder) Type() format.EncodingType {
	return format.EncodingBSSEncoding
}

func (e *BSSEncoder) Encode(array arrow.Array) (*EncodedData, error) {
	if array.Len() == 0 {
		return nil, ErrEmptyArray
	}

	switch arr := array.(type) {
	case *arrow.Float32Array:
		return e.encodeFloat32(arr)
	case *arrow.Float64Array:
		return e.encodeFloat64(arr)
	default:
		return nil, lerrors.New(lerrors.ErrUnsupportedType).
			Op("bss_encode").
			Build()
	}
}

func (e *BSSEncoder) encodeFloat32(arr *arrow.Float32Array) (*EncodedData, error) {
	numValues := arr.Len()
	nullN := arr.NullN()

	// Handle nulls: extract non-null values
	var values []float32
	var nullBitmap []byte

	if nullN > 0 {
		nullBitmap = ExtractNullBitmap(arr)
		// Filter out null values
		allValues := arr.Values()
		for i := 0; i < numValues; i++ {
			if !IsNull(nullBitmap, i) {
				values = append(values, allValues[i])
			}
		}
	} else {
		values = arr.Values()
	}

	packedNumValues := len(values)

	// 创建 4 个 byte stream
	streams := make([][]byte, 4)
	for i := 0; i < 4; i++ {
		streams[i] = make([]byte, packedNumValues)
	}

	// Byte Stream Split
	for i, v := range values {
		bits := math.Float32bits(v)
		streams[0][i] = byte(bits)
		streams[1][i] = byte(bits >> 8)
		streams[2][i] = byte(bits >> 16)
		streams[3][i] = byte(bits >> 24)
	}

	// 打包: [numValues:4][stream0...][stream1...][stream2...][stream3...]
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, uint32(packedNumValues))
	for _, s := range streams {
		buf.Write(s)
	}

	return &EncodedData{
		Data:       buf.Bytes(),
		Type:       format.EncodingBSSEncoding,
		NullBitmap: nullBitmap,
		NumValues:  numValues,
		NullCount:  nullN,
	}, nil
}

func (e *BSSEncoder) encodeFloat64(arr *arrow.Float64Array) (*EncodedData, error) {
	numValues := arr.Len()
	nullN := arr.NullN()

	// Handle nulls: extract non-null values
	var values []float64
	var nullBitmap []byte

	if nullN > 0 {
		nullBitmap = ExtractNullBitmap(arr)
		// Filter out null values
		allValues := arr.Values()
		for i := 0; i < numValues; i++ {
			if !IsNull(nullBitmap, i) {
				values = append(values, allValues[i])
			}
		}
	} else {
		values = arr.Values()
	}

	packedNumValues := len(values)

	// 创建 8 个 byte stream
	streams := make([][]byte, 8)
	for i := 0; i < 8; i++ {
		streams[i] = make([]byte, packedNumValues)
	}

	// Byte Stream Split
	for i, v := range values {
		bits := math.Float64bits(v)
		streams[0][i] = byte(bits)
		streams[1][i] = byte(bits >> 8)
		streams[2][i] = byte(bits >> 16)
		streams[3][i] = byte(bits >> 24)
		streams[4][i] = byte(bits >> 32)
		streams[5][i] = byte(bits >> 40)
		streams[6][i] = byte(bits >> 48)
		streams[7][i] = byte(bits >> 56)
	}

	// 打包
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, uint32(packedNumValues))
	for _, s := range streams {
		buf.Write(s)
	}

	return &EncodedData{
		Data:       buf.Bytes(),
		Type:       format.EncodingBSSEncoding,
		NullBitmap: nullBitmap,
		NumValues:  numValues,
		NullCount:  nullN,
	}, nil
}

func (e *BSSEncoder) EstimateSize(array arrow.Array) int {
	numValues := array.Len()
	nullN := array.NullN()
	// Estimate non-null values
	estimatedNonNull := numValues - nullN
	return estimatedNonNull * GetValueSize(array.DataType().ID())
}

func (e *BSSEncoder) SupportsType(dtype arrow.DataType) bool {
	id := dtype.ID()
	return id == arrow.FLOAT32 || id == arrow.FLOAT64
}
