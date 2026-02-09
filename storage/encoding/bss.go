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

	// 不支持 null
	if array.NullN() > 0 {
		return nil, ErrNullNotSupported
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
	values := arr.Values()
	numValues := len(values)

	// 创建 4 个 byte stream
	streams := make([][]byte, 4)
	for i := 0; i < 4; i++ {
		streams[i] = make([]byte, numValues)
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
	binary.Write(buf, binary.LittleEndian, uint32(numValues))
	for _, s := range streams {
		buf.Write(s)
	}

	return &EncodedData{
		Data:     buf.Bytes(),
		Type:     format.EncodingBSSEncoding,
		Metadata: nil,
	}, nil
}

func (e *BSSEncoder) encodeFloat64(arr *arrow.Float64Array) (*EncodedData, error) {
	values := arr.Values()
	numValues := len(values)

	// 创建 8 个 byte stream
	streams := make([][]byte, 8)
	for i := 0; i < 8; i++ {
		streams[i] = make([]byte, numValues)
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
	binary.Write(buf, binary.LittleEndian, uint32(numValues))
	for _, s := range streams {
		buf.Write(s)
	}

	return &EncodedData{
		Data:     buf.Bytes(),
		Type:     format.EncodingBSSEncoding,
		Metadata: nil,
	}, nil
}

func (e *BSSEncoder) EstimateSize(array arrow.Array) int {
	return array.Len() * GetValueSize(array.DataType().ID())
}

func (e *BSSEncoder) SupportsType(dtype arrow.DataType) bool {
	id := dtype.ID()
	return id == arrow.FLOAT32 || id == arrow.FLOAT64
}
