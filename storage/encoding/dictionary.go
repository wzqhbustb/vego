package encoding

import (
	"encoding/binary"
	"math"

	lerrors "github.com/wzqhbustb/vego/storage/errors"
	"github.com/wzqhbustb/vego/storage/arrow"
	"github.com/wzqhbustb/vego/storage/format"
)

type DictionaryEncoder struct{}

func NewDictionaryEncoder() *DictionaryEncoder {
	return &DictionaryEncoder{}
}

func (e *DictionaryEncoder) Type() format.EncodingType {
	return format.EncodingDictionary
}

func (e *DictionaryEncoder) Encode(array arrow.Array) (*EncodedData, error) {
	if array.Len() == 0 {
		return nil, ErrEmptyArray
	}

	// 不支持 null
	if array.NullN() > 0 {
		return nil, ErrNullNotSupported
	}

	switch arr := array.(type) {
	case *arrow.Int32Array:
		return e.encodeInt32(arr)
	case *arrow.Int64Array:
		return e.encodeInt64(arr)
	case *arrow.Float32Array:
		return e.encodeFloat32(arr)
	case *arrow.Float64Array:
		return e.encodeFloat64(arr)
	default:
		return nil, lerrors.New(lerrors.ErrUnsupportedType).
			Op("dictionary_encode").
			Build()
	}
}

func (e *DictionaryEncoder) encodeInt32(arr *arrow.Int32Array) (*EncodedData, error) {
	values := arr.Values()

	// 构建字典
	dict := make(map[int32]uint32)
	var dictValues []int32
	indices := make([]uint32, len(values))

	for i, v := range values {
		if idx, ok := dict[v]; ok {
			indices[i] = idx
		} else {
			idx := uint32(len(dictValues))
			dict[v] = idx
			dictValues = append(dictValues, v)
			indices[i] = idx
		}
	}

	return e.packDictionary(dictValues, indices, 4)
}

func (e *DictionaryEncoder) encodeInt64(arr *arrow.Int64Array) (*EncodedData, error) {
	values := arr.Values()

	dict := make(map[int64]uint32)
	var dictValues []int64
	indices := make([]uint32, len(values))

	for i, v := range values {
		if idx, ok := dict[v]; ok {
			indices[i] = idx
		} else {
			idx := uint32(len(dictValues))
			dict[v] = idx
			dictValues = append(dictValues, v)
			indices[i] = idx
		}
	}

	// 将 int64 转换为 bytes 存储
	dictBytes := make([]byte, len(dictValues)*8)
	for i, v := range dictValues {
		binary.LittleEndian.PutUint64(dictBytes[i*8:], uint64(v))
	}

	return e.packDictionaryBytes(dictBytes, indices, 8, uint32(len(dictValues)))
}

func (e *DictionaryEncoder) encodeFloat32(arr *arrow.Float32Array) (*EncodedData, error) {
	values := arr.Values()

	dict := make(map[float32]uint32)
	var dictValues []float32
	indices := make([]uint32, len(values))

	for i, v := range values {
		if idx, ok := dict[v]; ok {
			indices[i] = idx
		} else {
			idx := uint32(len(dictValues))
			dict[v] = idx
			dictValues = append(dictValues, v)
			indices[i] = idx
		}
	}

	// 将 float32 转换为 bytes
	dictBytes := make([]byte, len(dictValues)*4)
	for i, v := range dictValues {
		binary.LittleEndian.PutUint32(dictBytes[i*4:], math.Float32bits(v))
	}

	return e.packDictionaryBytes(dictBytes, indices, 4, uint32(len(dictValues)))
}

func (e *DictionaryEncoder) encodeFloat64(arr *arrow.Float64Array) (*EncodedData, error) {
	values := arr.Values()

	dict := make(map[float64]uint32)
	var dictValues []float64
	indices := make([]uint32, len(values))

	for i, v := range values {
		if idx, ok := dict[v]; ok {
			indices[i] = idx
		} else {
			idx := uint32(len(dictValues))
			dict[v] = idx
			dictValues = append(dictValues, v)
			indices[i] = idx
		}
	}

	dictBytes := make([]byte, len(dictValues)*8)
	for i, v := range dictValues {
		binary.LittleEndian.PutUint64(dictBytes[i*8:], math.Float64bits(v))
	}

	return e.packDictionaryBytes(dictBytes, indices, 8, uint32(len(dictValues)))
}

func (e *DictionaryEncoder) packDictionary(dictValues []int32, indices []uint32, valueSize int) (*EncodedData, error) {
	// 确定索引大小
	indexSize := 2
	if len(dictValues) > 65535 {
		indexSize = 4
	}

	// 计算大小
	headerSize := 10 // valueSize(1) + numEntries(4) + numValues(4) + indexSize(1)
	dictSize := len(dictValues) * valueSize
	indexArraySize := len(indices) * indexSize

	buf := make([]byte, headerSize+dictSize+indexArraySize)
	offset := 0

	// Header
	buf[offset] = byte(valueSize)
	offset++
	binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(dictValues)))
	offset += 4
	binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(indices)))
	offset += 4
	buf[offset] = byte(indexSize)
	offset++

	// Dictionary values
	for _, v := range dictValues {
		binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(v))
		offset += 4
	}

	// Indices
	for _, idx := range indices {
		if indexSize == 2 {
			binary.LittleEndian.PutUint16(buf[offset:offset+2], uint16(idx))
			offset += 2
		} else {
			binary.LittleEndian.PutUint32(buf[offset:offset+4], idx)
			offset += 4
		}
	}

	return &EncodedData{
		Data:     buf,
		Type:     format.EncodingDictionary,
		Metadata: nil,
	}, nil
}

func (e *DictionaryEncoder) packDictionaryBytes(dictBytes []byte, indices []uint32, valueSize int, numEntries uint32) (*EncodedData, error) {
	indexSize := 2
	if numEntries > 65535 {
		indexSize = 4
	}

	headerSize := 10
	dictSize := len(dictBytes)
	indexArraySize := len(indices) * indexSize

	buf := make([]byte, headerSize+dictSize+indexArraySize)
	offset := 0

	buf[offset] = byte(valueSize)
	offset++
	binary.LittleEndian.PutUint32(buf[offset:offset+4], numEntries)
	offset += 4
	binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(indices)))
	offset += 4
	buf[offset] = byte(indexSize)
	offset++

	copy(buf[offset:], dictBytes)
	offset += dictSize

	for _, idx := range indices {
		if indexSize == 2 {
			binary.LittleEndian.PutUint16(buf[offset:offset+2], uint16(idx))
			offset += 2
		} else {
			binary.LittleEndian.PutUint32(buf[offset:offset+4], idx)
			offset += 4
		}
	}

	return &EncodedData{
		Data:     buf,
		Type:     format.EncodingDictionary,
		Metadata: nil,
	}, nil
}

func (e *DictionaryEncoder) EstimateSize(array arrow.Array) int {
	// 保守估计：50% 基数
	numValues := array.Len()
	cardinality := numValues / 2
	if cardinality == 0 {
		cardinality = 1
	}

	valueSize := GetValueSize(array.DataType().ID())
	indexSize := 2
	if cardinality > 65535 {
		indexSize = 4
	}

	return 10 + cardinality*valueSize + numValues*indexSize
}

func (e *DictionaryEncoder) SupportsType(dtype arrow.DataType) bool {
	id := dtype.ID()
	return id == arrow.INT32 || id == arrow.INT64 || id == arrow.FLOAT32 || id == arrow.FLOAT64
}
