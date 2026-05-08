package encoding

import (
	"encoding/binary"
	"math"

	lerrors "github.com/wzqhbustb/vego/storage/errors"
	"github.com/wzqhbustb/vego/core"
	"github.com/wzqhbustb/vego/storage/format"
)

// DictionaryEncoder encodes values as dictionary indices with full null support.
// Null values are excluded from the dictionary and restored using null bitmap.
type DictionaryEncoder struct{}

func NewDictionaryEncoder() *DictionaryEncoder {
	return &DictionaryEncoder{}
}

func (e *DictionaryEncoder) Type() format.EncodingType {
	return format.EncodingDictionary
}

func (e *DictionaryEncoder) Encode(array core.Array) (*EncodedData, error) {
	if array.Len() == 0 {
		return nil, ErrEmptyArray
	}

	// 如果有 null，使用带 null 支持的编码
	if array.NullN() > 0 {
		return e.encodeWithNulls(array)
	}

	switch arr := array.(type) {
	case *core.Int32Array:
		return e.encodeInt32(arr)
	case *core.Int64Array:
		return e.encodeInt64(arr)
	case *core.Float32Array:
		return e.encodeFloat32(arr)
	case *core.Float64Array:
		return e.encodeFloat64(arr)
	default:
		return nil, lerrors.New(lerrors.ErrUnsupportedType).
			Op("dictionary_encode").
			Build()
	}
}

func (e *DictionaryEncoder) encodeWithNulls(array core.Array) (*EncodedData, error) {
	nullBitmap := ExtractNullBitmap(array) // 使用复制版本
	nulls := DecodeNullBitmap(nullBitmap, array.Len())

	switch arr := array.(type) {
	case *core.Int32Array:
		values := FilterInt32(arr.Values(), nulls)
		return e.encodeInt32WithValues(values, nullBitmap, array.Len(), array.NullN())
	case *core.Int64Array:
		values := FilterInt64(arr.Values(), nulls)
		return e.encodeInt64WithValues(values, nullBitmap, array.Len(), array.NullN())
	case *core.Float32Array:
		values := FilterFloat32(arr.Values(), nulls)
		return e.encodeFloat32WithValues(values, nullBitmap, array.Len(), array.NullN())
	case *core.Float64Array:
		values := FilterFloat64(arr.Values(), nulls)
		return e.encodeFloat64WithValues(values, nullBitmap, array.Len(), array.NullN())
	default:
		return nil, lerrors.New(lerrors.ErrUnsupportedType).
			Op("dictionary_encode_with_nulls").
			Build()
	}
}

func (e *DictionaryEncoder) encodeInt32(arr *core.Int32Array) (*EncodedData, error) {
	return e.encodeInt32WithValues(arr.Values(), nil, arr.Len(), 0)
}

func (e *DictionaryEncoder) encodeInt32WithValues(values []int32, nullBitmap []byte, numValues, nullCount int) (*EncodedData, error) {
	// 构建字典（只包含非 null 值）
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

	return e.packDictionaryWithNulls(dictValues, indices, 4, nullBitmap, numValues, nullCount)
}

func (e *DictionaryEncoder) encodeInt64(arr *core.Int64Array) (*EncodedData, error) {
	return e.encodeInt64WithValues(arr.Values(), nil, arr.Len(), 0)
}

func (e *DictionaryEncoder) encodeInt64WithValues(values []int64, nullBitmap []byte, numValues, nullCount int) (*EncodedData, error) {
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

	return e.packDictionaryBytesWithNulls(dictBytes, indices, 8, uint32(len(dictValues)), nullBitmap, numValues, nullCount)
}

func (e *DictionaryEncoder) encodeFloat32(arr *core.Float32Array) (*EncodedData, error) {
	return e.encodeFloat32WithValues(arr.Values(), nil, arr.Len(), 0)
}

func (e *DictionaryEncoder) encodeFloat32WithValues(values []float32, nullBitmap []byte, numValues, nullCount int) (*EncodedData, error) {
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

	return e.packDictionaryBytesWithNulls(dictBytes, indices, 4, uint32(len(dictValues)), nullBitmap, numValues, nullCount)
}

func (e *DictionaryEncoder) encodeFloat64(arr *core.Float64Array) (*EncodedData, error) {
	return e.encodeFloat64WithValues(arr.Values(), nil, arr.Len(), 0)
}

func (e *DictionaryEncoder) encodeFloat64WithValues(values []float64, nullBitmap []byte, numValues, nullCount int) (*EncodedData, error) {
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

	return e.packDictionaryBytesWithNulls(dictBytes, indices, 8, uint32(len(dictValues)), nullBitmap, numValues, nullCount)
}

func (e *DictionaryEncoder) packDictionary(dictValues []int32, indices []uint32, valueSize int) (*EncodedData, error) {
	return e.packDictionaryWithNulls(dictValues, indices, valueSize, nil, len(indices), 0)
}

func (e *DictionaryEncoder) packDictionaryWithNulls(dictValues []int32, indices []uint32, valueSize int, nullBitmap []byte, numValues, nullCount int) (*EncodedData, error) {
	// 确定索引大小
	indexSize := 2
	if len(dictValues) > 65535 {
		indexSize = 4
	}

	// 计算大小
	headerSize := 10 // valueSize(1) + numEntries(4) + packedNumValues(4) + indexSize(1)
	dictSize := len(dictValues) * valueSize
	indexArraySize := len(indices) * indexSize

	buf := make([]byte, headerSize+dictSize+indexArraySize)
	offset := 0

	// Header
	buf[offset] = byte(valueSize)
	offset++
	binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(dictValues)))
	offset += 4
	binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(indices))) // packedNumValues (non-null count)
	offset += 4
	buf[offset] = byte(indexSize)
	offset++

	// Dictionary values
	for _, v := range dictValues {
		binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(v))
		offset += 4
	}

	// Indices (only for non-null values)
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
		Data:       buf,
		Type:       format.EncodingDictionary,
		Metadata:   nil,
		NullBitmap: nullBitmap,
		NumValues:  numValues,
		NullCount:  nullCount,
	}, nil
}

func (e *DictionaryEncoder) packDictionaryBytes(dictBytes []byte, indices []uint32, valueSize int, numEntries uint32) (*EncodedData, error) {
	return e.packDictionaryBytesWithNulls(dictBytes, indices, valueSize, numEntries, nil, len(indices), 0)
}

func (e *DictionaryEncoder) packDictionaryBytesWithNulls(dictBytes []byte, indices []uint32, valueSize int, numEntries uint32, nullBitmap []byte, numValues, nullCount int) (*EncodedData, error) {
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
	binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(indices))) // packedNumValues
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
		Data:       buf,
		Type:       format.EncodingDictionary,
		Metadata:   nil,
		NullBitmap: nullBitmap,
		NumValues:  numValues,
		NullCount:  nullCount,
	}, nil
}

func (e *DictionaryEncoder) EstimateSize(array core.Array) int {
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

func (e *DictionaryEncoder) SupportsType(dtype core.DataType) bool {
	id := dtype.ID()
	return id == core.INT32 || id == core.INT64 || id == core.FLOAT32 || id == core.FLOAT64
}
