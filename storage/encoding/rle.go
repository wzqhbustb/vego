package encoding

import (
	"bytes"
	"encoding/binary"

	lerrors "github.com/wzqhbustb/vego/storage/errors"
	"github.com/wzqhbustb/vego/storage/arrow"
	"github.com/wzqhbustb/vego/storage/format"
)

type RLEEncoder struct{}

func NewRLEEncoder() *RLEEncoder {
	return &RLEEncoder{}
}

func (e *RLEEncoder) Type() format.EncodingType {
	return format.EncodingRLE
}

func (e *RLEEncoder) Encode(array arrow.Array) (*EncodedData, error) {
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
	default:
		return nil, lerrors.New(lerrors.ErrUnsupportedType).
			Op("rle_encode").
			Build()
	}
}

func (e *RLEEncoder) encodeInt32(arr *arrow.Int32Array) (*EncodedData, error) {
	values := arr.Values()
	if len(values) == 0 {
		return nil, ErrEmptyArray
	}

	// 编码 runs
	type run struct {
		value int32
		count uint32
	}
	var runs []run

	current := values[0]
	count := uint32(1)

	for i := 1; i < len(values); i++ {
		if values[i] == current {
			count++
		} else {
			runs = append(runs, run{value: current, count: count})
			current = values[i]
			count = 1
		}
	}
	runs = append(runs, run{value: current, count: count})

	// 打包: [numRuns:4][(value, count)...]
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, uint32(len(runs)))
	for _, r := range runs {
		binary.Write(buf, binary.LittleEndian, r.value)
		binary.Write(buf, binary.LittleEndian, r.count)
	}

	return &EncodedData{
		Data:     buf.Bytes(),
		Type:     format.EncodingRLE,
		Metadata: nil,
	}, nil
}

func (e *RLEEncoder) encodeInt64(arr *arrow.Int64Array) (*EncodedData, error) {
	values := arr.Values()
	if len(values) == 0 {
		return nil, ErrEmptyArray
	}

	type run struct {
		value int64
		count uint32
	}
	var runs []run

	current := values[0]
	count := uint32(1)

	for i := 1; i < len(values); i++ {
		if values[i] == current {
			count++
		} else {
			runs = append(runs, run{value: current, count: count})
			current = values[i]
			count = 1
		}
	}
	runs = append(runs, run{value: current, count: count})

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, uint32(len(runs)))
	for _, r := range runs {
		binary.Write(buf, binary.LittleEndian, r.value)
		binary.Write(buf, binary.LittleEndian, r.count)
	}

	return &EncodedData{
		Data:     buf.Bytes(),
		Type:     format.EncodingRLE,
		Metadata: nil,
	}, nil
}

func (e *RLEEncoder) EstimateSize(array arrow.Array) int {
	// 保守估计：每个值一个 run
	numValues := array.Len()
	valueSize := GetValueSize(array.DataType().ID())
	return 4 + numValues*(valueSize+4) // numRuns + (value + count) per run
}

func (e *RLEEncoder) SupportsType(dtype arrow.DataType) bool {
	id := dtype.ID()
	return id == arrow.INT32 || id == arrow.INT64
}
