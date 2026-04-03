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
	numValues := arr.Len()
	nullN := arr.NullN()

	// Handle nulls: extract non-null values and their positions
	var runs []struct {
		value int32
		count uint32
	}

	if nullN == 0 {
		// No nulls: simple RLE on all values
		if len(values) == 0 {
			return nil, ErrEmptyArray
		}

		current := values[0]
		count := uint32(1)

		for i := 1; i < len(values); i++ {
			if values[i] == current {
				count++
			} else {
				runs = append(runs, struct {
					value int32
					count uint32
				}{value: current, count: count})
				current = values[i]
				count = 1
			}
		}
		runs = append(runs, struct {
			value int32
			count uint32
		}{value: current, count: count})

		// Pack: [numRuns:4][(value, count)...]
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.LittleEndian, uint32(len(runs)))
		for _, r := range runs {
			binary.Write(buf, binary.LittleEndian, r.value)
			binary.Write(buf, binary.LittleEndian, r.count)
		}

		return &EncodedData{
			Data:      buf.Bytes(),
			Type:      format.EncodingRLE,
			NumValues: numValues,
			NullCount: 0,
		}, nil
	}

	// Has nulls: extract null bitmap and filter non-null values
	nullBitmap := ExtractNullBitmap(arr)

	// Filter non-null values and encode runs
	var nonNullValues []int32
	for i := 0; i < numValues; i++ {
		if !IsNull(nullBitmap, i) {
			nonNullValues = append(nonNullValues, values[i])
		}
	}

	if len(nonNullValues) == 0 {
		// All nulls: no runs to encode
		return &EncodedData{
			Data:       nil,
			Type:       format.EncodingRLE,
			NullBitmap: nullBitmap,
			NumValues:  numValues,
			NullCount:  nullN,
		}, nil
	}

	// RLE encode non-null values
	current := nonNullValues[0]
	count := uint32(1)

	for i := 1; i < len(nonNullValues); i++ {
		if nonNullValues[i] == current {
			count++
		} else {
			runs = append(runs, struct {
				value int32
				count uint32
			}{value: current, count: count})
			current = nonNullValues[i]
			count = 1
		}
	}
	runs = append(runs, struct {
		value int32
		count uint32
	}{value: current, count: count})

	// Pack
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, uint32(len(runs)))
	for _, r := range runs {
		binary.Write(buf, binary.LittleEndian, r.value)
		binary.Write(buf, binary.LittleEndian, r.count)
	}

	return &EncodedData{
		Data:       buf.Bytes(),
		Type:       format.EncodingRLE,
		NullBitmap: nullBitmap,
		NumValues:  numValues,
		NullCount:  nullN,
	}, nil
}

func (e *RLEEncoder) encodeInt64(arr *arrow.Int64Array) (*EncodedData, error) {
	values := arr.Values()
	numValues := arr.Len()
	nullN := arr.NullN()

	var runs []struct {
		value int64
		count uint32
	}

	if nullN == 0 {
		// No nulls
		if len(values) == 0 {
			return nil, ErrEmptyArray
		}

		current := values[0]
		count := uint32(1)

		for i := 1; i < len(values); i++ {
			if values[i] == current {
				count++
			} else {
				runs = append(runs, struct {
					value int64
					count uint32
				}{value: current, count: count})
				current = values[i]
				count = 1
			}
		}
		runs = append(runs, struct {
			value int64
			count uint32
		}{value: current, count: count})

		buf := new(bytes.Buffer)
		binary.Write(buf, binary.LittleEndian, uint32(len(runs)))
		for _, r := range runs {
			binary.Write(buf, binary.LittleEndian, r.value)
			binary.Write(buf, binary.LittleEndian, r.count)
		}

		return &EncodedData{
			Data:      buf.Bytes(),
			Type:      format.EncodingRLE,
			NumValues: numValues,
			NullCount: 0,
		}, nil
	}

	// Has nulls
	nullBitmap := ExtractNullBitmap(arr)

	var nonNullValues []int64
	for i := 0; i < numValues; i++ {
		if !IsNull(nullBitmap, i) {
			nonNullValues = append(nonNullValues, values[i])
		}
	}

	if len(nonNullValues) == 0 {
		return &EncodedData{
			Data:       nil,
			Type:       format.EncodingRLE,
			NullBitmap: nullBitmap,
			NumValues:  numValues,
			NullCount:  nullN,
		}, nil
	}

	current := nonNullValues[0]
	count := uint32(1)

	for i := 1; i < len(nonNullValues); i++ {
		if nonNullValues[i] == current {
			count++
		} else {
			runs = append(runs, struct {
				value int64
				count uint32
			}{value: current, count: count})
			current = nonNullValues[i]
			count = 1
		}
	}
	runs = append(runs, struct {
		value int64
		count uint32
	}{value: current, count: count})

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, uint32(len(runs)))
	for _, r := range runs {
		binary.Write(buf, binary.LittleEndian, r.value)
		binary.Write(buf, binary.LittleEndian, r.count)
	}

	return &EncodedData{
		Data:       buf.Bytes(),
		Type:       format.EncodingRLE,
		NullBitmap: nullBitmap,
		NumValues:  numValues,
		NullCount:  nullN,
	}, nil
}

func (e *RLEEncoder) EstimateSize(array arrow.Array) int {
	numValues := array.Len()
	valueSize := GetValueSize(array.DataType().ID())
	// Conservative: each value could be a separate run
	return 4 + numValues*(valueSize+4) // numRuns + (value + count) per run
}

func (e *RLEEncoder) SupportsType(dtype arrow.DataType) bool {
	id := dtype.ID()
	return id == arrow.INT32 || id == arrow.INT64
}
