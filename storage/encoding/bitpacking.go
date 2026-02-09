package encoding

import (
	"encoding/binary"
	"fmt"

	lerrors "github.com/wzqhbkjdx/vego/storage/errors"
	"github.com/wzqhbkjdx/vego/storage/arrow"
	"github.com/wzqhbkjdx/vego/storage/format"
)

type BitPackingEncoder struct {
	bitWidth uint8
}

func NewBitPackingEncoder(bitWidth uint8) *BitPackingEncoder {
	if bitWidth < 1 {
		bitWidth = 1
	}
	if bitWidth > 64 {
		bitWidth = 64
	}
	return &BitPackingEncoder{bitWidth: bitWidth}
}

func (e *BitPackingEncoder) Type() format.EncodingType {
	return format.EncodingBitPacked
}

func (e *BitPackingEncoder) Encode(array arrow.Array) (*EncodedData, error) {
	if array.Len() == 0 {
		return nil, ErrEmptyArray
	}

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
			Op("bitpacking_encode").
			Context("got_type", fmt.Sprintf("%T", array)).
			Build()
	}
}

func (e *BitPackingEncoder) encodeInt32(arr *arrow.Int32Array) (*EncodedData, error) {
	values := arr.Values()
	numValues := len(values)

	maxVal := (uint64(1) << e.bitWidth) - 1
	for i, v := range values {
		if v < 0 {
			return nil, lerrors.New(lerrors.ErrInvalidArgument).
				Op("bitpacking_encode_int32").
				Context("reason", "negative value not supported").
				Context("value", v).
				Context("index", i).
				Build()
		}
		if uint64(v) > maxVal {
			return nil, lerrors.New(lerrors.ErrInvalidArgument).
				Op("bitpacking_encode_int32").
				Context("reason", "value exceeds max for bitWidth").
				Context("value", v).
				Context("index", i).
				Context("max", maxVal).
				Context("bit_width", e.bitWidth).
				Build()
		}
	}

	packed := packBitsInt32(values, e.bitWidth)

	buf := make([]byte, 5+len(packed))
	buf[0] = e.bitWidth
	binary.LittleEndian.PutUint32(buf[1:5], uint32(numValues))
	copy(buf[5:], packed)

	return &EncodedData{
		Data:     buf,
		Type:     format.EncodingBitPacked,
		Metadata: nil,
	}, nil
}

func (e *BitPackingEncoder) encodeInt64(arr *arrow.Int64Array) (*EncodedData, error) {
	values := arr.Values()
	numValues := len(values)

	maxVal := (uint64(1) << e.bitWidth) - 1
	for i, v := range values {
		if v < 0 {
			return nil, lerrors.New(lerrors.ErrInvalidArgument).
				Op("bitpacking_encode_int64").
				Context("reason", "negative value not supported").
				Context("value", v).
				Context("index", i).
				Build()
		}
		if uint64(v) > maxVal {
			return nil, lerrors.New(lerrors.ErrInvalidArgument).
				Op("bitpacking_encode_int64").
				Context("reason", "value exceeds max for bitWidth").
				Context("value", v).
				Context("index", i).
				Context("max", maxVal).
				Context("bit_width", e.bitWidth).
				Build()
		}
	}

	packed := packBitsInt64(values, e.bitWidth)

	buf := make([]byte, 5+len(packed))
	buf[0] = e.bitWidth
	binary.LittleEndian.PutUint32(buf[1:5], uint32(numValues))
	copy(buf[5:], packed)

	return &EncodedData{
		Data:     buf,
		Type:     format.EncodingBitPacked,
		Metadata: nil,
	}, nil
}

func packBitsInt32(values []int32, bitWidth uint8) []byte {
	if len(values) == 0 {
		return []byte{}
	}

	numValues := len(values)
	totalBits := uint64(numValues) * uint64(bitWidth)
	totalBytes := (totalBits + 7) / 8
	result := make([]byte, totalBytes)

	var bitOffset uint8 = 0
	byteOffset := 0

	for _, v := range values {
		value := uint64(uint32(v))
		remainingBits := bitWidth

		for remainingBits > 0 {
			availableBits := 8 - bitOffset
			if availableBits > remainingBits {
				availableBits = remainingBits
			}

			mask := uint64((1 << availableBits) - 1)
			bitsToWrite := value & mask

			result[byteOffset] |= byte(bitsToWrite << bitOffset)

			value >>= availableBits
			remainingBits -= availableBits
			bitOffset += availableBits

			if bitOffset >= 8 {
				bitOffset = 0
				byteOffset++
			}
		}
	}
	return result
}

func packBitsInt64(values []int64, bitWidth uint8) []byte {
	if len(values) == 0 {
		return []byte{}
	}

	numValues := len(values)
	totalBits := uint64(numValues) * uint64(bitWidth)
	totalBytes := (totalBits + 7) / 8
	result := make([]byte, totalBytes)

	var bitOffset uint8 = 0
	byteOffset := 0

	for _, v := range values {
		value := uint64(v)
		remainingBits := bitWidth

		for remainingBits > 0 {
			availableBits := 8 - bitOffset
			if availableBits > remainingBits {
				availableBits = remainingBits
			}

			mask := uint64((1 << availableBits) - 1)
			bitsToWrite := value & mask

			result[byteOffset] |= byte(bitsToWrite << bitOffset)

			value >>= availableBits
			remainingBits -= availableBits
			bitOffset += availableBits

			if bitOffset >= 8 {
				bitOffset = 0
				byteOffset++
			}
		}
	}
	return result
}

func (e *BitPackingEncoder) EstimateSize(array arrow.Array) int {
	numValues := array.Len()
	// Header size (5) + packed data size
	return 5 + (numValues*int(e.bitWidth)+7)/8
}

func (e *BitPackingEncoder) SupportsType(dtype arrow.DataType) bool {
	id := dtype.ID()
	return id == arrow.INT32 || id == arrow.INT64
}
