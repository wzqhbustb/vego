package encoding

import (
	"encoding/binary"
	"fmt"

	lerrors "github.com/wzqhbkjdx/vego/storage/errors"
	"github.com/wzqhbkjdx/vego/storage/arrow"
)

type BitPackingDecoder struct{}

func NewBitPackingDecoder() *BitPackingDecoder {
	return &BitPackingDecoder{}
}

func (d *BitPackingDecoder) Decode(data []byte, dtype arrow.DataType) (arrow.Array, error) {
	if len(data) < 5 {
		return nil, lerrors.New(lerrors.ErrCorruptedFile).
			Op("bitpacking_decode").
			Context("reason", "data too short for header").
			Context("min_required", 5).
			Context("actual", len(data)).
			Build()
	}

	bitWidth := data[0]
	if bitWidth == 0 || bitWidth > 64 {
		return nil, lerrors.New(lerrors.ErrCorruptedFile).
			Op("bitpacking_decode").
			Context("reason", "invalid bitWidth in header").
			Context("bit_width", bitWidth).
			Build()
	}
	numValues := binary.LittleEndian.Uint32(data[1:5])

	packedData := data[5:]

	// 验证数据长度是否足够
	expectedBits := uint64(numValues) * uint64(bitWidth)
	expectedBytes := (expectedBits + 7) / 8
	if uint64(len(packedData)) < expectedBytes {
		return nil, lerrors.New(lerrors.ErrCorruptedFile).
			Op("bitpacking_decode").
			Context("reason", "data truncated").
			Context("expected", expectedBytes).
			Context("actual", len(packedData)).
			Build()
	}

	switch dtype.ID() {
	case arrow.INT32:
		return d.decodeInt32(packedData, int(numValues), bitWidth)
	case arrow.INT64:
		return d.decodeInt64(packedData, int(numValues), bitWidth)
	default:
		return nil, lerrors.New(lerrors.ErrUnsupportedType).
			Op("bitpacking_decode").
			Context("got_type", fmt.Sprintf("%v", dtype)).
			Build()
	}
}

func (d *BitPackingDecoder) decodeInt32(data []byte, numValues int, bitWidth uint8) (arrow.Array, error) {
	values := unpackBitsToInt32(data, numValues, bitWidth)
	return arrow.NewInt32Array(values, nil), nil
}

func (d *BitPackingDecoder) decodeInt64(data []byte, numValues int, bitWidth uint8) (arrow.Array, error) {
	values := unpackBitsToInt64(data, numValues, bitWidth)
	return arrow.NewInt64Array(values, nil), nil
}

// unpackBitsToInt32 从字节流中解包出多个 int32 值
func unpackBitsToInt32(data []byte, numValues int, bitWidth uint8) []int32 {
	values := make([]int32, numValues)
	if numValues == 0 {
		return values
	}

	var bitOffset uint8 = 0
	byteOffset := 0

	for i := 0; i < numValues; i++ {
		var value uint64
		remainingBits := bitWidth
		var bitsReadInValue uint8 = 0

		for remainingBits > 0 {
			availableBits := 8 - bitOffset
			if availableBits > remainingBits {
				availableBits = remainingBits
			}

			// 从当前字节中提取 bits
			mask := byte((1 << availableBits) - 1)
			bits := (data[byteOffset] >> bitOffset) & mask

			// 将提取的 bits 添加到 value 的正确位置
			value |= uint64(bits) << bitsReadInValue

			bitsReadInValue += availableBits
			remainingBits -= availableBits
			bitOffset += availableBits

			if bitOffset >= 8 {
				bitOffset = 0
				byteOffset++
			}
		}
		values[i] = int32(value)
	}
	return values
}

// unpackBitsToInt64 从字节流中解包出多个 int64 值
func unpackBitsToInt64(data []byte, numValues int, bitWidth uint8) []int64 {
	values := make([]int64, numValues)
	if numValues == 0 {
		return values
	}

	var bitOffset uint8 = 0
	byteOffset := 0

	for i := 0; i < numValues; i++ {
		var value uint64
		remainingBits := bitWidth
		var bitsReadInValue uint8 = 0

		for remainingBits > 0 {
			availableBits := 8 - bitOffset
			if availableBits > remainingBits {
				availableBits = remainingBits
			}

			mask := byte((1 << availableBits) - 1)
			bits := (data[byteOffset] >> bitOffset) & mask

			value |= uint64(bits) << bitsReadInValue

			bitsReadInValue += availableBits
			remainingBits -= availableBits
			bitOffset += availableBits

			if bitOffset >= 8 {
				bitOffset = 0
				byteOffset++
			}
		}
		values[i] = int64(value)
	}
	return values
}
