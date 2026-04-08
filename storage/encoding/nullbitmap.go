// Copyright 2024 Vego Authors
// Licensed under the Apache License, Version 2.0

package encoding

import (
	"encoding/binary"
	"math/bits"

	"github.com/wzqhbustb/vego/storage/arrow"
)

// EncodeNullBitmap encodes a []bool into a compact bitmap.
// nulls[i]=true means null (bit=0), nulls[i]=false means value exists (bit=1)
func EncodeNullBitmap(nulls []bool) []byte {
	n := len(nulls)
	if n == 0 {
		return nil
	}

	bytesNeeded := (n + 7) / 8
	data := make([]byte, bytesNeeded)

	for i, isNull := range nulls {
		if !isNull { // bit=1 means value exists
			byteIdx := i / 8
			bitIdx := uint(i % 8)
			data[byteIdx] |= (1 << bitIdx)
		}
	}
	return data
}

// DecodeNullBitmap decodes a bitmap into []bool.
// Returns a slice where true=null (bit=0), false=value exists (bit=1).
func DecodeNullBitmap(data []byte, n int) []bool {
	if n == 0 {
		return nil
	}

	nulls := make([]bool, n)
	for i := 0; i < n; i++ {
		byteIdx := i / 8
		bitIdx := uint(i % 8)
		nulls[i] = (data[byteIdx] & (1 << bitIdx)) == 0
	}
	return nulls
}

// ExtractNullBitmap extracts the null bitmap from an Arrow array.
// Returns nil if the array has no nulls.
func ExtractNullBitmap(array arrow.Array) []byte {
	if array.NullN() == 0 {
		return nil
	}

	// Get null bitmap from Arrow array
	bitmap := array.Data().NullBitmap()
	if bitmap == nil {
		return nil
	}

	// Arrow stores bitmap as []byte, but we need to copy it
	// because Arrow's bitmap might be shared/reused
	// 注意：Arrow 的 bitmap 可能有 padding（64字节对齐），需要截断到实际大小
	bitmapSize := BitmapSize(array.Len())
	data := make([]byte, bitmapSize)
	copy(data, bitmap.Bytes())
	return data
}

// CountNulls counts the number of nulls in a bitmap.
func CountNulls(bitmap []byte, n int) int {
	if n == 0 {
		return 0
	}
	// 使用 popcount 优化：null 数 = 总数 - 非 null 数
	// 注意：BitmapPopCount 计算所有字节，包括尾部的未使用位
	// 需要屏蔽尾部的多余位
	if len(bitmap) == 0 {
		return n
	}
	
	// 计算实际使用的位数
	usedBits := n
	fullBytes := usedBits / 8
	remainingBits := usedBits % 8
	
	count := 0
	
	// 64-bit 批处理优化：一次处理 8 字节
	fullUint64s := fullBytes / 8
	for i := 0; i < fullUint64s; i++ {
		idx := i * 8
		u64 := binary.LittleEndian.Uint64(bitmap[idx:])
		count += bits.OnesCount64(u64)
	}
	
	// 处理剩余的字节（不足 8 字节）
	for i := fullUint64s * 8; i < fullBytes && i < len(bitmap); i++ {
		count += bits.OnesCount8(bitmap[i])
	}
	
	// 处理最后一个不完整的字节
	if remainingBits > 0 && fullBytes < len(bitmap) {
		// 屏蔽尾部未使用的位（高位），只计算低 remainingBits 位
		lastByte := bitmap[fullBytes]
		mask := byte((1 << remainingBits) - 1)
		count += bits.OnesCount8(lastByte & mask)
	}
	
	return n - count
}

// CountNonNulls counts the number of non-null values in a bitmap.
func CountNonNulls(bitmap []byte, n int) int {
	return n - CountNulls(bitmap, n)
}

// IsNull checks if the value at index i is null.
func IsNull(bitmap []byte, i int) bool {
	byteIdx := i / 8
	bitIdx := uint(i % 8)
	return (bitmap[byteIdx] & (1 << bitIdx)) == 0
}

// SetNull sets the value at index i to null.
func SetNull(bitmap []byte, i int) {
	byteIdx := i / 8
	bitIdx := uint(i % 8)
	bitmap[byteIdx] &^= (1 << bitIdx)
}

// SetNotNull sets the value at index i to not null.
func SetNotNull(bitmap []byte, i int) {
	byteIdx := i / 8
	bitIdx := uint(i % 8)
	bitmap[byteIdx] |= (1 << bitIdx)
}

// Filter extracts non-null values of any type using generics.
func Filter[T any](values []T, nulls []bool) []T {
	result := make([]T, 0, len(values))
	for i, v := range values {
		if !nulls[i] {
			result = append(result, v)
		}
	}
	return result
}

// FilterInt32 extracts non-null int32 values.
func FilterInt32(values []int32, nulls []bool) []int32 {
	return Filter(values, nulls)
}

// FilterInt64 extracts non-null int64 values.
func FilterInt64(values []int64, nulls []bool) []int64 {
	return Filter(values, nulls)
}

// FilterFloat32 extracts non-null float32 values.
func FilterFloat32(values []float32, nulls []bool) []float32 {
	return Filter(values, nulls)
}

// FilterFloat64 extracts non-null float64 values.
func FilterFloat64(values []float64, nulls []bool) []float64 {
	return Filter(values, nulls)
}

// Expand expands values of any type using a null bitmap to restore original array.
// nullBitmap: bit=1 means value exists at that position in values slice
// values: non-null values in original order
// n: total number of values (including nulls)
// Returns error if values length doesn't match bitmap non-null count (data corruption).
func Expand[T any](values []T, nullBitmap []byte, n int) ([]T, error) {
	if n == 0 {
		return nil, nil
	}

	result := make([]T, n)
	valIdx := 0
	for i := 0; i < n; i++ {
		if !IsNull(nullBitmap, i) {
			if valIdx >= len(values) {
				return nil, ErrCorruptedNullBitmap
			}
			result[i] = values[valIdx]
			valIdx++
		}
	}
	if valIdx != len(values) {
		return nil, ErrCorruptedNullBitmap
	}
	return result, nil
}

// ExpandInt32 expands values using a null bitmap to restore original array.
func ExpandInt32(values []int32, nullBitmap []byte, n int) ([]int32, error) {
	return Expand(values, nullBitmap, n)
}

// ExpandInt64 expands int64 values using a null bitmap.
func ExpandInt64(values []int64, nullBitmap []byte, n int) ([]int64, error) {
	return Expand(values, nullBitmap, n)
}

// ExpandFloat32 expands float32 values using a null bitmap.
func ExpandFloat32(values []float32, nullBitmap []byte, n int) ([]float32, error) {
	return Expand(values, nullBitmap, n)
}

// ExpandFloat64 expands float64 values using a null bitmap.
func ExpandFloat64(values []float64, nullBitmap []byte, n int) ([]float64, error) {
	return Expand(values, nullBitmap, n)
}

// BitmapSize returns the number of bytes needed to store n bits.
func BitmapSize(n int) int {
	return (n + 7) / 8
}

// BitmapPopCount returns the number of set bits (1s) in the bitmap.
// Note: This counts ALL bits in the byte slice, including unused bits in the last byte.
// For accurate count of valid bits, use CountNonNulls(bitmap, n) instead.
func BitmapPopCount(bitmap []byte) int {
	count := 0
	for _, b := range bitmap {
		count += bits.OnesCount8(b)
	}
	return count
}

// CloneBitmap creates a copy of the bitmap.
func CloneBitmap(bitmap []byte) []byte {
	if bitmap == nil {
		return nil
	}
	clone := make([]byte, len(bitmap))
	copy(clone, bitmap)
	return clone
}
