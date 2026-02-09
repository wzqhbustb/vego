package arrow

import (
	"encoding/binary"
	"fmt"
	"unsafe"
)

// Buffer represents a contiguous memory region (Arrow's fundamental building block)
type Buffer struct {
	buf []byte
}

// NewBuffer creates a new buffer with specified size
func NewBuffer(size int) *Buffer {
	return &Buffer{
		buf: make([]byte, size),
	}
}

// NewBufferBytes creates a buffer from existing bytes
func NewBufferBytes(data []byte) *Buffer {
	return &Buffer{buf: data}
}

// Bytes returns the underlying byte slice
func (b *Buffer) Bytes() []byte {
	return b.buf
}

// Len returns the buffer length in bytes
func (b *Buffer) Len() int {
	return len(b.buf)
}

// Resize changes the buffer size (may allocate new memory)
func (b *Buffer) Resize(newSize int) {
	if newSize > len(b.buf) {
		newBuf := make([]byte, newSize)
		copy(newBuf, b.buf)
		b.buf = newBuf
	} else {
		b.buf = b.buf[:newSize]
	}
}

// --- Typed Access (zero-copy views) ---

// Int32 returns an int32 view of the buffer
func (b *Buffer) Int32() []int32 {
	if len(b.buf) == 0 {
		return nil
	}
	if len(b.buf)%4 != 0 {
		panic(fmt.Sprintf("buffer size %d not aligned to int32", len(b.buf)))
	}
	return unsafe.Slice((*int32)(unsafe.Pointer(&b.buf[0])), len(b.buf)/4)
}

// Int64 returns an int64 view of the buffer
func (b *Buffer) Int64() []int64 {
	if len(b.buf) == 0 {
		return nil // ← 添加这行
	}
	if len(b.buf)%8 != 0 {
		panic(fmt.Sprintf("buffer size %d not aligned to int64", len(b.buf)))
	}
	return unsafe.Slice((*int64)(unsafe.Pointer(&b.buf[0])), len(b.buf)/8)
}

// Float32 returns a float32 view of the buffer
func (b *Buffer) Float32() []float32 {
	if len(b.buf) == 0 {
		return nil // ← 添加这行
	}
	if len(b.buf)%4 != 0 {
		panic(fmt.Sprintf("buffer size %d not aligned to float32", len(b.buf)))
	}
	return unsafe.Slice((*float32)(unsafe.Pointer(&b.buf[0])), len(b.buf)/4)
}

// Float64 returns a float64 view of the buffer
func (b *Buffer) Float64() []float64 {
	if len(b.buf) == 0 {
		return nil // ← 添加这行
	}
	if len(b.buf)%8 != 0 {
		panic(fmt.Sprintf("buffer size %d not aligned to float64", len(b.buf)))
	}
	return unsafe.Slice((*float64)(unsafe.Pointer(&b.buf[0])), len(b.buf)/8)
}

// --- Factory Functions ---

// NewInt32Buffer creates a buffer from int32 slice
func NewInt32Buffer(data []int32) *Buffer {
	buf := make([]byte, len(data)*4)
	for i, v := range data {
		binary.LittleEndian.PutUint32(buf[i*4:], uint32(v))
	}
	return &Buffer{buf: buf}
}

// NewInt64Buffer creates a buffer from int64 slice
func NewInt64Buffer(data []int64) *Buffer {
	buf := make([]byte, len(data)*8)
	for i, v := range data {
		binary.LittleEndian.PutUint64(buf[i*8:], uint64(v))
	}
	return &Buffer{buf: buf}
}

// NewFloat32Buffer creates a buffer from float32 slice
func NewFloat32Buffer(data []float32) *Buffer {
	buf := make([]byte, len(data)*4)
	for i, v := range data {
		binary.LittleEndian.PutUint32(buf[i*4:], floatBitsToUint32(v))
	}
	return &Buffer{buf: buf}
}

// NewFloat64Buffer creates a buffer from float64 slice
func NewFloat64Buffer(data []float64) *Buffer {
	buf := make([]byte, len(data)*8)
	for i, v := range data {
		binary.LittleEndian.PutUint64(buf[i*8:], floatBitsToUint64(v))
	}
	return &Buffer{buf: buf}
}

// --- Helpers ---

func floatBitsToUint32(f float32) uint32 {
	return *(*uint32)(unsafe.Pointer(&f))
}

func uint32ToFloatBits(u uint32) float32 {
	return *(*float32)(unsafe.Pointer(&u))
}

func floatBitsToUint64(f float64) uint64 {
	return *(*uint64)(unsafe.Pointer(&f))
}

func uint64ToFloatBits(u uint64) float64 {
	return *(*float64)(unsafe.Pointer(&u))
}

// AlignTo64 returns size aligned to 64-byte boundary (Arrow requirement)
func AlignTo64(size int) int {
	return (size + 63) &^ 63
}
