package arrow

import "math/bits"

// Bitmap represents a compact representation of boolean values
// Used primarily for null masks in Arrow arrays
type Bitmap struct {
	buf    []byte
	length int // number of bits
}

// NewBitmap creates a new bitmap with specified length
func NewBitmap(length int) *Bitmap {
	numBytes := (length + 7) / 8
	return &Bitmap{
		buf:    make([]byte, numBytes),
		length: length,
	}
}

// NewBitmapFromBytes creates a bitmap from existing bytes
func NewBitmapFromBytes(data []byte, length int) *Bitmap {
	return &Bitmap{
		buf:    data,
		length: length,
	}
}

// Len returns the number of bits
func (b *Bitmap) Len() int {
	return b.length
}

// Bytes returns the underlying byte buffer
func (b *Bitmap) Bytes() []byte {
	return b.buf
}

// Set sets the bit at index i to 1
func (b *Bitmap) Set(i int) {
	if i < 0 || i >= b.length {
		panic("bitmap index out of range")
	}
	b.buf[i/8] |= 1 << (i % 8)
}

// Clear sets the bit at index i to 0
func (b *Bitmap) Clear(i int) {
	if i < 0 || i >= b.length {
		panic("bitmap index out of range")
	}
	b.buf[i/8] &^= 1 << (i % 8)
}

// IsSet returns true if bit at index i is 1
func (b *Bitmap) IsSet(i int) bool {
	if i < 0 || i >= b.length {
		panic("bitmap index out of range")
	}
	return (b.buf[i/8] & (1 << (i % 8))) != 0
}

// SetAll sets all bits to 1
func (b *Bitmap) SetAll() {
	for i := range b.buf {
		b.buf[i] = 0xFF
	}
}

// ClearAll sets all bits to 0
func (b *Bitmap) ClearAll() {
	for i := range b.buf {
		b.buf[i] = 0
	}
}

// CountSet returns the number of bits set to 1
func (b *Bitmap) CountSet() int {
	count := 0
	fullBytes := b.length / 8

	// ✅ 批量处理完整字节（8x 加速）
	for i := 0; i < fullBytes; i++ {
		count += bits.OnesCount8(b.buf[i]) // CPU 指令级优化
	}

	// 处理剩余位
	remainder := b.length % 8
	if remainder > 0 {
		mask := byte((1 << remainder) - 1)
		count += bits.OnesCount8(b.buf[fullBytes] & mask)
	}
	return count
}

func (b *Bitmap) Resize(newLength int) {
	if newLength == b.length {
		return
	}

	newNumBytes := (newLength + 7) / 8
	if newNumBytes > len(b.buf) {
		newBuf := make([]byte, newNumBytes)
		copy(newBuf, b.buf)
		b.buf = newBuf
	}
	b.length = newLength
}

// --- Helper: Create bitmap with all values set ---

// NewBitmapAllSet creates a bitmap with all bits set to 1
func NewBitmapAllSet(length int) *Bitmap {
	bm := NewBitmap(length)
	bm.SetAll()
	return bm
}
