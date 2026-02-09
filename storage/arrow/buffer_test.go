package arrow

import "testing"

func TestNewBuffer(t *testing.T) {
	buf := NewBuffer(100)
	if buf.Len() != 100 {
		t.Errorf("expected length 100, got %d", buf.Len())
	}
	if len(buf.Bytes()) != 100 {
		t.Errorf("expected bytes length 100, got %d", len(buf.Bytes()))
	}
}

func TestBufferResize(t *testing.T) {
	buf := NewBuffer(10)

	// Resize to larger
	buf.Resize(20)
	if buf.Len() != 20 {
		t.Errorf("expected length 20, got %d", buf.Len())
	}

	// Resize to smaller
	buf.Resize(5)
	if buf.Len() != 5 {
		t.Errorf("expected length 5, got %d", buf.Len())
	}
}

func TestBufferInt32View(t *testing.T) {
	data := []int32{1, 2, 3, 4, 5}
	buf := NewInt32Buffer(data)

	if buf.Len() != 20 {
		t.Errorf("expected 20 bytes, got %d", buf.Len())
	}

	view := buf.Int32()
	if len(view) != 5 {
		t.Errorf("expected 5 elements, got %d", len(view))
	}

	for i, v := range view {
		if v != data[i] {
			t.Errorf("element %d: expected %d, got %d", i, data[i], v)
		}
	}
}

func TestBufferInt64View(t *testing.T) {
	data := []int64{100, 200, 300}
	buf := NewInt64Buffer(data)

	if buf.Len() != 24 {
		t.Errorf("expected 24 bytes, got %d", buf.Len())
	}

	view := buf.Int64()
	for i, v := range view {
		if v != data[i] {
			t.Errorf("element %d: expected %d, got %d", i, data[i], v)
		}
	}
}

func TestBufferFloat32View(t *testing.T) {
	data := []float32{1.1, 2.2, 3.3, 4.4}
	buf := NewFloat32Buffer(data)

	view := buf.Float32()
	if len(view) != 4 {
		t.Errorf("expected 4 elements, got %d", len(view))
	}

	for i, v := range view {
		if v != data[i] {
			t.Errorf("element %d: expected %f, got %f", i, data[i], v)
		}
	}
}

func TestBufferFloat64View(t *testing.T) {
	data := []float64{1.111, 2.222, 3.333}
	buf := NewFloat64Buffer(data)

	view := buf.Float64()
	for i, v := range view {
		if v != data[i] {
			t.Errorf("element %d: expected %f, got %f", i, data[i], v)
		}
	}
}

// Test empty buffer handling (critical fix verification)
func TestBufferEmptyInt32(t *testing.T) {
	buf := NewBuffer(0)
	view := buf.Int32()
	if view != nil {
		t.Errorf("expected nil for empty buffer, got %v", view)
	}
}

func TestBufferEmptyInt64(t *testing.T) {
	buf := NewBuffer(0)
	view := buf.Int64()
	if view != nil {
		t.Errorf("expected nil for empty buffer, got %v", view)
	}
}

func TestBufferEmptyFloat32(t *testing.T) {
	buf := NewBuffer(0)
	view := buf.Float32()
	if view != nil {
		t.Errorf("expected nil for empty buffer, got %v", view)
	}
}

func TestBufferEmptyFloat64(t *testing.T) {
	buf := NewBuffer(0)
	view := buf.Float64()
	if view != nil {
		t.Errorf("expected nil for empty buffer, got %v", view)
	}
}

// Test alignment panic
func TestBufferInt32AlignmentPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic for unaligned buffer")
		}
	}()

	buf := NewBuffer(5) // Not aligned to 4 bytes
	buf.Int32()
}

func TestBufferInt64AlignmentPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic for unaligned buffer")
		}
	}()

	buf := NewBuffer(10) // Not aligned to 8 bytes
	buf.Int64()
}

func TestAlignTo64(t *testing.T) {
	tests := []struct {
		input    int
		expected int
	}{
		{0, 0},
		{1, 64},
		{63, 64},
		{64, 64},
		{65, 128},
		{128, 128},
		{129, 192},
	}

	for _, tt := range tests {
		result := AlignTo64(tt.input)
		if result != tt.expected {
			t.Errorf("AlignTo64(%d) = %d, expected %d", tt.input, result, tt.expected)
		}
	}
}

// Benchmark for zero-copy performance
func BenchmarkBufferFloat32View(b *testing.B) {
	data := make([]float32, 768) // Typical vector dimension
	for i := range data {
		data[i] = float32(i)
	}
	buf := NewFloat32Buffer(data)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = buf.Float32()
	}
}
