package encoding

import (
	"testing"

	"github.com/wzqhbustb/vego/storage/arrow"
	"github.com/wzqhbustb/vego/storage/format"
)

func TestZstdEncoder_Type(t *testing.T) {
	encoder := NewZstdEncoder(3)
	if encoder.Type() != format.EncodingZstd {
		t.Errorf("Expected type Zstd, got %v", encoder.Type())
	}
}

func TestZstdEncoder_Encode_Int32(t *testing.T) {
	encoder := NewZstdEncoder(3)
	decoder, err := NewZstdDecoder()
	if err != nil {
		t.Fatalf("Failed to create decoder: %v", err)
	}

	// 创建 Int32Array
	values := make([]int32, 1000)
	for i := range values {
		values[i] = int32(i)
	}
	array := arrow.NewInt32Array(values, nil)

	encoded, err := encoder.Encode(array)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	if encoded.Type != format.EncodingZstd {
		t.Errorf("Expected encoding type Zstd, got %v", encoded.Type)
	}

	// 解码验证
	decoded, err := decoder.Decode(encoded.Data, arrow.PrimInt32())
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	result := decoded.(*arrow.Int32Array)
	if result.Len() != len(values) {
		t.Errorf("Expected %d values, got %d", len(values), result.Len())
	}

	for i, expected := range values {
		if result.Value(i) != expected {
			t.Errorf("Value mismatch at %d: expected %d, got %d", i, expected, result.Value(i))
			break
		}
	}
}

func TestZstdEncoder_Encode_Float64(t *testing.T) {
	encoder := NewZstdEncoder(3)
	decoder, err := NewZstdDecoder()
	if err != nil {
		t.Fatalf("Failed to create decoder: %v", err)
	}

	values := make([]float64, 500)
	for i := range values {
		values[i] = float64(i) * 0.5
	}
	array := arrow.NewFloat64Array(values, nil)

	encoded, err := encoder.Encode(array)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := decoder.Decode(encoded.Data, arrow.PrimFloat64())
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	result := decoded.(*arrow.Float64Array)
	for i := 0; i < len(values); i++ {
		if result.Value(i) != values[i] {
			t.Errorf("Value mismatch at %d", i)
			break
		}
	}
}

func TestZstdEncoder_WithNulls(t *testing.T) {
	encoder := NewZstdEncoder(3)
	decoder, err := NewZstdDecoder()
	if err != nil {
		t.Fatalf("Failed to create decoder: %v", err)
	}

	// Zstd 支持 null
	builder := arrow.NewInt32Builder()
	for i := 0; i < 100; i++ {
		if i%10 == 0 {
			builder.AppendNull()
		} else {
			builder.Append(int32(i))
		}
	}
	array := builder.NewArray()

	encoded, err := encoder.Encode(array)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := decoder.Decode(encoded.Data, arrow.PrimInt32())
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	result := decoded.(*arrow.Int32Array)
	nullCount := 0
	for i := 0; i < 100; i++ {
		if i%10 == 0 {
			if result.IsValid(i) {
				t.Errorf("Expected null at index %d", i)
			}
			nullCount++
		} else {
			if !result.IsValid(i) {
				t.Errorf("Expected valid at index %d", i)
			}
			if result.Value(i) != int32(i) {
				t.Errorf("Value mismatch at %d", i)
			}
		}
	}

	if nullCount != 10 {
		t.Errorf("Expected 10 nulls, got %d", nullCount)
	}
}

func TestZstdEncoder_WithAllNulls(t *testing.T) {
	encoder := NewZstdEncoder(3)
	decoder, err := NewZstdDecoder()
	if err != nil {
		t.Fatalf("Failed to create decoder: %v", err)
	}

	builder := arrow.NewInt32Builder()
	for i := 0; i < 100; i++ {
		builder.AppendNull()
	}
	array := builder.NewArray()

	encoded, err := encoder.Encode(array)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := decoder.Decode(encoded.Data, arrow.PrimInt32())
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	result := decoded.(*arrow.Int32Array)
	if result.Len() != 100 {
		t.Errorf("Expected 100 values, got %d", result.Len())
	}
	for i := 0; i < 100; i++ {
		if result.IsValid(i) {
			t.Errorf("Expected null at index %d", i)
		}
	}
	if result.NullN() != 100 {
		t.Errorf("Expected 100 nulls, got %d", result.NullN())
	}
}

func TestZstdEncoder_WithComplexNulls(t *testing.T) {
	encoder := NewZstdEncoder(3)
	decoder, err := NewZstdDecoder()
	if err != nil {
		t.Fatalf("Failed to create decoder: %v", err)
	}

	// 测试一个长度不为8倍数且null分布复杂的数组
	const size = 123
	isValid := make([]bool, size)
	values := make([]int32, size)
	builder := arrow.NewInt32Builder()

	for i := 0; i < size; i++ {
		// 开头结尾和中间的null
		if i < 5 || i > size-5 || i%13 == 0 {
			builder.AppendNull()
			isValid[i] = false
		} else {
			values[i] = int32(i * 2)
			builder.Append(values[i])
			isValid[i] = true
		}
	}
	array := builder.NewArray()

	encoded, err := encoder.Encode(array)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := decoder.Decode(encoded.Data, arrow.PrimInt32())
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	result := decoded.(*arrow.Int32Array)
	if result.Len() != size {
		t.Fatalf("Expected length %d, got %d", size, result.Len())
	}

	for i := 0; i < size; i++ {
		if result.IsValid(i) != isValid[i] {
			t.Errorf("Mismatch IsValid at index %d: expected %v, got %v", i, isValid[i], result.IsValid(i))
		}
		if isValid[i] && result.Value(i) != values[i] {
			t.Errorf("Mismatch value at index %d: expected %d, got %d", i, values[i], result.Value(i))
		}
	}
}

func TestZstdEncoder_EmptyArray(t *testing.T) {
	encoder := NewZstdEncoder(3)

	values := []int32{}
	array := arrow.NewInt32Array(values, nil)

	_, err := encoder.Encode(array)
	if err != ErrEmptyArray {
		t.Errorf("Expected ErrEmptyArray, got %v", err)
	}
}

func TestZstdEncoder_SupportsType(t *testing.T) {
	encoder := NewZstdEncoder(3)

	// Zstd 支持所有类型
	if !encoder.SupportsType(arrow.PrimInt32()) {
		t.Error("Should support Int32")
	}
	if !encoder.SupportsType(arrow.PrimFloat64()) {
		t.Error("Should support Float64")
	}
}

func TestZstdEncoder_EstimateSize(t *testing.T) {
	encoder := NewZstdEncoder(3)

	values := make([]int32, 100)
	array := arrow.NewInt32Array(values, nil)

	estimated := encoder.EstimateSize(array)
	// Should be roughly 50% of original
	original := 100 * 4
	if estimated > original {
		t.Errorf("Estimated size %d > original %d", estimated, original)
	}
}

func TestZstdEncoder_DifferentLevels(t *testing.T) {
	values := make([]int32, 1000)
	for i := range values {
		values[i] = int32(i)
	}
	array := arrow.NewInt32Array(values, nil)

	for _, level := range []int{1, 3, 6, 9} {
		encoder := NewZstdEncoder(level)
		encoded, err := encoder.Encode(array)
		if err != nil {
			t.Fatalf("Level %d encode failed: %v", level, err)
		}
		t.Logf("Level %d: %d bytes", level, len(encoded.Data))
	}
}

func BenchmarkZstdEncoder_Encode(b *testing.B) {
	encoder := NewZstdEncoder(3)
	values := make([]int32, 10000)
	for i := range values {
		values[i] = int32(i)
	}
	array := arrow.NewInt32Array(values, nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoder.Encode(array)
	}
}

func BenchmarkZstdDecoder_Decode(b *testing.B) {
	encoder := NewZstdEncoder(3)
	values := make([]int32, 10000)
	for i := range values {
		values[i] = int32(i)
	}
	array := arrow.NewInt32Array(values, nil)
	encoded, _ := encoder.Encode(array)

	decoder, err := NewZstdDecoder()
	if err != nil {
		b.Fatalf("Failed to create decoder: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		decoder.Decode(encoded.Data, arrow.PrimInt32())
	}
}
