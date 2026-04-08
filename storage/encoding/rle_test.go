package encoding

import (
	"testing"

	"github.com/wzqhbustb/vego/storage/arrow"
	"github.com/wzqhbustb/vego/storage/format"
)

func TestRLEEncoder_Type(t *testing.T) {
	encoder := NewRLEEncoder()
	if encoder.Type() != format.EncodingRLE {
		t.Errorf("Expected type RLE, got %v", encoder.Type())
	}
}

func TestRLEEncoder_Encode_Int32(t *testing.T) {
	encoder := NewRLEEncoder()
	decoder := NewRLEDecoder()

	// 创建 RLE 友好的数据：长连续重复
	values := make([]int32, 1000)
	for i := range values {
		values[i] = int32(i / 100) // 0,0,0...,1,1,1...,2,2,2...
	}
	array := arrow.NewInt32Array(values, nil)

	encoded, err := encoder.Encode(array)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	if encoded.Type != format.EncodingRLE {
		t.Errorf("Expected encoding type RLE, got %v", encoded.Type)
	}

	// 解码验证
	decoded, err := decoder.Decode(encoded.Data, arrow.PrimInt32(), nil, encoded.NumValues)
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

func TestRLEEncoder_Encode_Int64(t *testing.T) {
	encoder := NewRLEEncoder()
	decoder := NewRLEDecoder()

	// Int64 数据
	values := make([]int64, 500)
	for i := range values {
		values[i] = int64(i / 50)
	}
	array := arrow.NewInt64Array(values, nil)

	encoded, err := encoder.Encode(array)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := decoder.Decode(encoded.Data, arrow.PrimInt64(), nil, encoded.NumValues)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	result := decoded.(*arrow.Int64Array)
	for i, expected := range values {
		if result.Value(i) != expected {
			t.Errorf("Value mismatch at %d", i)
			break
		}
	}
}

func TestRLEEncoder_WithNulls(t *testing.T) {
	encoder := NewRLEEncoder()
	decoder := NewRLEDecoder()

	builder := arrow.NewInt32Builder()
	for i := 0; i < 10; i++ {
		if i%2 == 0 {
			builder.Append(int32(i))
		} else {
			builder.AppendNull()
		}
	}
	array := builder.NewArray()

	encoded, err := encoder.Encode(array)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	if encoded.NullCount != 5 {
		t.Errorf("Expected NullCount=5, got %d", encoded.NullCount)
	}
	if encoded.NumValues != 10 {
		t.Errorf("Expected NumValues=10, got %d", encoded.NumValues)
	}

	// 解码并验证
	decoded, err := decoder.Decode(encoded.Data, arrow.PrimInt32(), encoded.NullBitmap, encoded.NumValues)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	result := decoded.(*arrow.Int32Array)
	if result.Len() != 10 {
		t.Fatalf("Expected 10 values, got %d", result.Len())
	}

	// 验证 null 位置
	for i := 0; i < 10; i++ {
		if i%2 == 0 {
			if result.IsNull(i) {
				t.Errorf("Expected non-null at %d", i)
			}
			if result.Value(i) != int32(i) {
				t.Errorf("Value mismatch at %d: expected %d, got %d", i, i, result.Value(i))
			}
		} else {
			if !result.IsNull(i) {
				t.Errorf("Expected null at %d", i)
			}
		}
	}
}

func TestRLEEncoder_UnsupportedType(t *testing.T) {
	encoder := NewRLEEncoder()

	values := []float32{1.0, 2.0, 3.0}
	array := arrow.NewFloat32Array(values, nil)

	_, err := encoder.Encode(array)
	if err == nil {
		t.Error("Expected error for unsupported type")
	}
}

func TestRLEEncoder_EmptyArray(t *testing.T) {
	encoder := NewRLEEncoder()

	values := []int32{}
	array := arrow.NewInt32Array(values, nil)

	_, err := encoder.Encode(array)
	if err != ErrEmptyArray {
		t.Errorf("Expected ErrEmptyArray, got %v", err)
	}
}

func TestRLEEncoder_SupportsType(t *testing.T) {
	encoder := NewRLEEncoder()

	if !encoder.SupportsType(arrow.PrimInt32()) {
		t.Error("Should support Int32")
	}
	if !encoder.SupportsType(arrow.PrimInt64()) {
		t.Error("Should support Int64")
	}
	if encoder.SupportsType(arrow.PrimFloat32()) {
		t.Error("Should not support Float32")
	}
}

func TestRLEEncoder_EstimateSize(t *testing.T) {
	encoder := NewRLEEncoder()

	values := make([]int32, 100)
	for i := range values {
		values[i] = int32(i / 10) // 10 runs
	}
	array := arrow.NewInt32Array(values, nil)

	estimated := encoder.EstimateSize(array)
	// Should be reasonable
	if estimated <= 0 {
		t.Error("EstimateSize should be positive")
	}
}

func TestRLEEncoder_SingleRun(t *testing.T) {
	encoder := NewRLEEncoder()
	decoder := NewRLEDecoder()

	// 所有值相同
	values := make([]int32, 1000)
	for i := range values {
		values[i] = 42
	}
	array := arrow.NewInt32Array(values, nil)

	encoded, err := encoder.Encode(array)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// 应该压缩得很好
	if len(encoded.Data) > 100 {
		t.Logf("Single run compression: %d bytes for 1000 values", len(encoded.Data))
	}

	decoded, err := decoder.Decode(encoded.Data, arrow.PrimInt32(), nil, encoded.NumValues)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	result := decoded.(*arrow.Int32Array)
	if result.Len() != 1000 {
		t.Errorf("Expected 1000 values, got %d", result.Len())
	}
	for i := 0; i < 1000; i++ {
		if result.Value(i) != 42 {
			t.Errorf("Value mismatch at %d", i)
			break
		}
	}
}

func BenchmarkRLEEncoder_Encode(b *testing.B) {
	encoder := NewRLEEncoder()
	values := make([]int32, 10000)
	for i := range values {
		values[i] = int32(i / 100)
	}
	array := arrow.NewInt32Array(values, nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoder.Encode(array)
	}
}

func BenchmarkRLEDecoder_Decode(b *testing.B) {
	encoder := NewRLEEncoder()
	values := make([]int32, 10000)
	for i := range values {
		values[i] = int32(i / 100)
	}
	array := arrow.NewInt32Array(values, nil)
	encoded, _ := encoder.Encode(array)

	decoder := NewRLEDecoder()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		decoder.Decode(encoded.Data, arrow.PrimInt32(), nil, encoded.NumValues)
	}
}
