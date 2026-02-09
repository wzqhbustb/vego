package encoding

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/wzqhbkjdx/vego/storage/arrow"
	"github.com/wzqhbkjdx/vego/storage/format"
)

func TestBSSEncoder_Type(t *testing.T) {
	encoder := NewBSSEncoder()
	if encoder.Type() != format.EncodingBSSEncoding {
		t.Errorf("Expected type BSSEncoding, got %v", encoder.Type())
	}
}

func TestBSSEncoder_Encode_Float32(t *testing.T) {
	encoder := NewBSSEncoder()
	decoder := NewBSSDecoder()

	// 创建 Float32Array
	values := make([]float32, 100)
	for i := range values {
		values[i] = float32(i) * 0.001
	}
	array := arrow.NewFloat32Array(values, nil)

	encoded, err := encoder.Encode(array)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	if encoded.Type != format.EncodingBSSEncoding {
		t.Errorf("Expected encoding type BSSEncoding, got %v", encoded.Type)
	}

	// 解码验证
	decoded, err := decoder.Decode(encoded.Data, arrow.PrimFloat32())
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	result := decoded.(*arrow.Float32Array)
	if result.Len() != len(values) {
		t.Errorf("Expected %d values, got %d", len(values), result.Len())
	}

	// 使用字节级比较避免浮点精度问题
	expectedBuf := new(bytes.Buffer)
	for _, v := range values {
		binary.Write(expectedBuf, binary.LittleEndian, v)
	}
	expectedBytes := expectedBuf.Bytes()

	resultBuf := new(bytes.Buffer)
	for i := 0; i < result.Len(); i++ {
		binary.Write(resultBuf, binary.LittleEndian, result.Value(i))
	}
	resultBytes := resultBuf.Bytes()

	if !bytes.Equal(expectedBytes, resultBytes) {
		t.Error("Decoded data mismatch")
	}
}

func TestBSSEncoder_Encode_Float64(t *testing.T) {
	encoder := NewBSSEncoder()
	decoder := NewBSSDecoder()

	// 创建 Float64Array
	values := make([]float64, 50)
	for i := range values {
		values[i] = float64(i) * 0.0001
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

func TestBSSEncoder_NullNotSupported(t *testing.T) {
	encoder := NewBSSEncoder()

	builder := arrow.NewFloat32Builder()
	for i := 0; i < 10; i++ {
		if i%2 == 0 {
			builder.Append(float32(i))
		} else {
			builder.AppendNull()
		}
	}
	array := builder.NewArray()

	_, err := encoder.Encode(array)
	if err != ErrNullNotSupported {
		t.Errorf("Expected ErrNullNotSupported, got %v", err)
	}
}

func TestBSSEncoder_UnsupportedType(t *testing.T) {
	encoder := NewBSSEncoder()

	// Int32 不支持
	values := []int32{1, 2, 3}
	array := arrow.NewInt32Array(values, nil)

	_, err := encoder.Encode(array)
	if err == nil {
		t.Error("Expected error for unsupported type")
	}
}

func TestBSSEncoder_EmptyArray(t *testing.T) {
	encoder := NewBSSEncoder()

	values := []float32{}
	array := arrow.NewFloat32Array(values, nil)

	_, err := encoder.Encode(array)
	if err != ErrEmptyArray {
		t.Errorf("Expected ErrEmptyArray, got %v", err)
	}
}

func TestBSSEncoder_SupportsType(t *testing.T) {
	encoder := NewBSSEncoder()

	if !encoder.SupportsType(arrow.PrimFloat32()) {
		t.Error("Should support Float32")
	}
	if !encoder.SupportsType(arrow.PrimFloat64()) {
		t.Error("Should support Float64")
	}
	if encoder.SupportsType(arrow.PrimInt32()) {
		t.Error("Should not support Int32")
	}
}

func TestBSSEncoder_EstimateSize(t *testing.T) {
	encoder := NewBSSEncoder()

	values := make([]float32, 100)
	array := arrow.NewFloat32Array(values, nil)

	estimated := encoder.EstimateSize(array)
	// 100 floats * 4 bytes = 400 bytes
	if estimated != 400 {
		t.Errorf("Expected estimate 400, got %d", estimated)
	}
}

func BenchmarkBSSEncoder_Encode_Float32(b *testing.B) {
	encoder := NewBSSEncoder()
	values := make([]float32, 10000)
	for i := range values {
		values[i] = float32(i) * 0.001
	}
	array := arrow.NewFloat32Array(values, nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoder.Encode(array)
	}
}

func BenchmarkBSSDecoder_Decode_Float32(b *testing.B) {
	encoder := NewBSSEncoder()
	values := make([]float32, 10000)
	for i := range values {
		values[i] = float32(i) * 0.001
	}
	array := arrow.NewFloat32Array(values, nil)
	encoded, _ := encoder.Encode(array)

	decoder := NewBSSDecoder()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		decoder.Decode(encoded.Data, arrow.PrimFloat32())
	}
}
