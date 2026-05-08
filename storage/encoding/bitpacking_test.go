package encoding

import (
	"fmt"
	"testing"

	"github.com/wzqhbustb/vego/core"
	"github.com/wzqhbustb/vego/storage/format"
)

func TestBitPackingEncoder_Type(t *testing.T) {
	encoder := NewBitPackingEncoder(8)
	if encoder.Type() != format.EncodingBitPacked {
		t.Errorf("Expected type BitPacked, got %v", encoder.Type())
	}
}

// TestBitPacking_Roundtrip is the core test for bitpacking.
// It tests various combinations of bit widths and array lengths.
func TestBitPacking_Roundtrip(t *testing.T) {
	testCases := []struct {
		bitWidth int
		len      int
	}{
		{1, 1},
		{1, 7},
		{1, 8},
		{1, 9},
		{1, 1000},
		{3, 1},
		{3, 7}, // 21 bits
		{3, 8}, // 24 bits (3 bytes)
		{3, 9}, // 27 bits
		{3, 1000},
		{8, 100},
		{12, 1},
		{12, 2}, // 24 bits (3 bytes)
		{12, 3}, // 36 bits
		{12, 1000},
		{31, 100},
		{32, 100},
	}

	for _, tc := range testCases {
		name := fmt.Sprintf("bitWidth=%d,len=%d", tc.bitWidth, tc.len)
		t.Run(name, func(t *testing.T) {
			encoder := NewBitPackingEncoder(uint8(tc.bitWidth))
			decoder := NewBitPackingDecoder()

			maxValue := uint64(1)<<tc.bitWidth - 1
			values := make([]int32, tc.len)
			for i := range values {
				// Create a pattern to test different values
				values[i] = int32(uint64(i) % (maxValue + 1))
			}
			array := core.NewInt32Array(values, nil)

			encoded, err := encoder.Encode(array)
			if err != nil {
				t.Fatalf("Encode failed: %v", err)
			}
			if encoded.Type != format.EncodingBitPacked {
				t.Errorf("Expected encoding type BitPacked, got %v", encoded.Type)
			}

			// Verify encoded length
			expectedBytes := (tc.len*tc.bitWidth + 7) / 8
			// Header: bitWidth (1 byte) + length (4 bytes)
			if len(encoded.Data) != 1+4+expectedBytes {
				t.Errorf("Expected encoded length %d, got %d", 1+4+expectedBytes, len(encoded.Data))
			}

			decoded, err := decoder.Decode(encoded.Data, core.PrimInt32(), nil, encoded.NumValues)
			if err != nil {
				t.Fatalf("Decode failed: %v", err)
			}

			result := decoded.(*core.Int32Array)
			if result.Len() != len(values) {
				t.Fatalf("Expected %d values, got %d", len(values), result.Len())
			}

			for i, expected := range values {
				if result.Value(i) != expected {
					t.Errorf("Value mismatch at %d: expected %d, got %d", i, expected, result.Value(i))
					// Stop after first error for readability
					return
				}
			}
		})
	}
}

func TestBitPackingEncoder_Encode_Int64(t *testing.T) {
	encoder := NewBitPackingEncoder(16)
	decoder := NewBitPackingDecoder()

	// 创建 Int64Array
	values := make([]int64, 50)
	for i := range values {
		values[i] = int64(i * 100)
	}
	array := core.NewInt64Array(values, nil)

	encoded, err := encoder.Encode(array)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := decoder.Decode(encoded.Data, core.PrimInt64(), nil, encoded.NumValues)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	result := decoded.(*core.Int64Array)
	for i, expected := range values {
		if result.Value(i) != expected {
			t.Errorf("Value mismatch at %d", i)
			break
		}
	}
}

func TestBitPackingEncoder_WithNulls(t *testing.T) {
	encoder := NewBitPackingEncoder(8)
	decoder := NewBitPackingDecoder()

	// 创建包含 null 的数组
	builder := core.NewInt32Builder()
	expectedValues := make([]int32, 0, 5)
	for i := 0; i < 10; i++ {
		if i%2 == 0 {
			builder.Append(int32(i))
			expectedValues = append(expectedValues, int32(i))
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
	if encoded.NullBitmap == nil {
		t.Error("Expected NullBitmap to be set")
	}

	// 解码并验证
	decoded, err := decoder.Decode(encoded.Data, core.PrimInt32(), encoded.NullBitmap, encoded.NumValues)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	result := decoded.(*core.Int32Array)
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

func TestBitPackingEncoder_UnsupportedType(t *testing.T) {
	encoder := NewBitPackingEncoder(8)

	// Float32 不支持
	values := []float32{1.0, 2.0, 3.0}
	array := core.NewFloat32Array(values, nil)

	_, err := encoder.Encode(array)
	if err == nil {
		t.Error("Expected error for unsupported type")
	}
}

func TestBitPackingEncoder_ValueTooLarge(t *testing.T) {
	encoder := NewBitPackingEncoder(4) // 只能表示 0-15

	values := []int32{1, 2, 3, 100} // 100 > 15
	array := core.NewInt32Array(values, nil)

	_, err := encoder.Encode(array)
	if err == nil {
		t.Error("Expected error for value exceeding bit width")
	}
}

func TestBitPackingEncoder_NegativeValue(t *testing.T) {
	encoder := NewBitPackingEncoder(8)
	array := core.NewInt32Array([]int32{1, 2, -3}, nil)
	_, err := encoder.Encode(array)
	if err == nil {
		t.Error("Expected error for negative value")
	}
}

func TestBitPackingEncoder_EmptyArray(t *testing.T) {
	encoder := NewBitPackingEncoder(8)

	values := []int32{}
	array := core.NewInt32Array(values, nil)

	_, err := encoder.Encode(array)
	if err != ErrEmptyArray {
		t.Errorf("Expected ErrEmptyArray, got %v", err)
	}
}

func TestBitPackingEncoder_SupportsType(t *testing.T) {
	encoder := NewBitPackingEncoder(8)

	if !encoder.SupportsType(core.PrimInt32()) {
		t.Error("Should support Int32")
	}
	if !encoder.SupportsType(core.PrimInt64()) {
		t.Error("Should support Int64")
	}
	if encoder.SupportsType(core.PrimFloat32()) {
		t.Error("Should not support Float32")
	}
}

func TestBitPackingEncoder_EstimateSize(t *testing.T) {
	encoder := NewBitPackingEncoder(8)

	values := make([]int32, 100)
	array := core.NewInt32Array(values, nil)

	estimated := encoder.EstimateSize(array)
	// 100 values * 8 bits = 100 bytes, plus header
	if estimated <= 0 {
		t.Error("EstimateSize should be positive")
	}
}

func TestBitPackingDecoder_CorruptedData(t *testing.T) {
	decoder := NewBitPackingDecoder()

	// Data too short for header
	_, err := decoder.Decode([]byte{0x01, 0x02, 0x03}, core.PrimInt32(), nil, 0)
	if err == nil {
		t.Error("Expected error for corrupted data (too short)")
	}

	// Data length in header doesn't match actual data length
	// Header: bitWidth=8, len=100. Data: only 1 byte.
	header := []byte{8, 100, 0, 0, 0}
	data := append(header, 0xAA)
	_, err = decoder.Decode(data, core.PrimInt32(), nil, 0)
	if err == nil {
		t.Error("Expected error for corrupted data (length mismatch)")
	}
}

func BenchmarkBitPackingEncoder_Encode(b *testing.B) {
	bitWidth := 12
	numValues := 10000
	encoder := NewBitPackingEncoder(uint8(bitWidth))
	values := make([]int32, numValues)
	maxValue := int32(1<<bitWidth - 1)
	for i := range values {
		values[i] = int32(i) % maxValue
	}
	array := core.NewInt32Array(values, nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoder.Encode(array)
	}
}

func BenchmarkBitPackingDecoder_Decode(b *testing.B) {
	bitWidth := 12
	numValues := 10000
	encoder := NewBitPackingEncoder(uint8(bitWidth))
	values := make([]int32, numValues)
	maxValue := int32(1<<bitWidth - 1)
	for i := range values {
		values[i] = int32(i) % maxValue
	}
	array := core.NewInt32Array(values, nil)
	encoded, _ := encoder.Encode(array)

	decoder := NewBitPackingDecoder()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		decoder.Decode(encoded.Data, core.PrimInt32(), nil, encoded.NumValues)
	}
}
