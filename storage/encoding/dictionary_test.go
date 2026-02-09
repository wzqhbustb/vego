package encoding

import (
	"encoding/binary"
	"math"
	"github.com/wzqhbustb/vego/storage/arrow"
	"github.com/wzqhbustb/vego/storage/format"
	"testing"
)

// ====================
// Basic Encode/Decode Tests
// ====================

func TestDictionaryEncoder_Basic_Int32(t *testing.T) {
	encoder := NewDictionaryEncoder()
	decoder := NewDictionaryDecoder()

	// Simple repeated values
	values := []int32{100, 200, 100, 300, 200, 100}
	array := arrow.NewInt32Array(values, nil)

	// Encode
	encoded, err := encoder.Encode(array)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	if encoded.Type != format.EncodingDictionary {
		t.Errorf("Expected encoding type Dictionary, got %v", encoded.Type)
	}

	// Decode
	decoded, err := decoder.Decode(encoded.Data, arrow.PrimInt32())
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	// Verify
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

func TestDictionaryEncoder_Basic_Float64(t *testing.T) {
	encoder := NewDictionaryEncoder()
	decoder := NewDictionaryDecoder()

	// Float64 values
	values := []float64{1.5, 2.5, 1.5, 3.5, 2.5}
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
	for i, expected := range values {
		if result.Value(i) != expected {
			t.Errorf("Value mismatch at %d: expected %f, got %f", i, expected, result.Value(i))
			break
		}
	}
}

func TestDictionaryEncoder_EmptyArray(t *testing.T) {
	encoder := NewDictionaryEncoder()

	// Empty array should return error
	values := []int32{}
	array := arrow.NewInt32Array(values, nil)

	_, err := encoder.Encode(array)
	if err != ErrEmptyArray {
		t.Errorf("Expected ErrEmptyArray, got %v", err)
	}
}

func TestDictionaryEncoder_SingleValue(t *testing.T) {
	encoder := NewDictionaryEncoder()
	decoder := NewDictionaryDecoder()

	// Single value repeated many times
	values := make([]int32, 100)
	for i := range values {
		values[i] = 42
	}

	array := arrow.NewInt32Array(values, nil)
	encoded, err := encoder.Encode(array)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// With single value, dictionary should have 1 entry
	// Check header: valueSize=4, numEntries=1
	if encoded.Data[0] != 4 {
		t.Errorf("Expected valueSize 4, got %d", encoded.Data[0])
	}
	numEntries := binary.LittleEndian.Uint32(encoded.Data[1:5])
	if numEntries != 1 {
		t.Errorf("Expected 1 dictionary entry for single value, got %d", numEntries)
	}

	decoded, err := decoder.Decode(encoded.Data, arrow.PrimInt32())
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	result := decoded.(*arrow.Int32Array)
	for i := 0; i < 100; i++ {
		if result.Value(i) != 42 {
			t.Errorf("Value mismatch at %d", i)
			break
		}
	}
}

func TestDictionaryEncoder_NullNotSupported(t *testing.T) {
	encoder := NewDictionaryEncoder()

	// Create array with nulls
	builder := arrow.NewInt32Builder()
	builder.Append(1)
	builder.AppendNull()
	builder.Append(2)
	array := builder.NewArray()

	_, err := encoder.Encode(array)
	if err != ErrNullNotSupported {
		t.Errorf("Expected ErrNullNotSupported, got %v", err)
	}
}

// ====================
// Different Data Types
// ====================

func TestDictionaryEncoder_Int64(t *testing.T) {
	encoder := NewDictionaryEncoder()
	decoder := NewDictionaryDecoder()

	// Int64 values
	values := []int64{10000000000, 20000000000, 10000000000, 30000000000}
	array := arrow.NewInt64Array(values, nil)

	encoded, err := encoder.Encode(array)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	if encoded.Data[0] != 8 {
		t.Errorf("Expected valueSize 8 for Int64, got %d", encoded.Data[0])
	}

	decoded, err := decoder.Decode(encoded.Data, arrow.PrimInt64())
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	result := decoded.(*arrow.Int64Array)
	for i, expected := range values {
		if result.Value(i) != expected {
			t.Errorf("Value mismatch at %d: expected %d, got %d", i, expected, result.Value(i))
			break
		}
	}
}

func TestDictionaryEncoder_Float32(t *testing.T) {
	encoder := NewDictionaryEncoder()
	decoder := NewDictionaryDecoder()

	// Float32 values
	values := []float32{1.1, 2.2, 1.1, 3.3, 2.2}
	array := arrow.NewFloat32Array(values, nil)

	encoded, err := encoder.Encode(array)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	if encoded.Data[0] != 4 {
		t.Errorf("Expected valueSize 4 for Float32, got %d", encoded.Data[0])
	}

	decoded, err := decoder.Decode(encoded.Data, arrow.PrimFloat32())
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	result := decoded.(*arrow.Float32Array)
	for i, expected := range values {
		// Use bits comparison for float
		expectedBits := math.Float32bits(expected)
		actualBits := math.Float32bits(result.Value(i))
		if expectedBits != actualBits {
			t.Errorf("Value mismatch at %d: expected %f, got %f", i, expected, result.Value(i))
			break
		}
	}
}

func TestDictionaryEncoder_UnsupportedType(t *testing.T) {
	encoder := NewDictionaryEncoder()

	// String is not supported (we don't have StringArray, but we can test the SupportsType method)
	if encoder.SupportsType(arrow.PrimInt32()) {
		t.Log("Int32 is supported")
	}
}

// ====================
// Large Dictionary Tests (uint32 indices)
// ====================

func TestDictionaryEncoder_LargeDictionary(t *testing.T) {
	encoder := NewDictionaryEncoder()
	decoder := NewDictionaryDecoder()

	// Create data with many unique values (> 65535)
	numValues := 100000
	values := make([]int32, numValues)
	for i := 0; i < numValues; i++ {
		values[i] = int32(i) // Each value is unique
	}

	array := arrow.NewInt32Array(values, nil)

	encoded, err := encoder.Encode(array)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// Check that indexSize is 4 (uint32) for large dictionary
	indexSize := encoded.Data[9]
	if indexSize != 4 {
		t.Errorf("Expected indexSize 4 for large dictionary, got %d", indexSize)
	}

	decoded, err := decoder.Decode(encoded.Data, arrow.PrimInt32())
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	result := decoded.(*arrow.Int32Array)
	if result.Len() != numValues {
		t.Errorf("Expected %d values, got %d", numValues, result.Len())
	}

	// Verify some values
	for i := 0; i < numValues; i += 1000 {
		if result.Value(i) != int32(i) {
			t.Errorf("Value mismatch at %d", i)
			break
		}
	}
}

// ====================
// Error Cases
// ====================

func TestDictionaryDecoder_TruncatedData(t *testing.T) {
	decoder := NewDictionaryDecoder()

	// Data too short for header
	shortData := []byte{4, 0, 0} // Only 3 bytes, need at least 10
	_, err := decoder.Decode(shortData, arrow.PrimInt32())
	if err == nil {
		t.Error("Expected error for truncated header")
	}
}

func TestDictionaryDecoder_InvalidValueSize(t *testing.T) {
	decoder := NewDictionaryDecoder()

	// Invalid valueSize in header
	data := make([]byte, 10)
	data[0] = 3 // Invalid: not 4 or 8 (for Int32/Int64/Float32/Float64)
	binary.LittleEndian.PutUint32(data[1:5], 1)
	binary.LittleEndian.PutUint32(data[5:9], 1)
	data[9] = 2

	_, err := decoder.Decode(data, arrow.PrimInt32())
	if err == nil {
		t.Error("Expected error for invalid valueSize")
	}
}

func TestDictionaryDecoder_InvalidIndexSize(t *testing.T) {
	decoder := NewDictionaryDecoder()

	// Invalid indexSize in header
	data := make([]byte, 10)
	data[0] = 4                                 // Valid valueSize
	data[9] = 3                                 // Invalid indexSize: not 2 or 4
	binary.LittleEndian.PutUint32(data[1:5], 1) // numEntries = 1
	binary.LittleEndian.PutUint32(data[5:9], 1) // numValues = 1

	_, err := decoder.Decode(data, arrow.PrimInt32())
	if err == nil {
		t.Error("Expected error for invalid indexSize")
	}
}

func TestDictionaryDecoder_InvalidIndex(t *testing.T) {
	decoder := NewDictionaryDecoder()

	// Create encoded data with invalid index
	encoder := NewDictionaryEncoder()
	values := []int32{100, 200, 100}
	array := arrow.NewInt32Array(values, nil)

	encoded, _ := encoder.Encode(array)

	// Corrupt the index to point beyond dictionary
	// Dictionary has 2 entries (100, 200), indices 0 and 1
	// Corrupt second occurrence of 100 to have index 5
	// Header is 10 bytes, dictionary is 2*4=8 bytes, so indices start at offset 18
	if encoded.Data[9] == 2 { // uint16 indices
		binary.LittleEndian.PutUint16(encoded.Data[20:22], 5) // Invalid index
	}

	_, err := decoder.Decode(encoded.Data, arrow.PrimInt32())
	if err == nil {
		t.Error("Expected error for invalid index")
	}
}

func TestDictionaryDecoder_WrongType(t *testing.T) {
	decoder := NewDictionaryDecoder()

	encoder := NewDictionaryEncoder()
	values := []int32{1, 2, 3}
	array := arrow.NewInt32Array(values, nil)

	encoded, _ := encoder.Encode(array)

	// Try to decode Int32 data as Int64
	_, err := decoder.Decode(encoded.Data, arrow.PrimInt64())
	if err == nil {
		t.Error("Expected error for wrong type")
	}
}

// ====================
// Compression Effectiveness
// ====================

func TestDictionaryEncoder_CompressionRatio(t *testing.T) {
	// Data with high repetition (good for dictionary encoding)
	values := make([]int32, 1000)
	for i := range values {
		values[i] = int32(i % 10) // Only 10 unique values
	}

	array := arrow.NewInt32Array(values, nil)

	encoder := NewDictionaryEncoder()
	encoded, err := encoder.Encode(array)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	// Original: 1000 * 4 = 4000 bytes
	// Encoded: 10 bytes header + 10*4 dictionary + 1000*2 indices = 10 + 40 + 2000 = 2050 bytes
	// Should be significantly smaller
	originalSize := len(values) * 4
	if len(encoded.Data) >= originalSize {
		t.Errorf("Dictionary encoding should compress. Original: %d, Encoded: %d", originalSize, len(encoded.Data))
	}

	// Verify it's actually smaller
	compressionRatio := float64(len(encoded.Data)) / float64(originalSize)
	if compressionRatio > 0.6 {
		t.Errorf("Compression ratio too high: %.2f, expected < 0.6", compressionRatio)
	}
}

// ====================
// EstimateSize Tests
// ====================

func TestDictionaryEncoder_EstimateSize(t *testing.T) {
	encoder := NewDictionaryEncoder()

	// Test with Int32 array
	values := make([]int32, 100)
	array := arrow.NewInt32Array(values, nil)

	estimated := encoder.EstimateSize(array)
	if estimated <= 0 {
		t.Error("EstimateSize should be positive")
	}

	// Estimate should be reasonable
	originalSize := len(values) * 4
	if estimated > originalSize*2 {
		t.Errorf("EstimateSize %d too large compared to original %d", estimated, originalSize)
	}
}

func TestDictionaryEncoder_SupportsType(t *testing.T) {
	encoder := NewDictionaryEncoder()

	// Supported types
	if !encoder.SupportsType(arrow.PrimInt32()) {
		t.Error("Should support Int32")
	}
	if !encoder.SupportsType(arrow.PrimInt64()) {
		t.Error("Should support Int64")
	}
	if !encoder.SupportsType(arrow.PrimFloat32()) {
		t.Error("Should support Float32")
	}
	if !encoder.SupportsType(arrow.PrimFloat64()) {
		t.Error("Should support Float64")
	}
}

// ====================
// Round Trip Tests
// ====================

func TestDictionaryEncoder_RoundTrip_VariousPatterns(t *testing.T) {
	tests := []struct {
		name   string
		values []int32
	}{
		{"ascending", []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
		{"descending", []int32{10, 9, 8, 7, 6, 5, 4, 3, 2, 1}},
		{"alternating", []int32{1, 2, 1, 2, 1, 2, 1, 2}},
		{"all_same", []int32{5, 5, 5, 5, 5, 5, 5}},
		{"runs", []int32{1, 1, 1, 2, 2, 2, 3, 3, 3}},
		{"sparse", []int32{0, 0, 0, 0, 100, 0, 0, 0, 0}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoder := NewDictionaryEncoder()
			decoder := NewDictionaryDecoder()

			array := arrow.NewInt32Array(tt.values, nil)

			encoded, err := encoder.Encode(array)
			if err != nil {
				t.Fatalf("Encode failed: %v", err)
			}

			decoded, err := decoder.Decode(encoded.Data, arrow.PrimInt32())
			if err != nil {
				t.Fatalf("Decode failed: %v", err)
			}

			result := decoded.(*arrow.Int32Array)
			if result.Len() != len(tt.values) {
				t.Errorf("Length mismatch: expected %d, got %d", len(tt.values), result.Len())
			}

			for i, expected := range tt.values {
				if result.Value(i) != expected {
					t.Errorf("Value mismatch at %d: expected %d, got %d", i, expected, result.Value(i))
					break
				}
			}
		})
	}
}

func TestDictionaryEncoder_RoundTrip_FloatPatterns(t *testing.T) {
	tests := []struct {
		name   string
		values []float64
	}{
		{"pi_multiples", []float64{3.14, 6.28, 3.14, 9.42, 6.28}},
		{"fractions", []float64{0.1, 0.2, 0.1, 0.3, 0.2}},
		{"negatives", []float64{-1.5, -2.5, -1.5, 0.0, -2.5}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoder := NewDictionaryEncoder()
			decoder := NewDictionaryDecoder()

			array := arrow.NewFloat64Array(tt.values, nil)

			encoded, err := encoder.Encode(array)
			if err != nil {
				t.Fatalf("Encode failed: %v", err)
			}

			decoded, err := decoder.Decode(encoded.Data, arrow.PrimFloat64())
			if err != nil {
				t.Fatalf("Decode failed: %v", err)
			}

			result := decoded.(*arrow.Float64Array)
			for i, expected := range tt.values {
				if result.Value(i) != expected {
					t.Errorf("Value mismatch at %d: expected %f, got %f", i, expected, result.Value(i))
					break
				}
			}
		})
	}
}

// ====================
// Benchmarks
// ====================

func BenchmarkDictionaryEncoder_Encode(b *testing.B) {
	encoder := NewDictionaryEncoder()

	// Low cardinality data
	values := make([]int32, 10000)
	for i := range values {
		values[i] = int32(i % 100)
	}
	array := arrow.NewInt32Array(values, nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoder.Encode(array)
	}
}

func BenchmarkDictionaryDecoder_Decode(b *testing.B) {
	encoder := NewDictionaryEncoder()
	decoder := NewDictionaryDecoder()

	values := make([]int32, 10000)
	for i := range values {
		values[i] = int32(i % 100)
	}
	array := arrow.NewInt32Array(values, nil)

	encoded, _ := encoder.Encode(array)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		decoder.Decode(encoded.Data, arrow.PrimInt32())
	}
}

func BenchmarkDictionaryEncoder_HighCardinality(b *testing.B) {
	encoder := NewDictionaryEncoder()

	// High cardinality data (all unique)
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
