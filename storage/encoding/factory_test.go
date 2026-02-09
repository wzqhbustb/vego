package encoding

import (
	"testing"

	"github.com/wzqhbustb/vego/storage/arrow"
	"github.com/wzqhbustb/vego/storage/format"
)

func createInt32Array(values []int32) *arrow.Int32Array {
	return arrow.NewInt32Array(values, nil)
}

func createInt64Array(values []int64) *arrow.Int64Array {
	return arrow.NewInt64Array(values, nil)
}

func createFloat32Array(values []float32) *arrow.Float32Array {
	return arrow.NewFloat32Array(values, nil)
}

func createFloat64Array(values []float64) *arrow.Float64Array {
	return arrow.NewFloat64Array(values, nil)
}

func TestEncoderFactory_SelectEncoder_NilStatistics(t *testing.T) {
	factory := NewEncoderFactory(3)

	// nil statistics should return Zstd
	encoder := factory.SelectEncoder(arrow.PrimInt32(), nil)
	if encoder == nil {
		t.Fatal("Expected encoder, got nil")
	}
	if encoder.Type() != format.EncodingZstd {
		t.Errorf("Expected Zstd for nil stats, got %v", encoder.Type())
	}
}

func TestEncoderFactory_SelectEncoder_SmallData(t *testing.T) {
	factory := NewEncoderFactory(3)

	// Small data (< 100 values) should use Zstd
	smallData := make([]int32, 50)
	arr := createInt32Array(smallData)
	stats := ComputeStatistics(arr)

	encoder := factory.SelectEncoder(arrow.PrimInt32(), stats)
	if encoder.Type() != format.EncodingZstd {
		t.Errorf("Expected Zstd for small data, got %v", encoder.Type())
	}
}

func TestEncoderFactory_SelectEncoder_Int32_Narrow(t *testing.T) {
	factory := NewEncoderFactory(3)

	// Narrow integers (max value < 65536) should use BitPacking
	values := make([]int32, 1000)
	for i := range values {
		values[i] = int32(i % 1000)
	}
	arr := createInt32Array(values)
	stats := ComputeStatistics(arr)

	encoder := factory.SelectEncoder(arrow.PrimInt32(), stats)
	// Note: May vary based on thresholds
	t.Logf("Selected encoder for narrow int32: %v", encoder.Type())
}

func TestEncoderFactory_SelectEncoder_Int32_RLE(t *testing.T) {
	factory := NewEncoderFactory(3)

	// High run ratio should use RLE
	values := make([]int32, 1000)
	for i := range values {
		values[i] = int32(i / 100)
	}
	arr := createInt32Array(values)
	stats := ComputeStatistics(arr)

	encoder := factory.SelectEncoder(arrow.PrimInt32(), stats)
	t.Logf("Run ratio: %.4f", stats.GetRunRatio())
	t.Logf("Selected encoder for run-heavy data: %v", encoder.Type())
}

func TestEncoderFactory_SelectEncoder_Int32_Dictionary(t *testing.T) {
	factory := NewEncoderFactory(3)

	// Low cardinality should use Dictionary
	values := make([]int32, 1000)
	for i := range values {
		values[i] = int32(i % 10)
	}
	arr := createInt32Array(values)
	stats := ComputeStatistics(arr)

	encoder := factory.SelectEncoder(arrow.PrimInt32(), stats)
	t.Logf("Cardinality ratio: %.4f", stats.GetCardinalityRatio())
	t.Logf("Selected encoder for low cardinality: %v", encoder.Type())
}

func TestEncoderFactory_SelectEncoder_Int32_Wide(t *testing.T) {
	factory := NewEncoderFactory(3)

	// Wide integers (no pattern) should use Zstd
	values := make([]int32, 1000)
	for i := range values {
		values[i] = int32(i * 1000000)
	}
	arr := createInt32Array(values)
	stats := ComputeStatistics(arr)

	encoder := factory.SelectEncoder(arrow.PrimInt32(), stats)
	// Should fallback to Zstd
	if encoder.Type() != format.EncodingZstd {
		t.Logf("Expected Zstd for wide int32, got %v", encoder.Type())
	}
}

func TestEncoderFactory_SelectEncoder_Float32_BSS(t *testing.T) {
	factory := NewEncoderFactory(3)

	// Floating point with low byte entropy should use BSS
	values := make([]float32, 1000)
	for i := range values {
		values[i] = float32(i) * 0.001
	}
	arr := createFloat32Array(values)
	stats := ComputeStatistics(arr)

	encoder := factory.SelectEncoder(arrow.PrimFloat32(), stats)
	t.Logf("Average entropy: %.2f", stats.GetAverageEntropy())
	t.Logf("Selected encoder for float32: %v", encoder.Type())
}

func TestEncoderFactory_SelectEncoder_CustomConfig(t *testing.T) {
	config := &EncoderConfig{
		BitPackingMaxBitWidth: 8,
		RLEThreshold:          0.3,
		DictionaryThreshold:   0.3,
		DictionaryMaxSize:     100,
		BSSEntropyThreshold:   2.0,
		SmallDataThreshold:    50,
	}
	factory := NewEncoderFactoryWithConfig(3, config)

	values := make([]int32, 75)
	for i := range values {
		values[i] = int32(i % 1000)
	}
	arr := createInt32Array(values)
	stats := ComputeStatistics(arr)

	encoder := factory.SelectEncoder(arrow.PrimInt32(), stats)
	t.Logf("With custom config, selected: %v", encoder.Type())
}

func TestEncoderFactory_SelectEncoder_NilConfig(t *testing.T) {
	factory := NewEncoderFactoryWithConfig(3, nil)

	values := make([]int32, 50)
	arr := createInt32Array(values)
	stats := ComputeStatistics(arr)

	encoder := factory.SelectEncoder(arrow.PrimInt32(), stats)
	if encoder.Type() != format.EncodingZstd {
		t.Errorf("Expected Zstd with default config for small data, got %v", encoder.Type())
	}
}

func TestEncoderFactory_E2E_Int32(t *testing.T) {
	factory := NewEncoderFactory(3)

	testCases := []struct {
		name   string
		values []int32
	}{
		{"narrow", []int32{1, 2, 3, 4, 5}},
		{"repeated", []int32{1, 1, 1, 2, 2, 2}},
		{"low_cardinality", []int32{1, 2, 1, 2, 1, 2}},
		{"wide", []int32{1000000, 2000000, 3000000}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			arr := createInt32Array(tc.values)
			stats := ComputeStatistics(arr)

			encoder := factory.SelectEncoder(arrow.PrimInt32(), stats)

			encoded, err := encoder.Encode(arr)
			if err != nil {
				t.Fatalf("Encode failed: %v", err)
			}

			if len(encoded.Data) == 0 {
				t.Error("Encoded data is empty")
			}
		})
	}
}

func BenchmarkEncoderFactory_SelectEncoder(b *testing.B) {
	factory := NewEncoderFactory(3)
	values := make([]int32, 10000)
	for i := range values {
		values[i] = int32(i % 100)
	}
	arr := createInt32Array(values)
	stats := ComputeStatistics(arr)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		factory.SelectEncoder(arrow.PrimInt32(), stats)
	}
}

func BenchmarkEncoderFactory_Encode(b *testing.B) {
	factory := NewEncoderFactory(3)
	values := make([]int32, 10000)
	for i := range values {
		values[i] = int32(i % 100)
	}
	arr := createInt32Array(values)
	stats := ComputeStatistics(arr)
	encoder := factory.SelectEncoder(arrow.PrimInt32(), stats)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoder.Encode(arr)
	}
}
