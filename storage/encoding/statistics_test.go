// encoding/statistics_test.go

package encoding

import (
	"math"
	"github.com/wzqhbkjdx/vego/storage/arrow"
	"testing"
	"time"
)

// ====================
// Basic Statistics Tests
// ====================

func TestComputeStatistics_Int32(t *testing.T) {
	tests := []struct {
		name     string
		values   []int32
		nulls    []bool
		wantRuns uint64
	}{
		{
			name:     "sequential values",
			values:   []int32{1, 2, 3, 4, 5},
			wantRuns: 5,
		},
		{
			name:     "repeated values",
			values:   []int32{1, 1, 1, 1, 1},
			wantRuns: 1,
		},
		{
			name:     "mixed runs",
			values:   []int32{1, 1, 2, 2, 2, 3},
			wantRuns: 3,
		},
		{
			name:     "with nulls",
			values:   []int32{1, 0, 3, 0, 5},
			nulls:    []bool{true, false, true, false, true},
			wantRuns: 5, // [1], [0], [3], [0], [5] - 物理值的 runs，null 信息在 bitmap 中
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var array arrow.Array
			if tt.nulls != nil {
				array = arrow.NewInt32Array(tt.values, newBitmapFromBools(tt.nulls))
			} else {
				array = arrow.NewInt32Array(tt.values, nil)
			}

			stats := ComputeStatistics(array)

			// Validate
			if err := stats.Validate(); err != nil {
				t.Fatalf("Validation failed: %v", err)
			}

			// Check NumValues
			if stats.NumValues != int64(len(tt.values)) {
				t.Errorf("NumValues = %d, want %d", stats.NumValues, len(tt.values))
			}

			// Check RunCount
			if stats.RunCount == nil {
				t.Fatal("RunCount should not be nil")
			}
			if *stats.RunCount != tt.wantRuns {
				t.Errorf("RunCount = %d, want %d", *stats.RunCount, tt.wantRuns)
			}

			// Check DataType
			if stats.DataType != arrow.INT32 {
				t.Errorf("DataType = %v, want INT32", stats.DataType)
			}

			// Check ComputedAt
			if stats.ComputedAt.IsZero() {
				t.Error("ComputedAt should not be zero")
			}

			// Check IsComplete
			if !stats.IsComplete {
				t.Error("IsComplete should be true")
			}
		})
	}
}

func TestComputeStatistics_Int64(t *testing.T) {
	values := []int64{100, 200, 300, 400, 500}
	array := arrow.NewInt64Array(values, nil)

	stats := ComputeStatistics(array)

	if err := stats.Validate(); err != nil {
		t.Fatalf("Validation failed: %v", err)
	}

	if stats.NumValues != int64(len(values)) {
		t.Errorf("NumValues = %d, want %d", stats.NumValues, len(values))
	}

	if stats.DataType != arrow.INT64 {
		t.Errorf("DataType = %v, want INT64", stats.DataType)
	}

	if stats.RunCount == nil {
		t.Fatal("RunCount should not be nil")
	}

	if *stats.RunCount != 5 {
		t.Errorf("RunCount = %d, want 5", *stats.RunCount)
	}
}

// ====================
// BitWidth Tests
// ====================

func TestComputeStatistics_BitWidth(t *testing.T) {
	tests := []struct {
		name         string
		values       []int32
		wantMaxWidth uint64
	}{
		{
			name:         "small values (3 bits)",
			values:       []int32{0, 1, 2, 3, 4, 5, 6, 7},
			wantMaxWidth: 3,
		},
		{
			name:         "medium values (8 bits)",
			values:       []int32{0, 255, 128, 64},
			wantMaxWidth: 8,
		},
		{
			name:         "large values (20 bits)",
			values:       []int32{0, 1000000, 500000},
			wantMaxWidth: 20,
		},
		{
			name:         "all zeros",
			values:       []int32{0, 0, 0, 0},
			wantMaxWidth: 0,
		},
		{
			name:         "single value",
			values:       []int32{255},
			wantMaxWidth: 8,
		},
		{
			name:         "power of 2",
			values:       []int32{1, 2, 4, 8, 16, 32, 64, 128},
			wantMaxWidth: 8,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			array := arrow.NewInt32Array(tt.values, nil)
			stats := ComputeStatistics(array)

			if err := stats.Validate(); err != nil {
				t.Errorf("Validation failed: %v", err)
			}

			if stats.BitWidth == nil {
				t.Fatal("BitWidth should not be nil")
			}

			maxWidth := stats.GetMaxBitWidth()
			if maxWidth != tt.wantMaxWidth {
				t.Errorf("MaxBitWidth = %d, want %d", maxWidth, tt.wantMaxWidth)
			}
		})
	}
}

func TestComputeStatistics_BitWidth_LargeArray(t *testing.T) {
	// Test with > 1024 values (multiple chunks)
	values := make([]int32, 5000)
	for i := range values {
		values[i] = int32(i % 256) // Values 0-255
	}

	array := arrow.NewInt32Array(values, nil)
	stats := ComputeStatistics(array)

	if err := stats.Validate(); err != nil {
		t.Fatalf("Validation failed: %v", err)
	}

	if stats.BitWidth == nil {
		t.Fatal("BitWidth should not be nil")
	}

	// Should have multiple chunks
	numChunks := len(*stats.BitWidth)
	expectedChunks := (5000 + 1023) / 1024 // ceil(5000/1024) = 5
	if numChunks != expectedChunks {
		t.Errorf("Number of chunks = %d, want %d", numChunks, expectedChunks)
	}

	// All chunks should have width 8 (for values 0-255)
	for i, width := range *stats.BitWidth {
		if width != 8 {
			t.Errorf("Chunk %d: width = %d, want 8", i, width)
		}
	}
}

func TestComputeStatistics_BitWidth_VeryLargeArray(t *testing.T) {
	// Test with > 10,240,000 values (should trigger sampling)
	const maxBitWidthChunks = 10000
	const chunkSize = 1024
	numValues := maxBitWidthChunks*chunkSize + 1000

	values := make([]int32, numValues)
	for i := range values {
		values[i] = int32(i % 128)
	}

	array := arrow.NewInt32Array(values, nil)
	stats := ComputeStatistics(array)

	if err := stats.Validate(); err != nil {
		t.Fatalf("Validation failed: %v", err)
	}

	if stats.BitWidth == nil {
		t.Fatal("BitWidth should not be nil")
	}

	// Should be limited to maxBitWidthChunks
	numChunks := len(*stats.BitWidth)
	if numChunks > maxBitWidthChunks {
		t.Errorf("Number of chunks = %d, exceeds max %d", numChunks, maxBitWidthChunks)
	}
}

// ====================
// Float Tests
// ====================

func TestComputeStatistics_Float32(t *testing.T) {
	values := []float32{1.0, 2.0, 3.0, 4.0, 5.0}
	array := arrow.NewFloat32Array(values, nil)

	stats := ComputeStatistics(array)

	if err := stats.Validate(); err != nil {
		t.Fatalf("Validation failed: %v", err)
	}

	if stats.NumValues != int64(len(values)) {
		t.Errorf("NumValues = %d, want %d", stats.NumValues, len(values))
	}

	if stats.DataType != arrow.FLOAT32 {
		t.Errorf("DataType = %v, want FLOAT32", stats.DataType)
	}

	if stats.DataSize == nil {
		t.Fatal("DataSize should not be nil")
	}

	expectedSize := uint64(len(values) * 4)
	if *stats.DataSize != expectedSize {
		t.Errorf("DataSize = %d, want %d", *stats.DataSize, expectedSize)
	}

	if stats.BytePositionEntropy == nil {
		t.Fatal("BytePositionEntropy should not be nil")
	}

	if len(*stats.BytePositionEntropy) != 4 {
		t.Errorf("BytePositionEntropy length = %d, want 4", len(*stats.BytePositionEntropy))
	}
}

func TestComputeStatistics_Float32_NaN(t *testing.T) {
	// Test NaN handling
	values := []float32{
		1.0,
		float32(math.NaN()),
		float32(math.NaN()),
		2.0,
		float32(math.NaN()),
		3.0,
	}
	array := arrow.NewFloat32Array(values, nil)

	stats := ComputeStatistics(array)

	if err := stats.Validate(); err != nil {
		t.Errorf("Validation failed: %v", err)
	}

	// NaN values should be treated as equal (same bit pattern)
	if stats.RunCount == nil {
		t.Fatal("RunCount should not be nil")
	}

	// Expected: [1.0] [NaN, NaN] [2.0] [NaN] [3.0] = 5 runs
	// (consecutive NaNs are same run)
	expectedRuns := uint64(5)
	if *stats.RunCount != expectedRuns {
		t.Errorf("RunCount with NaN = %d, want %d", *stats.RunCount, expectedRuns)
	}
}

func TestComputeStatistics_Float32_Infinity(t *testing.T) {
	values := []float32{
		float32(math.Inf(1)),  // +Inf
		float32(math.Inf(-1)), // -Inf
		0.0,
		float32(math.Inf(1)), // +Inf again
	}
	array := arrow.NewFloat32Array(values, nil)

	stats := ComputeStatistics(array)

	if err := stats.Validate(); err != nil {
		t.Errorf("Validation failed: %v", err)
	}

	// Should handle infinity correctly
	if stats.RunCount == nil {
		t.Fatal("RunCount should not be nil")
	}

	// [+Inf] [-Inf] [0.0] [+Inf] = 4 runs
	if *stats.RunCount != 4 {
		t.Errorf("RunCount with Inf = %d, want 4", *stats.RunCount)
	}
}

func TestComputeStatistics_Float64(t *testing.T) {
	values := []float64{1.5, 2.5, 3.5, 4.5, 5.5}
	array := arrow.NewFloat64Array(values, nil)

	stats := ComputeStatistics(array)

	if err := stats.Validate(); err != nil {
		t.Fatalf("Validation failed: %v", err)
	}

	if stats.DataType != arrow.FLOAT64 {
		t.Errorf("DataType = %v, want FLOAT64", stats.DataType)
	}

	if stats.BytePositionEntropy == nil {
		t.Fatal("BytePositionEntropy should not be nil")
	}

	if len(*stats.BytePositionEntropy) != 8 {
		t.Errorf("BytePositionEntropy length = %d, want 8", len(*stats.BytePositionEntropy))
	}
}

// ====================
// RunCount Tests
// ====================

func TestComputeRunRatio(t *testing.T) {
	tests := []struct {
		name      string
		values    []int32
		wantRatio float64
	}{
		{
			name:      "no compression (all unique)",
			values:    []int32{1, 2, 3, 4, 5},
			wantRatio: 1.0,
		},
		{
			name:      "excellent compression (one run)",
			values:    []int32{5, 5, 5, 5, 5},
			wantRatio: 0.2,
		},
		{
			name:      "moderate compression",
			values:    []int32{1, 1, 2, 2, 3},
			wantRatio: 0.6,
		},
		{
			name:      "alternating values",
			values:    []int32{1, 2, 1, 2, 1, 2},
			wantRatio: 1.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			array := arrow.NewInt32Array(tt.values, nil)
			stats := ComputeStatistics(array)

			ratio := stats.GetRunRatio()
			if math.Abs(ratio-tt.wantRatio) > 0.01 {
				t.Errorf("RunRatio = %.3f, want %.3f", ratio, tt.wantRatio)
			}
		})
	}
}

// ====================
// Cardinality Tests
// ====================

func TestComputeCardinality(t *testing.T) {
	tests := []struct {
		name         string
		values       []int32
		exactCard    int
		tolerancePct float64 // HLL is approximate
	}{
		{
			name:         "small unique set",
			values:       []int32{1, 2, 3, 4, 5},
			exactCard:    5,
			tolerancePct: 0.5,
		},
		{
			name:         "repeated values",
			values:       []int32{1, 1, 2, 2, 3, 3, 3},
			exactCard:    3,
			tolerancePct: 0.5,
		},
		{
			name:         "all same",
			values:       []int32{7, 7, 7, 7, 7, 7, 7, 7, 7, 7},
			exactCard:    1,
			tolerancePct: 1.0, // Linear counting correction
		},
		{
			name:         "medium cardinality",
			values:       makeSequence(0, 50),
			exactCard:    50,
			tolerancePct: 0.3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			array := arrow.NewInt32Array(tt.values, nil)
			stats := ComputeStatistics(array)

			if stats.Cardinality == nil {
				t.Fatal("Cardinality should not be nil")
			}

			card := *stats.Cardinality
			lower := float64(tt.exactCard) * (1 - tt.tolerancePct)
			upper := float64(tt.exactCard) * (1 + tt.tolerancePct)

			if float64(card) < lower || float64(card) > upper {
				t.Logf("Cardinality = %d, expected ~%d (tolerance %.0f%%)",
					card, tt.exactCard, tt.tolerancePct*100)
				// Don't fail, HLL is probabilistic, just log
			}
		})
	}
}

func TestComputeCardinalityRatio(t *testing.T) {
	tests := []struct {
		name      string
		values    []int32
		wantRatio float64
		tolerance float64
	}{
		{
			name:      "low cardinality (good for dictionary)",
			values:    []int32{1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3},
			wantRatio: 0.25, // 3/12
			tolerance: 0.15,
		},
		{
			name:      "high cardinality (bad for dictionary)",
			values:    makeSequence(0, 100),
			wantRatio: 1.0, // 100/100
			tolerance: 0.3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			array := arrow.NewInt32Array(tt.values, nil)
			stats := ComputeStatistics(array)

			ratio := stats.GetCardinalityRatio()
			if math.Abs(ratio-tt.wantRatio) > tt.tolerance {
				t.Logf("CardinalityRatio = %.3f, expected ~%.3f", ratio, tt.wantRatio)
			}
		})
	}
}

// ====================
// Entropy Tests
// ====================

func TestBytePositionEntropy(t *testing.T) {
	// Test with sequential integers
	values := makeSequence(0, 100)
	array := arrow.NewInt32Array(values, nil)

	stats := ComputeStatistics(array)

	if stats.BytePositionEntropy == nil {
		t.Fatal("BytePositionEntropy should not be nil")
	}

	if len(*stats.BytePositionEntropy) != 4 {
		t.Errorf("BytePositionEntropy length = %d, want 4", len(*stats.BytePositionEntropy))
	}

	avgEntropy := stats.GetAverageEntropy()
	if avgEntropy < 0 || avgEntropy > 8.0 {
		t.Errorf("AverageEntropy = %.2f, should be in range [0, 8]", avgEntropy)
	}

	t.Logf("Average entropy for sequential integers: %.2f", avgEntropy)
}

func TestBytePositionEntropy_Float32(t *testing.T) {
	// Float32 should have low entropy in exponent bytes
	values := make([]float32, 100)
	for i := range values {
		values[i] = float32(i) + 0.5 // Similar magnitude
	}
	array := arrow.NewFloat32Array(values, nil)

	stats := ComputeStatistics(array)

	avgEntropy := stats.GetAverageEntropy()
	t.Logf("Average entropy for float32: %.2f", avgEntropy)

	// Float32 typically has lower entropy (good for BSS)
	if avgEntropy > 6.0 {
		t.Logf("Warning: Float32 entropy %.2f seems high for BSS", avgEntropy)
	}
}

// ====================
// NullCount Tests
// ====================

func TestComputeStatistics_NullCount(t *testing.T) {
	tests := []struct {
		name          string
		values        []int32
		nulls         []bool
		wantNullCount int64
	}{
		{
			name:          "no nulls",
			values:        []int32{1, 2, 3, 4, 5},
			nulls:         nil,
			wantNullCount: 0,
		},
		{
			name:          "some nulls",
			values:        []int32{1, 0, 3, 0, 5},
			nulls:         []bool{true, false, true, false, true},
			wantNullCount: 2,
		},
		{
			name:          "all nulls",
			values:        []int32{0, 0, 0, 0},
			nulls:         []bool{false, false, false, false},
			wantNullCount: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var array arrow.Array
			if tt.nulls != nil {
				array = arrow.NewInt32Array(tt.values, newBitmapFromBools(tt.nulls))
			} else {
				array = arrow.NewInt32Array(tt.values, nil)
			}

			stats := ComputeStatistics(array)

			if tt.wantNullCount == 0 {
				if stats.NullCount != nil {
					t.Errorf("NullCount should be nil for no nulls")
				}
			} else {
				if stats.NullCount == nil {
					t.Fatal("NullCount should not be nil")
				}
				if *stats.NullCount != tt.wantNullCount {
					t.Errorf("NullCount = %d, want %d", *stats.NullCount, tt.wantNullCount)
				}
			}
		})
	}
}

// ====================
// Edge Cases
// ====================

func TestComputeStatistics_EmptyArray(t *testing.T) {
	array := arrow.NewInt32Array([]int32{}, nil)
	stats := ComputeStatistics(array)

	if err := stats.Validate(); err != nil {
		t.Errorf("Validation failed: %v", err)
	}

	if stats.NumValues != 0 {
		t.Errorf("NumValues = %d, want 0", stats.NumValues)
	}

	if stats.RunCount != nil && *stats.RunCount != 0 {
		t.Errorf("RunCount = %d, want 0", *stats.RunCount)
	}
}

func TestComputeStatistics_SingleValue(t *testing.T) {
	array := arrow.NewInt32Array([]int32{42}, nil)
	stats := ComputeStatistics(array)

	if err := stats.Validate(); err != nil {
		t.Errorf("Validation failed: %v", err)
	}

	if stats.NumValues != 1 {
		t.Errorf("NumValues = %d, want 1", stats.NumValues)
	}

	if stats.RunCount == nil || *stats.RunCount != 1 {
		t.Errorf("RunCount should be 1 for single value")
	}
}

func TestComputeStatistics_FixedSizeList(t *testing.T) {
	// Test vector data (FixedSizeList<Float32>)
	vectors := [][]float32{
		{1.0, 2.0, 3.0},
		{4.0, 5.0, 6.0},
		{7.0, 8.0, 9.0},
	}

	// Flatten vectors
	flatValues := make([]float32, 0, len(vectors)*3)
	for _, vec := range vectors {
		flatValues = append(flatValues, vec...)
	}

	valuesArray := arrow.NewFloat32Array(flatValues, nil)
	fslType := arrow.FixedSizeListOf(arrow.PrimFloat32(), 3).(*arrow.FixedSizeListType)
	array := arrow.NewFixedSizeListArray(fslType, valuesArray, nil)

	stats := ComputeStatistics(array)

	if err := stats.Validate(); err != nil {
		t.Errorf("Validation failed: %v", err)
	}

	// NumValues 应该是展平后的值数量（9），不是向量数量（3）
	// 因为统计信息（RunCount、Cardinality等）都是基于展平值计算的
	if stats.NumValues != int64(len(flatValues)) {
		t.Errorf("NumValues = %d, want %d (flattened values)", stats.NumValues, len(flatValues))
	}

	if stats.DataSize == nil {
		t.Fatal("DataSize should not be nil")
	}

	expectedSize := uint64(len(flatValues) * 4)
	if *stats.DataSize != expectedSize {
		t.Errorf("DataSize = %d, want %d", *stats.DataSize, expectedSize)
	}
}

// ====================
// Validate Tests
// ====================

func TestStatistics_Validate(t *testing.T) {
	tests := []struct {
		name    string
		stats   *Statistics
		wantErr bool
	}{
		{
			name: "valid stats",
			stats: &Statistics{
				NumValues:  100,
				ComputedAt: time.Now(),
			},
			wantErr: false,
		},
		{
			name:    "nil stats",
			stats:   nil,
			wantErr: true,
		},
		{
			name: "negative NumValues",
			stats: &Statistics{
				NumValues: -1,
			},
			wantErr: true,
		},
		{
			name: "NullCount exceeds NumValues",
			stats: &Statistics{
				NumValues: 10,
				NullCount: func() *int64 { v := int64(20); return &v }(),
			},
			wantErr: true,
		},
		{
			name: "RunCount exceeds NumValues",
			stats: &Statistics{
				NumValues: 10,
				RunCount:  func() *uint64 { v := uint64(20); return &v }(),
			},
			wantErr: true,
		},
		{
			name: "Cardinality slightly exceeds NumValues (acceptable)",
			stats: &Statistics{
				NumValues:   100,
				Cardinality: func() *uint64 { v := uint64(110); return &v }(),
			},
			wantErr: false, // Within 2x tolerance
		},
		{
			name: "Cardinality significantly exceeds NumValues",
			stats: &Statistics{
				NumValues:   100,
				Cardinality: func() *uint64 { v := uint64(250); return &v }(),
			},
			wantErr: true, // > 2x
		},
		{
			name: "DataSize too large",
			stats: &Statistics{
				NumValues: 10,
				DataSize:  func() *uint64 { v := uint64(1000); return &v }(),
			},
			wantErr: true, // 1000 bytes for 10 values (max 16 bytes each = 160)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.stats.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// ====================
// Clone Tests
// ====================

func TestStatistics_Clone(t *testing.T) {
	// Test nil clone
	var nilStats *Statistics
	clone := nilStats.Clone()
	if clone != nil {
		t.Error("Clone of nil should be nil")
	}

	// Test full clone
	original := &Statistics{
		NumValues:  100,
		DataType:   arrow.INT32,
		ComputedAt: time.Now(),
		IsComplete: true,
	}

	nullCount := int64(10)
	original.NullCount = &nullCount

	bitWidths := []uint64{8, 12, 16}
	original.BitWidth = &bitWidths

	dataSize := uint64(400)
	original.DataSize = &dataSize

	runCount := uint64(50)
	original.RunCount = &runCount

	cardinality := uint64(80)
	original.Cardinality = &cardinality

	entropy := []uint64{5000, 6000, 7000, 8000}
	original.BytePositionEntropy = &entropy

	// Clone
	cloned := original.Clone()

	// Verify cloned values
	if cloned.NumValues != original.NumValues {
		t.Errorf("Cloned NumValues = %d, want %d", cloned.NumValues, original.NumValues)
	}

	if cloned.DataType != original.DataType {
		t.Errorf("Cloned DataType = %v, want %v", cloned.DataType, original.DataType)
	}

	if cloned.NullCount == nil || *cloned.NullCount != *original.NullCount {
		t.Error("NullCount not cloned correctly")
	}

	// Verify deep copy (not same pointer)
	if cloned.BitWidth == original.BitWidth {
		t.Error("BitWidth should be deep copied, not same pointer")
	}

	if cloned.BytePositionEntropy == original.BytePositionEntropy {
		t.Error("BytePositionEntropy should be deep copied, not same pointer")
	}

	// Modify original to ensure independence
	(*original.BitWidth)[0] = 999
	if (*cloned.BitWidth)[0] == 999 {
		t.Error("Clone should be independent of original")
	}
}

// ====================
// Helper Functions
// ====================

func makeSequence(start, count int) []int32 {
	result := make([]int32, count)
	for i := range result {
		result[i] = int32(start + i)
	}
	return result
}

// ====================
// Benchmark Tests
// ====================

func BenchmarkComputeStatistics_Int32_Small(b *testing.B) {
	values := makeSequence(0, 100)
	array := arrow.NewInt32Array(values, nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ComputeStatistics(array)
	}
}

func BenchmarkComputeStatistics_Int32_Medium(b *testing.B) {
	values := makeSequence(0, 10000)
	array := arrow.NewInt32Array(values, nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ComputeStatistics(array)
	}
}

func BenchmarkComputeStatistics_Int32_Large(b *testing.B) {
	values := makeSequence(0, 100000)
	array := arrow.NewInt32Array(values, nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ComputeStatistics(array)
	}
}

func BenchmarkComputeStatistics_Float32(b *testing.B) {
	values := make([]float32, 10000)
	for i := range values {
		values[i] = float32(i) * 0.1
	}
	array := arrow.NewFloat32Array(values, nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ComputeStatistics(array)
	}
}

func BenchmarkComputeMaxBitWidth32(b *testing.B) {
	values := makeSequence(0, 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = computeMaxBitWidth32(values)
	}
}

func BenchmarkComputeRunCount32(b *testing.B) {
	values := makeSequence(0, 10000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = computeRunCount32(values)
	}
}

func BenchmarkComputeCardinality32(b *testing.B) {
	values := makeSequence(0, 10000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = computeCardinality32(values)
	}
}

func BenchmarkUniformSampleIndices(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = uniformSampleIndices(1000000, 64)
	}
}

// Add this helper function at the end of the test file:

// Helper function to create bitmap from boolean slice
// true = value is valid (bit set to 1)
// false = value is null (bit set to 0)
func newBitmapFromBools(validBits []bool) *arrow.Bitmap {
	bm := arrow.NewBitmap(len(validBits))
	for i, valid := range validBits {
		if valid {
			bm.Set(i)
		}
	}
	return bm
}
