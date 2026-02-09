package encoding

import (
	"math"
	"math/bits"
	"github.com/wzqhbkjdx/vego/storage/arrow"
	time "time"

	lerrors "github.com/wzqhbkjdx/vego/storage/errors"
)

// Stat represents different types of statistics we can compute on data blocks
type Stat int

const (
	StatBitWidth Stat = iota
	StatDataSize
	StatCardinality
	StatNullCount
	StatMaxLength
	StatRunCount
	StatBytePositionEntropy
)

func (s Stat) String() string {
	switch s {
	case StatBitWidth:
		return "BitWidth"
	case StatDataSize:
		return "DataSize"
	case StatCardinality:
		return "Cardinality"
	case StatNullCount:
		return "NullCount"
	case StatMaxLength:
		return "MaxLength"
	case StatRunCount:
		return "RunCount"
	case StatBytePositionEntropy:
		return "BytePositionEntropy"
	default:
		return "Unknown"
	}
}

// Statistics holds computed statistics for a data block
type Statistics struct {
	// Number of values in the block
	NumValues int64

	// Metadata
	DataType   arrow.TypeID // Data type for validation
	ComputedAt time.Time    // When statistics were computed
	IsComplete bool         // Whether full computation or sampling was used

	// Null statistics
	NullCount *int64

	// Fixed-width statistics (for integers/floats)
	BitWidth *[]uint64 // Max bit width per chunk (1024 values)
	DataSize *uint64   // Total data size in bytes

	// Variable-width statistics (for strings/binary)
	MaxLength *uint64 // Maximum length of variable-width element

	// Compression decision statistics
	RunCount    *uint64 // Number of runs for RLE decision
	Cardinality *uint64 // Approximate unique count for Dictionary

	// BSS (Byte Stream Split) decision
	BytePositionEntropy *[]uint64 // Entropy per byte position (scaled by 1000)
}

// ComputeStatistics computes all relevant statistics for an Arrow array
func ComputeStatistics(array arrow.Array) *Statistics {
	stats := &Statistics{
		NumValues:  int64(array.Len()),
		DataType:   array.DataType().ID(),
		ComputedAt: time.Now(),
		IsComplete: true, // Full computation
	}

	// Null count
	if array.NullN() > 0 {
		nullCount := int64(array.NullN())
		stats.NullCount = &nullCount
	}

	// Compute type-specific statistics
	switch arr := array.(type) {
	case *arrow.Int32Array:
		computeFixedWidthStats(stats, arr.Data().Buffers()[0], 32, arr.Len())
	case *arrow.Int64Array:
		computeFixedWidthStats(stats, arr.Data().Buffers()[0], 64, arr.Len())
	case *arrow.Float32Array:
		computeFloat32Stats(stats, arr.Data().Buffers()[0], arr.Len())
	case *arrow.Float64Array:
		computeFloat64Stats(stats, arr.Data().Buffers()[0], arr.Len())
	case *arrow.FixedSizeListArray:
		// For FSL (vectors), compute stats on the flattened values
		values := arr.Values()
		// 更新 NumValues 为展平后的值数量，以保持统计信息一致性
		stats.NumValues = int64(values.Len())

		switch valArr := values.(type) {
		case *arrow.Float32Array:
			computeFloat32Stats(stats, valArr.Data().Buffers()[0], valArr.Len())
		case *arrow.Int32Array:
			computeFixedWidthStats(stats, valArr.Data().Buffers()[0], 32, valArr.Len())
		}
	}

	return stats
}

// computeFixedWidthStats computes statistics for fixed-width integer types
func computeFixedWidthStats(stats *Statistics, buffer *arrow.Buffer, bitsPerValue int, numValues int) {
	data := buffer.Bytes()

	// Data size
	dataSize := uint64(len(data))
	stats.DataSize = &dataSize

	// Bit width calculation (chunked, 1024 values per chunk like Rust)
	const chunkSize = 1024
	const maxBitWidthChunks = 10000 // Limit to prevent excessive memory usage

	numChunks := (numValues + chunkSize - 1) / chunkSize
	if numChunks > maxBitWidthChunks {
		numChunks = maxBitWidthChunks
	}

	bitWidths := make([]uint64, 0, numChunks)

	switch bitsPerValue {
	case 32:
		values := buffer.Int32()

		if numValues > chunkSize*maxBitWidthChunks {
			// Sample chunks uniformly for very large arrays
			step := len(values) / (maxBitWidthChunks * chunkSize)
			for i := 0; i < maxBitWidthChunks; i++ {
				start := i * chunkSize * step
				end := start + chunkSize
				if end > len(values) {
					end = len(values)
				}
				maxWidth := computeMaxBitWidth32(values[start:end])
				bitWidths = append(bitWidths, maxWidth)
			}
		} else {
			// Process all chunks for normal-sized arrays
			for i := 0; i < len(values); i += chunkSize {
				end := i + chunkSize
				if end > len(values) {
					end = len(values)
				}
				maxWidth := computeMaxBitWidth32(values[i:end])
				bitWidths = append(bitWidths, maxWidth)
			}
		}
		stats.BitWidth = &bitWidths

		// Run count
		runCount := computeRunCount32(values)
		stats.RunCount = &runCount

		// Cardinality (using HyperLogLog approximation with small cardinality correction)
		cardinality := computeCardinality32(values)
		stats.Cardinality = &cardinality

	case 64:
		values := buffer.Int64()

		if numValues > chunkSize*maxBitWidthChunks {
			// Sample chunks uniformly for very large arrays
			step := len(values) / (maxBitWidthChunks * chunkSize)
			for i := 0; i < maxBitWidthChunks; i++ {
				start := i * chunkSize * step
				end := start + chunkSize
				if end > len(values) {
					end = len(values)
				}
				maxWidth := computeMaxBitWidth64(values[start:end])
				bitWidths = append(bitWidths, maxWidth)
			}
		} else {
			for i := 0; i < len(values); i += chunkSize {
				end := i + chunkSize
				if end > len(values) {
					end = len(values)
				}
				maxWidth := computeMaxBitWidth64(values[i:end])
				bitWidths = append(bitWidths, maxWidth)
			}
		}
		stats.BitWidth = &bitWidths

		runCount := computeRunCount64(values)
		stats.RunCount = &runCount

		cardinality := computeCardinality64(values)
		stats.Cardinality = &cardinality
	}

	// Byte position entropy (for BSS decision) - with uniform sampling
	entropy := computeBytePositionEntropy(data, bitsPerValue/8)
	stats.BytePositionEntropy = &entropy
}

// computeFloat32Stats computes statistics for float32 arrays
func computeFloat32Stats(stats *Statistics, buffer *arrow.Buffer, numValues int) {
	data := buffer.Bytes()

	dataSize := uint64(len(data))
	stats.DataSize = &dataSize

	// For floats, we primarily care about byte position entropy for BSS
	entropy := computeBytePositionEntropy(data, 4)
	stats.BytePositionEntropy = &entropy

	// Run count for floats (with bit-level comparison)
	values := buffer.Float32()
	runCount := computeRunCountFloat32(values)
	stats.RunCount = &runCount
}

// computeFloat64Stats computes statistics for float64 arrays
func computeFloat64Stats(stats *Statistics, buffer *arrow.Buffer, numValues int) {
	data := buffer.Bytes()

	dataSize := uint64(len(data))
	stats.DataSize = &dataSize

	entropy := computeBytePositionEntropy(data, 8)
	stats.BytePositionEntropy = &entropy

	values := buffer.Float64()
	runCount := computeRunCountFloat64(values)
	stats.RunCount = &runCount
}

// computeMaxBitWidth32 calculates the maximum bit width needed for a chunk of int32 values
// Note: This function assumes non-negative integers. For signed integers, consider using
// ZigZag encoding or storing the sign bit separately.
func computeMaxBitWidth32(values []int32) uint64 {
	if len(values) == 0 {
		return 0
	}

	var maxOr uint32

	// Fast path for arrays with at least 8 elements
	if len(values) >= 8 {
		i := 0
		limit := len(values) - 3

		// Unrolled loop for better performance (process 4 values at once)
		for i < limit {
			maxOr |= uint32(values[i]) |
				uint32(values[i+1]) |
				uint32(values[i+2]) |
				uint32(values[i+3])
			i += 4
		}

		// Handle remaining values
		for ; i < len(values); i++ {
			maxOr |= uint32(values[i])
		}
	} else {
		// Small array: simple loop
		for _, v := range values {
			maxOr |= uint32(v)
		}
	}

	if maxOr == 0 {
		return 0
	}
	return uint64(32 - bits.LeadingZeros32(maxOr))
}

// computeMaxBitWidth64 calculates the maximum bit width needed for a chunk of int64 values
// Note: This function assumes non-negative integers. For signed integers, consider using
// ZigZag encoding or storing the sign bit separately.
func computeMaxBitWidth64(values []int64) uint64 {
	if len(values) == 0 {
		return 0
	}

	var maxOr uint64

	// Fast path for arrays with at least 8 elements
	if len(values) >= 8 {
		i := 0
		limit := len(values) - 3

		// Unrolled loop for better performance
		for i < limit {
			maxOr |= uint64(values[i]) |
				uint64(values[i+1]) |
				uint64(values[i+2]) |
				uint64(values[i+3])
			i += 4
		}

		// Handle remaining values
		for ; i < len(values); i++ {
			maxOr |= uint64(values[i])
		}
	} else {
		// Small array: simple loop
		for _, v := range values {
			maxOr |= uint64(v)
		}
	}

	if maxOr == 0 {
		return 0
	}
	return uint64(64 - bits.LeadingZeros64(maxOr))
}

// computeRunCount32 counts the number of runs in int32 data
func computeRunCount32(values []int32) uint64 {
	if len(values) == 0 {
		return 0
	}

	runs := uint64(1)
	prev := values[0]

	for i := 1; i < len(values); i++ {
		if values[i] != prev {
			runs++
			prev = values[i]
		}
	}

	return runs
}

// computeRunCount64 counts the number of runs in int64 data
func computeRunCount64(values []int64) uint64 {
	if len(values) == 0 {
		return 0
	}

	runs := uint64(1)
	prev := values[0]

	for i := 1; i < len(values); i++ {
		if values[i] != prev {
			runs++
			prev = values[i]
		}
	}

	return runs
}

// computeRunCountFloat32 counts runs in float32 data using bit-level comparison
// This handles NaN correctly (treats all NaN as equal) and avoids floating-point precision issues
func computeRunCountFloat32(values []float32) uint64 {
	if len(values) == 0 {
		return 0
	}

	runs := uint64(1)
	prevBits := math.Float32bits(values[0])

	for i := 1; i < len(values); i++ {
		currBits := math.Float32bits(values[i])

		// Bit-level comparison: handles NaN and precision issues correctly
		if currBits != prevBits {
			runs++
			prevBits = currBits
		}
	}

	return runs
}

// computeRunCountFloat64 counts runs in float64 data using bit-level comparison
func computeRunCountFloat64(values []float64) uint64 {
	if len(values) == 0 {
		return 0
	}

	runs := uint64(1)
	prevBits := math.Float64bits(values[0])

	for i := 1; i < len(values); i++ {
		currBits := math.Float64bits(values[i])

		if currBits != prevBits {
			runs++
			prevBits = currBits
		}
	}

	return runs
}

// computeCardinality32 approximates cardinality using HyperLogLog with small cardinality correction
func computeCardinality32(values []int32) uint64 {
	// HyperLogLog with 2^4 = 16 registers (precision 4)
	// For m=16, the standard error is ~26%, which is acceptable for compression decisions
	const precision = 4
	const numRegisters = 1 << precision
	registers := make([]uint8, numRegisters)

	for _, v := range values {
		// Use MurmurHash3-based hash (in production, consider xxhash3 for better distribution)
		hash := hashInt32(v)

		// Get register index from first 'precision' bits
		registerIdx := hash >> (32 - precision)

		// Count leading zeros in remaining bits
		remaining := hash << precision
		leadingZeros := uint8(bits.LeadingZeros32(remaining)) + 1

		if leadingZeros > registers[registerIdx] {
			registers[registerIdx] = leadingZeros
		}
	}

	// HLL estimate using harmonic mean
	sum := 0.0
	zeroCount := 0
	for _, reg := range registers {
		if reg == 0 {
			zeroCount++
		}
		sum += math.Pow(2, -float64(reg))
	}

	// Alpha constant for m=16 (from HyperLogLog paper)
	const alpha = 0.673
	estimate := alpha * float64(numRegisters*numRegisters) / sum

	// Small cardinality correction (LinearCounting)
	// Improves accuracy for small sets (< 2.5 * m)
	if estimate <= 2.5*float64(numRegisters) && zeroCount > 0 {
		estimate = float64(numRegisters) * math.Log(float64(numRegisters)/float64(zeroCount))
	}

	return uint64(estimate)
}

// computeCardinality64 approximates cardinality for int64 with small cardinality correction
func computeCardinality64(values []int64) uint64 {
	const precision = 4
	const numRegisters = 1 << precision
	registers := make([]uint8, numRegisters)

	for _, v := range values {
		hash := hashInt64(v)
		registerIdx := hash >> (64 - precision)
		remaining := hash << precision
		leadingZeros := uint8(bits.LeadingZeros64(remaining)) + 1

		if leadingZeros > registers[registerIdx] {
			registers[registerIdx] = leadingZeros
		}
	}

	sum := 0.0
	zeroCount := 0
	for _, reg := range registers {
		if reg == 0 {
			zeroCount++
		}
		sum += math.Pow(2, -float64(reg))
	}

	const alpha = 0.673
	estimate := alpha * float64(numRegisters*numRegisters) / sum

	// Small cardinality correction
	if estimate <= 2.5*float64(numRegisters) && zeroCount > 0 {
		estimate = float64(numRegisters) * math.Log(float64(numRegisters)/float64(zeroCount))
	}

	return uint64(estimate)
}

// computeBytePositionEntropy calculates Shannon entropy for each byte position
// This is used to determine if BSS (Byte Stream Split) encoding is beneficial
// Uses uniform sampling to avoid bias towards data at the beginning
func computeBytePositionEntropy(data []byte, bytesPerValue int) []uint64 {
	const sampleSize = 64 // Sample 64 values like Rust implementation

	if len(data) == 0 || bytesPerValue == 0 {
		return []uint64{}
	}

	numValues := len(data) / bytesPerValue
	if numValues == 0 {
		return []uint64{}
	}

	// Generate uniform sample indices
	sampleIndices := uniformSampleIndices(numValues, sampleSize)
	sampleCount := len(sampleIndices)

	entropies := make([]uint64, bytesPerValue)

	// Calculate entropy for each byte position
	for pos := 0; pos < bytesPerValue; pos++ {
		byteCounts := make([]int, 256)

		// Count byte occurrences at this position with uniform sampling
		for _, idx := range sampleIndices {
			byteOffset := idx*bytesPerValue + pos
			if byteOffset < len(data) {
				byteCounts[data[byteOffset]]++
			}
		}

		// Calculate Shannon entropy: H = -Σ(p * log2(p))
		entropy := 0.0
		total := float64(sampleCount)

		for _, count := range byteCounts {
			if count > 0 {
				p := float64(count) / total
				entropy -= p * math.Log2(p)
			}
		}

		// Scale by 1000 for integer storage (preserves 3 decimal places)
		entropies[pos] = uint64(entropy * 1000)
	}

	return entropies
}

// uniformSampleIndices generates uniformly distributed sample indices
func uniformSampleIndices(numValues, sampleSize int) []int {
	if numValues <= sampleSize {
		// Return all indices
		indices := make([]int, numValues)
		for i := range indices {
			indices[i] = i
		}
		return indices
	}

	// Generate uniformly distributed samples
	indices := make([]int, sampleSize)
	step := float64(numValues) / float64(sampleSize)

	for i := 0; i < sampleSize; i++ {
		indices[i] = int(float64(i) * step)
	}

	return indices
}

// hashInt32 is a MurmurHash3 finalizer for int32
// Provides good avalanche properties for HyperLogLog
// In production, consider using xxhash3 for better distribution
func hashInt32(v int32) uint32 {
	h := uint32(v)
	h ^= h >> 16
	h *= 0x85ebca6b
	h ^= h >> 13
	h *= 0xc2b2ae35
	h ^= h >> 16
	return h
}

// hashInt64 is a MurmurHash3 finalizer for int64
func hashInt64(v int64) uint64 {
	h := uint64(v)
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	return h
}

// GetMaxBitWidth returns the maximum bit width across all chunks
func (s *Statistics) GetMaxBitWidth() uint64 {
	if s.BitWidth == nil || len(*s.BitWidth) == 0 {
		return 0
	}

	maxWidth := uint64(0)
	for _, width := range *s.BitWidth {
		if width > maxWidth {
			maxWidth = width
		}
	}
	return maxWidth
}

// GetAverageEntropy returns the average entropy across all byte positions
func (s *Statistics) GetAverageEntropy() float64 {
	if s.BytePositionEntropy == nil || len(*s.BytePositionEntropy) == 0 {
		return 0.0
	}

	sum := uint64(0)
	for _, entropy := range *s.BytePositionEntropy {
		sum += entropy
	}

	return float64(sum) / float64(len(*s.BytePositionEntropy)) / 1000.0
}

// GetRunRatio returns the ratio of runs to total values (for RLE decision)
// Lower ratio means better RLE compression (threshold: < 0.5)
func (s *Statistics) GetRunRatio() float64 {
	if s.RunCount == nil || s.NumValues == 0 {
		return 1.0 // No compression benefit
	}
	return float64(*s.RunCount) / float64(s.NumValues)
}

// GetCardinalityRatio returns the ratio of unique values to total values (for Dictionary decision)
// Lower ratio means better Dictionary compression (threshold: < 0.5)
func (s *Statistics) GetCardinalityRatio() float64 {
	if s.Cardinality == nil || s.NumValues == 0 {
		return 1.0
	}
	return float64(*s.Cardinality) / float64(s.NumValues)
}

// Validate checks the consistency of computed statistics
func (s *Statistics) Validate() error {
	if s == nil {
		return lerrors.New(lerrors.ErrInvalidArgument).
			Op("statistics_validate").
			Context("reason", "statistics is nil").
			Build()
	}

	if s.NumValues < 0 {
		return lerrors.New(lerrors.ErrInvalidArgument).
			Op("statistics_validate").
			Context("reason", "invalid NumValues").
			Context("value", s.NumValues).
			Build()
	}

	if s.NullCount != nil && *s.NullCount < 0 {
		return lerrors.New(lerrors.ErrInvalidArgument).
			Op("statistics_validate").
			Context("reason", "invalid NullCount").
			Context("value", *s.NullCount).
			Build()
	}

	if s.NullCount != nil && *s.NullCount > s.NumValues {
		return lerrors.New(lerrors.ErrInvalidArgument).
			Op("statistics_validate").
			Context("reason", "NullCount exceeds NumValues").
			Context("null_count", *s.NullCount).
			Context("num_values", s.NumValues).
			Build()
	}

	if s.Cardinality != nil && *s.Cardinality > uint64(s.NumValues) {
		// Note: Due to HyperLogLog estimation error, cardinality might slightly exceed NumValues
		// Only flag as error if it's significantly wrong (> 2x)
		if *s.Cardinality > uint64(s.NumValues)*2 {
			return lerrors.New(lerrors.ErrInvalidArgument).
				Op("statistics_validate").
				Context("reason", "Cardinality significantly exceeds NumValues").
				Context("cardinality", *s.Cardinality).
				Context("num_values", s.NumValues).
				Build()
		}
	}

	if s.RunCount != nil && *s.RunCount > uint64(s.NumValues) {
		return lerrors.New(lerrors.ErrInvalidArgument).
			Op("statistics_validate").
			Context("reason", "RunCount exceeds NumValues").
			Context("run_count", *s.RunCount).
			Context("num_values", s.NumValues).
			Build()
	}

	if s.DataSize != nil && s.NumValues > 0 {
		// Sanity check: data size should be reasonable relative to number of values
		maxExpectedSize := uint64(s.NumValues) * 16 // Assume max 16 bytes per value
		if *s.DataSize > maxExpectedSize {
			return lerrors.New(lerrors.ErrInvalidArgument).
				Op("statistics_validate").
				Context("reason", "DataSize seems too large for NumValues").
				Context("data_size", *s.DataSize).
				Context("num_values", s.NumValues).
				Build()
		}
	}

	return nil
}

// Clone creates a deep copy of the statistics
func (s *Statistics) Clone() *Statistics {
	if s == nil {
		return nil
	}

	clone := &Statistics{
		NumValues:  s.NumValues,
		DataType:   s.DataType,
		ComputedAt: s.ComputedAt,
		IsComplete: s.IsComplete,
	}

	if s.NullCount != nil {
		nullCount := *s.NullCount
		clone.NullCount = &nullCount
	}

	if s.BitWidth != nil {
		bitWidths := make([]uint64, len(*s.BitWidth))
		copy(bitWidths, *s.BitWidth)
		clone.BitWidth = &bitWidths
	}

	if s.DataSize != nil {
		dataSize := *s.DataSize
		clone.DataSize = &dataSize
	}

	if s.MaxLength != nil {
		maxLength := *s.MaxLength
		clone.MaxLength = &maxLength
	}

	if s.RunCount != nil {
		runCount := *s.RunCount
		clone.RunCount = &runCount
	}

	if s.Cardinality != nil {
		cardinality := *s.Cardinality
		clone.Cardinality = &cardinality
	}

	if s.BytePositionEntropy != nil {
		entropy := make([]uint64, len(*s.BytePositionEntropy))
		copy(entropy, *s.BytePositionEntropy)
		clone.BytePositionEntropy = &entropy
	}

	return clone
}
