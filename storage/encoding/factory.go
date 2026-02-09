package encoding

import (
	"github.com/wzqhbustb/vego/storage/arrow"
	"github.com/wzqhbustb/vego/storage/format"
)

// ====================
// P2: Configuration
// ====================

// EncoderConfig holds configuration for encoder selection
type EncoderConfig struct {
	BitPackingMaxBitWidth uint8
	RLEThreshold          float64
	DictionaryThreshold   float64
	DictionaryMaxSize     int
	BSSEntropyThreshold   float64
	SmallDataThreshold    int64
	RLEEarlyThreshold     float64
	EnableDeltaEncoding   bool
}

// DefaultEncoderConfig returns default configuration
func DefaultEncoderConfig() *EncoderConfig {
	return &EncoderConfig{
		BitPackingMaxBitWidth: 16,
		RLEThreshold:          0.5,
		DictionaryThreshold:   0.5,
		DictionaryMaxSize:     1 << 20,
		BSSEntropyThreshold:   4.0,
		SmallDataThreshold:    100,
		RLEEarlyThreshold:     0.1,
		EnableDeltaEncoding:   false,
	}
}

// ====================
// Encoder Factory
// ====================

// EncoderFactory is responsible for creating encoders with specific configurations.
type EncoderFactory struct {
	compressionLevel int
	config           *EncoderConfig
}

// NewEncoderFactory creates a new encoder factory with default config
func NewEncoderFactory(compressionLevel int) *EncoderFactory {
	return &EncoderFactory{
		compressionLevel: compressionLevel,
		config:           DefaultEncoderConfig(),
	}
}

// NewEncoderFactoryWithConfig creates a new encoder factory with custom config
func NewEncoderFactoryWithConfig(compressionLevel int, config *EncoderConfig) *EncoderFactory {
	if config == nil {
		config = DefaultEncoderConfig()
	}
	return &EncoderFactory{
		compressionLevel: compressionLevel,
		config:           config,
	}
}

// SelectEncoder selects the best encoder based on data type and statistics
func (f *EncoderFactory) SelectEncoder(dtype arrow.DataType, stats *Statistics) Encoder {
	// P0: nil 检查
	if stats == nil {
		return NewZstdEncoder(f.compressionLevel)
	}

	// P0: 小数据量优化
	if stats.NumValues < f.config.SmallDataThreshold {
		return NewZstdEncoder(f.compressionLevel)
	}

	switch dtype.ID() {
	case arrow.INT32, arrow.INT64:
		return f.selectIntegerEncoder(dtype, stats)
	case arrow.FLOAT32, arrow.FLOAT64:
		return f.selectFloatEncoder(dtype, stats)
	case arrow.FIXED_SIZE_LIST:
		return f.selectFixedSizeListEncoder(dtype, stats)
	default:
		return NewZstdEncoder(f.compressionLevel)
	}
}

// selectIntegerEncoder selects encoder for integer types
// 优先级：RLE (极低 run ratio) > Dictionary (极低基数 <10%) > BitPacking > Dictionary (中等基数) > RLE (中等) > Zstd
func (f *EncoderFactory) selectIntegerEncoder(dtype arrow.DataType, stats *Statistics) Encoder {
	maxBitWidth := stats.GetMaxBitWidth()
	runRatio := stats.GetRunRatio()
	cardRatio := stats.GetCardinalityRatio()

	// 第一优先级：RLE（极低的 run ratio）
	if runRatio < f.config.RLEEarlyThreshold {
		return NewRLEEncoder()
	}

	// 第二优先级：Dictionary（极低基数 < 10%，通常比 BitPacking 更好）
	if cardRatio < 0.1 {
		return f.createDictionaryEncoderWithFallback(stats)
	}

	// P2: Delta 编码预留
	if f.config.EnableDeltaEncoding && f.isMonotonic(stats) && maxBitWidth > 16 {
		// return NewDeltaEncoder()
	}

	// 第三优先级：BitPacking（窄整数）
	if maxBitWidth <= uint64(f.config.BitPackingMaxBitWidth) {
		// Safe cast: maxBitWidth <= 64 (which is < 256)
		return NewBitPackingEncoder(uint8(maxBitWidth))
	}

	// 第四优先级：Dictionary（中等基数 10% - 50%）
	if cardRatio < f.config.DictionaryThreshold {
		return f.createDictionaryEncoderWithFallback(stats)
	}

	// 第五优先级：RLE（中等 run ratio）
	if runRatio < f.config.RLEThreshold {
		return NewRLEEncoder()
	}

	return NewZstdEncoder(f.compressionLevel)
}

// selectFloatEncoder selects encoder for float types
func (f *EncoderFactory) selectFloatEncoder(dtype arrow.DataType, stats *Statistics) Encoder {
	// Check if BSS encoding is beneficial (low byte entropy)
	if stats.GetAverageEntropy() < f.config.BSSEntropyThreshold {
		// BSS + Zstd combination
		return NewCombinedEncoder(
			NewBSSEncoder(),
			NewZstdEncoder(f.compressionLevel),
		)
	}

	return NewZstdEncoder(f.compressionLevel)
}

// selectFixedSizeListEncoder handles vector types
func (f *EncoderFactory) selectFixedSizeListEncoder(dtype arrow.DataType, stats *Statistics) Encoder {
	fslType := dtype.(*arrow.FixedSizeListType)
	elemType := fslType.Elem()

	switch elemType.ID() {
	case arrow.FLOAT32, arrow.FLOAT64:
		return f.selectFloatEncoder(elemType, stats)
	case arrow.INT32, arrow.INT64:
		return f.selectIntegerEncoder(elemType, stats)
	default:
		return NewZstdEncoder(f.compressionLevel)
	}
}

// createDictionaryEncoderWithFallback creates Dictionary encoder with fallback to Zstd
func (f *EncoderFactory) createDictionaryEncoderWithFallback(stats *Statistics) Encoder {
	estimatedCardinality := int(float64(stats.NumValues) * stats.GetCardinalityRatio())

	if estimatedCardinality > f.config.DictionaryMaxSize {
		return NewZstdEncoder(f.compressionLevel)
	}

	return NewDictionaryEncoder()
}

// isMonotonic checks if data is monotonically increasing/decreasing
func (f *EncoderFactory) isMonotonic(stats *Statistics) bool {
	// TODO: implement monotonicity detection in Statistics
	return false
}

// GetCompressionLevel returns the compression level
func (f *EncoderFactory) GetCompressionLevel() int {
	return f.compressionLevel
}

// ====================
// Combined Encoder
// ====================

// CombinedEncoder chains multiple encoders (e.g., BSS + Zstd)
type CombinedEncoder struct {
	encoders []Encoder
}

// NewCombinedEncoder creates a new combined encoder
func NewCombinedEncoder(encoders ...Encoder) *CombinedEncoder {
	return &CombinedEncoder{encoders: encoders}
}

// Type returns the encoding type of the last encoder in the chain
func (e *CombinedEncoder) Type() format.EncodingType {
	if len(e.encoders) > 0 {
		return e.encoders[len(e.encoders)-1].Type()
	}
	// 永远不返回 Plain encoding，如果没有 encoder 则默认 Zstd
	// 这种情况理论上不应该发生，但作为防御性编程
	return format.EncodingZstd
}

// Encode applies all encoders in sequence
func (e *CombinedEncoder) Encode(array arrow.Array) (*EncodedData, error) {
	current := array
	var result *EncodedData
	var err error

	for i, encoder := range e.encoders {
		result, err = encoder.Encode(current)
		if err != nil {
			return nil, err
		}

		// If not the last encoder, decode back to array for next encoder
		// Note: This is inefficient but necessary for chaining
		if i < len(e.encoders)-1 {
			// Get decoder for this encoding type
			decoder, decErr := GetDecoder(result.Type)
			if decErr != nil {
				return nil, decErr
			}
			if decoder != nil {
				current, err = decoder.Decode(result.Data, current.DataType())
				if err != nil {
					return nil, err
				}
			}
		}
	}

	return result, nil
}

// EstimateSize estimates the size after all encodings
func (e *CombinedEncoder) EstimateSize(array arrow.Array) int {
	estimated := array.Len() * GetValueSize(array.DataType().ID())
	for _, encoder := range e.encoders {
		estimated = encoder.EstimateSize(array)
	}
	return estimated
}

// SupportsType checks if all encoders in the chain support the given type
func (e *CombinedEncoder) SupportsType(dtype arrow.DataType) bool {
	for _, encoder := range e.encoders {
		if !encoder.SupportsType(dtype) {
			return false
		}
	}
	return true
}
