// adaptive_config.go - Adaptive parameter configuration
package hnsw

import "math"

// AdaptiveConfig generates optimal HNSW parameters based on data characteristics
func AdaptiveConfig(dimension, expectedDatasetSize int) Config {
	return Config{
		Dimension:      dimension,
		M:              calculateOptimalM(dimension),
		EfConstruction: calculateOptimalEfConstruction(dimension, expectedDatasetSize),
		DistanceFunc:   L2Distance,
	}
}

// calculateOptimalM - Higher dimensions require more connections to maintain graph connectivity
func calculateOptimalM(dimension int) int {
	switch {
	case dimension <= 128:
		return 16
	case dimension <= 512:
		return 24
	case dimension <= 1024:
		return 32
	default: // 1536+ (OpenAI embedding)
		return 48
	}
}

// calculateOptimalEfConstruction - Grows logarithmically with dataset size
func calculateOptimalEfConstruction(dimension, datasetSize int) int {
	baseEf := 200

	// Dataset size factor: log10(N/10K)
	if datasetSize > 10000 {
		scaleFactor := math.Log10(float64(datasetSize) / 10000.0)
		baseEf = int(200 + 100*scaleFactor)
	}

	// High dimensions require more exploration
	if dimension > 512 {
		baseEf = int(float64(baseEf) * 1.5)
	}

	if baseEf > 800 {
		return 800
	}
	return baseEf
}

// calculateOptimalQueryEf - Dynamically calculates ef at search time
func calculateOptimalQueryEf(datasetSize, topK int) int {
	// Empirical formula: ef = 2*k + 50*log10(N/1000)
	baseEf := 2 * topK
	datasetScale := math.Log10(float64(datasetSize)/1000.0 + 1.0)
	ef := int(float64(baseEf) * (1.0 + datasetScale*0.5))

	if ef < topK*2 {
		return topK * 2
	}
	if ef > 1000 {
		return 1000
	}
	return ef
}

// SearchWithAdaptiveEf - Search using adaptive ef
func (h *HNSWIndex) SearchWithAdaptiveEf(query []float32, k int) ([]SearchResult, error) {
	ef := calculateOptimalQueryEf(len(h.nodes), k)
	return h.Search(query, k, ef)
}
