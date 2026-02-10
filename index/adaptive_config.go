// adaptive_config.go - 自适应参数配置
package hnsw

import "math"

// AdaptiveConfig 根据数据特征生成最优 HNSW 参数
func AdaptiveConfig(dimension, expectedDatasetSize int) Config {
	return Config{
		Dimension:      dimension,
		M:              calculateOptimalM(dimension),
		EfConstruction: calculateOptimalEfConstruction(dimension, expectedDatasetSize),
		DistanceFunc:   L2Distance,
	}
}

// calculateOptimalM - 维度越高，需要更多连接保持图连通性
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

// calculateOptimalEfConstruction - 随数据规模对数增长
func calculateOptimalEfConstruction(dimension, datasetSize int) int {
	baseEf := 200

	// 数据规模因子: log10(N/10K)
	if datasetSize > 10000 {
		scaleFactor := math.Log10(float64(datasetSize) / 10000.0)
		baseEf = int(200 + 100*scaleFactor)
	}

	// 高维需要更多探索
	if dimension > 512 {
		baseEf = int(float64(baseEf) * 1.5)
	}

	if baseEf > 800 {
		return 800
	}
	return baseEf
}

// calculateOptimalQueryEf - 搜索时动态计算 ef
func calculateOptimalQueryEf(datasetSize, topK int) int {
	// 经验公式: ef = 2*k + 50*log10(N/1000)
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

// SearchWithAdaptiveEf - 使用自适应 ef 的搜索
func (h *HNSWIndex) SearchWithAdaptiveEf(query []float32, k int) ([]SearchResult, error) {
	ef := calculateOptimalQueryEf(len(h.nodes), k)
	return h.Search(query, k, ef)
}
