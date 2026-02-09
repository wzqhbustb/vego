package bench

// BenchmarkConfig 基准测试配置
type BenchmarkConfig struct {
	// 数据规模
	DataSizes []int // 行数，如 [1000, 10000, 100000, 1000000]

	// 向量维度（用于向量测试）
	VectorDims []int // 如 [128, 768, 1536]

	// 列数
	ColumnCounts []int // 如 [1, 5, 10, 50, 100]

	// 压缩级别
	CompressionLevels []int // 如 [1, 3, 9]

	// 并发度
	Concurrencies []int // 如 [1, 4, 8, 16]

	// 输出目录
	OutputDir string

	// 是否保存详细结果
	Verbose bool
}

// DefaultConfig 返回默认配置
func DefaultConfig() *BenchmarkConfig {
	return &BenchmarkConfig{
		DataSizes:         []int{1000, 10000, 100000},
		VectorDims:        []int{768},
		ColumnCounts:      []int{1, 5, 10},
		CompressionLevels: []int{3},
		Concurrencies:     []int{1, 4, 8},
		OutputDir:         "bench_results",
		Verbose:           true,
	}
}
