package hnsw

import (
	"math"
	"math/rand"
	"sync"
	"time"
)

// The main structure of the HNSW index.
type HNSWIndex struct {
	// Core params
	M              int     // Maximum number of connections per level.
	Mmax           int     // The real value of the M.
	Mmax0          int     // Maximum number of connections at level 0.(2*M).
	efConstruction int     // Size of the dynamic list for the nearest neighbors during construction.
	ml             float64 // Level multiplier(normalization) (1/ln(M)).

	dimension int // Dimensionality of the vectors.

	nodes      []*Node // All nodes in the HNSW graph.
	entryPoint int32   // Entry point node ID.
	maxLevel   int32   // Maximum level in the HNSW hierarchy.

	distFunc DistanceFunc // Distance function used for measuring similarity.

	globalLock sync.RWMutex // Protects the entire index during insertions.

	rng *rand.Rand // Random number generator for level assignment.
	mu  sync.Mutex // Protects the RNG.
}

// Config holds the configuration parameters for the HNSW index.
type Config struct {
	M              int          // Maximum number of connections per level, default 16.
	EfConstruction int          // default 200.
	Dimension      int          // Vector dimensionality.
	DistanceFunc   DistanceFunc // default L2Distance.
	Seed           int64        // Seed for random level generation.
	Adaptive       bool         // If true, automatically calculate M and EfConstruction based on Dimension and ExpectedSize
	ExpectedSize   int          // Expected dataset size for adaptive parameter calculation (default: 10000)
}

func NewHNSW(config Config) *HNSWIndex {
	// ========== 自适应配置逻辑 ==========
	if config.Adaptive && config.Dimension > 0 {
		adaptive := calculateAdaptiveParams(config.Dimension, config.ExpectedSize)

		// 只覆盖用户未显式设置的值（<= 0 表示未设置）
		if config.M <= 0 {
			config.M = adaptive.M
		}
		if config.EfConstruction <= 0 {
			config.EfConstruction = adaptive.EfConstruction
		}
		if config.DistanceFunc == nil {
			config.DistanceFunc = adaptive.DistanceFunc
		}
	}

	if config.M <= 0 {
		config.M = 16
	}
	if config.Dimension <= 0 {
		panic("dimension must be positive")
	}
	if config.EfConstruction <= 0 {
		config.EfConstruction = 200
	}
	if config.DistanceFunc == nil {
		config.DistanceFunc = L2Distance
	}
	if config.Seed == 0 {
		config.Seed = time.Now().UnixNano()
	}

	// normalization factor for level generation
	ml := 1.0 / math.Log(float64(config.M))

	return &HNSWIndex{
		M:              config.M,
		Mmax:           config.M,
		Mmax0:          config.M * 2,
		efConstruction: config.EfConstruction,
		ml:             ml,
		dimension:      config.Dimension,
		nodes:          make([]*Node, 0, 10000),
		entryPoint:     -1, // -1 means no nodes yet
		maxLevel:       -1,
		distFunc:       config.DistanceFunc,
		rng:            rand.New(rand.NewSource(config.Seed)),
	}
}

// Add inserts a new vector into the HNSW index and returns its assigned node ID.
func (h *HNSWIndex) Add(vector []float32) (int, error) {
	if len(vector) != h.dimension {
		return -1, ErrDimensionMismatch
	}

	vectorCopy := make([]float32, len(vector))
	copy(vectorCopy, vector)

	// Generate a random level for the new node
	level := h.randomLevel()

	// Create the new node
	h.globalLock.Lock()
	nodeID := len(h.nodes)
	newNode := NewNode(nodeID, vectorCopy, level)
	h.nodes = append(h.nodes, newNode)
	h.globalLock.Unlock()

	if nodeID == 0 {
		h.globalLock.Lock()
		h.entryPoint = int32(nodeID)
		h.maxLevel = int32(level)
		h.globalLock.Unlock()
		return nodeID, nil
	}

	h.insert(newNode)

	return nodeID, nil
}

func (h *HNSWIndex) Search(query []float32, k int, ef int) ([]SearchResult, error) {
	if len(query) != h.dimension {
		return nil, ErrDimensionMismatch
	}

	if ef == 0 {
		ef = max(h.efConstruction, k)
	}

	h.globalLock.RLock()
	if h.entryPoint == -1 {
		h.globalLock.RUnlock()
		return nil, ErrEmptyIndex
	}
	ep := h.entryPoint
	maxLvl := h.maxLevel
	h.globalLock.RUnlock()

	return h.search(query, k, ef, int(ep), int(maxLvl))

}

// Len returns the number of nodes in the HNSW index.
func (h *HNSWIndex) Len() int {
	h.globalLock.RLock()
	defer h.globalLock.RUnlock()
	return len(h.nodes)
}

// randomLevel generates a random level for a new node based on an exponential distribution.
func (h *HNSWIndex) randomLevel() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	// Generate a uniform random number in (0,1)
	uniform := h.rng.Float64()
	// Calculate the level using the negative logarithm

	// Generate level using exponential distribution, simulating skip list's probabilistic layering
	// Formula: level = floor(-ln(U) * ml), where U ~ Uniform(0,1), ml = 1/ln(M)
	// Ensures P(level >= L) = (1/M)^L, meaning higher level nodes decay exponentially
	// For example, when M=16:
	//   - ~93.75% nodes at level 0
	//   - ~6.25% nodes at level 1
	//   - ~0.39% nodes at level 2
	level := int(math.Floor(-math.Log(uniform) * h.ml))

	maxLevel := 16 // Large enough, rarely exceeds 10 levels in practice
	if level > maxLevel {
		level = maxLevel
	}
	return level
}

// SearchResult represents a single search result with its ID and distance.
type SearchResult struct {
	ID       int
	Distance float32
}

// Helper function
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// calculateAdaptiveParams 根据维度和预期数据规模计算最优参数
func calculateAdaptiveParams(dimension, expectedSize int) Config {
	// 如果 expectedSize 未设置，使用默认值 10K
	if expectedSize <= 0 {
		expectedSize = 10000
	}

	// ========== 计算最优 M ==========
	// 原则：维度越高，需要更多连接保持图连通性
	m := 16 // 基础默认值

	switch {
	case dimension <= 128:
		// 低维向量 (小型 embeddings)：标准配置
		m = 16
	case dimension <= 512:
		// 中维向量 (BERT base 等)：适当增加连接
		m = 24
	case dimension <= 1024:
		// 高维向量 (BERT large 等)：需要更多连接
		m = 32
	default:
		// 超高维向量 (OpenAI text-embedding-3 1536 等)：最大连接
		m = 48
	}

	// ========== 计算最优 EfConstruction ==========
	// 基础值
	efConstruction := 200

	// 1. 基于数据规模的对数增长
	// 公式：ef = 200 + 100 * log10(N/10000)
	if expectedSize > 10000 {
		scaleFactor := math.Log10(float64(expectedSize) / 10000.0)
		efConstruction = int(200 + 100*scaleFactor)
	}

	// 2. 高维需要更多探索
	// 维度 > 512 时，efConstruction 增加 50%
	if dimension > 512 {
		efConstruction = int(float64(efConstruction) * 1.5)
	}

	// 3. 数据量特别大时需要更高 ef
	// 超过 100万时，额外增加
	if expectedSize > 1000000 {
		efConstruction = int(float64(efConstruction) * 1.3)
	}

	// ========== 设置上限保护 ==========
	if efConstruction > 800 {
		efConstruction = 800 // 防止内存爆炸
	}
	if m > 64 {
		m = 64 // 防止连接过多导致搜索变慢
	}

	return Config{
		M:              m,
		EfConstruction: efConstruction,
		DistanceFunc:   L2Distance,
	}
}
