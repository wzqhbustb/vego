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
	// ========== Adaptive Configuration Logic ==========
	if config.Adaptive && config.Dimension > 0 {
		adaptive := calculateAdaptiveParams(config.Dimension, config.ExpectedSize)

		// Only override values not explicitly set by user (<= 0 means unset)
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
		ef = max(200, k*2)
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

// calculateAdaptiveParams calculates optimal parameters based on dimension and expected dataset size
func calculateAdaptiveParams(dimension, expectedSize int) Config {
	// If expectedSize is not set, use default value 10K
	if expectedSize <= 0 {
		expectedSize = 10000
	}

	// ========== Calculate Optimal M ==========
	// Principle: Higher dimensions require more connections to maintain graph connectivity
	m := 16 // 基础默认值

	switch {
	case dimension <= 128:
		// Low-dimensional vectors (small embeddings): Standard configuration
		m = 16
	case dimension <= 512:
		// Medium-dimensional vectors (BERT base, etc.): Moderately increase connections
		m = 24
	case dimension <= 1024:
		// High-dimensional vectors (BERT large, etc.): Need more connections
		m = 32
	default:
		// Ultra-high-dimensional vectors (OpenAI text-embedding-3 1536, etc.): Maximum connections
		m = 48
	}

	// ========== Calculate Optimal EfConstruction ==========
	// Base value
	efConstruction := 200

	// 1. Logarithmic growth based on dataset size
	// Modified: Use stronger scaling factor 200 (was 100)
	// Formula: ef = 200 + 200 * log10(N/10000)
	if expectedSize > 10000 {
		scaleFactor := math.Log10(float64(expectedSize) / 10000.0)
		efConstruction = int(200 + 200*scaleFactor) // Key modification: 100 → 200
	}

	// 2. Extra boost for large-scale datasets
	// For datasets >50K, add another 30% to ensure build quality
	// This is critical for 100K datasets (400 * 1.3 = 520)
	if expectedSize > 50000 {
		efConstruction = int(float64(efConstruction) * 1.3)
	}

	// 3. High dimensions require more exploration
	// When dimension > 512, increase efConstruction by 50%
	if dimension > 512 {
		efConstruction = int(float64(efConstruction) * 1.5)
	}

	// ========== Set Upper Bound Protection ==========
	if efConstruction > 800 {
		efConstruction = 800 // Prevent memory explosion
	}
	if m > 64 {
		m = 64 // Prevent too many connections from slowing down search
	}

	return Config{
		M:              m,
		EfConstruction: efConstruction,
		DistanceFunc:   L2Distance,
	}
}
