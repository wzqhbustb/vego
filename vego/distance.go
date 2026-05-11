package vego

import hnsw "github.com/wzqhbustb/vego/index"

// Re-exported distance functions from the index package.
// Consumers should use these instead of importing index/ directly.
var (
	// L2Distance computes the squared L2 (Euclidean) distance.
	L2Distance = hnsw.L2Distance
	// L2DistanceSqrt computes the square root of the L2 distance.
	L2DistanceSqrt = hnsw.L2DistanceSqrt
	// CosineDistance computes the cosine distance (1 - cosine similarity).
	CosineDistance = hnsw.CosineDistance
	// InnerProductDistance computes the negative inner product.
	InnerProductDistance = hnsw.InnerProductDistance
)
