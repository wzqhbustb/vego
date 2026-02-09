package hnsw

import "math"

type DistanceFunc func(a, b []float32) float32

// L2Distance computes the L2 (Euclidean) distance between two vectors.
func L2Distance(a, b []float32) float32 {
	if len(a) != len(b) {
		panic("vector dimensions mismatch")
	}
	var sum float32
	for i := range a {
		diff := a[i] - b[i]
		sum += diff * diff
	}
	return sum // Note: returning squared distance for efficiency
}

// L2DistanceSqrt computes the square root of the L2 distance between two vectors.
func L2DistanceSqrt(a, b []float32) float32 {
	return float32(math.Sqrt(float64(L2Distance(a, b))))
}

// InnerProductDistance computes the inner product distance between two vectors.
// Note: Inner product is not a true distance metric, but is often used in similarity search.
// Distance = 1 - InnerProduct
func InnerProductDistance(a, b []float32) float32 {
	if len(a) != len(b) {
		panic("vector dimensions mismatch")
	}

	var sum float32
	for i := range a {
		sum += a[i] * b[i]
	}

	// We negate the inner product to convert it into a distance metric
	return -sum
}

// CosineDistance computes the cosine distance between two vectors.
// Distance = 1 - CosineSimilarity
func CosineDistance(a, b []float32) float32 {
	if len(a) != len(b) {
		panic("vector dimensions mismatch")
	}

	var dotProduct, normA, normB float32

	for i := range a {
		dotProduct += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}

	if normA == 0 || normB == 0 {
		return 1.0
	}

	cosineSim := dotProduct / (float32(math.Sqrt(float64(normA))) * float32(math.Sqrt(float64(normB))))

	return 1.0 - cosineSim
}
