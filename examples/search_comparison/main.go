// Search Comparison Example
// This example compares different distance functions and EF values.
//
// Run: go run main.go
package main

import (
	"fmt"
	"math"
	"math/rand"
	"time"

	hnsw "github.com/wzqhbustb/vego/index"
)

func main() {
	fmt.Println("=== Vego Search Comparison Demo ===")
	fmt.Println()

	vectorCount := 2000
	dimension := 128

	// Generate random vectors
	fmt.Printf("Generating %d random vectors...\n", vectorCount)
	vectors := make([][]float32, vectorCount)
	for i := 0; i < vectorCount; i++ {
		vec := make([]float32, dimension)
		for j := range vec {
			vec[j] = rand.Float32()
		}
		vectors[i] = vec
	}
	fmt.Println()

	// Create indexes with different distance functions
	configs := []struct {
		name   string
		config hnsw.Config
	}{
		{
			name: "L2 Distance (Euclidean)",
			config: hnsw.Config{
				Dimension:      dimension,
				DistanceFunc:   hnsw.L2Distance,
				M:              16,
				EfConstruction: 200,
			},
		},
		{
			name: "Cosine Distance",
			config: hnsw.Config{
				Dimension:      dimension,
				DistanceFunc:   hnsw.CosineDistance,
				M:              16,
				EfConstruction: 200,
			},
		},
		{
			name: "Inner Product",
			config: hnsw.Config{
				Dimension:      dimension,
				DistanceFunc:   hnsw.InnerProductDistance,
				M:              16,
				EfConstruction: 200,
			},
		},
	}

	indexes := make([]*hnsw.HNSWIndex, len(configs))

	// Build indexes
	for i, cfg := range configs {
		fmt.Printf("Building index with %s...\n", cfg.name)
		indexes[i] = hnsw.NewHNSW(cfg.config)
		
		start := time.Now()
		for _, vec := range vectors {
			indexes[i].Add(vec)
		}
		elapsed := time.Since(start)
		
		fmt.Printf("  ✓ Built index with %d vectors in %v\n", indexes[i].Len(), elapsed)
		fmt.Println()
	}

	// Generate a query vector
	query := make([]float32, dimension)
	for j := range query {
		query[j] = rand.Float32()
	}
	fmt.Println("Query vector generated")
	fmt.Println()

	// Compare search results with different EF values
	fmt.Println("=== Search Comparison ===")
	fmt.Println()

	efValues := []int{50, 100, 200}
	k := 10

	for i, cfg := range configs {
		fmt.Printf("--- %s ---\n", cfg.name)
		
		for _, ef := range efValues {
			start := time.Now()
			results, err := indexes[i].Search(query, k, ef)
			if err != nil {
				fmt.Printf("  Error: %v\n", err)
				continue
			}
			elapsed := time.Since(start)

			distances := make([]float32, len(results))
			for j, r := range results {
				distances[j] = r.Distance
			}

			fmt.Printf("  EF=%d:\n", ef)
			fmt.Printf("    Time: %v\n", elapsed)
			fmt.Printf("    Distances: [%.4f, %.4f, ..., %.4f]\n", 
				distances[0], distances[1], distances[len(distances)-1])
			fmt.Printf("    Avg Distance: %.4f\n", avg(distances))
		}
		fmt.Println()
	}

	// Compare distance metrics for the same vectors
	fmt.Println("=== Distance Metric Comparison ===")
	fmt.Println()
	
	// Pick two vectors to compare
	vec1 := vectors[0]
	vec2 := vectors[1]
	
	fmt.Printf("Comparing two vectors:\n")
	fmt.Printf("  Vector 1: ID=0, first 5 values = [%.4f, %.4f, %.4f, %.4f, %.4f]\n",
		vec1[0], vec1[1], vec1[2], vec1[3], vec1[4])
	fmt.Printf("  Vector 2: ID=1, first 5 values = [%.4f, %.4f, %.4f, %.4f, %.4f]\n",
		vec2[0], vec2[1], vec2[2], vec2[3], vec2[4])
	fmt.Println()
	
	l2Dist := hnsw.L2Distance(vec1, vec2)
	cosineDist := hnsw.CosineDistance(vec1, vec2)
	innerProd := hnsw.InnerProductDistance(vec1, vec2)
	
	fmt.Printf("Distance Metrics:\n")
	fmt.Printf("  L2 Distance:    %.6f\n", l2Dist)
	fmt.Printf("  Cosine Distance: %.6f (similarity = %.6f)\n", cosineDist, 1-cosineDist)
	fmt.Printf("  Inner Product:  %.6f\n", innerProd)
	fmt.Println()

	// Explanation
	fmt.Println("=== Distance Function Guide ===")
	fmt.Println()
	fmt.Println("L2 Distance (Euclidean):")
	fmt.Println("  - Measures straight-line distance between vectors")
	fmt.Println("  - Use when: magnitude matters, physical space similarity")
	fmt.Println()
	fmt.Println("Cosine Distance:")
	fmt.Println("  - Measures angle between vectors (ignores magnitude)")
	fmt.Println("  - Use when: direction matters more than magnitude")
	fmt.Println("  - Common for: text embeddings, document similarity")
	fmt.Println()
	fmt.Println("Inner Product:")
	fmt.Println("  - Measures projection of one vector onto another")
	fmt.Println("  - Use when: maximizing alignment, some neural network outputs")
	fmt.Println()

	// EF parameter guide
	fmt.Println("=== EF Parameter Guide ===")
	fmt.Println()
	fmt.Println("EF (Search Scope) affects recall vs speed trade-off:")
	fmt.Println("  - Low EF (50):   Fast search, lower recall")
	fmt.Println("  - Mid EF (100):  Balanced speed and recall")
	fmt.Println("  - High EF (200): Slower search, higher recall")
	fmt.Println()
	fmt.Println("Recommendation:")
	fmt.Println("  - Online serving (low latency): EF=50-100")
	fmt.Println("  - Offline batch processing:     EF=200-400")
	fmt.Println("  - High recall requirement:      EF=400+")
	fmt.Println()
	fmt.Println("=== Demo completed! ===")
}

func avg(values []float32) float32 {
	if len(values) == 0 {
		return 0
	}
	sum := float32(0)
	for _, v := range values {
		sum += v
	}
	return sum / float32(len(values))
}

func magnitude(v []float32) float32 {
	sum := float32(0)
	for _, x := range v {
		sum += x * x
	}
	return float32(math.Sqrt(float64(sum)))
}
