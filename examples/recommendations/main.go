// Recommendation System Example
// This example demonstrates how to use Vego to build a recommendation engine.
// It showcases user-based and item-based collaborative filtering patterns
// using vector similarity search.
//
// Real-world use cases:
//   - E-commerce product recommendations
//   - Content streaming suggestions
//   - News article recommendations
//   - Social media feed ranking
//
// Run: go run main.go
package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sort"

	"github.com/wzqhbustb/vego/vego"
)

// Product represents an item in our catalog
type Product struct {
	ID       string
	Name     string
	Category string
	Price    float64
	Brand    string
}

// User represents a customer with preferences
type User struct {
	ID       string
	Name     string
	AgeGroup string
	Region   string
}

func main() {
	fmt.Println("=== Vego Recommendation System Demo ===")
	fmt.Println()
	fmt.Println("This example demonstrates how to build a recommendation")
	fmt.Println("engine using vector similarity. Items and users are encoded")
	fmt.Println("as vectors, enabling fast similarity-based recommendations.")
	fmt.Println()

	// Create temporary directory
	tmpDir, err := os.MkdirTemp("", "vego_recommendations_demo")
	if err != nil {
		fmt.Printf("Failed to create temp directory: %v\n", err)
		os.Exit(1)
	}
	defer os.RemoveAll(tmpDir)

	// Step 1: Initialize database
	fmt.Println("🛒 Initializing recommendation engine...")
	db, err := vego.Open(tmpDir, vego.WithDimension(128))
	if err != nil {
		fmt.Printf("Failed to open database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	products, err := db.Collection("products")
	if err != nil {
		fmt.Printf("Failed to get products collection: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("✓ Recommendation engine initialized")
	fmt.Println()

	// Step 2: Define product catalog
	catalog := []Product{
		{ID: "p-001", Name: "Wireless Headphones", Category: "electronics", Price: 199.99, Brand: "AudioTech"},
		{ID: "p-002", Name: "Running Shoes", Category: "sports", Price: 129.99, Brand: "RunFast"},
		{ID: "p-003", Name: "Smart Watch", Category: "electronics", Price: 299.99, Brand: "TechGear"},
		{ID: "p-004", Name: "Yoga Mat", Category: "sports", Price: 49.99, Brand: "ZenFit"},
		{ID: "p-005", Name: "Gaming Laptop", Category: "electronics", Price: 1499.99, Brand: "GamePro"},
		{ID: "p-006", Name: "Coffee Maker", Category: "home", Price: 89.99, Brand: "BrewMaster"},
		{ID: "p-007", Name: "Bluetooth Speaker", Category: "electronics", Price: 79.99, Brand: "AudioTech"},
		{ID: "p-008", Name: "Fitness Tracker", Category: "sports", Price: 99.99, Brand: "RunFast"},
		{ID: "p-009", Name: "Standing Desk", Category: "home", Price: 399.99, Brand: "ErgoLife"},
		{ID: "p-010", Name: "Mechanical Keyboard", Category: "electronics", Price: 149.99, Brand: "KeyMaster"},
		{ID: "p-011", Name: "Protein Powder", Category: "sports", Price: 39.99, Brand: "MuscleMax"},
		{ID: "p-012", Name: "Air Purifier", Category: "home", Price: 249.99, Brand: "CleanAir"},
	}

	// Step 3: Index products with feature vectors
	// In production, these vectors come from a model trained on product attributes,
	// descriptions, images, or user interaction patterns.
	fmt.Println("📦 Indexing product catalog...")
	ctx := context.Background()

	for _, p := range catalog {
		vector := generateProductVector(p, 128)

		doc := &vego.Document{
			ID:     p.ID,
			Vector: vector,
			Metadata: map[string]interface{}{
				"name":     p.Name,
				"category": p.Category,
				"price":    p.Price,
				"brand":    p.Brand,
			},
		}

		if err := products.InsertContext(ctx, doc); err != nil {
			fmt.Printf("Failed to insert product %s: %v\n", p.ID, err)
			continue
		}
	}
	fmt.Printf("✓ Indexed %d products\n\n", len(catalog))

	// Step 4: Simulate user browsing history
	// A user's "interest vector" is derived from items they've viewed/purchased.
	fmt.Println("═══════════════════════════════════════════════════════════════")
	fmt.Println("  Scenario 1: 'Customers who viewed this also viewed...'")
	fmt.Println("═══════════════════════════════════════════════════════════════")
	fmt.Println()

	// User is currently viewing the Smart Watch
	viewedProduct := catalog[2] // Smart Watch
	fmt.Printf("User is viewing: %s ($%.2f, %s)\n", viewedProduct.Name, viewedProduct.Price, viewedProduct.Category)
	fmt.Println()

	viewedVector := generateProductVector(viewedProduct, 128)
	results, err := products.SearchContext(ctx, viewedVector, 4)
	if err != nil {
		fmt.Printf("Search failed: %v\n", err)
	} else {
		fmt.Println("Recommended (item similarity):")
		for i, r := range results {
			if r.Document.ID == viewedProduct.ID {
				continue // Skip the item itself
			}
			similarity := 1.0 - float64(r.Distance)
			fmt.Printf("  %d. %s ($%.2f, %s) - %.1f%% match\n",
				i,
				r.Document.Metadata["name"],
				r.Document.Metadata["price"],
				r.Document.Metadata["category"],
				similarity*100)
		}
	}
	fmt.Println()

	// Step 5: Category-constrained recommendations
	fmt.Println("═══════════════════════════════════════════════════════════════")
	fmt.Println("  Scenario 2: Category-filtered Recommendations")
	fmt.Println("═══════════════════════════════════════════════════════════════")
	fmt.Println()

	fmt.Println("User bought a Gaming Laptop. Recommend electronics accessories:")
	fmt.Println()

	electronicsFilter := &vego.MetadataFilter{
		Field:    "category",
		Operator: "eq",
		Value:    "electronics",
	}

	laptopVector := generateProductVector(catalog[4], 128) // Gaming Laptop
	results, err = products.SearchWithFilter(laptopVector, 5, electronicsFilter)
	if err != nil {
		fmt.Printf("Filtered search failed: %v\n", err)
	} else {
		for i, r := range results {
			if r.Document.ID == "p-005" {
				continue
			}
			similarity := 1.0 - float64(r.Distance)
			fmt.Printf("  %d. %s ($%.2f) - %.1f%% match\n",
				i+1,
				r.Document.Metadata["name"],
				r.Document.Metadata["price"],
				similarity*100)
		}
	}
	fmt.Println()

	// Step 6: Price-range constrained recommendations
	fmt.Println("═══════════════════════════════════════════════════════════════")
	fmt.Println("  Scenario 3: Price-range Aware Recommendations")
	fmt.Println("═══════════════════════════════════════════════════════════════")
	fmt.Println()

	fmt.Println("User likes Wireless Headphones. Recommend items under $100:")
	fmt.Println()

	priceFilter := &vego.AndFilter{
		Filters: []vego.Filter{
			&vego.MetadataFilter{Field: "price", Operator: "lt", Value: 100.0},
			&vego.MetadataFilter{Field: "category", Operator: "ne", Value: "electronics"},
		},
	}

	headphonesVector := generateProductVector(catalog[0], 128)
	results, err = products.SearchWithFilter(headphonesVector, 5, priceFilter)
	if err != nil {
		fmt.Printf("Filtered search failed: %v\n", err)
	} else {
		for i, r := range results {
			similarity := 1.0 - float64(r.Distance)
			fmt.Printf("  %d. %s ($%.2f, %s) - %.1f%% match\n",
				i+1,
				r.Document.Metadata["name"],
				r.Document.Metadata["price"],
				r.Document.Metadata["category"],
				similarity*100)
		}
	}
	fmt.Println()

	// Step 7: Personalized recommendations from user profile
	fmt.Println("═══════════════════════════════════════════════════════════════")
	fmt.Println("  Scenario 4: Personalized User Profile Recommendations")
	fmt.Println("═══════════════════════════════════════════════════════════════")
	fmt.Println()

	// Simulate a user who has purchased: Running Shoes, Yoga Mat, Fitness Tracker
	// Their "taste vector" is the average of these item vectors.
	fmt.Println("User profile: bought Running Shoes, Yoga Mat, Fitness Tracker")
	fmt.Println("Generating personalized recommendations...")
	fmt.Println()

	tasteVector := averageVectors(
		generateProductVector(catalog[1], 128), // Running Shoes
		generateProductVector(catalog[3], 128), // Yoga Mat
		generateProductVector(catalog[7], 128), // Fitness Tracker
	)

	results, err = products.SearchContext(ctx, tasteVector, 5)
	if err != nil {
		fmt.Printf("Search failed: %v\n", err)
	} else {
		// Deduplicate and score
		seen := make(map[string]bool)
		fmt.Println("Personalized recommendations:")
		recNo := 1
		for _, r := range results {
			if seen[r.Document.ID] {
				continue
			}
			seen[r.Document.ID] = true
			if r.Document.ID == "p-002" || r.Document.ID == "p-004" || r.Document.ID == "p-008" {
				continue // Skip already purchased
			}
			similarity := 1.0 - float64(r.Distance)
			fmt.Printf("  %d. %s ($%.2f, %s) - %.1f%% match\n",
				recNo,
				r.Document.Metadata["name"],
				r.Document.Metadata["price"],
				r.Document.Metadata["category"],
				similarity*100)
			recNo++
		}
	}
	fmt.Println()

	// Step 8: Brand affinity recommendations
	fmt.Println("═══════════════════════════════════════════════════════════════")
	fmt.Println("  Scenario 5: Brand Affinity Recommendations")
	fmt.Println("═══════════════════════════════════════════════════════════════")
	fmt.Println()

	fmt.Println("User likes AudioTech products. Recommend other AudioTech items:")
	fmt.Println()

	brandFilter := &vego.MetadataFilter{
		Field:    "brand",
		Operator: "eq",
		Value:    "AudioTech",
	}

	// Search with a generic electronics vector, filtered by brand
	genericElectronics := generateProductVector(Product{Category: "electronics", Brand: "AudioTech"}, 128)
	results, err = products.SearchWithFilter(genericElectronics, 5, brandFilter)
	if err != nil {
		fmt.Printf("Filtered search failed: %v\n", err)
	} else {
		for i, r := range results {
			similarity := 1.0 - float64(r.Distance)
			fmt.Printf("  %d. %s ($%.2f) - %.1f%% match\n",
				i+1,
				r.Document.Metadata["name"],
				r.Document.Metadata["price"],
				similarity*100)
		}
	}
	fmt.Println()

	fmt.Println("=== Recommendation Demo completed! ===")
	fmt.Println()
	fmt.Println("Key concepts demonstrated:")
	fmt.Println("  • Item-to-item collaborative filtering (similar products)")
	fmt.Println("  • Category-constrained recommendations")
	fmt.Println("  • Price-range aware recommendations")
	fmt.Println("  • User taste profile (average of past interactions)")
	fmt.Println("  • Brand affinity filtering")
	fmt.Println()
	fmt.Println("Production tips:")
	fmt.Println("  • Use trained embedding models for better representations")
	fmt.Println("  • Combine with popularity, freshness, and diversity signals")
	fmt.Println("  • A/B test different recommendation strategies")
	fmt.Println("  • Cache user taste vectors for sub-millisecond serving")
	fmt.Println("  • Use batch search for multi-user recommendation requests")
}

// generateProductVector creates a deterministic feature vector for a product.
// In production, this would be the output of a trained neural network.
func generateProductVector(p Product, dim int) []float32 {
	vec := make([]float32, dim)
	seed := hashString(p.ID + p.Category + p.Brand)
	r := rand.New(rand.NewSource(seed))

	// Base random vector
	for i := range vec {
		vec[i] = r.Float32()
	}

	// Inject category signal into first few dimensions
	categorySeed := hashString(p.Category)
	cr := rand.New(rand.NewSource(categorySeed))
	for i := 0; i < dim/4; i++ {
		vec[i] += cr.Float32() * 0.5
	}

	// Inject brand signal into next few dimensions
	brandSeed := hashString(p.Brand)
	br := rand.New(rand.NewSource(brandSeed))
	for i := dim / 4; i < dim/2; i++ {
		vec[i] += br.Float32() * 0.3
	}

	return vec
}

// averageVectors computes the element-wise average of multiple vectors.
func averageVectors(vectors ...[]float32) []float32 {
	if len(vectors) == 0 {
		return nil
	}
	dim := len(vectors[0])
	result := make([]float32, dim)
	for _, v := range vectors {
		for i := range result {
			result[i] += v[i]
		}
	}
	for i := range result {
		result[i] /= float32(len(vectors))
	}
	return result
}

func hashString(s string) int64 {
	var h int64 = 5381
	for _, c := range s {
		h = ((h << 5) + h) + int64(c)
	}
	return h
}

// sortFloat32 is a helper for sorting float32 slices (not used directly but kept for completeness).
func sortFloat32Slice(s []float32) {
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
}
