// Metadata Filtering Example
// This example demonstrates how to use metadata filters for refined vector search.
// Filters allow you to search within specific subsets of your data.
//
// Run: go run main.go
package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"

	"github.com/wzqhbustb/vego/vego"
)

func main() {
	fmt.Println("=== Vego Collection API - Metadata Filtering Demo ===")
	fmt.Println()

	// Create temporary directory
	tmpDir, _ := os.MkdirTemp("", "vego_filter_demo")
	defer os.RemoveAll(tmpDir)

	// Initialize database and collection
	db, err := vego.Open(tmpDir, vego.WithDimension(128))
	if err != nil {
		panic(err)
	}
	defer db.Close()

	products, err := db.Collection("products")
	if err != nil {
		panic(err)
	}
	fmt.Println("✓ Initialized product catalog")
	fmt.Println()

	// Insert sample products with rich metadata
	ctx := context.Background()
	fmt.Println("📦 Inserting products...")

	productList := []*vego.Document{
		{
			ID:     "prod-001",
			Vector: generateVector(128, 1),
			Metadata: map[string]interface{}{
				"name":        "Wireless Headphones Pro",
				"category":    "electronics",
				"brand":       "TechCo",
				"price":       299.99,
				"rating":      4.5,
				"in_stock":    true,
				"tags":        []string{"audio", "wireless", "bluetooth"},
				"release_year": 2024,
			},
		},
		{
			ID:     "prod-002",
			Vector: generateVector(128, 2),
			Metadata: map[string]interface{}{
				"name":        "Laptop Stand Aluminum",
				"category":    "accessories",
				"brand":       "ErgoLife",
				"price":       49.99,
				"rating":      4.2,
				"in_stock":    true,
				"tags":        []string{"ergonomic", "office", "laptop"},
				"release_year": 2023,
			},
		},
		{
			ID:     "prod-003",
			Vector: generateVector(128, 3),
			Metadata: map[string]interface{}{
				"name":        "Gaming Mouse RGB",
				"category":    "electronics",
				"brand":       "GameTech",
				"price":       79.99,
				"rating":      4.7,
				"in_stock":    false,
				"tags":        []string{"gaming", "mouse", "rgb"},
				"release_year": 2024,
			},
		},
		{
			ID:     "prod-004",
			Vector: generateVector(128, 4),
			Metadata: map[string]interface{}{
				"name":        "USB-C Hub 7-in-1",
				"category":    "accessories",
				"brand":       "TechCo",
				"price":       59.99,
				"rating":      4.0,
				"in_stock":    true,
				"tags":        []string{"usb", "hub", "connectivity"},
				"release_year": 2023,
			},
		},
		{
			ID:     "prod-005",
			Vector: generateVector(128, 5),
			Metadata: map[string]interface{}{
				"name":        "Mechanical Keyboard",
				"category":    "electronics",
				"brand":       "KeyMaster",
				"price":       149.99,
				"rating":      4.8,
				"in_stock":    true,
				"tags":        []string{"keyboard", "mechanical", "gaming"},
				"release_year": 2024,
			},
		},
		{
			ID:     "prod-006",
			Vector: generateVector(128, 6),
			Metadata: map[string]interface{}{
				"name":        "Webcam 4K Pro",
				"category":    "electronics",
				"brand":       "VisionTech",
				"price":       199.99,
				"rating":      4.3,
				"in_stock":    true,
				"tags":        []string{"camera", "video", "streaming"},
				"release_year": 2024,
			},
		},
		{
			ID:     "prod-007",
			Vector: generateVector(128, 7),
			Metadata: map[string]interface{}{
				"name":        "Desk Lamp LED",
				"category":    "accessories",
				"brand":       "LightWorks",
				"price":       35.99,
				"rating":      4.1,
				"in_stock":    true,
				"tags":        []string{"lighting", "desk", "led"},
				"release_year": 2022,
			},
		},
		{
			ID:     "prod-008",
			Vector: generateVector(128, 8),
			Metadata: map[string]interface{}{
				"name":        "Noise Cancelling Earbuds",
				"category":    "electronics",
				"brand":       "TechCo",
				"price":       179.99,
				"rating":      4.6,
				"in_stock":    false,
				"tags":        []string{"audio", "wireless", "noise-cancelling"},
				"release_year": 2024,
			},
		},
	}

	for _, p := range productList {
		if err := products.InsertContext(ctx, p); err != nil {
			panic(err)
		}
	}
	fmt.Printf("✓ Inserted %d products\n\n", len(productList))

	query := generateVector(128, 1) // Similar to electronics products

	// Demo 1: Simple Equality Filter
	fmt.Println("═══════════════════════════════════════════════════════════")
	fmt.Println("Demo 1: Simple Equality Filter (category = 'electronics')")
	fmt.Println("═══════════════════════════════════════════════════════════")

	filter1 := &vego.MetadataFilter{
		Field:    "category",
		Operator: "eq",
		Value:    "electronics",
	}

	results, _ := products.SearchWithFilter(query, 10, filter1)
	fmt.Printf("Found %d electronics products:\n", len(results))
	for _, r := range results {
		fmt.Printf("  • %s ($%.2f)\n", r.Document.Metadata["name"], r.Document.Metadata["price"])
	}
	fmt.Println()

	// Demo 2: Numeric Comparison Filter (price < 100)
	fmt.Println("═══════════════════════════════════════════════════════════")
	fmt.Println("Demo 2: Price Filter (price < 100)")
	fmt.Println("═══════════════════════════════════════════════════════════")

	filter2 := &vego.MetadataFilter{
		Field:    "price",
		Operator: "lt",
		Value:    100.0,
	}

	results, _ = products.SearchWithFilter(query, 10, filter2)
	fmt.Printf("Found %d products under $100:\n", len(results))
	for _, r := range results {
		fmt.Printf("  • %s ($%.2f)\n", r.Document.Metadata["name"], r.Document.Metadata["price"])
	}
	fmt.Println()

	// Demo 3: Boolean Filter (in_stock = true)
	fmt.Println("═══════════════════════════════════════════════════════════")
	fmt.Println("Demo 3: Stock Filter (in_stock = true)")
	fmt.Println("═══════════════════════════════════════════════════════════")

	filter3 := &vego.MetadataFilter{
		Field:    "in_stock",
		Operator: "eq",
		Value:    true,
	}

	results, _ = products.SearchWithFilter(query, 10, filter3)
	fmt.Printf("Found %d products in stock:\n", len(results))
	for _, r := range results {
		inStock := r.Document.Metadata["in_stock"].(bool)
		status := "✓"
		if !inStock {
			status = "✗"
		}
		fmt.Printf("  %s %s\n", status, r.Document.Metadata["name"])
	}
	fmt.Println()

	// Demo 4: Rating Filter (rating >= 4.5)
	fmt.Println("═══════════════════════════════════════════════════════════")
	fmt.Println("Demo 4: Quality Filter (rating >= 4.5)")
	fmt.Println("═══════════════════════════════════════════════════════════")

	filter4 := &vego.MetadataFilter{
		Field:    "rating",
		Operator: "gte",
		Value:    4.5,
	}

	results, _ = products.SearchWithFilter(query, 10, filter4)
	fmt.Printf("Found %d highly-rated products (4.5+ stars):\n", len(results))
	for _, r := range results {
		fmt.Printf("  • %s (⭐ %.1f)\n", r.Document.Metadata["name"], r.Document.Metadata["rating"])
	}
	fmt.Println()

	// Demo 5: AND Filter (category = 'electronics' AND in_stock = true)
	fmt.Println("═══════════════════════════════════════════════════════════")
	fmt.Println("Demo 5: AND Filter (electronics AND in stock)")
	fmt.Println("═══════════════════════════════════════════════════════════")

	andFilter := &vego.AndFilter{
		Filters: []vego.Filter{
			&vego.MetadataFilter{Field: "category", Operator: "eq", Value: "electronics"},
			&vego.MetadataFilter{Field: "in_stock", Operator: "eq", Value: true},
		},
	}

	results, _ = products.SearchWithFilter(query, 10, andFilter)
	fmt.Printf("Found %d electronics products in stock:\n", len(results))
	for _, r := range results {
		fmt.Printf("  • %s ($%.2f)\n", r.Document.Metadata["name"], r.Document.Metadata["price"])
	}
	fmt.Println()

	// Demo 6: OR Filter (brand = 'TechCo' OR brand = 'GameTech')
	fmt.Println("═══════════════════════════════════════════════════════════")
	fmt.Println("Demo 6: OR Filter (TechCo OR GameTech products)")
	fmt.Println("═══════════════════════════════════════════════════════════")

	orFilter := &vego.OrFilter{
		Filters: []vego.Filter{
			&vego.MetadataFilter{Field: "brand", Operator: "eq", Value: "TechCo"},
			&vego.MetadataFilter{Field: "brand", Operator: "eq", Value: "GameTech"},
		},
	}

	results, _ = products.SearchWithFilter(query, 10, orFilter)
	fmt.Printf("Found %d products from TechCo or GameTech:\n", len(results))
	for _, r := range results {
		fmt.Printf("  • %s (Brand: %s)\n", r.Document.Metadata["name"], r.Document.Metadata["brand"])
	}
	fmt.Println()

	// Demo 7: Complex Nested Filter
	fmt.Println("═══════════════════════════════════════════════════════════")
	fmt.Println("Demo 7: Complex Filter (2024 electronics, in stock, rating >= 4.5)")
	fmt.Println("═══════════════════════════════════════════════════════════")

	complexFilter := &vego.AndFilter{
		Filters: []vego.Filter{
			&vego.MetadataFilter{Field: "category", Operator: "eq", Value: "electronics"},
			&vego.MetadataFilter{Field: "release_year", Operator: "eq", Value: 2024},
			&vego.MetadataFilter{Field: "in_stock", Operator: "eq", Value: true},
			&vego.MetadataFilter{Field: "rating", Operator: "gte", Value: 4.5},
		},
	}

	results, _ = products.SearchWithFilter(query, 10, complexFilter)
	fmt.Printf("Found %d premium 2024 electronics in stock:\n", len(results))
	for _, r := range results {
		fmt.Printf("  • %s ($%.2f, ⭐ %.1f)\n",
			r.Document.Metadata["name"],
			r.Document.Metadata["price"],
			r.Document.Metadata["rating"])
	}
	fmt.Println()

	// Demo 8: String Contains Filter
	fmt.Println("═══════════════════════════════════════════════════════════")
	fmt.Println("Demo 8: String Contains (name contains 'Pro')")
	fmt.Println("═══════════════════════════════════════════════════════════")

	filter8 := &vego.MetadataFilter{
		Field:    "name",
		Operator: "contains",
		Value:    "Pro",
	}

	results, _ = products.SearchWithFilter(query, 10, filter8)
	fmt.Printf("Found %d products with 'Pro' in name:\n", len(results))
	for _, r := range results {
		fmt.Printf("  • %s\n", r.Document.Metadata["name"])
	}
	fmt.Println()

	fmt.Println("=== Metadata Filtering Demo completed! ===")
	fmt.Println()
	fmt.Println("Available filter operators:")
	fmt.Println("  eq    - Equal")
	fmt.Println("  ne    - Not equal")
	fmt.Println("  gt    - Greater than")
	fmt.Println("  gte   - Greater than or equal")
	fmt.Println("  lt    - Less than")
	fmt.Println("  lte   - Less than or equal")
	fmt.Println("  in    - In list")
	fmt.Println("  contains - String contains")
	fmt.Println()
	fmt.Println("Filter types:")
	fmt.Println("  MetadataFilter - Single field condition")
	fmt.Println("  AndFilter      - All conditions must match")
	fmt.Println("  OrFilter       - Any condition can match")
}

// generateVector creates a deterministic random vector
func generateVector(dim int, seed int) []float32 {
	vec := make([]float32, dim)
	r := rand.New(rand.NewSource(int64(seed)))
	for i := range vec {
		vec[i] = r.Float32()
	}
	return vec
}
