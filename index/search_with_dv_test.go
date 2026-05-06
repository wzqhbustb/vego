// Copyright 2024 Vego Authors
// Licensed under the Apache License, Version 2.0

package hnsw

import (
	"testing"
)

func TestSearchWithDV(t *testing.T) {
	// Create HNSW index
	config := Config{
		Dimension:      128,
		M:              16,
		EfConstruction: 200,
		DistanceFunc:   L2Distance,
	}
	index := NewHNSW(config)

	// Add 10 vectors
	vectors := make([][]float32, 10)
	for i := 0; i < 10; i++ {
		vec := make([]float32, 128)
		for j := range vec {
			vec[j] = float32(i) * 0.1  // Different values for different IDs
		}
		vectors[i] = vec
		_, err := index.Add(vec)
		if err != nil {
			t.Fatalf("Failed to add vector %d: %v", i, err)
		}
	}

	// Create query vector (similar to vector 5)
	query := make([]float32, 128)
	for j := range query {
		query[j] = float32(5) * 0.1
	}

	t.Run("NoDeletions", func(t *testing.T) {
		// No deletions - should return all 5 results
		isDeleted := func(id int) bool { return false }
		
		results, err := index.SearchWithDV(query, 5, 100, isDeleted)
		if err != nil {
			t.Fatalf("SearchWithDV failed: %v", err)
		}
		
		if len(results) != 5 {
			t.Errorf("Expected 5 results, got %d", len(results))
		}
	})

	t.Run("WithDeletions", func(t *testing.T) {
		// Mark nodes 0, 1, 2 as deleted
		deletedNodes := map[int]bool{0: true, 1: true, 2: true}
		isDeleted := func(id int) bool { return deletedNodes[id] }
		
		results, err := index.SearchWithDV(query, 5, 100, isDeleted)
		if err != nil {
			t.Fatalf("SearchWithDV failed: %v", err)
		}
		
		if len(results) != 5 {
			t.Errorf("Expected 5 results, got %d", len(results))
		}
		
		// Verify deleted nodes are not in results
		for _, r := range results {
			if deletedNodes[r.ID] {
				t.Errorf("Result contains deleted node %d", r.ID)
			}
		}
	})

	t.Run("AllDeleted", func(t *testing.T) {
		// Mark all nodes as deleted
		isDeleted := func(id int) bool { return true }
		
		results, err := index.SearchWithDV(query, 5, 100, isDeleted)
		if err != nil {
			t.Fatalf("SearchWithDV failed: %v", err)
		}
		
		if len(results) != 0 {
			t.Errorf("Expected 0 results when all deleted, got %d", len(results))
		}
	})

	t.Run("PartialDeletions", func(t *testing.T) {
		// Delete every other node (50% deletion rate).
		// Pass k*2 to compensate — SearchWithDV searches exactly k candidates
		// and post-filters; the caller is responsible for over-fetch.
		isDeleted := func(id int) bool { return id%2 == 0 }

		results, err := index.SearchWithDV(query, 6, 100, isDeleted)
		if err != nil {
			t.Fatalf("SearchWithDV failed: %v", err)
		}
		
		if len(results) != 3 {
			t.Errorf("Expected 3 results, got %d", len(results))
		}
		
		// Verify all results have odd IDs
		for _, r := range results {
			if r.ID%2 == 0 {
				t.Errorf("Result contains even ID %d which should be deleted", r.ID)
			}
		}
	})
}

func TestSearchWithDVNotEnoughCandidates(t *testing.T) {
	// Create small index
	config := Config{
		Dimension:      128,
		M:              16,
		EfConstruction: 200,
		DistanceFunc:   L2Distance,
	}
	index := NewHNSW(config)

	// Add only 3 vectors
	for i := 0; i < 3; i++ {
		vec := make([]float32, 128)
		for j := range vec {
			vec[j] = float32(i)
		}
		_, err := index.Add(vec)
		if err != nil {
			t.Fatalf("Failed to add vector: %v", err)
		}
	}

	query := make([]float32, 128)
	
	// Request 5 results but only 3 exist, 1 is deleted
	isDeleted := func(id int) bool { return id == 0 }
	
	results, err := index.SearchWithDV(query, 5, 100, isDeleted)
	if err != nil {
		t.Fatalf("SearchWithDV failed: %v", err)
	}
	
	// Should return at most 2 (3 total - 1 deleted)
	if len(results) > 2 {
		t.Errorf("Expected at most 2 results (3 total - 1 deleted), got %d", len(results))
	}
	
	// Verify node 0 is not in results
	for _, r := range results {
		if r.ID == 0 {
			t.Error("Result contains deleted node 0")
		}
	}
}

func TestSearchWithDVInvalidDimension(t *testing.T) {
	config := Config{
		Dimension:      128,
		DistanceFunc:   L2Distance,
	}
	index := NewHNSW(config)

	// Wrong dimension
	query := make([]float32, 64)
	isDeleted := func(id int) bool { return false }
	
	_, err := index.SearchWithDV(query, 5, 100, isDeleted)
	if err != ErrDimensionMismatch {
		t.Errorf("Expected ErrDimensionMismatch, got %v", err)
	}
}

func TestSearchWithDVEmptyIndex(t *testing.T) {
	config := Config{
		Dimension:      128,
		DistanceFunc:   L2Distance,
	}
	index := NewHNSW(config)

	query := make([]float32, 128)
	isDeleted := func(id int) bool { return false }
	
	_, err := index.SearchWithDV(query, 5, 100, isDeleted)
	if err != ErrEmptyIndex {
		t.Errorf("Expected ErrEmptyIndex, got %v", err)
	}
}

func BenchmarkSearchWithDV(b *testing.B) {
	config := Config{
		Dimension:      128,
		M:              16,
		EfConstruction: 200,
		DistanceFunc:   L2Distance,
	}
	index := NewHNSW(config)

	// Add 1000 vectors
	for i := 0; i < 1000; i++ {
		vec := make([]float32, 128)
		for j := range vec {
			vec[j] = float32(i % 10)
		}
		index.Add(vec)
	}

	query := make([]float32, 128)
	for j := range query {
		query[j] = 5.0
	}

	// 10% deletion rate
	isDeleted := func(id int) bool { return id%10 == 0 }

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := index.SearchWithDV(query, 10, 100, isDeleted)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSearchWithDVNoFilter(b *testing.B) {
	config := Config{
		Dimension:      128,
		M:              16,
		EfConstruction: 200,
		DistanceFunc:   L2Distance,
	}
	index := NewHNSW(config)

	// Add 1000 vectors
	for i := 0; i < 1000; i++ {
		vec := make([]float32, 128)
		for j := range vec {
			vec[j] = float32(i % 10)
		}
		index.Add(vec)
	}

	query := make([]float32, 128)
	for j := range query {
		query[j] = 5.0
	}

	// No deletions
	isDeleted := func(id int) bool { return false }

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := index.SearchWithDV(query, 10, 100, isDeleted)
		if err != nil {
			b.Fatal(err)
		}
	}
}
