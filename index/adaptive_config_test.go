package hnsw

import (
	"fmt"
	"testing"
)

func TestAdaptiveConfig100K(t *testing.T) {
	config := Config{
		Dimension:      128,
		Adaptive:       true,
		ExpectedSize:   100000,
		M:              0, // Let adaptive calculate
		EfConstruction: 0, // Let adaptive calculate
		Seed:           42,
	}

	index := NewHNSW(config)

	fmt.Printf("Adaptive Config for 100K D128:\n")
	fmt.Printf("  M: %d (expected: 16)\n", index.M)
	fmt.Printf("  EfConstruction: %d (expected: ~520)\n", index.efConstruction)

	if index.efConstruction < 500 {
		t.Errorf("Expected EfConstruction >= 500, got %d", index.efConstruction)
	}
}
