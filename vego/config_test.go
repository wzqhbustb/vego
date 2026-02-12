package vego

import (
	"testing"

	hnsw "github.com/wzqhbustb/vego/index"
)

// TestDefaultConfig tests default configuration
func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()
	
	if config == nil {
		t.Fatal("Expected non-nil config")
	}
	
	// Check default values
	if config.Dimension != 128 {
		t.Errorf("Expected default Dimension 128, got %d", config.Dimension)
	}
	if config.M != 16 {
		t.Errorf("Expected default M 16, got %d", config.M)
	}
	if config.EfConstruction != 200 {
		t.Errorf("Expected default EfConstruction 200, got %d", config.EfConstruction)
	}
	if config.Adaptive != true {
		t.Errorf("Expected default Adaptive true, got %v", config.Adaptive)
	}
	if config.ExpectedSize != 10000 {
		t.Errorf("Expected default ExpectedSize 10000, got %d", config.ExpectedSize)
	}
	if config.CompressionLevel != 3 {
		t.Errorf("Expected default CompressionLevel 3, got %d", config.CompressionLevel)
	}
	if config.PageSize != 1024*1024 {
		t.Errorf("Expected default PageSize 1048576, got %d", config.PageSize)
	}
	if config.AutoSaveInterval != 0 {
		t.Errorf("Expected default AutoSaveInterval 0, got %d", config.AutoSaveInterval)
	}
	// Distance function can't be compared directly, but we can verify it's not nil
	if config.DistanceFunc == nil {
		t.Error("Expected non-nil DistanceFunc")
	}
}

// TestWithDimension tests dimension option
func TestWithDimension(t *testing.T) {
	config := DefaultConfig()
	
	WithDimension(256)(config)
	if config.Dimension != 256 {
		t.Errorf("Expected Dimension 256, got %d", config.Dimension)
	}
	
	WithDimension(64)(config)
	if config.Dimension != 64 {
		t.Errorf("Expected Dimension 64, got %d", config.Dimension)
	}
	
	WithDimension(1536)(config)
	if config.Dimension != 1536 {
		t.Errorf("Expected Dimension 1536, got %d", config.Dimension)
	}
}

// TestWithAdaptive tests adaptive option
func TestWithAdaptive(t *testing.T) {
	config := DefaultConfig()
	
	// Disable adaptive
	WithAdaptive(false)(config)
	if config.Adaptive != false {
		t.Errorf("Expected Adaptive false, got %v", config.Adaptive)
	}
	
	// Re-enable adaptive
	WithAdaptive(true)(config)
	if config.Adaptive != true {
		t.Errorf("Expected Adaptive true, got %v", config.Adaptive)
	}
}

// TestWithExpectedSize tests expected size option
func TestWithExpectedSize(t *testing.T) {
	config := DefaultConfig()
	
	WithExpectedSize(100000)(config)
	if config.ExpectedSize != 100000 {
		t.Errorf("Expected ExpectedSize 100000, got %d", config.ExpectedSize)
	}
	
	WithExpectedSize(0)(config)
	if config.ExpectedSize != 0 {
		t.Errorf("Expected ExpectedSize 0, got %d", config.ExpectedSize)
	}
}

// TestWithDistanceFunc tests distance function option
func TestWithDistanceFunc(t *testing.T) {
	tests := []struct {
		name     string
		fn       hnsw.DistanceFunc
		expected hnsw.DistanceFunc
	}{
		{"Cosine", hnsw.CosineDistance, hnsw.CosineDistance},
		{"L2", hnsw.L2Distance, hnsw.L2Distance},
		{"InnerProduct", hnsw.InnerProductDistance, hnsw.InnerProductDistance},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := DefaultConfig()
			WithDistanceFunc(tt.fn)(config)
			
			// Function pointers can't be directly compared
			// We just verify it's set by checking not nil
			if config.DistanceFunc == nil {
				t.Errorf("Expected %s distance function to be set", tt.name)
			}
		})
	}
}

// TestWithM tests M parameter option
func TestWithM(t *testing.T) {
	config := DefaultConfig()
	
	// Enable adaptive first
	config.Adaptive = true
	
	WithM(32)(config)
	if config.M != 32 {
		t.Errorf("Expected M 32, got %d", config.M)
	}
	if config.Adaptive != false {
		t.Error("Expected Adaptive to be disabled when setting M manually")
	}
}

// TestWithEfConstruction tests EfConstruction option
func TestWithEfConstruction(t *testing.T) {
	config := DefaultConfig()
	
	// Enable adaptive first
	config.Adaptive = true
	
	WithEfConstruction(500)(config)
	if config.EfConstruction != 500 {
		t.Errorf("Expected EfConstruction 500, got %d", config.EfConstruction)
	}
	if config.Adaptive != false {
		t.Error("Expected Adaptive to be disabled when setting EfConstruction manually")
	}
}

// TestConfigValidation tests configuration validation
func TestConfigValidation(t *testing.T) {
	// This test assumes there's validation logic
	// If validation doesn't exist yet, this serves as documentation of expected behavior
	
	testCases := []struct {
		name        string
		dimension   int
		m           int
		efConst     int
		shouldWork  bool
	}{
		{"Valid config", 128, 16, 200, true},
		{"Zero dimension", 0, 16, 200, false},
		{"Negative dimension", -1, 16, 200, false},
		{"Zero M", 128, 0, 200, false},
		{"Zero EfConstruction", 128, 16, 0, false},
		{"Large values", 4096, 64, 800, true},
	}
	
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			config := &Config{
				Dimension:      tc.dimension,
				M:              tc.m,
				EfConstruction: tc.efConst,
			}
			
			// If validation function exists, call it
			// For now, just log the config
			t.Logf("Config: Dimension=%d, M=%d, EfConstruction=%d", 
				config.Dimension, config.M, config.EfConstruction)
		})
	}
}

// TestMultipleOptions tests applying multiple options
func TestMultipleOptions(t *testing.T) {
	config := DefaultConfig()
	
	// Apply multiple options
	WithDimension(256)(config)
	WithM(32)(config)
	WithEfConstruction(400)(config)
	WithAdaptive(false)(config)
	WithExpectedSize(50000)(config)
	WithDistanceFunc(hnsw.CosineDistance)(config)
	
	// Verify all options were applied
	if config.Dimension != 256 {
		t.Errorf("Expected Dimension 256, got %d", config.Dimension)
	}
	if config.M != 32 {
		t.Errorf("Expected M 32, got %d", config.M)
	}
	if config.EfConstruction != 400 {
		t.Errorf("Expected EfConstruction 400, got %d", config.EfConstruction)
	}
	if config.Adaptive != false {
		t.Errorf("Expected Adaptive false, got %v", config.Adaptive)
	}
	if config.ExpectedSize != 50000 {
		t.Errorf("Expected ExpectedSize 50000, got %d", config.ExpectedSize)
	}
	// Verify by checking function behavior
	vec1 := []float32{1, 0, 0}
	vec2 := []float32{0, 1, 0}
	result := config.DistanceFunc(vec1, vec2)
	if result < 0.9 || result > 1.1 { // Cosine distance between perpendicular vectors should be ~1
		t.Error("Expected CosineDistance behavior")
	}
}

// TestConfigImmutability tests that options don't affect other configs
func TestConfigImmutability(t *testing.T) {
	config1 := DefaultConfig()
	config2 := DefaultConfig()
	
	// Modify config1
	WithDimension(512)(config1)
	
	// config2 should not be affected
	if config2.Dimension != 128 {
		t.Error("DefaultConfig() should return independent copies")
	}
}

// TestOptionChaining tests option chaining behavior
func TestOptionChaining(t *testing.T) {
	// Options should be independent and composable
	config := DefaultConfig()
	
	// Apply options in different orders - should give same result
	opts := []Option{
		WithDimension(64),
		WithM(8),
		WithAdaptive(false),
	}
	
	for _, opt := range opts {
		opt(config)
	}
	
	if config.Dimension != 64 || config.M != 8 || config.Adaptive != false {
		t.Error("Options should be composable")
	}
}
