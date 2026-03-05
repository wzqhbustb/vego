package vego

import (
	hnsw "github.com/wzqhbustb/vego/index"
)

// Config holds database configuration
type Config struct {
	// Vector configuration
	Dimension      int
	M              int
	EfConstruction int
	DistanceFunc   hnsw.DistanceFunc
	Adaptive       bool
	ExpectedSize   int

	// Storage configuration
	CompressionLevel int // 1-9 for ZSTD
	PageSize         int // Default 1MB

	// Auto-save configuration
	AutoSaveInterval int // Seconds, 0 = disabled

	// Compaction configuration
	AutoCompact        bool    // Enable automatic compaction
	CompactThreshold   float64 // Deletion rate threshold (0.0-1.0), default 0.3
	CompactMinInterval int     // Minimum seconds between compactions, default 300 (5 min)
	CompactMaxInterval int     // Maximum seconds between compactions, default 604800 (7 days), 0=disable
}

// DefaultConfig returns default configuration
func DefaultConfig() *Config {
	return &Config{
		Dimension:          128,
		M:                  16,
		EfConstruction:     200,
		DistanceFunc:       hnsw.L2Distance,
		Adaptive:           true,
		ExpectedSize:       10000,
		CompressionLevel:   3,
		PageSize:           1024 * 1024,
		AutoSaveInterval:   0,
		AutoCompact:        true,   // Enabled by default
		CompactThreshold:   0.30,   // 30% deletion rate
		CompactMinInterval: 300,    // 5 minutes
		CompactMaxInterval: 604800, // 7 days
	}
}

// Option is a functional option for configuration
type Option func(*Config)

// WithDimension sets the vector dimension
func WithDimension(d int) Option {
	return func(c *Config) {
		c.Dimension = d
	}
}

// WithAdaptive enables adaptive configuration
func WithAdaptive(enabled bool) Option {
	return func(c *Config) {
		c.Adaptive = enabled
	}
}

// WithDistanceFunc sets the distance function
func WithDistanceFunc(fn hnsw.DistanceFunc) Option {
	return func(c *Config) {
		c.DistanceFunc = fn
	}
}

// WithExpectedSize sets the expected dataset size for adaptive configuration
func WithExpectedSize(size int) Option {
	return func(c *Config) {
		c.ExpectedSize = size
	}
}

// WithM sets the HNSW M parameter (max connections per layer)
func WithM(m int) Option {
	return func(c *Config) {
		c.M = m
		c.Adaptive = false // Disable adaptive when manually set
	}
}

// WithEfConstruction sets the HNSW EfConstruction parameter
func WithEfConstruction(ef int) Option {
	return func(c *Config) {
		c.EfConstruction = ef
		c.Adaptive = false // Disable adaptive when manually set
	}
}

// WithAutoCompact enables or disables automatic compaction
func WithAutoCompact(enabled bool) Option {
	return func(c *Config) {
		c.AutoCompact = enabled
	}
}

// WithCompactThreshold sets the deletion rate threshold for automatic compaction
// Value should be between 0.0 and 1.0 (e.g., 0.3 means 30% deletion rate triggers compaction)
func WithCompactThreshold(threshold float64) Option {
	return func(c *Config) {
		if threshold < 0.0 {
			threshold = 0.0
		}
		if threshold > 1.0 {
			threshold = 1.0
		}
		c.CompactThreshold = threshold
	}
}

// WithCompactMinInterval sets the minimum interval between automatic compactions (in seconds)
func WithCompactMinInterval(seconds int) Option {
	return func(c *Config) {
		if seconds < 0 {
			seconds = 0
		}
		c.CompactMinInterval = seconds
	}
}

// WithCompactMaxInterval sets the maximum interval between automatic compactions (in seconds)
// When this interval is reached, compaction will be forced regardless of deletion rate.
// Set to 0 to disable max interval check (not recommended for production).
func WithCompactMaxInterval(seconds int) Option {
	return func(c *Config) {
		if seconds < 0 {
			seconds = 0
		}
		c.CompactMaxInterval = seconds
	}
}
