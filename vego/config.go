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
}

// DefaultConfig returns default configuration
func DefaultConfig() *Config {
	return &Config{
		Dimension:        128,
		M:                16,
		EfConstruction:   200,
		DistanceFunc:     hnsw.L2Distance,
		Adaptive:         true,
		ExpectedSize:     10000,
		CompressionLevel: 3,
		PageSize:         1024 * 1024,
		AutoSaveInterval: 0,
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
