// Copyright 2024 Vego Authors
// Licensed under the Apache License, Version 2.0

package format

import (
	"errors"
	"fmt"
	"testing"
)

// ExampleVersionChecker demonstrates basic version checking
func ExampleVersionChecker() {
	// Create a checker for V1.1 reader
	checker := NewVersionChecker(V1_1)

	// Check if we can read a V1.0 file
	if err := checker.CheckReadCompatibility(V1_0.Encoded()); err != nil {
		fmt.Println("Cannot read V1.0:", err)
	} else {
		fmt.Println("Can read V1.0 file")
	}

	// Check if we can read a V1.2 file
	if err := checker.CheckReadCompatibility(V1_2.Encoded()); err != nil {
		// Extract detailed error information
		var verr *VersionError
		if errors.As(err, &verr) {
			fmt.Printf("Cannot read V1.2: %s\n", verr.Suggestion)
		}
	}

	// Output:
	// Can read V1.0 file
	// Cannot read V1.2: Please upgrade to Vego 1.2 or later
}

// ExampleVersionChecker_GetReadStrategy demonstrates read strategy selection
func ExampleVersionChecker_GetReadStrategy() {
	checker := NewVersionChecker(V1_2)

	// Check strategy for different file versions
	strategies := []struct {
		version  VersionPolicy
		features uint32
	}{
		{V1_2, V1_2.FeatureFlags}, // Same version
		{V1_1, V1_1.FeatureFlags}, // Older with RowIndex
		{V1_0, V1_0.FeatureFlags}, // Older without RowIndex
	}

	for _, s := range strategies {
		strategy := checker.GetReadStrategy(s.version.Encoded(), s.features)
		fmt.Printf("V%s file: %s strategy\n", s.version.String(), strategy.String())
	}

	// Output:
	// V1.2 file: normal strategy
	// V1.1 file: compatible strategy
	// V1.0 file: fallback_linear_scan strategy
}

// ExampleVersionChecker_CanUseFeature demonstrates feature detection
func ExampleVersionChecker_CanUseFeature() {
	// V1.1 reader has RowIndex but not BlockCache
	checker := NewVersionChecker(V1_1)

	// Check features against a V1.2 file
	v12Features := V1_2.FeatureFlags // Has both RowIndex and BlockCache

	fmt.Println("V1.1 reader with V1.2 file:")
	fmt.Printf("  Can use RowIndex: %v\n", checker.CanUseFeature(v12Features, FeatureRowIndex))
	fmt.Printf("  Can use BlockCache: %v\n", checker.CanUseFeature(v12Features, FeatureBlockCache))

	// Check features against a V1.0 file
	v10Features := V1_0.FeatureFlags // Has neither
	fmt.Println("V1.1 reader with V1.0 file:")
	fmt.Printf("  Can use RowIndex: %v\n", checker.CanUseFeature(v10Features, FeatureRowIndex))

	// Output:
	// V1.1 reader with V1.2 file:
	//   Can use RowIndex: true
	//   Can use BlockCache: false
	// V1.1 reader with V1.0 file:
	//   Can use RowIndex: false
}

// ExampleVersionError demonstrates error handling with suggestions
func ExampleVersionError() {
	checker := NewVersionChecker(V1_0)

	// Try to read a newer file
	err := checker.CheckReadCompatibility(V1_2.Encoded())
	if err != nil {
		// Check specific error types
		if errors.Is(err, ErrVersionTooNew) {
			fmt.Println("Error: File is too new for this reader")
		}

		// Get detailed information
		var verr *VersionError
		if errors.As(err, &verr) {
			fmt.Printf("File version: %d.%d\n", verr.FileVersion>>8, verr.FileVersion&0xFF)
			fmt.Printf("Reader version: %d.%d\n", verr.ReaderVersion>>8, verr.ReaderVersion&0xFF)
			fmt.Printf("Suggestion: %s\n", verr.Suggestion)
		}
	}

	// Output:
	// Error: File is too new for this reader
	// File version: 1.2
	// Reader version: 1.0
	// Suggestion: Please upgrade to Vego 1.2 or later
}

// Example_normalizeVersion demonstrates version normalization for legacy files
func Example_normalizeVersion() {
	// Legacy files may have version = 1
	legacyVersions := []uint16{1, 0x0100, 0x0101, 0x0102}

	for _, v := range legacyVersions {
		normalized := NormalizeVersion(v)
		vp := VersionFromEncoded(normalized)
		fmt.Printf("Input %d → Normalized 0x%04X → Version %s\n", v, normalized, vp.String())
	}

	// Output:
	// Input 1 → Normalized 0x0100 → Version 1.0
	// Input 256 → Normalized 0x0100 → Version 1.0
	// Input 257 → Normalized 0x0101 → Version 1.1
	// Input 258 → Normalized 0x0102 → Version 1.2
}

// TestIntegration_VersionWithFooter tests version checking integrated with Footer
func TestIntegration_VersionWithFooter(t *testing.T) {
	// Create a V1.1 file with RowIndex
	footer := NewFooter()
	footer.SetFormatVersion(V1_1)
	footer.SetRowIndexInfo(12345, 4096, 0xDEADBEEF)

	// Get format metadata from footer
	fm := footer.GetFormatMetadata()

	// Create version checker for V1.2 reader
	checker := NewVersionChecker(V1_2)

	// Check compatibility
	if err := checker.CheckReadCompatibility(fm.Version.Encoded()); err != nil {
		t.Fatalf("V1.2 reader should be able to read V1.1 file: %v", err)
	}

	// Determine read strategy
	strategy := checker.GetReadStrategy(fm.Version.Encoded(), fm.Features)
	if strategy != ReadStrategyCompatible {
		t.Errorf("Expected Compatible strategy, got %s", strategy.String())
	}

	// Check if we can use RowIndex
	if !checker.CanUseFeature(fm.Features, FeatureRowIndex) {
		t.Error("V1.2 reader should be able to use RowIndex from V1.1 file")
	}

	// Check that we cannot use BlockCache (file doesn't have it)
	if checker.CanUseFeature(fm.Features, FeatureBlockCache) {
		t.Error("V1.1 file should not support BlockCache")
	}
}

// TestIntegration_MigrationScenario tests a migration scenario
func TestIntegration_MigrationScenario(t *testing.T) {
	// Scenario: User has V1.0 file, wants to upgrade to V1.1

	// Simulate old V1.0 file
	oldFooter := NewFooter()
	oldFooter.SetFormatVersion(V1_0)
	oldFooter.AddMetadata("user.data", "important")

	// User tries to open with V1.1 reader (should work with fallback)
	checker := NewVersionChecker(V1_1)
	oldVersion := oldFooter.GetFormatVersion()

	if err := checker.CheckReadCompatibility(oldVersion.Encoded()); err != nil {
		t.Fatalf("V1.1 reader should be able to read V1.0 file: %v", err)
	}

	strategy := checker.GetReadStrategy(oldVersion.Encoded(), oldVersion.FeatureFlags)
	if strategy != ReadStrategyFallbackLinearScan {
		t.Errorf("Expected FallbackLinearScan for V1.0 file, got %s", strategy.String())
	}

	// Now migrate to V1.1
	newFooter := NewFooter()
	newFooter.SetFormatVersion(V1_1)
	newFooter.SetRowIndexInfo(1000, 2048, 0)
	
	// Copy user metadata
	for k, v := range oldFooter.GetUserMetadata() {
		newFooter.AddMetadata(k, v)
	}

	// Verify migrated file
	if !newFooter.HasRowIndex() {
		t.Error("Migrated file should have RowIndex")
	}
	if newFooter.Metadata["user.data"] != "important" {
		t.Error("User metadata should be preserved after migration")
	}

	// V1.1 reader can now use normal strategy
	newVersion := newFooter.GetFormatVersion()
	newStrategy := checker.GetReadStrategy(newVersion.Encoded(), newVersion.FeatureFlags)
	if newStrategy != ReadStrategyNormal {
		t.Errorf("Expected Normal strategy after migration, got %s", newStrategy.String())
	}
}

// TestIntegration_ErrorMessages tests that error messages are helpful
func TestIntegration_ErrorMessages(t *testing.T) {
	tests := []struct {
		name           string
		readerVersion  VersionPolicy
		fileVersion    VersionPolicy
		wantErrType    error
		wantSuggestion string
	}{
		{
			name:           "V1.0 reader with V1.2 file",
			readerVersion:  V1_0,
			fileVersion:    V1_2,
			wantErrType:    ErrVersionTooNew,
			wantSuggestion: "Please upgrade to Vego 1.2 or later",
		},
		{
			name:           "V1.1 reader with V2.0 file",
			readerVersion:  V1_1,
			fileVersion:    VersionPolicy{2, 0, 0},
			wantErrType:    ErrVersionTooNew,
			wantSuggestion: "Please use Vego 2.x to read this file",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			checker := NewVersionChecker(tt.readerVersion)
			err := checker.CheckReadCompatibility(tt.fileVersion.Encoded())

			if err == nil {
				t.Fatal("Expected error")
			}

			if !errors.Is(err, tt.wantErrType) {
				t.Errorf("Expected error to be %v", tt.wantErrType)
			}

			var verr *VersionError
			if !errors.As(err, &verr) {
				t.Fatal("Expected *VersionError")
			}

			if verr.Suggestion == "" {
				t.Error("Expected non-empty Suggestion")
			}
		})
	}
}
