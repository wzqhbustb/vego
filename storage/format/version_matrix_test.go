// Copyright 2024 Vego Authors
// Licensed under the Apache License, Version 2.0

package format

import (
	"errors"
	"fmt"
	"testing"
)

// TestCompatibilityMatrix_Comprehensive tests all reader/file version combinations
// This provides a comprehensive compatibility matrix validation
func TestCompatibilityMatrix_Comprehensive(t *testing.T) {
	versions := []VersionPolicy{V1_0, V1_1, V1_2}
	
	// Define expected compatibility matrix
	// matrix[readerIdx][fileIdx] = expected compatibility
	expectedMatrix := [][]bool{
		// Reader V1.0
		{true, false, false}, // V1.0 can read: V1.0 only
		// Reader V1.1
		{true, true, false},  // V1.1 can read: V1.0, V1.1
		// Reader V1.2
		{true, true, true},   // V1.2 can read: V1.0, V1.1, V1.2
	}
	
	for readerIdx, reader := range versions {
		for fileIdx, file := range versions {
			name := fmt.Sprintf("reader_v%s_file_v%s", reader.String(), file.String())
			t.Run(name, func(t *testing.T) {
				checker := NewVersionChecker(reader)
				err := checker.CheckReadCompatibility(file.Encoded())
				
				expected := expectedMatrix[readerIdx][fileIdx]
				got := (err == nil)
				
				if got != expected {
					if expected {
						t.Errorf("Expected reader V%s to read file V%s, but got error: %v",
							reader.String(), file.String(), err)
					} else {
						t.Errorf("Expected reader V%s to reject file V%s, but succeeded",
							reader.String(), file.String())
					}
				}
				
				// Verify error type for incompatible cases
				if !expected && err != nil {
					if !errors.Is(err, ErrVersionTooNew) {
						t.Errorf("Expected ErrVersionTooNew for incompatible versions, got: %T", err)
					}
					
					var verr *VersionError
					if !errors.As(err, &verr) {
						t.Errorf("Expected *VersionError, got: %T", err)
					} else if verr.Suggestion == "" {
						t.Error("VersionError should have non-empty Suggestion")
					}
				}
			})
		}
	}
}

// TestCompatibilityMatrix_ReadStrategy tests read strategy for all combinations
func TestCompatibilityMatrix_ReadStrategy(t *testing.T) {
	testCases := []struct {
		reader       VersionPolicy
		file         VersionPolicy
		fileFeatures uint32
		expected     ReadStrategy
	}{
		// V1.0 reader
		{V1_0, V1_0, V1_0.FeatureFlags, ReadStrategyNormal},
		
		// V1.1 reader
		{V1_1, V1_0, V1_0.FeatureFlags, ReadStrategyFallbackLinearScan}, // No RowIndex
		{V1_1, V1_1, V1_1.FeatureFlags, ReadStrategyNormal},
		
		// V1.2 reader
		{V1_2, V1_0, V1_0.FeatureFlags, ReadStrategyFallbackLinearScan}, // No RowIndex
		{V1_2, V1_1, V1_1.FeatureFlags, ReadStrategyCompatible},         // Has RowIndex, no BlockCache
		{V1_2, V1_2, V1_2.FeatureFlags, ReadStrategyNormal},
		
		// Edge case: file version newer than reader (should be caught earlier)
		{V1_0, V1_1, V1_1.FeatureFlags, ReadStrategyUnsupported},
		{V1_1, V1_2, V1_2.FeatureFlags, ReadStrategyUnsupported},
	}
	
	for _, tc := range testCases {
		name := fmt.Sprintf("reader_v%s_file_v%s_strategy_%s",
			tc.reader.String(), tc.file.String(), tc.expected.String())
		t.Run(name, func(t *testing.T) {
			checker := NewVersionChecker(tc.reader)
			strategy := checker.GetReadStrategy(tc.file.Encoded(), tc.fileFeatures)
			
			if strategy != tc.expected {
				t.Errorf("GetReadStrategy() = %s, want %s", strategy.String(), tc.expected.String())
			}
		})
	}
}

// TestCompatibilityMatrix_FeatureDetection tests feature availability across versions
func TestCompatibilityMatrix_FeatureDetection(t *testing.T) {
	type featureTest struct {
		reader         VersionPolicy
		file           VersionPolicy
		feature        uint32
		featureName    string
		shouldBeUsable bool
	}
	
	tests := []featureTest{
		// Basic columnar - available in all versions
		{V1_0, V1_0, FeatureBasicColumnar, "BasicColumnar", true},
		{V1_1, V1_0, FeatureBasicColumnar, "BasicColumnar", true},
		{V1_2, V1_0, FeatureBasicColumnar, "BasicColumnar", true},
		
		// RowIndex - available from V1.1+
		{V1_0, V1_0, FeatureRowIndex, "RowIndex", false}, // Neither has it
		{V1_1, V1_0, FeatureRowIndex, "RowIndex", false}, // File doesn't have it
		{V1_1, V1_1, FeatureRowIndex, "RowIndex", true},  // Both have it
		{V1_2, V1_1, FeatureRowIndex, "RowIndex", true},  // Both have it
		{V1_2, V1_2, FeatureRowIndex, "RowIndex", true},  // Both have it
		
		// BlockCache - available from V1.2+
		{V1_0, V1_0, FeatureBlockCache, "BlockCache", false}, // Neither has it
		{V1_1, V1_1, FeatureBlockCache, "BlockCache", false}, // Neither has it
		{V1_2, V1_1, FeatureBlockCache, "BlockCache", false}, // File doesn't have it
		{V1_2, V1_2, FeatureBlockCache, "BlockCache", true},  // Both have it
		
		// Zstd compression - available in all versions
		{V1_0, V1_0, FeatureZstdCompression, "ZstdCompression", true},
		{V1_1, V1_1, FeatureZstdCompression, "ZstdCompression", true},
		{V1_2, V1_2, FeatureZstdCompression, "ZstdCompression", true},
	}
	
	for _, tt := range tests {
		name := fmt.Sprintf("reader_v%s_file_v%s_%s",
			tt.reader.String(), tt.file.String(), tt.featureName)
		t.Run(name, func(t *testing.T) {
			checker := NewVersionChecker(tt.reader)
			usable := checker.CanUseFeature(tt.file.FeatureFlags, tt.feature)
			
			if usable != tt.shouldBeUsable {
				t.Errorf("CanUseFeature(%s) = %v, want %v",
					tt.featureName, usable, tt.shouldBeUsable)
			}
		})
	}
}

// TestCompatibilityMatrix_MajorVersionMismatch tests major version incompatibility
func TestCompatibilityMatrix_MajorVersionMismatch(t *testing.T) {
	testCases := []struct {
		readerMajor uint8
		fileMajor   uint8
		shouldFail  bool
	}{
		{1, 1, false}, // Same major version
		{1, 2, true},  // V1 reader cannot read V2 file
		{2, 1, true},  // V2 reader cannot read V1 file (different format)
		{2, 2, false}, // Same major version
		{1, 3, true},  // V1 reader cannot read V3 file
	}
	
	for _, tc := range testCases {
		name := fmt.Sprintf("reader_v%d_file_v%d", tc.readerMajor, tc.fileMajor)
		t.Run(name, func(t *testing.T) {
			reader := VersionPolicy{tc.readerMajor, 0, 0}
			file := VersionPolicy{tc.fileMajor, 0, 0}
			
			checker := NewVersionChecker(reader)
			err := checker.CheckReadCompatibility(file.Encoded())
			
			if tc.shouldFail {
				if err == nil {
					t.Errorf("Expected error for major version mismatch")
				}
				if !errors.Is(err, ErrVersionTooNew) {
					t.Errorf("Expected ErrVersionTooNew, got: %v", err)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
			}
		})
	}
}

// TestCompatibilityMatrix_EdgeCases tests edge cases in compatibility checking
func TestCompatibilityMatrix_EdgeCases(t *testing.T) {
	t.Run("same_version_different_features", func(t *testing.T) {
		// Reader and file have same version but different feature flags
		// (shouldn't happen in practice, but test resilience)
		reader := VersionPolicy{1, 1, FeatureBasicColumnar}
		checker := NewVersionChecker(reader)
		
		// File has extra features
		fileFeatures := FeatureBasicColumnar | FeatureRowIndex
		strategy := checker.GetReadStrategy(reader.Encoded(), fileFeatures)
		
		// Should use Normal strategy (same version)
		if strategy != ReadStrategyNormal {
			t.Errorf("Expected Normal strategy for same version, got %s", strategy.String())
		}
		
		// But CanUseFeature should return false for features reader doesn't have
		if checker.CanUseFeature(fileFeatures, FeatureRowIndex) {
			t.Error("Reader should not be able to use feature it doesn't support")
		}
	})
	
	t.Run("zero_version", func(t *testing.T) {
		checker := NewVersionChecker(V1_0)
		
		// Version 0.0 (invalid but test handling)
		err := checker.CheckReadCompatibility(0x0000)
		
		// Should reject major version mismatch
		if err == nil {
			t.Error("Expected error for version 0.0")
		}
	})
	
	t.Run("max_version", func(t *testing.T) {
		checker := NewVersionChecker(V1_0)
		
		// Max version 255.255
		err := checker.CheckReadCompatibility(0xFFFF)
		
		// Should reject (too new)
		if err == nil {
			t.Error("Expected error for version 255.255")
		}
	})
}

// TestCompatibilityMatrix_WithFooter tests compatibility checking integrated with Footer
func TestCompatibilityMatrix_WithFooter(t *testing.T) {
	testCases := []struct {
		name         string
		footerSetup  func() *Footer
		readerVer    VersionPolicy
		canRead      bool
		hasRowIndex  bool
		hasBlockCache bool
	}{
		{
			name: "V1.0_file_V1.0_reader",
			footerSetup: func() *Footer {
				f := NewFooter()
				f.SetFormatVersion(V1_0)
				return f
			},
			readerVer:    V1_0,
			canRead:      true,
			hasRowIndex:  false,
			hasBlockCache: false,
		},
		{
			name: "V1.1_file_V1.2_reader",
			footerSetup: func() *Footer {
				f := NewFooter()
				f.SetFormatVersion(V1_1)
				f.SetRowIndexInfo(1000, 2048, 0xDEADBEEF)
				return f
			},
			readerVer:    V1_2,
			canRead:      true,
			hasRowIndex:  true,
			hasBlockCache: false,
		},
		{
			name: "V1.2_file_V1.0_reader",
			footerSetup: func() *Footer {
				f := NewFooter()
				f.SetFormatVersion(V1_2)
				f.SetRowIndexInfo(1000, 2048, 0)
				f.SetBlockCacheInfo(4096)
				return f
			},
			readerVer:    V1_0,
			canRead:      false,
			hasRowIndex:  true,
			hasBlockCache: true,
		},
	}
	
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			footer := tc.footerSetup()
			fm := footer.GetFormatMetadata()
			
			checker := NewVersionChecker(tc.readerVer)
			err := checker.CheckReadCompatibility(fm.Version.Encoded())
			
			if tc.canRead && err != nil {
				t.Errorf("Expected reader V%s to read file V%s, got error: %v",
					tc.readerVer.String(), fm.Version.String(), err)
			}
			if !tc.canRead && err == nil {
				t.Errorf("Expected reader V%s to reject file V%s",
					tc.readerVer.String(), fm.Version.String())
			}
			
			// Verify metadata
			if fm.HasRowIndex != tc.hasRowIndex {
				t.Errorf("HasRowIndex = %v, want %v", fm.HasRowIndex, tc.hasRowIndex)
			}
			if fm.HasBlockCache != tc.hasBlockCache {
				t.Errorf("HasBlockCache = %v, want %v", fm.HasBlockCache, tc.hasBlockCache)
			}
		})
	}
}
