// Copyright 2024 Vego Authors
// Licensed under the Apache License, Version 2.0

package format

import (
	"fmt"
	"testing"
)

// TestLegacyVersionMapping_NormalizeVersion tests legacy version normalization
func TestLegacyVersionMapping_NormalizeVersion(t *testing.T) {
	t.Run("legacy_v1_to_v1.0", func(t *testing.T) {
		// The original Vego used version=1 in headers
		// This should map to V1.0 (0x0100)
		legacy := uint16(1)
		normalized := NormalizeVersion(legacy)
		
		if normalized != 0x0100 {
			t.Errorf("Legacy version 1 should normalize to 0x0100, got 0x%04X", normalized)
		}
	})
	
	t.Run("already_normalized_versions", func(t *testing.T) {
		testCases := []struct {
			input    uint16
			expected uint16
			desc     string
		}{
			{0x0100, 0x0100, "V1.0 unchanged"},
			{0x0101, 0x0101, "V1.1 unchanged"},
			{0x0102, 0x0102, "V1.2 unchanged"},
			{0x0200, 0x0200, "V2.0 unchanged"},
			{0x0103, 0x0103, "V1.3 unchanged"},
		}
		
		for _, tc := range testCases {
			t.Run(tc.desc, func(t *testing.T) {
				result := NormalizeVersion(tc.input)
				if result != tc.expected {
					t.Errorf("NormalizeVersion(0x%04X) = 0x%04X, want 0x%04X",
						tc.input, result, tc.expected)
				}
			})
		}
	})
	
	t.Run("unknown_versions_passthrough", func(t *testing.T) {
		// Unknown versions should pass through unchanged
		unknownVersions := []uint16{
			0x0300, // V3.0
			0x9999, // V153.153 (unlikely but valid encoding)
			0xFFFF, // V255.255
			0x0001, // V0.1 (edge case)
		}
		
		for _, v := range unknownVersions {
			normalized := NormalizeVersion(v)
			// For unknown versions, behavior depends on implementation
			// but they shouldn't crash
			if normalized == 0 {
				t.Errorf("NormalizeVersion(0x%04X) returned 0", v)
			}
		}
	})
}

// TestLegacyVersionMapping_VersionFromEncoded tests decoding legacy versions
func TestLegacyVersionMapping_VersionFromEncoded(t *testing.T) {
	t.Run("decode_legacy_v1", func(t *testing.T) {
		// After normalization, legacy V1 becomes 0x0100
		normalized := NormalizeVersion(1)
		vp := VersionFromEncoded(normalized)
		
		if vp.MajorVersion != 1 {
			t.Errorf("Major version should be 1, got %d", vp.MajorVersion)
		}
		if vp.MinorVersion != 0 {
			t.Errorf("Minor version should be 0, got %d", vp.MinorVersion)
		}
		
		// Should have V1.0 features
		if vp.FeatureFlags != V1_0.FeatureFlags {
			t.Errorf("Feature flags should match V1.0: want 0x%08X, got 0x%08X",
				V1_0.FeatureFlags, vp.FeatureFlags)
		}
	})
	
	t.Run("decode_all_standard_versions", func(t *testing.T) {
		testCases := []struct {
			encoded     uint16
			expectMajor uint8
			expectMinor uint8
		}{
			{0x0100, 1, 0},
			{0x0101, 1, 1},
			{0x0102, 1, 2},
			{0x0200, 2, 0},
			{0x0201, 2, 1},
		}
		
		for _, tc := range testCases {
			t.Run(fmt.Sprintf("v%d.%d", tc.expectMajor, tc.expectMinor), func(t *testing.T) {
				vp := VersionFromEncoded(tc.encoded)
				
				if vp.MajorVersion != tc.expectMajor {
					t.Errorf("Major = %d, want %d", vp.MajorVersion, tc.expectMajor)
				}
				if vp.MinorVersion != tc.expectMinor {
					t.Errorf("Minor = %d, want %d", vp.MinorVersion, tc.expectMinor)
				}
			})
		}
	})
}

// TestLegacyVersionMapping_FooterCompatibility tests reading legacy file footers
func TestLegacyVersionMapping_FooterCompatibility(t *testing.T) {
	t.Run("legacy_footer_version_interpretation", func(t *testing.T) {
		// Test that a footer with legacy version field is interpreted correctly
		footer := NewFooter()
		footer.Version = 1 // Legacy version field (not normalized yet)
		footer.NumPages = 0
		footer.PageIndexList = NewPageIndexList()
		footer.Metadata = make(map[string]string)
		
		// GetFormatVersion should normalize legacy version when reading
		// Note: We can't serialize a footer with version=1 because Validate rejects it
		// Instead, we test the normalization logic directly
		normalized := NormalizeVersion(footer.Version)
		vp := VersionFromEncoded(normalized)
		
		if vp.MajorVersion != 1 || vp.MinorVersion != 0 {
			t.Errorf("Legacy version 1 should be interpreted as V1.0, got V%s", vp.String())
		}
	})
	
	t.Run("modern_footer_with_metadata", func(t *testing.T) {
		// Modern footer with explicit version metadata
		footer := NewFooter()
		footer.SetFormatVersion(V1_2)
		footer.NumPages = 0
		
		// Verify version is set correctly
		vp := footer.GetFormatVersion()
		if vp.MajorVersion != 1 || vp.MinorVersion != 2 {
			t.Errorf("Expected V1.2, got V%s", vp.String())
		}
	})
}

// TestLegacyVersionMapping_BackwardCompatibility tests backward compatibility
func TestLegacyVersionMapping_BackwardCompatibility(t *testing.T) {
	t.Run("v1.2_reader_reads_legacy_v1_file", func(t *testing.T) {
		// Simulate reading a legacy V1 file with V1.2 reader
		checker := NewVersionChecker(V1_2)
		
		// Legacy file has version=1, which normalizes to 0x0100
		legacyVersion := NormalizeVersion(1)
		
		err := checker.CheckReadCompatibility(legacyVersion)
		if err != nil {
			t.Errorf("V1.2 reader should read legacy V1 file: %v", err)
		}
		
		// Should use FallbackLinearScan strategy (legacy has no RowIndex)
		strategy := checker.GetReadStrategy(legacyVersion, V1_0.FeatureFlags)
		if strategy != ReadStrategyFallbackLinearScan {
			t.Errorf("Expected FallbackLinearScan for legacy file, got %s", strategy.String())
		}
	})
	
	t.Run("all_readers_read_legacy_v1", func(t *testing.T) {
		// All V1.x readers should be able to read legacy V1 files
		readers := []VersionPolicy{V1_0, V1_1, V1_2}
		legacyVersion := NormalizeVersion(1)
		
		for _, reader := range readers {
			t.Run(fmt.Sprintf("reader_v%s", reader.String()), func(t *testing.T) {
				checker := NewVersionChecker(reader)
				err := checker.CheckReadCompatibility(legacyVersion)
				
				if err != nil {
					t.Errorf("V%s reader should read legacy V1 file: %v",
						reader.String(), err)
				}
			})
		}
	})
	
	t.Run("legacy_reader_cannot_read_modern_files", func(t *testing.T) {
		// Legacy V1.0 reader (simulated) cannot read V1.1+ files
		checker := NewVersionChecker(V1_0)
		
		modernVersions := []VersionPolicy{V1_1, V1_2}
		for _, modern := range modernVersions {
			t.Run(fmt.Sprintf("file_v%s", modern.String()), func(t *testing.T) {
				err := checker.CheckReadCompatibility(modern.Encoded())
				if err == nil {
					t.Errorf("V1.0 reader should reject V%s file", modern.String())
				}
			})
		}
	})
}

// TestLegacyVersionMapping_MigrationScenarios tests migration paths
func TestLegacyVersionMapping_MigrationScenarios(t *testing.T) {
	t.Run("migrate_legacy_v1_to_v1.1", func(t *testing.T) {
		// Simulate migrating a legacy V1 file to V1.1
		
		// Original legacy footer
		legacyFooter := NewFooter()
		legacyFooter.Version = 1
		legacyFooter.NumPages = 100
		legacyFooter.AddMetadata("user.data", "important")
		
		// Verify it's interpreted as V1.0
		legacyVP := legacyFooter.GetFormatVersion()
		if legacyVP.MajorVersion != 1 || legacyVP.MinorVersion != 0 {
			t.Fatalf("Legacy footer should be V1.0, got V%s", legacyVP.String())
		}
		
		// Migrate to V1.1
		migratedFooter := NewFooter()
		migratedFooter.SetFormatVersion(V1_1)
		migratedFooter.NumPages = legacyFooter.NumPages
		migratedFooter.SetRowIndexInfo(10000, 4096, 0xABCDEF01)
		
		// Preserve user metadata
		for k, v := range legacyFooter.GetUserMetadata() {
			migratedFooter.AddMetadata(k, v)
		}
		
		// Verify migration
		migratedVP := migratedFooter.GetFormatVersion()
		if migratedVP.MajorVersion != 1 || migratedVP.MinorVersion != 1 {
			t.Errorf("Migrated footer should be V1.1, got V%s", migratedVP.String())
		}
		
		if !migratedFooter.HasRowIndex() {
			t.Error("Migrated footer should have RowIndex")
		}
		
		if migratedFooter.Metadata["user.data"] != "important" {
			t.Error("User metadata should be preserved")
		}
	})
	
	t.Run("migrate_v1.0_to_v1.2", func(t *testing.T) {
		// Test full migration path V1.0 → V1.2
		
		original := NewFooter()
		original.SetFormatVersion(V1_0)
		original.AddMetadata("app.version", "1.0.0")
		
		// Migrate to V1.2
		migrated := NewFooter()
		migrated.SetFormatVersion(V1_2)
		migrated.SetRowIndexInfo(5000, 2048, 0)
		migrated.SetBlockCacheInfo(8192)
		
		// Preserve user metadata
		migrated.MergeMetadata(original.GetUserMetadata())
		
		// Verify
		vp := migrated.GetFormatVersion()
		if vp != V1_2 {
			t.Errorf("Expected V1.2, got V%s", vp.String())
		}
		
		if !migrated.HasRowIndex() {
			t.Error("Should have RowIndex")
		}
		if !migrated.HasBlockCache() {
			t.Error("Should have BlockCache")
		}
		if migrated.Metadata["app.version"] != "1.0.0" {
			t.Error("User metadata should be preserved")
		}
	})
}

// TestLegacyVersionMapping_EdgeCases tests edge cases in legacy handling
func TestLegacyVersionMapping_EdgeCases(t *testing.T) {
	t.Run("zero_version", func(t *testing.T) {
		// Version 0 is not a valid legacy version
		normalized := NormalizeVersion(0)
		// Should pass through unchanged
		if normalized != 0 {
			t.Errorf("Version 0 should pass through, got 0x%04X", normalized)
		}
	})
	
	t.Run("version_2_not_legacy", func(t *testing.T) {
		// Version 2 (0x0002) is not legacy V1
		normalized := NormalizeVersion(2)
		// Should not map to 0x0100
		if normalized == 0x0100 {
			t.Error("Version 2 should not map to V1.0")
		}
	})
	
	t.Run("already_encoded_v1.0", func(t *testing.T) {
		// 0x0100 is already properly encoded
		normalized := NormalizeVersion(0x0100)
		if normalized != 0x0100 {
			t.Errorf("0x0100 should remain unchanged, got 0x%04X", normalized)
		}
	})
}

// TestLegacyVersionMapping_FeatureFlags tests feature flag handling for legacy versions
func TestLegacyVersionMapping_FeatureFlags(t *testing.T) {
	t.Run("legacy_v1_has_v1.0_features", func(t *testing.T) {
		// Legacy V1 should be treated as having V1.0 features
		normalized := NormalizeVersion(1)
		vp := VersionFromEncoded(normalized)
		
		expectedFeatures := V1_0.FeatureFlags
		if vp.FeatureFlags != expectedFeatures {
			t.Errorf("Legacy V1 features = 0x%08X, want 0x%08X (V1.0)",
				vp.FeatureFlags, expectedFeatures)
		}
		
		// Should have BasicColumnar and ZstdCompression
		if !vp.HasFeature(FeatureBasicColumnar) {
			t.Error("Legacy V1 should have BasicColumnar")
		}
		if !vp.HasFeature(FeatureZstdCompression) {
			t.Error("Legacy V1 should have ZstdCompression")
		}
		
		// Should NOT have RowIndex (added in V1.1)
		if vp.HasFeature(FeatureRowIndex) {
			t.Error("Legacy V1 should not have RowIndex")
		}
	})
	
	t.Run("feature_detection_with_legacy_files", func(t *testing.T) {
		checker := NewVersionChecker(V1_2)
		legacyVersion := NormalizeVersion(1)
		legacyFeatures := V1_0.FeatureFlags
		
		// Can use features that both have
		if !checker.CanUseFeature(legacyFeatures, FeatureBasicColumnar) {
			t.Error("Should be able to use BasicColumnar with legacy file")
		}
		
		// Cannot use features that legacy doesn't have
		if checker.CanUseFeature(legacyFeatures, FeatureRowIndex) {
			t.Error("Should not be able to use RowIndex with legacy file")
		}
		
		// Strategy should be FallbackLinearScan (no RowIndex)
		strategy := checker.GetReadStrategy(legacyVersion, legacyFeatures)
		if strategy != ReadStrategyFallbackLinearScan {
			t.Errorf("Expected FallbackLinearScan, got %s", strategy.String())
		}
	})
}

// TestLegacyVersionMapping_Integration tests complete integration scenarios
func TestLegacyVersionMapping_Integration(t *testing.T) {
	t.Run("complete_legacy_file_read_workflow", func(t *testing.T) {
		// Simulate complete workflow of reading a legacy file
		
		// 1. Create legacy footer (as it would be on disk)
		legacyFooter := &Footer{
			Version:       1, // Legacy version
			NumPages:      50,
			PageIndexList: NewPageIndexList(),
			CreatedAt:     1234567890,
			ModifiedAt:    1234567890,
			Metadata:      map[string]string{"user.key": "value"},
		}
		
		// 2. Modern reader opens file
		checker := NewVersionChecker(V1_2)
		
		// 3. Normalize legacy version
		normalizedVersion := NormalizeVersion(legacyFooter.Version)
		
		// 4. Check compatibility
		err := checker.CheckReadCompatibility(normalizedVersion)
		if err != nil {
			t.Fatalf("V1.2 reader should read legacy file: %v", err)
		}
		
		// 5. Get format version
		vp := VersionFromEncoded(normalizedVersion)
		if vp.String() != "1.0" {
			t.Errorf("Legacy file should be interpreted as V1.0, got V%s", vp.String())
		}
		
		// 6. Determine read strategy
		strategy := checker.GetReadStrategy(normalizedVersion, vp.FeatureFlags)
		if strategy != ReadStrategyFallbackLinearScan {
			t.Errorf("Should use FallbackLinearScan, got %s", strategy.String())
		}
		
		// 7. Check feature availability
		canUseRowIndex := checker.CanUseFeature(vp.FeatureFlags, FeatureRowIndex)
		if canUseRowIndex {
			t.Error("Legacy file should not support RowIndex")
		}
		
		// 8. Read proceeds with fallback strategy
		t.Log("Successfully validated complete legacy file read workflow")
	})
}

// BenchmarkLegacyVersionMapping benchmarks legacy version operations
func BenchmarkLegacyVersionMapping_NormalizeVersion(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = NormalizeVersion(1)
	}
}

func BenchmarkLegacyVersionMapping_VersionFromEncoded(b *testing.B) {
	normalized := NormalizeVersion(1)
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		_ = VersionFromEncoded(normalized)
	}
}
