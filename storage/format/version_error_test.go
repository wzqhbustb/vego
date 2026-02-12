// Copyright 2024 Vego Authors
// Licensed under the Apache License, Version 2.0

package format

import (
	"errors"
	"strings"
	"testing"
)

// TestErrorCoverage_VersionError tests all error paths in VersionError
func TestErrorCoverage_VersionError(t *testing.T) {
	t.Run("error_message_format", func(t *testing.T) {
		err := &VersionError{
			Op:            "test_operation",
			FileVersion:   0x0102,
			ReaderVersion: 0x0100,
			Reason:        "file is newer",
			Suggestion:    "Please upgrade to Vego 1.2",
		}
		
		msg := err.Error()
		
		// Verify all components are present in error message
		if !strings.Contains(msg, "test_operation") {
			t.Error("Error message should contain operation name")
		}
		if !strings.Contains(msg, "1.2") { // File version
			t.Error("Error message should contain file version")
		}
		if !strings.Contains(msg, "1.0") { // Reader version
			t.Error("Error message should contain reader version")
		}
		if !strings.Contains(msg, "file is newer") {
			t.Error("Error message should contain reason")
		}
		if !strings.Contains(msg, "Please upgrade") {
			t.Error("Error message should contain suggestion")
		}
	})
	
	t.Run("unwrap_newer_version", func(t *testing.T) {
		err := &VersionError{
			Op:            "read",
			FileVersion:   0x0102,
			ReaderVersion: 0x0100,
			Reason:        "file version is newer than reader",
			Suggestion:    "upgrade",
		}
		
		if !errors.Is(err, ErrVersionTooNew) {
			t.Error("Should unwrap to ErrVersionTooNew")
		}
		if errors.Is(err, ErrVersionTooOld) {
			t.Error("Should not unwrap to ErrVersionTooOld")
		}
	})
	
	t.Run("unwrap_mismatch_version", func(t *testing.T) {
		err := &VersionError{
			Op:            "read",
			FileVersion:   0x0200,
			ReaderVersion: 0x0100,
			Reason:        "major version mismatch",
			Suggestion:    "use correct version",
		}
		
		if !errors.Is(err, ErrVersionTooNew) {
			t.Error("Major mismatch should unwrap to ErrVersionTooNew")
		}
	})
	
	t.Run("unwrap_older_version", func(t *testing.T) {
		err := &VersionError{
			Op:            "read",
			FileVersion:   0x0100,
			ReaderVersion: 0x0102,
			Reason:        "file is older than minimum supported",
			Suggestion:    "migrate file",
		}
		
		if !errors.Is(err, ErrVersionTooOld) {
			t.Error("Should unwrap to ErrVersionTooOld")
		}
	})
	
	t.Run("unwrap_feature_unsupported", func(t *testing.T) {
		err := &VersionError{
			Op:            "use_feature",
			FileVersion:   0x0100,
			ReaderVersion: 0x0100,
			Reason:        "feature not available",
			Suggestion:    "disable feature",
		}
		
		if !errors.Is(err, ErrFeatureNotSupported) {
			t.Error("Should unwrap to ErrFeatureNotSupported for unknown reason")
		}
	})
	
	t.Run("errors_as_extraction", func(t *testing.T) {
		originalErr := &VersionError{
			Op:            "test",
			FileVersion:   0x0102,
			ReaderVersion: 0x0100,
			Reason:        "test reason",
			Suggestion:    "test suggestion",
		}
		
		var err error = originalErr
		
		var extractedErr *VersionError
		if !errors.As(err, &extractedErr) {
			t.Fatal("errors.As should extract VersionError")
		}
		
		if extractedErr.Op != "test" {
			t.Error("Extracted error should preserve Op")
		}
		if extractedErr.FileVersion != 0x0102 {
			t.Error("Extracted error should preserve FileVersion")
		}
		if extractedErr.Suggestion != "test suggestion" {
			t.Error("Extracted error should preserve Suggestion")
		}
	})
}

// TestErrorCoverage_ValidateVersion tests version validation error cases
func TestErrorCoverage_ValidateVersion(t *testing.T) {
	testCases := []struct {
		name        string
		version     uint16
		shouldError bool
		errorType   error
	}{
		{"valid_v1.0", 0x0100, false, nil},
		{"valid_v1.1", 0x0101, false, nil},
		{"valid_v1.2", 0x0102, false, nil},
		{"too_old_zero", 0x0000, true, nil},
		{"too_old_below_min", 0x0001, true, nil},
		{"too_new_future", 0x0200, true, nil},
		{"too_new_far_future", 0xFFFF, true, nil},
	}
	
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateVersion(tc.version)
			
			if tc.shouldError && err == nil {
				t.Errorf("Expected error for version 0x%04X", tc.version)
			}
			if !tc.shouldError && err != nil {
				t.Errorf("Unexpected error for version 0x%04X: %v", tc.version, err)
			}
		})
	}
}

// TestErrorCoverage_ParseVersion tests all parse error paths
func TestErrorCoverage_ParseVersion(t *testing.T) {
	testCases := []struct {
		input       string
		shouldError bool
		description string
	}{
		// Valid cases
		{"1.0", false, "valid v1.0"},
		{"1.1", false, "valid v1.1"},
		{"2.0", false, "valid v2.0"},
		{"10.99", false, "valid large version"},
		
		// Error cases
		{"", true, "empty string"},
		{"1", true, "missing minor version"},
		{"1.", true, "missing minor number"},
		{".1", true, "missing major version"},
		{"1.2.3", true, "too many parts"},
		{"1.2.3.4", true, "way too many parts"},
		{"a.b", true, "non-numeric major"},
		{"1.b", true, "non-numeric minor"},
		{"a.1", true, "non-numeric major with numeric minor"},
		{"1..2", true, "double dot"},
		{"1.2.", true, "trailing dot"},
		{".1.2", true, "leading dot"},
		{"-1.0", true, "negative major"},
		{"1.-1", true, "negative minor"},
		{"999.999", true, "out of range"},
		{"v1.0", true, "with prefix"},
		{"1.0.0", true, "three components"},
		{"  1.0", true, "leading whitespace"},
		{"1.0  ", true, "trailing whitespace"},
		{"1 . 0", true, "spaces around dot"},
	}
	
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			_, err := ParseVersion(tc.input)
			
			if tc.shouldError && err == nil {
				t.Errorf("Expected error for input %q", tc.input)
			}
			if !tc.shouldError && err != nil {
				t.Errorf("Unexpected error for input %q: %v", tc.input, err)
			}
		})
	}
}

// TestErrorCoverage_CheckReadCompatibility tests all compatibility check error paths
func TestErrorCoverage_CheckReadCompatibility(t *testing.T) {
	testCases := []struct {
		name          string
		readerVersion VersionPolicy
		fileVersion   uint16
		shouldError   bool
		expectedErr   error
		checkSuggestion func(string) bool
	}{
		{
			name:          "same_version_ok",
			readerVersion: V1_1,
			fileVersion:   V1_1.Encoded(),
			shouldError:   false,
		},
		{
			name:          "older_file_ok",
			readerVersion: V1_2,
			fileVersion:   V1_0.Encoded(),
			shouldError:   false,
		},
		{
			name:          "newer_minor_version",
			readerVersion: V1_0,
			fileVersion:   V1_1.Encoded(),
			shouldError:   true,
			expectedErr:   ErrVersionTooNew,
			checkSuggestion: func(s string) bool {
				return strings.Contains(s, "upgrade") && strings.Contains(s, "1.1")
			},
		},
		{
			name:          "newer_major_version",
			readerVersion: V1_0,
			fileVersion:   0x0200,
			shouldError:   true,
			expectedErr:   ErrVersionTooNew,
			checkSuggestion: func(s string) bool {
				return strings.Contains(s, "2.x") || strings.Contains(s, "Vego 2")
			},
		},
		{
			name:          "major_version_mismatch_lower",
			readerVersion: VersionPolicy{2, 0, 0},
			fileVersion:   V1_0.Encoded(),
			shouldError:   true,
			expectedErr:   ErrVersionTooNew,
		},
		{
			name:          "far_future_version",
			readerVersion: V1_0,
			fileVersion:   0xFF00, // V255.0
			shouldError:   true,
			expectedErr:   ErrVersionTooNew,
		},
	}
	
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			checker := NewVersionChecker(tc.readerVersion)
			err := checker.CheckReadCompatibility(tc.fileVersion)
			
			if tc.shouldError {
				if err == nil {
					t.Fatal("Expected error but got none")
				}
				
				// Check error type
				if !errors.Is(err, tc.expectedErr) {
					t.Errorf("Expected error type %v, got %v", tc.expectedErr, err)
				}
				
				// Check VersionError structure
				var verr *VersionError
				if !errors.As(err, &verr) {
					t.Fatal("Expected *VersionError")
				}
				
				// Verify error fields
				if verr.Op == "" {
					t.Error("VersionError.Op should not be empty")
				}
				if verr.Reason == "" {
					t.Error("VersionError.Reason should not be empty")
				}
				if verr.Suggestion == "" {
					t.Error("VersionError.Suggestion should not be empty")
				}
				if verr.FileVersion != tc.fileVersion {
					t.Errorf("FileVersion = 0x%04X, want 0x%04X", verr.FileVersion, tc.fileVersion)
				}
				if verr.ReaderVersion != tc.readerVersion.Encoded() {
					t.Errorf("ReaderVersion = 0x%04X, want 0x%04X",
						verr.ReaderVersion, tc.readerVersion.Encoded())
				}
				
				// Check suggestion content if validator provided
				if tc.checkSuggestion != nil && !tc.checkSuggestion(verr.Suggestion) {
					t.Errorf("Suggestion %q did not pass validation", verr.Suggestion)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
			}
		})
	}
}

// TestErrorCoverage_ParseFeatureFlags tests feature flag parsing errors
func TestErrorCoverage_ParseFeatureFlags(t *testing.T) {
	testCases := []struct {
		input       string
		shouldError bool
		description string
	}{
		// Valid cases
		{"0x00000000", false, "zero"},
		{"0x00000001", false, "one"},
		{"0xFFFFFFFF", false, "max uint32"},
		{"0X00000001", false, "uppercase 0X"},
		{"00000001", false, "no prefix"},
		{"1", false, "short hex"},
		{"ABC", false, "hex letters"},
		
		// Error cases
		{"", true, "empty string"},
		{"0x", true, "prefix only"},
		{"0X", true, "uppercase prefix only"},
		{"invalid", true, "non-hex string"},
		{"0xGGGGGGGG", true, "invalid hex chars"},
		{"0x100000000", true, "too large for uint32"},
		{"0x-1", true, "negative hex"},
		{" 0x00000001", true, "leading space"},
		{"0x00000001 ", true, "trailing space"},
		{"0x 00000001", true, "space after prefix"},
	}
	
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			_, err := ParseFeatureFlags(tc.input)
			
			if tc.shouldError && err == nil {
				t.Errorf("Expected error for input %q", tc.input)
			}
			if !tc.shouldError && err != nil {
				t.Errorf("Unexpected error for input %q: %v", tc.input, err)
			}
		})
	}
}

// TestErrorCoverage_FooterMetadataValidation tests footer metadata validation errors
func TestErrorCoverage_FooterMetadataValidation(t *testing.T) {
	t.Run("rowindex_without_feature", func(t *testing.T) {
		footer := NewFooter()
		footer.SetFormatVersion(V1_0) // V1.0 doesn't have RowIndex feature
		footer.SetRowIndexInfo(1000, 2048, 0)
		
		err := footer.ValidateFormatMetadata()
		if err == nil {
			t.Error("Expected error: RowIndex metadata without feature support")
		}
		if !strings.Contains(err.Error(), "RowIndex") {
			t.Error("Error should mention RowIndex")
		}
	})
	
	t.Run("blockcache_without_feature", func(t *testing.T) {
		footer := NewFooter()
		footer.SetFormatVersion(V1_0) // V1.0 doesn't have BlockCache
		footer.SetBlockCacheInfo(4096)
		
		err := footer.ValidateFormatMetadata()
		if err == nil {
			t.Error("Expected error: BlockCache metadata without feature support")
		}
		if !strings.Contains(err.Error(), "BlockCache") {
			t.Error("Error should mention BlockCache")
		}
	})
	
	t.Run("valid_v1.1_with_rowindex", func(t *testing.T) {
		footer := NewFooter()
		footer.SetFormatVersion(V1_1)
		footer.SetRowIndexInfo(1000, 2048, 0)
		
		err := footer.ValidateFormatMetadata()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
	})
	
	t.Run("valid_v1.2_with_all_features", func(t *testing.T) {
		footer := NewFooter()
		footer.SetFormatVersion(V1_2)
		footer.SetRowIndexInfo(1000, 2048, 0)
		footer.SetBlockCacheInfo(4096)
		
		err := footer.ValidateFormatMetadata()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
	})
}

// TestErrorCoverage_EdgeCases tests edge cases that might cause errors
func TestErrorCoverage_EdgeCases(t *testing.T) {
	t.Run("version_encoding_boundary", func(t *testing.T) {
		// Test boundary values
		testCases := []struct {
			major, minor uint8
			expected     uint16
		}{
			{0, 0, 0x0000},
			{0, 255, 0x00FF},
			{255, 0, 0xFF00},
			{255, 255, 0xFFFF},
		}
		
		for _, tc := range testCases {
			vp := VersionPolicy{tc.major, tc.minor, 0}
			encoded := vp.Encoded()
			if encoded != tc.expected {
				t.Errorf("Encoding(%d.%d) = 0x%04X, want 0x%04X",
					tc.major, tc.minor, encoded, tc.expected)
			}
		}
	})
	
	t.Run("feature_flags_all_bits", func(t *testing.T) {
		// Test all bits set
		vp := VersionPolicy{1, 0, 0xFFFFFFFF}
		
		// Should have all features
		if !vp.HasFeature(FeatureBasicColumnar) {
			t.Error("Should have BasicColumnar")
		}
		if !vp.HasFeature(FeatureRowIndex) {
			t.Error("Should have RowIndex")
		}
		if !vp.HasFeature(FeatureEncryption) {
			t.Error("Should have Encryption")
		}
	})
	
	t.Run("empty_version_string", func(t *testing.T) {
		_, err := ParseVersion("")
		if err == nil {
			t.Error("Empty string should cause parse error")
		}
	})
	
	t.Run("nil_checker_operations", func(t *testing.T) {
		// VersionChecker with zero version (edge case)
		checker := NewVersionChecker(VersionPolicy{0, 0, 0})
		
		// Should not panic
		err := checker.CheckReadCompatibility(0x0100)
		if err == nil {
			t.Error("V0.0 reader should reject V1.0 file")
		}
	})
}

// TestErrorCoverage_Comprehensive runs a comprehensive error test suite
func TestErrorCoverage_Comprehensive(t *testing.T) {
	t.Run("all_error_types_tested", func(t *testing.T) {
		// Verify we can generate all defined error types
		
		// ErrVersionTooNew
		checker := NewVersionChecker(V1_0)
		err := checker.CheckReadCompatibility(V1_1.Encoded())
		if !errors.Is(err, ErrVersionTooNew) {
			t.Error("Failed to generate ErrVersionTooNew")
		}
		
		// ErrVersionTooOld (via VersionError.Unwrap)
		oldErr := &VersionError{
			Op:            "test",
			FileVersion:   0x0100,
			ReaderVersion: 0x0102,
			Reason:        "older than supported",
			Suggestion:    "migrate",
		}
		if !errors.Is(oldErr, ErrVersionTooOld) {
			t.Error("Failed to generate ErrVersionTooOld")
		}
		
		// ErrFeatureNotSupported (via VersionError.Unwrap)
		featureErr := &VersionError{
			Op:            "test",
			FileVersion:   0x0100,
			ReaderVersion: 0x0100,
			Reason:        "feature missing",
			Suggestion:    "disable",
		}
		if !errors.Is(featureErr, ErrFeatureNotSupported) {
			t.Error("Failed to generate ErrFeatureNotSupported")
		}
	})
	
	t.Run("error_wrapping_chain", func(t *testing.T) {
		// Test error wrapping through the chain
		checker := NewVersionChecker(V1_0)
		err := checker.CheckReadCompatibility(V1_2.Encoded())
		
		// Should be a VersionError
		var verr *VersionError
		if !errors.As(err, &verr) {
			t.Fatal("Should be *VersionError")
		}
		
		// Should also be ErrVersionTooNew
		if !errors.Is(err, ErrVersionTooNew) {
			t.Error("Should wrap ErrVersionTooNew")
		}
		
		// Error message should be helpful
		msg := err.Error()
		if len(msg) < 20 {
			t.Error("Error message too short, likely missing information")
		}
	})
}

// BenchmarkErrorGeneration benchmarks error generation performance
func BenchmarkErrorGeneration_VersionError(b *testing.B) {
	for i := 0; i < b.N; i++ {
		err := &VersionError{
			Op:            "bench",
			FileVersion:   0x0102,
			ReaderVersion: 0x0100,
			Reason:        "test",
			Suggestion:    "upgrade",
		}
		_ = err.Error()
	}
}

func BenchmarkErrorGeneration_CheckCompatibility(b *testing.B) {
	checker := NewVersionChecker(V1_0)
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		_ = checker.CheckReadCompatibility(V1_2.Encoded())
	}
}
