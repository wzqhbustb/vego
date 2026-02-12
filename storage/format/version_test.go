// Copyright 2024 Vego Authors
// Licensed under the Apache License, Version 2.0

package format

import (
	"errors"
	"fmt"
	"testing"
)

func TestFeatureConstants(t *testing.T) {
	// Test that feature flags are powers of 2
	features := []uint32{
		FeatureBasicColumnar,
		FeatureZstdCompression,
		FeatureDictionaryEncoding,
		FeatureRLE,
		FeatureBitPacking,
		FeatureRowIndex,
		FeatureBlockCache,
		FeatureAsyncIO,
		FeatureFullZip,
		FeatureChecksum,
		FeatureEncryption,
	}

	for i, f := range features {
		// Check it's a power of 2 (only one bit set)
		if f == 0 {
			t.Errorf("Feature at index %d is zero", i)
			continue
		}
		if f&(f-1) != 0 {
			t.Errorf("Feature at index %d (%d) is not a power of 2", i, f)
		}
	}

	// Test FeatureFlagsToStrings
	flags := FeatureBasicColumnar | FeatureRowIndex | FeatureBlockCache
	strs := FeaturesToStrings(flags)
	if len(strs) != 3 {
		t.Errorf("Expected 3 features, got %d: %v", len(strs), strs)
	}
}

func TestVersionEncoding(t *testing.T) {
	tests := []struct {
		vp       VersionPolicy
		expected uint16
	}{
		{V1_0, 0x0100},
		{V1_1, 0x0101},
		{V1_2, 0x0102},
		{VersionPolicy{2, 0, 0}, 0x0200},
		{VersionPolicy{1, 255, 0}, 0x01FF},
	}

	for _, tt := range tests {
		t.Run(tt.vp.String(), func(t *testing.T) {
			encoded := tt.vp.Encoded()
			if encoded != tt.expected {
				t.Errorf("Encoded() = 0x%04X, want 0x%04X", encoded, tt.expected)
			}

			// Test roundtrip
			decoded := VersionFromEncoded(encoded)
			if decoded.MajorVersion != tt.vp.MajorVersion {
				t.Errorf("MajorVersion = %d, want %d", decoded.MajorVersion, tt.vp.MajorVersion)
			}
			if decoded.MinorVersion != tt.vp.MinorVersion {
				t.Errorf("MinorVersion = %d, want %d", decoded.MinorVersion, tt.vp.MinorVersion)
			}
		})
	}
}

func TestVersionString(t *testing.T) {
	tests := []struct {
		vp       VersionPolicy
		expected string
	}{
		{V1_0, "1.0"},
		{V1_1, "1.1"},
		{V1_2, "1.2"},
		{VersionPolicy{2, 5, 0}, "2.5"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			s := tt.vp.String()
			if s != tt.expected {
				t.Errorf("String() = %q, want %q", s, tt.expected)
			}
		})
	}
}

func TestParseVersion(t *testing.T) {
	tests := []struct {
		input       string
		wantMajor   uint8
		wantMinor   uint8
		wantFlags   uint32
		wantErr     bool
	}{
		{"1.0", 1, 0, V1_0.FeatureFlags, false},
		{"1.1", 1, 1, V1_1.FeatureFlags, false},
		{"1.2", 1, 2, V1_2.FeatureFlags, false},
		{"2.0", 2, 0, 0, false},           // Unknown version, no flags
		{"0.1", 0, 1, 0, false},           // Edge case
		{"invalid", 0, 0, 0, true},        // Invalid format
		{"1", 0, 0, 0, true},              // Missing minor
		{"1.2.3", 0, 0, 0, true},          // Too many parts
		{"a.b", 0, 0, 0, true},            // Non-numeric
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			vp, err := ParseVersion(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ParseVersion(%q) expected error, got nil", tt.input)
				}
				return
			}
			if err != nil {
				t.Errorf("ParseVersion(%q) unexpected error: %v", tt.input, err)
				return
			}
			if vp.MajorVersion != tt.wantMajor {
				t.Errorf("MajorVersion = %d, want %d", vp.MajorVersion, tt.wantMajor)
			}
			if vp.MinorVersion != tt.wantMinor {
				t.Errorf("MinorVersion = %d, want %d", vp.MinorVersion, tt.wantMinor)
			}
			if vp.FeatureFlags != tt.wantFlags {
				t.Errorf("FeatureFlags = 0x%08X, want 0x%08X", vp.FeatureFlags, tt.wantFlags)
			}
		})
	}
}

func TestVersionCompatibility(t *testing.T) {
	// Test CanRead: newer can read older
	tests := []struct {
		reader   VersionPolicy
		file     VersionPolicy
		canRead  bool
	}{
		{V1_0, V1_0, true},   // Same version
		{V1_1, V1_0, true},   // V1.1 can read V1.0
		{V1_2, V1_0, true},   // V1.2 can read V1.0
		{V1_2, V1_1, true},   // V1.2 can read V1.1
		{V1_0, V1_1, false},  // V1.0 cannot read V1.1
		{V1_1, V1_2, false},  // V1.1 cannot read V1.2
		{V1_0, V1_2, false},  // V1.0 cannot read V1.2
		// Major version mismatch
		{VersionPolicy{2, 0, 0}, VersionPolicy{1, 0, 0}, false},
		{VersionPolicy{1, 0, 0}, VersionPolicy{2, 0, 0}, false},
	}

	for _, tt := range tests {
		name := tt.reader.String() + "_read_" + tt.file.String()
		t.Run(name, func(t *testing.T) {
			result := tt.reader.CanRead(tt.file)
			if result != tt.canRead {
				t.Errorf("CanRead() = %v, want %v", result, tt.canRead)
			}

			// Test CanBeReadBy is the inverse
			inverse := tt.file.CanBeReadBy(tt.reader)
			if inverse != tt.canRead {
				t.Errorf("CanBeReadBy() = %v, want %v", inverse, tt.canRead)
			}
		})
	}
}

func TestNormalizeVersion(t *testing.T) {
	tests := []struct {
		input    uint16
		expected uint16
	}{
		{1, 0x0100},       // Legacy V1 -> V1.0
		{0x0100, 0x0100},  // V1.0 unchanged
		{0x0101, 0x0101},  // V1.1 unchanged
		{0x0102, 0x0102},  // V1.2 unchanged
		{0x0200, 0x0200},  // V2.0 unchanged (unknown but pass through)
		{0x9999, 0x9999},  // Unknown unchanged
		{0, 0},            // Zero unchanged
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("0x%04X", tt.input), func(t *testing.T) {
			result := NormalizeVersion(tt.input)
			if result != tt.expected {
				t.Errorf("NormalizeVersion(0x%04X) = 0x%04X, want 0x%04X",
					tt.input, result, tt.expected)
			}
		})
	}
}

func TestVersionChecker(t *testing.T) {
	checker := NewVersionChecker(V1_1)

	// Test ReaderVersion
	if checker.ReaderVersion() != V1_1 {
		t.Errorf("ReaderVersion() = %v, want %v", checker.ReaderVersion(), V1_1)
	}

	// Test CheckReadCompatibility
	compatibilityTests := []struct {
		fileVersion uint16
		wantErr     bool
		errType     error
	}{
		{V1_0.Encoded(), false, nil},        // V1.0 OK
		{V1_1.Encoded(), false, nil},        // V1.1 OK
		{V1_2.Encoded(), true, ErrVersionTooNew}, // V1.2 too new
		{0x0200, true, ErrVersionTooNew},    // V2.0 major mismatch
	}

	for _, tt := range compatibilityTests {
		t.Run(fmt.Sprintf("compat_0x%04X", tt.fileVersion), func(t *testing.T) {
			err := checker.CheckReadCompatibility(tt.fileVersion)
			if tt.wantErr {
				if err == nil {
					t.Errorf("CheckReadCompatibility(0x%04X) expected error", tt.fileVersion)
					return
				}
				var verr *VersionError
				if !errors.As(err, &verr) {
					t.Errorf("Expected *VersionError, got %T", err)
				}
				if !errors.Is(err, tt.errType) {
					t.Errorf("Expected error to be %v, got %v", tt.errType, err)
				}
				return
			}
			if err != nil {
				t.Errorf("CheckReadCompatibility(0x%04X) unexpected error: %v", tt.fileVersion, err)
			}
		})
	}
}

func TestVersionCheckerCanUseFeature(t *testing.T) {
	checker := NewVersionChecker(V1_1) // Has RowIndex but not BlockCache

	tests := []struct {
		fileFeatures uint32
		feature      uint32
		want         bool
	}{
		// File and reader both have RowIndex
		{V1_1.FeatureFlags, FeatureRowIndex, true},
		// File has BlockCache but reader doesn't
		{V1_2.FeatureFlags, FeatureBlockCache, false},
		// File doesn't have RowIndex
		{V1_0.FeatureFlags, FeatureRowIndex, false},
		// Both have basic columnar
		{V1_0.FeatureFlags, FeatureBasicColumnar, true},
	}

	for _, tt := range tests {
		t.Run(FeatureFlagsToHex(tt.feature), func(t *testing.T) {
			result := checker.CanUseFeature(tt.fileFeatures, tt.feature)
			if result != tt.want {
				t.Errorf("CanUseFeature(0x%08X, 0x%08X) = %v, want %v",
					tt.fileFeatures, tt.feature, result, tt.want)
			}
		})
	}
}

func TestVersionCheckerGetReadStrategy(t *testing.T) {
	checker := NewVersionChecker(V1_2)

	tests := []struct {
		fileVersion  uint16
		fileFeatures uint32
		want         ReadStrategy
	}{
		// Same version
		{V1_2.Encoded(), V1_2.FeatureFlags, ReadStrategyNormal},
		// Older without RowIndex -> fallback
		{V1_0.Encoded(), V1_0.FeatureFlags, ReadStrategyFallbackLinearScan},
		// Older with RowIndex -> compatible
		{V1_1.Encoded(), V1_1.FeatureFlags, ReadStrategyCompatible},
		// Newer (should not happen in practice if CheckReadCompatibility is called first)
		{V1_2.Encoded() + 1, 0, ReadStrategyUnsupported},
	}

	for _, tt := range tests {
		name := fmt.Sprintf("v%04X_f%08X", tt.fileVersion, tt.fileFeatures)
		t.Run(name, func(t *testing.T) {
			result := checker.GetReadStrategy(tt.fileVersion, tt.fileFeatures)
			if result != tt.want {
				t.Errorf("GetReadStrategy(0x%04X, 0x%08X) = %v, want %v",
					tt.fileVersion, tt.fileFeatures, result, tt.want)
			}
		})
	}
}

func TestReadStrategyString(t *testing.T) {
	tests := []struct {
		strategy ReadStrategy
		expected string
	}{
		{ReadStrategyNormal, "normal"},
		{ReadStrategyCompatible, "compatible"},
		{ReadStrategyFallbackLinearScan, "fallback_linear_scan"},
		{ReadStrategyUnsupported, "unsupported"},
		{ReadStrategy(999), "unsupported"}, // Unknown value
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := tt.strategy.String()
			if result != tt.expected {
				t.Errorf("String() = %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestVersionError(t *testing.T) {
	err := &VersionError{
		Op:            "test_op",
		FileVersion:   0x0102,
		ReaderVersion: 0x0100,
		Reason:        "file version is newer than reader",
		Suggestion:    "Please upgrade",
	}

	// Test Error() message format
	msg := err.Error()
	if msg == "" {
		t.Error("Error() returned empty string")
	}

	// Test Unwrap
	if !errors.Is(err, ErrVersionTooNew) {
		t.Errorf("Expected error to wrap ErrVersionTooNew")
	}

	// Test old version error
	oldErr := &VersionError{
		Op:            "test",
		FileVersion:   0x0100,
		ReaderVersion: 0x0102,
		Reason:        "file version is older than supported",
		Suggestion:    "Please migrate",
	}
	if !errors.Is(oldErr, ErrVersionTooOld) {
		t.Errorf("Expected error to wrap ErrVersionTooOld")
	}
}

func TestVersionPolicyHasFeature(t *testing.T) {
	vp := VersionPolicy{
		MajorVersion: 1,
		MinorVersion: 1,
		FeatureFlags: FeatureBasicColumnar | FeatureRowIndex,
	}

	if !vp.HasFeature(FeatureBasicColumnar) {
		t.Error("Expected HasFeature(BasicColumnar) = true")
	}
	if !vp.HasFeature(FeatureRowIndex) {
		t.Error("Expected HasFeature(RowIndex) = true")
	}
	if vp.HasFeature(FeatureBlockCache) {
		t.Error("Expected HasFeature(BlockCache) = false")
	}
}

func TestFeatureFlagsHexConversion(t *testing.T) {
	tests := []struct {
		flags    uint32
		expected string
	}{
		{0, "0x00000000"},
		{FeatureBasicColumnar, "0x00000001"},
		{FeatureRowIndex, "0x00000020"},
		{0xFFFFFFFF, "0xFFFFFFFF"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			hex := FeatureFlagsToHex(tt.flags)
			if hex != tt.expected {
				t.Errorf("FeatureFlagsToHex(0x%08X) = %q, want %q", tt.flags, hex, tt.expected)
			}

			// Test roundtrip
			parsed, err := ParseFeatureFlags(hex)
			if err != nil {
				t.Errorf("ParseFeatureFlags(%q) error: %v", hex, err)
				return
			}
			if parsed != tt.flags {
				t.Errorf("ParseFeatureFlags(%q) = 0x%08X, want 0x%08X", hex, parsed, tt.flags)
			}
		})
	}

	// Test ParseFeatureFlags with different formats
	parseTests := []struct {
		input    string
		expected uint32
		wantErr  bool
	}{
		{"0x00000001", 1, false},
		{"0X00000001", 1, false}, // Uppercase 0X
		{"00000001", 1, false},   // No prefix
		{"abc", 0xABC, false},    // Short hex
		{"invalid", 0, true},     // Invalid
	}

	for _, tt := range parseTests {
		t.Run(tt.input, func(t *testing.T) {
			result, err := ParseFeatureFlags(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ParseFeatureFlags(%q) expected error", tt.input)
				}
				return
			}
			if err != nil {
				t.Errorf("ParseFeatureFlags(%q) unexpected error: %v", tt.input, err)
				return
			}
			if result != tt.expected {
				t.Errorf("ParseFeatureFlags(%q) = 0x%08X, want 0x%08X", tt.input, result, tt.expected)
			}
		})
	}
}

// BenchmarkVersionEncoding benchmarks version encoding/decoding
func BenchmarkVersionEncoding(b *testing.B) {
	for i := 0; i < b.N; i++ {
		encoded := V1_1.Encoded()
		_ = VersionFromEncoded(encoded)
	}
}

// BenchmarkVersionChecker benchmarks compatibility checking
func BenchmarkVersionChecker(b *testing.B) {
	checker := NewVersionChecker(V1_2)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = checker.CheckReadCompatibility(V1_0.Encoded())
	}
}
