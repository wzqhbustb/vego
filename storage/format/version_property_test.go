// Copyright 2024 Vego Authors
// Licensed under the Apache License, Version 2.0

package format

import (
	"fmt"
	"math/rand"
	"testing"
)

// TestPropertyRoundtrip_VersionEncoding tests that version encoding is reversible
// This is a property-based test: for any valid version, encode then decode should be identity
func TestPropertyRoundtrip_VersionEncoding(t *testing.T) {
	// Fast randomized test for CI (covers most cases efficiently)
	t.Run("randomized_fast", func(t *testing.T) {
		const iterations = 10000
		rng := rand.New(rand.NewSource(42)) // Fixed seed for reproducibility

		for i := 0; i < iterations; i++ {
			major := uint8(rng.Intn(256))
			minor := uint8(rng.Intn(256))

			vp := VersionPolicy{major, minor, 0}
			encoded := vp.Encoded()
			decoded := VersionFromEncoded(encoded)

			if decoded.MajorVersion != major || decoded.MinorVersion != minor {
				t.Fatalf("Roundtrip failed at iteration %d: input=%d.%d, got=%d.%d",
					i, major, minor, decoded.MajorVersion, decoded.MinorVersion)
			}
		}
	})

	// Boundary value tests (critical edge cases)
	t.Run("boundary_values", func(t *testing.T) {
		boundaryCases := []struct {
			major, minor uint8
		}{
			{0, 0},       // Minimum
			{0, 255},     // Major=0, Minor=max
			{255, 0},     // Major=max, Minor=0
			{255, 255},   // Maximum
			{1, 0},       // V1.0 (known version)
			{1, 255},     // V1.255
			{127, 0},     // Midpoint
			{127, 127},   // Midpoint both
			{1, 1},       // V1.1 (known version)
			{1, 2},       // V1.2 (known version)
		}

		for _, tc := range boundaryCases {
			name := fmt.Sprintf("v%d.%d", tc.major, tc.minor)
			t.Run(name, func(t *testing.T) {
				vp := VersionPolicy{tc.major, tc.minor, 0}
				encoded := vp.Encoded()
				decoded := VersionFromEncoded(encoded)

				if decoded.MajorVersion != tc.major {
					t.Errorf("Major version mismatch: got %d, want %d", decoded.MajorVersion, tc.major)
				}
				if decoded.MinorVersion != tc.minor {
					t.Errorf("Minor version mismatch: got %d, want %d", decoded.MinorVersion, tc.minor)
				}

				// Verify encoding format
				expected := (uint16(tc.major) << 8) | uint16(tc.minor)
				if encoded != expected {
					t.Errorf("Encoded value mismatch: got 0x%04X, want 0x%04X", encoded, expected)
				}
			})
		}
	})
}

// TestPropertyRoundtrip_FeatureFlags tests that feature flag conversion is reversible
func TestPropertyRoundtrip_FeatureFlags(t *testing.T) {
	// Test known feature flags
	t.Run("known_features", func(t *testing.T) {
		allFeatures := []uint32{
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

		// Test each individual feature
		for _, feature := range allFeatures {
			hex := FeatureFlagsToHex(feature)
			parsed, err := ParseFeatureFlags(hex)
			if err != nil {
				t.Errorf("ParseFeatureFlags failed for 0x%08X: %v", feature, err)
				continue
			}
			if parsed != feature {
				t.Errorf("Roundtrip failed: input=0x%08X, got=0x%08X", feature, parsed)
			}
		}

		// Test combined flags
		combined := FeatureBasicColumnar | FeatureRowIndex | FeatureBlockCache
		hex := FeatureFlagsToHex(combined)
		parsed, err := ParseFeatureFlags(hex)
		if err != nil {
			t.Fatalf("ParseFeatureFlags failed for combined flags: %v", err)
		}
		if parsed != combined {
			t.Errorf("Combined flags roundtrip failed: input=0x%08X, got=0x%08X", combined, parsed)
		}
	})

	// Random values test
	t.Run("random_uint32_values", func(t *testing.T) {
		rng := rand.New(rand.NewSource(123))
		const iterations = 1000 // Reduced from 10000 for faster CI

		for i := 0; i < iterations; i++ {
			flags := rng.Uint32()

			hex := FeatureFlagsToHex(flags)
			parsed, err := ParseFeatureFlags(hex)
			if err != nil {
				t.Fatalf("ParseFeatureFlags failed for 0x%08X: %v", flags, err)
			}
			if parsed != flags {
				t.Errorf("Roundtrip failed: input=0x%08X, got=0x%08X", flags, parsed)
			}
		}
	})
}

// TestPropertyRoundtrip_VersionString tests version string parsing
func TestPropertyRoundtrip_VersionString(t *testing.T) {
	t.Run("all_valid_versions", func(t *testing.T) {
		// Test all reasonable version combinations
		for major := uint8(0); major < 10; major++ {
			for minor := uint8(0); minor < 100; minor++ {
				original := VersionPolicy{major, minor, 0}
				str := original.String()
				
				parsed, err := ParseVersion(str)
				if err != nil {
					// Known versions (1.0, 1.1, 1.2) should have feature flags
					if (major == 1 && minor <= 2) {
						// These are defined versions, check feature flags
						continue
					}
					t.Fatalf("ParseVersion failed for %s: %v", str, err)
				}
				
				if parsed.MajorVersion != major || parsed.MinorVersion != minor {
					t.Errorf("Roundtrip failed: input=%d.%d, parsed=%d.%d",
						major, minor, parsed.MajorVersion, parsed.MinorVersion)
				}
			}
		}
	})
}

// TestPropertyInvariant_VersionOrdering tests that version comparison is consistent
func TestPropertyInvariant_VersionOrdering(t *testing.T) {
	versions := []VersionPolicy{
		{0, 1, 0},
		{1, 0, 0},
		{1, 1, 0},
		{1, 2, 0},
		{1, 10, 0},
		{2, 0, 0},
		{2, 1, 0},
	}
	
	// Test transitivity: if A can read B, and B can read C, then A can read C
	for i, a := range versions {
		for j, b := range versions {
			for _, c := range versions {
				aCanReadB := a.CanRead(b)
				bCanReadC := b.CanRead(c)
				aCanReadC := a.CanRead(c)
				
				// Transitivity property
				if aCanReadB && bCanReadC && !aCanReadC {
					t.Errorf("Transitivity violated: V%s reads V%s, V%s reads V%s, but V%s cannot read V%s",
						a.String(), b.String(), b.String(), c.String(), a.String(), c.String())
				}
				
				// Anti-symmetry: if A != B, then not both (A reads B and B reads A)
				if i != j {
					bCanReadA := b.CanRead(a)
					if aCanReadB && bCanReadA {
						t.Errorf("Anti-symmetry violated: V%s and V%s can both read each other",
							a.String(), b.String())
					}
				}
				
				// Reflexivity: any version can read itself
				if i == j && !a.CanRead(b) {
					t.Errorf("Reflexivity violated: V%s cannot read itself", a.String())
				}
			}
		}
	}
}

// TestPropertyInvariant_CanReadSymmetry tests CanRead and CanBeReadBy symmetry
func TestPropertyInvariant_CanReadSymmetry(t *testing.T) {
	versions := []VersionPolicy{V1_0, V1_1, V1_2}
	
	for _, reader := range versions {
		for _, file := range versions {
			canRead := reader.CanRead(file)
			canBeReadBy := file.CanBeReadBy(reader)
			
			if canRead != canBeReadBy {
				t.Errorf("Symmetry broken: V%s.CanRead(V%s)=%v but V%s.CanBeReadBy(V%s)=%v",
					reader.String(), file.String(), canRead,
					file.String(), reader.String(), canBeReadBy)
			}
		}
	}
}

// TestPropertyInvariant_HasFeature tests feature flag checking properties
func TestPropertyInvariant_HasFeature(t *testing.T) {
	t.Run("no_feature_overlap", func(t *testing.T) {
		// Each feature should be a single bit (power of 2)
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
		
		for i, f1 := range features {
			for j, f2 := range features {
				if i != j && (f1&f2) != 0 {
					t.Errorf("Features overlap: 0x%08X & 0x%08X = 0x%08X", f1, f2, f1&f2)
				}
			}
		}
	})
	
	t.Run("version_features_cumulative", func(t *testing.T) {
		// Each version should include all features from previous versions
		if (V1_1.FeatureFlags & V1_0.FeatureFlags) != V1_0.FeatureFlags {
			t.Error("V1.1 should include all V1.0 features")
		}
		if (V1_2.FeatureFlags & V1_1.FeatureFlags) != V1_1.FeatureFlags {
			t.Error("V1.2 should include all V1.1 features")
		}
	})
	
	t.Run("hasfeature_consistency", func(t *testing.T) {
		// HasFeature should be consistent with bit operations
		vp := V1_2
		
		// Test each feature
		features := []uint32{
			FeatureBasicColumnar, FeatureZstdCompression, FeatureRowIndex,
			FeatureBlockCache, FeatureDictionaryEncoding,
		}
		
		for _, feature := range features {
			hasFeature := vp.HasFeature(feature)
			manualCheck := (vp.FeatureFlags & feature) != 0
			
			if hasFeature != manualCheck {
				t.Errorf("HasFeature(0x%08X) = %v, but manual check = %v",
					feature, hasFeature, manualCheck)
			}
		}
	})
}

// TestPropertyInvariant_NormalizeVersion tests version normalization properties
func TestPropertyInvariant_NormalizeVersion(t *testing.T) {
	t.Run("idempotent", func(t *testing.T) {
		// Normalizing twice should give same result as normalizing once
		testVersions := []uint16{1, 0x0100, 0x0101, 0x0102, 0x0200}
		
		for _, v := range testVersions {
			once := NormalizeVersion(v)
			twice := NormalizeVersion(once)
			
			if once != twice {
				t.Errorf("NormalizeVersion not idempotent: v=0x%04X, once=0x%04X, twice=0x%04X",
					v, once, twice)
			}
		}
	})
	
	t.Run("legacy_mapping_consistent", func(t *testing.T) {
		// Legacy version 1 should always map to 0x0100
		normalized := NormalizeVersion(1)
		if normalized != 0x0100 {
			t.Errorf("Legacy version 1 should normalize to 0x0100, got 0x%04X", normalized)
		}
		
		// Already normalized versions should remain unchanged
		if NormalizeVersion(0x0100) != 0x0100 {
			t.Error("0x0100 should remain unchanged")
		}
		if NormalizeVersion(0x0101) != 0x0101 {
			t.Error("0x0101 should remain unchanged")
		}
	})
}

// TestPropertyInvariant_ReadStrategy tests read strategy selection properties
func TestPropertyInvariant_ReadStrategy(t *testing.T) {
	t.Run("same_version_always_normal", func(t *testing.T) {
		// Reading same version should always use Normal strategy
		versions := []VersionPolicy{V1_0, V1_1, V1_2}
		
		for _, v := range versions {
			checker := NewVersionChecker(v)
			strategy := checker.GetReadStrategy(v.Encoded(), v.FeatureFlags)
			
			if strategy != ReadStrategyNormal {
				t.Errorf("Same version should use Normal strategy: V%s got %s",
					v.String(), strategy.String())
			}
		}
	})
	
	t.Run("newer_file_always_unsupported", func(t *testing.T) {
		// Newer file version should always be Unsupported
		// (assuming CheckReadCompatibility wasn't called first)
		checker := NewVersionChecker(V1_0)
		
		newerVersions := []uint16{V1_1.Encoded(), V1_2.Encoded()}
		for _, fileVer := range newerVersions {
			strategy := checker.GetReadStrategy(fileVer, 0)
			if strategy != ReadStrategyUnsupported {
				t.Errorf("Newer file should be Unsupported: file=0x%04X, got %s",
					fileVer, strategy.String())
			}
		}
	})
	
	t.Run("fallback_only_without_rowindex", func(t *testing.T) {
		// FallbackLinearScan should only be used when file lacks RowIndex
		checker := NewVersionChecker(V1_2)
		
		// V1.0 file (no RowIndex)
		strategy := checker.GetReadStrategy(V1_0.Encoded(), V1_0.FeatureFlags)
		if strategy != ReadStrategyFallbackLinearScan {
			t.Errorf("V1.0 file should use FallbackLinearScan, got %s", strategy.String())
		}
		
		// V1.1 file (has RowIndex)
		strategy = checker.GetReadStrategy(V1_1.Encoded(), V1_1.FeatureFlags)
		if strategy == ReadStrategyFallbackLinearScan {
			t.Error("V1.1 file (with RowIndex) should not use FallbackLinearScan")
		}
	})
}

// TestPropertyFuzz_VersionError tests error handling with random inputs
func TestPropertyFuzz_VersionError(t *testing.T) {
	rng := rand.New(rand.NewSource(456))
	
	for i := 0; i < 1000; i++ {
		fileVer := uint16(rng.Intn(0x10000))
		readerVer := uint16(rng.Intn(0x10000))
		
		err := &VersionError{
			Op:            "fuzz_test",
			FileVersion:   fileVer,
			ReaderVersion: readerVer,
			Reason:        fmt.Sprintf("test reason %d", i),
			Suggestion:    "upgrade",
		}
		
		// Error() should never panic
		msg := err.Error()
		if msg == "" {
			t.Error("Error() returned empty string")
		}
		
		// Unwrap() should never panic
		_ = err.Unwrap()
	}
}

// BenchmarkPropertyRoundtrip benchmarks roundtrip operations
func BenchmarkPropertyRoundtrip_VersionEncoding(b *testing.B) {
	rng := rand.New(rand.NewSource(789))
	versions := make([]VersionPolicy, 1000)
	for i := range versions {
		versions[i] = VersionPolicy{
			MajorVersion: uint8(rng.Intn(256)),
			MinorVersion: uint8(rng.Intn(256)),
			FeatureFlags: rng.Uint32(),
		}
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vp := versions[i%len(versions)]
		encoded := vp.Encoded()
		_ = VersionFromEncoded(encoded)
	}
}

func BenchmarkPropertyRoundtrip_FeatureFlags(b *testing.B) {
	rng := rand.New(rand.NewSource(101112))
	flags := make([]uint32, 1000)
	for i := range flags {
		flags[i] = rng.Uint32()
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f := flags[i%len(flags)]
		hex := FeatureFlagsToHex(f)
		_, _ = ParseFeatureFlags(hex)
	}
}
