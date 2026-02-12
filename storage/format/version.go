// Copyright 2024 Vego Authors
// Licensed under the Apache License, Version 2.0

package format

import (
	"fmt"
	"strconv"
	"strings"
)

// Feature flags for format capabilities
// These indicate what features a specific format version supports
const (
	FeatureBasicColumnar uint32 = 1 << iota
	FeatureZstdCompression
	FeatureDictionaryEncoding
	FeatureRLE
	FeatureBitPacking
	FeatureRowIndex        // V1.1: ID to row index mapping
	FeatureBlockCache      // V1.2: Block cache hints
	FeatureAsyncIO         // Phase 2: Async I/O support
	FeatureFullZip         // Phase 3: Full zip compression
	FeatureChecksum        // Per-page CRC32 checksum
	FeatureEncryption      // AES encryption
)

// FeatureFlagName returns the string representation of a feature flag
func FeatureFlagName(f uint32) string {
	switch f {
	case FeatureBasicColumnar:
		return "BasicColumnar"
	case FeatureZstdCompression:
		return "ZstdCompression"
	case FeatureDictionaryEncoding:
		return "DictionaryEncoding"
	case FeatureRLE:
		return "RLE"
	case FeatureBitPacking:
		return "BitPacking"
	case FeatureRowIndex:
		return "RowIndex"
	case FeatureBlockCache:
		return "BlockCache"
	case FeatureAsyncIO:
		return "AsyncIO"
	case FeatureFullZip:
		return "FullZip"
	case FeatureChecksum:
		return "Checksum"
	case FeatureEncryption:
		return "Encryption"
	default:
		return fmt.Sprintf("Unknown(%d)", f)
	}
}

// FeaturesToStrings converts feature flags to string slice
func FeaturesToStrings(features uint32) []string {
	var result []string
	for i := 0; i < 32; i++ {
		flag := uint32(1) << i
		if features&flag != 0 {
			result = append(result, FeatureFlagName(flag))
		}
	}
	return result
}

// VersionPolicy defines the capabilities of a specific format version
type VersionPolicy struct {
	MajorVersion uint8
	MinorVersion uint8
	FeatureFlags uint32 // Features supported by this version
}

// Predefined version policies
var (
	V1_0 = VersionPolicy{
		MajorVersion: 1,
		MinorVersion: 0,
		FeatureFlags: FeatureBasicColumnar | FeatureZstdCompression,
	}

	V1_1 = VersionPolicy{
		MajorVersion: 1,
		MinorVersion: 1,
		FeatureFlags: V1_0.FeatureFlags | FeatureRowIndex,
	}

	V1_2 = VersionPolicy{
		MajorVersion: 1,
		MinorVersion: 2,
		FeatureFlags: V1_1.FeatureFlags | FeatureBlockCache,
	}

	// CurrentFormatVersion is the latest version supported by this implementation
	CurrentFormatVersion = V1_2

	// MinReadableVersion is the oldest version that can be read
	MinReadableVersion = V1_0
)

// Encoded returns the version encoded as uint16: (Major << 8) | Minor
func (vp VersionPolicy) Encoded() uint16 {
	return (uint16(vp.MajorVersion) << 8) | uint16(vp.MinorVersion)
}

// String returns the version as "Major.Minor" string
func (vp VersionPolicy) String() string {
	return fmt.Sprintf("%d.%d", vp.MajorVersion, vp.MinorVersion)
}

// CanRead returns true if this version can read files created by 'other' version
// Rules:
//   - Must be same major version
//   - Must be >= other minor version
func (vp VersionPolicy) CanRead(other VersionPolicy) bool {
	return vp.MajorVersion == other.MajorVersion &&
		vp.MinorVersion >= other.MinorVersion
}

// CanBeReadBy returns true if files created by this version can be read by 'other' version
func (vp VersionPolicy) CanBeReadBy(other VersionPolicy) bool {
	return other.CanRead(vp)
}

// HasFeature returns true if this version supports the given feature
func (vp VersionPolicy) HasFeature(feature uint32) bool {
	return (vp.FeatureFlags & feature) != 0
}

// ParseVersion parses a version string like "1.1" into VersionPolicy
// Note: This only parses the version number, not the feature flags
func ParseVersion(s string) (VersionPolicy, error) {
	parts := strings.Split(s, ".")
	if len(parts) != 2 {
		return VersionPolicy{}, fmt.Errorf("invalid version format %q, expected Major.Minor", s)
	}

	major, err := strconv.ParseUint(parts[0], 10, 8)
	if err != nil {
		return VersionPolicy{}, fmt.Errorf("invalid major version: %w", err)
	}

	minor, err := strconv.ParseUint(parts[1], 10, 8)
	if err != nil {
		return VersionPolicy{}, fmt.Errorf("invalid minor version: %w", err)
	}

	vp := VersionPolicy{
		MajorVersion: uint8(major),
		MinorVersion: uint8(minor),
	}

	// Lookup predefined features
	switch vp.Encoded() {
	case V1_0.Encoded():
		vp.FeatureFlags = V1_0.FeatureFlags
	case V1_1.Encoded():
		vp.FeatureFlags = V1_1.FeatureFlags
	case V1_2.Encoded():
		vp.FeatureFlags = V1_2.FeatureFlags
	default:
		// Unknown version, features will be empty
		vp.FeatureFlags = 0
	}

	return vp, nil
}

// VersionFromEncoded creates VersionPolicy from encoded uint16
func VersionFromEncoded(encoded uint16) VersionPolicy {
	vp := VersionPolicy{
		MajorVersion: uint8(encoded >> 8),
		MinorVersion: uint8(encoded & 0xFF),
	}

	// Lookup predefined features
	switch encoded {
	case V1_0.Encoded():
		vp.FeatureFlags = V1_0.FeatureFlags
	case V1_1.Encoded():
		vp.FeatureFlags = V1_1.FeatureFlags
	case V1_2.Encoded():
		vp.FeatureFlags = V1_2.FeatureFlags
	}

	return vp
}

// NormalizeVersion maps legacy version numbers to new format
// Legacy V1 (version = 1) is mapped to V1.0 (0x0100)
func NormalizeVersion(v uint16) uint16 {
	switch v {
	case 1:
		// Legacy format V1 (before structured versioning)
		return V1_0.Encoded() // 0x0100
	case V1_0.Encoded(), V1_1.Encoded(), V1_2.Encoded():
		// Already new format
		return v
	default:
		// Unknown version, return as-is for further handling
		return v
	}
}

// Version errors
var (
	ErrVersionTooOld       = fmt.Errorf("file version too old, migration required")
	ErrVersionTooNew       = fmt.Errorf("file version too new, please upgrade reader")
	ErrFeatureNotSupported = fmt.Errorf("file uses unsupported feature")
)

// VersionError provides detailed context for version-related errors
type VersionError struct {
	Op            string // Operation that failed
	FileVersion   uint16 // Version in the file
	ReaderVersion uint16 // Version of the reader
	Reason        string // Detailed reason
	Suggestion    string // User-friendly suggestion
}

// Error implements the error interface
func (e *VersionError) Error() string {
	fileMajor, fileMinor := e.FileVersion>>8, e.FileVersion&0xFF
	readerMajor, readerMinor := e.ReaderVersion>>8, e.ReaderVersion&0xFF

	return fmt.Sprintf(
		"version error in %s: file=%d.%d, reader=%d.%d: %s; %s",
		e.Op, fileMajor, fileMinor, readerMajor, readerMinor,
		e.Reason, e.Suggestion,
	)
}

// Unwrap allows errors.Is() to work with VersionError
func (e *VersionError) Unwrap() error {
	switch {
	case strings.Contains(e.Reason, "newer"):
		return ErrVersionTooNew
	case strings.Contains(e.Reason, "mismatch"):
		// Major version mismatch - also considered "too new" for the reader
		return ErrVersionTooNew
	case strings.Contains(e.Reason, "older"):
		return ErrVersionTooOld
	default:
		return ErrFeatureNotSupported
	}
}

// ReadStrategy defines how to read files of different versions
type ReadStrategy int

const (
	ReadStrategyUnsupported ReadStrategy = iota
	ReadStrategyNormal                   // Full feature support
	ReadStrategyCompatible               // Ignore optional new features
	ReadStrategyFallbackLinearScan       // No RowIndex, full table scan
)

// String returns the string representation of ReadStrategy
func (s ReadStrategy) String() string {
	switch s {
	case ReadStrategyNormal:
		return "normal"
	case ReadStrategyCompatible:
		return "compatible"
	case ReadStrategyFallbackLinearScan:
		return "fallback_linear_scan"
	default:
		return "unsupported"
	}
}

// VersionChecker provides version compatibility checking
type VersionChecker struct {
	readerVersion VersionPolicy
}

// NewVersionChecker creates a new version checker for the given reader version
func NewVersionChecker(readerVersion VersionPolicy) *VersionChecker {
	return &VersionChecker{readerVersion: readerVersion}
}

// CheckReadCompatibility verifies if a file version can be read
// Returns nil if compatible, VersionError otherwise
func (vc *VersionChecker) CheckReadCompatibility(fileVersion uint16) error {
	fileVP := VersionFromEncoded(fileVersion)

	// Check major version compatibility
	if fileVP.MajorVersion != vc.readerVersion.MajorVersion {
		return &VersionError{
			Op:            "check_read_compatibility",
			FileVersion:   fileVersion,
			ReaderVersion: vc.readerVersion.Encoded(),
			Reason:        fmt.Sprintf("major version mismatch: file=%d, reader=%d", 
				fileVP.MajorVersion, vc.readerVersion.MajorVersion),
			Suggestion:    fmt.Sprintf("Please use Vego %d.x to read this file", fileVP.MajorVersion),
		}
	}

	// Check if file is newer than reader
	if fileVP.MinorVersion > vc.readerVersion.MinorVersion {
		return &VersionError{
			Op:            "check_read_compatibility",
			FileVersion:   fileVersion,
			ReaderVersion: vc.readerVersion.Encoded(),
			Reason:        "file version is newer than reader",
			Suggestion:    fmt.Sprintf("Please upgrade to Vego %d.%d or later", 
				fileVP.MajorVersion, fileVP.MinorVersion),
		}
	}

	return nil
}

// CanUseFeature checks if a specific feature can be used with the given file
// Both the file and reader must support the feature
func (vc *VersionChecker) CanUseFeature(fileFeatures uint32, feature uint32) bool {
	return (fileFeatures & feature) != 0 && (vc.readerVersion.FeatureFlags & feature) != 0
}

// GetReadStrategy determines the best strategy for reading a file
func (vc *VersionChecker) GetReadStrategy(fileVersion uint16, fileFeatures uint32) ReadStrategy {
	fileVP := VersionFromEncoded(fileVersion)

	// Same version: normal read
	if fileVP.MajorVersion == vc.readerVersion.MajorVersion &&
		fileVP.MinorVersion == vc.readerVersion.MinorVersion {
		return ReadStrategyNormal
	}

	// Older version: check for missing features
	if fileVP.MinorVersion < vc.readerVersion.MinorVersion {
		// Check if file lacks RowIndex
		if (fileFeatures & FeatureRowIndex) == 0 {
			return ReadStrategyFallbackLinearScan
		}
		return ReadStrategyCompatible
	}

	// Newer version (should have been caught by CheckReadCompatibility)
	return ReadStrategyUnsupported
}

// ReaderVersion returns the reader version this checker was created with
func (vc *VersionChecker) ReaderVersion() VersionPolicy {
	return vc.readerVersion
}

// FeatureFlagsToHex converts feature flags to hexadecimal string for storage
func FeatureFlagsToHex(flags uint32) string {
	return fmt.Sprintf("0x%08X", flags)
}

// ParseFeatureFlags parses feature flags from hexadecimal string
func ParseFeatureFlags(s string) (uint32, error) {
	s = strings.TrimPrefix(s, "0x")
	s = strings.TrimPrefix(s, "0X")
	
	val, err := strconv.ParseUint(s, 16, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid feature flags format: %w", err)
	}
	
	return uint32(val), nil
}
