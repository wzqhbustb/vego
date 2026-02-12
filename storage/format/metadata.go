// Copyright 2024 Vego Authors
// Licensed under the Apache License, Version 2.0

package format

import (
	"fmt"
	"strconv"
	"strings"
)

// Metadata keys for Vego format information
// Using "vego." prefix to avoid conflicts with user metadata
const (
	// Format version info
	MetadataFormatVersion  = "vego.format.version"   // e.g., "1.1"
	MetadataFormatFeatures = "vego.format.features"  // e.g., "0x00000042"

	// RowIndex info (V1.1+)
	MetadataRowIndexOffset   = "vego.rowindex.offset"   // int64 as string
	MetadataRowIndexSize     = "vego.rowindex.size"     // int32 as string
	MetadataRowIndexChecksum = "vego.rowindex.checksum" // uint32 as hex string

	// BlockCache info (V1.2+)
	MetadataBlockCacheEnabled   = "vego.blockcache.enabled"    // "true" or "false"
	MetadataBlockCacheBlockSize = "vego.blockcache.block_size" // int as string
)

// FormatMetadata provides structured access to format-related metadata
type FormatMetadata struct {
	Version  VersionPolicy
	Features uint32

	// RowIndex info (optional, V1.1+)
	HasRowIndex   bool
	RowIndexOffset int64
	RowIndexSize   int32
	RowIndexChecksum uint32

	// BlockCache info (optional, V1.2+)
	HasBlockCache   bool
	BlockCacheBlockSize int32
}

// IsVegoMetadataKey returns true if the key is a Vego internal metadata key
func IsVegoMetadataKey(key string) bool {
	return strings.HasPrefix(key, "vego.")
}

// SetFormatVersion sets the format version in footer metadata
func (f *Footer) SetFormatVersion(vp VersionPolicy) {
	if f.Metadata == nil {
		f.Metadata = make(map[string]string)
	}
	f.Metadata[MetadataFormatVersion] = vp.String()
	f.Metadata[MetadataFormatFeatures] = FeatureFlagsToHex(vp.FeatureFlags)
	f.Version = vp.Encoded()
}

// GetFormatVersion extracts the format version from footer metadata
// Returns V1_0 if not found (for backward compatibility with old files)
func (f *Footer) GetFormatVersion() VersionPolicy {
	// First check the explicit version string
	if versionStr, ok := f.Metadata[MetadataFormatVersion]; ok {
		if vp, err := ParseVersion(versionStr); err == nil {
			// Parse features if available
			if featuresStr, ok := f.Metadata[MetadataFormatFeatures]; ok {
				if features, err := ParseFeatureFlags(featuresStr); err == nil {
					vp.FeatureFlags = features
				}
			}
			return vp
		}
	}

	// Fall back to Footer.Version field (for files without metadata)
	// Normalize legacy version numbers
	normalized := NormalizeVersion(f.Version)
	return VersionFromEncoded(normalized)
}

// SetRowIndexInfo stores RowIndex location information in footer metadata
func (f *Footer) SetRowIndexInfo(offset int64, size int32, checksum uint32) {
	if f.Metadata == nil {
		f.Metadata = make(map[string]string)
	}
	f.Metadata[MetadataRowIndexOffset] = strconv.FormatInt(offset, 10)
	f.Metadata[MetadataRowIndexSize] = strconv.FormatInt(int64(size), 10)
	f.Metadata[MetadataRowIndexChecksum] = fmt.Sprintf("0x%08X", checksum)
}

// GetRowIndexInfo extracts RowIndex location information from footer metadata
// Returns ok=false if RowIndex info is not present
func (f *Footer) GetRowIndexInfo() (offset int64, size int32, checksum uint32, ok bool) {
	offsetStr, ok1 := f.Metadata[MetadataRowIndexOffset]
	sizeStr, ok2 := f.Metadata[MetadataRowIndexSize]
	
	if !ok1 || !ok2 {
		return 0, 0, 0, false
	}

	offset, err := strconv.ParseInt(offsetStr, 10, 64)
	if err != nil {
		return 0, 0, 0, false
	}

	size64, err := strconv.ParseInt(sizeStr, 10, 32)
	if err != nil {
		return 0, 0, 0, false
	}
	size = int32(size64)

	// Checksum is optional
	if checksumStr, ok := f.Metadata[MetadataRowIndexChecksum]; ok {
		fmt.Sscanf(checksumStr, "0x%08X", &checksum)
	}

	return offset, size, checksum, true
}

// HasRowIndex returns true if footer contains RowIndex metadata
func (f *Footer) HasRowIndex() bool {
	_, _, _, ok := f.GetRowIndexInfo()
	return ok
}

// SetBlockCacheInfo stores BlockCache configuration in footer metadata
func (f *Footer) SetBlockCacheInfo(blockSize int32) {
	if f.Metadata == nil {
		f.Metadata = make(map[string]string)
	}
	f.Metadata[MetadataBlockCacheEnabled] = "true"
	f.Metadata[MetadataBlockCacheBlockSize] = strconv.FormatInt(int64(blockSize), 10)
}

// GetBlockCacheInfo extracts BlockCache configuration from footer metadata
// Returns ok=false if BlockCache is not enabled
func (f *Footer) GetBlockCacheInfo() (blockSize int32, ok bool) {
	enabledStr, ok1 := f.Metadata[MetadataBlockCacheEnabled]
	blockSizeStr, ok2 := f.Metadata[MetadataBlockCacheBlockSize]

	if !ok1 || !ok2 {
		return 0, false
	}

	if enabledStr != "true" {
		return 0, false
	}

	blockSize64, err := strconv.ParseInt(blockSizeStr, 10, 32)
	if err != nil {
		return 0, false
	}

	return int32(blockSize64), true
}

// HasBlockCache returns true if footer contains BlockCache metadata
func (f *Footer) HasBlockCache() bool {
	_, ok := f.GetBlockCacheInfo()
	return ok
}

// GetFormatMetadata extracts all format metadata from footer
func (f *Footer) GetFormatMetadata() FormatMetadata {
	fm := FormatMetadata{
		Version: f.GetFormatVersion(),
		Features: f.GetFormatVersion().FeatureFlags,
	}

	// Extract RowIndex info
	if offset, size, checksum, ok := f.GetRowIndexInfo(); ok {
		fm.HasRowIndex = true
		fm.RowIndexOffset = offset
		fm.RowIndexSize = size
		fm.RowIndexChecksum = checksum
	}

	// Extract BlockCache info
	if blockSize, ok := f.GetBlockCacheInfo(); ok {
		fm.HasBlockCache = true
		fm.BlockCacheBlockSize = blockSize
	}

	return fm
}

// SetFormatMetadata sets all format metadata in footer
func (f *Footer) SetFormatMetadata(fm FormatMetadata) {
	f.SetFormatVersion(fm.Version)
	
	if fm.HasRowIndex {
		f.SetRowIndexInfo(fm.RowIndexOffset, fm.RowIndexSize, fm.RowIndexChecksum)
	}

	if fm.HasBlockCache {
		f.SetBlockCacheInfo(fm.BlockCacheBlockSize)
	}
}

// ClearVegoMetadata removes all Vego internal metadata keys
// Useful when copying metadata from one file to another
func (f *Footer) ClearVegoMetadata() {
	if f.Metadata == nil {
		return
	}
	
	for key := range f.Metadata {
		if IsVegoMetadataKey(key) {
			delete(f.Metadata, key)
		}
	}
}

// GetUserMetadata returns only user-defined metadata (excluding vego. keys)
func (f *Footer) GetUserMetadata() map[string]string {
	result := make(map[string]string)
	for k, v := range f.Metadata {
		if !IsVegoMetadataKey(k) {
			result[k] = v
		}
	}
	return result
}

// MergeMetadata merges user metadata into footer, preserving Vego internal metadata
func (f *Footer) MergeMetadata(userMetadata map[string]string) {
	if f.Metadata == nil {
		f.Metadata = make(map[string]string)
	}
	
	// First, save Vego internal metadata
	vegoMeta := make(map[string]string)
	for k, v := range f.Metadata {
		if IsVegoMetadataKey(k) {
			vegoMeta[k] = v
		}
	}
	
	// Clear and rebuild
	f.Metadata = make(map[string]string)
	
	// Restore Vego metadata
	for k, v := range vegoMeta {
		f.Metadata[k] = v
	}
	
	// Add user metadata (excluding vego. keys)
	for k, v := range userMetadata {
		if !IsVegoMetadataKey(k) {
			f.Metadata[k] = v
		}
	}
}

// ValidateFormatMetadata validates that format metadata is consistent
func (f *Footer) ValidateFormatMetadata() error {
	vp := f.GetFormatVersion()
	
	// Check if RowIndex metadata is consistent with version
	hasRowIndexMeta := f.HasRowIndex()
	versionHasRowIndex := vp.HasFeature(FeatureRowIndex)
	
	if hasRowIndexMeta && !versionHasRowIndex {
		return fmt.Errorf("footer has RowIndex metadata but version %s does not support it", vp.String())
	}
	
	// Check if BlockCache metadata is consistent with version
	hasBlockCacheMeta := f.HasBlockCache()
	versionHasBlockCache := vp.HasFeature(FeatureBlockCache)
	
	if hasBlockCacheMeta && !versionHasBlockCache {
		return fmt.Errorf("footer has BlockCache metadata but version %s does not support it", vp.String())
	}
	
	return nil
}
