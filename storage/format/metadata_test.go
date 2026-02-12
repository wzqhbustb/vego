// Copyright 2024 Vego Authors
// Licensed under the Apache License, Version 2.0

package format

import (
	"bytes"
	"testing"
)

func TestIsVegoMetadataKey(t *testing.T) {
	tests := []struct {
		key      string
		expected bool
	}{
		{"vego.format.version", true},
		{"vego.rowindex.offset", true},
		{"vego.blockcache.enabled", true},
		{"user.key", false},
		{"myapp.metadata", false},
		{"vego", false}, // no dot after prefix
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			result := IsVegoMetadataKey(tt.key)
			if result != tt.expected {
				t.Errorf("IsVegoMetadataKey(%q) = %v, want %v", tt.key, result, tt.expected)
			}
		})
	}
}

func TestFooterSetGetFormatVersion(t *testing.T) {
	footer := NewFooter()

	// Test setting V1.0
	footer.SetFormatVersion(V1_0)
	if footer.Version != V1_0.Encoded() {
		t.Errorf("Footer.Version = 0x%04X, want 0x%04X", footer.Version, V1_0.Encoded())
	}
	if footer.Metadata[MetadataFormatVersion] != "1.0" {
		t.Errorf("Metadata[version] = %q, want %q", footer.Metadata[MetadataFormatVersion], "1.0")
	}
	expectedFeatures := FeatureFlagsToHex(V1_0.FeatureFlags)
	if footer.Metadata[MetadataFormatFeatures] != expectedFeatures {
		t.Errorf("Metadata[features] = %q, want %q", footer.Metadata[MetadataFormatFeatures], expectedFeatures)
	}

	// Test getting back
	vp := footer.GetFormatVersion()
	if vp.MajorVersion != 1 || vp.MinorVersion != 0 {
		t.Errorf("GetFormatVersion() = %s, want 1.0", vp.String())
	}
	if vp.FeatureFlags != V1_0.FeatureFlags {
		t.Errorf("FeatureFlags = 0x%08X, want 0x%08X", vp.FeatureFlags, V1_0.FeatureFlags)
	}

	// Test V1.1
	footer.SetFormatVersion(V1_1)
	vp = footer.GetFormatVersion()
	if !vp.HasFeature(FeatureRowIndex) {
		t.Error("V1.1 should have FeatureRowIndex")
	}
}

func TestFooterGetFormatVersionFallback(t *testing.T) {
	// Test fallback to Footer.Version when metadata is missing
	footer := NewFooter()
	
	// Simulate old file: only Footer.Version is set
	footer.Version = 1 // Legacy V1
	delete(footer.Metadata, MetadataFormatVersion)
	delete(footer.Metadata, MetadataFormatFeatures)
	
	vp := footer.GetFormatVersion()
	if vp.MajorVersion != 1 || vp.MinorVersion != 0 {
		t.Errorf("Fallback version = %s, want 1.0", vp.String())
	}

	// Test with new format version in Footer.Version
	footer.Version = 0x0101
	vp = footer.GetFormatVersion()
	if vp.MajorVersion != 1 || vp.MinorVersion != 1 {
		t.Errorf("Version from Footer.Version = %s, want 1.1", vp.String())
	}
}

func TestFooterRowIndexInfo(t *testing.T) {
	footer := NewFooter()

	// Test setting RowIndex info
	footer.SetRowIndexInfo(12345, 4096, 0xDEADBEEF)

	// Verify metadata is set
	if footer.Metadata[MetadataRowIndexOffset] != "12345" {
		t.Errorf("RowIndexOffset = %q, want %q", footer.Metadata[MetadataRowIndexOffset], "12345")
	}
	if footer.Metadata[MetadataRowIndexSize] != "4096" {
		t.Errorf("RowIndexSize = %q, want %q", footer.Metadata[MetadataRowIndexSize], "4096")
	}
	if footer.Metadata[MetadataRowIndexChecksum] != "0xDEADBEEF" {
		t.Errorf("RowIndexChecksum = %q, want %q", footer.Metadata[MetadataRowIndexChecksum], "0xDEADBEEF")
	}

	// Test getting back
	offset, size, checksum, ok := footer.GetRowIndexInfo()
	if !ok {
		t.Error("GetRowIndexInfo() returned ok=false")
	}
	if offset != 12345 {
		t.Errorf("offset = %d, want %d", offset, 12345)
	}
	if size != 4096 {
		t.Errorf("size = %d, want %d", size, 4096)
	}
	if checksum != 0xDEADBEEF {
		t.Errorf("checksum = 0x%08X, want 0x%08X", checksum, 0xDEADBEEF)
	}

	// Test HasRowIndex
	if !footer.HasRowIndex() {
		t.Error("HasRowIndex() should return true")
	}
}

func TestFooterRowIndexInfoNotPresent(t *testing.T) {
	footer := NewFooter()

	// Without RowIndex info
	offset, size, checksum, ok := footer.GetRowIndexInfo()
	if ok {
		t.Error("GetRowIndexInfo() should return ok=false when not present")
	}
	if offset != 0 || size != 0 || checksum != 0 {
		t.Errorf("Expected zeros, got offset=%d, size=%d, checksum=0x%08X", offset, size, checksum)
	}

	if footer.HasRowIndex() {
		t.Error("HasRowIndex() should return false when not present")
	}
}

func TestFooterBlockCacheInfo(t *testing.T) {
	footer := NewFooter()

	// Test setting BlockCache info
	footer.SetBlockCacheInfo(65536)

	// Verify metadata
	if footer.Metadata[MetadataBlockCacheEnabled] != "true" {
		t.Errorf("BlockCacheEnabled = %q, want %q", footer.Metadata[MetadataBlockCacheEnabled], "true")
	}
	if footer.Metadata[MetadataBlockCacheBlockSize] != "65536" {
		t.Errorf("BlockCacheBlockSize = %q, want %q", footer.Metadata[MetadataBlockCacheBlockSize], "65536")
	}

	// Test getting back
	blockSize, ok := footer.GetBlockCacheInfo()
	if !ok {
		t.Error("GetBlockCacheInfo() returned ok=false")
	}
	if blockSize != 65536 {
		t.Errorf("blockSize = %d, want %d", blockSize, 65536)
	}

	if !footer.HasBlockCache() {
		t.Error("HasBlockCache() should return true")
	}
}

func TestFooterBlockCacheNotEnabled(t *testing.T) {
	footer := NewFooter()

	// Without BlockCache
	blockSize, ok := footer.GetBlockCacheInfo()
	if ok {
		t.Error("GetBlockCacheInfo() should return ok=false when not present")
	}
	if blockSize != 0 {
		t.Errorf("blockSize = %d, want 0", blockSize)
	}

	// Test with disabled flag
	footer.Metadata[MetadataBlockCacheEnabled] = "false"
	footer.Metadata[MetadataBlockCacheBlockSize] = "65536"
	blockSize, ok = footer.GetBlockCacheInfo()
	if ok {
		t.Error("GetBlockCacheInfo() should return ok=false when disabled")
	}
}

func TestFooterGetFormatMetadata(t *testing.T) {
	footer := NewFooter()
	footer.SetFormatVersion(V1_1)
	footer.SetRowIndexInfo(1000, 2048, 0x12345678)

	fm := footer.GetFormatMetadata()

	if fm.Version.MajorVersion != 1 || fm.Version.MinorVersion != 1 {
		t.Errorf("Version = %s, want 1.1", fm.Version.String())
	}
	if !fm.HasRowIndex {
		t.Error("HasRowIndex should be true")
	}
	if fm.RowIndexOffset != 1000 {
		t.Errorf("RowIndexOffset = %d, want 1000", fm.RowIndexOffset)
	}
	if fm.RowIndexSize != 2048 {
		t.Errorf("RowIndexSize = %d, want 2048", fm.RowIndexSize)
	}
}

func TestFooterSetFormatMetadata(t *testing.T) {
	footer := NewFooter()

	fm := FormatMetadata{
		Version:          V1_2,
		Features:         V1_2.FeatureFlags,
		HasRowIndex:      true,
		RowIndexOffset:   5000,
		RowIndexSize:     8192,
		RowIndexChecksum: 0xAABBCCDD,
		HasBlockCache:    true,
		BlockCacheBlockSize: 65536,
	}

	footer.SetFormatMetadata(fm)

	// Verify all metadata is set
	vp := footer.GetFormatVersion()
	if vp.String() != "1.2" {
		t.Errorf("Version = %s, want 1.2", vp.String())
	}

	if !footer.HasRowIndex() {
		t.Error("HasRowIndex should be true")
	}

	if !footer.HasBlockCache() {
		t.Error("HasBlockCache should be true")
	}
}

func TestFooterClearVegoMetadata(t *testing.T) {
	footer := NewFooter()

	// Add Vego and user metadata
	footer.SetFormatVersion(V1_1)
	footer.SetRowIndexInfo(1000, 2000, 0)
	footer.AddMetadata("user.key1", "value1")
	footer.AddMetadata("user.key2", "value2")

	// Verify both exist
	if len(footer.Metadata) < 4 {
		t.Fatal("Expected at least 4 metadata entries")
	}

	// Clear Vego metadata
	footer.ClearVegoMetadata()

	// Verify only user metadata remains
	if len(footer.Metadata) != 2 {
		t.Errorf("Expected 2 metadata entries, got %d", len(footer.Metadata))
	}
	if footer.Metadata["user.key1"] != "value1" {
		t.Error("user.key1 should be preserved")
	}
	if footer.Metadata["user.key2"] != "value2" {
		t.Error("user.key2 should be preserved")
	}
	if _, ok := footer.Metadata[MetadataFormatVersion]; ok {
		t.Error("vego.format.version should be cleared")
	}
}

func TestFooterGetUserMetadata(t *testing.T) {
	footer := NewFooter()

	// Add mixed metadata
	footer.SetFormatVersion(V1_1)
	footer.AddMetadata("user.key1", "value1")
	footer.AddMetadata("app.setting", "config")

	userMeta := footer.GetUserMetadata()

	if len(userMeta) != 2 {
		t.Errorf("Expected 2 user metadata entries, got %d", len(userMeta))
	}
	if userMeta["user.key1"] != "value1" {
		t.Error("user.key1 missing or incorrect")
	}
	if userMeta["app.setting"] != "config" {
		t.Error("app.setting missing or incorrect")
	}

	// Verify vego keys are not included
	if _, ok := userMeta[MetadataFormatVersion]; ok {
		t.Error("vego.format.version should not be in user metadata")
	}
}

func TestFooterMergeMetadata(t *testing.T) {
	footer := NewFooter()

	// Set initial Vego metadata
	footer.SetFormatVersion(V1_1)
	footer.SetRowIndexInfo(1000, 2000, 0)

	// Merge user metadata
	userMeta := map[string]string{
		"doc.author":    "test",
		"doc.timestamp": "1234567890",
	}
	footer.MergeMetadata(userMeta)

	// Verify Vego metadata is preserved
	vp := footer.GetFormatVersion()
	if vp.String() != "1.1" {
		t.Errorf("Vego version metadata should be preserved, got %s", vp.String())
	}

	// Verify user metadata is added
	if footer.Metadata["doc.author"] != "test" {
		t.Error("doc.author should be added")
	}

	// Test that MergeMetadata replaces old user metadata
	newUserMeta := map[string]string{
		"doc.title": "new title",
	}
	footer.MergeMetadata(newUserMeta)

	if _, ok := footer.Metadata["doc.author"]; ok {
		t.Error("Old user metadata should be replaced")
	}
	if footer.Metadata["doc.title"] != "new title" {
		t.Error("doc.title should be added")
	}

	// Test that vego. keys in user metadata are ignored
	footer.MergeMetadata(map[string]string{
		"vego.hacked": "bad value",
		"normal.key":  "good value",
	})
	if _, ok := footer.Metadata["vego.hacked"]; ok {
		t.Error("vego. keys from user metadata should be ignored")
	}
}

func TestFooterValidateFormatMetadata(t *testing.T) {
	// Valid: V1.1 with RowIndex
	footer := NewFooter()
	footer.SetFormatVersion(V1_1)
	footer.SetRowIndexInfo(1000, 2000, 0)
	if err := footer.ValidateFormatMetadata(); err != nil {
		t.Errorf("Valid V1.1 with RowIndex: unexpected error: %v", err)
	}

	// Valid: V1.2 with both RowIndex and BlockCache
	footer2 := NewFooter()
	footer2.SetFormatVersion(V1_2)
	footer2.SetRowIndexInfo(1000, 2000, 0)
	footer2.SetBlockCacheInfo(65536)
	if err := footer2.ValidateFormatMetadata(); err != nil {
		t.Errorf("Valid V1.2 with RowIndex and BlockCache: unexpected error: %v", err)
	}

	// Valid: V1.0 without RowIndex
	footer3 := NewFooter()
	footer3.SetFormatVersion(V1_0)
	if err := footer3.ValidateFormatMetadata(); err != nil {
		t.Errorf("Valid V1.0 without RowIndex: unexpected error: %v", err)
	}
}

func TestFooterValidateFormatMetadataInconsistent(t *testing.T) {
	// Invalid: V1.0 with RowIndex metadata
	footer := NewFooter()
	footer.SetFormatVersion(V1_0)
	footer.SetRowIndexInfo(1000, 2000, 0) // Inconsistent!

	if err := footer.ValidateFormatMetadata(); err == nil {
		t.Error("Expected error for V1.0 with RowIndex metadata")
	}

	// Invalid: V1.1 with BlockCache metadata
	footer2 := NewFooter()
	footer2.SetFormatVersion(V1_1)
	footer2.SetBlockCacheInfo(65536) // Inconsistent!

	if err := footer2.ValidateFormatMetadata(); err == nil {
		t.Error("Expected error for V1.1 with BlockCache metadata")
	}
}

func TestFooterRoundtrip(t *testing.T) {
	// Test that metadata survives write/read roundtrip
	original := NewFooter()
	original.SetFormatVersion(V1_2)
	original.SetRowIndexInfo(12345, 4096, 0xDEADBEEF)
	original.SetBlockCacheInfo(65536)
	original.AddMetadata("user.author", "test")

	// Simulate write/read with buffer
	var buf bytes.Buffer
	if _, err := original.WriteTo(&buf); err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}

	// Read back
	readFooter := &Footer{}
	if _, err := readFooter.ReadFrom(&buf); err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}

	// Verify format metadata
	vp := readFooter.GetFormatVersion()
	if vp.String() != "1.2" {
		t.Errorf("Version = %s, want 1.2", vp.String())
	}

	if !readFooter.HasRowIndex() {
		t.Error("RowIndex metadata should be preserved")
	}

	offset, size, checksum, ok := readFooter.GetRowIndexInfo()
	if !ok || offset != 12345 || size != 4096 || checksum != 0xDEADBEEF {
		t.Error("RowIndex info mismatch")
	}

	if !readFooter.HasBlockCache() {
		t.Error("BlockCache metadata should be preserved")
	}

	// Verify user metadata
	if readFooter.Metadata["user.author"] != "test" {
		t.Error("User metadata should be preserved")
	}
}

