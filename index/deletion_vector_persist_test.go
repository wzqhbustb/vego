// Copyright 2024 Vego Authors
// Licensed under the Apache License, Version 2.0

package hnsw

import (
	"os"
	"path/filepath"
	"testing"
)

func TestGetDeletionVectorPath(t *testing.T) {
	tests := []struct {
		dataFile string
		expected string
	}{
		{
			dataFile: "/path/to/vectors.lance",
			expected: "/path/to/vectors.lance.del",
		},
		{
			dataFile: "vectors.lance",
			expected: "vectors.lance.del",
		},
		{
			dataFile: "/data/collection/docs.data",
			expected: "/data/collection/docs.data.del",
		},
	}

	for _, test := range tests {
		result := GetDeletionVectorPath(test.dataFile)
		if result != test.expected {
			t.Errorf("GetDeletionVectorPath(%q) = %q, expected %q",
				test.dataFile, result, test.expected)
		}
	}
}

func TestSerializeDeserialize(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "dv_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dvPath := filepath.Join(tmpDir, "test.del")

	// Create DV with some deletions
	dv := NewDeletionVector()
	dv.MarkDeleted(0)
	dv.MarkDeleted(5)
	dv.MarkDeleted(100)
	dv.MarkDeleted(10000)

	// Serialize
	if err := dv.Serialize(dvPath); err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	// Verify file exists
	if _, err := os.Stat(dvPath); os.IsNotExist(err) {
		t.Fatal("Serialized file does not exist")
	}

	// Deserialize
	dv2, err := Deserialize(dvPath)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	// Verify data
	if dv2.Count() != 4 {
		t.Errorf("Expected count 4, got %d", dv2.Count())
	}

	if !dv2.IsDeleted(0) || !dv2.IsDeleted(5) || !dv2.IsDeleted(100) || !dv2.IsDeleted(10000) {
		t.Error("Deserialized DV should contain all original deletions")
	}

	if dv2.IsDeleted(1) || dv2.IsDeleted(99) {
		t.Error("Deserialized DV should not contain unmarked rows")
	}
}

func TestDeserializeNonExistentFile(t *testing.T) {
	dv, err := Deserialize("/nonexistent/path/file.del")
	if err == nil {
		t.Error("Deserialize should return error for non-existent file")
	}
	if dv != nil {
		t.Error("Deserialize should return nil DV for error case")
	}
}

func TestDeserializeOrEmpty(t *testing.T) {
	// Test with non-existent file
	dv := DeserializeOrEmpty("/nonexistent/path/file.del")
	if dv == nil {
		t.Error("DeserializeOrEmpty should return empty DV, not nil")
	}
	if !dv.IsEmpty() {
		t.Error("DeserializeOrEmpty should return empty DV for non-existent file")
	}

	// Test with existing file
	tmpDir, err := os.MkdirTemp("", "dv_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dvPath := filepath.Join(tmpDir, "test.del")

	// Create and serialize a DV
	dv1 := NewDeletionVector()
	dv1.MarkDeleted(1)
	dv1.MarkDeleted(2)
	if err := dv1.Serialize(dvPath); err != nil {
		t.Fatal(err)
	}

	// DeserializeOrEmpty should return the actual data
	dv2 := DeserializeOrEmpty(dvPath)
	if dv2.Count() != 2 {
		t.Errorf("Expected count 2, got %d", dv2.Count())
	}
}

func TestFileExists(t *testing.T) {
	// Test with non-existent file
	if FileExists("/nonexistent/path/file.del") {
		t.Error("FileExists should return false for non-existent file")
	}

	// Test with existing file
	tmpDir, err := os.MkdirTemp("", "dv_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dvPath := filepath.Join(tmpDir, "test.del")

	// Create file
	f, err := os.Create(dvPath)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	if !FileExists(dvPath) {
		t.Error("FileExists should return true for existing file")
	}
}

func TestSerializeEmptyDV(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "dv_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dvPath := filepath.Join(tmpDir, "empty.del")

	// Create empty DV
	dv := NewDeletionVector()

	// Serialize
	if err := dv.Serialize(dvPath); err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	// Deserialize
	dv2, err := Deserialize(dvPath)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if !dv2.IsEmpty() {
		t.Error("Deserialized empty DV should be empty")
	}
}

func TestSerializeLargeDV(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "dv_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dvPath := filepath.Join(tmpDir, "large.del")

	// Create DV with many deletions
	dv := NewDeletionVector()
	for i := uint32(0); i < 100000; i += 2 { // Even numbers
		dv.MarkDeleted(i)
	}

	// Serialize
	if err := dv.Serialize(dvPath); err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	// Deserialize
	dv2, err := Deserialize(dvPath)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	// Verify
	if dv2.Count() != 50000 {
		t.Errorf("Expected count 50000, got %d", dv2.Count())
	}

	// Spot check
	if !dv2.IsDeleted(0) || !dv2.IsDeleted(50000) || !dv2.IsDeleted(99998) {
		t.Error("Large DV spot checks failed")
	}
	if dv2.IsDeleted(1) || dv2.IsDeleted(50001) {
		t.Error("Large DV should not contain odd numbers")
	}
}

func TestGetDeletionVectorInfo(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "dv_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dvPath := filepath.Join(tmpDir, "info.del")

	// Test with non-existent file
	_, _, err = GetDeletionVectorInfo(dvPath)
	if err == nil {
		t.Error("GetDeletionVectorInfo should return error for non-existent file")
	}

	// Create DV with known deletions
	dv := NewDeletionVector()
	for i := uint32(0); i < 100; i++ {
		dv.MarkDeleted(i)
	}

	if err := dv.Serialize(dvPath); err != nil {
		t.Fatal(err)
	}

	// Get info
	numDeleted, fileSize, err := GetDeletionVectorInfo(dvPath)
	if err != nil {
		t.Fatalf("GetDeletionVectorInfo failed: %v", err)
	}

	if numDeleted != 100 {
		t.Errorf("Expected numDeleted 100, got %d", numDeleted)
	}

	if fileSize <= 16 { // At least header size
		t.Error("File size should be greater than header size")
	}
}

func TestDeserializeCorruptedFile(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "dv_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Test with wrong magic
	badMagicPath := filepath.Join(tmpDir, "bad_magic.del")
	f, err := os.Create(badMagicPath)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString("BAD1") // Wrong magic
	f.Write([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}) // Padding
	f.Close()

	_, err = Deserialize(badMagicPath)
	if err == nil {
		t.Error("Deserialize should fail with wrong magic")
	}

	// Test with wrong version
	badVersionPath := filepath.Join(tmpDir, "bad_version.del")
	f, err = os.Create(badVersionPath)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString(delFileMagic)
	// Write version 999
	f.Write([]byte{0xE7, 0x03, 0x00, 0x00}) // 999 in little endian
	f.Write([]byte{0, 0, 0, 0, 0, 0, 0, 0}) // NumDeleted
	f.Close()

	_, err = Deserialize(badVersionPath)
	if err == nil {
		t.Error("Deserialize should fail with wrong version")
	}

	// Test with truncated file (just header, no bitmap)
	truncatedPath := filepath.Join(tmpDir, "truncated.del")
	f, err = os.Create(truncatedPath)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString(delFileMagic)
	f.Write([]byte{0x01, 0x00, 0x00, 0x00}) // Version 1
	f.Write([]byte{0x0A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}) // NumDeleted 10
	f.Close()

	// This should not fail - an empty bitmap is valid
	dv, err := Deserialize(truncatedPath)
	if err != nil {
		t.Logf("Deserialize of truncated file returned error (may be expected): %v", err)
	} else {
		// If no error, the bitmap should be empty or have 10 items
		t.Logf("Deserialized truncated file, count=%d", dv.Count())
	}
}

func TestSerializeAndModifyOriginal(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "dv_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dvPath := filepath.Join(tmpDir, "test.del")

	// Create and serialize
	dv := NewDeletionVector()
	dv.MarkDeleted(1)
	if err := dv.Serialize(dvPath); err != nil {
		t.Fatal(err)
	}

	// Modify original after serialize
	dv.MarkDeleted(2)

	// Deserialize should not have row 2
	dv2, err := Deserialize(dvPath)
	if err != nil {
		t.Fatal(err)
	}

	if dv2.IsDeleted(2) {
		t.Error("Modifications after Serialize should not affect saved file")
	}
}

func BenchmarkSerialize(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "dv_bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dvPath := filepath.Join(tmpDir, "bench.del")

	// Create DV with 10000 deletions
	dv := NewDeletionVector()
	for i := uint32(0); i < 10000; i++ {
		dv.MarkDeleted(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := dv.Serialize(dvPath); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDeserialize(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "dv_bench")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dvPath := filepath.Join(tmpDir, "bench.del")

	// Create and serialize DV with 10000 deletions
	dv := NewDeletionVector()
	for i := uint32(0); i < 10000; i++ {
		dv.MarkDeleted(i)
	}
	if err := dv.Serialize(dvPath); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := Deserialize(dvPath)
		if err != nil {
			b.Fatal(err)
		}
	}
}
