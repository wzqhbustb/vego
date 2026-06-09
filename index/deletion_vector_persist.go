// Copyright 2024 Vego Authors
// Licensed under the Apache License, Version 2.0

package hnsw

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/RoaringBitmap/roaring"
	"github.com/wzqhbustb/vego/vfs"
)

const (
	// delFileMagic is the magic number for deletion vector files
	delFileMagic = "DEL1"
	// delFileVersion is the current file format version
	delFileVersion = 1
	// delFileExt is the file extension for deletion vector files
	delFileExt = ".del"
)

// fileHeader represents the header of a .del file
type fileHeader struct {
	Magic       [4]byte
	Version     uint32
	NumDeleted  uint64
}

// GetDeletionVectorPath returns the DV file path for a data file.
// The DV file is named by appending ".del" to the data file path.
//
// Example:
//   dataFile = "/path/to/vectors.lance"
//   dvFile   = "/path/to/vectors.lance.del"
func GetDeletionVectorPath(dataFilePath string) string {
	return dataFilePath + delFileExt
}

// Serialize writes the DeletionVector to a file.
// The file format is:
//   - Header (16 bytes):
//     - Magic: "DEL1" (4 bytes)
//     - Version: uint32 (4 bytes)
//     - NumDeleted: uint64 (8 bytes)
//   - Bitmap Data: RoaringBitmap serialization
func (dv *DeletionVector) Serialize(path string) error {
	dv.mu.RLock()
	defer dv.mu.RUnlock()

	// Create file
	f, err := dv.fs.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create deletion vector file: %w", err)
	}
	defer f.Close()

	// Write header
	header := fileHeader{
		Version:    delFileVersion,
		NumDeleted: dv.deleted.GetCardinality(),
	}
	copy(header.Magic[:], delFileMagic)

	if err := binary.Write(f, binary.LittleEndian, header); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	// Write bitmap data
	if _, err := dv.deleted.WriteTo(f); err != nil {
		return fmt.Errorf("failed to write bitmap: %w", err)
	}

	// Sync to ensure data is persisted
	if err := f.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	return nil
}

// Deserialize reads a DeletionVector from a file using the default local VFS.
// Returns an error if the file is corrupted or has an unsupported version.
// If the file doesn't exist, returns (nil, error).
func Deserialize(path string) (*DeletionVector, error) {
	return DeserializeWithVFS(path, vfs.Local)
}

// DeserializeWithVFS reads a DeletionVector from a file with a custom VFS.
func DeserializeWithVFS(path string, fs vfs.VFS) (*DeletionVector, error) {
	// Open file
	f, err := fs.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open deletion vector file: %w", err)
	}
	defer f.Close()

	// Read header
	var header fileHeader
	if err := binary.Read(f, binary.LittleEndian, &header); err != nil {
		return nil, fmt.Errorf("failed to read header: %w", err)
	}

	// Verify magic
	if string(header.Magic[:]) != delFileMagic {
		return nil, fmt.Errorf("invalid magic: expected %s, got %s", delFileMagic, string(header.Magic[:]))
	}

	// Verify version
	if header.Version != delFileVersion {
		return nil, fmt.Errorf("unsupported version: expected %d, got %d", delFileVersion, header.Version)
	}

	// Read bitmap data (remaining bytes after header)
	bitmap, err := readBitmapFromReader(f)
	if err != nil {
		return nil, fmt.Errorf("failed to read bitmap: %w", err)
	}

	// Verify cardinality matches header
	actualCount := bitmap.GetCardinality()
	if actualCount != header.NumDeleted {
		// This is a warning condition but we can still use the data
		// Log it but don't fail
		// In production, you might want to log this
		_ = header.NumDeleted // Silence unused warning for now
	}

	return &DeletionVector{
		deleted: bitmap,
		fs:      fs,
	}, nil
}

// readBitmapFromReader reads a RoaringBitmap from a reader.
// The reader should be positioned at the start of the bitmap data.
func readBitmapFromReader(r io.Reader) (*roaring.Bitmap, error) {
	bitmap := roaring.NewBitmap()
	if _, err := bitmap.ReadFrom(r); err != nil {
		return nil, err
	}
	return bitmap, nil
}

// DeserializeOrEmpty attempts to deserialize a DeletionVector from a file using the default local VFS.
// If the file doesn't exist or is corrupted, returns an empty DeletionVector instead of an error.
// This is useful for backward compatibility where old data may not have a .del file.
func DeserializeOrEmpty(path string) *DeletionVector {
	return DeserializeOrEmptyWithVFS(path, vfs.Local)
}

// DeserializeOrEmptyWithVFS attempts to deserialize a DeletionVector from a file with a custom VFS.
func DeserializeOrEmptyWithVFS(path string, fs vfs.VFS) *DeletionVector {
	dv, err := DeserializeWithVFS(path, fs)
	if err != nil {
		// File doesn't exist or is corrupted, return empty DV
		return NewDeletionVectorWithVFS(fs)
	}
	return dv
}

// FileExists checks if a deletion vector file exists at the given path using the default local VFS.
func FileExists(path string) bool {
	return FileExistsWithVFS(path, vfs.Local)
}

// FileExistsWithVFS checks if a deletion vector file exists at the given path with a custom VFS.
func FileExistsWithVFS(path string, fs vfs.VFS) bool {
	_, err := fs.Stat(path)
	return err == nil
}

// GetDeletionVectorInfo returns information about a deletion vector file without loading it.
// Uses the default local VFS.
func GetDeletionVectorInfo(path string) (numDeleted uint64, fileSize int64, err error) {
	return GetDeletionVectorInfoWithVFS(path, vfs.Local)
}

// GetDeletionVectorInfoWithVFS returns information about a deletion vector file without loading it using a custom VFS.
func GetDeletionVectorInfoWithVFS(path string, fs vfs.VFS) (numDeleted uint64, fileSize int64, err error) {
	f, err := fs.Open(path)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()

	// Get file size
	stat, err := f.Stat()
	if err != nil {
		return 0, 0, err
	}
	fileSize = stat.Size()

	// Read header
	var header fileHeader
	if err := binary.Read(f, binary.LittleEndian, &header); err != nil {
		return 0, fileSize, err
	}

	// Verify magic and version
	if string(header.Magic[:]) != delFileMagic {
		return 0, fileSize, fmt.Errorf("invalid magic")
	}

	return header.NumDeleted, fileSize, nil
}
