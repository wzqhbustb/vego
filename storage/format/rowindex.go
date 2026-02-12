// Copyright 2024 Vego Authors
// Licensed under the Apache License, Version 2.0

package format

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

// RowIndex Magic Number (ASCII "RIDX")
const RowIndexMagic uint32 = 0x52494458

// RowIndex represents a mapping from document IDs to row indices.
// It is stored as an independent Page in V1.1+ files.
//
// Structure:
//   - Magic (4 bytes): 0x52494458 ("RIDX")
//   - NumEntries (4 bytes): Number of ID -> Row mappings
//   - BucketCount (4 bytes): Number of hash table buckets
//   - HashTable (BucketCount * 8 bytes): Array of int64 offsets into EntryArray (-1 = empty)
//   - EntryArray (NumEntries * 16 bytes): Array of {IDHash, RowIndex} pairs
type RowIndex struct {
	// Magic number for validation
	Magic uint32

	// NumEntries is the number of ID -> Row mappings
	NumEntries int32

	// BucketCount is the number of hash table buckets
	// Should be a power of 2, typically 2x NumEntries
	BucketCount int32

	// HashTable is the hash table for O(1) lookups
	// Each entry is an offset into EntryArray (-1 means empty bucket)
	HashTable []int64

	// Entries stores the actual ID -> Row mappings
	Entries []RowIndexEntry

	// Checksum for the RowIndex data
	Checksum uint32
}

// RowIndexEntry represents a single ID -> Row mapping
type RowIndexEntry struct {
	// IDHash is a 64-bit hash of the document ID
	// Using hash allows storing any ID type (string, int, etc.)
	IDHash uint64

	// RowIndex is the 0-based row index in the table
	RowIndex int64
}

// NewRowIndex creates a new RowIndex with the given capacity
func NewRowIndex(expectedEntries int32) *RowIndex {
	// Bucket count should be power of 2 and ~2x entries for good load factor
	bucketCount := nextPowerOf2(expectedEntries * 2)
	if bucketCount < 16 {
		bucketCount = 16
	}

	hashTable := make([]int64, bucketCount)
	// Initialize all slots to -1 (empty)
	for i := range hashTable {
		hashTable[i] = -1
	}

	return &RowIndex{
		Magic:       RowIndexMagic,
		BucketCount: bucketCount,
		HashTable:   hashTable,
		Entries:     make([]RowIndexEntry, 0, expectedEntries),
	}
}

// nextPowerOf2 returns the next power of 2 >= n
func nextPowerOf2(n int32) int32 {
	if n <= 0 {
		return 1
	}
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	return n + 1
}

// hashID computes a 64-bit hash of the given ID
func hashID(id string) uint64 {
	// FNV-1a hash
	h := uint64(14695981039346656037) // FNV offset basis
	for _, c := range id {
		h ^= uint64(c)
		h *= 1099511628211 // FNV prime
	}
	return h
}

// Insert adds a new ID -> Row mapping to the RowIndex
func (r *RowIndex) Insert(id string, rowIndex int64) error {
	if r.Magic != RowIndexMagic {
		return fmt.Errorf("invalid row index magic number")
	}

	idHash := hashID(id)
	entry := RowIndexEntry{IDHash: idHash, RowIndex: rowIndex}

	// Find bucket using hash
	bucketIdx := int32(idHash % uint64(r.BucketCount))

	// Linear probing for collision resolution
	for i := int32(0); i < r.BucketCount; i++ {
		idx := (bucketIdx + i) % r.BucketCount
		slotValue := r.HashTable[idx]

		// Check if slot is empty (initialized to -1)
		if slotValue == -1 {
			// Empty slot found
			r.HashTable[idx] = int64(len(r.Entries))
			r.Entries = append(r.Entries, entry)
			r.NumEntries++

			// Check if rehash is needed (load factor > 0.75)
			if float64(r.NumEntries)/float64(r.BucketCount) > 0.75 {
				r.rehash()
			}
			return nil
		}

		// Check if same ID already exists (update)
		if slotValue >= 0 && slotValue < int64(len(r.Entries)) {
			if r.Entries[slotValue].IDHash == idHash {
				// Update existing entry
				r.Entries[slotValue].RowIndex = rowIndex
				return nil
			}
		}
	}

	// Table is full (should not happen with rehashing)
	return fmt.Errorf("hash table is full")
}

// Lookup returns the row index for the given ID, or -1 if not found
func (r *RowIndex) Lookup(id string) int64 {
	if r.Magic != RowIndexMagic || r.NumEntries == 0 {
		return -1
	}

	idHash := hashID(id)
	bucketIdx := int32(idHash % uint64(r.BucketCount))

	// Linear probing
	for i := int32(0); i < r.BucketCount; i++ {
		idx := (bucketIdx + i) % r.BucketCount
		entryIdx := r.HashTable[idx]

		// Empty slot - ID not found
		if entryIdx == -1 {
			return -1
		}

		// Check if this is the entry we're looking for
		if entryIdx >= 0 && entryIdx < int64(len(r.Entries)) {
			if r.Entries[entryIdx].IDHash == idHash {
				return r.Entries[entryIdx].RowIndex
			}
		}
	}

	return -1
}

// rehash increases the hash table size and rehashes all entries
func (r *RowIndex) rehash() {
	newBucketCount := r.BucketCount * 2
	newHashTable := make([]int64, newBucketCount)
	// Initialize with -1 (empty)
	for i := range newHashTable {
		newHashTable[i] = -1
	}

	// Rehash all entries
	for entryIdx, entry := range r.Entries {
		bucketIdx := int32(entry.IDHash % uint64(newBucketCount))

		// Linear probing
		for i := int32(0); i < newBucketCount; i++ {
			idx := (bucketIdx + i) % newBucketCount
			if newHashTable[idx] == -1 {
				newHashTable[idx] = int64(entryIdx)
				break
			}
		}
	}

	r.BucketCount = newBucketCount
	r.HashTable = newHashTable
}

// EncodedSize returns the size in bytes of the encoded RowIndex
func (r *RowIndex) EncodedSize() int {
	// Magic(4) + NumEntries(4) + BucketCount(4) + HashTable + Entries + Checksum(4)
	return 4 + 4 + 4 + len(r.HashTable)*8 + len(r.Entries)*16 + 4
}

// WriteTo writes the RowIndex to a writer
func (r *RowIndex) WriteTo(w io.Writer) (int64, error) {
	if r.Magic != RowIndexMagic {
		return 0, fmt.Errorf("invalid row index magic number")
	}

	buf := new(bytes.Buffer)

	// Write header
	binary.Write(buf, ByteOrder, r.Magic)
	binary.Write(buf, ByteOrder, r.NumEntries)
	binary.Write(buf, ByteOrder, r.BucketCount)

	// Write hash table
	for _, offset := range r.HashTable {
		binary.Write(buf, ByteOrder, offset)
	}

	// Write entries
	for _, entry := range r.Entries {
		binary.Write(buf, ByteOrder, entry.IDHash)
		binary.Write(buf, ByteOrder, entry.RowIndex)
	}

	// Calculate checksum
	data := buf.Bytes()
	r.Checksum = crc32.ChecksumIEEE(data)

	// Write checksum
	binary.Write(buf, ByteOrder, r.Checksum)

	n, err := w.Write(buf.Bytes())
	return int64(n), err
}

// ReadFrom reads the RowIndex from a reader
func (r *RowIndex) ReadFrom(rd io.Reader) (int64, error) {
	// Read header
	if err := binary.Read(rd, ByteOrder, &r.Magic); err != nil {
		return 4, fmt.Errorf("failed to read magic: %w", err)
	}
	if r.Magic != RowIndexMagic {
		return 4, fmt.Errorf("invalid row index magic: expected 0x%08X, got 0x%08X", RowIndexMagic, r.Magic)
	}

	if err := binary.Read(rd, ByteOrder, &r.NumEntries); err != nil {
		return 8, fmt.Errorf("failed to read num entries: %w", err)
	}

	if err := binary.Read(rd, ByteOrder, &r.BucketCount); err != nil {
		return 12, fmt.Errorf("failed to read bucket count: %w", err)
	}

	// Read hash table
	r.HashTable = make([]int64, r.BucketCount)
	bytesRead := int64(12)
	for i := int32(0); i < r.BucketCount; i++ {
		if err := binary.Read(rd, ByteOrder, &r.HashTable[i]); err != nil {
			return bytesRead, fmt.Errorf("failed to read hash table: %w", err)
		}
		bytesRead += 8
	}

	// Read entries
	r.Entries = make([]RowIndexEntry, r.NumEntries)
	for i := int32(0); i < r.NumEntries; i++ {
		if err := binary.Read(rd, ByteOrder, &r.Entries[i].IDHash); err != nil {
			return bytesRead, fmt.Errorf("failed to read entry id hash: %w", err)
		}
		bytesRead += 8

		if err := binary.Read(rd, ByteOrder, &r.Entries[i].RowIndex); err != nil {
			return bytesRead, fmt.Errorf("failed to read entry row index: %w", err)
		}
		bytesRead += 8
	}

	// Read checksum
	var storedChecksum uint32
	if err := binary.Read(rd, ByteOrder, &storedChecksum); err != nil {
		return bytesRead, fmt.Errorf("failed to read checksum: %w", err)
	}
	bytesRead += 4

	// Verify checksum
	// Note: We would need to re-read the data to compute the checksum
	// For now, we just store it
	r.Checksum = storedChecksum

	return bytesRead, nil
}

// ToPage converts the RowIndex to a Page for storage
func (r *RowIndex) ToPage() (*Page, error) {
	var buf bytes.Buffer
	if _, err := r.WriteTo(&buf); err != nil {
		return nil, err
	}

	page := NewPage(-1, PageTypeIndex, EncodingPlain) // ColumnIndex = -1 for RowIndex
	page.NumValues = r.NumEntries
	page.SetData(buf.Bytes(), int32(buf.Len()))

	return page, nil
}

// FromPage creates a RowIndex from a Page
func RowIndexFromPage(page *Page) (*RowIndex, error) {
	if page.Type != PageTypeIndex {
		return nil, fmt.Errorf("invalid page type: expected PageTypeIndex, got %v", page.Type)
	}

	reader := bytes.NewReader(page.Data)
	ri := &RowIndex{}
	if _, err := ri.ReadFrom(reader); err != nil {
		return nil, err
	}

	return ri, nil
}

// Validate validates the RowIndex structure
func (r *RowIndex) Validate() error {
	if r.Magic != RowIndexMagic {
		return fmt.Errorf("invalid magic number: expected 0x%08X, got 0x%08X", RowIndexMagic, r.Magic)
	}
	if r.NumEntries < 0 {
		return fmt.Errorf("invalid num entries: %d", r.NumEntries)
	}
	if r.BucketCount <= 0 || (r.BucketCount&(r.BucketCount-1)) != 0 {
		return fmt.Errorf("bucket count must be power of 2: %d", r.BucketCount)
	}
	if int32(len(r.HashTable)) != r.BucketCount {
		return fmt.Errorf("hash table size mismatch: expected %d, got %d", r.BucketCount, len(r.HashTable))
	}
	if int32(len(r.Entries)) != r.NumEntries {
		return fmt.Errorf("entries size mismatch: expected %d, got %d", r.NumEntries, len(r.Entries))
	}
	return nil
}

// Stats returns statistics about the RowIndex
func (r *RowIndex) Stats() RowIndexStats {
	stats := RowIndexStats{
		NumEntries:  r.NumEntries,
		BucketCount: r.BucketCount,
		MemoryUsage: r.EncodedSize(),
	}

	// Calculate load factor
	if r.BucketCount > 0 {
		stats.LoadFactor = float64(r.NumEntries) / float64(r.BucketCount)
	}

	// Count used buckets
	usedBuckets := int32(0)
	for _, offset := range r.HashTable {
		if offset >= 0 {
			usedBuckets++
		}
	}
	stats.UsedBuckets = usedBuckets

	return stats
}

// RowIndexStats contains statistics about a RowIndex
type RowIndexStats struct {
	NumEntries   int32
	BucketCount  int32
	UsedBuckets  int32
	LoadFactor   float64
	MemoryUsage  int // bytes
}

func (s RowIndexStats) String() string {
	return fmt.Sprintf("RowIndex{entries=%d, buckets=%d, load_factor=%.2f, memory=%d bytes}",
		s.NumEntries, s.BucketCount, s.LoadFactor, s.MemoryUsage)
}
