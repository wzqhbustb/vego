package catalog

import (
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/wzqhbustb/vego/vfs"
)

const (
	delFileMagic   = "DEL1"
	delFileVersion = 1
	delFileExt     = ".del"

	// tempFileInfix is used for atomic writes in deletion store.
	tempFileInfix = ".vego-tmp."
)

// deletionFileHeader represents the header of a .del file.
type deletionFileHeader struct {
	Magic      [4]byte
	Version    uint32
	NumDeleted uint64
}

// DeletionStore manages logical deletions using a bitmap.
// It is independent of the index package and uses the same on-disk format.
type DeletionStore struct {
	deleted *roaring.Bitmap
	fs      vfs.VFS // filesystem abstraction; defaults to vfs.Local
	mu      sync.RWMutex
}

// NewDeletionStore creates a new empty DeletionStore using the default local VFS.
func NewDeletionStore() *DeletionStore {
	return NewDeletionStoreWithVFS(vfs.Local)
}

// NewDeletionStoreWithVFS creates a new empty DeletionStore with a custom VFS.
func NewDeletionStoreWithVFS(fs vfs.VFS) *DeletionStore {
	return &DeletionStore{
		deleted: roaring.NewBitmap(),
		fs:      fs,
	}
}

// NewDeletionStoreFromBitmap creates a DeletionStore from an existing bitmap using the default local VFS.
// The bitmap is cloned to avoid shared state.
func NewDeletionStoreFromBitmap(bitmap *roaring.Bitmap) *DeletionStore {
	return NewDeletionStoreFromBitmapWithVFS(bitmap, vfs.Local)
}

// NewDeletionStoreFromBitmapWithVFS creates a DeletionStore from an existing bitmap with a custom VFS.
func NewDeletionStoreFromBitmapWithVFS(bitmap *roaring.Bitmap, fs vfs.VFS) *DeletionStore {
	if bitmap == nil {
		return NewDeletionStoreWithVFS(fs)
	}
	return &DeletionStore{
		deleted: bitmap.Clone(),
		fs:      fs,
	}
}

// MarkDeleted marks a row ID as deleted.
func (ds *DeletionStore) MarkDeleted(rowID uint32) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.deleted.Add(rowID)
}

// UnmarkDeleted unmarks a row ID.
// Returns true if the row was previously marked as deleted.
func (ds *DeletionStore) UnmarkDeleted(rowID uint32) bool {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	if !ds.deleted.Contains(rowID) {
		return false
	}
	ds.deleted.Remove(rowID)
	return true
}

// IsDeleted checks if a row ID is marked as deleted.
func (ds *DeletionStore) IsDeleted(rowID uint32) bool {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	return ds.deleted.Contains(rowID)
}

// Count returns the number of deleted rows.
func (ds *DeletionStore) Count() int {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	return int(ds.deleted.GetCardinality())
}

// IsEmpty returns true if no rows are marked as deleted.
func (ds *DeletionStore) IsEmpty() bool {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	return ds.deleted.IsEmpty()
}

// Clear removes all deletion marks.
func (ds *DeletionStore) Clear() {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	ds.deleted.Clear()
}

// Save persists the DeletionStore to a file using atomic write pattern:
// write to temp file, fsync, rename to final path.
func (ds *DeletionStore) Save(path string) error {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	tmpPath := path + tempFileInfix + fmt.Sprintf("%d-%x", time.Now().UnixNano(), rand.Int63())

	f, err := ds.fs.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("create deletion store temp file: %w", err)
	}

	header := deletionFileHeader{
		Version:    delFileVersion,
		NumDeleted: ds.deleted.GetCardinality(),
	}
	copy(header.Magic[:], delFileMagic)

	if err := binary.Write(f, binary.LittleEndian, header); err != nil {
		f.Close()
		ds.fs.Remove(tmpPath)
		return fmt.Errorf("write header: %w", err)
	}

	if _, err := ds.deleted.WriteTo(f); err != nil {
		f.Close()
		ds.fs.Remove(tmpPath)
		return fmt.Errorf("write bitmap: %w", err)
	}

	if err := f.Sync(); err != nil {
		f.Close()
		ds.fs.Remove(tmpPath)
		return fmt.Errorf("sync file: %w", err)
	}

	if err := f.Close(); err != nil {
		ds.fs.Remove(tmpPath)
		return fmt.Errorf("close file: %w", err)
	}

	// Atomic rename
	if err := ds.fs.Rename(tmpPath, path); err != nil {
		ds.fs.Remove(tmpPath)
		return fmt.Errorf("rename deletion store: %w", err)
	}

	return nil
}

// Load reads a DeletionStore from a file.
func (ds *DeletionStore) Load(path string) error {
	f, err := ds.fs.Open(path)
	if err != nil {
		return fmt.Errorf("open deletion store file: %w", err)
	}
	defer f.Close()

	var header deletionFileHeader
	if err := binary.Read(f, binary.LittleEndian, &header); err != nil {
		return fmt.Errorf("read header: %w", err)
	}

	if string(header.Magic[:]) != delFileMagic {
		return fmt.Errorf("invalid magic: expected %s, got %s", delFileMagic, string(header.Magic[:]))
	}
	if header.Version != delFileVersion {
		return fmt.Errorf("unsupported version: expected %d, got %d", delFileVersion, header.Version)
	}

	bitmap := roaring.NewBitmap()
	if _, err := bitmap.ReadFrom(f); err != nil {
		return fmt.Errorf("read bitmap: %w", err)
	}

	ds.mu.Lock()
	ds.deleted = bitmap
	ds.mu.Unlock()
	return nil
}

// LoadOrEmpty attempts to load a DeletionStore from a file using the default local VFS.
// If the file doesn't exist or is corrupted, returns an empty DeletionStore.
func LoadOrEmpty(path string) *DeletionStore {
	return LoadOrEmptyWithVFS(path, vfs.Local)
}

// LoadOrEmptyWithVFS attempts to load a DeletionStore from a file with a custom VFS.
// If the file doesn't exist or is corrupted, returns an empty DeletionStore.
func LoadOrEmptyWithVFS(path string, fs vfs.VFS) *DeletionStore {
	ds := NewDeletionStoreWithVFS(fs)
	if err := ds.Load(path); err != nil {
		// File doesn't exist or is corrupted, return empty
		return NewDeletionStoreWithVFS(fs)
	}
	return ds
}

// DeletionStorePath returns the DV file path for a data file.
func DeletionStorePath(dataFilePath string) string {
	return dataFilePath + delFileExt
}

// readBitmapFromReader reads a RoaringBitmap from a reader.
func readBitmapFromReader(r io.Reader) (*roaring.Bitmap, error) {
	bitmap := roaring.NewBitmap()
	if _, err := bitmap.ReadFrom(r); err != nil {
		return nil, err
	}
	return bitmap, nil
}
