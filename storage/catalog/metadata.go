package catalog

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	"github.com/wzqhbustb/vego/vfs"
)

// DocMeta stores metadata for a document (not stored in column storage).
type DocMeta struct {
	ID       string                 `json:"id"`
	RowIndex int64                  `json:"row_index"` // Row position in column storage (-1 = unset, >= 0 = valid)
	Metadata map[string]interface{} `json:"metadata"`
}

// MetadataStore is the in-memory and on-disk metadata storage.
type MetadataStore struct {
	// idHash -> DocMeta
	entries map[int64]DocMeta
	// string ID -> idHash (for quick lookup)
	idToHash map[string]int64
	path     string
	fs       vfs.VFS // filesystem abstraction; defaults to vfs.Local
	mu       sync.RWMutex
}

// NewMetadataStore creates a new metadata store for the given metadata file path using the default local VFS.
func NewMetadataStore(path string) *MetadataStore {
	return NewMetadataStoreWithVFS(path, vfs.Local)
}

// NewMetadataStoreWithVFS creates a new metadata store with a custom VFS.
func NewMetadataStoreWithVFS(path string, fs vfs.VFS) *MetadataStore {
	return &MetadataStore{
		entries:  make(map[int64]DocMeta),
		idToHash: make(map[string]int64),
		path:     path,
		fs:       fs,
	}
}

// Path returns the on-disk path of the metadata store.
func (s *MetadataStore) Path() string {
	return s.path
}

// GetByID looks up a document by its string ID.
func (s *MetadataStore) GetByID(id string) (DocMeta, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	hash, exists := s.idToHash[id]
	if !exists {
		return DocMeta{}, false
	}
	meta, exists := s.entries[hash]
	return meta, exists
}

// GetByHash looks up a document by its int64 hash.
func (s *MetadataStore) GetByHash(hash int64) (DocMeta, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	meta, exists := s.entries[hash]
	return meta, exists
}

// Put stores or updates a document metadata entry.
func (s *MetadataStore) Put(id string, hash int64, meta DocMeta) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.entries[hash] = meta
	s.idToHash[id] = hash
}

// Delete removes a document metadata entry by ID and hash.
func (s *MetadataStore) Delete(id string, hash int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.entries, hash)
	delete(s.idToHash, id)
}

// Count returns the number of documents in the metadata store.
func (s *MetadataStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.idToHash)
}

// AllEntries returns a shallow copy of all entries (idHash -> DocMeta).
// The returned map is safe to iterate without holding the store's lock,
// but the DocMeta values themselves should not be modified.
func (s *MetadataStore) AllEntries() map[int64]DocMeta {
	s.mu.RLock()
	defer s.mu.RUnlock()

	copy := make(map[int64]DocMeta, len(s.entries))
	for k, v := range s.entries {
		copy[k] = v
	}
	return copy
}

// Save persists the metadata store to disk as JSON.
func (s *MetadataStore) Save() error {
	s.mu.RLock()
	// Deep-copy maps under lock to avoid "concurrent map iteration and write"
	// during json.Encode after the lock is released.
	entriesCopy := make(map[int64]DocMeta, len(s.entries))
	for k, v := range s.entries {
		entriesCopy[k] = v
	}
	idToHashCopy := make(map[string]int64, len(s.idToHash))
	for k, v := range s.idToHash {
		idToHashCopy[k] = v
	}
	s.mu.RUnlock()

	data := struct {
		Entries  map[int64]DocMeta `json:"entries"`
		IDToHash map[string]int64  `json:"id_to_hash"`
	}{
		Entries:  entriesCopy,
		IDToHash: idToHashCopy,
	}

	file, err := s.fs.Create(s.path)
	if err != nil {
		return fmt.Errorf("create metadata file: %w", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(data); err != nil {
		return fmt.Errorf("encode metadata: %w", err)
	}

	return nil
}

// Load reads the metadata store from disk.
// If the file does not exist, the store remains empty (no error).
func (s *MetadataStore) Load() error {
	_, err := s.fs.Stat(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}

	file, err := s.fs.Open(s.path)
	if err != nil {
		return fmt.Errorf("open metadata file: %w", err)
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return fmt.Errorf("read metadata file: %w", err)
	}

	var stored struct {
		Entries  map[int64]DocMeta `json:"entries"`
		IDToHash map[string]int64  `json:"id_to_hash"`
	}

	if err := json.Unmarshal(data, &stored); err != nil {
		return fmt.Errorf("decode metadata: %w", err)
	}

	s.mu.Lock()
	s.entries = stored.Entries
	s.idToHash = stored.IDToHash
	if s.entries == nil {
		s.entries = make(map[int64]DocMeta)
	}
	if s.idToHash == nil {
		s.idToHash = make(map[string]int64)
	}
	s.mu.Unlock()

	return nil
}

// LoadWithRepair loads metadata from disk and repairs unset RowIndex values
// by looking them up from the data file. This handles backward compatibility
// for old metadata that did not store RowIndex.
//
// lookupRowIndex is called for each document with an unset or zero RowIndex.
// It should return the actual row index, or -1 if not found.
// fileExists and supportsRowIndex are used to determine whether to log warnings.
func (s *MetadataStore) LoadWithRepair(
	lookupRowIndex func(id string) int64,
	fileExists bool,
	supportsRowIndex bool,
) error {
	if err := s.Load(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for idHash, meta := range s.entries {
		// If RowIndex < 0 (unset) or == 0 (possibly old data), try to rebuild
		if meta.RowIndex < 0 || meta.RowIndex == 0 {
			if rowIdx := lookupRowIndex(meta.ID); rowIdx >= 0 {
				meta.RowIndex = rowIdx
				s.entries[idHash] = meta
			} else if meta.RowIndex < 0 {
				// Only log warning for new format (RowIndex=-1) that failed to rebuild
				// Old format (RowIndex=0) might be valid data at row 0, don't warn
				if fileExists && supportsRowIndex {
					log.Printf("[catalog] Warning: Document %s has unset RowIndex but not found in RowIndex file", meta.ID)
				}
			}
		}
	}

	return nil
}
