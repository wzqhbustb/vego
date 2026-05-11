package vego

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/wzqhbustb/vego/core"
	"github.com/wzqhbustb/vego/storage/catalog"
	"github.com/wzqhbustb/vego/storage/column"
	"github.com/wzqhbustb/vego/storage/encoding"
	"github.com/wzqhbustb/vego/storage/format"
)

const (
	// dataFileName is the Lance format data file for vectors
	dataFileName = "vectors.lance"
	// metaFileName stores ID mapping and metadata
	metaFileName = "metadata.json"
	// maxBufferSize is the maximum documents to buffer before flush
	maxBufferSize = 1000
	// defaultCompactionThreshold is the deletion rate threshold for automatic compaction
	defaultCompactionThreshold = 0.3
)

// DocumentStorage handles persistence of documents using columnar storage.
// Vectors are stored in Lance format for efficient access,
// while ID and metadata are stored separately in JSON.
type DocumentStorage struct {
	path      string
	dimension int

	// Column storage for vectors
	factory *encoding.EncoderFactory

	// Write buffering
	writeBuffer []*Document
	bufferIndex map[string]int // id -> index in writeBuffer for O(1) lookup
	bufferSize  int
	maxBuffer   int

	// Snapshot manages metadata and deletion state.
	snapshot *catalog.Snapshot

	// BlockCache for page-level caching (optional, shared across storages)
	blockCache *format.BlockCache

	// Format version for column storage (V1_0, V1_1, V1_2)
	version format.VersionPolicy

	// Cached parsed RowIndex for O(1) ID→row lookup without per-call
	// LoadRowIndex overhead. The BlockCache already caches the raw page,
	// but each new RowIndexReader still parses the 200KB binary page.
	// By caching the parsed *format.RowIndex, subsequent Get calls skip
	// the LoadRowIndex cost entirely (1-2ms savings per call). Invalidated
	// on flush/rewrite/close.
	cachedRowIndex *format.RowIndex
	rowIdxMu       sync.Mutex
	rowIdxTried    bool // true once ensureRowIndex has been attempted

	// State tracking
	dirty  bool
	mu     sync.RWMutex
	closed bool
}

// StorageStats contains statistics about the storage
type StorageStats struct {
	DocumentCount int
	BufferSize    int
	DataFileSize  int64
	MetaFileSize  int64
	FormatVersion string  // File format version (e.g., "1.2")
	DeletedCount  int     // Number of logically deleted documents
	DeletionRate  float64 // Deletion rate (0.0 - 1.0)
}

// cleanupTempFiles removes stale temporary files from previous crashed writes.
// Only removes temp files for the data file (vectors.lance.tmp.*), not other .tmp.* files.
func cleanupTempFiles(dir string) {
	// Use precise pattern: only match data file temp files (vectors.lance.tmp.*)
	// Avoid matching user files like backup.tmp.bak or config.tmp.json
	pattern := filepath.Join(dir, dataFileName+".tmp.*")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return // Glob error, skip cleanup
	}
	for _, m := range matches {
		if err := os.Remove(m); err != nil {
			log.Printf("[Storage] Warning: failed to remove temp file %s: %v", m, err)
		} else {
			log.Printf("[Storage] Cleaned up temp file: %s", m)
		}
	}
}

// NewDocumentStorage creates a new document storage instance.
// Optionally accepts a shared BlockCache for page-level caching.
// Optionally accepts a format version (defaults to V1_2 for RowIndex + BlockCache support).
func NewDocumentStorage(path string, dimension int, cache ...*format.BlockCache) (*DocumentStorage, error) {
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, fmt.Errorf("create storage directory: %w", err)
	}

	// Clean up any stale temp files from previous crashes
	cleanupTempFiles(path)

	s := &DocumentStorage{
		path:           path,
		dimension:      dimension,
		factory:        encoding.NewEncoderFactory(3),
		snapshot:       catalog.NewSnapshot(path, format.V1_2),
		bufferIndex:    make(map[string]int),
		maxBuffer:      maxBufferSize,
		version:        format.V1_2, // Default to V1.2 for RowIndex + BlockCache support
	}

	// Optional BlockCache for shared caching across storages
	if len(cache) > 0 && cache[0] != nil {
		s.blockCache = cache[0]
	}

	// Try to load existing data
	if err := s.load(); err != nil {
		return nil, fmt.Errorf("load existing data: %w", err)
	}

	// Try to load existing deletion vector
	s.snapshot.LoadDeletionStore()

	return s, nil
}

// createSchema creates the Arrow schema for vector storage.
func (s *DocumentStorage) createSchema() *core.Schema {
	return s.snapshot.Schema(s.dimension)
}

// Put stores a single document.
func (s *DocumentStorage) Put(doc *Document) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return fmt.Errorf("storage is closed")
	}

	// Set timestamp if not already set
	if doc.Timestamp.IsZero() {
		doc.Timestamp = time.Now()
	}

	// Check if document already exists in metadata store
	_, exists := s.snapshot.MetaStore.GetByID(doc.ID)

	if exists {
		// Update existing - remove old entry first
		if err := s.deleteFromStorage(doc.ID); err != nil {
			return fmt.Errorf("delete old document: %w", err)
		}
	}

	// Remove from buffer if present (for updates)
	if idx, exists := s.bufferIndex[doc.ID]; exists {
		s.writeBuffer = append(s.writeBuffer[:idx], s.writeBuffer[idx+1:]...)
		s.bufferSize--
		delete(s.bufferIndex, doc.ID)
		for i := idx; i < len(s.writeBuffer); i++ {
			s.bufferIndex[s.writeBuffer[i].ID] = i
		}
	}

	// Add to buffer
	s.writeBuffer = append(s.writeBuffer, doc.Clone())
	s.bufferIndex[doc.ID] = len(s.writeBuffer) - 1
	s.bufferSize++
	s.dirty = true

	// Flush if buffer is full
	if s.bufferSize >= s.maxBuffer {
		return s.flush()
	}

	return nil
}

// PutBatch stores multiple documents efficiently.
func (s *DocumentStorage) PutBatch(docs []*Document) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return fmt.Errorf("storage is closed")
	}

	for _, doc := range docs {
		// Set timestamp if not already set
		if doc.Timestamp.IsZero() {
			doc.Timestamp = time.Now()
		}

		_, exists := s.snapshot.MetaStore.GetByID(doc.ID)

		if exists {
			if err := s.deleteFromStorage(doc.ID); err != nil {
				return fmt.Errorf("delete old document %s: %w", doc.ID, err)
			}
		}

		// Remove from buffer if present (for updates)
		if idx, exists := s.bufferIndex[doc.ID]; exists {
			s.writeBuffer = append(s.writeBuffer[:idx], s.writeBuffer[idx+1:]...)
			s.bufferSize--
			delete(s.bufferIndex, doc.ID)
			// Rebuild indices for elements shifted left.
			for i := idx; i < len(s.writeBuffer); i++ {
				s.bufferIndex[s.writeBuffer[i].ID] = i
			}
		}

		s.writeBuffer = append(s.writeBuffer, doc.Clone())
		s.bufferIndex[doc.ID] = len(s.writeBuffer) - 1
		s.bufferSize++
	}

	s.dirty = true

	if s.bufferSize >= s.maxBuffer {
		return s.flush()
	}

	return nil
}

// Get retrieves a document by ID.
// For V1.1+ files with RowIndex, uses O(1) lookup.
// Falls back to full scan for V1.0 files or when RowIndex is not available.
func (s *DocumentStorage) Get(id string) (*Document, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, fmt.Errorf("storage is closed")
	}

	// Check buffer first (most recent data)
	if idx, exists := s.bufferIndex[id]; exists {
		return s.writeBuffer[idx].Clone(), nil
	}

	// Try RowIndex optimized path (V1.1+)
	doc, usedRowIndex, err := s.tryReadByRowIndex(id)
	if err == nil && doc != nil {
		// Successfully found via RowIndex
		return doc, nil
	}
	if err != nil && err != ErrDocumentNotFound {
		return nil, err
	}
	// If RowIndex was used but document not found, return error
	if usedRowIndex {
		return nil, ErrDocumentNotFound
	}
	// Otherwise, continue to fallback path (file may not exist or doesn't have RowIndex)

	// Fallback: Check metadata store + full scan (V1.0 style)
	meta, exists := s.snapshot.MetaStore.GetByID(id)
	if !exists {
		return nil, ErrDocumentNotFound
	}
	idHash := catalog.HashID(id)

	// Read vector from column storage
	vector, timestamp, err := s.readVectorByHash(idHash)
	if err != nil {
		return nil, err
	}

	return &Document{
		ID:        meta.ID,
		Vector:    vector,
		Metadata:  meta.Metadata,
		Timestamp: time.Unix(0, timestamp),
	}, nil
}

// GetMetadataOnly returns the metadata map for the given document ID
// without reading the vector from column storage. It checks the write
// buffer first, then falls back to the in-memory metadata store.
//
// This is an O(1) pure-memory operation, suitable for high-frequency
// filter checks during HNSW search. The returned map is an internal
// reference; callers must not modify it.
//
// INVARIANT: Metadata maps in writeBuffer and metaStore are never
// modified in-place. Updates create new maps (e.g. shallowCopyMap in
// memory.Update). This ensures the returned reference is safe for
// concurrent read-only access without holding s.mu.
func (s *DocumentStorage) GetMetadataOnly(id string) (map[string]interface{}, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check buffer first (most recent data)
	if idx, exists := s.bufferIndex[id]; exists {
		return s.writeBuffer[idx].Metadata, true
	}

	// Check metadata store
	meta, exists := s.snapshot.MetaStore.GetByID(id)
	if !exists {
		return nil, false
	}
	return meta.Metadata, true
}

// CheckVisibility returns the metadata for the given document ID if the
// document is visible (not deleted). It combines the deletion check and
// metadata lookup into a single call to reduce lock overhead.
//
// Returns (metadata, true) if the document exists and is not deleted.
// Returns (nil, false) if the document is deleted or does not exist.
// The returned metadata map is an internal reference; callers must not
// modify it.
//
// INVARIANT: Metadata maps are never modified in-place (new maps are
// created on update). This guarantees the returned reference is safe for
// concurrent read-only access after CheckVisibility releases s.mu.
func (s *DocumentStorage) CheckVisibility(id string) (map[string]interface{}, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, false
	}

	// Check buffer first (most recent data).
	// No DV check needed: MarkDeleted already removes from buffer rather
	// than setting a DV entry, and Put/update clears the metaStore entry
	// for the old version (via deleteFromStorage). A document in the
	// buffer therefore can never have a corresponding DV mark — the two
	// deletion paths (buffer removal vs DV marking) are mutually exclusive.
	if idx, exists := s.bufferIndex[id]; exists {
		return s.writeBuffer[idx].Metadata, true
	}

	// Not in buffer: check metadata store + deletion vector.
	meta, exists := s.snapshot.MetaStore.GetByID(id)
	if !exists {
		return nil, false
	}
	if s.snapshot.DeletionStore.IsDeleted(uint32(meta.RowIndex)) {
		return nil, false
	}
	return meta.Metadata, true
}

// GetBatch retrieves multiple documents by IDs.
func (s *DocumentStorage) GetBatch(ids []string) (map[string]*Document, error) {
	results := make(map[string]*Document, len(ids))

	for _, id := range ids {
		doc, err := s.Get(id)
		if err != nil {
			continue // Skip not found
		}
		results[id] = doc
	}

	return results, nil
}

// Delete removes a document by ID.
func (s *DocumentStorage) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return fmt.Errorf("storage is closed")
	}

	// Remove from buffer if present
	if idx, exists := s.bufferIndex[id]; exists {
		s.writeBuffer = append(s.writeBuffer[:idx], s.writeBuffer[idx+1:]...)
		s.bufferSize--
		delete(s.bufferIndex, id)
		for i := idx; i < len(s.writeBuffer); i++ {
			s.bufferIndex[s.writeBuffer[i].ID] = i
		}
		return nil
	}

	return s.deleteFromStorage(id)
}

// deleteFromStorage removes a document from storage.
func (s *DocumentStorage) deleteFromStorage(id string) error {
	idHash := catalog.HashID(id)

	s.snapshot.MetaStore.Delete(id, idHash)

	s.dirty = true

	// Note: We don't immediately rewrite the column storage file.
	// The deleted document will be filtered out on next read.
	// A background compaction process could clean this up periodically.

	return s.snapshot.MetaStore.Save()
}

// MarkDeleted marks a document as deleted using logical deletion.
// The document is marked via DeletionVector rather than being physically removed.
// This enables efficient deletion without rewriting the entire storage file.
func (s *DocumentStorage) MarkDeleted(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return fmt.Errorf("storage is closed")
	}

	// Check if document is in write buffer (not yet flushed)
	if idx, exists := s.bufferIndex[id]; exists {
		// Remove from buffer - document never makes it to disk
		s.writeBuffer = append(s.writeBuffer[:idx], s.writeBuffer[idx+1:]...)
		s.bufferSize--
		delete(s.bufferIndex, id)
		for i := idx; i < len(s.writeBuffer); i++ {
			s.bufferIndex[s.writeBuffer[i].ID] = i
		}
		s.dirty = true
		return nil
	}

	// Document is in storage, use DV to mark as deleted
	rowID, exists := s.getRowID(id)
	if !exists {
		return ErrDocumentNotFound
	}

	s.snapshot.DeletionStore.MarkDeleted(uint32(rowID))
	s.dirty = true
	return nil
}

// IsDeleted checks if a document is marked as deleted.
func (s *DocumentStorage) IsDeleted(id string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.isDeletedLocked(id)
}

// isDeletedLocked checks if a document is deleted without acquiring s.mu.
// The caller must hold s.mu.RLock or s.mu.Lock.
func (s *DocumentStorage) isDeletedLocked(id string) bool {
	if s.closed {
		return false
	}

	rowID, exists := s.getRowID(id)
	if !exists {
		return false
	}

	return s.snapshot.DeletionStore.IsDeleted(uint32(rowID))
}

// IsDeletedByRowID checks if a row is deleted directly by rowID.
// This is more efficient than IsDeleted when the rowID is already known.
func (s *DocumentStorage) IsDeletedByRowID(rowID int64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return false
	}

	return s.snapshot.DeletionStore.IsDeleted(uint32(rowID))
}

// getRowID returns the row index for a document ID.
// The row index is stored in the document metadata and corresponds to
// the position in the column storage file.
func (s *DocumentStorage) getRowID(id string) (int64, bool) {
	meta, exists := s.snapshot.MetaStore.GetByID(id)
	if !exists {
		// Caller MUST check the bool return value; the int64 is undefined
		// when bool is false. Returning 0 instead of -1 avoids the uint32
		// overflow hazard (0xFFFFFFFF) if a caller ignores the bool.
		return 0, false
	}

	return meta.RowIndex, true
}

// GetDeletionStats returns statistics about deletions.
func (s *DocumentStorage) GetDeletionStats() (deletedCount int, totalCount int, deletionRate float64) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	totalCount = s.snapshot.MetaStore.Count() + s.bufferSize

	deletedCount = s.snapshot.DeletionStore.Count()

	if totalCount > 0 {
		deletionRate = float64(deletedCount) / float64(totalCount)
	}

	return deletedCount, totalCount, deletionRate
}

// ClearDeletionVector clears all deletion marks.
// This should be called after compaction when deleted rows are physically removed.
func (s *DocumentStorage) ClearDeletionVector() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.snapshot.DeletionStore.Clear()
	s.dirty = true
}

// saveDeletionVector persists the DeletionStore to disk.
func (s *DocumentStorage) saveDeletionVector() error {
	return s.snapshot.SaveDeletionStore()
}

// GetAllValidDocuments returns all documents that are not marked as deleted.
// This is used during compaction to rebuild the storage without deleted documents.
//
// Note: This loads all documents into memory. For large datasets during
// Compact operations, this is acceptable since Compact is an infrequent,
// heavy-weight operation (rebuilds entire storage + HNSW index). Consider
// using a streaming approach in the future if memory becomes a concern.
func (s *DocumentStorage) GetAllValidDocuments() ([]*Document, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, fmt.Errorf("storage is closed")
	}

	// Check if data file exists
	dataFile := filepath.Join(s.path, dataFileName)
	if _, err := os.Stat(dataFile); os.IsNotExist(err) {
		// No data file yet, return empty slice
		return []*Document{}, nil
	}

	// Read all documents from storage
	docs, err := s.readAllDocuments()
	if err != nil {
		return nil, err
	}

	// Include buffered documents (not yet flushed to disk)
	for _, bufDoc := range s.writeBuffer {
		docs = append(docs, bufDoc)
	}

	// Filter out deleted documents.  Deduplicate by ID so that a
	// buffered update overwrites its on-disk predecessor.
	seen := make(map[string]*Document, len(docs))
	for _, doc := range docs {
		if s.isDeletedLocked(doc.ID) {
			continue
		}
		if existing, ok := seen[doc.ID]; !ok || doc.Timestamp.After(existing.Timestamp) {
			seen[doc.ID] = doc
		}
	}

	validDocs := make([]*Document, 0, len(seen))
	for _, doc := range seen {
		validDocs = append(validDocs, doc)
	}

	return validDocs, nil
}

// ForEach iterates over all valid (non-deleted) documents, including both
// buffered (not yet flushed) and persisted documents. The callback receives
// each document; returning false stops iteration early.
//
// Documents passed to the callback are cloned before delivery, but Metadata
// values are shallow-copied (slices, maps, and pointers inside Metadata are
// shared with the original). Do not mutate nested Metadata values.
//
// WARNING: The callback executes while storage's read lock is held.
// Long-running callbacks will block all writes to the storage.
func (s *DocumentStorage) ForEach(ctx context.Context, fn func(*Document) bool) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return fmt.Errorf("storage is closed")
	}

	// Step 1: Iterate buffered documents first (most recent).
	// No deletion check needed: buffer documents are always visible
	// (MarkDeleted removes from buffer rather than setting a DV entry,
	// so a document in the buffer can never be deleted).
	bufferedIDs := make(map[string]struct{}, len(s.writeBuffer))
	for _, doc := range s.writeBuffer {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		bufferedIDs[doc.ID] = struct{}{}
		if !fn(doc.Clone()) {
			return nil
		}
	}

	// Step 2: Iterate persisted documents, skipping those already seen in buffer
	dataFile := filepath.Join(s.path, dataFileName)
	if _, err := os.Stat(dataFile); os.IsNotExist(err) {
		return nil
	}

	docs, err := s.readAllDocuments()
	if err != nil {
		return err
	}

	for _, doc := range docs {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if s.isDeletedLocked(doc.ID) {
			continue
		}
		if _, inBuffer := bufferedIDs[doc.ID]; inBuffer {
			continue
		}
		if !fn(doc.Clone()) {
			break
		}
	}
	return nil
}

// Flush writes all buffered documents to storage.
func (s *DocumentStorage) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.flush()
}

// flush is the internal flush implementation (must hold lock)
func (s *DocumentStorage) flush() error {
	// Save deletion vector even if buffer is empty (DV might have been modified)
	if s.bufferSize == 0 {
		if s.dirty {
			if err := s.saveDeletionVector(); err != nil {
				return fmt.Errorf("save deletion vector: %w", err)
			}
			s.dirty = false
		}
		return nil
	}

	// Check for version upgrade before writing
	dataFile := filepath.Join(s.path, dataFileName)
	oldVersion := s.version // Default to current configured version
	if fileVer, err := s.getFileVersion(); err == nil {
		oldVersion = fileVer
	}
	
	// Detect version upgrade
	if oldVersion != s.version {
		// File will be upgraded to new version
		// This is expected behavior when configuration changes
		_ = oldVersion // Silence unused warning in production
	}

	// Invalidate cache before reading existing data
	// This ensures we read the latest data from disk, not stale cache
	if s.blockCache != nil {
		cacheKey := column.GenerateCacheKey(dataFile)
		s.blockCache.InvalidateByPrefix(cacheKey)
	}

	// Read existing vectors if file exists
	var existingDocs []*Document
	if _, err := os.Stat(dataFile); err == nil {
		docs, err := s.readAllDocuments()
		if err != nil {
			return fmt.Errorf("read existing documents: %w", err)
		}
		existingDocs = docs
	}

	// Add buffered documents
	allDocs := append(existingDocs, s.writeBuffer...)

	// Rewrite storage using configured version (may upgrade old files)
	if err := s.rewriteStorage(allDocs); err != nil {
		return fmt.Errorf("rewrite storage: %w", err)
	}

	// Invalidate cache again after writing
	if s.blockCache != nil {
		cacheKey := column.GenerateCacheKey(dataFile)
		s.blockCache.InvalidateByPrefix(cacheKey)
	}

	// Invalidate cached RowIndex — the file was rewritten,
	// so row positions have changed.
	s.rowIdxMu.Lock()
	s.cachedRowIndex = nil
	s.rowIdxTried = false
	s.rowIdxMu.Unlock()

	// Clear buffer
	s.writeBuffer = s.writeBuffer[:0]
	s.bufferIndex = make(map[string]int)
	s.bufferSize = 0

	// Save deletion vector
	if err := s.saveDeletionVector(); err != nil {
		return fmt.Errorf("save deletion vector: %w", err)
	}

	s.dirty = false

	return nil
}

// Rewrite rewrites the entire storage with the given documents.
// This is used during compaction to remove deleted documents and optimize storage layout.
// All existing data will be replaced by the provided documents.
func (s *DocumentStorage) Rewrite(docs []*Document) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return fmt.Errorf("storage is closed")
	}

	return s.rewriteStorage(docs)
}

// rewriteStorage writes all documents to column storage and metadata store.
// This is called during Flush and potentially during compaction.
func (s *DocumentStorage) rewriteStorage(docs []*Document) error {

	// Deduplicate by ID (last write wins based on timestamp)
	// Use map to track the latest document for each ID
	docMap := make(map[string]*Document)
	for _, doc := range docs {
		existing, exists := docMap[doc.ID]
		if !exists || doc.Timestamp.After(existing.Timestamp) {
			docMap[doc.ID] = doc
		}
	}

	// Convert map to slice while preserving original order
	// This ensures RowIndex assignment is deterministic
	uniqueDocs := make([]*Document, 0, len(docMap))
	seen := make(map[string]bool)
	for _, doc := range docs {
		if latest, exists := docMap[doc.ID]; exists && !seen[doc.ID] {
			uniqueDocs = append(uniqueDocs, latest)
			seen[doc.ID] = true
		}
	}

	// Write column storage first (to establish row indices)
	if err := s.writeColumnStorage(uniqueDocs); err != nil {
		return fmt.Errorf("write column storage: %w", err)
	}

	// Update metadata store with row indices
	// The row index corresponds to the position in the written column storage
	for i, doc := range uniqueDocs {
		idHash := catalog.HashID(doc.ID)
		s.snapshot.MetaStore.Put(doc.ID, idHash, catalog.DocMeta{
			ID:       doc.ID,
			RowIndex: int64(i),
			Metadata: doc.Metadata,
		})
	}

	// Save metadata
	if err := s.snapshot.MetaStore.Save(); err != nil {
		return fmt.Errorf("save metadata: %w", err)
	}

	// Invalidate cached RowIndex — file was rewritten, row positions changed.
	s.rowIdxMu.Lock()
	s.cachedRowIndex = nil
	s.rowIdxTried = false
	s.rowIdxMu.Unlock()

	return nil
}

// writeColumnStorage writes vectors to columnar format with RowIndex support.
// This method uses atomic write pattern: write to temp file, fsync, rename.
// On failure, original data file remains intact.
func (s *DocumentStorage) writeColumnStorage(docs []*Document) error {
	if len(docs) == 0 {
		return nil
	}

	dataFile := filepath.Join(s.path, dataFileName)
	// Use timestamp to avoid conflicts with stale temp files
	tempFile := dataFile + ".tmp." + strconv.FormatInt(time.Now().UnixNano(), 10)
	schema := s.createSchema()

	// Step 1: Write to temporary file
	if err := s.doWriteColumnStorage(tempFile, schema, docs); err != nil {
		os.Remove(tempFile) // Best effort cleanup
		return fmt.Errorf("write temp file: %w", err)
	}

	// Step 2: Fsync temp file to ensure data is on disk
	if err := fsyncFile(tempFile); err != nil {
		os.Remove(tempFile) // Best effort cleanup
		return fmt.Errorf("fsync temp file: %w", err)
	}

	// Step 3: Atomic rename (POSIX guarantee)
	if err := os.Rename(tempFile, dataFile); err != nil {
		os.Remove(tempFile) // Best effort cleanup
		return fmt.Errorf("rename temp file: %w", err)
	}

	// Step 4: Fsync directory to ensure rename is persisted
	if err := fsyncDir(s.path); err != nil {
		// Don't fail here, data is already safe
		log.Printf("[Storage] Warning: fsync directory failed: %v", err)
	}

	return nil
}

// doWriteColumnStorage performs the actual write operation to the specified file.
func (s *DocumentStorage) doWriteColumnStorage(filePath string, schema *core.Schema, docs []*Document) error {
	// Use RowIndexWriter for V1.1+ format support
	writer, err := column.NewRowIndexWriter(filePath, schema, s.version, s.factory)
	if err != nil {
		return fmt.Errorf("create row index writer: %w", err)
	}

	// Configure BlockCache if available (V1.2+)
	if s.blockCache != nil {
		writer.SetBlockSize(format.DefaultBlockSize)
	}

	// Build arrays
	idBuilder := core.NewInt64Builder()
	vectorBuilder := core.NewFixedSizeListBuilder(
		core.FixedSizeListOf(core.PrimFloat32(), s.dimension).(*core.FixedSizeListType),
	)
	timestampBuilder := core.NewInt64Builder()

	// Populate builders
	for _, doc := range docs {
		idBuilder.Append(catalog.HashID(doc.ID))
		vectorBuilder.AppendValues(doc.Vector)
		timestampBuilder.Append(doc.Timestamp.UnixNano())
	}

	// Create arrays
	idArray := idBuilder.NewArray()
	vectorArray := vectorBuilder.NewArray()
	timestampArray := timestampBuilder.NewArray()

	// Create record batch
	batch, err := core.NewRecordBatch(schema, len(docs), []core.Array{
		idArray, vectorArray, timestampArray,
	})
	if err != nil {
		writer.Close()
		return fmt.Errorf("create record batch: %w", err)
	}

	// Write batch
	if err := writer.WriteRecordBatch(batch); err != nil {
		writer.Close()
		return fmt.Errorf("write record batch: %w", err)
	}

	// Build RowIndex: ID -> Row mapping
	for i, doc := range docs {
		if err := writer.AddRowID(doc.ID, int64(i)); err != nil {
			writer.Close()
			return fmt.Errorf("add row ID for document %s: %w", doc.ID, err)
		}
	}

	// Close writer to flush data and write RowIndex page
	if err := writer.Close(); err != nil {
		return fmt.Errorf("close writer: %w", err)
	}

	return nil
}

// fsyncFile performs fsync on a file to ensure data is persisted to disk.
func fsyncFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}

// fsyncDir performs fsync on a directory to ensure metadata changes are persisted.
// On Windows, directory fsync may not be supported and is silently ignored.
func fsyncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	
	err = dir.Sync()
	// Windows does not support directory sync, ignore EINVAL
	if err != nil && !errors.Is(err, syscall.EINVAL) {
		return err
	}
	return nil
}

// readAllDocuments reads all documents from storage.
func (s *DocumentStorage) readAllDocuments() ([]*Document, error) {
	dataFile := filepath.Join(s.path, dataFileName)
	
	// Use BlockCache-aware reader if cache is configured
	var reader *column.Reader
	var err error
	if s.blockCache != nil {
		reader, err = column.NewReaderWithCache(dataFile, s.blockCache)
	} else {
		reader, err = column.NewReader(dataFile)
	}
	if err != nil {
		return nil, fmt.Errorf("open reader: %w", err)
	}
	defer reader.Close()

	batch, err := reader.ReadRecordBatch()
	if err != nil {
		return nil, fmt.Errorf("read record batch: %w", err)
	}

	if batch.NumRows() == 0 {
		return []*Document{}, nil
	}

	// Extract columns
	idHashArray := batch.Column(0).(*core.Int64Array)
	vectorArray := batch.Column(1).(*core.FixedSizeListArray)
	timestampArray := batch.Column(2).(*core.Int64Array)

	// Get metadata snapshot for lookups
	entries := s.snapshot.MetaStore.AllEntries()

	// Use a map to deduplicate by hash (last write wins)
	// This handles the case where updates create duplicate entries
	docMap := make(map[int64]*Document)
	vectorValues := vectorArray.Values().(*core.Float32Array).Values()

	for i := 0; i < batch.NumRows(); i++ {
		idHash := idHashArray.Value(i)
		
		// Skip if not in metadata (deleted)
		meta, exists := entries[idHash]
		if !exists {
			continue
		}

		// Extract vector
		start := i * s.dimension
		end := start + s.dimension
		vector := make([]float32, s.dimension)
		copy(vector, vectorValues[start:end])

		// Last write wins - later entries overwrite earlier ones
		docMap[idHash] = &Document{
			ID:        meta.ID,
			Vector:    vector,
			Metadata:  meta.Metadata,
			Timestamp: time.Unix(0, timestampArray.Value(i)),
		}
	}

	// Convert map to slice
	docs := make([]*Document, 0, len(docMap))
	for _, doc := range docMap {
		docs = append(docs, doc)
	}

	return docs, nil
}

// readVectorByHash reads a vector by its ID hash.
// Returns the most recent version if multiple entries exist.
func (s *DocumentStorage) readVectorByHash(idHash int64) ([]float32, int64, error) {
	docs, err := s.readAllDocuments()
	if err != nil {
		return nil, 0, err
	}

	// Find the most recent version for this hash
	var latestDoc *Document
	for _, doc := range docs {
		if catalog.HashID(doc.ID) == idHash {
			if latestDoc == nil || doc.Timestamp.After(latestDoc.Timestamp) {
				latestDoc = doc
			}
		}
	}

	if latestDoc != nil {
		return latestDoc.Vector, latestDoc.Timestamp.UnixNano(), nil
	}

	return nil, 0, fmt.Errorf("vector not found for hash: %d", idHash)
}

// ensureRowIndex loads and caches the parsed RowIndex for O(1) ID→row
// lookup. Called once on the first Get that reaches tryReadByRowIndex.
// Must be called with s.mu at least RLock'd.
func (s *DocumentStorage) ensureRowIndex() {
	s.rowIdxMu.Lock()
	defer s.rowIdxMu.Unlock()

	if s.cachedRowIndex != nil || s.rowIdxTried {
		return
	}
	s.rowIdxTried = true

	if s.closed {
		return
	}

	dataFile := filepath.Join(s.path, dataFileName)
	if _, err := os.Stat(dataFile); err != nil {
		return
	}

	// Quick-reject: configured version lacks RowIndex support.
	if !s.version.HasFeature(format.FeatureRowIndex) {
		return
	}

	var reader *column.RowIndexReader
	var err error
	if s.blockCache != nil {
		reader, err = column.NewRowIndexReaderWithCache(dataFile, s.blockCache)
	} else {
		reader, err = column.NewRowIndexReader(dataFile)
	}
	if err != nil {
		return
	}
	defer reader.Close()

	if !reader.HasRowIndex() {
		return
	}

	// Defensive: confirm that the file's actual version matches the
	// configured version. In normal operation these always agree; this
	// check guards against a misconfigured or corrupt file.
	if !reader.GetVersion().HasFeature(format.FeatureRowIndex) {
		return
	}

	if err := reader.LoadRowIndex(); err != nil {
		return
	}

	s.cachedRowIndex = reader.GetRowIndex()
}

// tryReadByRowIndex attempts to read a document using RowIndex.
// Uses a cached parsed RowIndex for O(1) purely in-memory ID→row lookup,
// avoiding the expensive LoadRowIndex (200KB page parse) on every call.
// Returns (doc, usedRowIndex, error). If RowIndex is not available, returns (nil, false, nil).
func (s *DocumentStorage) tryReadByRowIndex(id string) (*Document, bool, error) {
	// Load cached RowIndex once per storage lifecycle
	s.ensureRowIndex()

	// O(1) pure-memory lookup on the cached parsed RowIndex
	s.rowIdxMu.Lock()
	ri := s.cachedRowIndex
	s.rowIdxMu.Unlock()

	if ri == nil {
		return nil, false, nil // RowIndex not available
	}

	rowIdx := ri.Lookup(id)
	if rowIdx == -1 {
		return nil, true, ErrDocumentNotFound
	}

	// Create a lightweight reader for ReadRowAt only.
	// File open + footer read is microseconds; the expensive
	// LoadRowIndex is avoided via the cached RowIndex above.
	dataFile := filepath.Join(s.path, dataFileName)
	var reader *column.RowIndexReader
	var err error
	if s.blockCache != nil {
		reader, err = column.NewRowIndexReaderWithCache(dataFile, s.blockCache)
	} else {
		reader, err = column.NewRowIndexReader(dataFile)
	}
	if err != nil {
		return nil, false, nil
	}
	defer reader.Close()

	// Read the specific row using O(1) random access
	rowValues, err := reader.ReadRowAt(rowIdx)
	if err != nil {
		return nil, true, fmt.Errorf("read row %d: %w", rowIdx, err)
	}

	// Extract values: [id_hash, vector, timestamp]
	if len(rowValues) != 3 {
		return nil, true, fmt.Errorf("expected 3 columns, got %d", len(rowValues))
	}

	idHash, ok := rowValues[0].(int64)
	if !ok {
		return nil, true, fmt.Errorf("invalid id_hash type: %T", rowValues[0])
	}
	vector, ok := rowValues[1].([]float32)
	if !ok {
		return nil, true, fmt.Errorf("invalid vector type: %T", rowValues[1])
	}
	timestamp, ok := rowValues[2].(int64)
	if !ok {
		return nil, true, fmt.Errorf("invalid timestamp type: %T", rowValues[2])
	}

	// Get metadata
	meta, exists := s.snapshot.MetaStore.GetByHash(idHash)
	if !exists {
		return nil, true, ErrDocumentNotFound
	}

	return &Document{
		ID:        meta.ID,
		Vector:    vector,
		Metadata:  meta.Metadata,
		Timestamp: time.Unix(0, timestamp),
	}, true, nil
}



// lookupRowIndexFromFile looks up the row index for a document ID from the data file.
// This is used for backward compatibility when loading old metadata without RowIndex.
// Returns -1 if the ID is not found or the file doesn't exist.
func (s *DocumentStorage) lookupRowIndexFromFile(id string) int64 {
	dataFile := filepath.Join(s.path, dataFileName)

	// Check if file exists
	if _, err := os.Stat(dataFile); err != nil {
		return -1
	}

	// Check if file supports RowIndex
	if !s.supportsRowIndex() {
		return -1
	}

	// Open RowIndexReader
	var reader *column.RowIndexReader
	var err error
	if s.blockCache != nil {
		reader, err = column.NewRowIndexReaderWithCache(dataFile, s.blockCache)
	} else {
		reader, err = column.NewRowIndexReader(dataFile)
	}
	if err != nil {
		return -1
	}
	defer reader.Close()

	// Check if file has RowIndex
	if !reader.HasRowIndex() {
		return -1
	}

	// Lookup row index
	rowIdx, err := reader.LookupRowID(id)
	if err != nil {
		return -1
	}

	return rowIdx
}

// load loads existing data.
func (s *DocumentStorage) load() error {
	dataFile := filepath.Join(s.path, dataFileName)
	fileExists := false
	if _, err := os.Stat(dataFile); err == nil {
		fileExists = true
	}
	return s.snapshot.MetaStore.LoadWithRepair(
		s.lookupRowIndexFromFile,
		fileExists,
		s.supportsRowIndex(),
	)
}

// Stats returns statistics about the storage.
func (s *DocumentStorage) Stats() StorageStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	docCount := s.snapshot.MetaStore.Count() + s.bufferSize

	var dataSize, metaSize int64
	
	dataFile := filepath.Join(s.path, dataFileName)
	if info, err := os.Stat(dataFile); err == nil {
		dataSize = info.Size()
	}

	if info, err := os.Stat(s.snapshot.MetaStore.Path()); err == nil {
		metaSize = info.Size()
	}
	
	// Get file format version
	formatVersion := s.snapshot.FormatVersion(s.getFileVersion)

	// Calculate deletion stats
	deletedCount := s.snapshot.DeletionStore.Count()
	deletionRate := 0.0
	if docCount > 0 {
		deletionRate = float64(deletedCount) / float64(docCount)
	}

	return StorageStats{
		DocumentCount: docCount,
		BufferSize:    s.bufferSize,
		DataFileSize:  dataSize,
		MetaFileSize:  metaSize,
		FormatVersion: formatVersion,
		DeletedCount:  deletedCount,
		DeletionRate:  deletionRate,
	}
}

// getFileVersion reads the actual file format version from disk.
// Returns the configured version if file doesn't exist or can't be read.
func (s *DocumentStorage) getFileVersion() (format.VersionPolicy, error) {
	dataFile := filepath.Join(s.path, dataFileName)
	
	// Check if file exists
	if _, err := os.Stat(dataFile); err != nil {
		return s.version, fmt.Errorf("data file not found: %w", err)
	}
	
	// Open reader to get version from footer
	reader, err := column.NewRowIndexReader(dataFile)
	if err != nil {
		return s.version, fmt.Errorf("open reader: %w", err)
	}
	defer reader.Close()
	
	return reader.GetVersion(), nil
}

// supportsRowIndex checks if the current data file supports RowIndex.
// This checks both the configured version and the actual file version.
func (s *DocumentStorage) supportsRowIndex() bool {
	// Check configured version
	if !s.version.HasFeature(format.FeatureRowIndex) {
		return false
	}
	
	// Check actual file version
	fileVer, err := s.getFileVersion()
	if err != nil {
		// File doesn't exist or can't be read, assume supported for new files
		return true
	}
	
	return fileVer.HasFeature(format.FeatureRowIndex)
}

// Close flushes pending writes and closes the storage.
func (s *DocumentStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	if err := s.flush(); err != nil {
		return fmt.Errorf("flush on close: %w", err)
	}

	s.closed = true

	// Release cached RowIndex
	s.rowIdxMu.Lock()
	s.cachedRowIndex = nil
	s.rowIdxTried = false
	s.rowIdxMu.Unlock()

	return nil
}


