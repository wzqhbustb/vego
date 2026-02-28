package vego

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/wzqhbustb/vego/storage/arrow"
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
)

// docMeta stores metadata for a document (not stored in column storage)
type docMeta struct {
	ID       string                 `json:"id"`
	Metadata map[string]interface{} `json:"metadata"`
}

// metadataStore is the in-memory and on-disk metadata storage
type metadataStore struct {
	// idHash -> docMeta
	entries map[int64]docMeta
	// string ID -> idHash (for quick lookup)
	idToHash map[string]int64
	path     string
	mu       sync.RWMutex
}

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
	bufferSize  int
	maxBuffer   int

	// Metadata storage (separate from column storage)
	metaStore *metadataStore

	// BlockCache for page-level caching (optional, shared across storages)
	blockCache *format.BlockCache

	// Format version for column storage (V1_0, V1_1, V1_2)
	version format.VersionPolicy

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
	FormatVersion string // File format version (e.g., "1.2")
}

// NewDocumentStorage creates a new document storage instance.
// Optionally accepts a shared BlockCache for page-level caching.
// Optionally accepts a format version (defaults to V1_2 for RowIndex + BlockCache support).
func NewDocumentStorage(path string, dimension int, cache ...*format.BlockCache) (*DocumentStorage, error) {
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, fmt.Errorf("create storage directory: %w", err)
	}

	metaStore := &metadataStore{
		entries:  make(map[int64]docMeta),
		idToHash: make(map[string]int64),
		path:     filepath.Join(path, metaFileName),
	}

	s := &DocumentStorage{
		path:      path,
		dimension: dimension,
		factory:   encoding.NewEncoderFactory(3),
		metaStore: metaStore,
		maxBuffer: maxBufferSize,
		version:   format.V1_2, // Default to V1.2 for RowIndex + BlockCache support
	}

	// Optional BlockCache for shared caching across storages
	if len(cache) > 0 && cache[0] != nil {
		s.blockCache = cache[0]
	}

	// Try to load existing data
	if err := s.load(); err != nil {
		return nil, fmt.Errorf("load existing data: %w", err)
	}

	return s, nil
}

// hashID converts a string ID to int64 hash for column storage
func hashID(id string) int64 {
	h := fnv.New64a()
	h.Write([]byte(id))
	return int64(h.Sum64())
}

// createSchema creates the Arrow schema for vector storage
func (s *DocumentStorage) createSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id_hash", Type: arrow.PrimInt64(), Nullable: false},
		{Name: "vector", Type: arrow.VectorType(s.dimension), Nullable: false},
		{Name: "timestamp", Type: arrow.PrimInt64(), Nullable: false},
	}, nil)
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
	s.metaStore.mu.RLock()
	_, exists := s.metaStore.idToHash[doc.ID]
	s.metaStore.mu.RUnlock()

	if exists {
		// Update existing - remove old entry first
		if err := s.deleteFromStorage(doc.ID); err != nil {
			return fmt.Errorf("delete old document: %w", err)
		}
	}

	// Remove from buffer if present (for updates)
	for i, bufDoc := range s.writeBuffer {
		if bufDoc.ID == doc.ID {
			// Remove from buffer by replacing with last element and truncating
			s.writeBuffer = append(s.writeBuffer[:i], s.writeBuffer[i+1:]...)
			s.bufferSize--
			break
		}
	}

	// Add to buffer
	s.writeBuffer = append(s.writeBuffer, doc.Clone())
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

		s.metaStore.mu.RLock()
		_, exists := s.metaStore.idToHash[doc.ID]
		s.metaStore.mu.RUnlock()

		if exists {
			if err := s.deleteFromStorage(doc.ID); err != nil {
				return fmt.Errorf("delete old document %s: %w", doc.ID, err)
			}
		}

		s.writeBuffer = append(s.writeBuffer, doc.Clone())
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
	for _, doc := range s.writeBuffer {
		if doc.ID == id {
			return doc.Clone(), nil
		}
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
	s.metaStore.mu.RLock()
	idHash, exists := s.metaStore.idToHash[id]
	if !exists {
		s.metaStore.mu.RUnlock()
		return nil, ErrDocumentNotFound
	}
	meta := s.metaStore.entries[idHash]
	s.metaStore.mu.RUnlock()

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
	for i, doc := range s.writeBuffer {
		if doc.ID == id {
			s.writeBuffer = append(s.writeBuffer[:i], s.writeBuffer[i+1:]...)
			s.bufferSize--
			return nil
		}
	}

	return s.deleteFromStorage(id)
}

// deleteFromStorage removes a document from storage.
func (s *DocumentStorage) deleteFromStorage(id string) error {
	idHash := hashID(id)

	s.metaStore.mu.Lock()
	delete(s.metaStore.entries, idHash)
	delete(s.metaStore.idToHash, id)
	s.metaStore.mu.Unlock()

	s.dirty = true

	// Note: We don't immediately rewrite the column storage file.
	// The deleted document will be filtered out on next read.
	// A background compaction process could clean this up periodically.

	return s.saveMetadata()
}

// Flush writes all buffered documents to storage.
func (s *DocumentStorage) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.flush()
}

// flush is the internal flush implementation (must hold lock)
func (s *DocumentStorage) flush() error {
	if s.bufferSize == 0 {
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

	// Clear buffer
	s.writeBuffer = s.writeBuffer[:0]
	s.bufferSize = 0
	s.dirty = false

	return nil
}

// rewriteStorage writes all documents to column storage and metadata store.
func (s *DocumentStorage) rewriteStorage(docs []*Document) error {

	// Deduplicate by ID (last write wins based on timestamp)
	docMap := make(map[string]*Document)
	for _, doc := range docs {
		existing, exists := docMap[doc.ID]
		if !exists || doc.Timestamp.After(existing.Timestamp) {
			docMap[doc.ID] = doc
		}
	}

	// Convert map to slice
	uniqueDocs := make([]*Document, 0, len(docMap))
	for _, doc := range docMap {
		uniqueDocs = append(uniqueDocs, doc)
	}

	// Update metadata store
	s.metaStore.mu.Lock()
	for _, doc := range uniqueDocs {
		idHash := hashID(doc.ID)
		s.metaStore.entries[idHash] = docMeta{
			ID:       doc.ID,
			Metadata: doc.Metadata,
		}
		s.metaStore.idToHash[doc.ID] = idHash
	}
	s.metaStore.mu.Unlock()

	// Save metadata
	if err := s.saveMetadata(); err != nil {
		return fmt.Errorf("save metadata: %w", err)
	}

	// Write column storage
	if err := s.writeColumnStorage(uniqueDocs); err != nil {
		return fmt.Errorf("write column storage: %w", err)
	}

	return nil
}

// writeColumnStorage writes vectors to columnar format with RowIndex support.
func (s *DocumentStorage) writeColumnStorage(docs []*Document) error {
	if len(docs) == 0 {
		return nil
	}

	dataFile := filepath.Join(s.path, dataFileName)
	schema := s.createSchema()

	// Remove existing file to ensure clean write
	if _, err := os.Stat(dataFile); err == nil {
		if err := os.Remove(dataFile); err != nil {
			return fmt.Errorf("remove existing file: %w", err)
		}
	}

	// Use RowIndexWriter for V1.1+ format support
	writer, err := column.NewRowIndexWriter(dataFile, schema, s.version, s.factory)
	if err != nil {
		return fmt.Errorf("create row index writer: %w", err)
	}

	// Configure BlockCache if available (V1.2+)
	if s.blockCache != nil {
		writer.SetBlockSize(format.DefaultBlockSize)
	}

	// Build arrays
	idBuilder := arrow.NewInt64Builder()
	vectorBuilder := arrow.NewFixedSizeListBuilder(
		arrow.FixedSizeListOf(arrow.PrimFloat32(), s.dimension).(*arrow.FixedSizeListType),
	)
	timestampBuilder := arrow.NewInt64Builder()

	// Populate builders
	for _, doc := range docs {
		idBuilder.Append(hashID(doc.ID))
		vectorBuilder.AppendValues(doc.Vector)
		timestampBuilder.Append(doc.Timestamp.UnixNano())
	}

	// Create arrays
	idArray := idBuilder.NewArray()
	vectorArray := vectorBuilder.NewArray()
	timestampArray := timestampBuilder.NewArray()

	// Create record batch
	batch, err := arrow.NewRecordBatch(schema, len(docs), []arrow.Array{
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
	idHashArray := batch.Column(0).(*arrow.Int64Array)
	vectorArray := batch.Column(1).(*arrow.FixedSizeListArray)
	timestampArray := batch.Column(2).(*arrow.Int64Array)

	// Get metadata
	s.metaStore.mu.RLock()
	defer s.metaStore.mu.RUnlock()

	// Use a map to deduplicate by hash (last write wins)
	// This handles the case where updates create duplicate entries
	docMap := make(map[int64]*Document)
	vectorValues := vectorArray.Values().(*arrow.Float32Array).Values()

	for i := 0; i < batch.NumRows(); i++ {
		idHash := idHashArray.Value(i)
		
		// Skip if not in metadata (deleted)
		meta, exists := s.metaStore.entries[idHash]
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
		if hashID(doc.ID) == idHash {
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

// tryReadByRowIndex attempts to read a document using RowIndex.
// Returns (doc, usedRowIndex, error). If RowIndex is not available, returns (nil, false, nil).
func (s *DocumentStorage) tryReadByRowIndex(id string) (*Document, bool, error) {
	dataFile := filepath.Join(s.path, dataFileName)
	
	// Check if file exists
	if _, err := os.Stat(dataFile); err != nil {
		return nil, false, nil
	}
	
	// Check if file supports RowIndex (version check)
	if !s.supportsRowIndex() {
		return nil, false, nil
	}
	
	// Open RowIndexReader with cache if available
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
	
	// Double-check: file has RowIndex
	if !reader.HasRowIndex() {
		return nil, false, nil
	}
	
	// Lookup row index by ID (O(1))
	rowIdx, err := reader.LookupRowID(id)
	if err != nil {
		// ID not found in RowIndex
		return nil, true, ErrDocumentNotFound
	}
	
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
	s.metaStore.mu.RLock()
	meta, exists := s.metaStore.entries[idHash]
	s.metaStore.mu.RUnlock()
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



// saveMetadata saves the metadata store to disk.
func (s *DocumentStorage) saveMetadata() error {
	s.metaStore.mu.RLock()
	data := struct {
		Entries  map[int64]docMeta `json:"entries"`
		IDToHash map[string]int64  `json:"id_to_hash"`
	}{
		Entries:  s.metaStore.entries,
		IDToHash: s.metaStore.idToHash,
	}
	s.metaStore.mu.RUnlock()

	file, err := os.Create(s.metaStore.path)
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

// loadMetadata loads the metadata store from disk.
func (s *DocumentStorage) loadMetadata() error {
	_, err := os.Stat(s.metaStore.path)
	if os.IsNotExist(err) {
		// No existing metadata, start fresh
		return nil
	}
	if err != nil {
		return err
	}

	data, err := os.ReadFile(s.metaStore.path)
	if err != nil {
		return fmt.Errorf("read metadata file: %w", err)
	}

	var stored struct {
		Entries  map[int64]docMeta `json:"entries"`
		IDToHash map[string]int64  `json:"id_to_hash"`
	}

	if err := json.Unmarshal(data, &stored); err != nil {
		return fmt.Errorf("decode metadata: %w", err)
	}

	s.metaStore.mu.Lock()
	s.metaStore.entries = stored.Entries
	s.metaStore.idToHash = stored.IDToHash
	if s.metaStore.entries == nil {
		s.metaStore.entries = make(map[int64]docMeta)
	}
	if s.metaStore.idToHash == nil {
		s.metaStore.idToHash = make(map[string]int64)
	}
	s.metaStore.mu.Unlock()

	return nil
}

// load loads existing data.
func (s *DocumentStorage) load() error {
	return s.loadMetadata()
}

// Stats returns statistics about the storage.
func (s *DocumentStorage) Stats() StorageStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	s.metaStore.mu.RLock()
	docCount := len(s.metaStore.idToHash) + s.bufferSize
	s.metaStore.mu.RUnlock()

	var dataSize, metaSize int64
	
	dataFile := filepath.Join(s.path, dataFileName)
	if info, err := os.Stat(dataFile); err == nil {
		dataSize = info.Size()
	}

	if info, err := os.Stat(s.metaStore.path); err == nil {
		metaSize = info.Size()
	}
	
	// Get file format version
	formatVersion := s.version.String() // Default to configured version
	if fileVer, err := s.getFileVersion(); err == nil {
		formatVersion = fileVer.String()
	}

	return StorageStats{
		DocumentCount: docCount,
		BufferSize:    s.bufferSize,
		DataFileSize:  dataSize,
		MetaFileSize:  metaSize,
		FormatVersion: formatVersion,
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
	return nil
}


