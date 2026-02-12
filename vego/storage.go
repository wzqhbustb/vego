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
}

// NewDocumentStorage creates a new document storage instance.
func NewDocumentStorage(path string, dimension int) (*DocumentStorage, error) {
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
func (s *DocumentStorage) Get(id string) (*Document, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, fmt.Errorf("storage is closed")
	}

	// Check buffer first
	for _, doc := range s.writeBuffer {
		if doc.ID == id {
			return doc.Clone(), nil
		}
	}

	// Check metadata store
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

	// Read existing vectors if file exists
	var existingDocs []*Document
	dataFile := filepath.Join(s.path, dataFileName)
	if _, err := os.Stat(dataFile); err == nil {
		docs, err := s.readAllDocuments()
		if err != nil {
			return fmt.Errorf("read existing documents: %w", err)
		}
		existingDocs = docs
	}

	// Add buffered documents
	allDocs := append(existingDocs, s.writeBuffer...)

	// Rewrite storage
	if err := s.rewriteStorage(allDocs); err != nil {
		return fmt.Errorf("rewrite storage: %w", err)
	}

	// Clear buffer
	s.writeBuffer = s.writeBuffer[:0]
	s.bufferSize = 0
	s.dirty = false

	return nil
}

// rewriteStorage writes all documents to column storage and metadata store.
func (s *DocumentStorage) rewriteStorage(docs []*Document) error {
	// Update metadata store
	s.metaStore.mu.Lock()
	for _, doc := range docs {
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
	if err := s.writeColumnStorage(docs); err != nil {
		return fmt.Errorf("write column storage: %w", err)
	}

	return nil
}

// writeColumnStorage writes vectors to Lance format.
func (s *DocumentStorage) writeColumnStorage(docs []*Document) error {
	if len(docs) == 0 {
		return nil
	}

	dataFile := filepath.Join(s.path, dataFileName)
	schema := s.createSchema()

	writer, err := column.NewWriter(dataFile, schema, s.factory)
	if err != nil {
		return fmt.Errorf("create writer: %w", err)
	}
	defer writer.Close()

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
		return fmt.Errorf("create record batch: %w", err)
	}

	// Write batch
	if err := writer.WriteRecordBatch(batch); err != nil {
		return fmt.Errorf("write record batch: %w", err)
	}

	return nil
}

// readAllDocuments reads all documents from storage.
func (s *DocumentStorage) readAllDocuments() ([]*Document, error) {
	dataFile := filepath.Join(s.path, dataFileName)
	
	reader, err := column.NewReader(dataFile)
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

	docs := make([]*Document, 0, batch.NumRows())
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

		docs = append(docs, &Document{
			ID:        meta.ID,
			Vector:    vector,
			Metadata:  meta.Metadata,
			Timestamp: time.Unix(0, timestampArray.Value(i)),
		})
	}

	return docs, nil
}

// readVectorByHash reads a vector by its ID hash.
func (s *DocumentStorage) readVectorByHash(idHash int64) ([]float32, int64, error) {
	docs, err := s.readAllDocuments()
	if err != nil {
		return nil, 0, err
	}

	for _, doc := range docs {
		if hashID(doc.ID) == idHash {
			return doc.Vector, doc.Timestamp.UnixNano(), nil
		}
	}

	return nil, 0, fmt.Errorf("vector not found for hash: %d", idHash)
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

	return StorageStats{
		DocumentCount: docCount,
		BufferSize:    s.bufferSize,
		DataFileSize:  dataSize,
		MetaFileSize:  metaSize,
	}
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


