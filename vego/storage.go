package vego

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/wzqhbustb/vego/storage/arrow"
	"github.com/wzqhbustb/vego/storage/column"
	"github.com/wzqhbustb/vego/storage/encoding"
)

// DocumentStorage handles persistent storage of documents
type DocumentStorage struct {
	path    string
	writer  *column.Writer
	reader  *column.Reader
	encoder *encoding.EncoderFactory
	docs    map[string]*Document // In-memory cache for simple implementation
	mu      sync.RWMutex
}

// NewDocumentStorage creates a new document storage
func NewDocumentStorage(path string, config *Config) (*DocumentStorage, error) {
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}

	encoderFactory := encoding.NewEncoderFactory(config.CompressionLevel)

	// Define schema for documents
	schema := arrow.NewSchema([]arrow.Field{
		arrow.NewField("id", arrow.PrimString(), false),
		arrow.NewField("vector", arrow.VectorType(config.Dimension), false),
		arrow.NewField("metadata", arrow.PrimBinary(), false),
		arrow.NewField("timestamp", arrow.PrimInt64(), false),
	}, nil)

	// Try to open existing or create new
	dataFile := filepath.Join(path, "data.lance")
	var writer *column.Writer
	var err error

	if _, statErr := os.Stat(dataFile); os.IsNotExist(statErr) {
		writer, err = column.NewWriter(dataFile, schema, encoderFactory)
	} else {
		// Append mode not supported yet, use new file
		writer, err = column.NewWriter(dataFile+".tmp", schema, encoderFactory)
	}

	if err != nil {
		return nil, fmt.Errorf("create writer: %w", err)
	}

	return &DocumentStorage{
		path:    path,
		writer:  writer,
		encoder: encoderFactory,
		docs:    make(map[string]*Document),
	}, nil
}

// Put stores a single document
func (s *DocumentStorage) Put(id string, doc *Document) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Store in memory cache
	s.docs[id] = doc.Clone()

	// Persist to disk as JSON (simple implementation)
	docPath := filepath.Join(s.path, id+".json")
	data, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("marshal document: %w", err)
	}

	if err := os.WriteFile(docPath, data, 0644); err != nil {
		return fmt.Errorf("write document: %w", err)
	}

	return nil
}

// PutBatch stores multiple documents
func (s *DocumentStorage) PutBatch(docs []*Document) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, doc := range docs {
		// Store in memory cache
		s.docs[doc.ID] = doc.Clone()

		// Persist to disk
		docPath := filepath.Join(s.path, doc.ID+".json")
		data, err := json.Marshal(doc)
		if err != nil {
			return fmt.Errorf("marshal document %s: %w", doc.ID, err)
		}

		if err := os.WriteFile(docPath, data, 0644); err != nil {
			return fmt.Errorf("write document %s: %w", doc.ID, err)
		}
	}

	return nil
}

// Get retrieves a document by ID
func (s *DocumentStorage) Get(id string) (*Document, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Try memory cache first
	if doc, exists := s.docs[id]; exists {
		return doc.Clone(), nil
	}

	// Load from disk
	docPath := filepath.Join(s.path, id+".json")
	data, err := os.ReadFile(docPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("document %s not found", id)
		}
		return nil, fmt.Errorf("read document: %w", err)
	}

	var doc Document
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("unmarshal document: %w", err)
	}

	// Cache it
	s.docs[id] = &doc

	return doc.Clone(), nil
}

// Delete removes a document
func (s *DocumentStorage) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Remove from cache
	delete(s.docs, id)

	// Remove from disk
	docPath := filepath.Join(s.path, id+".json")
	if err := os.Remove(docPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("delete document: %w", err)
	}

	return nil
}

// Flush ensures all data is written
func (s *DocumentStorage) Flush() error {
	return s.writer.Close()
}

// Close closes the storage
func (s *DocumentStorage) Close() error {
	return s.writer.Close()
}
