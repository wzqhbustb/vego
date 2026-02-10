package vego

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	hnsw "github.com/wzqhbustb/vego/index"
)

// Collection represents a collection of documents with vector search capability
type Collection struct {
	name      string
	path      string
	dimension int

	// HNSW index for vector search
	index *hnsw.HNSWIndex

	// Storage for documents
	storage *DocumentStorage

	// Document ID -> HNSW node ID mapping
	docToNode map[string]int
	nodeToDoc map[int]string

	mu     sync.RWMutex
	config *Config
}

// NewCollection creates a new collection
func NewCollection(name, path string, config *Config) (*Collection, error) {
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}

	coll := &Collection{
		name:      name,
		path:      path,
		dimension: config.Dimension,
		docToNode: make(map[string]int),
		nodeToDoc: make(map[int]string),
		config:    config,
	}

	// Initialize HNSW index
	hnswConfig := hnsw.Config{
		Dimension:      config.Dimension,
		M:              config.M,
		EfConstruction: config.EfConstruction,
		DistanceFunc:   config.DistanceFunc,
		Adaptive:       config.Adaptive,
		ExpectedSize:   config.ExpectedSize,
	}
	coll.index = hnsw.NewHNSW(hnswConfig)

	// Initialize document storage
	storagePath := filepath.Join(path, "documents")
	storage, err := NewDocumentStorage(storagePath, config)
	if err != nil {
		return nil, fmt.Errorf("init document storage: %w", err)
	}
	coll.storage = storage

	// Try to load existing data
	if err := coll.load(); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("load collection: %w", err)
	}

	return coll, nil
}

// Insert adds a document to the collection
func (c *Collection) Insert(doc *Document) error {
	if err := doc.Validate(c.dimension); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if document already exists
	if _, exists := c.docToNode[doc.ID]; exists {
		return fmt.Errorf("document %s already exists", doc.ID)
	}

	// Add to HNSW index
	nodeID, err := c.index.Add(doc.Vector)
	if err != nil {
		return fmt.Errorf("add to index: %w", err)
	}

	// Store document
	if err := c.storage.Put(doc.ID, doc); err != nil {
		// Rollback index
		c.index.Delete(nodeID) // Need to implement Delete in HNSW
		return fmt.Errorf("store document: %w", err)
	}

	// Update mappings
	c.docToNode[doc.ID] = nodeID
	c.nodeToDoc[nodeID] = doc.ID

	return nil
}

// InsertBatch adds multiple documents in batch (more efficient)
func (c *Collection) InsertBatch(docs []*Document) error {
	if len(docs) == 0 {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Validate all documents first
	for i, doc := range docs {
		if err := doc.Validate(c.dimension); err != nil {
			return fmt.Errorf("document %d: %w", i, err)
		}
		if _, exists := c.docToNode[doc.ID]; exists {
			return fmt.Errorf("document %s already exists", doc.ID)
		}
	}

	// Insert into HNSW
	for _, doc := range docs {
		nodeID, err := c.index.Add(doc.Vector)
		if err != nil {
			return fmt.Errorf("add to index: %w", err)
		}
		c.docToNode[doc.ID] = nodeID
		c.nodeToDoc[nodeID] = doc.ID
	}

	// Store documents
	if err := c.storage.PutBatch(docs); err != nil {
		return fmt.Errorf("store documents: %w", err)
	}

	return nil
}

// Get retrieves a document by ID
func (c *Collection) Get(id string) (*Document, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.storage.Get(id)
}

// Delete removes a document from the collection
func (c *Collection) Delete(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	nodeID, exists := c.docToNode[id]
	if !exists {
		return fmt.Errorf("document %s not found", id)
	}

	// Delete from storage
	if err := c.storage.Delete(id); err != nil {
		return fmt.Errorf("delete from storage: %w", err)
	}

	// Delete from index (soft delete - mark as deleted)
	// Note: Full delete requires rebuilding index
	delete(c.docToNode, id)
	delete(c.nodeToDoc, nodeID)

	return nil
}

// Update updates a document
func (c *Collection) Update(doc *Document) error {
	if err := doc.Validate(c.dimension); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	nodeID, exists := c.docToNode[doc.ID]
	if !exists {
		return fmt.Errorf("document %s not found", doc.ID)
	}

	// Update storage
	if err := c.storage.Put(doc.ID, doc); err != nil {
		return fmt.Errorf("update document: %w", err)
	}

	// Update vector in index (delete old, insert new)
	// This is simplified - in production, might want to update in place
	newNodeID, err := c.index.Add(doc.Vector)
	if err != nil {
		return fmt.Errorf("update index: %w", err)
	}

	delete(c.nodeToDoc, nodeID)
	c.docToNode[doc.ID] = newNodeID
	c.nodeToDoc[newNodeID] = doc.ID

	return nil
}

// Search performs vector similarity search
func (c *Collection) Search(query []float32, k int, opts ...SearchOption) ([]SearchResult, error) {
	if len(query) != c.dimension {
		return nil, fmt.Errorf("query dimension mismatch: expected %d, got %d", c.dimension, len(query))
	}

	options := &SearchOptions{
		EF: 0, // Use default
	}
	for _, opt := range opts {
		opt(options)
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	// Search HNSW index
	hnswResults, err := c.index.Search(query, k, options.EF)
	if err != nil {
		return nil, fmt.Errorf("search index: %w", err)
	}

	// Map to documents
	results := make([]SearchResult, 0, len(hnswResults))
	for _, hr := range hnswResults {
		docID, exists := c.nodeToDoc[hr.ID]
		if !exists {
			continue // Skip deleted documents
		}

		doc, err := c.storage.Get(docID)
		if err != nil {
			continue // Skip missing documents
		}

		results = append(results, SearchResult{
			Document: doc,
			Distance: hr.Distance,
		})
	}

	return results, nil
}

// SearchWithFilter performs vector search with metadata filter
func (c *Collection) SearchWithFilter(query []float32, k int, filter Filter) ([]SearchResult, error) {
	// First search without filter
	results, err := c.Search(query, k*2) // Get more candidates
	if err != nil {
		return nil, err
	}

	// Apply filter
	var filtered []SearchResult
	for _, r := range results {
		if filter.Match(r.Document) {
			filtered = append(filtered, r)
			if len(filtered) >= k {
				break
			}
		}
	}

	return filtered, nil
}

// Count returns number of documents in collection
func (c *Collection) Count() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.docToNode)
}

// Save persists collection to disk
func (c *Collection) Save() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Save HNSW index
	indexPath := filepath.Join(c.path, "index")
	if err := c.index.SaveToLance(indexPath); err != nil {
		return fmt.Errorf("save index: %w", err)
	}

	// Save mappings
	mappingsPath := filepath.Join(c.path, "mappings.json")
	if err := c.saveMappings(mappingsPath); err != nil {
		return fmt.Errorf("save mappings: %w", err)
	}

	// Flush document storage
	if err := c.storage.Flush(); err != nil {
		return fmt.Errorf("flush storage: %w", err)
	}

	return nil
}

// Close closes the collection
func (c *Collection) Close() error {
	// Auto-save on close
	if err := c.Save(); err != nil {
		return err
	}
	return c.storage.Close()
}

// Drop removes the collection and all its data
func (c *Collection) Drop() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return os.RemoveAll(c.path)
}

func (c *Collection) load() error {
	// Load HNSW index
	indexPath := filepath.Join(c.path, "index")
	if _, err := os.Stat(indexPath); err == nil {
		loadedIndex, err := hnsw.LoadHNSWFromLance(indexPath)
		if err != nil {
			return fmt.Errorf("load index: %w", err)
		}
		c.index = loadedIndex
	}

	// Load mappings
	mappingsPath := filepath.Join(c.path, "mappings.json")
	if err := c.loadMappings(mappingsPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("load mappings: %w", err)
	}

	return nil
}

func (c *Collection) saveMappings(path string) error {
	// Implementation to save docToNode and nodeToDoc mappings
	return nil
}

func (c *Collection) loadMappings(path string) error {
	// Implementation to load mappings
	return nil
}
