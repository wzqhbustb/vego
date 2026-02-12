package vego

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

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
	storage, err := NewDocumentStorage(storagePath, config.Dimension)
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
// Deprecated: Use InsertContext instead
func (c *Collection) Insert(doc *Document) error {
	return c.InsertContext(context.Background(), doc)
}

// InsertContext adds a document to the collection with context support
func (c *Collection) InsertContext(ctx context.Context, doc *Document) error {
	if err := doc.Validate(c.dimension); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Check context cancellation
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

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
	if err := c.storage.Put(doc); err != nil {
		// Rollback: Remove from mappings (node remains orphaned in index until rebuilt)
		// Note: HNSW doesn't support Delete, so the node will stay in the index
		// but won't be discoverable through normal operations
		log.Printf("Warning: Failed to store document %s, node %d is orphaned", doc.ID, nodeID)
		return fmt.Errorf("store document: %w", err)
	}

	// Update mappings
	c.docToNode[doc.ID] = nodeID
	c.nodeToDoc[nodeID] = doc.ID

	// Update timestamp
	doc.Timestamp = time.Now()

	return nil
}

// InsertBatch adds multiple documents in batch (more efficient)
// Deprecated: Use InsertBatchContext instead
func (c *Collection) InsertBatch(docs []*Document) error {
	return c.InsertBatchContext(context.Background(), docs)
}

// InsertBatchContext adds multiple documents with context support
func (c *Collection) InsertBatchContext(ctx context.Context, docs []*Document) error {
	if len(docs) == 0 {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Check context cancellation
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

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
		// Check context cancellation periodically
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		nodeID, err := c.index.Add(doc.Vector)
		if err != nil {
			return fmt.Errorf("add to index: %w", err)
		}
		c.docToNode[doc.ID] = nodeID
		c.nodeToDoc[nodeID] = doc.ID
		doc.Timestamp = time.Now()
	}

	// Store documents
	if err := c.storage.PutBatch(docs); err != nil {
		return fmt.Errorf("store documents: %w", err)
	}

	return nil
}

// GetBatch retrieves multiple documents by IDs
// Returns a map of id -> document (missing documents are omitted)
func (c *Collection) GetBatch(ids []string) (map[string]*Document, error) {
	return c.GetBatchContext(context.Background(), ids)
}

// GetBatchContext retrieves multiple documents with context support
func (c *Collection) GetBatchContext(ctx context.Context, ids []string) (map[string]*Document, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Check context cancellation
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	results := make(map[string]*Document, len(ids))
	for _, id := range ids {
		// Check context cancellation periodically
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		doc, err := c.storage.Get(id)
		if err != nil {
			// Skip not found documents
			continue
		}
		results[id] = doc
	}

	return results, nil
}

// DeleteBatch removes multiple documents from the collection
func (c *Collection) DeleteBatch(ids []string) error {
	return c.DeleteBatchContext(context.Background(), ids)
}

// DeleteBatchContext removes multiple documents with context support
func (c *Collection) DeleteBatchContext(ctx context.Context, ids []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check context cancellation
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	var lastErr error
	for _, id := range ids {
		// Check context cancellation periodically
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		nodeID, exists := c.docToNode[id]
		if !exists {
			continue // Skip non-existent documents
		}

		// Delete from storage
		if err := c.storage.Delete(id); err != nil {
			lastErr = err
			continue // Continue with other deletions even if one fails
		}

		// Delete from index mapping
		delete(c.docToNode, id)
		delete(c.nodeToDoc, nodeID)
	}

	return lastErr
}

// Get retrieves a document by ID
// Deprecated: Use GetContext instead
func (c *Collection) Get(id string) (*Document, error) {
	return c.GetContext(context.Background(), id)
}

// GetContext retrieves a document by ID with context support
func (c *Collection) GetContext(ctx context.Context, id string) (*Document, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Check context cancellation
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	return c.storage.Get(id)
}

// Delete removes a document from the collection
// Deprecated: Use DeleteContext instead
func (c *Collection) Delete(id string) error {
	return c.DeleteContext(context.Background(), id)
}

// DeleteContext removes a document from the collection with context support
func (c *Collection) DeleteContext(ctx context.Context, id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check context cancellation
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

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

// Update updates a document's metadata and vector
// WARNING: This creates a new node in the index. The old node remains (HNSW doesn't support delete).
// For production use, consider: 1) Periodic index rebuild, 2) Or use Delete + Insert pattern
// Deprecated: Use UpdateContext instead
func (c *Collection) Update(doc *Document) error {
	return c.UpdateContext(context.Background(), doc)
}

// UpdateContext updates a document with context support
func (c *Collection) UpdateContext(ctx context.Context, doc *Document) error {
	if err := doc.Validate(c.dimension); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Check context cancellation
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	oldNodeID, exists := c.docToNode[doc.ID]
	if !exists {
		return fmt.Errorf("document %s not found", doc.ID)
	}

	// Update storage first
	if err := c.storage.Put(doc); err != nil {
		return fmt.Errorf("update document: %w", err)
	}

	// Add new vector to index
	newNodeID, err := c.index.Add(doc.Vector)
	if err != nil {
		return fmt.Errorf("update index: %w", err)
	}

	// Update mappings (old node becomes orphaned)
	delete(c.nodeToDoc, oldNodeID)
	c.docToNode[doc.ID] = newNodeID
	c.nodeToDoc[newNodeID] = doc.ID
	doc.Timestamp = time.Now()

	return nil
}

// Upsert inserts or updates a document
// Deprecated: Use UpsertContext instead
func (c *Collection) Upsert(doc *Document) error {
	return c.UpsertContext(context.Background(), doc)
}

// UpsertContext inserts or updates a document with context support
func (c *Collection) UpsertContext(ctx context.Context, doc *Document) error {
	c.mu.RLock()
	_, exists := c.docToNode[doc.ID]
	c.mu.RUnlock()

	if exists {
		return c.UpdateContext(ctx, doc)
	}
	return c.InsertContext(ctx, doc)
}

// Search performs vector similarity search
// Deprecated: Use SearchContext instead
func (c *Collection) Search(query []float32, k int, opts ...SearchOption) ([]SearchResult, error) {
	return c.SearchContext(context.Background(), query, k, opts...)
}

// SearchContext performs vector similarity search with context support
func (c *Collection) SearchContext(ctx context.Context, query []float32, k int, opts ...SearchOption) ([]SearchResult, error) {
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

	// Check context cancellation
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Search HNSW index
	hnswResults, err := c.index.Search(query, k, options.EF)
	if err != nil {
		return nil, fmt.Errorf("search index: %w", err)
	}

	// Map to documents
	results := make([]SearchResult, 0, len(hnswResults))
	for _, hr := range hnswResults {
		// Check context cancellation periodically
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		docID, exists := c.nodeToDoc[hr.ID]
		if !exists {
			log.Printf("Warning: node %d has no document mapping (orphaned)", hr.ID)
			continue // Skip deleted/orphaned nodes
		}

		doc, err := c.storage.Get(docID)
		if err != nil {
			log.Printf("Warning: failed to load document %s: %v", docID, err)
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
// Dynamically expands search scope until enough filtered results are found
func (c *Collection) SearchWithFilter(query []float32, k int, filter Filter) ([]SearchResult, error) {
	batchSize := k * 2
	maxBatchSize := k * 20
	maxAttempts := 5

	var allFiltered []SearchResult

	for attempt := 0; attempt < maxAttempts && batchSize <= maxBatchSize; attempt++ {
		// Search with current batch size
		results, err := c.Search(query, batchSize)
		if err != nil {
			return nil, err
		}

		// Apply filter
		allFiltered = allFiltered[:0] // Reset
		for _, r := range results {
			if r.Document != nil && filter.Match(r.Document) {
				allFiltered = append(allFiltered, r)
				if len(allFiltered) >= k {
					return allFiltered[:k], nil
				}
			}
		}

		// If we got all available results and found some matches, return them
		if len(results) < batchSize {
			// We got all available results
			return allFiltered, nil
		}

		// Otherwise expand search
		batchSize *= 2
	}

	return allFiltered, nil
}

// SearchBatch performs multiple vector searches in parallel
func (c *Collection) SearchBatch(queries [][]float32, k int, opts ...SearchOption) ([][]SearchResult, error) {
	if len(queries) == 0 {
		return [][]SearchResult{}, nil
	}

	results := make([][]SearchResult, len(queries))
	errors := make([]error, len(queries))

	// Use worker pool for parallel search
	var wg sync.WaitGroup
	numWorkers := 4
	if len(queries) < numWorkers {
		numWorkers = len(queries)
	}

	jobs := make(chan int, len(queries))

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				results[i], errors[i] = c.Search(queries[i], k, opts...)
			}
		}()
	}

	for i := range queries {
		jobs <- i
	}
	close(jobs)
	wg.Wait()

	// Check for errors
	for i, err := range errors {
		if err != nil {
			return nil, fmt.Errorf("search query %d: %w", i, err)
		}
	}

	return results, nil
}

// Count returns number of documents in collection
func (c *Collection) Count() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.docToNode)
}

// CollectionStats contains collection statistics
type CollectionStats struct {
	Name        string    // Collection name
	Count       int       // Number of documents
	Dimension   int       // Vector dimension
	IndexNodes  int       // Total HNSW nodes (includes orphaned)
	OrphanNodes int       // Orphaned nodes (from updates)
	LastUpdate  time.Time // Last modification time
}

// Stats returns collection statistics
func (c *Collection) Stats() CollectionStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Count unique node IDs in mapping (all nodes ever created)
	allNodes := make(map[int]bool)
	for _, nodeID := range c.docToNode {
		allNodes[nodeID] = true
	}
	totalIndexNodes := len(allNodes)
	docCount := len(c.docToNode)

	return CollectionStats{
		Name:        c.name,
		Count:       docCount,
		Dimension:   c.dimension,
		IndexNodes:  totalIndexNodes,
		OrphanNodes: 0, // Will need HNSW API to accurately count
		LastUpdate:  time.Now(),
	}
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
	data := map[string]interface{}{
		"docToNode": c.docToNode,
		"nodeToDoc": c.nodeToDoc,
	}

	bytes, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal mappings: %w", err)
	}

	if err := os.WriteFile(path, bytes, 0644); err != nil {
		return fmt.Errorf("write mappings: %w", err)
	}

	return nil
}

func (c *Collection) loadMappings(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	var mappings map[string]interface{}
	if err := json.Unmarshal(data, &mappings); err != nil {
		return fmt.Errorf("unmarshal mappings: %w", err)
	}

	// Load docToNode
	if docToNodeRaw, ok := mappings["docToNode"].(map[string]interface{}); ok {
		for k, v := range docToNodeRaw {
			if nodeID, ok := v.(float64); ok {
				c.docToNode[k] = int(nodeID)
			}
		}
	}

	// Load nodeToDoc
	if nodeToDocRaw, ok := mappings["nodeToDoc"].(map[string]interface{}); ok {
		for k, v := range nodeToDocRaw {
			if docID, ok := v.(string); ok {
				if nodeID, ok := parseIntKey(k); ok {
					c.nodeToDoc[nodeID] = docID
				}
			}
		}
	}

	return nil
}

// parseIntKey converts string key to int (JSON only supports string keys)
func parseIntKey(s string) (int, bool) {
	var i int
	_, err := fmt.Sscanf(s, "%d", &i)
	return i, err == nil
}
