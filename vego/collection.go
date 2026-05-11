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
	"github.com/wzqhbustb/vego/storage/catalog"
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
	idMapping *catalog.IDMapping

	mu     sync.RWMutex
	config *Config

	// Auto-compaction fields
	compactStopCh    chan struct{}  // Signal to stop background compaction goroutine
	compactTriggerCh chan struct{}  // Manual trigger channel
	compactWg        sync.WaitGroup // Wait group for graceful shutdown
	lastCompactTime  time.Time      // Last compaction timestamp
	compacting       bool           // Whether compaction is in progress
	compactMu        sync.RWMutex   // Protects compacting and lastCompactTime
}

// CompactState represents the state of compaction
type CompactState int

const (
	CompactIdle CompactState = iota
	CompactChecking
	CompactCompacting
	CompactCompleted
	CompactFailed
)

// CompactStatus provides information about compaction status
type CompactStatus struct {
	State       CompactState  // Current state
	Progress    float64       // Progress 0.0 - 1.0
	Message     string        // Human-readable description
	LastError   error         // Last error if failed
	NextRunTime time.Time     // Estimated next run time
}

// String returns human-readable state name
func (s CompactState) String() string {
	switch s {
	case CompactIdle:
		return "Idle"
	case CompactChecking:
		return "Checking"
	case CompactCompacting:
		return "Compacting"
	case CompactCompleted:
		return "Completed"
	case CompactFailed:
		return "Failed"
	default:
		return "Unknown"
	}
}

// NewCollection creates a new collection
func NewCollection(name, path string, config *Config) (*Collection, error) {
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}

	coll := &Collection{
		name:             name,
		path:             path,
		dimension:        config.Dimension,
		idMapping:        catalog.NewIDMapping(),
		config:           config,
		compactStopCh:    make(chan struct{}),
		compactTriggerCh: make(chan struct{}, 1), // Buffered to avoid blocking
		lastCompactTime:  time.Now(), // Initialize to prevent immediate max interval trigger
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
		return nil, wrapError("NewCollection", name, "", err)
	}
	coll.storage = storage

	// Try to load existing data
	if err := coll.load(); err != nil && !os.IsNotExist(err) {
		return nil, wrapError("NewCollection", name, "", err)
	}

	// Start background auto-compaction if enabled
	if config.AutoCompact {
		coll.compactWg.Add(1)
		go coll.compactLoop()
		log.Printf("[Collection %s] Auto-compaction enabled (threshold: %.0f%%, interval: %ds)",
			name, config.CompactThreshold*100, config.CompactMinInterval)
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
	if _, exists := c.idMapping.Map(doc.ID); exists {
		return wrapError("InsertContext", c.name, doc.ID, ErrDuplicateID)
	}

	// Add to HNSW index
	nodeID, err := c.index.Add(doc.Vector)
	if err != nil {
		return wrapError("InsertContext", c.name, doc.ID, err)
	}

	// Store document
	if err := c.storage.Put(doc); err != nil {
		// Rollback: Remove from mappings (node remains orphaned in index until rebuilt)
		// Note: HNSW doesn't support Delete, so the node will stay in the index
		// but won't be discoverable through normal operations
		log.Printf("Warning: Failed to store document %s, node %d is orphaned", doc.ID, nodeID)
		return wrapError("InsertContext", c.name, doc.ID, err)
	}

	// Update mappings
	c.idMapping.Put(doc.ID, nodeID)

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
	for _, doc := range docs {
		if err := doc.Validate(c.dimension); err != nil {
			return wrapError("InsertBatchContext", c.name, doc.ID, ErrValidationFailed)
		}
		if _, exists := c.idMapping.Map(doc.ID); exists {
			return wrapError("InsertBatchContext", c.name, doc.ID, ErrDuplicateID)
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
			return wrapError("InsertBatchContext", c.name, doc.ID, err)
		}
		c.idMapping.Put(doc.ID, nodeID)
		doc.Timestamp = time.Now()
	}

	// Store documents
	if err := c.storage.PutBatch(docs); err != nil {
		return wrapError("InsertBatchContext", c.name, "", err)
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

// DeleteBatchContext removes multiple documents with context support (logical deletion using DV)
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

		// Check if document exists
		_, exists := c.idMapping.Map(id)
		if !exists {
			continue // Skip non-existent documents
		}

		// Logical delete: Mark as deleted in Storage (DV)
		// Note: nodeToDoc mapping is preserved for search filtering (delayed cleanup)
		if err := c.storage.MarkDeleted(id); err != nil {
			lastErr = err
			continue // Continue with other deletions even if one fails
		}

		// Remove from docToNode (document is no longer accessible via ID)
		// nodeToDoc is kept for search to filter out deleted nodes
		c.idMapping.Delete(id)
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

// GetDocumentWithoutVector retrieves a document's metadata without reading
// the vector from column storage. This is an O(1) pure-memory operation
// suitable for paths that only need ID + Metadata (e.g. keyword search
// candidate resolution, filter-only lookups).
//
// The returned Document has a nil Vector. Callers must not modify the
// Metadata map (it is an internal reference protected by the INVARIANT
// that metadata maps are never mutated in-place).
func (c *Collection) GetDocumentWithoutVector(ctx context.Context, id string) (*Document, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	metadata, ok := c.storage.GetMetadataOnly(id)
	if !ok {
		return nil, ErrDocumentNotFound
	}
	return &Document{
		ID:       id,
		Vector:   nil,
		Metadata: metadata,
	}, nil
}

// GetBatchDocumentsWithoutVector is the batch version of
// GetDocumentWithoutVector. It fetches metadata for multiple documents
// without reading vectors from column storage.
func (c *Collection) GetBatchDocumentsWithoutVector(ctx context.Context, ids []string) (map[string]*Document, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	results := make(map[string]*Document, len(ids))
	for _, id := range ids {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		metadata, ok := c.storage.GetMetadataOnly(id)
		if !ok {
			continue
		}
		results[id] = &Document{
			ID:       id,
			Vector:   nil,
			Metadata: metadata,
		}
	}
	return results, nil
}

// ForEach iterates over all valid (non-deleted) documents in the collection.
// The callback receives each document; returning false stops iteration early.
// Documents are provided in an unspecified order.
//
// This includes both buffered (not yet flushed) and persisted documents,
// consistent with Get/GetContext behavior.
//
// Documents passed to the callback are cloned before delivery, but Metadata
// values are shallow-copied (slices, maps, and pointers inside Metadata are
// shared with the original). Do not mutate nested Metadata values.
//
// WARNING: The callback executes while Collection's read lock is held.
// Do NOT call Insert/Delete/Update/Compact or any other modifying method
// inside the callback, or it will deadlock.
//
// WARNING: Long-running callbacks will block all writes to the collection.
// Keep callback execution short; copy data out if post-processing is needed.
func (c *Collection) ForEach(fn func(*Document) bool) error {
	return c.ForEachContext(context.Background(), fn)
}

// ForEachContext is the context-aware version of ForEach.
// It checks context cancellation before acquiring the read lock.
func (c *Collection) ForEachContext(ctx context.Context, fn func(*Document) bool) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	return c.storage.ForEach(ctx, fn)
}

// Delete removes a document from the collection
// Deprecated: Use DeleteContext instead
func (c *Collection) Delete(id string) error {
	return c.DeleteContext(context.Background(), id)
}

// DeleteContext removes a document from the collection with context support (logical deletion using DV)
func (c *Collection) DeleteContext(ctx context.Context, id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check context cancellation
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Check if document exists
	_, exists := c.idMapping.Map(id)
	if !exists {
		return wrapError("DeleteContext", c.name, id, ErrDocumentNotFound)
	}

	// Logical delete: Mark as deleted in Storage (DV)
	// Note: nodeToDoc mapping is preserved for search filtering (delayed cleanup)
	if err := c.storage.MarkDeleted(id); err != nil {
		return wrapError("DeleteContext", c.name, id, err)
	}

	// Remove from docToNode (document is no longer accessible via ID)
	// nodeToDoc is kept for search to filter out deleted nodes
	c.idMapping.Delete(id)

	return nil
}

// Update updates a document's metadata and vector using DV (logical delete + insert)
// The old version is marked as deleted in DV, new version is inserted.
// Note: HNSW old node is preserved (orphaned) until Compact rebuilds the index.
// Deprecated: Use UpdateContext instead
func (c *Collection) Update(doc *Document) error {
	return c.UpdateContext(context.Background(), doc)
}

// UpdateContext updates a document with context support using DV
// Critical ordering: Must MarkDeleted BEFORE Insert to use old RowIndex
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

	_, exists := c.idMapping.Map(doc.ID)
	if !exists {
		return wrapError("UpdateContext", c.name, doc.ID, ErrDocumentNotFound)
	}

	// ⚠️ CRITICAL ORDERING: MarkDeleted BEFORE Insert
	// Reason: MarkDeleted uses the current docMeta.RowIndex (old value)
	// If we Insert first, docMeta.RowIndex would be updated to new value
	// causing the old version to NOT be marked as deleted
	if err := c.storage.MarkDeleted(doc.ID); err != nil {
		return wrapError("UpdateContext", c.name, doc.ID, err)
	}

	// Insert new version (creates new RowIndex and new node)
	if err := c.storage.Put(doc); err != nil {
		return wrapError("UpdateContext", c.name, doc.ID, err)
	}

	// Add new vector to HNSW index
	newNodeID, err := c.index.Add(doc.Vector)
	if err != nil {
		return wrapError("UpdateContext", c.name, doc.ID, err)
	}

	// Update mapping to point to new node
	c.idMapping.Put(doc.ID, newNodeID)
	// ⚠️ DELAYED CLEANUP: old node→doc reverse mapping is preserved
	// Reason: Concurrent search may be using oldNodeID
	// It will be filtered out by DV during search
	// Full cleanup happens during Compact
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
	_, exists := c.idMapping.Map(doc.ID)
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
// Filters out documents marked as deleted via DeletionVector (DV)
func (c *Collection) SearchContext(ctx context.Context, query []float32, k int, opts ...SearchOption) ([]SearchResult, error) {
	if len(query) != c.dimension {
		return nil, wrapError("SearchContext", c.name, "", ErrDimensionMismatch)
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

	// Use SearchWithDV to search with deletion filtering.
	// SearchWithDV searches exactly k candidates and post-filters; pass k*2
	// here to compensate for a moderate deletion rate (same behavior as before
	// the internal k*2 was moved to the caller).
	isDeleted := func(nodeID int) bool {
		docID, exists := c.idMapping.Reverse(nodeID)
		if !exists {
			return true // Orphaned node (no mapping) - treat as deleted
		}
		return c.storage.IsDeleted(docID)
	}

	hnswResults, err := c.index.SearchWithDV(query, k*2, options.EF, isDeleted)
	if err != nil {
		return nil, wrapError("SearchContext", c.name, "", err)
	}

	// Map to documents. The k*2 multiplier above causes SearchWithDV to
	// return up to k*2 filtered candidates; we only want the top k here.
	results := make([]SearchResult, 0, min(k, len(hnswResults)))
	for _, hr := range hnswResults {
		if len(results) >= k {
			break
		}

		// Check context cancellation periodically
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		docID, exists := c.idMapping.Reverse(hr.ID)
		if !exists {
			log.Printf("Warning: node %d has no document mapping (orphaned)", hr.ID)
			continue // Skip orphaned nodes
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

// SearchWithFilter performs vector search with metadata filter.
// It performs a single large HNSW search with metadata pre-filtering
// to avoid the repeated search penalty of the old expansion-loop design.
func (c *Collection) SearchWithFilter(query []float32, k int, filter Filter, opts ...SearchOption) ([]SearchResult, error) {
	return c.SearchWithFilterContext(context.Background(), query, k, filter, opts...)
}

// SearchWithFilterContext is the context-aware version of SearchWithFilter.
// Instead of the old expansion loop (multiple HNSW searches), it performs
// a single large HNSW search with an isExcluded callback that filters out
// both physically-deleted and metadata-mismatch documents in memory.
//
// The over-fetch multiplier (default 10) controls how many extra candidates
// HNSW searches for before filtering. Higher values handle higher archive
// rates without falling back to a second search.
func (c *Collection) SearchWithFilterContext(ctx context.Context, query []float32, k int, filter Filter, opts ...SearchOption) ([]SearchResult, error) {
	if len(query) != c.dimension {
		return nil, wrapError("SearchWithFilterContext", c.name, "", ErrDimensionMismatch)
	}

	options := &SearchOptions{
		EF:        0, // Use default (handled by SearchWithDV)
		OverFetch: 10,
	}
	for _, opt := range opts {
		opt(options)
	}
	if options.OverFetch < 1 {
		options.OverFetch = 1
	}
	if options.OverFetch > 20 {
		options.OverFetch = 20
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	// Check context cancellation
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Combine deletion-vector filtering and metadata filtering into a
	// single isExcluded callback. This eliminates the old expansion loop
	// by letting HNSW search once and filter in-memory.
	isExcluded := func(nodeID int) bool {
		docID, exists := c.idMapping.Reverse(nodeID)
		if !exists {
			return true
		}
		metadata, ok := c.storage.CheckVisibility(docID)
		if !ok {
			return true
		}
		if filter != nil {
			doc := &Document{ID: docID, Metadata: metadata}
			if !filter.Match(doc) {
				return true
			}
		}
		return false
	}

	// Attempt 1: single large search with over-fetch.
	hnswResults, err := c.index.SearchWithDV(query, k*options.OverFetch, options.EF, isExcluded)
	if err != nil {
		return nil, wrapError("SearchWithFilterContext", c.name, "", err)
	}

	// Minimal fallback: if the first attempt didn't yield enough results
	// and we haven't hit the max over-fetch, try once more at max budget.
	// Skip fallback when the total dataset is smaller than k (no point
	// searching harder) or the context has been cancelled.
	if len(hnswResults) < k && options.OverFetch < 20 && c.index.Len() > k*options.OverFetch {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		hnswResults, err = c.index.SearchWithDV(query, k*20, options.EF, isExcluded)
		if err != nil {
			return nil, wrapError("SearchWithFilterContext", c.name, "", err)
		}
	}

	// Map to full documents. We only need k results, so stop early
	// once we have enough — a single storage.Get call costs ~4ms, and
	// the HNSW over-fetch produces up to k*overFetch candidates (typically
	// 30-90 more than k). Stopping early saves 60+ wasted I/O calls.
	results := make([]SearchResult, 0, min(k, len(hnswResults)))
	for _, hr := range hnswResults {
		if len(results) >= k {
			break
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		docID, exists := c.idMapping.Reverse(hr.ID)
		if !exists {
			continue
		}
		doc, err := c.storage.Get(docID)
		if err != nil {
			continue
		}
		results = append(results, SearchResult{
			Document: doc,
			Distance: hr.Distance,
		})
	}
	return results, nil
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
	for _, err := range errors {
		if err != nil {
			return nil, wrapError("SearchBatch", c.name, "", err)
		}
	}

	return results, nil
}

// Count returns number of documents in collection
func (c *Collection) Count() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.idMapping.Count()
}

// CollectionStats contains collection statistics
type CollectionStats struct {
	Name         string    // Collection name
	Count        int       // Number of documents (active)
	Dimension    int       // Vector dimension
	IndexNodes   int       // Total HNSW nodes (includes orphaned)
	OrphanNodes  int       // Orphaned nodes (from updates)
	DeletedCount int       // Number of deleted documents (via DV)
	DeletionRate float64   // Deletion rate (0.0 - 1.0)
	LastUpdate   time.Time // Last modification time
}

// Stats returns collection statistics
func (c *Collection) Stats() CollectionStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Count unique node IDs in mapping (all nodes ever created)
	allNodes := make(map[int]bool)
	for _, nodeID := range c.idMapping.All() {
		allNodes[nodeID] = true
	}
	totalIndexNodes := len(allNodes)
	docCount := c.idMapping.Count()

	// Get deletion stats from storage
	deletedCount, totalCount, deletionRate := c.storage.GetDeletionStats()
	_ = totalCount // totalCount includes deleted docs, not used directly

	return CollectionStats{
		Name:         c.name,
		Count:        docCount,
		Dimension:    c.dimension,
		IndexNodes:   totalIndexNodes,
		OrphanNodes:  0, // Will need HNSW API to accurately count
		DeletedCount: deletedCount,
		DeletionRate: deletionRate,
		LastUpdate:   time.Now(),
	}
}

// Compact rebuilds the collection by removing deleted documents and optimizing storage.
// This is a blocking operation that will lock the collection during execution.
//
// The compaction process:
// 1. Retrieves all valid (non-deleted) documents from storage
// 2. Rebuilds the HNSW index with only valid documents
// 3. Rewrites the storage file to remove deleted rows
// 4. Clears the deletion vector
// 5. Rebuilds document-to-node mappings
//
// Note: This operation blocks all reads and writes during execution.
// For large collections, consider running this during maintenance windows.
func (c *Collection) Compact() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Step 0: Flush write buffer so that documents not yet persisted
	// are included in the compaction.  Without this flush, buffered
	// documents would be silently dropped when the index is rebuilt.
	if err := c.storage.Flush(); err != nil {
		return wrapError("Compact", c.name, "", fmt.Errorf("flush buffer: %w", err))
	}

	// Step 1: Get all valid documents (not marked as deleted)
	validDocs, err := c.storage.GetAllValidDocuments()
	if err != nil {
		return wrapError("Compact", c.name, "", fmt.Errorf("get valid documents: %w", err))
	}

	// Step 2: Create new HNSW index with only valid documents
	newIndex := hnsw.NewHNSW(hnsw.Config{
		Dimension:      c.dimension,
		M:              c.config.M,
		EfConstruction: c.config.EfConstruction,
		DistanceFunc:   c.config.DistanceFunc,
		Adaptive:       c.config.Adaptive,
		ExpectedSize:   len(validDocs),
	})

	// Build new mappings
	newDocToNode := make(map[string]int)
	newNodeToDoc := make(map[int]string)

	for _, doc := range validDocs {
		nodeID, err := newIndex.Add(doc.Vector)
		if err != nil {
			return wrapError("Compact", c.name, doc.ID, fmt.Errorf("add to index: %w", err))
		}
		newDocToNode[doc.ID] = nodeID
		newNodeToDoc[nodeID] = doc.ID
	}

	// Step 3: Rewrite storage (remove deleted rows)
	if err := c.storage.Rewrite(validDocs); err != nil {
		return wrapError("Compact", c.name, "", fmt.Errorf("rewrite storage: %w", err))
	}

	// Step 4: Clear deletion vector (deleted rows are now physically removed)
	c.storage.ClearDeletionVector()

	// Step 5: Atomic replacement of index and mappings
	c.index = newIndex
	c.idMapping.Replace(newDocToNode, newNodeToDoc)

	return nil
}

// compactLoop is the background goroutine for auto-compaction
func (c *Collection) compactLoop() {
	defer c.compactWg.Done()

	// Initial delay before first check (10 seconds after startup)
	select {
	case <-time.After(10 * time.Second):
	case <-c.compactStopCh:
		return
	}

	ticker := time.NewTicker(30 * time.Second) // Check every 30 seconds
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if should, reason := c.shouldAutoCompact(); should {
				c.doAutoCompact(reason)
			}

		case <-c.compactTriggerCh:
			// Manual trigger
			c.doAutoCompact("manual trigger")

		case <-c.compactStopCh:
			return
		}
	}
}

// shouldAutoCompact checks if compaction should be triggered
func (c *Collection) shouldAutoCompact() (bool, string) {
	c.compactMu.RLock()
	defer c.compactMu.RUnlock()

	// Check if auto-compact is enabled
	if !c.config.AutoCompact {
		return false, "auto-compact disabled"
	}

	// Check if already compacting
	if c.compacting {
		return false, "already compacting"
	}

	// Check minimum interval
	minInterval := time.Duration(c.config.CompactMinInterval) * time.Second
	if time.Since(c.lastCompactTime) < minInterval {
		return false, "too frequent"
	}

	// Check maximum interval (force compact if exceeded)
	if c.config.CompactMaxInterval > 0 {
		maxInterval := time.Duration(c.config.CompactMaxInterval) * time.Second
		if time.Since(c.lastCompactTime) > maxInterval {
			return true, fmt.Sprintf("max interval reached (%.1fh > %.1fh)",
				time.Since(c.lastCompactTime).Hours(), maxInterval.Hours())
		}
	}

	// Check deletion rate
	stats := c.Stats()
	if stats.DeletionRate >= c.config.CompactThreshold {
		return true, fmt.Sprintf("deletion rate %.2f%% >= %.2f%%",
			stats.DeletionRate*100, c.config.CompactThreshold*100)
	}

	return false, "no condition met"
}

// doAutoCompact performs the actual compaction with double-check to prevent concurrent execution.
func (c *Collection) doAutoCompact(reason string) {
	// Double-check with write lock to prevent race condition where multiple
	// goroutines pass shouldAutoCompact() before any sets compacting=true
	c.compactMu.Lock()
	if c.compacting {
		c.compactMu.Unlock()
		log.Printf("[Collection %s] Auto-compaction skipped: already in progress", c.name)
		return
	}
	c.compacting = true
	c.compactMu.Unlock()

	log.Printf("[Collection %s] Auto-compaction started: %s", c.name, reason)
	start := time.Now()

	err := c.Compact()

	c.compactMu.Lock()
	c.compacting = false
	c.lastCompactTime = time.Now()
	c.compactMu.Unlock()

	if err != nil {
		log.Printf("[Collection %s] Auto-compaction failed: %v", c.name, err)
	} else {
		log.Printf("[Collection %s] Auto-compaction completed in %v", c.name, time.Since(start))
	}
}

// GetCompactStatus returns the current compaction status
func (c *Collection) GetCompactStatus() CompactStatus {
	c.compactMu.RLock()
	defer c.compactMu.RUnlock()

	if !c.config.AutoCompact {
		return CompactStatus{
			State:   CompactIdle,
			Message: "Auto-compaction disabled",
		}
	}

	if c.compacting {
		return CompactStatus{
			State:    CompactCompacting,
			Message:  "Compaction in progress",
			Progress: 0.5, // Approximate
		}
	}

	nextRun := c.lastCompactTime.Add(time.Duration(c.config.CompactMinInterval) * time.Second)
	return CompactStatus{
		State:       CompactIdle,
		Message:     "Waiting for next check",
		NextRunTime: nextRun,
	}
}

// TriggerCompact manually triggers a compaction if not already running
func (c *Collection) TriggerCompact() error {
	if !c.config.AutoCompact {
		return fmt.Errorf("auto-compaction is disabled")
	}

	select {
	case c.compactTriggerCh <- struct{}{}:
		return nil
	default:
		return fmt.Errorf("compact trigger channel full, try again later")
	}
}

// Save persists collection to disk
func (c *Collection) Save() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Save HNSW index
	indexPath := filepath.Join(c.path, "index")
	if err := saveHNSWIndex(c.index, indexPath); err != nil {
		return wrapError("Save", c.name, "", err)
	}

	// Save mappings
	mappingsPath := filepath.Join(c.path, "mappings.json")
	if err := c.saveMappings(mappingsPath); err != nil {
		return wrapError("Save", c.name, "", err)
	}

	// Flush document storage
	if err := c.storage.Flush(); err != nil {
		return wrapError("Save", c.name, "", err)
	}

	return nil
}

// Close closes the collection
func (c *Collection) Close() error {
	// Stop background auto-compaction if enabled
	if c.config.AutoCompact {
		close(c.compactStopCh)
		
		// Wait for compaction to finish with timeout
		done := make(chan struct{})
		go func() {
			c.compactWg.Wait()
			close(done)
		}()
		
		select {
		case <-done:
			// Gracefully stopped
		case <-time.After(30 * time.Second):
			log.Printf("[Collection %s] Warning: Auto-compaction did not stop in time", c.name)
		}
	}

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
		loadedIndex, err := loadHNSWIndex(indexPath)
		if err != nil {
			return wrapError("load", c.name, "", ErrIndexCorrupted)
		}
		c.index = loadedIndex
	}

	// Load mappings
	mappingsPath := filepath.Join(c.path, "mappings.json")
	if err := c.loadMappings(mappingsPath); err != nil && !os.IsNotExist(err) {
		return wrapError("load", c.name, "", err)
	}

	return nil
}

func (c *Collection) saveMappings(path string) error {
	data := map[string]interface{}{
		"docToNode": c.idMapping.All(),
		"nodeToDoc": c.idMapping.AllReverse(),
	}

	bytes, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}

	if err := os.WriteFile(path, bytes, 0644); err != nil {
		return err
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
		return ErrIndexCorrupted
	}

	newDocToNode := make(map[string]int)
	newNodeToDoc := make(map[int]string)

	// Load docToNode
	if docToNodeRaw, ok := mappings["docToNode"].(map[string]interface{}); ok {
		for k, v := range docToNodeRaw {
			if nodeID, ok := v.(float64); ok {
				newDocToNode[k] = int(nodeID)
			}
		}
	}

	// Load nodeToDoc
	if nodeToDocRaw, ok := mappings["nodeToDoc"].(map[string]interface{}); ok {
		for k, v := range nodeToDocRaw {
			if docID, ok := v.(string); ok {
				if nodeID, ok := parseIntKey(k); ok {
					newNodeToDoc[nodeID] = docID
				}
			}
		}
	}

	c.idMapping.Replace(newDocToNode, newNodeToDoc)
	return nil
}

// parseIntKey converts string key to int (JSON only supports string keys)
func parseIntKey(s string) (int, bool) {
	var i int
	_, err := fmt.Sscanf(s, "%d", &i)
	return i, err == nil
}
