# Vego API Review Report

**Review Date**: 2026-02-11  
**Scope**: `vego` package public API  
**Status**: Phase 0 - Pre-implementation Review

---

## Executive Summary

This document captures API design issues identified during the pre-Phase-1 review. Items marked as **P0** should be addressed before or during Phase 1 implementation.

**Key Decisions**:
- Context support will be added gradually (non-breaking)
- Search results remain value types (not pointers)
- Pagination uses cursor-based approach (offset not recommended for vector search)
- Iterator uses Go-style channel pattern

---

## 1. Context Support Missing ⭐ P0

### Problem
All API methods lack `context.Context` support, making it impossible to:
- Set timeouts for long-running operations
- Cancel operations gracefully
- Implement distributed tracing
- Pass request-scoped values

### Current API
```go
func (c *Collection) Search(query []float32, k int, opts ...SearchOption) ([]SearchResult, error)
func (c *Collection) Insert(doc *Document) error
func (c *Collection) Get(id string) (*Document, error)
```

### Recommended API (Gradual Migration)

```go
// Phase 0: Add Context methods alongside existing ones
func (c *Collection) Get(id string) (*Document, error)                    // Existing
func (c *Collection) GetContext(ctx context.Context, id string) (*Document, error)  // NEW

func (c *Collection) Insert(doc *Document) error                          // Existing
func (c *Collection) InsertContext(ctx context.Context, doc *Document) error        // NEW

func (c *Collection) Search(query []float32, k int, opts ...SearchOption) (SearchResults, error)             // Existing
func (c *Collection) SearchContext(ctx context.Context, query []float32, k int, opts ...SearchOption) (SearchResults, error)  // NEW

// Implementation: existing methods delegate to Context versions
func (c *Collection) Get(id string) (*Document, error) {
    return c.GetContext(context.Background(), id)
}
```

### Deprecation Timeline
- **Phase 0-2**: Both APIs coexist, old methods marked `// Deprecated: Use GetContext`
- **Phase 3 (v1.0)**: Remove non-Context methods

### Impact
- **Breaking Change**: No (gradual migration)
- **Effort**: Medium
- **Timeline**: Phase 0

---

## 2. Batch Operations Incomplete ⭐ P0

### Problem
Only `InsertBatch` is exposed. Other batch operations are implemented internally but not accessible to users.

### Missing APIs
| Method | Internal Status | Priority |
|--------|----------------|----------|
| `GetBatch(ids []string)` | Implemented in storage | P0 |
| `DeleteBatch(ids []string)` | Not implemented | P0 |
| `UpsertBatch(docs []*Document)` | Not implemented | P1 |

### Recommended API
```go
// GetBatch retrieves multiple documents by IDs
// Returns a map of id -> document (missing documents are omitted)
func (c *Collection) GetBatch(ids []string) (map[string]*Document, error)
func (c *Collection) GetBatchContext(ctx context.Context, ids []string) (map[string]*Document, error)
```

### Use Cases
- **GetBatch**: Loading search results efficiently (Phase 1 optimization)

### Atomicity Note
Batch operations should be atomic where possible. If any operation fails, partial results may remain. For full atomicity, use Batch transaction API (see Section 9).

---

## 4. Opaque Error Types ⭐ P0

### Problem
Current errors are plain `fmt.Errorf` strings, making it impossible for users to:
- Distinguish between "not found" and "internal error"
- Implement retry logic based on error type
- Provide localized error messages

### Recommended Error Types
```go
package vego

import "errors"

// Sentinel errors for common cases
var (
    ErrDocumentNotFound   = errors.New("document not found")
    ErrDuplicateID        = errors.New("document already exists")
    ErrDimensionMismatch  = errors.New("vector dimension mismatch")
    ErrCollectionNotFound = errors.New("collection not found")
    ErrCollectionClosed   = errors.New("collection is closed")
    ErrInvalidFilter      = errors.New("invalid filter expression")
    ErrIndexCorrupted     = errors.New("index corrupted")
)

// Error provides structured error information
type Error struct {
    Op    string // Operation: "Get", "Insert", "Search"
    Coll  string // Collection name
    DocID string // Document ID (if applicable)
    Err   error  // Underlying error
}

func (e *Error) Error() string {
    if e.DocID != "" {
        return fmt.Sprintf("vego: %s on collection %s (doc %s) failed: %v", e.Op, e.Coll, e.DocID, e.Err)
    }
    return fmt.Sprintf("vego: %s on collection %s failed: %v", e.Op, e.Coll, e.Err)
}

func (e *Error) Unwrap() error { return e.Err }

// Helper functions for error checking
func IsNotFound(err error) bool {
    return errors.Is(err, ErrDocumentNotFound)
}

func IsDuplicate(err error) bool {
    return errors.Is(err, ErrDuplicateID)
}

func IsDimensionMismatch(err error) bool {
    return errors.Is(err, ErrDimensionMismatch)
}
```

### Usage Example
```go
doc, err := coll.GetContext(ctx, "doc-123")
if err != nil {
    if vego.IsNotFound(err) {
        // Handle not found gracefully
        return nil, nil
    }
    // Handle other errors
    return nil, err
}
```

---

## 5. Iterator/Scan Interface ⭐ P1

### Problem
No way to iterate over all documents in a collection. Common for:
- Bulk export
- Data migration
- Index rebuilding
- Statistics collection

### Recommended API (Channel-based, Go-style)
```go
// DocumentResult represents either a document or an error
type DocumentResult struct {
    Document *Document
    Err      error
}

// Iter returns a channel for iterating over all documents
// Channel is closed when iteration completes or context is cancelled
func (c *Collection) Iter(ctx context.Context) <-chan DocumentResult

// IterWithFilter returns an iterator with metadata filtering
func (c *Collection) IterWithFilter(ctx context.Context, filter Filter) <-chan DocumentResult
```

### Usage Example
```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
defer cancel()

for result := range coll.Iter(ctx) {
    if result.Err != nil {
        log.Printf("Iteration error: %v", result.Err)
        continue
    }
    // Process document
    process(result.Document)
}
```

### Implementation Notes
- Channel buffer size: 100 (configurable)
- Internal pagination (1,000 documents per batch)
- Respects context cancellation between batches
- Automatically closes channel on completion or error

---

## 6. Search Results Enhancement ⭐ P1

### Problem
`SearchResult` only contains document and distance. Users cannot determine:
- Total number of matching documents (for pagination)
- Whether results were truncated
- Search execution statistics

### Current API
```go
type SearchResult struct {
    Document *Document
    Distance float32
}
```

### Recommended API (Value Type, Not Pointer)
```go
// SearchResult remains unchanged (single result)
type SearchResult struct {
    Document *Document
    Distance float32
}

// SearchResults contains results and metadata (VALUE TYPE)
type SearchResults struct {
    Results    []SearchResult
    Total      int           // Total documents that matched (for pagination)
    Scanned    int           // Number of documents scanned
    DurationMs int64         // Search execution time
    NextCursor *SearchCursor // For pagination (nil if no more results)
}

// Search returns value type (not pointer)
func (c *Collection) SearchContext(ctx context.Context, query []float32, k int, opts ...SearchOption) (SearchResults, error)

// Legacy method (delegates to Context version)
func (c *Collection) Search(query []float32, k int, opts ...SearchOption) (SearchResults, error)
```

### Why Value Type?
- Avoids heap allocation (better GC performance)
- Immutable (caller can't modify internal state)
- Zero value is valid (empty results)
- Consistent with Go standard library patterns

---

## 7. Pagination Support ⭐ P1

### Problem
`Search` returns top-k results but no way to get subsequent pages.

### Why Not Offset-Based?
Vector search results are unstable:
- Documents with same distance may have non-deterministic order
- Offset 100 might return different results on each call
- HNSW algorithm doesn't guarantee stable ordering

### Recommended API (Cursor-based)
```go
type SearchCursor struct {
    Query        []float32     // Original query
    LastDistance float32       // Distance of last result
    LastID       string        // Document ID of last result (tie-breaker)
    Filter       Filter        // Preserved filter
    EF           int           // Preserved EF setting
}

// Search includes cursor for next page (if available)
type SearchResults struct {
    Results    []SearchResult
    Total      int
    Scanned    int
    DurationMs int64
    NextCursor *SearchCursor  // nil if no more results
}

// SearchNext continues from cursor
func (c *Collection) SearchNext(ctx context.Context, cursor *SearchCursor, k int) (SearchResults, error)
```

### Usage Example
```go
// First page
results, err := coll.SearchContext(ctx, query, 10)
if err != nil {
    return err
}
process(results.Results)

// Subsequent pages
for results.NextCursor != nil {
    results, err = coll.SearchNext(ctx, results.NextCursor, 10)
    if err != nil {
        return err
    }
    process(results.Results)
}
```

---

## 8. Metadata Type Safety ⭐ P2

### Problem
`Metadata map[string]interface{}` is flexible but error-prone:
- No compile-time type checking
- Requires type assertions at runtime
- Inconsistent types across documents

### Recommended Helper Methods
```go
func (d *Document) GetString(key string) (string, bool)
func (d *Document) GetInt(key string) (int, bool)
func (d *Document) GetFloat(key string) (float64, bool)
func (d *Document) GetBool(key string) (bool, bool)
func (d *Document) GetTime(key string) (time.Time, bool)

// With default values
func (d *Document) GetStringOr(key string, defaultVal string) string
func (d *Document) GetIntOr(key string, defaultVal int) int
func (d *Document) GetFloatOr(key string, defaultVal float64) float64
```

### Usage Example
```go
// Before (verbose, error-prone)
if val, ok := doc.Metadata["count"].(int); ok {
    count = val
} else {
    count = 0
}

// After (clean, safe)
count := doc.GetIntOr("count", 0)
```

### Future: Schema Validation (Phase 3+)
```go
type MetadataSchema struct {
    Fields map[string]MetadataField
}

type MetadataField struct {
    Type     string // "string", "int", "float", "bool", "time"
    Required bool
    Indexed  bool   // For future BTree index
}

func (c *Collection) SetMetadataSchema(schema *MetadataSchema) error
```

---

## 9. Batch Transaction Support ⭐ P1

### Problem
Multiple operations cannot be grouped into atomic transactions.

### Recommended API
```go
// Batch provides atomic multi-operation support
type Batch struct {
    coll *Collection
    ops  []batchOp
}

// Create a new batch
func (c *Collection) NewBatch() *Batch

// Add operations to batch
func (b *Batch) Insert(doc *Document)
func (b *Batch) Delete(id string)
func (b *Batch) Update(doc *Document)
func (b *Batch) Upsert(doc *Document)

// Commit executes all operations atomically (best effort)
// Returns error if any operation fails
func (b *Batch) Commit(ctx context.Context) error

// Rollback cancels pending operations (before Commit)
func (b *Batch) Rollback()
```

### Usage Example
```go
batch := coll.NewBatch()
batch.Insert(doc1)
batch.Insert(doc2)
batch.Delete("old-id")

if err := batch.Commit(ctx); err != nil {
    batch.Rollback()
    return err
}
```

### Atomicity Guarantees
- **Within HNSW**: Operations are atomic (single lock)
- **Within Storage**: Operations are atomic (single flush)
- **Cross-layer**: Best effort. If HNSW succeeds but Storage fails, HNSW changes persist (orphan nodes can be cleaned by RebuildIndex)

---

## 10. Observability Support ⭐ P2

### Metrics Interface
```go
type Metrics interface {
    RecordSearch(duration time.Duration, scanned int)
    RecordCacheHit()
    RecordCacheMiss()
    RecordIndexBuild(duration time.Duration, nodes int)
}

func (c *Collection) WithMetrics(m Metrics) *Collection
```

### Health Check
```go
type HealthStatus struct {
    Status       string  // "healthy", "degraded", "unhealthy"
    IndexSize    int
    CacheHitRate float64
    LastError    error
}

func (c *Collection) Health() HealthStatus
```

---

## Priority Matrix

| Issue | Priority | Breaking Change | Effort | Phase |
|-------|----------|-----------------|--------|-------|
| Context Support | P0 | **No** (gradual) | Medium | Phase 0 |
| Batch Operations | P0 | No | Low | Phase 0 |
| Error Types | P0 | No | Medium | Phase 0 |
| Cache Configuration | P0 | No | Low | Phase 1 |
| Config Validation | P0 | No | Low | Phase 0 |
| Iterator Interface | P1 | No | Medium | Phase 1 |
| Pagination (Cursor) | P1 | No | Medium | Phase 1 |
| Search Results | P1 | **Yes** (return type) | Low | Phase 0 |
| Batch Transaction | P1 | No | Medium | Phase 1 |
| Metadata Helpers | P2 | No | Low | Phase 2 |
| Metrics | P2 | No | Low | Phase 2 |
| Health Check | P2 | No | Low | Phase 2 |

---

## Implementation Roadmap

### Phase 0 (Foundation)
1. Add `GetBatch`, `DeleteBatch` methods
2. Define structured error types (`ErrDocumentNotFound`, etc.)
3. Add `Config.Validate()`
4. Change `Search` return type to `SearchResults` (value)
5. Add Context methods alongside existing ones

### Phase 1 (Storage Hardening)
1. Implement structured `CacheConfig`, `IndexConfig`, `IOConfig`
2. Implement Row Index and LRU Cache
3. Add iterator interface
4. Add cursor-based pagination
5. Add Batch transaction support

### Phase 2 (MVP)
1. Add metadata type helpers
2. Add metrics interface
3. Add health check
4. Deprecate non-Context methods

### Phase 3 (v1.0)
1. Remove deprecated methods (breaking change)
2. Consider metadata schema validation

---

## Migration Guide

### Context Migration
```go
// Before (still works during Phase 0-2)
doc, err := coll.Get("doc-123")

// After (recommended)
doc, err := coll.GetContext(ctx, "doc-123")

// For simple scripts/tests
import "context"
doc, err := coll.GetContext(context.Background(), "doc-123")
```

### Search Results Migration
```go
// Before
results, err := coll.Search(query, 10)
for _, r := range results {
    // r is SearchResult
}

// After
results, err := coll.Search(query, 10)
for _, r := range results.Results {
    // r is SearchResult
}
fmt.Printf("Total: %d, Scanned: %d\n", results.Total, results.Scanned)
```

### Error Handling Migration
```go
// Before
if err != nil {
    if strings.Contains(err.Error(), "not found") {
        // handle not found
    }
}

// After
if err != nil {
    if vego.IsNotFound(err) {
        // handle not found
    }
}
```

---

## Appendix: Current vs Recommended API Summary

| Current | Recommended | Phase | Breaking |
|---------|-------------|-------|----------|
| `Get(id string)` | `Get(id string)` + `GetContext(ctx, id)` | 0 | No |
| `Insert(doc)` | `Insert(doc)` + `InsertContext(ctx, doc)` | 0 | No |
| `Search(query, k, opts...)` | `Search(query, k, opts...)` + `SearchContext(ctx, query, k, opts...)` | 0 | No |
| `Search` returns `[]SearchResult` | `Search` returns `SearchResults` (value) | 0 | **Yes** |
| - | `GetBatch(ids)` / `GetBatchContext(ctx, ids)` | 0 | No |
| - | `DeleteBatch(ids)` / `DeleteBatchContext(ctx, ids)` | 0 | No |
| `Config{...}` | `Config{Cache: CacheConfig{}, Index: IndexConfig{}}` | 1 | No |
| `fmt.Errorf(...)` | `vego.ErrDocumentNotFound`, `IsNotFound(err)` | 0 | No |
| - | `Iter(ctx)` channel | 1 | No |
| - | `SearchNext(ctx, cursor, k)` | 1 | No |
| - | `NewBatch()` / `Commit(ctx)` | 1 | No |

---

*This document should be reviewed and updated after each phase completion.*
