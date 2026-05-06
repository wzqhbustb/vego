// Package memory provides an embedded Agent memory service built on top of Vego.
//
// It offers persistent, semantically-searchable memory for AI agents:
// conversation facts are stored as vector embeddings and retrieved via a
// hybrid search pipeline combining HNSW vector search, BM25 keyword search,
// and Reciprocal Rank Fusion (RRF).
//
// # Quick Start
//
//	store, err := memory.Open("./my_agent_memory",
//	    memory.WithLLM(apiKey, "", "gpt-4o-mini", 0.1),
//	    memory.WithEmbedding(apiKey, "", "text-embedding-3-small", 1536),
//	)
//	if err != nil { log.Fatal(err) }
//	defer store.Close()
//
//	// Store a memory
//	mem, err := store.Store(ctx, "User prefers dark mode", []string{"preference"})
//
//	// Search memories
//	results, err := store.Search(ctx, "user preferences",
//	    memory.Limit(5),
//	    memory.MinScore(0.3),
//	)
//
//	// Update a memory (archive-and-create)
//	updated, err := store.Update(ctx, mem.ID, "User prefers dark mode with blue accent", []string{"preference"})
//
//	// Soft-delete a memory
//	err = store.Delete(ctx, mem.ID)
//
// # Architecture
//
// MemoryStore wraps a Vego collection with three additional layers:
//   - LLM client: fact extraction from conversations and reconciliation decisions
//   - Embedding client: text → vector conversion (OpenAI-compatible API)
//   - Inverted index: in-memory BM25 full-text search (rebuilt on Open)
//
// Each [Memory] is stored as a Vego Document with the full Memory struct
// serialized as JSON in the document metadata. State and type fields are
// additionally stored as top-level metadata keys to enable filtered search.
//
// # Memory Lifecycle
//
// A memory transitions through the following states:
//
//	active → paused  (via Pause; excluded from search, still readable by Get)
//	paused → active  (via Resume; re-indexed for search)
//	active → deleted (via Delete; excluded from search, still readable by Get)
//	active → archived (via Update; superseded by a new memory)
//
// Use [MemoryStore.Compact] to physically remove deleted and archived memories,
// reclaiming disk space. Active and paused memories are preserved.
//
// # Search Pipeline
//
// The default hybrid search runs a 10-stage pipeline:
//  1. Temporal query normalization
//  2. Vector search (HNSW nearest neighbors)
//  3. Keyword search (BM25 via inverted index)
//  4. RRF score fusion
//  5. Dual-channel bonus (optional; for results hit by both channels)
//  6. Vector similarity weighting (optional)
//  7. Second-hop expansion (associative recall for top-N results)
//  8. Pinned memory boost
//  9. Recency boost
//  10. Gap-stop truncation
//
// Pure vector search is available via [EnableHybrid](false).
//
// # Ingestion
//
// Two ingestion modes are supported via [MemoryStore.Ingest]:
//   - ModeNormal: LLM extracts structured facts → [MemoryStore.Reconcile] merges
//     with existing memories (ADD / UPDATE / DELETE / NOOP decisions)
//   - ModeRaw: direct session storage with content-hash deduplication, no LLM needed
//
// # Error Handling
//
// Errors from this package fall into the following categories:
//
// Validation errors (not retryable — fix input before retrying):
//   - Empty or nil context: "context must not be nil"
//   - Empty ID: "id must not be empty"
//   - Content too long: "content length %d exceeds maximum %d"
//   - Too many tags: "tag count %d exceeds maximum %d"
//   - Empty content: "content must not be empty"
//   - Invalid state transition: "cannot delete memory %s: state is %s"
//   - Batch too large: "bulk size %d exceeds maximum %d"
//   - Invalid config: "invalid config: ..."
//
// Infrastructure errors (retryable — may succeed on retry):
//   - Embedding API failure: wraps HTTP errors as "embed: ..."
//   - LLM API failure: wraps HTTP errors as "llm request failed"
//   - Disk I/O errors: wraps Vego storage errors as "insert: ...", "update state: ..."
//
// Data errors (not retryable — indicates data corruption):
//   - Corrupt document: "unmarshal memory %s: ..."
//   - Missing metadata: "document %s: missing or empty _data field"
//
// All infrastructure errors are wrapped with context using fmt.Errorf and %w,
// so callers can use [errors.Is] and [errors.As] to inspect the root cause.
// For example, to check if a Get failed because the ID was not found:
//
//	mem, err := store.Get(ctx, id)
//	if err != nil {
//	    // Check wrapped error from Vego layer
//	    if strings.Contains(err.Error(), "not found") {
//	        // handle not-found
//	    }
//	}
//
// # Thread Safety
//
// [MemoryStore] is safe for concurrent use. Write operations (Store, Update,
// Delete, Pause, Resume, StoreBatch, Bootstrap) are serialized via an
// internal mutex. Read operations (Get, Search, List, Stats) are lock-free
// at the memory layer and use Vego's internal read-write coordination.
//
// # Configuration
//
// Use functional options ([Option]) to customize behavior:
//
//	store, err := memory.Open(path,
//	    memory.WithLLM(apiKey, baseURL, model, temperature),
//	    memory.WithEmbedding(apiKey, baseURL, model, dims),
//	    memory.WithSearchLimit(20),
//	    memory.WithMinScore(0.4),
//	    memory.WithGapStop(0.5),
//	)
//
// See [DefaultConfig] for all default values.
package memory
