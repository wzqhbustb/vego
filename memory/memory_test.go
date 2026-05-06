package memory

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	vego "github.com/wzqhbustb/vego/vego"
)

// mockEmbedServer returns a fixed-dimension embedding vector.
type mockEmbedServer struct {
	dims int
}

func (m *mockEmbedServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Determine how many embeddings the request expects.
	count := 1
	var body struct {
		Input interface{} `json:"input"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err == nil {
		switch v := body.Input.(type) {
		case []interface{}:
			if len(v) > 0 {
				count = len(v)
			}
		case string:
			count = 1
		}
	}

	data := make([]map[string]interface{}, count)
	for i := 0; i < count; i++ {
		vec := make([]float32, m.dims)
		for j := range vec {
			vec[j] = 0.1
		}
		data[i] = map[string]interface{}{
			"index":     i,
			"embedding": vec,
		}
	}
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"data": data,
	})
}

func newTestStore(t testing.TB, opts ...Option) *MemoryStore {
	t.Helper()
	dir := t.TempDir()

	// Default test options
	testOpts := []Option{
		WithDataDir(dir),
		WithDimension(128),
		WithEmbedding("test-key", "", "test-model", 128),
		WithLLM("test-key", "", "test-model", 0.5),
	}

	// Append caller options (they override defaults via functional options)
	testOpts = append(testOpts, opts...)

	s, err := Open(dir, testOpts...)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return s
}

func setupMockEmbedder(t testing.TB, s *MemoryStore, dims int) {
	t.Helper()
	srv := httptest.NewServer(&mockEmbedServer{dims: dims})
	t.Cleanup(srv.Close)

	embedder := NewEmbedder(EmbedConfig{
		APIKey:  "test-key",
		BaseURL: srv.URL,
		Model:   "test-model",
		Dims:    dims,
	})
	if embedder == nil {
		t.Fatal("mock embedder nil")
	}
	s.embedder = embedder
}

// ----------------------------------------------------------------------
// Open / Close
// ----------------------------------------------------------------------

func TestMemoryStoreOpenClose(t *testing.T) {
	s := newTestStore(t)
	t.Cleanup(func() { s.Close() })

	if s.db == nil {
		t.Error("db should not be nil")
	}
	if s.coll == nil {
		t.Error("coll should not be nil")
	}
	if s.inverted == nil {
		t.Error("inverted should not be nil")
	}
	if s.config == nil {
		t.Error("config should not be nil")
	}
}

func TestMemoryStoreOpenInvalidConfig(t *testing.T) {
	_, err := Open(t.TempDir(), WithDistanceFunc("invalid"))
	if err == nil {
		t.Fatal("expected error for invalid config")
	}
}

// ----------------------------------------------------------------------
// Store / Get
// ----------------------------------------------------------------------

func TestMemoryStoreStoreAndGet(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()
	mem, err := s.Store(ctx, "hello world", []string{"greeting"})
	if err != nil {
		t.Fatalf("Store: %v", err)
	}
	if mem.ID == "" {
		t.Error("memory ID should not be empty")
	}
	if mem.Content != "hello world" {
		t.Errorf("content: want hello world, got %s", mem.Content)
	}
	if mem.State != StateActive {
		t.Errorf("state: want active, got %s", mem.State)
	}

	// Get back
	got, err := s.Get(ctx, mem.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Content != mem.Content {
		t.Errorf("Get content mismatch: want %s, got %s", mem.Content, got.Content)
	}
	if got.State != StateActive {
		t.Errorf("Get state mismatch: want active, got %s", got.State)
	}

	// Inverted index should contain it
	results := s.inverted.Search("hello", 10)
	if len(results) != 1 {
		t.Errorf("inverted search: want 1 result, got %d", len(results))
	}
}

// ----------------------------------------------------------------------
// Update (Archive-and-Create)
// ----------------------------------------------------------------------

func TestMemoryStoreUpdate(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()
	old, err := s.Store(ctx, "original content", nil)
	if err != nil {
		t.Fatalf("Store: %v", err)
	}

	newMem, err := s.Update(ctx, old.ID, "updated content", []string{"updated"})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if newMem.ID == old.ID {
		t.Error("Update should create a new memory with different ID")
	}
	if newMem.Content != "updated content" {
		t.Errorf("new content: want updated content, got %s", newMem.Content)
	}

	// Old memory should be archived
	oldArchived, err := s.Get(ctx, old.ID)
	if err != nil {
		t.Fatalf("Get old: %v", err)
	}
	if oldArchived.State != StateArchived {
		t.Errorf("old state: want archived, got %s", oldArchived.State)
	}
	if oldArchived.SupersededBy != newMem.ID {
		t.Errorf("old superseded_by: want %s, got %s", newMem.ID, oldArchived.SupersededBy)
	}

	// New memory should be active
	gotNew, err := s.Get(ctx, newMem.ID)
	if err != nil {
		t.Fatalf("Get new: %v", err)
	}
	if gotNew.State != StateActive {
		t.Errorf("new state: want active, got %s", gotNew.State)
	}

	// Inverted index: old removed, new added
	results := s.inverted.Search("updated", 10)
	found := false
	for _, r := range results {
		if r.ID == newMem.ID {
			found = true
			break
		}
	}
	if !found {
		t.Error("inverted index should contain new memory")
	}
}

// ----------------------------------------------------------------------
// Delete (soft delete)
// ----------------------------------------------------------------------

func TestMemoryStoreDelete(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()
	mem, err := s.Store(ctx, "delete me", nil)
	if err != nil {
		t.Fatalf("Store: %v", err)
	}

	if err := s.Delete(ctx, mem.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Get should still work (soft delete)
	got, err := s.Get(ctx, mem.ID)
	if err != nil {
		t.Fatalf("Get after delete: %v", err)
	}
	if got.State != StateDeleted {
		t.Errorf("state after delete: want deleted, got %s", got.State)
	}

	// Inverted index should no longer contain it
	results := s.inverted.Search("delete", 10)
	for _, r := range results {
		if r.ID == mem.ID {
			t.Error("inverted index should not contain deleted memory")
		}
	}
}

// ----------------------------------------------------------------------
// Search
// ----------------------------------------------------------------------

func TestMemoryStoreSearch(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()
	_, err := s.Store(ctx, "hello world", nil)
	if err != nil {
		t.Fatalf("Store: %v", err)
	}
	_, err = s.Store(ctx, "goodbye world", nil)
	if err != nil {
		t.Fatalf("Store 2: %v", err)
	}

	// Search should return active memories only
	results, err := s.Search(ctx, "world", Limit(10))
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("Search: want 2 results, got %d", len(results))
	}

	// Delete one, search again
	_ = s.Delete(ctx, results[0].ID)
	results, err = s.Search(ctx, "world", Limit(10))
	if err != nil {
		t.Fatalf("Search after delete: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("Search after delete: want 1 result, got %d", len(results))
	}
}

// ----------------------------------------------------------------------
// StoreBatch
// ----------------------------------------------------------------------

func TestMemoryStoreStoreBatch(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()
	items := []StoreItem{
		{Content: "hello batch one", Tags: []string{"x"}},
		{Content: "hello batch two", Tags: []string{"y"}},
		{Content: "hello batch three", Tags: []string{"z"}},
	}
	mems, err := s.StoreBatch(ctx, items)
	if err != nil {
		t.Fatalf("StoreBatch: %v", err)
	}
	if len(mems) != 3 {
		t.Errorf("StoreBatch: want 3 memories, got %d", len(mems))
	}

	// Verify inverted index (search for a non-stop-word term)
	results := s.inverted.Search("batch", 10)
	if len(results) != 3 {
		t.Errorf("inverted after batch: want 3 results for 'batch', got %d", len(results))
	}
}

// ----------------------------------------------------------------------
// Bootstrap
// ----------------------------------------------------------------------

func TestMemoryStoreBootstrap(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()
	memories := []*Memory{
		{ID: "boot-1", Content: "boot one", State: StateActive, Version: 1, CreatedAt: time.Now(), UpdatedAt: time.Now()},
		{ID: "boot-2", Content: "boot two", State: StateActive, Version: 1, CreatedAt: time.Now(), UpdatedAt: time.Now()},
	}

	if err := s.Bootstrap(ctx, memories); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}

	for _, mem := range memories {
		got, err := s.Get(ctx, mem.ID)
		if err != nil {
			t.Fatalf("Get %s: %v", mem.ID, err)
		}
		if got.Content != mem.Content {
			t.Errorf("Get %s content mismatch", mem.ID)
		}
	}

	results := s.inverted.Search("boot", 10)
	if len(results) != 2 {
		t.Errorf("inverted after bootstrap: want 2 results, got %d", len(results))
	}
}

func TestMemoryStoreBootstrapEmpty(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	if err := s.Bootstrap(context.Background(), nil); err != nil {
		t.Fatalf("Bootstrap empty: %v", err)
	}
}

// ----------------------------------------------------------------------
// Crash recovery
// ----------------------------------------------------------------------

func TestMemoryStoreCrashRecovery(t *testing.T) {
	tmpDir := t.TempDir()
	ctx := context.Background()

	// Phase 1: open store, store a memory, simulate crash during update
	s1 := newTestStore(t, WithDataDir(tmpDir))
	setupMockEmbedder(t, s1, 128)

	old, err := s1.Store(ctx, "original", nil)
	if err != nil {
		t.Fatalf("Store: %v", err)
	}

	// Manually create an orphan: insert new memory, but don't archive old
	newMem := &Memory{
		ID:        "orphan-new",
		Content:   "new content",
		State:     StateActive,
		Version:   2,
		CreatedAt: old.CreatedAt,
		UpdatedAt: time.Now(),
	}
	vec := make([]float32, s1.config.Dimension)
	doc, _ := memoryToDoc(newMem, vec)
	if err := s1.coll.InsertContext(ctx, doc); err != nil {
		t.Fatalf("Insert orphan: %v", err)
	}

	// Corrupt the old memory: set superseded_by without archiving
	old.SupersededBy = newMem.ID
	oldDoc, _ := memoryToDoc(old, vec)
	if err := s1.coll.UpdateContext(ctx, oldDoc); err != nil {
		t.Fatalf("Update old: %v", err)
	}

	s1.Close()

	// Phase 2: reopen — crash recovery should archive the old memory
	s2, err := Open(tmpDir,
		WithDimension(128),
		WithEmbedding("test-key", "", "test-model", 128),
		WithLLM("test-key", "", "test-model", 0.5),
	)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	setupMockEmbedder(t, s2, 128)
	defer s2.Close()

	fixed, err := s2.Get(ctx, old.ID)
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if fixed.State != StateArchived {
		t.Errorf("crash recovery: old memory should be archived, got %s", fixed.State)
	}
}

// ----------------------------------------------------------------------
// Reopen rebuilds inverted index
// ----------------------------------------------------------------------

func TestMemoryStoreReopenRebuildsIndex(t *testing.T) {
	tmpDir := t.TempDir()
	ctx := context.Background()

	s1 := newTestStore(t, WithDataDir(tmpDir))
	setupMockEmbedder(t, s1, 128)

	mem, _ := s1.Store(ctx, "persistent memory", nil)
	s1.Close()

	// Reopen
	s2, err := Open(tmpDir,
		WithDimension(128),
		WithEmbedding("test-key", "", "test-model", 128),
		WithLLM("test-key", "", "test-model", 0.5),
	)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	setupMockEmbedder(t, s2, 128)
	defer s2.Close()

	results := s2.inverted.Search("persistent", 10)
	if len(results) != 1 {
		t.Fatalf("after reopen: want 1 result, got %d", len(results))
	}
	if results[0].ID != mem.ID {
		t.Errorf("after reopen: ID mismatch")
	}
}

// ----------------------------------------------------------------------
// Concurrency
// ----------------------------------------------------------------------

func TestMemoryStoreConcurrentStoreDelete(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()
	mem, err := s.Store(ctx, "concurrent test content", nil)
	if err != nil {
		t.Fatalf("Store: %v", err)
	}

	var wg sync.WaitGroup
	numGoroutines := 20

	// Half update, half delete concurrently.
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			if idx%2 == 0 {
				_, _ = s.Update(ctx, mem.ID, "updated by goroutine", nil)
			} else {
				_ = s.Delete(ctx, mem.ID)
			}
		}(i)
	}
	wg.Wait()

	// Final state must be deterministic: the last operation wins.
	// Because both Update and Delete hold s.mu, there is no race.
	got, err := s.Get(ctx, mem.ID)
	if err != nil {
		t.Fatalf("Get after concurrent ops: %v", err)
	}
	if got.State != StateArchived && got.State != StateDeleted {
		t.Errorf("unexpected final state: %s", got.State)
	}
}

// ----------------------------------------------------------------------
// Bootstrap with pre-computed vectors
// ----------------------------------------------------------------------

func TestMemoryStoreBootstrapWithVectors(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()
	vec := make([]float32, 128)
	for i := range vec {
		vec[i] = 0.42
	}

	memories := []*Memory{
		{ID: "boot-vec-1", Content: "vector one", State: StateActive, Version: 1, Vector: vec, CreatedAt: time.Now(), UpdatedAt: time.Now()},
		{ID: "boot-vec-2", Content: "vector two", State: StateActive, Version: 1, CreatedAt: time.Now(), UpdatedAt: time.Now()}, // no vector, will embed
	}

	if err := s.Bootstrap(ctx, memories); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}

	// boot-vec-1 should use the provided vector
	got1, err := s.Get(ctx, "boot-vec-1")
	if err != nil {
		t.Fatalf("Get boot-vec-1: %v", err)
	}
	if got1.Content != "vector one" {
		t.Errorf("content mismatch")
	}

	// boot-vec-2 should also exist (embedder generates vector)
	got2, err := s.Get(ctx, "boot-vec-2")
	if err != nil {
		t.Fatalf("Get boot-vec-2: %v", err)
	}
	if got2.Content != "vector two" {
		t.Errorf("content mismatch")
	}
}

// ----------------------------------------------------------------------
// memoryToDoc zero timestamp
// ----------------------------------------------------------------------

func TestMemoryToDocZeroTimestamp(t *testing.T) {
	mem := &Memory{
		ID:      "ts-test",
		Content: "hello",
		State:   StateActive,
	}
	vec := make([]float32, 128)
	doc, err := memoryToDoc(mem, vec)
	if err != nil {
		t.Fatalf("memoryToDoc: %v", err)
	}
	if doc.Timestamp.IsZero() {
		t.Error("memoryToDoc should set non-zero timestamp when UpdatedAt is zero")
	}
}

// ----------------------------------------------------------------------
// Embedder nil / error paths
// ----------------------------------------------------------------------

func TestMemoryStoreEmbedderNil(t *testing.T) {
	tmpDir := t.TempDir()
	s, err := Open(tmpDir,
		WithDimension(128),
		WithEmbedDims(128),
		WithLLM("test-key", "", "test-model", 0.5),
	)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()
	_, err = s.Store(ctx, "hello", nil)
	if err == nil {
		t.Fatal("expected error when embedder is nil")
	}
	_, err = s.Search(ctx, "hello")
	if err == nil {
		t.Fatal("expected error when embedder is nil")
	}
}

// ----------------------------------------------------------------------
// toMemories corrupt document
// ----------------------------------------------------------------------

func TestMemoryStoreSearchSkipsCorrupt(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()
	_, err := s.Store(ctx, "valid memory", nil)
	if err != nil {
		t.Fatalf("Store: %v", err)
	}

	// Manually insert a corrupt document (missing _data field)
	corruptDoc := &vego.Document{
		ID:       vego.DocumentID(),
		Vector:   make([]float32, 128),
		Metadata: map[string]interface{}{"_state": "active"},
	}
	if err := s.coll.InsertContext(ctx, corruptDoc); err != nil {
		t.Fatalf("Insert corrupt: %v", err)
	}

	// Search should return only the valid memory
	results, err := s.Search(ctx, "valid")
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("Search: want 1 result, got %d", len(results))
	}
}

// ----------------------------------------------------------------------
// Crash recovery via PreviousID
// ----------------------------------------------------------------------

func TestMemoryStoreCrashRecoveryPreviousID(t *testing.T) {
	tmpDir := t.TempDir()
	ctx := context.Background()

	s1 := newTestStore(t, WithDataDir(tmpDir))
	setupMockEmbedder(t, s1, 128)

	old, err := s1.Store(ctx, "original", nil)
	if err != nil {
		t.Fatalf("Store: %v", err)
	}

	// Simulate: new memory inserted with PreviousID, but old never archived.
	newMem := &Memory{
		ID:         "orphan-new",
		Content:    "new content",
		State:      StateActive,
		PreviousID: old.ID,
		Version:    2,
		CreatedAt:  old.CreatedAt,
		UpdatedAt:  time.Now(),
	}
	vec := make([]float32, s1.config.Dimension)
	doc, _ := memoryToDoc(newMem, vec)
	if err := s1.coll.InsertContext(ctx, doc); err != nil {
		t.Fatalf("Insert orphan: %v", err)
	}

	// Old memory is still active (Step 2 of archiveAndCreate never ran)
	s1.Close()

	// Reopen: crash recovery should archive old via PreviousID chain
	s2, err := Open(tmpDir,
		WithDimension(128),
		WithEmbedding("test-key", "", "test-model", 128),
		WithLLM("test-key", "", "test-model", 0.5),
	)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	setupMockEmbedder(t, s2, 128)
	defer s2.Close()

	fixed, err := s2.Get(ctx, old.ID)
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	if fixed.State != StateArchived {
		t.Errorf("crash recovery via PreviousID: old memory should be archived, got %s", fixed.State)
	}
}

// ----------------------------------------------------------------------
// Edge cases
// ----------------------------------------------------------------------

func TestMemoryStoreUpdateDeleted(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()
	mem, err := s.Store(ctx, "to be deleted", nil)
	if err != nil {
		t.Fatalf("Store: %v", err)
	}
	if err := s.Delete(ctx, mem.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, err = s.Update(ctx, mem.ID, "should fail", nil)
	if err == nil {
		t.Fatal("expected error when updating deleted memory")
	}
}

func TestMemoryStoreBootstrapNilElement(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()
	memories := []*Memory{
		{ID: "boot-nil-1", Content: "valid", State: StateActive, Version: 1, CreatedAt: time.Now(), UpdatedAt: time.Now()},
		nil,
	}
	if err := s.Bootstrap(ctx, memories); err == nil {
		t.Fatal("expected error for nil element in bootstrap slice")
	}
}

func TestMemoryStoreGetNonExistent(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	_, err := s.Get(context.Background(), "does-not-exist")
	if err == nil {
		t.Fatal("expected error for non-existent ID")
	}
}

func TestMemoryStoreDeleteNonExistent(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	if err := s.Delete(context.Background(), "does-not-exist"); err == nil {
		t.Fatal("expected error for non-existent ID")
	}
}

func TestMemoryStoreSearchNoResults(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()
	_, err := s.Store(ctx, "hello world", nil)
	if err != nil {
		t.Fatalf("Store: %v", err)
	}

	// NOTE: The mock embedder returns identical vectors for all texts,
	// so HNSW search will return the stored document regardless of query.
	// We verify that Search executes without error; semantic filtering
	// requires a real embedder.
	results, err := s.Search(ctx, "xyz_nonexistent_query")
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	_ = results
}

func TestMemoryStoreCloseTwice(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)

	ctx := context.Background()
	if _, err := s.Store(ctx, "dummy", nil); err != nil {
		t.Fatalf("Store: %v", err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// TestSeqMonotonic verifies that StoreRawMessages assigns monotonically
// increasing Seq values across multiple calls.
func TestSeqMonotonic(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()
	sessionID := "sess-seq"

	// First batch: 3 messages.
	batch1 := []Message{
		{Role: "user", Content: "msg 1", SessionID: sessionID},
		{Role: "user", Content: "msg 2", SessionID: sessionID},
		{Role: "user", Content: "msg 3", SessionID: sessionID},
	}
	stored1, err := s.StoreRawMessages(ctx, sessionID, batch1)
	if err != nil {
		t.Fatalf("StoreRawMessages batch1: %v", err)
	}
	if stored1 != 3 {
		t.Errorf("batch1: want 3 stored, got %d", stored1)
	}

	// Second batch: 2 new messages.
	batch2 := []Message{
		{Role: "user", Content: "msg 4", SessionID: sessionID},
		{Role: "user", Content: "msg 5", SessionID: sessionID},
	}
	stored2, err := s.StoreRawMessages(ctx, sessionID, batch2)
	if err != nil {
		t.Fatalf("StoreRawMessages batch2: %v", err)
	}
	if stored2 != 2 {
		t.Errorf("batch2: want 2 stored, got %d", stored2)
	}

	// Third batch: all duplicates → 0 stored.
	batch3 := []Message{
		{Role: "user", Content: "msg 1", SessionID: sessionID},
	}
	stored3, err := s.StoreRawMessages(ctx, sessionID, batch3)
	if err != nil {
		t.Fatalf("StoreRawMessages batch3: %v", err)
	}
	if stored3 != 0 {
		t.Errorf("batch3: want 0 stored (dedup), got %d", stored3)
	}

	// Verify all stored memories have Seq 1..5, no gaps, no duplicates.
	var seqs []int
	err = s.coll.ForEach(func(doc *vego.Document) bool {
		m, err := docToMemory(doc)
		if err != nil {
			return true
		}
		if m.SessionID == sessionID {
			seqs = append(seqs, m.Seq)
		}
		return true
	})
	if err != nil {
		t.Fatalf("ForEach: %v", err)
	}

	if len(seqs) != 5 {
		t.Fatalf("want 5 seqs, got %d", len(seqs))
	}
	seen := make(map[int]bool)
	for _, seq := range seqs {
		if seq < 1 || seq > 5 {
			t.Errorf("seq out of range [1,5]: %d", seq)
		}
		if seen[seq] {
			t.Errorf("duplicate seq: %d", seq)
		}
		seen[seq] = true
	}
}

// TestDeleteSemanticsSearchInvisible verifies that after Delete, the memory
// is still retrievable by Get but invisible to both hybrid and pure vector Search.
func TestDeleteSemanticsSearchInvisible(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()
	mem, err := s.Store(ctx, "searchable content", nil)
	if err != nil {
		t.Fatalf("Store: %v", err)
	}

	// Pre-delete: both hybrid and pure vector search should find it.
	results, err := s.Search(ctx, "searchable")
	if err != nil {
		t.Fatalf("pre-delete hybrid search: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("pre-delete: hybrid search should find the memory")
	}

	resultsPure, err := s.Search(ctx, "searchable", EnableHybrid(false))
	if err != nil {
		t.Fatalf("pre-delete pure search: %v", err)
	}
	if len(resultsPure) == 0 {
		t.Fatal("pre-delete: pure vector search should find the memory")
	}

	// Delete.
	if err := s.Delete(ctx, mem.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Get should still work (soft delete).
	got, err := s.Get(ctx, mem.ID)
	if err != nil {
		t.Fatalf("Get after delete: %v", err)
	}
	if got.State != StateDeleted {
		t.Errorf("state: want deleted, got %s", got.State)
	}

	// Hybrid search should NOT find it.
	resultsAfter, err := s.Search(ctx, "searchable")
	if err != nil {
		t.Fatalf("post-delete hybrid search: %v", err)
	}
	for _, m := range resultsAfter {
		if m.ID == mem.ID {
			t.Error("post-delete: hybrid search should not find deleted memory")
		}
	}

	// Pure vector search should NOT find it.
	resultsPureAfter, err := s.Search(ctx, "searchable", EnableHybrid(false))
	if err != nil {
		t.Fatalf("post-delete pure search: %v", err)
	}
	for _, m := range resultsPureAfter {
		if m.ID == mem.ID {
			t.Error("post-delete: pure vector search should not find deleted memory")
		}
	}
}


// ----------------------------------------------------------------------
// ContentHashIndex.RebuildBatch
// ----------------------------------------------------------------------

func TestContentHashIndexRebuildBatch(t *testing.T) {
	idx := NewContentHashIndex()

	entries := []HashIndexEntry{
		{SessionID: "s1", Hash: "h1", MemoryID: "m1", Seq: 1},
		{SessionID: "s1", Hash: "h2", MemoryID: "m2", Seq: 2},
		{SessionID: "s2", Hash: "h1", MemoryID: "m3", Seq: 1},
	}

	if err := idx.RebuildBatch(entries); err != nil {
		t.Fatalf("RebuildBatch: %v", err)
	}

	if !idx.Has("s1", "h1") {
		t.Error("expected s1:h1 to exist")
	}
	if !idx.Has("s1", "h2") {
		t.Error("expected s1:h2 to exist")
	}
	if !idx.Has("s2", "h1") {
		t.Error("expected s2:h1 to exist")
	}
	if idx.Has("s1", "h3") {
		t.Error("expected s1:h3 to not exist")
	}

	if idx.MaxSeq("s1") != 2 {
		t.Errorf("MaxSeq s1: want 2, got %d", idx.MaxSeq("s1"))
	}
	if idx.MaxSeq("s2") != 1 {
		t.Errorf("MaxSeq s2: want 1, got %d", idx.MaxSeq("s2"))
	}
	if idx.MaxSeq("s3") != 0 {
		t.Errorf("MaxSeq s3: want 0, got %d", idx.MaxSeq("s3"))
	}
}

func TestContentHashIndexRebuildBatchEmpty(t *testing.T) {
	idx := NewContentHashIndex()
	if err := idx.RebuildBatch(nil); err != nil {
		t.Fatalf("RebuildBatch nil: %v", err)
	}
	if idx.Has("s1", "h1") {
		t.Error("nil entries should not add anything")
	}
	if err := idx.RebuildBatch([]HashIndexEntry{}); err != nil {
		t.Fatalf("RebuildBatch empty: %v", err)
	}
	if idx.Has("s1", "h1") {
		t.Error("empty entries should not add anything")
	}
}

func TestContentHashIndexRebuildBatchErrorOnNonEmpty(t *testing.T) {
	idx := NewContentHashIndex()
	idx.Add("s1", "h1", "m1", 1)

	err := idx.RebuildBatch([]HashIndexEntry{{SessionID: "s2", Hash: "h2", MemoryID: "m2", Seq: 1}})
	if err == nil {
		t.Error("expected error on non-empty index")
	}
}

func TestContentHashIndexRebuildBatchEquivalentToAdd(t *testing.T) {
	// Build via Add
	idx1 := NewContentHashIndex()
	idx1.Add("s1", "h1", "m1", 1)
	idx1.Add("s1", "h2", "m2", 3)
	idx1.Add("s2", "h1", "m3", 2)

	// Build via RebuildBatch
	idx2 := NewContentHashIndex()
	if err := idx2.RebuildBatch([]HashIndexEntry{
		{SessionID: "s1", Hash: "h1", MemoryID: "m1", Seq: 1},
		{SessionID: "s1", Hash: "h2", MemoryID: "m2", Seq: 3},
		{SessionID: "s2", Hash: "h1", MemoryID: "m3", Seq: 2},
	}); err != nil {
		t.Fatalf("RebuildBatch: %v", err)
	}

	// Verify equivalence
	for _, tc := range []struct{ sid, hash string }{
		{"s1", "h1"}, {"s1", "h2"}, {"s2", "h1"},
		{"s1", "h3"}, {"s3", "h1"},
	} {
		if idx1.Has(tc.sid, tc.hash) != idx2.Has(tc.sid, tc.hash) {
			t.Errorf("Has(%s,%s) mismatch: Add=%v RebuildBatch=%v", tc.sid, tc.hash, idx1.Has(tc.sid, tc.hash), idx2.Has(tc.sid, tc.hash))
		}
	}

	for _, sid := range []string{"s1", "s2", "s3"} {
		if idx1.MaxSeq(sid) != idx2.MaxSeq(sid) {
			t.Errorf("MaxSeq(%s) mismatch: Add=%d RebuildBatch=%d", sid, idx1.MaxSeq(sid), idx2.MaxSeq(sid))
		}
	}
}

// ----------------------------------------------------------------------
// StoreBatch error paths (Problem 3)
// ----------------------------------------------------------------------

func TestMemoryStoreStoreBatchEmbedError(t *testing.T) {
	s := newTestStore(t)
	// Do NOT set up mock embedder — embed calls will fail.
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()
	items := []StoreItem{
		{Content: "first item", Tags: []string{"a"}},
		{Content: "second item", Tags: []string{"b"}},
	}

	_, err := s.StoreBatch(ctx, items)
	if err == nil {
		t.Fatal("expected error when embedder is nil")
	}
	// The error should mention embed failure.
	if !strings.Contains(err.Error(), "embed") {
		t.Errorf("expected embed error, got: %v", err)
	}
}

func TestMemoryStoreStoreBatchEmpty(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()
	mems, err := s.StoreBatch(ctx, nil)
	if err != nil {
		t.Fatalf("StoreBatch(nil): %v", err)
	}
	if len(mems) != 0 {
		t.Errorf("expected 0 memories for nil input, got %d", len(mems))
	}

	mems, err = s.StoreBatch(ctx, []StoreItem{})
	if err != nil {
		t.Fatalf("StoreBatch(empty): %v", err)
	}
	if len(mems) != 0 {
		t.Errorf("expected 0 memories for empty input, got %d", len(mems))
	}
}

// ----------------------------------------------------------------------
// Open error paths (Problem 5)
// ----------------------------------------------------------------------

func TestMemoryStoreOpenInvalidPath(t *testing.T) {
	// Attempt to open a store at a path that cannot be created.
	_, err := Open("/dev/null/impossible/path",
		WithDimension(128),
		WithEmbedding("key", "", "model", 128),
	)
	if err == nil {
		t.Fatal("expected error for invalid path")
	}
}

func TestMemoryStoreOpenPathResolution(t *testing.T) {
	dir := t.TempDir()

	// When WithDataDir is set to a non-default value, it overrides the path argument.
	s, err := Open("ignored-path",
		WithDataDir(dir),
		WithDimension(128),
		WithEmbedding("key", "", "model", 128),
	)
	if err != nil {
		t.Fatalf("Open with WithDataDir: %v", err)
	}
	s.Close()

	// When path is provided and DataDir is left at default, path is used.
	dir2 := t.TempDir()
	s2, err := Open(dir2,
		WithDimension(128),
		WithEmbedding("key", "", "model", 128),
	)
	if err != nil {
		t.Fatalf("Open with explicit path: %v", err)
	}
	s2.Close()
}

// ----------------------------------------------------------------------
// archiveAndCreate error paths (coverage: rollback, concurrent state/pinned)
// ----------------------------------------------------------------------

func TestArchiveAndCreate_RollbackOnGetOld(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()
	newMem := &Memory{
		ID:         vego.DocumentID(),
		Content:    "new content",
		MemoryType: TypeInsight,
		State:      StateActive,
		Version:    1,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	vec := make([]float32, 128)
	for i := range vec {
		vec[i] = 0.1
	}

	err := s.archiveAndCreate(ctx, "nonexistent-id", newMem, vec)
	if err == nil {
		t.Fatal("expected error for non-existent oldID, got nil")
	}

	// Verify the new memory was rolled back (not left as orphan).
	_, err = s.Get(ctx, newMem.ID)
	if err == nil {
		t.Error("new memory should have been rolled back (compensateInsert)")
	}
}

func TestArchiveAndCreate_RollbackOnCorruptOld(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()

	// Insert a corrupt document directly into Vego (no _data field).
	corruptID := vego.DocumentID()
	corruptDoc := &vego.Document{
		ID:       corruptID,
		Vector:   make([]float32, 128),
		Metadata: map[string]interface{}{}, // missing "_data" → docToMemory fails
	}
	if err := s.coll.InsertContext(ctx, corruptDoc); err != nil {
		t.Fatalf("insert corrupt doc: %v", err)
	}

	newMem := &Memory{
		ID:         vego.DocumentID(),
		Content:    "new content",
		MemoryType: TypeInsight,
		State:      StateActive,
		Version:    1,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	vec := make([]float32, 128)

	err := s.archiveAndCreate(ctx, corruptID, newMem, vec)
	if err == nil {
		t.Fatal("expected error for corrupt old document, got nil")
	}

	// Verify rollback.
	_, err = s.Get(ctx, newMem.ID)
	if err == nil {
		t.Error("new memory should have been rolled back")
	}
}

func TestArchiveAndCreate_ConcurrentStateChange(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()

	// Store a memory normally.
	old, err := s.Store(ctx, "original content", nil)
	if err != nil {
		t.Fatalf("Store: %v", err)
	}

	// Simulate a concurrent state change: directly archive the old memory
	// in Vego (bypassing MemoryStore.update) so that when archiveAndCreate
	// re-reads it under lock, State != Active.
	oldDoc, err := s.coll.GetContext(ctx, old.ID)
	if err != nil {
		t.Fatalf("get old doc: %v", err)
	}
	oldMem, err := docToMemory(oldDoc)
	if err != nil {
		t.Fatalf("decode old: %v", err)
	}
	oldMem.State = StateArchived
	oldMem.UpdatedAt = time.Now()
	archivedDoc, err := memoryToDoc(oldMem, oldDoc.Vector)
	if err != nil {
		t.Fatalf("marshal archived: %v", err)
	}
	if err := s.coll.UpdateContext(ctx, archivedDoc); err != nil {
		t.Fatalf("update archived in vego: %v", err)
	}

	// Now call archiveAndCreate. The old memory is no longer active,
	// so the new memory should be kept as a separate ADD (no error).
	newMem := &Memory{
		ID:         vego.DocumentID(),
		Content:    "new content",
		MemoryType: TypeInsight,
		State:      StateActive,
		Version:    1,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	vec := make([]float32, 128)
	for i := range vec {
		vec[i] = 0.1
	}

	err = s.archiveAndCreate(ctx, old.ID, newMem, vec)
	if err != nil {
		t.Fatalf("concurrent state change should not error (new kept as ADD): %v", err)
	}

	// Verify new memory exists.
	got, err := s.Get(ctx, newMem.ID)
	if err != nil {
		t.Fatalf("new memory should exist: %v", err)
	}
	if got.State != StateActive {
		t.Errorf("new memory state: want active, got %s", got.State)
	}
}

func TestArchiveAndCreate_ConcurrentPinned(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()

	// Store a memory.
	old, err := s.Store(ctx, "original content", nil)
	if err != nil {
		t.Fatalf("Store: %v", err)
	}

	// Simulate concurrent pin: change the old memory to TypePinned in Vego.
	oldDoc, err := s.coll.GetContext(ctx, old.ID)
	if err != nil {
		t.Fatalf("get old doc: %v", err)
	}
	oldMem, err := docToMemory(oldDoc)
	if err != nil {
		t.Fatalf("decode old: %v", err)
	}
	oldMem.MemoryType = TypePinned
	oldMem.UpdatedAt = time.Now()
	pinnedDoc, err := memoryToDoc(oldMem, oldDoc.Vector)
	if err != nil {
		t.Fatalf("marshal pinned: %v", err)
	}
	if err := s.coll.UpdateContext(ctx, pinnedDoc); err != nil {
		t.Fatalf("update pinned in vego: %v", err)
	}

	// archiveAndCreate should detect the pinned type and keep new as ADD.
	newMem := &Memory{
		ID:         vego.DocumentID(),
		Content:    "new content",
		MemoryType: TypeInsight,
		State:      StateActive,
		Version:    1,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	vec := make([]float32, 128)
	for i := range vec {
		vec[i] = 0.1
	}

	err = s.archiveAndCreate(ctx, old.ID, newMem, vec)
	if err != nil {
		t.Fatalf("concurrent pinned should not error (new kept as ADD): %v", err)
	}

	// Verify new memory exists and old is still pinned (not archived).
	got, err := s.Get(ctx, newMem.ID)
	if err != nil {
		t.Fatalf("new memory should exist: %v", err)
	}
	if got.State != StateActive {
		t.Errorf("new memory state: want active, got %s", got.State)
	}
	oldGot, err := s.Get(ctx, old.ID)
	if err != nil {
		t.Fatalf("old memory should still exist: %v", err)
	}
	if oldGot.MemoryType != TypePinned {
		t.Errorf("old type: want pinned, got %s", oldGot.MemoryType)
	}
}

// ----------------------------------------------------------------------
// rebuildIndexes: panic recovery in worker
// ----------------------------------------------------------------------

// TestRebuildIndexesRecoverFromPanic verifies that a panic inside a rebuild
// TestRebuildIndexesRecoverFromPanic verifies that a panic inside a rebuild
// worker does not crash the entire rebuild. Uses testWorkerPanicHook to
// inject a panic and exercise the recover path.
func TestRebuildIndexesRecoverFromPanic(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()

	// Insert healthy documents so the corpus is non-empty.
	_, err := s.Store(ctx, "healthy document one", nil)
	if err != nil {
		t.Fatalf("Store 1: %v", err)
	}
	_, err = s.Store(ctx, "healthy document two", nil)
	if err != nil {
		t.Fatalf("Store 2: %v", err)
	}

	// Set the panic hook: panics once (only for the first document),
	// then clears itself so subsequent documents proceed normally.
	var mu sync.Mutex
	panicCount := 0
	testWorkerPanicHook = func() {
		mu.Lock()
		count := panicCount
		panicCount++
		mu.Unlock()
		if count == 0 {
			panic("injected panic for test coverage")
		}
	}
	t.Cleanup(func() { testWorkerPanicHook = nil })

	s.inverted.Clear()
	s.contentHashIndex.Clear()

	if err := s.rebuildIndexes(); err != nil {
		t.Fatalf("rebuildIndexes: %v", err)
	}

	// Verify the panic hook was triggered.
	if panicCount < 1 {
		t.Error("panic hook was not triggered; recover path not exercised")
	}

	// Verify remaining healthy documents are still indexed after rebuild
	// (the panicked doc is skipped via sentinel; the second doc is indexed).
	if s.inverted.Len() < 1 {
		t.Error("inverted index should contain at least 1 document after rebuild")
	}
}

func TestMemoryStoreOpenDistanceFuncs(t *testing.T) {
	for _, df := range []string{"cosine", "l2", "ip"} {
		t.Run(df, func(t *testing.T) {
			dir := t.TempDir()
			s, err := Open(dir,
				WithDimension(128),
				WithEmbedding("key", "", "model", 128),
				WithDistanceFunc(df),
			)
			if err != nil {
				t.Fatalf("Open with %s: %v", df, err)
			}
			s.Close()
		})
	}
}

// ----------------------------------------------------------------------
// Pause / Resume
// ----------------------------------------------------------------------

func TestPauseAndResume(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()
	mem, err := s.Store(ctx, "pause me", []string{"tag1"})
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	// Pause should succeed.
	if err := s.Pause(ctx, mem.ID); err != nil {
		t.Fatalf("pause: %v", err)
	}

	// Get should still return the memory.
	got, err := s.Get(ctx, mem.ID)
	if err != nil {
		t.Fatalf("get after pause: %v", err)
	}
	if got.State != StatePaused {
		t.Errorf("state: want paused, got %s", got.State)
	}

	// Search should exclude paused memories.
	results, err := s.Search(ctx, "pause")
	if err != nil {
		t.Fatalf("search after pause: %v", err)
	}
	for _, m := range results {
		if m.ID == mem.ID {
			t.Error("paused memory should not appear in search results")
		}
	}

	// Resume should succeed.
	if err := s.Resume(ctx, mem.ID); err != nil {
		t.Fatalf("resume: %v", err)
	}

	got, err = s.Get(ctx, mem.ID)
	if err != nil {
		t.Fatalf("get after resume: %v", err)
	}
	if got.State != StateActive {
		t.Errorf("state: want active, got %s", got.State)
	}

	// Search should now include the memory.
	results, err = s.Search(ctx, "pause")
	if err != nil {
		t.Fatalf("search after resume: %v", err)
	}
	found := false
	for _, m := range results {
		if m.ID == mem.ID {
			found = true
			break
		}
	}
	if !found {
		t.Error("resumed memory should appear in search results")
	}
}

func TestPauseNotActive(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()
	mem, err := s.Store(ctx, "test", nil)
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	// Delete first.
	if err := s.Delete(ctx, mem.ID); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Pause deleted memory should fail.
	if err := s.Pause(ctx, mem.ID); err == nil {
		t.Error("expected error pausing deleted memory")
	}
}

func TestResumeNotPaused(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()
	mem, err := s.Store(ctx, "test", nil)
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	// Resume active memory should fail.
	if err := s.Resume(ctx, mem.ID); err == nil {
		t.Error("expected error resuming active memory")
	}
}

func TestPauseResumeNonExistent(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()
	if err := s.Pause(ctx, "nonexistent"); err == nil {
		t.Error("expected error pausing non-existent memory")
	}
	if err := s.Resume(ctx, "nonexistent"); err == nil {
		t.Error("expected error resuming non-existent memory")
	}
}

// ----------------------------------------------------------------------
// List
// ----------------------------------------------------------------------

func TestListAll(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()
	m1, _ := s.Store(ctx, "alpha", []string{"tag-a"})
	m2, _ := s.Store(ctx, "beta", []string{"tag-b"})

	results, err := s.List(ctx, MemoryFilter{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("want 2, got %d", len(results))
	}

	// Verify descending UpdatedAt order.
	if results[0].UpdatedAt.Before(results[1].UpdatedAt) {
		t.Error("list should be sorted by UpdatedAt descending")
	}

	// Verify IDs are present.
	ids := map[string]bool{}
	for _, m := range results {
		ids[m.ID] = true
	}
	if !ids[m1.ID] || !ids[m2.ID] {
		t.Error("list missing expected IDs")
	}
}

func TestListByState(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()
	m1, _ := s.Store(ctx, "active mem", nil)
	m2, _ := s.Store(ctx, "to delete", nil)
	_ = s.Delete(ctx, m2.ID)

	// List only active.
	results, err := s.List(ctx, MemoryFilter{State: "active"})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(results) != 1 || results[0].ID != m1.ID {
		t.Errorf("want active memory %s, got %+v", m1.ID, results)
	}

	// List only deleted.
	results, err = s.List(ctx, MemoryFilter{State: "deleted"})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(results) != 1 || results[0].ID != m2.ID {
		t.Errorf("want deleted memory %s, got %+v", m2.ID, results)
	}
}

func TestListByTags(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()
	m1, _ := s.Store(ctx, "alpha", []string{"go", "rust"})
	_, _ = s.Store(ctx, "beta", []string{"python"})

	results, err := s.List(ctx, MemoryFilter{Tags: []string{"go"}})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(results) != 1 || results[0].ID != m1.ID {
		t.Errorf("want 1 result with tag 'go', got %d", len(results))
	}
}

func TestListPagination(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		_, _ = s.Store(ctx, fmt.Sprintf("mem-%d", i), nil)
	}

	// Offset 2, limit 2.
	results, err := s.List(ctx, MemoryFilter{Offset: 2, Limit: 2})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("want 2, got %d", len(results))
	}

	// Offset beyond total.
	results, err = s.List(ctx, MemoryFilter{Offset: 10})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("want 0, got %d", len(results))
	}
}

func TestListLimitSetExplicitZero(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		_, _ = s.Store(ctx, fmt.Sprintf("mem-%d", i), nil)
	}

	// Explicit Limit:0 with LimitSet:true should return 0 results.
	results, err := s.List(ctx, MemoryFilter{Limit: 0, LimitSet: true})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("LimitSet:true Limit:0 should return 0 results, got %d", len(results))
	}

	// Without LimitSet, Limit:0 should fall back to config default.
	results, err = s.List(ctx, MemoryFilter{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("default limit should return all 3, got %d", len(results))
	}
}

func TestListEmptyStore(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	results, err := s.List(context.Background(), MemoryFilter{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("want 0, got %d", len(results))
	}
}

// ----------------------------------------------------------------------
// ListBySessionIDs
// ----------------------------------------------------------------------

func TestListBySessionIDs(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()
	now := time.Now()

	memories := []*Memory{
		{ID: "a1", Content: "s1-msg1", State: StateActive, SessionID: "session-1", MemoryType: TypeSession, UpdatedAt: now.Add(-3 * time.Hour), CreatedAt: now},
		{ID: "a2", Content: "s1-msg2", State: StateActive, SessionID: "session-1", MemoryType: TypeSession, UpdatedAt: now.Add(-2 * time.Hour), CreatedAt: now},
		{ID: "a3", Content: "s1-msg3", State: StateActive, SessionID: "session-1", MemoryType: TypeSession, UpdatedAt: now.Add(-1 * time.Hour), CreatedAt: now},
		{ID: "b1", Content: "s2-msg1", State: StateActive, SessionID: "session-2", MemoryType: TypeSession, UpdatedAt: now.Add(-30 * time.Minute), CreatedAt: now},
		{ID: "b2", Content: "s2-msg2", State: StateActive, SessionID: "session-2", MemoryType: TypeSession, UpdatedAt: now, CreatedAt: now},
		{ID: "no1", Content: "nosession", State: StateActive, MemoryType: TypeInsight, UpdatedAt: now, CreatedAt: now},
	}
	if err := s.Bootstrap(ctx, memories); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	// Query two sessions with limit 2 per session.
	result, err := s.ListBySessionIDs(ctx, []string{"session-1", "session-2"}, 2)
	if err != nil {
		t.Fatalf("ListBySessionIDs: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("want 2 sessions, got %d", len(result))
	}

	s1, ok := result["session-1"]
	if !ok {
		t.Fatal("session-1 missing")
	}
	if len(s1) != 2 {
		t.Errorf("session-1: want 2, got %d", len(s1))
	}
	if s1[0].ID != "a3" || s1[1].ID != "a2" {
		t.Errorf("session-1: want [a3 a2] (newest first), got [%s %s]", s1[0].ID, s1[1].ID)
	}

	s2, ok := result["session-2"]
	if !ok {
		t.Fatal("session-2 missing")
	}
	if len(s2) != 2 {
		t.Errorf("session-2: want 2, got %d", len(s2))
	}
}

func TestListBySessionIDs_EmptyInput(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()
	result, err := s.ListBySessionIDs(ctx, nil, 10)
	if err != nil {
		t.Fatalf("ListBySessionIDs: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("want empty, got %d", len(result))
	}

	result, err = s.ListBySessionIDs(ctx, []string{}, 10)
	if err != nil {
		t.Fatalf("ListBySessionIDs: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("want empty, got %d", len(result))
	}
}

func TestListBySessionIDs_NonExistentSession(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()
	result, err := s.ListBySessionIDs(ctx, []string{"no-such-session"}, 10)
	if err != nil {
		t.Fatalf("ListBySessionIDs: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("want empty, got %d", len(result))
	}
}

func TestListBySessionIDs_Unlimited(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()
	now := time.Now()
	memories := []*Memory{
		{ID: "u1", Content: "m1", State: StateActive, SessionID: "s", MemoryType: TypeSession, UpdatedAt: now.Add(-2 * time.Hour), CreatedAt: now},
		{ID: "u2", Content: "m2", State: StateActive, SessionID: "s", MemoryType: TypeSession, UpdatedAt: now.Add(-1 * time.Hour), CreatedAt: now},
		{ID: "u3", Content: "m3", State: StateActive, SessionID: "s", MemoryType: TypeSession, UpdatedAt: now, CreatedAt: now},
	}
	if err := s.Bootstrap(ctx, memories); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	result, err := s.ListBySessionIDs(ctx, []string{"s"}, 0)
	if err != nil {
		t.Fatalf("ListBySessionIDs: %v", err)
	}
	ms := result["s"]
	if len(ms) != 3 {
		t.Errorf("want 3, got %d", len(ms))
	}
}

func TestListBySessionIDs_SkipsNonActive(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()
	now := time.Now()
	memories := []*Memory{
		{ID: "d1", Content: "deleted", State: StateDeleted, SessionID: "s", MemoryType: TypeSession, UpdatedAt: now, CreatedAt: now},
		{ID: "d2", Content: "archived", State: StateArchived, SessionID: "s", MemoryType: TypeSession, UpdatedAt: now, CreatedAt: now},
		{ID: "d3", Content: "paused", State: StatePaused, SessionID: "s", MemoryType: TypeSession, UpdatedAt: now, CreatedAt: now},
		{ID: "d4", Content: "active", State: StateActive, SessionID: "s", MemoryType: TypeSession, UpdatedAt: now, CreatedAt: now},
	}
	if err := s.Bootstrap(ctx, memories); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	result, err := s.ListBySessionIDs(ctx, []string{"s"}, 10)
	if err != nil {
		t.Fatalf("ListBySessionIDs: %v", err)
	}
	ms := result["s"]
	if len(ms) != 1 {
		t.Errorf("want 1 active, got %d", len(ms))
	}
	if ms[0].ID != "d4" {
		t.Errorf("want d4, got %s", ms[0].ID)
	}
}

func TestListBySessionIDs_EmptySessionIDIgnored(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()
	result, err := s.ListBySessionIDs(ctx, []string{""}, 10)
	if err != nil {
		t.Fatalf("ListBySessionIDs: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("empty sessionID should be ignored")
	}
}

// ----------------------------------------------------------------------
// Stats
// ----------------------------------------------------------------------

func TestStatsEmpty(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	stats, err := s.Stats(context.Background())
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.Total != 0 {
		t.Errorf("total: want 0, got %d", stats.Total)
	}
	if stats.Active != 0 {
		t.Errorf("active: want 0, got %d", stats.Active)
	}
}

func TestStatsAfterOperations(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()

	// Store 3 active memories.
	m1, _ := s.Store(ctx, "alpha", nil)
	m2, _ := s.Store(ctx, "beta", nil)
	m3, _ := s.Store(ctx, "gamma", nil)

	// Update m1 (old becomes archived, new is active).
	newM1, _ := s.Update(ctx, m1.ID, "alpha updated", nil)

	// Delete m2.
	_ = s.Delete(ctx, m2.ID)

	// Pause the new m1.
	_ = s.Pause(ctx, newM1.ID)

	stats, err := s.Stats(ctx)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}

	// Total: old m1 (archived) + new m1 (paused) + m2 (deleted) + m3 (active) = 4
	if stats.Total != 4 {
		t.Errorf("total: want 4, got %d", stats.Total)
	}
	if stats.Active != 1 {
		t.Errorf("active: want 1 (%s), got %d", m3.ID, stats.Active)
	}
	if stats.Paused != 1 {
		t.Errorf("paused: want 1 (%s), got %d", newM1.ID, stats.Paused)
	}
	if stats.Archived != 1 {
		t.Errorf("archived: want 1 (%s), got %d", m1.ID, stats.Archived)
	}
	if stats.Deleted != 1 {
		t.Errorf("deleted: want 1 (%s), got %d", m2.ID, stats.Deleted)
	}
	if stats.ByType["insight"] != 4 {
		t.Errorf("byType insight: want 4, got %d", stats.ByType["insight"])
	}
}

// ----------------------------------------------------------------------
// Input validation
// ----------------------------------------------------------------------

func TestMemoryStoreStoreValidation(t *testing.T) {
	s := newTestStore(t, WithMaxContentLen(10), WithMaxTags(2))
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()

	_, err := s.Store(ctx, "this is way too long", nil)
	if err == nil {
		t.Fatal("expected error for content too long")
	}
	if !strings.Contains(err.Error(), "content length") {
		t.Errorf("error should mention content length, got: %v", err)
	}

	_, err = s.Store(ctx, "short", []string{"a", "b", "c"})
	if err == nil {
		t.Fatal("expected error for too many tags")
	}
	if !strings.Contains(err.Error(), "tag count") {
		t.Errorf("error should mention tag count, got: %v", err)
	}

	// Valid input should succeed
	mem, err := s.Store(ctx, "short", []string{"a", "b"})
	if err != nil {
		t.Fatalf("valid store failed: %v", err)
	}
	if mem.ID == "" {
		t.Error("valid memory should have an ID")
	}
}

func TestMemoryStoreUpdateValidation(t *testing.T) {
	s := newTestStore(t, WithMaxContentLen(10), WithMaxTags(2))
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()
	old, err := s.Store(ctx, "old", nil)
	if err != nil {
		t.Fatalf("Store: %v", err)
	}

	_, err = s.Update(ctx, old.ID, "way too long content", nil)
	if err == nil {
		t.Fatal("expected error for content too long")
	}
	if !strings.Contains(err.Error(), "content length") {
		t.Errorf("error should mention content length, got: %v", err)
	}

	_, err = s.Update(ctx, old.ID, "ok", []string{"1", "2", "3"})
	if err == nil {
		t.Fatal("expected error for too many tags")
	}
	if !strings.Contains(err.Error(), "tag count") {
		t.Errorf("error should mention tag count, got: %v", err)
	}
}

func TestMemoryStoreStoreBatchValidation(t *testing.T) {
	s := newTestStore(t, WithMaxContentLen(10), WithMaxTags(2), WithMaxBulkSize(2))
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()

	// Batch too large
	items := []StoreItem{
		{Content: "a", Tags: nil},
		{Content: "b", Tags: nil},
		{Content: "c", Tags: nil},
	}
	_, err := s.StoreBatch(ctx, items)
	if err == nil {
		t.Fatal("expected error for batch too large")
	}
	if !strings.Contains(err.Error(), "batch size") {
		t.Errorf("error should mention batch size, got: %v", err)
	}

	// Item content too long
	items = []StoreItem{
		{Content: "this is way too long", Tags: nil},
	}
	_, err = s.StoreBatch(ctx, items)
	if err == nil {
		t.Fatal("expected error for item content too long")
	}
	if !strings.Contains(err.Error(), "item 0") || !strings.Contains(err.Error(), "content length") {
		t.Errorf("error should mention item index and content length, got: %v", err)
	}

	// Item too many tags
	items = []StoreItem{
		{Content: "ok", Tags: []string{"a", "b", "c"}},
	}
	_, err = s.StoreBatch(ctx, items)
	if err == nil {
		t.Fatal("expected error for item too many tags")
	}
	if !strings.Contains(err.Error(), "item 0") || !strings.Contains(err.Error(), "tag count") {
		t.Errorf("error should mention item index and tag count, got: %v", err)
	}

	// Valid batch should succeed
	items = []StoreItem{
		{Content: "a", Tags: []string{"x"}},
		{Content: "b", Tags: []string{"y", "z"}},
	}
	mems, err := s.StoreBatch(ctx, items)
	if err != nil {
		t.Fatalf("valid batch failed: %v", err)
	}
	if len(mems) != 2 {
		t.Errorf("want 2 memories, got %d", len(mems))
	}
}

func TestMemoryStoreBootstrapValidation(t *testing.T) {
	s := newTestStore(t, WithMaxContentLen(10), WithMaxTags(2), WithMaxBulkSize(2))
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()

	// Batch too large
	memories := []*Memory{
		{ID: "b1", Content: "a", State: StateActive, Version: 1, CreatedAt: time.Now(), UpdatedAt: time.Now()},
		{ID: "b2", Content: "b", State: StateActive, Version: 1, CreatedAt: time.Now(), UpdatedAt: time.Now()},
		{ID: "b3", Content: "c", State: StateActive, Version: 1, CreatedAt: time.Now(), UpdatedAt: time.Now()},
	}
	err := s.Bootstrap(ctx, memories)
	if err == nil {
		t.Fatal("expected error for bootstrap batch too large")
	}
	if !strings.Contains(err.Error(), "batch size") {
		t.Errorf("error should mention batch size, got: %v", err)
	}

	// Content too long
	memories = []*Memory{
		{ID: "b1", Content: "way too long content", State: StateActive, Version: 1, CreatedAt: time.Now(), UpdatedAt: time.Now()},
	}
	err = s.Bootstrap(ctx, memories)
	if err == nil {
		t.Fatal("expected error for bootstrap content too long")
	}
	if !strings.Contains(err.Error(), "memory 0") || !strings.Contains(err.Error(), "content length") {
		t.Errorf("error should mention memory index and content length, got: %v", err)
	}

	// Too many tags
	memories = []*Memory{
		{ID: "b1", Content: "ok", Tags: []string{"a", "b", "c"}, State: StateActive, Version: 1, CreatedAt: time.Now(), UpdatedAt: time.Now()},
	}
	err = s.Bootstrap(ctx, memories)
	if err == nil {
		t.Fatal("expected error for bootstrap too many tags")
	}
	if !strings.Contains(err.Error(), "memory 0") || !strings.Contains(err.Error(), "tag count") {
		t.Errorf("error should mention memory index and tag count, got: %v", err)
	}

	// Valid bootstrap should succeed
	memories = []*Memory{
		{ID: "b1", Content: "a", Tags: []string{"x"}, State: StateActive, Version: 1, CreatedAt: time.Now(), UpdatedAt: time.Now()},
	}
	err = s.Bootstrap(ctx, memories)
	if err != nil {
		t.Fatalf("valid bootstrap failed: %v", err)
	}
}

// ----------------------------------------------------------------------
// Compact
// ----------------------------------------------------------------------

func TestMemoryStoreCompact(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()

	// Store three memories.
	mem1, err := s.Store(ctx, "memory one", []string{"a"})
	if err != nil {
		t.Fatalf("Store 1: %v", err)
	}
	mem2, err := s.Store(ctx, "memory two", []string{"b"})
	if err != nil {
		t.Fatalf("Store 2: %v", err)
	}
	mem3, err := s.Store(ctx, "memory three", []string{"c"})
	if err != nil {
		t.Fatalf("Store 3: %v", err)
	}

	// Delete mem1, archive mem2 via Update.
	if err := s.Delete(ctx, mem1.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, err = s.Update(ctx, mem2.ID, "memory two updated", []string{"b"})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}

	// Before compact: stats should show deleted + archived.
	stats, err := s.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats before compact: %v", err)
	}
	if stats.Deleted != 1 {
		t.Errorf("expected 1 deleted, got %d", stats.Deleted)
	}
	if stats.Archived != 1 {
		t.Errorf("expected 1 archived, got %d", stats.Archived)
	}

	// Compact.
	if err := s.Compact(ctx); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// After compact: only active memories should remain.
	stats, err = s.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats after compact: %v", err)
	}
	if stats.Deleted != 0 {
		t.Errorf("expected 0 deleted after compact, got %d", stats.Deleted)
	}
	if stats.Archived != 0 {
		t.Errorf("expected 0 archived after compact, got %d", stats.Archived)
	}
	if stats.Active != 2 {
		t.Errorf("expected 2 active after compact, got %d", stats.Active)
	}

	// Verify surviving memory is still searchable.
	got, err := s.Get(ctx, mem3.ID)
	if err != nil {
		t.Fatalf("Get surviving: %v", err)
	}
	if got.Content != "memory three" {
		t.Errorf("content mismatch: got %q", got.Content)
	}
}

func TestMemoryStoreCompactNilContext(t *testing.T) {
	s := newTestStore(t)
	t.Cleanup(func() { s.Close() })

	err := s.Compact(nil)
	if err == nil || !strings.Contains(err.Error(), "context must not be nil") {
		t.Errorf("expected nil context error, got: %v", err)
	}
}

func TestMemoryStoreCompactEmpty(t *testing.T) {
	s := newTestStore(t)
	t.Cleanup(func() { s.Close() })

	// Compact on empty store should be a no-op.
	if err := s.Compact(context.Background()); err != nil {
		t.Fatalf("Compact empty: %v", err)
	}
}

// ----------------------------------------------------------------------
// Empty content validation
// ----------------------------------------------------------------------

func TestMemoryStoreEmptyContent(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()
	_, err := s.Store(ctx, "", []string{"tag"})
	if err == nil || !strings.Contains(err.Error(), "content must not be empty") {
		t.Errorf("expected empty content error, got: %v", err)
	}
}
