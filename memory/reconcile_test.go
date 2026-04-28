package memory

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	vego "github.com/wzqhbustb/vego/vego"
)

// ----------------------------------------------------------------------
// Empty / edge cases
// ----------------------------------------------------------------------

func TestReconcileEmpty(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()

	result, err := s.Reconcile(context.Background(), "agent-1", nil)
	if err != nil {
		t.Fatalf("reconcile nil: %v", err)
	}
	if result.Added != 0 || result.Updated != 0 || result.Deleted != 0 || result.Skipped != 0 {
		t.Errorf("expected zero result, got %+v", result)
	}
}

func TestReconcileQueryIntentFilter(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()

	facts := []ExtractedFact{
		{Content: "search for something", QueryIntent: true},
		{Content: "real fact", QueryIntent: false},
	}
	result, err := s.Reconcile(context.Background(), "agent-1", facts)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.Added != 1 {
		t.Errorf("expected 1 added (query intent filtered), got %+v", result)
	}
}

// ----------------------------------------------------------------------
// Heuristic path (no LLM)
// ----------------------------------------------------------------------

func TestReconcileHeuristicAdd(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()
	s.llm = nil // force heuristic path

	ctx := context.Background()

	// Pre-store an existing memory
	_, err := s.Store(ctx, "existing memory", []string{"tag1"})
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	// Reconcile a different fact
	facts := []ExtractedFact{{Content: "brand new fact"}}
	result, err := s.Reconcile(ctx, "agent-1", facts)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.Added != 1 {
		t.Errorf("expected 1 added, got %+v", result)
	}
}

func TestReconcileHeuristicUpdate(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()
	s.llm = nil // force heuristic path

	ctx := context.Background()

	// Pre-store an existing memory
	mem, err := s.Store(ctx, "exact match", []string{"tag1"})
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	// Reconcile an identical fact (case-insensitive, trimmed)
	facts := []ExtractedFact{{Content: "  exact match  "}}
	result, err := s.Reconcile(ctx, "agent-1", facts)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.Updated != 1 {
		t.Errorf("expected 1 updated, got %+v", result)
	}

	// Update uses archiveAndCreate: old memory is archived, new memory created.
	// Verify old memory is archived.
	old, err := s.Get(ctx, mem.ID)
	if err != nil {
		t.Fatalf("get old: %v", err)
	}
	if old.State != StateArchived {
		t.Errorf("old state = %s, want archived", old.State)
	}

	// Search for the new content to verify a new memory exists.
	results, err := s.Search(ctx, "exact match")
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("expected new memory with updated content")
	}
}

// ----------------------------------------------------------------------
// Pinned protection
// ----------------------------------------------------------------------

func TestReconcilePinnedProtectDelete(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()
	s.llm = nil

	ctx := context.Background()

	// Store a pinned memory
	mem := &Memory{
		ID:         vego.DocumentID(),
		Content:    "pinned content",
		MemoryType: TypePinned,
		State:      StateActive,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	vec, _ := s.embed(ctx, mem.Content)
	doc, _ := memoryToDoc(mem, vec)
	s.mu.Lock()
	s.coll.InsertContext(ctx, doc)
	s.inverted.Add(mem.ID, mem.Content)
	s.mu.Unlock()

	// Directly call executeAction with DELETE on pinned memory
	result := &IngestResult{}
	fact := &ExtractedFact{Content: "different content"}
	err := s.executeAction(ctx, "agent-1", fact, "DELETE", mem.ID, result)
	if err != nil {
		t.Fatalf("executeAction: %v", err)
	}
	if result.Deleted != 0 || result.Skipped != 1 {
		t.Errorf("expected skipped=1 for pinned delete, got %+v", result)
	}

	// Verify memory still exists and is active
	m, err := s.Get(ctx, mem.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if m.State != StateActive {
		t.Errorf("state = %s, want active", m.State)
	}
}

func TestReconcilePinnedProtectUpdate(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()
	s.llm = nil

	ctx := context.Background()

	// Store a pinned memory
	mem := &Memory{
		ID:         vego.DocumentID(),
		Content:    "pinned content",
		MemoryType: TypePinned,
		State:      StateActive,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	vec, _ := s.embed(ctx, mem.Content)
	doc, _ := memoryToDoc(mem, vec)
	s.mu.Lock()
	s.coll.InsertContext(ctx, doc)
	s.inverted.Add(mem.ID, mem.Content)
	s.mu.Unlock()

	// Execute UPDATE on pinned memory → should downgrade to ADD
	result := &IngestResult{}
	fact := &ExtractedFact{Content: "new content"}
	err := s.executeAction(ctx, "agent-1", fact, "UPDATE", mem.ID, result)
	if err != nil {
		t.Fatalf("executeAction: %v", err)
	}
	if result.Added != 1 || result.Updated != 0 {
		t.Errorf("expected ADD=1 (downgrade), got %+v", result)
	}
}

// ----------------------------------------------------------------------
// MaxFacts limit
// ----------------------------------------------------------------------

func TestReconcileMaxFacts(t *testing.T) {
	s := newTestStore(t, WithIngestParams(2, 1000))
	setupMockEmbedder(t, s, 128)
	defer s.Close()
	s.llm = nil

	facts := []ExtractedFact{
		{Content: "fact1"},
		{Content: "fact2"},
		{Content: "fact3"},
	}
	result, err := s.Reconcile(context.Background(), "agent-1", facts)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.Added != 2 {
		t.Errorf("expected 2 added (max facts=2), got %+v", result)
	}
}

// ----------------------------------------------------------------------
// NOOP action
// ----------------------------------------------------------------------

func TestReconcileNoop(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()
	s.llm = nil

	result := &IngestResult{}
	fact := &ExtractedFact{Content: "whatever"}
	err := s.executeAction(context.Background(), "agent-1", fact, "NOOP", "", result)
	if err != nil {
		t.Fatalf("executeAction: %v", err)
	}
	if result.Skipped != 1 {
		t.Errorf("expected skipped=1, got %+v", result)
	}
}

// ----------------------------------------------------------------------
// DELETE action (non-pinned)
// ----------------------------------------------------------------------

func TestReconcileDelete(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()
	s.llm = nil

	ctx := context.Background()

	mem, err := s.Store(ctx, "to be deleted", []string{"tag"})
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	result := &IngestResult{}
	fact := &ExtractedFact{Content: "irrelevant"}
	err = s.executeAction(ctx, "agent-1", fact, "DELETE", mem.ID, result)
	if err != nil {
		t.Fatalf("executeAction: %v", err)
	}
	if result.Deleted != 1 {
		t.Errorf("expected deleted=1, got %+v", result)
	}

	// Verify soft delete
	m, err := s.Get(ctx, mem.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if m.State != StateDeleted {
		t.Errorf("state = %s, want deleted", m.State)
	}
}

// ----------------------------------------------------------------------
// Concurrent reconcile
// ----------------------------------------------------------------------

func TestReconcileConcurrent(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()
	s.llm = nil

	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			facts := []ExtractedFact{{Content: fmt.Sprintf("concurrent fact %d", idx)}}
			_, err := s.Reconcile(ctx, "agent-1", facts)
			if err != nil {
				t.Errorf("reconcile goroutine %d: %v", idx, err)
			}
		}(i)
	}
	wg.Wait()

	// Verify all 10 facts were added (use ForEach to avoid SearchLimit dependency)
	count := 0
	if err := s.coll.ForEach(func(doc *vego.Document) bool {
		m, err := docToMemory(doc)
		if err != nil {
			t.Logf("skip corrupt doc: %v", err)
			return true
		}
		if m.State == StateActive {
			count++
		}
		return true
	}); err != nil {
		t.Fatalf("foreach: %v", err)
	}
	if count != 10 {
		t.Errorf("expected 10 active memories, got %d", count)
	}
}

// ----------------------------------------------------------------------
// Mock LLM server
// ----------------------------------------------------------------------

type mockLLMServer struct {
	response string
}

func (m *mockLLMServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"choices": []map[string]interface{}{
			{
				"message": map[string]interface{}{
					"content": m.response,
				},
			},
		},
		"usage": map[string]interface{}{
			"prompt_tokens":     10,
			"completion_tokens": 5,
		},
	})
}

func setupMockLLM(t *testing.T, s *MemoryStore, response string) {
	t.Helper()
	srv := httptest.NewServer(&mockLLMServer{response: response})
	t.Cleanup(srv.Close)
	s.llm = NewLLMClient(LLMConfig{
		APIKey:  "test",
		BaseURL: srv.URL,
		Model:   "test-model",
	})
	if s.llm == nil {
		t.Fatal("mock llm nil")
	}
}

// ----------------------------------------------------------------------
// LLM path tests
// ----------------------------------------------------------------------

func TestReconcileLLMAdd(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()

	ctx := context.Background()
	_, err := s.Store(ctx, "existing memory", []string{"tag"})
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	setupMockLLM(t, s, `{"action":"ADD","target_id":1,"reason":"new fact"}`)

	facts := []ExtractedFact{{Content: "brand new fact"}}
	result, err := s.Reconcile(ctx, "agent-1", facts)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.Added != 1 {
		t.Errorf("expected 1 added, got %+v", result)
	}
}

func TestReconcileLLMUpdate(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()

	ctx := context.Background()
	mem, err := s.Store(ctx, "old content", []string{"tag"})
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	setupMockLLM(t, s, `{"action":"UPDATE","target_id":1,"reason":"content changed"}`)

	facts := []ExtractedFact{{Content: "updated content"}}
	result, err := s.Reconcile(ctx, "agent-1", facts)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.Updated != 1 {
		t.Errorf("expected 1 updated, got %+v", result)
	}

	old, err := s.Get(ctx, mem.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if old.State != StateArchived {
		t.Errorf("old state = %s, want archived", old.State)
	}
}

func TestReconcileLLMDelete(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()

	ctx := context.Background()
	mem, err := s.Store(ctx, "to delete", []string{"tag"})
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	setupMockLLM(t, s, `{"action":"DELETE","target_id":1,"reason":"outdated"}`)

	facts := []ExtractedFact{{Content: "irrelevant"}}
	result, err := s.Reconcile(ctx, "agent-1", facts)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.Deleted != 1 {
		t.Errorf("expected 1 deleted, got %+v", result)
	}

	m, err := s.Get(ctx, mem.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if m.State != StateDeleted {
		t.Errorf("state = %s, want deleted", m.State)
	}
}

func TestReconcileLLMNoop(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()

	ctx := context.Background()
	_, err := s.Store(ctx, "keep this", []string{"tag"})
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	setupMockLLM(t, s, `{"action":"NOOP","target_id":1,"reason":"already exists"}`)

	facts := []ExtractedFact{{Content: "keep this"}}
	result, err := s.Reconcile(ctx, "agent-1", facts)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.Skipped != 1 {
		t.Errorf("expected 1 skipped, got %+v", result)
	}
}

func TestReconcileLLMInvalidAction(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()

	ctx := context.Background()
	_, err := s.Store(ctx, "existing", []string{"tag"})
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	setupMockLLM(t, s, `{"action":"UNKNOWN","target_id":1,"reason":"?"}`)

	facts := []ExtractedFact{{Content: "new fact"}}
	result, err := s.Reconcile(ctx, "agent-1", facts)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.Added != 1 {
		t.Errorf("expected 1 added (fallback), got %+v", result)
	}
}

func TestReconcileLLMInvalidTargetID(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()

	ctx := context.Background()
	_, err := s.Store(ctx, "existing", []string{"tag"})
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	setupMockLLM(t, s, `{"action":"UPDATE","target_id":999,"reason":"wrong id"}`)

	facts := []ExtractedFact{{Content: "new fact"}}
	result, err := s.Reconcile(ctx, "agent-1", facts)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.Added != 1 {
		t.Errorf("expected 1 added (fallback), got %+v", result)
	}
}

// ----------------------------------------------------------------------
// Metadata overlay (regression test for nil map panic)
// ----------------------------------------------------------------------

// TestReconcileUpdateNilMetadataOverlay verifies that UPDATE works when the
// old memory has nil Metadata and the incoming fact carries metadata (e.g.
// temporal).  This path previously panicked because copyMap(nil)
// returns nil, and the overlay loop tried to write into a nil map.
func TestReconcileUpdateNilMetadataOverlay(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()
	s.llm = nil // force heuristic path

	ctx := context.Background()

	// Store a memory without Metadata (Store does not set Metadata).
	mem, err := s.Store(ctx, "exact match", nil)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	if mem.Metadata != nil {
		t.Fatalf("precondition failed: old memory should have nil Metadata")
	}

	// Reconcile an identical fact with metadata overlay.
	facts := []ExtractedFact{
		{
			Content: "exact match",
			Metadata: map[string]interface{}{
				"temporal": &TemporalMetadata{
					ResolvedStart: "2026-04-20",
					Display:       "昨天",
				},
			},
		},
	}
	result, err := s.Reconcile(ctx, "agent-1", facts)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.Updated != 1 {
		t.Errorf("expected 1 updated, got %+v", result)
	}

	// Verify the new memory has the overlay metadata.
	old, err := s.Get(ctx, mem.ID)
	if err != nil {
		t.Fatalf("get old: %v", err)
	}
	if old.State != StateArchived {
		t.Errorf("old state = %s, want archived", old.State)
	}

	results, err := s.Search(ctx, "exact match")
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("expected new memory with updated content")
	}
	newMem := results[0]
	if newMem.Metadata == nil {
		t.Fatal("expected new memory to have metadata")
	}
	if _, ok := newMem.Metadata["temporal"]; !ok {
		t.Errorf("expected temporal metadata in new memory, got %v", newMem.Metadata)
	}
}
