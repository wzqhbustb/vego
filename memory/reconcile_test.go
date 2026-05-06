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
	t.Cleanup(func() { s.Close() })

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
	t.Cleanup(func() { s.Close() })

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
	t.Cleanup(func() { s.Close() })
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
	t.Cleanup(func() { s.Close() })
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
	t.Cleanup(func() { s.Close() })
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
	t.Cleanup(func() { s.Close() })
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
	t.Cleanup(func() { s.Close() })
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
	t.Cleanup(func() { s.Close() })
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
	t.Cleanup(func() { s.Close() })
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
	t.Cleanup(func() { s.Close() })
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
	t.Cleanup(func() { s.Close() })

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
	t.Cleanup(func() { s.Close() })

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
	t.Cleanup(func() { s.Close() })

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
	t.Cleanup(func() { s.Close() })

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
	t.Cleanup(func() { s.Close() })

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
	t.Cleanup(func() { s.Close() })

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
	t.Cleanup(func() { s.Close() })
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

// ----------------------------------------------------------------------
// executeAction white-box tests (coverage gaps in action execution)
// ----------------------------------------------------------------------

func TestExecuteAction_UpdateWithoutTarget(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	result := &IngestResult{}
	fact := &ExtractedFact{Content: "update content", Tags: []string{"a"}}
	err := s.executeAction(context.Background(), "agent-1", fact, "UPDATE", "", result)
	if err == nil {
		t.Fatal("expected error for UPDATE without targetID")
	}
	if result.Updated != 0 {
		t.Errorf("Updated should be 0, got %d", result.Updated)
	}
}

func TestExecuteAction_UpdatePinnedTarget(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()

	mem := &Memory{
		ID:         vego.DocumentID(),
		Content:    "pinned content",
		MemoryType: TypePinned,
		State:      StateActive,
		Version:    1,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	vec, err := s.embed(ctx, mem.Content)
	if err != nil {
		t.Fatalf("embed: %v", err)
	}
	doc, err := memoryToDoc(mem, vec)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := s.coll.InsertContext(ctx, doc); err != nil {
		t.Fatalf("insert: %v", err)
	}
	s.inverted.Add(mem.ID, mem.Content)

	result := &IngestResult{}
	fact := &ExtractedFact{Content: "updated content", Tags: []string{"a"}}
	err = s.executeAction(ctx, "agent-1", fact, "UPDATE", mem.ID, result)
	if err != nil {
		t.Fatalf("UPDATE on pinned should downgrade to ADD, got error: %v", err)
	}
	if result.Added != 1 {
		t.Errorf("expected 1 Added (downgrade), got %+v", result)
	}
	if result.Updated != 0 {
		t.Errorf("expected 0 Updated, got %d", result.Updated)
	}
}

func TestExecuteAction_UpdateNonActiveTarget(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()

	mem := &Memory{
		ID:         vego.DocumentID(),
		Content:    "archived content",
		MemoryType: TypeInsight,
		State:      StateArchived,
		Version:    1,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	vec, err := s.embed(ctx, mem.Content)
	if err != nil {
		t.Fatalf("embed: %v", err)
	}
	doc, err := memoryToDoc(mem, vec)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := s.coll.InsertContext(ctx, doc); err != nil {
		t.Fatalf("insert: %v", err)
	}

	result := &IngestResult{}
	fact := &ExtractedFact{Content: "updated content", Tags: []string{"a"}}
	err = s.executeAction(ctx, "agent-1", fact, "UPDATE", mem.ID, result)
	if err != nil {
		t.Fatalf("UPDATE on non-active should downgrade to ADD, got error: %v", err)
	}
	if result.Added != 1 {
		t.Errorf("expected 1 Added (downgrade), got %+v", result)
	}
}

func TestExecuteAction_UpdateGetError(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	result := &IngestResult{}
	fact := &ExtractedFact{Content: "content", Tags: []string{"a"}}
	err := s.executeAction(context.Background(), "agent-1", fact, "UPDATE", "nonexistent", result)
	if err == nil {
		t.Fatal("expected error for UPDATE with non-existent target")
	}
}

func TestExecuteAction_DeleteWithoutTarget(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	result := &IngestResult{}
	fact := &ExtractedFact{Content: "delete me"}
	err := s.executeAction(context.Background(), "agent-1", fact, "DELETE", "", result)
	if err == nil {
		t.Fatal("expected error for DELETE without targetID")
	}
	if result.Deleted != 0 {
		t.Errorf("Deleted should be 0, got %d", result.Deleted)
	}
}

func TestExecuteAction_DeletePinnedTarget(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()

	mem := &Memory{
		ID:         vego.DocumentID(),
		Content:    "pinned content",
		MemoryType: TypePinned,
		State:      StateActive,
		Version:    1,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	vec, err := s.embed(ctx, mem.Content)
	if err != nil {
		t.Fatalf("embed: %v", err)
	}
	doc, err := memoryToDoc(mem, vec)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := s.coll.InsertContext(ctx, doc); err != nil {
		t.Fatalf("insert: %v", err)
	}
	s.inverted.Add(mem.ID, mem.Content)

	result := &IngestResult{}
	fact := &ExtractedFact{Content: "delete me"}
	err = s.executeAction(ctx, "agent-1", fact, "DELETE", mem.ID, result)
	if err != nil {
		t.Fatalf("DELETE on pinned should skip, got error: %v", err)
	}
	if result.Skipped != 1 {
		t.Errorf("expected 1 Skipped, got %+v", result)
	}
	if result.Deleted != 0 {
		t.Errorf("expected 0 Deleted, got %d", result.Deleted)
	}
}

func TestExecuteAction_DeleteNonActiveTarget(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()

	mem := &Memory{
		ID:         vego.DocumentID(),
		Content:    "archived content",
		MemoryType: TypeInsight,
		State:      StateArchived,
		Version:    1,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	vec, err := s.embed(ctx, mem.Content)
	if err != nil {
		t.Fatalf("embed: %v", err)
	}
	doc, err := memoryToDoc(mem, vec)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := s.coll.InsertContext(ctx, doc); err != nil {
		t.Fatalf("insert: %v", err)
	}

	result := &IngestResult{}
	fact := &ExtractedFact{Content: "delete me"}
	err = s.executeAction(ctx, "agent-1", fact, "DELETE", mem.ID, result)
	if err != nil {
		t.Fatalf("DELETE on non-active should skip, got error: %v", err)
	}
	if result.Skipped != 1 {
		t.Errorf("expected 1 Skipped, got %+v", result)
	}
}

func TestExecuteAction_DeleteGetError(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	result := &IngestResult{}
	fact := &ExtractedFact{Content: "delete me"}
	err := s.executeAction(context.Background(), "agent-1", fact, "DELETE", "nonexistent", result)
	if err == nil {
		t.Fatal("expected error for DELETE with non-existent target")
	}
}

func TestExecuteAction_UnknownAction(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	result := &IngestResult{}
	fact := &ExtractedFact{Content: "hello"}
	err := s.executeAction(context.Background(), "agent-1", fact, "INVALID", "", result)
	if err == nil {
		t.Fatal("expected error for unknown action")
	}
}

// ----------------------------------------------------------------------
// Reconcile error path tests
// ----------------------------------------------------------------------

func TestReconcile_SearchError(t *testing.T) {
	s := newTestStore(t)
	t.Cleanup(func() { s.Close() })

	// Explicitly break the embedder to force a deterministic search error
	// (rather than relying on HTTP 401 from the real OpenAI endpoint).
	s.embedder = nil

	facts := []ExtractedFact{{Content: "test fact"}}
	result, err := s.Reconcile(context.Background(), "agent-1", facts)
	if err != nil {
		t.Fatalf("Reconcile should not fail on search error: %v", err)
	}
	// Without embedder: findCandidates returns error → candidates=nil.
	// decideAction with no candidates → ADD → executeAction ADD also
	// fails (embedder nil) → Skipped++.
	if result.Skipped != 1 {
		t.Errorf("expected 1 Skipped, got %+v", result)
	}
}

func TestReconcile_DecideActionLLMError(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()
	mem, err := s.Store(ctx, "existing content", nil)
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	// LLM returns invalid chat response → CompleteJSON fails → decideAction returns error → ADD fallback.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`not-valid-json`))
	}))
	t.Cleanup(srv.Close)
	s.llm = NewLLMClient(LLMConfig{
		APIKey:  "test",
		BaseURL: srv.URL,
		Model:   "test-model",
	})

	facts := []ExtractedFact{{Content: "brand new fact"}}
	result, err := s.Reconcile(ctx, "agent-1", facts)
	if err != nil {
		t.Fatalf("Reconcile should not fail: %v", err)
	}
	// Fallback to ADD after decideAction parse error.
	if result.Added != 1 {
		t.Errorf("expected 1 Added (fallback), got %+v", result)
	}
	_ = mem
}


// ----------------------------------------------------------------------
// ADD error path test
// ----------------------------------------------------------------------

func TestExecuteAction_ADD_EmbedError(t *testing.T) {
	s := newTestStore(t)
	// Deliberately no mock embedder → embed will fail.
	t.Cleanup(func() { s.Close() })

	result := &IngestResult{}
	fact := &ExtractedFact{Content: "brand new fact", Tags: []string{"test"}}
	err := s.executeAction(context.Background(), "agent-1", fact, "ADD", "", result)
	if err == nil {
		t.Fatal("expected embed error for ADD without embedder")
	}
	if result.Added != 0 {
		t.Errorf("Added should be 0 on error, got %d", result.Added)
	}
}

// ----------------------------------------------------------------------
// Reconcile: panic in search goroutine → semaphore release
// ----------------------------------------------------------------------

func TestReconcile_SearchPanicSemaphoreRelease(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	// Seed an existing memory so StoreBatch inside executeAction has something
	// to work with (avoid nil embedder).
	_, err := s.Store(context.Background(), "existing memory for reconcile", nil)
	if err != nil {
		t.Fatalf("Store seed: %v", err)
	}

	// Set up mock LLM so decideAction returns ADD with valid JSON.
	setupMockLLM(t, s, `ADD||`)

	// Inject a panic in findCandidates (called from the search goroutine).
	// The panic must happen while searchSem is held. The defer searchSem.Release(1)
	// (registered before findCandidates) must release the semaphore so the
	// WaitGroup completes and Reconcile returns without deadlock.
	var mu sync.Mutex
	panicCount := 0
	testReconcileSearchPanicHook = func() {
		mu.Lock()
		panicCount++
		mu.Unlock()
		panic("injected panic in findCandidates")
	}
	t.Cleanup(func() { testReconcileSearchPanicHook = nil })

	ctx := context.Background()
	result, err := s.Reconcile(ctx, "agent-panic-test", []ExtractedFact{
		{Content: "fact one", SourceMsg: 0},
		{Content: "fact two", SourceMsg: 1},
		{Content: "fact three", SourceMsg: 2},
	})
	if err != nil {
		t.Fatalf("Reconcile should not error on search panic: %v", err)
	}
	// All facts fall through to ADD fallback when search panics.
	if result.Added != 3 {
		t.Errorf("expected 3 added (fallback), got %+v", result)
	}
	mu.Lock()
	if panicCount != 3 {
		t.Errorf("panic hook should have been called 3 times, got %d", panicCount)
	}
	mu.Unlock()
}

// ----------------------------------------------------------------------
// Near-duplicate suppression
// ----------------------------------------------------------------------

func TestReconcileNearDupSuppression(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()

	// Seed an existing memory.
	_, err := s.Store(ctx, "user likes golang", nil)
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	// Fact with identical content — mock embedder gives identical vectors,
	// so cosine similarity = 1.0.
	facts := []ExtractedFact{{Content: "user likes golang"}}

	// With threshold = 0.95, the fact should be suppressed (NOOP) without LLM call.
	s.config.NearDupThreshold = 0.95
	result, err := s.Reconcile(ctx, "agent-1", facts)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.Added != 0 {
		t.Errorf("added: want 0 (suppressed), got %d", result.Added)
	}
	if result.Updated != 0 {
		t.Errorf("updated: want 0 (suppressed), got %d", result.Updated)
	}
	if result.NearDupSkipped != 1 {
		t.Errorf("nearDupSkipped: want 1 (suppressed), got %d", result.NearDupSkipped)
	}

	// With threshold disabled (0), it should go through normal Reconcile path.
	facts2 := []ExtractedFact{{Content: "user likes golang"}}
	s.config.NearDupThreshold = 0
	result2, err := s.Reconcile(ctx, "agent-1", facts2)
	if err != nil {
		t.Fatalf("reconcile no threshold: %v", err)
	}
	if result2.NearDupSkipped != 0 {
		t.Errorf("nearDupSkipped: want 0 (disabled), got %d", result2.NearDupSkipped)
	}
}
