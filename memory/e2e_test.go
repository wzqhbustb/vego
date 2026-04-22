package memory

import (
	"context"
	"fmt"
	"testing"
)

// TestIngestEndToEnd validates the full Extract → Reconcile → Search pipeline.
func TestIngestEndToEnd(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()

	ctx := context.Background()

	// 1. Bootstrap initial memories
	initial := []*Memory{
		{ID: "mem-1", Content: "Alice likes coffee", MemoryType: TypeInsight, State: StateActive, Tags: []string{"preference"}},
		{ID: "mem-2", Content: "Bob prefers tea", MemoryType: TypeInsight, State: StateActive, Tags: []string{"preference"}},
	}
	if err := s.Bootstrap(ctx, initial); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	// 2. Extract facts from messages (ModeRaw for determinism)
	messages := []Message{
		{Role: "user", Content: "Alice now likes tea"},
		{Role: "assistant", Content: "Noted, Alice switched to tea"},
	}
	facts, err := s.ExtractFacts(ctx, messages, ModeRaw)
	if err != nil {
		t.Fatalf("extract facts: %v", err)
	}
	if len(facts) != 2 {
		t.Fatalf("expected 2 facts, got %d", len(facts))
	}

	// 3. Reconcile (heuristic path, no LLM)
	s.llm = nil
	result, err := s.Reconcile(ctx, "agent-1", facts)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	// One fact should ADD, the other should ADD (no exact match)
	if result.Added != 2 {
		t.Errorf("expected 2 added, got %+v", result)
	}

	// 4. Search verifies new memories exist
	results, err := s.Search(ctx, "Alice tea")
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("expected search results for 'Alice tea'")
	}

	// 5. Store raw session messages
	stored, err := s.StoreRawMessages(ctx, "sess-e2e", []Message{
		{Role: "user", Content: "session message 1"},
		{Role: "assistant", Content: "session message 2"},
	})
	if err != nil {
		t.Fatalf("store raw messages: %v", err)
	}
	if stored != 2 {
		t.Errorf("expected 2 stored, got %d", stored)
	}

	// 6. Deduplication: same session messages again → 0 new
	stored2, err := s.StoreRawMessages(ctx, "sess-e2e", []Message{
		{Role: "user", Content: "session message 1"},
	})
	if err != nil {
		t.Fatalf("store raw messages 2: %v", err)
	}
	if stored2 != 0 {
		t.Errorf("expected 0 stored (dedup), got %d", stored2)
	}
}

// BenchmarkExtractFacts measures raw fact extraction (no network).
func BenchmarkExtractFacts(b *testing.B) {
	messages := []Message{
		{Role: "user", Content: "hello world"},
		{Role: "assistant", Content: "hi there"},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = extractFactsRaw(messages)
	}
}

// BenchmarkComputeContentHash measures SHA256 throughput.
func BenchmarkComputeContentHash(b *testing.B) {
	content := "The quick brown fox jumps over the lazy dog"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = computeContentHash(content)
	}
}

// TestIngestEndToEndUpdate verifies the UPDATE path in Reconcile.
func TestIngestEndToEndUpdate(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()

	ctx := context.Background()

	// Pre-store a memory
	mem, err := s.Store(ctx, "exact content", []string{"tag1"})
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	// Reconcile identical fact (heuristic → UPDATE)
	s.llm = nil
	facts := []ExtractedFact{{Content: "  exact content  "}}
	result, err := s.Reconcile(ctx, "agent-1", facts)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if result.Updated != 1 {
		t.Fatalf("expected 1 updated, got %+v", result)
	}

	// Verify old memory archived
	old, err := s.Get(ctx, mem.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if old.State != StateArchived {
		t.Errorf("old state = %s, want archived", old.State)
	}

	// Verify new memory exists with updated content
	results, err := s.Search(ctx, "exact content")
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("expected new memory")
	}
	// Find the active one (not archived)
	var found bool
	for _, m := range results {
		if m.State == StateActive && m.Content == "  exact content  " {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected active memory with updated content")
	}
}

// BenchmarkReconcile measures the heuristic reconcile path with a pre-populated store.
func BenchmarkReconcile(b *testing.B) {
	s := newTestStore(b)
	setupMockEmbedder(b, s, 128)
	defer s.Close()
	s.llm = nil // heuristic path for determinism

	ctx := context.Background()
	// Pre-populate with 20 memories for realistic candidate search.
	for i := 0; i < 20; i++ {
		_, _ = s.Store(ctx, fmt.Sprintf("base memory content %d", i), []string{"tag"})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		facts := []ExtractedFact{{Content: fmt.Sprintf("benchmark fact %d", i)}}
		_, _ = s.Reconcile(ctx, "agent-1", facts)
	}
}
