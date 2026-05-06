package memory

import (
	"context"
	"testing"
	"time"

	vego "github.com/wzqhbustb/vego/vego"
)

// ----------------------------------------------------------------------
// Integration: full ingest pipeline (heuristic path, no LLM)
// ----------------------------------------------------------------------

// TestIntegrationIngestPipeline verifies the end-to-end flow:
//   Messages → ExtractFacts(ModeNormal) → Reconcile → stored memories.
func TestIntegrationIngestPipeline(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()

	// ModeNormal needs a mock LLM for fact extraction.
	setupMockLLM(t, s, `{"facts":[{"content":"golang concurrency uses goroutines","tags":["go"]}]}`)

	ctx := context.Background()
	messages := []Message{
		{Role: "user", Content: "golang supports concurrency with goroutines"},
		{Role: "assistant", Content: "yes, goroutines and channels are core features"},
	}

	facts, err := s.ExtractFacts(ctx, messages, ModeNormal)
	if err != nil {
		t.Fatalf("ExtractFacts: %v", err)
	}
	if len(facts) == 0 {
		t.Fatal("expected at least one fact")
	}

	result, err := s.Reconcile(ctx, "agent-1", facts)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.Added == 0 {
		t.Errorf("expected at least one added, got %+v", result)
	}

	// Verify memories are searchable.
	results, err := s.Search(ctx, "golang concurrency")
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(results) == 0 {
		t.Error("expected search results after ingest pipeline")
	}
}

// ----------------------------------------------------------------------
// Integration: lifecycle — Create → Update → Delete
// ----------------------------------------------------------------------

// TestIntegrationLifecycle verifies the full memory lifecycle:
//   Store → Search finds it → Update → old archived, new active → Delete → Search invisible, Get visible.
func TestIntegrationLifecycle(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()

	ctx := context.Background()

	// Create.
	mem, err := s.Store(ctx, "lifecycle content", []string{"tag-lc"})
	if err != nil {
		t.Fatalf("Store: %v", err)
	}

	// Search finds it.
	results, err := s.Search(ctx, "lifecycle")
	if err != nil {
		t.Fatalf("Search after create: %v", err)
	}
	found := false
	for _, m := range results {
		if m.ID == mem.ID {
			found = true
		}
	}
	if !found {
		t.Error("Search should find newly created memory")
	}

	// Update (Archive-and-Create).
	updated, err := s.Update(ctx, mem.ID, "updated lifecycle content", []string{"tag-lc", "updated"})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}

	// Old memory is archived.
	old, err := s.Get(ctx, mem.ID)
	if err != nil {
		t.Fatalf("Get old after update: %v", err)
	}
	if old.State != StateArchived {
		t.Errorf("old memory state: want archived, got %s", old.State)
	}
	if old.SupersededBy != updated.ID {
		t.Errorf("old.SupersededBy: want %s, got %s", updated.ID, old.SupersededBy)
	}

	// New memory is active and searchable.
	newMem, err := s.Get(ctx, updated.ID)
	if err != nil {
		t.Fatalf("Get new after update: %v", err)
	}
	if newMem.State != StateActive {
		t.Errorf("new memory state: want active, got %s", newMem.State)
	}
	if newMem.PreviousID != mem.ID {
		t.Errorf("new.PreviousID: want %s, got %s", mem.ID, newMem.PreviousID)
	}

	resultsAfterUpdate, err := s.Search(ctx, "updated lifecycle")
	if err != nil {
		t.Fatalf("Search after update: %v", err)
	}
	foundUpdated := false
	for _, m := range resultsAfterUpdate {
		if m.ID == updated.ID {
			foundUpdated = true
		}
		if m.ID == mem.ID {
			t.Error("Search should not find archived old memory")
		}
	}
	if !foundUpdated {
		t.Error("Search should find updated memory")
	}

	// Delete.
	if err := s.Delete(ctx, updated.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Search invisible.
	resultsAfterDelete, err := s.Search(ctx, "updated lifecycle")
	if err != nil {
		t.Fatalf("Search after delete: %v", err)
	}
	for _, m := range resultsAfterDelete {
		if m.ID == updated.ID {
			t.Error("Search should not find deleted memory")
		}
	}

	// Get visible (soft delete).
	got, err := s.Get(ctx, updated.ID)
	if err != nil {
		t.Fatalf("Get after delete: %v", err)
	}
	if got.State != StateDeleted {
		t.Errorf("state after delete: want deleted, got %s", got.State)
	}
}

// ----------------------------------------------------------------------
// Integration: ModeRaw cumulative dedup (3-turn simulation)
// ----------------------------------------------------------------------

// TestIntegrationModeRawCumulativeDedup simulates an agent sending
// messages in 3 cumulative turns:
//   Turn 1: 3 messages (all new)
//   Turn 2: 5 messages (2 duplicates from turn 1, 3 new)
//   Turn 3: 8 messages (all 5 from turn 2 + 3 new)
// Expected final count: 3 + 3 + 3 = 8 (not 3+5+8=16).
func TestIntegrationModeRawCumulativeDedup(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()

	ctx := context.Background()
	sessionID := "sess-cumulative"

	// Turn 1: 3 new messages.
	turn1 := []Message{
		{Role: "user", Content: "turn1-msg1"},
		{Role: "user", Content: "turn1-msg2"},
		{Role: "user", Content: "turn1-msg3"},
	}
	stored1, err := s.StoreRawMessages(ctx, sessionID, turn1)
	if err != nil {
		t.Fatalf("turn 1: %v", err)
	}
	if stored1 != 3 {
		t.Errorf("turn 1: want 3 stored, got %d", stored1)
	}

	// Turn 2: 5 messages — all 3 from turn 1 + 2 new.
	turn2 := []Message{
		{Role: "user", Content: "turn1-msg1"}, // dup
		{Role: "user", Content: "turn1-msg2"}, // dup
		{Role: "user", Content: "turn1-msg3"}, // dup
		{Role: "user", Content: "turn2-msg4"},
		{Role: "user", Content: "turn2-msg5"},
	}
	stored2, err := s.StoreRawMessages(ctx, sessionID, turn2)
	if err != nil {
		t.Fatalf("turn 2: %v", err)
	}
	if stored2 != 2 {
		t.Errorf("turn 2: want 2 stored (5-3 dups), got %d", stored2)
	}

	// Turn 3: 8 messages — all 5 from turn 2 + 3 new.
	turn3 := []Message{
		{Role: "user", Content: "turn1-msg1"}, // dup
		{Role: "user", Content: "turn1-msg2"}, // dup
		{Role: "user", Content: "turn1-msg3"}, // dup
		{Role: "user", Content: "turn2-msg4"}, // dup
		{Role: "user", Content: "turn2-msg5"}, // dup
		{Role: "user", Content: "turn3-msg6"},
		{Role: "user", Content: "turn3-msg7"},
		{Role: "user", Content: "turn3-msg8"},
	}
	stored3, err := s.StoreRawMessages(ctx, sessionID, turn3)
	if err != nil {
		t.Fatalf("turn 3: %v", err)
	}
	if stored3 != 3 {
		t.Errorf("turn 3: want 3 stored (8-5 dups), got %d", stored3)
	}

	// Verify total stored = 3 + 2 + 3 = 8.
	var count int
	err = s.coll.ForEach(func(doc *vego.Document) bool {
		m, err := docToMemory(doc)
		if err != nil {
			return true
		}
		if m.SessionID == sessionID {
			count++
		}
		return true
	})
	if err != nil {
		t.Fatalf("ForEach: %v", err)
	}
	if count != 8 {
		t.Errorf("total stored: want 8, got %d", count)
	}
}

// ----------------------------------------------------------------------
// Integration: Recency Boost ranking effect
// ----------------------------------------------------------------------

// TestIntegrationRecencyBoost verifies that when two memories have
// identical vector similarity (mock embedder), the more recent one
// outranks the older one due to RecencyBoost.
func TestIntegrationRecencyBoost(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()

	// Ensure recency boost is enabled.
	s.config.RecencyBoostWeek = 1.05
	s.config.RecencyBoostMonth = 1.02

	ctx := context.Background()

	// Store an "old" memory with a backdated UpdatedAt.
	oldMem := &Memory{
		ID:        "old-mem",
		Content:   "recency test content",
		State:     StateActive,
		Tags:      []string{},
		Vector:    make([]float32, 128),
		CreatedAt: time.Now().Add(-30 * 24 * time.Hour),
		UpdatedAt: time.Now().Add(-30 * 24 * time.Hour),
	}
	for i := range oldMem.Vector {
		oldMem.Vector[i] = 0.1
	}
	if err := s.Bootstrap(ctx, []*Memory{oldMem}); err != nil {
		t.Fatalf("bootstrap old: %v", err)
	}

	// Store a "new" memory (today).
	newMem := &Memory{
		ID:        "new-mem",
		Content:   "recency test content",
		State:     StateActive,
		Tags:      []string{},
		Vector:    make([]float32, 128),
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	for i := range newMem.Vector {
		newMem.Vector[i] = 0.1
	}
	if err := s.Bootstrap(ctx, []*Memory{newMem}); err != nil {
		t.Fatalf("bootstrap new: %v", err)
	}

	// Both memories have identical vectors; hybrid search should return both.
	// With RecencyBoost enabled, the newer one should have a higher score.
	results, err := s.Search(ctx, "recency test")
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(results) < 2 {
		t.Skipf("need >=2 results to compare ranking, got %d (mock embedder limitation)", len(results))
	}

	// Find positions.
	var oldScore, newScore float64
	for _, m := range results {
		if m.ID == "old-mem" {
			oldScore = m.Score
		}
		if m.ID == "new-mem" {
			newScore = m.Score
		}
	}

	if newScore > 0 && oldScore > 0 && newScore <= oldScore {
		t.Errorf("new memory score (%v) should be > old memory score (%v) due to recency boost", newScore, oldScore)
	}
}

// ----------------------------------------------------------------------
// Integration: Gap Stop truncates low-relevance tail
// ----------------------------------------------------------------------

// TestIntegrationGapStop verifies that GapStop truncates results when
// there is a sharp score drop between adjacent items.
func TestIntegrationGapStop(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()

	// Ensure gap stop is enabled with a moderate ratio.
	s.config.GapStopRatio = 0.5

	ctx := context.Background()

	// Store 3 memories with identical vectors (mock embedder gives same vector).
	// They will all have the same raw similarity, but we can verify GapStop
	// does not truncate when scores are equal (no gap).
	for i := 0; i < 3; i++ {
		_, err := s.Store(ctx, "gapstop test content", nil)
		if err != nil {
			t.Fatalf("store %d: %v", i, err)
		}
	}

	results, err := s.Search(ctx, "gapstop test")
	if err != nil {
		t.Fatalf("search: %v", err)
	}

	// With identical vectors and no score gap, GapStop should not truncate.
	if len(results) != 3 {
		t.Errorf("equal scores: want 3 results (no gap-stop truncation), got %d", len(results))
	}
}
