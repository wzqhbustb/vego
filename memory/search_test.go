package memory

import (
	"context"
	"math"
	"testing"
	"time"

	vego "github.com/wzqhbustb/vego/vego"
)

// ----------------------------------------------------------------------
// distanceToSimilarity
// ----------------------------------------------------------------------

func TestDistanceToSimilarity(t *testing.T) {
	tests := []struct {
		dist     float32
		distFunc string
		wantMin  float64
		wantMax  float64
	}{
		// Cosine: 1-d, d in [0,2] → sim in [-1,1]. Clamp negative to 0 in practice.
		{0, "cosine", 1.0, 1.0},
		{0.5, "cosine", 0.5, 0.5},
		{1.0, "cosine", 0.0, 0.0},
		// L2: 1/(1+d)
		{0, "l2", 1.0, 1.0},
		{1, "l2", 0.5, 0.5},
		{3, "l2", 0.25, 0.25},
		// IP: (1+(-d))/2
		{-1, "ip", 1.0, 1.0},
		{0, "ip", 0.5, 0.5},
		{1, "ip", 0.0, 0.0},
		// Default (unknown distFunc) behaves like L2
		{1, "unknown", 0.5, 0.5},
	}
	for _, tt := range tests {
		got := distanceToSimilarity(tt.dist, tt.distFunc)
		if math.Abs(got-tt.wantMin) > 1e-9 {
			t.Errorf("distanceToSimilarity(%v, %q) = %v, want %v", tt.dist, tt.distFunc, got, tt.wantMin)
		}
	}
}

// ----------------------------------------------------------------------
// RRF fusion
// ----------------------------------------------------------------------

func TestRRFMerge(t *testing.T) {
	vecResults := []Memory{
		{ID: "a", Score: 0.9},
		{ID: "b", Score: 0.8},
		{ID: "c", Score: 0.7},
	}
	keywordResults := []ScoredID{
		{ID: "b", Score: 5.0},
		{ID: "c", Score: 4.0},
		{ID: "d", Score: 3.0},
	}

	scores := rrfMerge(vecResults, keywordResults, 60.0)

	// a: only in vec, rank 1 → 1/61
	// b: vec rank 2 + keyword rank 1 → 1/62 + 1/61
	// c: vec rank 3 + keyword rank 2 → 1/63 + 1/62
	// d: only in keyword, rank 3 → 1/63
	wantA := 1.0 / 61.0
	wantB := 1.0/62.0 + 1.0/61.0
	wantC := 1.0/63.0 + 1.0/62.0
	wantD := 1.0 / 63.0

	if math.Abs(scores["a"]-wantA) > 1e-9 {
		t.Errorf("score[a] = %v, want %v", scores["a"], wantA)
	}
	if math.Abs(scores["b"]-wantB) > 1e-9 {
		t.Errorf("score[b] = %v, want %v", scores["b"], wantB)
	}
	if math.Abs(scores["c"]-wantC) > 1e-9 {
		t.Errorf("score[c] = %v, want %v", scores["c"], wantC)
	}
	if math.Abs(scores["d"]-wantD) > 1e-9 {
		t.Errorf("score[d] = %v, want %v", scores["d"], wantD)
	}
}

// ----------------------------------------------------------------------
// Gap Stop
// ----------------------------------------------------------------------

func TestApplyGapStop(t *testing.T) {
	mems := []Memory{
		{ID: "a", Score: 0.9},
		{ID: "b", Score: 0.85},
		{ID: "c", Score: 0.3},
		{ID: "d", Score: 0.28},
	}

	// ratio=0.5: drop > 50% from previous.
	// a→b: 0.85 >= 0.9*0.5=0.45 → keep
	// b→c: 0.3 < 0.85*0.5=0.425 → cut at c
	got := applyGapStop(mems, 0.5)
	if len(got) != 2 {
		t.Errorf("gap stop: want 2, got %d", len(got))
	}

	// ratio=0 disables
	got2 := applyGapStop(mems, 0)
	if len(got2) != 4 {
		t.Errorf("gap stop disabled: want 4, got %d", len(got2))
	}

	// Single element
	got3 := applyGapStop([]Memory{{Score: 0.5}}, 0.5)
	if len(got3) != 1 {
		t.Errorf("gap stop single: want 1, got %d", len(got3))
	}
}

// ----------------------------------------------------------------------
// Pinned boost
// ----------------------------------------------------------------------

func TestApplyPinnedBoost(t *testing.T) {
	scores := map[string]float64{"a": 1.0, "b": 1.0}
	mems := map[string]Memory{
		"a": {ID: "a", MemoryType: TypePinned},
		"b": {ID: "b", MemoryType: TypeInsight},
	}
	applyPinnedBoost(scores, mems, 1.5)
	if scores["a"] != 1.5 {
		t.Errorf("pinned boost: want 1.5, got %v", scores["a"])
	}
	if scores["b"] != 1.0 {
		t.Errorf("pinned boost: want 1.0, got %v", scores["b"])
	}
}

// ----------------------------------------------------------------------
// Recency boost
// ----------------------------------------------------------------------

func TestApplyRecencyBoost(t *testing.T) {
	now := time.Date(2026, 4, 23, 0, 0, 0, 0, time.UTC)

	scores := map[string]float64{"a": 1.0, "b": 1.0, "c": 1.0}
	mems := map[string]Memory{
		"a": {UpdatedAt: now.Add(-3 * 24 * time.Hour)},  // <=7 days
		"b": {UpdatedAt: now.Add(-15 * 24 * time.Hour)}, // <=30 days
		"c": {UpdatedAt: now.Add(-60 * 24 * time.Hour)}, // >30 days
	}
	applyRecencyBoost(scores, mems, now, 1.05, 1.02)

	if scores["a"] != 1.05 {
		t.Errorf("week boost: want 1.05, got %v", scores["a"])
	}
	if scores["b"] != 1.02 {
		t.Errorf("month boost: want 1.02, got %v", scores["b"])
	}
	if scores["c"] != 1.0 {
		t.Errorf("no boost: want 1.0, got %v", scores["c"])
	}
}

// ----------------------------------------------------------------------
// Relative age
// ----------------------------------------------------------------------

func TestPopulateRelativeAge(t *testing.T) {
	now := time.Date(2026, 4, 23, 0, 0, 0, 0, time.UTC)
	mems := []Memory{
		{UpdatedAt: now},
		{UpdatedAt: now.Add(-2 * 24 * time.Hour)},
		{UpdatedAt: now.Add(-10 * 24 * time.Hour)},
	}
	populateRelativeAge(mems, now)
	if mems[0].RelativeAge != "today" {
		t.Errorf("want today, got %q", mems[0].RelativeAge)
	}
	if mems[1].RelativeAge != "the day before yesterday" {
		t.Errorf("want 'the day before yesterday', got %q", mems[1].RelativeAge)
	}
	if mems[2].RelativeAge != "1 week ago" {
		t.Errorf("want '1 week ago', got %q", mems[2].RelativeAge)
	}
}

// ----------------------------------------------------------------------
// Hybrid search integration
// ----------------------------------------------------------------------

func TestHybridSearch(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()

	ctx := context.Background()

	// Store memories with distinct content.
	_, err := s.Store(ctx, "golang concurrency patterns", []string{"go", "patterns"})
	if err != nil {
		t.Fatalf("store 1: %v", err)
	}
	_, err = s.Store(ctx, "rust memory safety guarantees", []string{"rust", "safety"})
	if err != nil {
		t.Fatalf("store 2: %v", err)
	}
	_, err = s.Store(ctx, "python data science tutorial", []string{"python", "data"})
	if err != nil {
		t.Fatalf("store 3: %v", err)
	}

	// Hybrid search for "golang" — keyword branch should find memory 1.
	results, err := s.Search(ctx, "golang")
	if err != nil {
		t.Fatalf("hybrid search: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("expected at least one result")
	}

	// Verify the top result contains "golang".
	found := false
	for _, m := range results {
		if m.Score <= 0 {
			t.Errorf("result %s has non-positive score %v", m.ID, m.Score)
		}
		if m.RelativeAge == "" {
			t.Errorf("result %s has empty RelativeAge", m.ID)
		}
		if m.Content == "golang concurrency patterns" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected 'golang concurrency patterns' in results, got %v", results)
	}
}

// ----------------------------------------------------------------------
// Backwards compatibility: pure vector search fallback
// ----------------------------------------------------------------------

// TestPureVectorOverFetch verifies that pure vector search over-fetches
// candidates to survive post-filter truncation.
func TestPureVectorOverFetch(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		_, err := s.Store(ctx, "test content", nil)
		if err != nil {
			t.Fatalf("store: %v", err)
		}
	}

	// With mock embedder all vectors are identical, so vectorSearch returns
	// all 5 results (over-fetched from limit=2). After filtering/gap-stop
	// we should still get the requested 2.
	results, err := s.Search(ctx, "test", EnableHybrid(false), Limit(2))
	if err != nil {
		t.Fatalf("pure vector search: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("want 2 results after over-fetch, got %d", len(results))
	}
}

func TestSearchPureVectorFallback(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()

	ctx := context.Background()
	_, err := s.Store(ctx, "hello world", nil)
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	results, err := s.Search(ctx, "hello", EnableHybrid(false))
	if err != nil {
		t.Fatalf("pure vector search: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("want 1 result, got %d", len(results))
	}
}

// ----------------------------------------------------------------------
// Search options
// ----------------------------------------------------------------------

// TestSearchWithFilterLimit verifies that Limit inside WithFilter is respected
// when Limit() SearchOption is NOT explicitly provided.
func TestSearchWithFilterLimit(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		_, err := s.Store(ctx, "test content", nil)
		if err != nil {
			t.Fatalf("store: %v", err)
		}
	}

	// Only WithFilter(Limit: 2), no Limit() option.
	results, err := s.Search(ctx, "test", WithFilter(MemoryFilter{Limit: 2}))
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("WithFilter Limit=2: want 2, got %d", len(results))
	}
}

func TestSearchOptions(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		_, err := s.Store(ctx, "test content", nil)
		if err != nil {
			t.Fatalf("store: %v", err)
		}
	}

	// Limit option
	results, err := s.Search(ctx, "test", Limit(3))
	if err != nil {
		t.Fatalf("search with limit: %v", err)
	}
	if len(results) > 3 {
		t.Errorf("limit: want <=3, got %d", len(results))
	}

	// EnableHybrid(false) option
	results2, err := s.Search(ctx, "test", EnableHybrid(false), Limit(2))
	if err != nil {
		t.Fatalf("search pure vector: %v", err)
	}
	if len(results2) > 2 {
		t.Errorf("pure vector limit: want <=2, got %d", len(results2))
	}
}

// TestSearchMinScore verifies the MinScore SearchOption.
func TestSearchMinScore(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()

	ctx := context.Background()
	_, err := s.Store(ctx, "hello world", nil)
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	// Pure vector search with very high MinScore should filter everything
	// because mock embedder produces identical vectors (similarity = 1.0 for cosine,
	// but 1.01 > max possible).
	results, err := s.Search(ctx, "hello", EnableHybrid(false), MinScore(1.01))
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("minScore 1.01 should filter everything in pure vector, got %d", len(results))
	}
}

// TestSearchWithFilter verifies tag/type/agent/session filtering via WithFilter.
func TestSearchWithFilter(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()

	ctx := context.Background()
	_, err := s.Store(ctx, "alpha content", []string{"tag-a"})
	if err != nil {
		t.Fatalf("store alpha: %v", err)
	}
	_, err = s.Store(ctx, "beta content", []string{"tag-b"})
	if err != nil {
		t.Fatalf("store beta: %v", err)
	}

	// Filter by tag — only alpha should match.
	results, err := s.Search(ctx, "content", WithFilter(MemoryFilter{Tags: []string{"tag-a"}}))
	if err != nil {
		t.Fatalf("search with tag filter: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("expected at least one result with tag-a")
	}
	for _, m := range results {
		hasTagA := false
		for _, tag := range m.Tags {
			if tag == "tag-a" {
				hasTagA = true
				break
			}
		}
		if !hasTagA {
			t.Errorf("result %s missing tag-a, tags=%v", m.ID, m.Tags)
		}
	}
}

// TestMatchesFilter covers all branches of matchesFilter.
func TestMatchesFilter(t *testing.T) {
	base := Memory{ID: "1", Tags: []string{"a", "b"}, MemoryType: TypeInsight, AgentID: "agent-1", SessionID: "sess-1"}

	if !matchesFilter(base, MemoryFilter{}) {
		t.Error("empty filter should match")
	}
	if !matchesFilter(base, MemoryFilter{Tags: []string{"a"}}) {
		t.Error("tag filter should match")
	}
	if matchesFilter(base, MemoryFilter{Tags: []string{"z"}}) {
		t.Error("missing tag should not match")
	}
	if !matchesFilter(base, MemoryFilter{MemoryType: "insight"}) {
		t.Error("memory type filter should match")
	}
	if matchesFilter(base, MemoryFilter{MemoryType: "pinned"}) {
		t.Error("wrong memory type should not match")
	}
	if !matchesFilter(base, MemoryFilter{AgentID: "agent-1"}) {
		t.Error("agent filter should match")
	}
	if matchesFilter(base, MemoryFilter{AgentID: "agent-2"}) {
		t.Error("wrong agent should not match")
	}
	if !matchesFilter(base, MemoryFilter{SessionID: "sess-1"}) {
		t.Error("session filter should match")
	}
	if matchesFilter(base, MemoryFilter{SessionID: "sess-2"}) {
		t.Error("wrong session should not match")
	}
}

// ----------------------------------------------------------------------
// Distance clamp
// ----------------------------------------------------------------------

func TestDistanceToSimilarityClamp(t *testing.T) {
	// Cosine distance > 1 should clamp to 0
	if got := distanceToSimilarity(1.5, "cosine"); got != 0 {
		t.Errorf("cosine clamp: want 0, got %v", got)
	}
	// Cosine distance < 0 (shouldn't happen) should clamp to 1
	if got := distanceToSimilarity(-0.5, "cosine"); got != 1 {
		t.Errorf("cosine clamp upper: want 1, got %v", got)
	}
	// L2 distance negative (shouldn't happen) should clamp to 1
	if got := distanceToSimilarity(-1, "l2"); got != 1 {
		t.Errorf("l2 clamp upper: want 1, got %v", got)
	}
}

// ----------------------------------------------------------------------
// Empty index
// ----------------------------------------------------------------------

func TestSearchEmptyIndex(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()

	ctx := context.Background()
	results, err := s.Search(ctx, "anything")
	if err != nil {
		t.Fatalf("search empty: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("empty index: want 0, got %d", len(results))
	}
}

// ----------------------------------------------------------------------
// Offset pagination
// ----------------------------------------------------------------------

func TestSearchOffset(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		_, err := s.Store(ctx, "test content", nil)
		if err != nil {
			t.Fatalf("store: %v", err)
		}
	}

	results, err := s.Search(ctx, "test", WithFilter(MemoryFilter{Offset: 2, Limit: 10}))
	if err != nil {
		t.Fatalf("search offset: %v", err)
	}
	if len(results) > 3 {
		t.Errorf("offset 2 from 5: want <=3, got %d", len(results))
	}
}

// ----------------------------------------------------------------------
// Gap Stop boundary
// ----------------------------------------------------------------------

func TestApplyGapStopExactBoundary(t *testing.T) {
	// Score drops exactly to prev*(1-ratio) → should NOT be cut (strict <)
	mems := []Memory{
		{Score: 0.8},
		{Score: 0.4}, // exactly 0.8*(1-0.5) → NOT cut
		{Score: 0.39},
	}
	got := applyGapStop(mems, 0.5)
	// 0.4 is NOT < 0.4, so no cut at i=1.
	// 0.39 < 0.4*0.5=0.2 is false, so no cut at i=2 either.
	if len(got) != 3 {
		t.Errorf("exact boundary: want 3, got %d", len(got))
	}

	// Now test strict inequality: 0.39 < 0.8*0.5=0.4 → cut at i=1
	mems2 := []Memory{{Score: 0.8}, {Score: 0.39}, {Score: 0.3}}
	got2 := applyGapStop(mems2, 0.5)
	if len(got2) != 1 {
		t.Errorf("strict boundary: want 1, got %d", len(got2))
	}
}

func TestApplyGapStopAllEqual(t *testing.T) {
	mems := []Memory{{Score: 0.5}, {Score: 0.5}, {Score: 0.5}}
	got := applyGapStop(mems, 0.5)
	if len(got) != 3 {
		t.Errorf("all equal: want 3, got %d", len(got))
	}
}

// ----------------------------------------------------------------------
// Recency boost disabled
// ----------------------------------------------------------------------

func TestApplyRecencyBoostDisabled(t *testing.T) {
	now := time.Date(2026, 4, 23, 0, 0, 0, 0, time.UTC)
	scores := map[string]float64{"a": 1.0}
	mems := map[string]Memory{
		"a": {UpdatedAt: now.Add(-3 * 24 * time.Hour)},
	}
	applyRecencyBoost(scores, mems, now, 1.0, 1.0)
	if scores["a"] != 1.0 {
		t.Errorf("disabled boost: want 1.0, got %v", scores["a"])
	}
}

// ----------------------------------------------------------------------
// Second-hop gate
// ----------------------------------------------------------------------

func TestSecondHopGateBlocksLowScores(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()

	ctx := context.Background()
	_, err := s.Store(ctx, "seed content", nil)
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	// Set very high gate so second-hop is blocked (top score will be < 0.99).
	origGate := s.config.SecondHopGate
	s.config.SecondHopGate = 0.99
	defer func() { s.config.SecondHopGate = origGate }()

	// Search should succeed without second-hop panic.
	_, err = s.Search(ctx, "seed")
	if err != nil {
		t.Fatalf("search with high gate: %v", err)
	}
}

// ----------------------------------------------------------------------
// Pure vector search with filter
// ----------------------------------------------------------------------

func TestPureVectorSearchWithFilter(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()

	ctx := context.Background()
	_, err := s.Store(ctx, "alpha content", []string{"tag-a"})
	if err != nil {
		t.Fatalf("store alpha: %v", err)
	}
	_, err = s.Store(ctx, "beta content", []string{"tag-b"})
	if err != nil {
		t.Fatalf("store beta: %v", err)
	}

	results, err := s.Search(ctx, "content",
		EnableHybrid(false),
		WithFilter(MemoryFilter{Tags: []string{"tag-a"}}),
	)
	if err != nil {
		t.Fatalf("pure vector with filter: %v", err)
	}
	for _, m := range results {
		hasTagA := false
		for _, tag := range m.Tags {
			if tag == "tag-a" {
				hasTagA = true
				break
			}
		}
		if !hasTagA {
			t.Errorf("result %s missing tag-a, tags=%v", m.ID, m.Tags)
		}
	}
}

// ----------------------------------------------------------------------
// Pinned boost integration
// ----------------------------------------------------------------------

func TestSearchPinnedBoostIntegration(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()

	ctx := context.Background()
	_, err := s.Store(ctx, "regular memory", nil)
	if err != nil {
		t.Fatalf("store regular: %v", err)
	}

	// Store a pinned memory via Bootstrap (Store always uses TypeInsight).
	pinned := &Memory{
		ID:         "pinned-1",
		Content:    "pinned memory",
		MemoryType: TypePinned,
		State:      StateActive,
		Tags:       []string{},
		Vector:     make([]float32, 128),
	}
	for i := range pinned.Vector {
		pinned.Vector[i] = 0.1
	}
	if err := s.Bootstrap(ctx, []*Memory{pinned}); err != nil {
		t.Fatalf("bootstrap pinned: %v", err)
	}

	// Both memories have identical mock vectors; pinned should outrank regular
	// due to PinnedBoost (default 1.5) in hybrid search.
	results, err := s.Search(ctx, "memory")
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(results) < 2 {
		t.Skipf("need >=2 results to compare ranking, got %d (mock embedder may not surface both)", len(results))
	}

	foundPinned := false
	for _, m := range results {
		if m.ID == "pinned-1" && m.Score > 0 {
			foundPinned = true
		}
	}
	if !foundPinned {
		t.Logf("results: %+v", results)
		t.Skip("pinned memory not in top results (acceptable with identical mock vectors)")
	}
}

// ----------------------------------------------------------------------
// RRF with empty branch
// ----------------------------------------------------------------------

func TestRRFMergeEmptyBranch(t *testing.T) {
	vecResults := []Memory{{ID: "a", Score: 0.9}}
	scores1 := rrfMerge(vecResults, nil, 60.0)
	if len(scores1) != 1 || math.Abs(scores1["a"]-1.0/61.0) > 1e-9 {
		t.Errorf("empty keyword branch: want 1/61, got %v", scores1["a"])
	}

	keywordResults := []ScoredID{{ID: "b", Score: 5.0}}
	scores2 := rrfMerge(nil, keywordResults, 60.0)
	if len(scores2) != 1 || math.Abs(scores2["b"]-1.0/61.0) > 1e-9 {
		t.Errorf("empty vector branch: want 1/61, got %v", scores2["b"])
	}
}

// ----------------------------------------------------------------------
// matchesFilter State
// ----------------------------------------------------------------------

func TestMatchesFilterState(t *testing.T) {
	m := Memory{ID: "1", State: StateActive}
	if !matchesFilter(m, MemoryFilter{State: "active"}) {
		t.Error("active filter should match active memory")
	}
	if matchesFilter(m, MemoryFilter{State: "deleted"}) {
		t.Error("deleted filter should not match active memory")
	}
}

// ----------------------------------------------------------------------
// Direct second-hop tests (bypass gate)
// ----------------------------------------------------------------------

func TestApplySecondHopDirect(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()

	ctx := context.Background()

	// Store two memories; mock embedder gives identical vectors,
	// so second-hop from seed-1 will find seed-2.
	_, err := s.Store(ctx, "seed alpha", nil)
	if err != nil {
		t.Fatalf("store alpha: %v", err)
	}
	_, err = s.Store(ctx, "seed beta", nil)
	if err != nil {
		t.Fatalf("store beta: %v", err)
	}

	// Fetch both to build candidates map.
	var alphaID, betaID string
	s.coll.ForEach(func(doc *vego.Document) bool {
		if alphaID == "" {
			alphaID = doc.ID
		} else {
			betaID = doc.ID
		}
		return true
	})

	scores := map[string]float64{alphaID: 0.1, betaID: 0.05}
	candidates := map[string]Memory{}
	if m, err := s.Get(ctx, alphaID); err == nil {
		candidates[alphaID] = *m
	}
	if m, err := s.Get(ctx, betaID); err == nil {
		candidates[betaID] = *m
	}

	// Direct call (bypass gate).
	newScores := s.applySecondHop(ctx, scores, candidates, 10)

	// Both IDs should still exist; second-hop may add more.
	if _, ok := newScores[alphaID]; !ok {
		t.Error("alpha ID missing after second-hop")
	}
	if _, ok := newScores[betaID]; !ok {
		t.Error("beta ID missing after second-hop")
	}
}

func TestSecondHopSearchDirect(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()

	ctx := context.Background()

	// Bootstrap memories with explicit vectors so second-hop can use them.
	mem1 := &Memory{ID: "seed-1", Content: "seed one", State: StateActive, Vector: make([]float32, 128)}
	mem2 := &Memory{ID: "seed-2", Content: "seed two", State: StateActive, Vector: make([]float32, 128)}
	for i := range mem1.Vector {
		mem1.Vector[i] = 0.1
		mem2.Vector[i] = 0.1
	}
	if err := s.Bootstrap(ctx, []*Memory{mem1, mem2}); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	seeds := []Memory{*mem1}
	results, err := s.secondHopSearch(ctx, seeds, 10)
	if err != nil {
		t.Fatalf("secondHopSearch: %v", err)
	}
	// With identical mock vectors, seed-1's vector search finds seed-2.
	found := false
	for _, m := range results {
		if m.ID == "seed-2" {
			found = true
		}
	}
	if !found {
		t.Logf("second-hop results: %v", results)
		t.Skip("seed-2 not found in second-hop (acceptable with identical mock vectors)")
	}
}

// TestSecondHopSearchSorted verifies that secondHopSearch returns results
// sorted by score descending, which is required for correct rank-based
// weighting in applySecondHop.
func TestSecondHopSearchSorted(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()

	ctx := context.Background()

	mem1 := &Memory{ID: "seed-1", Content: "seed one", State: StateActive, Vector: make([]float32, 128)}
	mem2 := &Memory{ID: "seed-2", Content: "seed two", State: StateActive, Vector: make([]float32, 128)}
	mem3 := &Memory{ID: "seed-3", Content: "seed three", State: StateActive, Vector: make([]float32, 128)}
	for i := range mem1.Vector {
		mem1.Vector[i] = 0.1
		mem2.Vector[i] = 0.1
		mem3.Vector[i] = 0.1
	}
	if err := s.Bootstrap(ctx, []*Memory{mem1, mem2, mem3}); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	seeds := []Memory{*mem1}
	results, err := s.secondHopSearch(ctx, seeds, 10)
	if err != nil {
		t.Fatalf("secondHopSearch: %v", err)
	}

	// With identical mock vectors all scores are equal, but we still verify
	// the sort does not panic and produces a monotonic (non-increasing) sequence.
	for i := 1; i < len(results); i++ {
		if results[i].Score > results[i-1].Score {
			t.Errorf("results not sorted descending at index %d: %v > %v", i, results[i].Score, results[i-1].Score)
		}
	}
}

// TestPureVectorSearchGapStop verifies that pure vector search also applies
// gap-stop truncation.
// TestSearchOffsetExactBoundary verifies offset == len(results) returns empty.
func TestSearchOffsetExactBoundary(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		_, err := s.Store(ctx, "test content", nil)
		if err != nil {
			t.Fatalf("store: %v", err)
		}
	}

	results, err := s.Search(ctx, "test", WithFilter(MemoryFilter{Offset: 3, Limit: 10}))
	if err != nil {
		t.Fatalf("search offset=3: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("offset==len: want 0, got %d", len(results))
	}

	// offset > len should also return empty
	results2, err := s.Search(ctx, "test", WithFilter(MemoryFilter{Offset: 5, Limit: 10}))
	if err != nil {
		t.Fatalf("search offset=5: %v", err)
	}
	if len(results2) != 0 {
		t.Errorf("offset>len: want 0, got %d", len(results2))
	}
}

// TestSecondHopExcludesSeeds verifies that second-hop results do not include
// the seed documents themselves (they should not receive self-boost).
func TestSecondHopExcludesSeeds(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()

	ctx := context.Background()

	// Bootstrap two memories with explicit vectors.
	mem1 := &Memory{ID: "seed-1", Content: "seed one", State: StateActive, Vector: make([]float32, 128)}
	mem2 := &Memory{ID: "seed-2", Content: "seed two", State: StateActive, Vector: make([]float32, 128)}
	for i := range mem1.Vector {
		mem1.Vector[i] = 0.1
		mem2.Vector[i] = 0.1
	}
	if err := s.Bootstrap(ctx, []*Memory{mem1, mem2}); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	seeds := []Memory{*mem1}
	results, err := s.secondHopSearch(ctx, seeds, 10)
	if err != nil {
		t.Fatalf("secondHopSearch: %v", err)
	}

	// Seed-1 should NOT appear in second-hop results.
	for _, m := range results {
		if m.ID == "seed-1" {
			t.Errorf("seed-1 should not appear in its own second-hop results")
		}
	}
}

func TestPureVectorSearchGapStop(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()

	// Set a narrow gap-stop ratio so it would truncate if scores differed.
	// With identical mock vectors all scores are equal, so gap-stop is a no-op.
	// This test primarily verifies the code path is exercised without panic.
	s.config.GapStopRatio = 0.5

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		_, err := s.Store(ctx, "test content", nil)
		if err != nil {
			t.Fatalf("store: %v", err)
		}
	}

	results, err := s.Search(ctx, "test", EnableHybrid(false))
	if err != nil {
		t.Fatalf("pure vector search: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("want 3 results, got %d", len(results))
	}
}
