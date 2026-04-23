package memory

import (
	"context"
	"testing"
	"time"
)

// Probe 1: pureVectorSearch on empty index — should NOT return error
func TestProbe_PureVectorEmptyIndex(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()

	ctx := context.Background()
	results, err := s.Search(ctx, "anything", EnableHybrid(false))
	if err != nil {
		t.Fatalf("pure vector search on empty index should not error, got: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("want 0, got %d", len(results))
	}
}

// Probe 2: matchesFilter with empty (non-nil) Tags slice
func TestProbe_MatchesFilterEmptyTags(t *testing.T) {
	m := Memory{ID: "1", Tags: []string{"a"}}
	// nil Tags → no tag filter → matches
	if !matchesFilter(m, MemoryFilter{Tags: nil}) {
		t.Error("nil Tags should match")
	}
	// empty non-nil Tags → unified with nil semantics → no tag filter → matches
	if !matchesFilter(m, MemoryFilter{Tags: []string{}}) {
		t.Error("empty non-nil Tags should match (unified with nil semantics)")
	}
}

// Probe 3: GapStop with first item having score=0
func TestProbe_GapStopZeroScores(t *testing.T) {
	mems := []Memory{{Score: 0}, {Score: 0}, {Score: 0}}
	got := applyGapStop(mems, 0.5)
	// 0 < 0*(1-0.5) = 0 is false, so no truncation
	if len(got) != 3 {
		t.Errorf("zero scores: want 3, got %d", len(got))
	}
}

// Probe 4: Negative ratio in GapStop
func TestProbe_GapStopNegativeRatio(t *testing.T) {
	mems := []Memory{{Score: 0.9}, {Score: 0.1}}
	got := applyGapStop(mems, -0.5)
	// ratio <= 0 → disabled
	if len(got) != 2 {
		t.Errorf("negative ratio: want 2, got %d", len(got))
	}
}

// Probe 5: RRF with duplicate IDs in same list
func TestProbe_RRFDuplicateIDs(t *testing.T) {
	vecResults := []Memory{{ID: "a"}, {ID: "a"}} // same ID twice
	scores := rrfMerge(vecResults, nil, 60.0)
	// "a" appears at rank 1 and rank 2 → scores accumulate
	want := 1.0/61.0 + 1.0/62.0
	got := scores["a"]
	diff := got - want
	if diff < 0 {
		diff = -diff
	}
	if diff > 1e-12 {
		t.Errorf("duplicate RRF: want %v, got %v", want, got)
	}
}

// Probe 6: Recency boost with future timestamp (negative age)
func TestProbe_RecencyBoostFutureTimestamp(t *testing.T) {
	now := time.Date(2026, 4, 23, 12, 0, 0, 0, time.UTC)
	scores := map[string]float64{"a": 1.0}
	mems := map[string]Memory{
		"a": {UpdatedAt: now.Add(24 * time.Hour)}, // 1 day in future
	}
	applyRecencyBoost(scores, mems, now, 1.05, 1.02)
	// negative age → age <= 7*24h is true → weekBoost applied
	if scores["a"] != 1.05 {
		t.Errorf("future timestamp: want 1.05, got %v", scores["a"])
	}
}
