package memory

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	vego "github.com/wzqhbustb/vego/vego"
)

// ----------------------------------------------------------------------
// Distance → Similarity conversion
// ----------------------------------------------------------------------

// distanceToSimilarity converts Vego distance to a 0-1 similarity score.
// Larger return value means more relevant.  Result is clamped to [0,1].
func distanceToSimilarity(distance float32, distFunc string) float64 {
	d := float64(distance)
	var sim float64
	switch distFunc {
	case "cosine":
		// Vego CosineDistance returns 1-cos(a,b), range [0, 2]
		sim = 1.0 - d
	case "l2":
		// L2 range [0, +∞); map to (0, 1]
		sim = 1.0 / (1.0 + d)
	case "ip":
		// InnerProduct distance = -dot(a,b); for normalized vectors range [-1, 1]
		sim = (1.0 + (-d)) / 2.0
	default:
		sim = 1.0 / (1.0 + d)
	}
	if sim < 0 {
		return 0
	}
	if sim > 1 {
		return 1
	}
	return sim
}

// ----------------------------------------------------------------------
// Hybrid search (10-stage pipeline)
// ----------------------------------------------------------------------

// hybridSearch performs the full hybrid search pipeline.
//
// Pipeline:
//  1. Temporal query normalization
//  2. Vector search (HNSW + metadata filter)
//  3. Distance → similarity conversion
//  4. Similarity filtering (MinScore)
//  5. Keyword search (BM25 inverted index)
//  6. RRF fusion (first hop)
//  7. Second-hop expansion (optional)
//  8. Pinned boost
//  9. Recency boost
//  10. Sort + Gap Stop + Pagination + Relative age + Temporal projection
func (s *MemoryStore) hybridSearch(ctx context.Context, query string, filter MemoryFilter) ([]Memory, error) {
	if query == "" {
		return nil, nil
	}
	now := time.Now()

	// Resolve defaults from config.
	limit := filter.Limit
	if !filter.LimitSet {
		limit = s.config.SearchLimit
	}
	minScore := filter.MinScore
	if !filter.MinScoreSet {
		minScore = s.config.MinScore
	}

	// Stage 1: Temporal normalization.
	normalizedQuery := NormalizeTemporalRecallQuery(query, now)

	// Stage 2-4: Vector search → distance conversion → similarity filter.
	vec, err := s.embed(ctx, normalizedQuery)
	if err != nil {
		return nil, fmt.Errorf("embed query: %w", err)
	}
	vecResults, err := s.vectorSearch(ctx, vec, limit, minScore)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		// Log and continue with keyword-only results so a transient
		// HNSW issue does not turn into a silent partial-failure.
		slog.WarnContext(ctx, "vector search failed, continuing with keyword-only results", "err", err)
		vecResults = nil
	}

	// Stage 5: Keyword search.
	keywordResults, err := s.inverted.SearchContext(ctx, normalizedQuery, limit*3)
	if err != nil {
		return nil, fmt.Errorf("keyword search: %w", err)
	}

	// Stage 6: First-hop RRF fusion.
	scores := rrfMerge(vecResults, keywordResults, s.config.RRFK)

	// Collect candidate memories.  Vector results already have Memory objects;
	// keyword-only results need to be fetched.
	candidates := make(map[string]Memory, len(scores))
	for _, m := range vecResults {
		candidates[m.ID] = m
	}
	for _, si := range keywordResults {
		if _, ok := candidates[si.ID]; ok {
			continue
		}
		m, err := s.getWithoutVector(ctx, si.ID)
		if err != nil {
			slog.Warn("skip vanished keyword result", "id", si.ID, "err", err)
			continue
		}
		// Defensive: inverted index should only contain active memories, but a
		// narrow race window exists between inverted.Search and s.Get.
		if m.State != StateActive {
			slog.Warn("skip non-active keyword result", "id", si.ID, "state", m.State)
			continue
		}
		m.Score = si.Score
		candidates[si.ID] = *m
	}

	// Stage 7: Second-hop expansion (gated by SecondHopGate).
	if s.config.SecondHopWeight > 0 && len(scores) > 0 {
		// Compute top score for gate check.
		topScore := 0.0
		for _, sc := range scores {
			if sc > topScore {
				topScore = sc
			}
		}
		if topScore >= s.config.SecondHopGate {
			scores = s.applySecondHop(ctx, scores, candidates, limit)
		}
	}

	// Stage 8: Pinned boost.
	if s.config.PinnedBoost != 1.0 {
		applyPinnedBoost(scores, candidates, s.config.PinnedBoost)
	}

	// Stage 9: Recency boost.
	if s.config.RecencyBoostWeek != 1.0 || s.config.RecencyBoostMonth != 1.0 {
		applyRecencyBoost(scores, candidates, now, s.config.RecencyBoostWeek, s.config.RecencyBoostMonth)
	}

	// Assemble, filter, sort, and apply post-processing.
	results := make([]Memory, 0, len(scores))
	for id, score := range scores {
		if m, ok := candidates[id]; ok && matchesFilter(m, filter) {
			m.Score = score
			results = append(results, m)
		}
	}

	sort.Slice(results, func(i, j int) bool {
		if results[i].Score != results[j].Score {
			return results[i].Score > results[j].Score
		}
		return results[i].UpdatedAt.After(results[j].UpdatedAt)
	})

	// Stage 10: Gap Stop + Pagination + Relative age + Projection.
	results = applyGapStop(results, s.config.GapStopRatio)

	if filter.Offset > 0 {
		if filter.Offset >= len(results) {
			results = results[:0]
		} else {
			results = results[filter.Offset:]
		}
	}
	if limit > 0 && limit < len(results) {
		results = results[:limit]
	}

	populateRelativeAge(results, now)

	for i := range results {
		results[i].Content = TemporalRecallProjection(results[i].Content, results[i].Metadata, now)
	}

	return results, nil
}

// ----------------------------------------------------------------------
// Stage 2-4: Vector search with distance conversion and similarity filter
// ----------------------------------------------------------------------

func (s *MemoryStore) vectorSearch(ctx context.Context, queryVec []float32, limit int, minScore float64) ([]Memory, error) {
	var vf vego.Filter = &vego.MetadataFilter{
		Field:    metaKeyState,
		Operator: "eq",
		Value:    string(StateActive),
	}

	results, err := s.coll.SearchWithFilterContext(ctx, queryVec, limit, vf, vego.WithOverFetch(s.config.SearchOverFetch))
	if err != nil {
		return nil, fmt.Errorf("vector search: %w", err)
	}

	out := make([]Memory, 0, len(results))
	for _, r := range results {
		m, err := docToMemory(r.Document)
		if err != nil {
			slog.Warn("skip corrupt document in vector search", "id", r.Document.ID, "err", err)
			continue
		}
		sim := distanceToSimilarity(r.Distance, s.config.DistanceFunc)
		if sim < minScore {
			continue
		}
		m.Score = sim
		out = append(out, *m)
	}
	return out, nil
}

// ----------------------------------------------------------------------
// Stage 6: RRF fusion
// ----------------------------------------------------------------------

// rrfMerge fuses vector and keyword rankings using Reciprocal Rank Fusion.
// For each result list the score contribution is 1/(K + rank).
func rrfMerge(vecResults []Memory, keywordResults []ScoredID, k float64) map[string]float64 {
	scores := make(map[string]float64)
	for rank, m := range vecResults {
		scores[m.ID] += 1.0 / (k + float64(rank+1))
	}
	for rank, si := range keywordResults {
		scores[si.ID] += 1.0 / (k + float64(rank+1))
	}
	return scores
}

// ----------------------------------------------------------------------
// Stage 7: Second-hop expansion
// ----------------------------------------------------------------------

// applySecondHop performs second-hop expansion from the top-N seeds.
// It returns a new scores map with second-hop contributions merged in.
func (s *MemoryStore) applySecondHop(ctx context.Context, scores map[string]float64, candidates map[string]Memory, limit int) map[string]float64 {
	// Pick top seeds by current score.
	type scored struct {
		id    string
		score float64
	}
	seedList := make([]scored, 0, len(scores))
	for id, sc := range scores {
		seedList = append(seedList, scored{id, sc})
	}
	sort.Slice(seedList, func(i, j int) bool {
		return seedList[i].score > seedList[j].score
	})

	topN := s.config.SecondHopTopN
	if topN > len(seedList) {
		topN = len(seedList)
	}

	var seeds []Memory
	for i := 0; i < topN; i++ {
		if m, ok := candidates[seedList[i].id]; ok {
			seeds = append(seeds, m)
		}
	}
	if len(seeds) == 0 {
		return scores
	}

	hopResults, err := s.secondHopSearch(ctx, seeds, limit)
	if err != nil {
		slog.WarnContext(ctx, "second-hop search failed", "err", err)
		return scores
	}

	// Merge with reduced weight.
	for rank, m := range hopResults {
		scores[m.ID] += s.config.SecondHopWeight / (s.config.RRFK + float64(rank+1))
		if _, ok := candidates[m.ID]; !ok {
			candidates[m.ID] = m
		}
	}
	return scores
}

// secondHopSearch expands from seed memories using their stored embedding vectors.
func (s *MemoryStore) secondHopSearch(ctx context.Context, seeds []Memory, limit int) ([]Memory, error) {
	var vf vego.Filter = &vego.MetadataFilter{
		Field:    metaKeyState,
		Operator: "eq",
		Value:    string(StateActive),
	}

	// Pre-populate seen with seed IDs so seeds don't receive second-hop
	// self-boost (the design intent is to discover *related* documents).
	seen := make(map[string]struct{}, len(seeds))
	for _, seed := range seeds {
		seen[seed.ID] = struct{}{}
	}

	// Launch parallel searches — each seed is independent.
	type seedResult struct {
		mems []Memory
	}
	results := make([]seedResult, len(seeds))
	var wg sync.WaitGroup

	for i, seed := range seeds {
		wg.Add(1)
		go func(idx int, seedID string) {
			defer wg.Done()

			// Check context cancellation.
			if err := ctx.Err(); err != nil {
				return
			}

			// Fetch the seed's vector from storage (Memory.Vector is transient).
			doc, err := s.coll.GetContext(ctx, seedID)
			if err != nil {
				return
			}
			if len(doc.Vector) == 0 {
				return
			}

			vecResults, err := s.coll.SearchWithFilterContext(ctx, doc.Vector, limit, vf, vego.WithOverFetch(s.config.SearchOverFetch))
			if err != nil {
				return
			}

			var local []Memory
			for _, r := range vecResults {
				m, err := docToMemory(r.Document)
				if err != nil {
					continue
				}
				sim := distanceToSimilarity(r.Distance, s.config.DistanceFunc)
				m.Score = sim
				local = append(local, *m)
			}
			results[idx].mems = local
		}(i, seed.ID)
	}
	wg.Wait()

	// Merge and deduplicate across all seed results.
	var all []Memory
	for _, sr := range results {
		for _, m := range sr.mems {
			if _, ok := seen[m.ID]; ok {
				continue
			}
			seen[m.ID] = struct{}{}
			all = append(all, m)
		}
	}

	// Sort by similarity descending so that applySecondHop's rank-based
	// weighting is meaningful.
	sort.Slice(all, func(i, j int) bool {
		return all[i].Score > all[j].Score
	})

	return all, nil
}

// ----------------------------------------------------------------------
// Stage 8-9: Boosts
// ----------------------------------------------------------------------

func applyPinnedBoost(scores map[string]float64, memories map[string]Memory, boost float64) {
	for id, m := range memories {
		if m.MemoryType == TypePinned {
			scores[id] *= boost
		}
	}
}

func applyRecencyBoost(scores map[string]float64, memories map[string]Memory, now time.Time, weekBoost, monthBoost float64) {
	for id, m := range memories {
		age := now.Sub(m.UpdatedAt)
		switch {
		case age <= 7*24*time.Hour:
			scores[id] *= weekBoost
		case age <= 30*24*time.Hour:
			scores[id] *= monthBoost
		}
	}
}

// ----------------------------------------------------------------------
// Stage 10: Gap Stop + Relative age
// ----------------------------------------------------------------------

// applyGapStop truncates results when the score drops sharply between
// adjacent items.  ratio=0 disables the feature.
func applyGapStop(sorted []Memory, ratio float64) []Memory {
	if ratio <= 0 || len(sorted) <= 1 {
		return sorted
	}
	for i := 1; i < len(sorted); i++ {
		if sorted[i].Score < sorted[i-1].Score*(1-ratio) {
			return sorted[:i]
		}
	}
	return sorted
}

// populateRelativeAge fills Memory.RelativeAge for each result.
func populateRelativeAge(memories []Memory, now time.Time) {
	for i := range memories {
		memories[i].RelativeAge = humanRelative(memories[i].UpdatedAt, now)
	}
}

// matchesFilter checks whether a memory satisfies the optional filter criteria.
func matchesFilter(m Memory, filter MemoryFilter) bool {
	if filter.State != "" && string(m.State) != filter.State {
		return false
	}
	if len(filter.Tags) > 0 {
		hasTag := false
		for _, ft := range filter.Tags {
			for _, mt := range m.Tags {
				if mt == ft {
					hasTag = true
					break
				}
			}
			if hasTag {
				break
			}
		}
		if !hasTag {
			return false
		}
	}
	if filter.MemoryType != "" && string(m.MemoryType) != filter.MemoryType {
		return false
	}
	if filter.AgentID != "" && m.AgentID != filter.AgentID {
		return false
	}
	if filter.SessionID != "" && m.SessionID != filter.SessionID {
		return false
	}
	return true
}
