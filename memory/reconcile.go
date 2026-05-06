package memory

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	vego "github.com/wzqhbustb/vego/vego"
	"golang.org/x/sync/semaphore"
)

// ----------------------------------------------------------------------
// Reconcile
// ----------------------------------------------------------------------

// Reconcile compares extracted facts against existing memories and performs
// ADD / UPDATE / DELETE / NOOP operations.
//
// Search phase runs concurrently (up to 4 workers) while decision+execution
// remains sequential so that each fact sees the cumulative effect of prior
// actions.
func (s *MemoryStore) Reconcile(ctx context.Context, agentID string, facts []ExtractedFact) (*IngestResult, error) {
	if ctx == nil {
		return nil, fmt.Errorf("context must not be nil")
	}
	if len(facts) == 0 {
		return &IngestResult{}, nil
	}

	// 1. Filter out query-intent facts.
	facts = filterQueryIntent(facts)
	if len(facts) == 0 {
		return &IngestResult{}, nil
	}

	// 2. Apply max facts limit.
	if s.config.MaxFacts > 0 && len(facts) > s.config.MaxFacts {
		facts = facts[:s.config.MaxFacts]
	}

	result := &IngestResult{}

	// ------------------------------------------------------------------
	// Phase 0: batch embed all facts to amortize network RTT.
	// If batch embed fails, fallback to per-fact embed in findCandidates.
	// ------------------------------------------------------------------
	texts := make([]string, len(facts))
	for i, f := range facts {
		texts[i] = f.Content
	}
	factVecs, embedErr := s.embedBatch(ctx, texts)
	if embedErr != nil {
		s.logger.WarnContext(ctx, "batch embed failed, falling back to per-fact embed", "error", embedErr)
		factVecs = make([][]float32, len(facts)) // all nil → findCandidates will embed individually
	}

	searchSem := semaphore.NewWeighted(4) // concurrent search limit

	// ------------------------------------------------------------------
	// Phase 1: concurrent candidate search
	// ------------------------------------------------------------------
	type work struct {
		candidates []*candidateMapping
		topSim     float64 // highest vector similarity from findCandidates
		err        error
	}
	works := make([]work, len(facts))
	var searchWg sync.WaitGroup

	for i := range facts {
		if err := ctx.Err(); err != nil {
			return result, err
		}

		searchWg.Add(1)
		go func(idx int) {
			defer searchWg.Done()
			defer func() {
				if r := recover(); r != nil {
					s.logger.Error("search goroutine panic", "idx", idx, "fact", facts[idx].Content, "recover", r)
					works[idx].err = fmt.Errorf("panic in search goroutine: %v", r)
				}
			}()
			if err := searchSem.Acquire(ctx, 1); err != nil {
				works[idx].err = err
				return
			}
			defer searchSem.Release(1)
			works[idx].candidates, works[idx].topSim, works[idx].err = s.findCandidates(ctx, &facts[idx], factVecs[idx])
		}(i)
	}
	searchWg.Wait()

	// ------------------------------------------------------------------
	// Phase 2: sequential decision + execution
	// ------------------------------------------------------------------
	for i := range facts {
		if err := ctx.Err(); err != nil {
			return result, err
		}

		if works[i].err != nil {
			s.logger.WarnContext(ctx, "candidate search failed", "error", works[i].err)
			works[i].candidates = nil // fallback: treat as no candidates → ADD
		}

		// Near-duplicate suppression: if the top candidate is extremely similar,
		// skip LLM decision and treat as NOOP to save LLM call cost.
		if s.config.NearDupThreshold > 0 && works[i].topSim >= s.config.NearDupThreshold {
			s.logger.InfoContext(ctx, "near-duplicate suppressed", "fact", facts[i].Content, "top_sim", works[i].topSim, "threshold", s.config.NearDupThreshold)
			result.NearDupSkipped++
			continue
		}

		action, targetID, err := s.decideAction(ctx, &facts[i], works[i].candidates)
		if err != nil {
			s.logger.WarnContext(ctx, "action decision failed", "error", err)
			action, targetID = "ADD", "" // fallback
		}

		if err := s.executeAction(ctx, agentID, &facts[i], action, targetID, result); err != nil {
			s.logger.WarnContext(ctx, "action execution failed", "action", action, "error", err)
			result.Skipped++
		}
	}

	return result, nil
}

func filterQueryIntent(facts []ExtractedFact) []ExtractedFact {
	filtered := make([]ExtractedFact, 0, len(facts))
	for _, f := range facts {
		if !f.QueryIntent {
			filtered = append(filtered, f)
		}
	}
	return filtered
}

// ----------------------------------------------------------------------
// Candidate search
// ----------------------------------------------------------------------

type candidateMapping struct {
	intID    int
	memoryID string
	memory   *Memory
}

var activeFilter = &vego.MetadataFilter{
	Field:    "_state",
	Operator: "eq",
	Value:    string(StateActive),
}

// testReconcileSearchPanicHook is set by tests to exercise the panic-recovery
// path in the Reconcile search goroutine. If set, findCandidates calls it
// before any real work; if it panics, the defer searchSem.Release(1) (registered
// before the findCandidates call) must still execute to avoid a semaphore leak.
var testReconcileSearchPanicHook func()

func (s *MemoryStore) findCandidates(ctx context.Context, fact *ExtractedFact, vec []float32) ([]*candidateMapping, float64, error) {
	if testReconcileSearchPanicHook != nil {
		testReconcileSearchPanicHook()
	}
	// 1. Vector search (vec may be nil — caller embeds in batch).
	if vec == nil {
		var err error
		vec, err = s.embed(ctx, fact.Content)
		if err != nil {
			return nil, 0, fmt.Errorf("embed: %w", err)
		}
	}

	vecResults, err := s.coll.SearchWithFilterContext(ctx, vec, s.config.SearchLimit, activeFilter, vego.WithOverFetch(s.config.SearchOverFetch))
	if err != nil {
		return nil, 0, fmt.Errorf("vector search: %w", err)
	}

	// Track the highest raw vector similarity for near-duplicate suppression.
	topSim := 0.0
	for _, r := range vecResults {
		sim := distanceToSimilarity(r.Distance, s.config.DistanceFunc)
		if sim > topSim {
			topSim = sim
		}
	}

	// 2. Keyword search.
	keywordResults, err := s.inverted.SearchContext(ctx, fact.Content, s.config.SearchLimit)
	if err != nil {
		return nil, 0, fmt.Errorf("keyword search: %w", err)
	}

	// 3. Merge & deduplicate.
	seen := make(map[string]struct{})
	var candidates []*candidateMapping
	intID := 1

	addResult := func(id string) {
		if _, ok := seen[id]; ok {
			return
		}
		seen[id] = struct{}{}
		m, err := s.Get(ctx, id)
		if err != nil {
			s.logger.WarnContext(ctx, "skip vanished candidate", "id", id, "err", err)
			return
		}
		if m.State != StateActive {
			return // defensive: only active memories are valid candidates
		}
		candidates = append(candidates, &candidateMapping{
			intID:    intID,
			memoryID: id,
			memory:   m,
		})
		intID++
	}

	for _, r := range vecResults {
		if r.Document != nil {
			addResult(r.Document.ID)
		}
	}
	for _, sid := range keywordResults {
		addResult(sid.ID)
	}

	return candidates, topSim, nil
}

// ----------------------------------------------------------------------
// Action decision
// ----------------------------------------------------------------------

func (s *MemoryStore) decideAction(ctx context.Context, fact *ExtractedFact, candidates []*candidateMapping) (string, string, error) {
	if len(candidates) == 0 {
		return "ADD", "", nil
	}

	if s.llm == nil {
		// No LLM: use heuristic — if any candidate content is identical, UPDATE.
		for _, c := range candidates {
			if strings.EqualFold(strings.TrimSpace(c.memory.Content), strings.TrimSpace(fact.Content)) {
				return "UPDATE", c.memoryID, nil
			}
		}
		return "ADD", "", nil
	}

	systemPrompt := buildReconcileSystemPrompt()
	userPrompt := buildReconcileUserPrompt(fact, candidates)

	// Serialize LLM calls to avoid API rate limits in concurrent Reconcile.
	// Currently Phase 2 is sequential, but this semaphore provides defense-
	// in-depth if Phase 2 is ever made concurrent.
	if err := s.llmSem.Acquire(ctx, 1); err != nil {
		return "", "", fmt.Errorf("acquire llm sem: %w", err)
	}
	defer s.llmSem.Release(1)

	resp, err := s.llm.CompleteJSON(ctx, systemPrompt, userPrompt)
	if err != nil {
		return "", "", fmt.Errorf("llm chat: %w", err)
	}

	var decision struct {
		Action   string `json:"action"`
		TargetID int    `json:"target_id"`
		Reason   string `json:"reason"`
	}
	if err := json.Unmarshal([]byte(resp), &decision); err != nil {
		return "", "", fmt.Errorf("parse llm response: %w", err)
	}

	action := strings.ToUpper(strings.TrimSpace(decision.Action))
	switch action {
	case "ADD", "UPDATE", "DELETE", "NOOP":
		// valid
	default:
		action = "ADD" // fallback
	}

	// Map integer ID back to memory ID.
	var targetMemoryID string
	for _, c := range candidates {
		if c.intID == decision.TargetID {
			targetMemoryID = c.memoryID
			break
		}
	}

	// If UPDATE/DELETE but target not found → ADD.
	if (action == "UPDATE" || action == "DELETE") && targetMemoryID == "" {
		action = "ADD"
	}

	return action, targetMemoryID, nil
}

func buildReconcileSystemPrompt() string {
	return "你是一个记忆管理助手。给定一条新事实和若干候选记忆，决定操作类型：" +
		"ADD（新增独立记忆）、UPDATE（用新事实完全替换旧记忆）、DELETE（删除错误记忆）、NOOP（无操作）。\n\n" +
		"关键规则（按优先级排序）：\n" +
		"1. UPDATE 意味着完全替换旧记忆的内容，旧记忆会被归档但内容不可再搜索。" +
		"如果新事实是旧记忆的补充或扩展（例如旧记忆说\"喜欢咖啡\"，新事实说\"也喜欢茶\"），" +
		"必须输出 ADD 而非 UPDATE，避免信息丢失。\n" +
		"2. 候选记忆与新事实内容几乎相同，仅细节有更新 → UPDATE。\n" +
		"3. 候选记忆与新事实直接矛盾（如偏好改变、状态迁移、事实更正）→ UPDATE。" +
		"保留历史链可追溯，不要 DELETE。\n" +
		"4. DELETE 仅用于完全错误且无用的信息（如事实提取错误、用户明确否定\"我从来没说过这个\"）。" +
		"信息过时或偏好改变不应 DELETE。Pinned 记忆不可 DELETE。\n" +
		"5. 候选记忆与新事实完全无关 → ADD。\n\n" +
		"请返回 JSON: {\"action\": \"ADD|UPDATE|DELETE|NOOP\", \"target_id\": 1, \"reason\": \"...\"}"
}

func buildReconcileUserPrompt(fact *ExtractedFact, candidates []*candidateMapping) string {
	var b strings.Builder
	b.WriteString("新事实: ")
	b.WriteString(fact.Content)
	if len(fact.Tags) > 0 {
		b.WriteString("\n标签: ")
		b.WriteString(strings.Join(fact.Tags, ", "))
	}
	b.WriteString("\n\n候选记忆:\n")
	for _, c := range candidates {
		b.WriteString("[")
		b.WriteString(strconv.Itoa(c.intID))
		b.WriteString("] ")
		b.WriteString(c.memory.Content)
		if c.memory.MemoryType != "" {
			b.WriteString(" \n类型: ")
			b.WriteString(string(c.memory.MemoryType))
		}
		if len(c.memory.Tags) > 0 {
			b.WriteString(" 标签: ")
			b.WriteString(strings.Join(c.memory.Tags, ", "))
		}
		b.WriteString("\n")
	}
	return b.String()
}

// ----------------------------------------------------------------------
// Action execution
// ----------------------------------------------------------------------

func (s *MemoryStore) executeAction(ctx context.Context, agentID string, fact *ExtractedFact, action, targetID string, result *IngestResult) error {
	switch action {
	case "ADD":
		if err := validateInput(fact.Content, fact.Tags, s.config); err != nil {
			return fmt.Errorf("validate ADD fact: %w", err)
		}
		mem := &Memory{
			ID:         vego.DocumentID(),
			Content:    fact.Content,
			MemoryType: TypeInsight,
			State:      StateActive,
			Tags:       append([]string(nil), fact.Tags...),
			AgentID:    agentID,
			Version:    1,
			Metadata:   copyMap(fact.Metadata),
			CreatedAt:  time.Now(),
			UpdatedAt:  time.Now(),
		}
		vec, err := s.embed(ctx, mem.Content)
		if err != nil {
			return fmt.Errorf("embed: %w", err)
		}
		doc, err := memoryToDoc(mem, vec)
		if err != nil {
			return fmt.Errorf("marshal: %w", err)
		}

		s.mu.Lock()
		defer s.mu.Unlock()

		if err := s.coll.InsertContext(ctx, doc); err != nil {
			return fmt.Errorf("insert: %w", err)
		}
		s.inverted.Add(mem.ID, mem.Content)
		result.Added++
		return nil

	case "UPDATE":
		if targetID == "" {
			return fmt.Errorf("UPDATE without targetID")
		}
		old, err := s.Get(ctx, targetID)
		if err != nil {
			return fmt.Errorf("get target: %w", err)
		}
		if old.MemoryType == TypePinned {
			// Downgrade to ADD.
			return s.executeAction(ctx, agentID, fact, "ADD", "", result)
		}
		if old.State != StateActive {
			// Target was archived/deleted by a concurrent reconcile; downgrade to ADD.
			return s.executeAction(ctx, agentID, fact, "ADD", "", result)
		}
		_, err = s.update(ctx, targetID, fact.Content, fact.Tags, fact.Metadata)
		if err != nil {
			return fmt.Errorf("update: %w", err)
		}
		result.Updated++
		return nil

	case "DELETE":
		if targetID == "" {
			return fmt.Errorf("DELETE without targetID")
		}
		old, err := s.Get(ctx, targetID)
		if err != nil {
			return fmt.Errorf("get target: %w", err)
		}
		if old.MemoryType == TypePinned {
			result.Skipped++
			return nil
		}
		if old.State != StateActive {
			// Already archived/deleted by a concurrent reconcile.
			result.Skipped++
			return nil
		}
		if err := s.Delete(ctx, targetID); err != nil {
			return fmt.Errorf("delete: %w", err)
		}
		result.Deleted++
		return nil

	case "NOOP":
		result.Skipped++
		return nil

	default:
		return fmt.Errorf("unknown action: %s", action)
	}
}
