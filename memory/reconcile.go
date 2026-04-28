package memory

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
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
	searchSem := semaphore.NewWeighted(4) // concurrent search limit

	// ------------------------------------------------------------------
	// Phase 1: concurrent candidate search
	// ------------------------------------------------------------------
	type work struct {
		candidates []*candidateMapping
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
			if err := searchSem.Acquire(ctx, 1); err != nil {
				works[idx].err = err
				return
			}
			works[idx].candidates, works[idx].err = s.findCandidates(ctx, &facts[idx])
			defer searchSem.Release(1)
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
			slog.WarnContext(ctx, "candidate search failed", "error", works[i].err)
			works[i].candidates = nil // fallback: treat as no candidates → ADD
		}

		action, targetID, err := s.decideAction(ctx, &facts[i], works[i].candidates)
		if err != nil {
			slog.WarnContext(ctx, "action decision failed", "error", err)
			action, targetID = "ADD", "" // fallback
		}

		if err := s.executeAction(ctx, agentID, &facts[i], action, targetID, result); err != nil {
			slog.WarnContext(ctx, "action execution failed", "action", action, "error", err)
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

func (s *MemoryStore) findCandidates(ctx context.Context, fact *ExtractedFact) ([]*candidateMapping, error) {
	// 1. Vector search.
	vec, err := s.embed(ctx, fact.Content)
	if err != nil {
		return nil, fmt.Errorf("embed: %w", err)
	}

	vecResults, err := s.coll.SearchWithFilterContext(ctx, vec, s.config.SearchLimit, activeFilter, vego.WithOverFetch(s.config.SearchOverFetch))
	if err != nil {
		return nil, fmt.Errorf("vector search: %w", err)
	}

	// 2. Keyword search.
	keywordResults, err := s.inverted.SearchContext(ctx, fact.Content, s.config.SearchLimit)
	if err != nil {
		return nil, fmt.Errorf("keyword search: %w", err)
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
			slog.WarnContext(ctx, "skip vanished candidate", "id", id, "err", err)
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

	return candidates, nil
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
		"ADD（新增）、UPDATE（替换旧记忆）、DELETE（删除旧记忆）、NOOP（无操作）。\n\n" +
		"规则：\n" +
		"- 如果候选记忆内容与事实几乎相同 → UPDATE\n" +
		"- 如果事实与候选记忆矛盾 → DELETE 旧 + ADD 新（即 UPDATE）\n" +
		"- 如果完全无关 → ADD\n\n" +
		"请返回 JSON: {\"action\": \"ADD|UPDATE|DELETE|NOOP\", \"target_id\": 1, \"reason\": \"...\"}"
}

func buildReconcileUserPrompt(fact *ExtractedFact, candidates []*candidateMapping) string {
	var b strings.Builder
	b.WriteString("新事实: ")
	b.WriteString(fact.Content)
	b.WriteString("\n\n")
	b.WriteString("候选记忆:\n")
	for _, c := range candidates {
		b.WriteString("候选记忆 ")
		b.WriteString(strconv.Itoa(c.intID))
		b.WriteString(": ")
		b.WriteString(c.memory.Content)
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
