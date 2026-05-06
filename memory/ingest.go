package memory

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	vego "github.com/wzqhbustb/vego/vego"
)

// ----------------------------------------------------------------------
// Unified Ingest entry point
// ----------------------------------------------------------------------

// Ingest orchestrates the full ingestion pipeline:
//
// ModeNormal:
//  1. Truncate messages to MaxConversationRunes
//  2. ExtractFacts → Reconcile
//
// ModeRaw:
//  1. Truncate messages to MaxConversationRunes
//  2. StoreRawMessages (which internally handles ExtractFacts(ModeRaw) + dedup + insert)
//
// Returns IngestResult and any error.
func (s *MemoryStore) Ingest(ctx context.Context, req IngestRequest) (*IngestResult, error) {
	if ctx == nil {
		return nil, fmt.Errorf("context must not be nil")
	}
	messages := req.Messages
	if s.config.MaxConversationRunes > 0 {
		messages = truncateMessages(messages, s.config.MaxConversationRunes)
	}

	switch req.Mode {
	case ModeNormal:
		if req.AgentID == "" {
			return nil, fmt.Errorf("AgentID is required for ModeNormal")
		}
		facts, err := s.ExtractFacts(ctx, messages, ModeNormal)
		if err != nil {
			return nil, err
		}
		return s.Reconcile(ctx, req.AgentID, facts)

	case ModeRaw:
		if req.SessionID == "" {
			return nil, fmt.Errorf("SessionID is required for ModeRaw")
		}
		stored, err := s.StoreRawMessages(ctx, req.SessionID, messages)
		if err != nil {
			return nil, err
		}
		// StoreRawMessages already handles ExtractFacts(ModeRaw) internally;
		// wrap the count into an IngestResult.
		return &IngestResult{Added: stored}, nil

	default:
		return nil, fmt.Errorf("unknown ingest mode: %d", req.Mode)
	}
}

// truncateMessages keeps the newest messages (at the end of the slice) such
// that the total rune count does not exceed maxRunes. Oldest messages are
// dropped first. This ensures the LLM prompt does not grow unboundedly.
func truncateMessages(messages []Message, maxRunes int) []Message {
	if maxRunes <= 0 || len(messages) == 0 {
		return messages
	}

	var total int
	for _, m := range messages {
		total += utf8.RuneCountInString(m.Content)
	}
	if total <= maxRunes {
		return messages
	}

	// Drop oldest messages first until we fit within the budget.
	for i := 0; i < len(messages)-1; i++ {
		total -= utf8.RuneCountInString(messages[i].Content)
		if total <= maxRunes {
			return messages[i+1:]
		}
	}
	// All messages dropped and total still exceeds maxRunes
	// (single message longer than limit) — keep only the last message.
	return messages[len(messages)-1:]
}

// ----------------------------------------------------------------------
// Fact extraction
// ----------------------------------------------------------------------

// ExtractFacts converts raw messages into structured facts.
// ModeRaw bypasses LLM and returns each message as a fact directly.
// After extraction, relative temporal expressions are normalized to absolute dates.
func (s *MemoryStore) ExtractFacts(ctx context.Context, messages []Message, mode IngestMode) ([]ExtractedFact, error) {
	if ctx == nil {
		return nil, fmt.Errorf("context must not be nil")
	}
	if len(messages) == 0 {
		return nil, nil
	}

	var facts []ExtractedFact
	var err error
	switch mode {
	case ModeRaw:
		facts = extractFactsRaw(messages)
	case ModeNormal:
		facts, err = s.extractFactsLLM(ctx, messages)
	default:
		return nil, fmt.Errorf("unknown ingest mode: %d", mode)
	}
	if err != nil {
		return nil, err
	}

	// Normalize relative temporal expressions to absolute dates.
	facts = NormalizeTemporalFacts(facts, messages, time.Now())
	return facts, nil
}

// extractFactsRaw converts messages to facts without LLM processing.
func extractFactsRaw(messages []Message) []ExtractedFact {
	facts := make([]ExtractedFact, len(messages))
	for i, msg := range messages {
		facts[i] = ExtractedFact{
			Content:   msg.Content,
			Tags:      append([]string(nil), msg.Tags...),
			SourceMsg: i,
		}
	}
	return facts
}

// extractFactsLLM performs LLM-based fact extraction with retry and fallback.
func (s *MemoryStore) extractFactsLLM(ctx context.Context, messages []Message) ([]ExtractedFact, error) {
	if s.llm == nil {
		return nil, fmt.Errorf("llm not configured")
	}
	return s.extractFactsWithRetry(ctx, messages)
}

// extractFactsWithRetry calls the LLM to extract facts from messages.
// It retries once on failure, then falls back to raw mode if both attempts fail.
func (s *MemoryStore) extractFactsWithRetry(ctx context.Context, messages []Message) ([]ExtractedFact, error) {
	systemPrompt := buildFactExtractionPrompt()
	userPrompt := buildFactExtractionUserPrompt(messages)

	var raw string
	var err error
	for attempt := 0; attempt < 2; attempt++ {
		raw, err = s.llm.CompleteJSON(ctx, systemPrompt, userPrompt)
		if err == nil {
			break
		}
		s.logger.WarnContext(ctx, "llm fact extraction failed, retrying", "attempt", attempt+1, "error", err)
	}
	if err != nil {
		s.logger.WarnContext(ctx, "llm fact extraction failed after retry, falling back to raw", "error", err)
		return extractFactsRaw(messages), nil
	}

	facts, err := ParseJSON[[]ExtractedFact](raw)
	if err != nil {
		s.logger.WarnContext(ctx, "llm response parse failed, falling back to raw", "error", err)
		return extractFactsRaw(messages), nil
	}

	return facts, nil
}

// buildFactExtractionPrompt returns the system prompt for fact extraction.
func buildFactExtractionPrompt() string {
	return "你是一个记忆提取助手。从对话中提取简洁、自包含的事实。\n\n" +
		"规则：\n" +
		"1. 输出数组中每个 item 对应一条事实。\n" +
		"2. 将同一主题的事实合并为一条更全面的描述。\n" +
		"3. 丢弃问候、寒暄和纯粹社交客套。\n" +
		"4. 显式保留时间锚点（日期、时间、截止日期）。\n" +
		"5. 每条事实必须自包含，无需原始对话即可理解。\n" +
		"6. 搜索意图标记 query_intent: true（例如“查找”、“搜索”、“查询”）。\n" +
		"7. 使用内容原始语言描述事实。\n" +
		"8. 包含相关标签以分类事实（例如 preference、project、contact）。\n" +
		"9. 不提取已经是常识的事实。\n" +
		"10. 如果一条消息包含多个不同事实，拆分为多个 item。\n" +
		"11. 保持事实简洁（理想情况下 1-2 句话）。\n" +
		"12. 保持事实准确性，不推断或编造信息。\n\n" +
		"输出 JSON: [{\"content\":\"...\",\"tags\":[\"...\"],\"query_intent\":false}]"
}

// buildFactExtractionUserPrompt builds the user prompt from messages.
func buildFactExtractionUserPrompt(messages []Message) string {
	var b strings.Builder
	for _, m := range messages {
		b.WriteString(m.Role)
		b.WriteString(": ")
		b.WriteString(m.Content)
		b.WriteString("\n")
	}
	return b.String()
}

// ----------------------------------------------------------------------
// Raw session storage (ModeRaw)
// ----------------------------------------------------------------------

// StoreRawMessages stores raw conversation messages with deduplication.
// It returns the number of newly stored messages.
func (s *MemoryStore) StoreRawMessages(ctx context.Context, sessionID string, messages []Message) (int, error) {
	if ctx == nil {
		return 0, fmt.Errorf("context must not be nil")
	}
	if sessionID == "" {
		return 0, fmt.Errorf("sessionID must not be empty")
	}
	if len(messages) == 0 {
		return 0, nil
	}

	facts, err := s.ExtractFacts(ctx, messages, ModeRaw)
	if err != nil {
		return 0, err
	}

	// Pre-compute hashes and embeds outside the lock to avoid blocking
	// all write operations during network I/O.
	type prepared struct {
		fact    *ExtractedFact
		hash    string
		vec     []float32
		agentID string
	}
	preparedList := make([]prepared, 0, len(facts))
	for i := range facts {
		if err := validateInput(facts[i].Content, facts[i].Tags, s.config); err != nil {
			return 0, fmt.Errorf("message %d: %w", i, err)
		}
		// Use the *original* message content for dedup hash, not the
		// temporal-normalized fact content.  This prevents cross-day
		// dedup failure (e.g. "昨天发布" normalized to different dates).
		srcIdx := facts[i].SourceMsg
		var hashSrc string
		if srcIdx >= 0 && srcIdx < len(messages) {
			hashSrc = messages[srcIdx].Content
		} else {
			hashSrc = facts[i].Content
		}
		hash := computeContentHash(hashSrc)
		if s.contentHashIndex.Has(sessionID, hash) {
			continue
		}
		vec, err := s.embed(ctx, facts[i].Content)
		if err != nil {
			return 0, err
		}
		agentID := ""
		if facts[i].SourceMsg >= 0 && facts[i].SourceMsg < len(messages) {
			agentID = messages[facts[i].SourceMsg].AgentID
		}
		preparedList = append(preparedList, prepared{&facts[i], hash, vec, agentID})
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	nextSeq := s.contentHashIndex.MaxSeq(sessionID) + 1
	stored := 0
	for _, p := range preparedList {
		if err := ctx.Err(); err != nil {
			return stored, err
		}
		// Re-check dedup under lock: another goroutine may have inserted it.
		if s.contentHashIndex.Has(sessionID, p.hash) {
			continue
		}

		mem := &Memory{
			ID:          vego.DocumentID(),
			Content:     p.fact.Content,
			MemoryType:  TypeSession,
			State:       StateActive,
			Tags:        append([]string(nil), p.fact.Tags...),
			AgentID:     p.agentID,
			SessionID:   sessionID,
			Seq:         nextSeq,
			ContentHash: p.hash,
			Version:     1,
			Metadata:    copyMap(p.fact.Metadata),
			CreatedAt:   time.Now(),
			UpdatedAt:   time.Now(),
		}

		doc, err := memoryToDoc(mem, p.vec)
		if err != nil {
			return stored, fmt.Errorf("marshal session message seq=%d: %w", nextSeq, err)
		}
		if err := s.coll.InsertContext(ctx, doc); err != nil {
			return stored, fmt.Errorf("insert session message seq=%d: %w", nextSeq, err)
		}

		s.inverted.Add(mem.ID, mem.Content)
		s.contentHashIndex.Add(sessionID, p.hash, mem.ID, nextSeq)
		nextSeq++
		stored++
	}

	return stored, nil
}

// computeContentHash computes a SHA256 fingerprint for deduplication.
func computeContentHash(content string) string {
	h := sha256.Sum256([]byte(content))
	return hex.EncodeToString(h[:])
}
