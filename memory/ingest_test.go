package memory

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	vego "github.com/wzqhbustb/vego/vego"
)

// ----------------------------------------------------------------------
// computeContentHash
// ----------------------------------------------------------------------

func TestComputeContentHash(t *testing.T) {
	h1 := computeContentHash("hello world")
	h2 := computeContentHash("hello world")
	h3 := computeContentHash("hello world ")

	if h1 != h2 {
		t.Errorf("same content produced different hashes: %s vs %s", h1, h2)
	}
	if h1 == h3 {
		t.Errorf("different content produced same hash: %s", h1)
	}
	if len(h1) != 64 {
		t.Errorf("expected SHA256 hex length 64, got %d", len(h1))
	}
}

// ----------------------------------------------------------------------
// ExtractFacts — ModeRaw
// ----------------------------------------------------------------------

func TestExtractFactsRaw(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()

	messages := []Message{
		{Role: "user", Content: "hello", Tags: []string{"greeting"}},
		{Role: "assistant", Content: "hi there", Tags: []string{"greeting", "reply"}},
	}

	facts, err := s.ExtractFacts(context.Background(), messages, ModeRaw)
	if err != nil {
		t.Fatalf("extract facts: %v", err)
	}
	if len(facts) != 2 {
		t.Fatalf("expected 2 facts, got %d", len(facts))
	}
	if facts[0].Content != "hello" {
		t.Errorf("fact[0].Content = %q, want hello", facts[0].Content)
	}
	if facts[0].SourceMsg != 0 {
		t.Errorf("fact[0].SourceMsg = %d, want 0", facts[0].SourceMsg)
	}
	if facts[1].Content != "hi there" {
		t.Errorf("fact[1].Content = %q, want hi there", facts[1].Content)
	}
	if len(facts[1].Tags) != 2 {
		t.Errorf("fact[1].Tags = %v, want 2 tags", facts[1].Tags)
	}
}

func TestExtractFactsRawEmpty(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()

	facts, err := s.ExtractFacts(context.Background(), nil, ModeRaw)
	if err != nil {
		t.Fatalf("extract facts: %v", err)
	}
	if facts != nil && len(facts) != 0 {
		t.Errorf("expected nil or empty facts, got %v", facts)
	}
}

// ----------------------------------------------------------------------
// ExtractFacts — ModeNormal placeholder
// ----------------------------------------------------------------------

func TestExtractFactsModeNormalNoLLM(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()
	// Ensure llm is nil.
	s.llm = nil

	messages := []Message{{Role: "user", Content: "hello"}}
	_, err := s.ExtractFacts(context.Background(), messages, ModeNormal)
	if err == nil {
		t.Fatal("expected error when llm is nil for ModeNormal")
	}
}

func TestExtractFactsModeNormalFallback(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()

	// Use mock server that always returns 500 to test retry + fallback path.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)
	s.llm = NewLLMClient(LLMConfig{APIKey: "test", BaseURL: srv.URL, Model: "test"})

	messages := []Message{
		{Role: "user", Content: "fallback test"},
	}
	facts, err := s.ExtractFacts(context.Background(), messages, ModeNormal)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(facts) != 1 || facts[0].Content != "fallback test" {
		t.Errorf("unexpected facts: %v", facts)
	}
}

// ----------------------------------------------------------------------
// StoreRawMessages — basic storage
// ----------------------------------------------------------------------

func TestStoreRawMessages(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()

	ctx := context.Background()
	messages := []Message{
		{Role: "user", Content: "first message"},
		{Role: "assistant", Content: "second message"},
	}

	stored, err := s.StoreRawMessages(ctx, "sess-1", messages)
	if err != nil {
		t.Fatalf("store raw messages: %v", err)
	}
	if stored != 2 {
		t.Fatalf("expected 2 stored, got %d", stored)
	}

	// Verify in Vego — use ForEach to avoid search ranking ambiguity
	// with mock embedder (all vectors are identical).
	var found []*Memory
	if err := s.coll.ForEach(func(doc *vego.Document) bool {
		m, err := docToMemory(doc)
		if err != nil {
			t.Logf("skip corrupt doc: %v", err)
			return true
		}
		if m.SessionID == "sess-1" {
			found = append(found, m)
		}
		return true
	}); err != nil {
		t.Fatalf("foreach: %v", err)
	}
	if len(found) != 2 {
		t.Fatalf("expected 2 session memories, got %d", len(found))
	}

	for _, m := range found {
		if m.MemoryType != TypeSession {
			t.Errorf("memory_type = %s, want session", m.MemoryType)
		}
		if m.SessionID != "sess-1" {
			t.Errorf("session_id = %s, want sess-1", m.SessionID)
		}
		if m.Seq < 1 || m.Seq > 2 {
			t.Errorf("seq = %d, want 1 or 2", m.Seq)
		}
		if m.ContentHash == "" {
			t.Error("content_hash should not be empty")
		}
	}
}

// ----------------------------------------------------------------------
// Deduplication
// ----------------------------------------------------------------------

func TestStoreRawMessagesDedup(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()

	ctx := context.Background()
	messages := []Message{
		{Role: "user", Content: "hello"},
		{Role: "user", Content: "hello"}, // duplicate
		{Role: "assistant", Content: "world"},
	}

	stored, err := s.StoreRawMessages(ctx, "sess-dedup", messages)
	if err != nil {
		t.Fatalf("store raw messages: %v", err)
	}
	if stored != 2 {
		t.Fatalf("expected 2 stored (1 duplicate), got %d", stored)
	}

	// Verify via ContentHashIndex
	if !s.contentHashIndex.Has("sess-dedup", computeContentHash("hello")) {
		t.Error("contentHashIndex should contain 'hello'")
	}
	if !s.contentHashIndex.Has("sess-dedup", computeContentHash("world")) {
		t.Error("contentHashIndex should contain 'world'")
	}
}

// ----------------------------------------------------------------------
// Cumulative storage (multi-turn)
// ----------------------------------------------------------------------

func TestStoreRawMessagesCumulative(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()

	ctx := context.Background()

	// First turn
	stored1, err := s.StoreRawMessages(ctx, "sess-cum", []Message{
		{Role: "user", Content: "msg1"},
	})
	if err != nil {
		t.Fatalf("turn 1: %v", err)
	}
	if stored1 != 1 {
		t.Fatalf("turn 1 expected 1, got %d", stored1)
	}

	// Second turn
	stored2, err := s.StoreRawMessages(ctx, "sess-cum", []Message{
		{Role: "assistant", Content: "msg2"},
	})
	if err != nil {
		t.Fatalf("turn 2: %v", err)
	}
	if stored2 != 1 {
		t.Fatalf("turn 2 expected 1, got %d", stored2)
	}

	// Third turn — duplicate
	stored3, err := s.StoreRawMessages(ctx, "sess-cum", []Message{
		{Role: "user", Content: "msg1"},
	})
	if err != nil {
		t.Fatalf("turn 3: %v", err)
	}
	if stored3 != 0 {
		t.Fatalf("turn 3 expected 0 (dedup), got %d", stored3)
	}

	// Verify total count via Search
	all, err := s.Search(ctx, "msg")
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("expected 2 unique memories, got %d", len(all))
	}

	// Verify Seq progression
	maxSeq := s.contentHashIndex.MaxSeq("sess-cum")
	if maxSeq != 2 {
		t.Errorf("maxSeq = %d, want 2", maxSeq)
	}
}

// ----------------------------------------------------------------------
// Error cases
// ----------------------------------------------------------------------

func TestStoreRawMessagesEmptySession(t *testing.T) {
	s := newTestStore(t)
	defer s.Close()

	ctx := context.Background()
	_, err := s.StoreRawMessages(ctx, "", []Message{{Role: "user", Content: "hello"}})
	if err == nil {
		t.Fatal("expected error for empty sessionID")
	}
}

func TestStoreRawMessagesEmptyMessages(t *testing.T) {
	s := newTestStore(t)
	defer s.Close()

	ctx := context.Background()
	stored, err := s.StoreRawMessages(ctx, "sess-empty", nil)
	if err != nil {
		t.Fatalf("expected no error for empty messages, got %v", err)
	}
	if stored != 0 {
		t.Errorf("expected 0 stored, got %d", stored)
	}
}

// ----------------------------------------------------------------------
// ContentHashIndex rebuild after reopen
// ----------------------------------------------------------------------

func TestContentHashIndexRebuildOnReopen(t *testing.T) {
	dir := t.TempDir()

	// Open, store, close
	store1, err := Open(dir, WithDataDir(dir), WithDimension(128), WithEmbedding("test", "", "m", 128))
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}
	setupMockEmbedder(t, store1, 128)

	ctx := context.Background()
	_, err = store1.StoreRawMessages(ctx, "sess-reopen", []Message{
		{Role: "user", Content: "persist me"},
	})
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	if err := store1.Close(); err != nil {
		t.Fatalf("close 1: %v", err)
	}

	// Reopen
	store2, err := Open(dir, WithDataDir(dir), WithDimension(128), WithEmbedding("test", "", "m", 128))
	if err != nil {
		t.Fatalf("open 2: %v", err)
	}
	setupMockEmbedder(t, store2, 128)
	defer store2.Close()

	// Deduplication should still work
	stored, err := store2.StoreRawMessages(ctx, "sess-reopen", []Message{
		{Role: "user", Content: "persist me"},
	})
	if err != nil {
		t.Fatalf("store after reopen: %v", err)
	}
	if stored != 0 {
		t.Fatalf("expected 0 (dedup after rebuild), got %d", stored)
	}
}

// ----------------------------------------------------------------------
// IngestResult placeholder
// ----------------------------------------------------------------------

func TestIngestResultZero(t *testing.T) {
	var r IngestResult
	if r.Added != 0 || r.Updated != 0 || r.Deleted != 0 || r.Skipped != 0 {
		t.Errorf("expected zero values, got %+v", r)
	}
}

// ----------------------------------------------------------------------
// extractFactsWithRetry — LLM path
// ----------------------------------------------------------------------

func TestExtractFactsWithRetrySuccess(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()

	// LLM returns valid JSON array of facts
	setupMockLLM(t, s, `[{"content":"extracted fact","tags":["tag1"],"query_intent":false}]`)

	messages := []Message{
		{Role: "user", Content: "hello world"},
	}
	facts, err := s.ExtractFacts(context.Background(), messages, ModeNormal)
	if err != nil {
		t.Fatalf("extract facts: %v", err)
	}
	if len(facts) != 1 {
		t.Fatalf("expected 1 fact, got %d", len(facts))
	}
	if facts[0].Content != "extracted fact" {
		t.Errorf("content = %q, want 'extracted fact'", facts[0].Content)
	}
}

func TestExtractFactsWithRetryLLMError(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()

	// LLM returns HTTP error → fallback to raw
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)
	s.llm = NewLLMClient(LLMConfig{APIKey: "test", BaseURL: srv.URL, Model: "test"})

	messages := []Message{{Role: "user", Content: "fallback test"}}
	facts, err := s.ExtractFacts(context.Background(), messages, ModeNormal)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(facts) != 1 || facts[0].Content != "fallback test" {
		t.Errorf("expected raw fallback, got %+v", facts)
	}
}

func TestExtractFactsWithRetryParseError(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()

	// LLM returns invalid JSON → fallback to raw
	setupMockLLM(t, s, `not json at all`)

	messages := []Message{{Role: "user", Content: "parse fallback"}}
	facts, err := s.ExtractFacts(context.Background(), messages, ModeNormal)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(facts) != 1 || facts[0].Content != "parse fallback" {
		t.Errorf("expected raw fallback, got %+v", facts)
	}
}

func TestExtractFactsWrappedFormat(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()

	// LLM returns wrapped object format {"facts":[...]}
	setupMockLLM(t, s, `{"facts":[{"content":"wrapped fact","tags":["t1"],"query_intent":false}]}`)

	messages := []Message{{Role: "user", Content: "hello world"}}
	facts, err := s.ExtractFacts(context.Background(), messages, ModeNormal)
	if err != nil {
		t.Fatalf("extract facts: %v", err)
	}
	if len(facts) != 1 {
		t.Fatalf("expected 1 fact, got %d", len(facts))
	}
	if facts[0].Content != "wrapped fact" {
		t.Errorf("content = %q, want 'wrapped fact'", facts[0].Content)
	}
}

// TestExtractFactsTrailingGarbage verifies that a trailing quote (or other
// garbage) after a valid JSON object does not cause a parse failure.
// Regression test for qwen2.5 occasionally appending an extra '"'.
func TestExtractFactsTrailingGarbage(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()

	// Valid JSON followed by an extra double-quote.
	setupMockLLM(t, s, `{"facts":[{"content":"weather is sunny","tags":["weather"]}]}"`)

	messages := []Message{{Role: "user", Content: "today is sunny"}}
	facts, err := s.ExtractFacts(context.Background(), messages, ModeNormal)
	if err != nil {
		t.Fatalf("extract facts: %v", err)
	}
	if len(facts) != 1 {
		t.Fatalf("expected 1 fact, got %d", len(facts))
	}
	if facts[0].Content != "weather is sunny" {
		t.Errorf("content = %q, want 'weather is sunny'", facts[0].Content)
	}
}

// ----------------------------------------------------------------------
// Concurrent safety
// ----------------------------------------------------------------------

func TestStoreRawMessagesConcurrent(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()

	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			msgs := []Message{
				{Role: "user", Content: fmt.Sprintf("msg-%d", idx)},
			}
			_, err := s.StoreRawMessages(ctx, "sess-concurrent", msgs)
			if err != nil {
				t.Errorf("store goroutine %d: %v", idx, err)
			}
		}(i)
	}
	wg.Wait()

	// Verify all 10 unique messages were stored with distinct Seq values
	maxSeq := s.contentHashIndex.MaxSeq("sess-concurrent")
	if maxSeq != 10 {
		t.Errorf("maxSeq = %d, want 10 (some Seq values were duplicated)", maxSeq)
	}

	// Verify deduplication still works
	stored, err := s.StoreRawMessages(ctx, "sess-concurrent", []Message{
		{Role: "user", Content: "msg-0"},
	})
	if err != nil {
		t.Fatalf("dedup store: %v", err)
	}
	if stored != 0 {
		t.Errorf("expected 0 (dedup), got %d", stored)
	}
}
// ----------------------------------------------------------------------
// Ingest entry-point coverage (non-integration)
// ----------------------------------------------------------------------

func TestIngest_ModeRaw(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()
	req := IngestRequest{
		Messages: []Message{
			{Role: "user", Content: "hello world"},
			{Role: "assistant", Content: "hi there"},
		},
		Mode:      ModeRaw,
		SessionID: "sess-ingest-raw",
	}

	res, err := s.Ingest(ctx, req)
	if err != nil {
		t.Fatalf("Ingest ModeRaw: %v", err)
	}
	if res.Added != 2 {
		t.Errorf("expected Added=2, got %d", res.Added)
	}
	if res.Updated != 0 || res.Deleted != 0 || res.Skipped != 0 {
		t.Errorf("unexpected non-zero fields: %+v", res)
	}

	// Deduplication on second ingest
	res2, err := s.Ingest(ctx, req)
	if err != nil {
		t.Fatalf("Ingest ModeRaw second time: %v", err)
	}
	if res2.Added != 0 {
		t.Errorf("expected Added=0 (dedup), got %d", res2.Added)
	}
}

func TestIngest_ModeNormal_MissingAgentID(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	_, err := s.Ingest(context.Background(), IngestRequest{
		Messages: []Message{{Role: "user", Content: "test"}},
		Mode:     ModeNormal,
		AgentID:  "",
	})
	if err == nil {
		t.Fatal("expected error for missing AgentID in ModeNormal")
	}
}

func TestIngest_ModeRaw_MissingSessionID(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	_, err := s.Ingest(context.Background(), IngestRequest{
		Messages: []Message{{Role: "user", Content: "test"}},
		Mode:     ModeRaw,
		SessionID: "",
	})
	if err == nil {
		t.Fatal("expected error for missing SessionID in ModeRaw")
	}
}

func TestIngest_UnknownMode(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	_, err := s.Ingest(context.Background(), IngestRequest{
		Messages: []Message{{Role: "user", Content: "test"}},
		Mode:     IngestMode(99),
	})
	if err == nil {
		t.Fatal("expected error for unknown ingest mode")
	}
}

func TestIngest_Truncation(t *testing.T) {
	s := newTestStore(t, WithIngestParams(10, 5))
	setupMockEmbedder(t, s, 128)
	t.Cleanup(func() { s.Close() })

	ctx := context.Background()
	req := IngestRequest{
		Messages: []Message{
			{Role: "user", Content: "hello"},
			{Role: "user", Content: "world"}, // total 10 runes, max=5 → drop "hello" (5)
		},
		Mode:      ModeRaw,
		SessionID: "sess-truncate",
	}

	res, err := s.Ingest(ctx, req)
	if err != nil {
		t.Fatalf("Ingest with truncation: %v", err)
	}
	// "hello" (5 runes) dropped, "world" (5 runes) kept → 1 stored
	if res.Added != 1 {
		t.Errorf("expected Added=1 after truncation, got %d", res.Added)
	}
}

// ----------------------------------------------------------------------
// truncateMessages pure-function tests
// ----------------------------------------------------------------------

func TestTruncateMessages(t *testing.T) {
	tests := []struct {
		name     string
		msgs     []Message
		max      int
		wantLen  int
		wantLast string
	}{
		{
			name:     "no truncation needed",
			msgs:     []Message{{Role: "user", Content: "hi"}},
			max:      10,
			wantLen:  1,
			wantLast: "hi",
		},
		{
			name: "drop oldest",
			msgs: []Message{
				{Role: "user", Content: "hello"},
				{Role: "user", Content: "world"},
			},
			max:      5,
			wantLen:  1,
			wantLast: "world",
		},
		{
			name: "drop multiple oldest",
			msgs: []Message{
				{Role: "user", Content: "a"},
				{Role: "user", Content: "bb"},
				{Role: "user", Content: "ccc"},
			},
			max:      4,
			wantLen:  1,
			wantLast: "ccc",
		},
		{
			name:     "single message exceeds limit",
			msgs:     []Message{{Role: "user", Content: "hello world"}},
			max:      5,
			wantLen:  1,
			wantLast: "hello world",
		},
		{
			name:     "empty messages",
			msgs:     []Message{},
			max:      10,
			wantLen:  0,
			wantLast: "",
		},
		{
			name:     "maxRunes zero means no limit",
			msgs:     []Message{{Role: "user", Content: "hello"}},
			max:      0,
			wantLen:  1,
			wantLast: "hello",
		},
		{
			name:     "negative maxRunes means no limit",
			msgs:     []Message{{Role: "user", Content: "hello"}},
			max:      -1,
			wantLen:  1,
			wantLast: "hello",
		},
		{
			name: "utf8 rune counting",
			msgs: []Message{
				{Role: "user", Content: "你好"},    // 2 runes
				{Role: "user", Content: "世界"},    // 2 runes
			},
			max:      2,
			wantLen:  1,
			wantLast: "世界",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := truncateMessages(tt.msgs, tt.max)
			if len(got) != tt.wantLen {
				t.Fatalf("len=%d, want %d", len(got), tt.wantLen)
			}
			if tt.wantLen > 0 && got[len(got)-1].Content != tt.wantLast {
				t.Errorf("last content=%q, want %q", got[len(got)-1].Content, tt.wantLast)
			}
		})
	}
}

// ----------------------------------------------------------------------
// Fuzzing: LLM JSON response parsing
// ----------------------------------------------------------------------

func FuzzParseJSONExtractedFacts(f *testing.F) {
	// Seed corpus with valid and edge-case inputs.
	f.Add(`[{"content":"hello","tags":["greeting"],"query_intent":false}]`)
	f.Add(`[]`)
	f.Add(`[{"content":"","tags":[],"query_intent":true}]`)
	f.Add(`[{"content":"a","tags":["tag1","tag2"],"query_intent":false,"source_msg":0}]`)
	f.Add(`[{"content":"with metadata","tags":[],"metadata":{"temporal":{"kind":"explicit_absolute"}}}]`)

	f.Fuzz(func(t *testing.T, raw string) {
		// ParseJSON[[]ExtractedFact] must never panic, regardless of input.
		facts, err := ParseJSON[[]ExtractedFact](raw)
		_ = facts
		_ = err
	})
}

func FuzzExtractFactsRaw(f *testing.F) {
	f.Add("hello world")
	f.Add("")
	f.Add("{\"key\": \"not an array\"}")
	f.Add(string([]byte{0xFF, 0xFE, 0xFD})) // invalid UTF-8

	f.Fuzz(func(t *testing.T, content string) {
		// extractFactsRaw must never panic, regardless of input.
		msg := Message{Content: content, Timestamp: time.Now()}
		facts := extractFactsRaw([]Message{msg})
		if len(facts) != 1 {
			t.Errorf("expected 1 fact, got %d", len(facts))
		}
	})
}

func FuzzParseJSONTags(f *testing.F) {
	f.Add(`["tag1","tag2"]`)
	f.Add(`[]`)
	f.Add(`[""]`)
	f.Add(`["tag with spaces"]`)

	f.Fuzz(func(t *testing.T, raw string) {
		// ParseJSON[[]string] must never panic.
		tags, err := ParseJSON[[]string](raw)
		_ = tags
		_ = err
	})
}
