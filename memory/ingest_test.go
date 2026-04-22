package memory

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

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
	defer srv.Close()
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
	defer srv.Close()
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
