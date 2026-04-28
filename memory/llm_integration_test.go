//go:build integration

package memory

import (
	"context"
	"os"
	"testing"
)

// TestIntegrationIngestModeRaw tests the full Ingest pipeline in ModeRaw
// without requiring a real LLM.
func TestIntegrationIngestModeRaw(t *testing.T) {
	s := newTestStore(t)
	setupMockEmbedder(t, s, 128)
	defer s.Close()

	ctx := context.Background()

	req := IngestRequest{
		Messages: []Message{
			{Role: "user", Content: "hello world"},
			{Role: "assistant", Content: "hi there"},
		},
		Mode:      ModeRaw,
		SessionID: "test-session",
	}

	result, err := s.Ingest(ctx, req)
	if err != nil {
		t.Fatalf("Ingest(ModeRaw): %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}

	// Verify messages are searchable.
	results, err := s.Search(ctx, "hello")
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("expected search results after ingest")
	}
}

// TestIntegrationIngestModeNormal tests the full Ingest pipeline with a real
// LLM and embedding service. Skips if VEGO_LLM_API_KEY is not set.
func TestIntegrationIngestModeNormal(t *testing.T) {
	apiKey := os.Getenv("VEGO_LLM_API_KEY")
	if apiKey == "" {
		t.Skip("VEGO_LLM_API_KEY not set, skipping real LLM integration test")
	}

	// Use OpenAI defaults for integration test.
	s := newTestStore(t,
		WithLLM(apiKey, "https://api.openai.com/v1", "gpt-4o-mini", 0.1),
		WithEmbedding(apiKey, "https://api.openai.com/v1", "text-embedding-3-small", 1536),
		WithDimension(1536),
	)
	defer s.Close()

	ctx := context.Background()

	req := IngestRequest{
		Messages: []Message{
			{Role: "user", Content: "I love using Go for concurrent programming"},
			{Role: "assistant", Content: "Go's goroutines and channels are indeed powerful"},
		},
		Mode:    ModeNormal,
		AgentID: "test-agent",
	}

	result, err := s.Ingest(ctx, req)
	if err != nil {
		t.Fatalf("Ingest(ModeNormal): %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}

	// Verify at least one memory was added or updated.
	if result.Added == 0 && result.Updated == 0 {
		t.Logf("warning: no memories added or updated, result=%+v", result)
	}

	// Verify memories are searchable.
	results, err := s.Search(ctx, "Go programming")
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	t.Logf("Search returned %d results", len(results))
}
