package memory

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNewEmbedderNil(t *testing.T) {
	if NewEmbedder(EmbedConfig{}) != nil {
		t.Error("Expected nil embedder when APIKey is empty")
	}
}

func TestNewEmbedderDefaults(t *testing.T) {
	e := NewEmbedder(EmbedConfig{APIKey: "sk-test"})
	if e == nil {
		t.Fatal("Expected non-nil embedder")
	}
	if e.baseURL != "https://api.openai.com/v1" {
		t.Errorf("BaseURL default: want %s, got %s", "https://api.openai.com/v1", e.baseURL)
	}
	if e.model != "text-embedding-3-small" {
		t.Errorf("Model default: want %s, got %s", "text-embedding-3-small", e.model)
	}
	if e.dims != 1536 {
		t.Errorf("Dims default: want %d, got %d", 1536, e.dims)
	}
	if e.Dims() != 1536 {
		t.Errorf("Dims() default: want %d, got %d", 1536, e.Dims())
	}
}

func TestNewEmbedderNegativeDims(t *testing.T) {
	e := NewEmbedder(EmbedConfig{APIKey: "sk-test", Dims: -1})
	if e == nil {
		t.Fatal("Expected non-nil embedder")
	}
	if e.dims != 1536 {
		t.Errorf("Negative dims should fall back to default: want %d, got %d", 1536, e.dims)
	}
}

func TestNewEmbedderCustom(t *testing.T) {
	e := NewEmbedder(EmbedConfig{
		APIKey:  "sk-test",
		BaseURL: "https://custom.example.com/v1/",
		Model:   "text-embedding-3-large",
		Dims:    3072,
	})
	if e == nil {
		t.Fatal("Expected non-nil embedder")
	}
	if e.baseURL != "https://custom.example.com/v1" {
		t.Errorf("BaseURL trim: want %s, got %s", "https://custom.example.com/v1", e.baseURL)
	}
	if e.model != "text-embedding-3-large" {
		t.Errorf("Model: want %s, got %s", "text-embedding-3-large", e.model)
	}
	if e.dims != 3072 {
		t.Errorf("Dims: want %d, got %d", 3072, e.dims)
	}
}

func TestEmbedSuccess(t *testing.T) {
	var gotReq embeddingRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&gotReq); err != nil {
			t.Errorf("decode request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(embeddingResponse{
			Data: []embeddingData{
				{Index: 0, Embedding: []float32{0.1, 0.2, 0.3, 0.4}},
			},
		}); err != nil {
			t.Errorf("encode response: %v", err)
		}
	}))
	t.Cleanup(srv.Close)

	embedder := NewEmbedder(EmbedConfig{
		APIKey:  "sk-test",
		BaseURL: srv.URL,
		Model:   "test-model",
		Dims:    4,
	})

	vec, err := embedder.Embed(context.Background(), "hello world")
	if err != nil {
		t.Fatalf("Embed failed: %v", err)
	}
	if len(vec) != 4 {
		t.Fatalf("Vector length: want %d, got %d", 4, len(vec))
	}
	if vec[0] != 0.1 || vec[1] != 0.2 || vec[2] != 0.3 || vec[3] != 0.4 {
		t.Errorf("Vector values mismatch: got %v", vec)
	}
	if gotReq.Model != "test-model" {
		t.Errorf("Request model: want test-model, got %s", gotReq.Model)
	}
	var inputTexts []string
	switch v := gotReq.Input.(type) {
	case []string:
		inputTexts = v
	case []interface{}:
		for _, item := range v {
			if s, ok := item.(string); ok {
				inputTexts = append(inputTexts, s)
			}
		}
	case string:
		inputTexts = []string{v}
	}
	if len(inputTexts) != 1 || inputTexts[0] != "hello world" {
		t.Errorf("Request input: want ['hello world'], got %v", gotReq.Input)
	}
	if gotReq.EncodingFormat != "float" {
		t.Errorf("Request encoding_format: want float, got %s", gotReq.EncodingFormat)
	}
}

func TestEmbed500Error(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		if _, err := w.Write([]byte(`internal error`)); err != nil {
			t.Errorf("write response: %v", err)
		}
	}))
	t.Cleanup(srv.Close)

	embedder := NewEmbedder(EmbedConfig{
		APIKey:  "sk-test",
		BaseURL: srv.URL,
	})

	_, err := embedder.Embed(context.Background(), "hello")
	if err == nil {
		t.Fatal("Expected error for 500 response")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Errorf("Error should mention status 500: %v", err)
	}
}

func TestEmbedNoData(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(embeddingResponse{
			Data: []embeddingData{},
		}); err != nil {
			t.Errorf("encode response: %v", err)
		}
	}))
	t.Cleanup(srv.Close)

	embedder := NewEmbedder(EmbedConfig{
		APIKey:  "sk-test",
		BaseURL: srv.URL,
	})

	_, err := embedder.Embed(context.Background(), "hello")
	if err == nil {
		t.Fatal("Expected error for empty data")
	}
}

func TestEmbedDimensionMismatch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(embeddingResponse{
			Data: []embeddingData{
				{Index: 0, Embedding: []float32{0.1, 0.2}}, // only 2 dims, but expected 4
			},
		}); err != nil {
			t.Errorf("encode response: %v", err)
		}
	}))
	t.Cleanup(srv.Close)

	embedder := NewEmbedder(EmbedConfig{
		APIKey:  "sk-test",
		BaseURL: srv.URL,
		Dims:    4,
	})

	_, err := embedder.Embed(context.Background(), "hello")
	if err == nil {
		t.Fatal("Expected error for dimension mismatch")
	}
	if !strings.Contains(err.Error(), "dimension mismatch") {
		t.Errorf("Error should mention dimension mismatch: %v", err)
	}
}

func TestEmbedContextCancel(t *testing.T) {
	embedder := NewEmbedder(EmbedConfig{APIKey: "sk-test"})
	embedder.http = &http.Client{
		Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			<-req.Context().Done()
			return nil, req.Context().Err()
		}),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := embedder.Embed(ctx, "hello")
	if err == nil {
		t.Fatal("Expected error for cancelled context")
	}
	if !strings.Contains(err.Error(), "context") {
		t.Errorf("Error should mention context: %v", err)
	}
}

func TestEmbedBatchParallel(t *testing.T) {
	callCount := 0
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		callCount++
		idx := callCount
		mu.Unlock()

		var req embeddingRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(embeddingResponse{
			Data: []embeddingData{
				{Index: 0, Embedding: []float32{float32(idx) * 0.1}},
			},
		}); err != nil {
			t.Errorf("encode response: %v", err)
		}
	}))
	t.Cleanup(srv.Close)

	embedder := NewEmbedder(EmbedConfig{
		APIKey:      "sk-test",
		BaseURL:     srv.URL,
		Model:       "test-model",
		Dims:        1,
		Concurrency: 2,
	})

	vecs, err := embedder.EmbedBatch(context.Background(), []string{"a", "b", "c"})
	if err != nil {
		t.Fatalf("EmbedBatch parallel failed: %v", err)
	}
	if len(vecs) != 3 {
		t.Fatalf("expected 3 vectors, got %d", len(vecs))
	}
	if callCount != 3 {
		t.Errorf("parallel mode should make 3 HTTP calls, got %d", callCount)
	}
}

func TestEmbedBatchSerial(t *testing.T) {
	callCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		var req embeddingRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(embeddingResponse{
			Data: []embeddingData{
				{Index: 0, Embedding: []float32{float32(callCount) * 0.1}},
			},
		}); err != nil {
			t.Errorf("encode response: %v", err)
		}
	}))
	t.Cleanup(srv.Close)

	embedder := NewEmbedder(EmbedConfig{
		APIKey:  "sk-test",
		BaseURL: srv.URL,
		Model:   "test-model",
		Dims:    1,
		Concurrency: 1,
	})

	vecs, err := embedder.EmbedBatch(context.Background(), []string{"hello", "world"})
	if err != nil {
		t.Fatalf("EmbedBatch serial failed: %v", err)
	}
	if len(vecs) != 2 {
		t.Fatalf("expected 2 vectors, got %d", len(vecs))
	}
	if callCount != 2 {
		t.Errorf("serial mode should make 2 HTTP calls, got %d", callCount)
	}
	if vecs[0][0] != 0.1 {
		t.Errorf("first vector: want 0.1, got %f", vecs[0][0])
	}
	if vecs[1][0] != 0.2 {
		t.Errorf("second vector: want 0.2, got %f", vecs[1][0])
	}
}
