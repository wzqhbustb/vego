package memory

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"time"
)

// EmbedConfig holds the configuration for the embedding client.
type EmbedConfig struct {
	APIKey  string
	BaseURL string
	Model   string
	Dims    int
}

// Embedder is an OpenAI-compatible HTTP client for text embeddings.
type Embedder struct {
	apiKey  string
	baseURL string
	model   string
	dims    int
	http    *http.Client
}

// NewEmbedder creates a new embedding client from the given configuration.
// Returns nil if APIKey is empty (embedding features disabled).
func NewEmbedder(cfg EmbedConfig) *Embedder {
	if cfg.APIKey == "" {
		return nil
	}

	baseURL := cfg.BaseURL
	if baseURL == "" {
		baseURL = "https://api.openai.com/v1"
	}
	model := cfg.Model
	if model == "" {
		model = "text-embedding-3-small"
	}
	dims := cfg.Dims
	if dims <= 0 {
		dims = 1536
	}

	return &Embedder{
		apiKey:  cfg.APIKey,
		baseURL: strings.TrimSuffix(baseURL, "/"),
		model:   model,
		dims:    dims,
		http: &http.Client{
			Timeout: 120 * time.Second,
		},
	}
}

// CloseIdleConnections closes any idle connections in the underlying
// HTTP client to prevent TCP connection leaks in long-running processes.
func (e *Embedder) CloseIdleConnections() {
	if e != nil && e.http != nil {
		e.http.CloseIdleConnections()
	}
}

// Embed generates an embedding vector for the given text.
func (e *Embedder) Embed(ctx context.Context, text string) ([]float32, error) {
	if e == nil {
		return nil, fmt.Errorf("embedder is nil")
	}
	if ctx == nil {
		return nil, fmt.Errorf("context must not be nil")
	}
	start := time.Now()
	vec, err := e.embed(ctx, text)
	if err != nil {
		slog.Error("embed request failed",
			"model", e.model,
			"duration_ms", time.Since(start).Milliseconds(),
			"error", err,
		)
		return nil, err
	}
	slog.Info("embed request completed",
		"model", e.model,
		"duration_ms", time.Since(start).Milliseconds(),
	)
	return vec, nil
}

// Dims returns the expected vector dimension.
func (e *Embedder) Dims() int {
	if e == nil {
		return 0
	}
	return e.dims
}

// EmbedBatch generates embedding vectors for multiple texts in a single API call.
// Returns a slice of vectors in the same order as the input texts.
func (e *Embedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	if e == nil {
		return nil, fmt.Errorf("embedder is nil")
	}
	if ctx == nil {
		return nil, fmt.Errorf("context must not be nil")
	}
	if len(texts) == 0 {
		return nil, nil
	}
	start := time.Now()
	vecs, err := e.embedBatch(ctx, texts)
	if err != nil {
		slog.Error("embed batch request failed",
			"model", e.model,
			"batch_size", len(texts),
			"duration_ms", time.Since(start).Milliseconds(),
			"error", err,
		)
		return nil, err
	}
	slog.Info("embed batch request completed",
		"model", e.model,
		"batch_size", len(texts),
		"duration_ms", time.Since(start).Milliseconds(),
	)
	return vecs, nil
}

// embedBatch performs the actual batch HTTP request.
func (e *Embedder) embedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	reqBody := embeddingRequest{
		Model:          e.model,
		Input:          texts,
		EncodingFormat: "float",
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	url := e.baseURL + "/embeddings"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+e.apiKey)

	resp, err := e.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	var result embeddingResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	if len(result.Data) == 0 {
		return nil, fmt.Errorf("response has no data")
	}

	// Sort by index to preserve input order.
	sort.Slice(result.Data, func(i, j int) bool {
		return result.Data[i].Index < result.Data[j].Index
	})

	vecs := make([][]float32, len(texts))
	for _, d := range result.Data {
		if d.Index < 0 || d.Index >= len(texts) {
			return nil, fmt.Errorf("invalid index %d in response", d.Index)
		}
		if len(d.Embedding) != e.dims {
			return nil, fmt.Errorf("dimension mismatch at index %d: expected %d, got %d", d.Index, e.dims, len(d.Embedding))
		}
		vecs[d.Index] = d.Embedding
	}

	// Verify all slots filled.
	for i, v := range vecs {
		if v == nil {
			return nil, fmt.Errorf("missing embedding at index %d", i)
		}
	}

	return vecs, nil
}

// embed performs the actual HTTP request for a single text.
func (e *Embedder) embed(ctx context.Context, text string) ([]float32, error) {
	vecs, err := e.embedBatch(ctx, []string{text})
	if err != nil {
		return nil, err
	}
	return vecs[0], nil
}

// --- internal request/response types ---

type embeddingRequest struct {
	Model          string      `json:"model"`
	Input          interface{} `json:"input"`
	EncodingFormat string      `json:"encoding_format,omitempty"`
}

type embeddingData struct {
	Index     int       `json:"index"`
	Embedding []float32 `json:"embedding"`
}

type embeddingResponse struct {
	Data []embeddingData `json:"data"`
}
