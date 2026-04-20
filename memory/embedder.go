package memory

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
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

// Embed generates an embedding vector for the given text.
func (e *Embedder) Embed(ctx context.Context, text string) ([]float32, error) {
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
	return e.dims
}

// embed performs the actual HTTP request.
func (e *Embedder) embed(ctx context.Context, text string) ([]float32, error) {
	reqBody := embeddingRequest{
		Model:          e.model,
		Input:          text,
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

	vec := result.Data[0].Embedding
	if len(vec) != e.dims {
		return nil, fmt.Errorf("dimension mismatch: expected %d, got %d", e.dims, len(vec))
	}

	return vec, nil
}

// --- internal request/response types ---

type embeddingRequest struct {
	Model          string `json:"model"`
	Input          string `json:"input"`
	EncodingFormat string `json:"encoding_format,omitempty"`
}

type embeddingResponse struct {
	Data []struct {
		Embedding []float32 `json:"embedding"`
	} `json:"data"`
}
