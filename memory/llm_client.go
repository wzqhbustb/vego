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

const maxErrorBody = 4096 // limit error response body to avoid huge error messages

// LLMConfig holds the configuration for the LLM client.
type LLMConfig struct {
	APIKey      string
	BaseURL     string
	Model       string
	Temperature float64
}

// LLMClient is an OpenAI-compatible HTTP client for LLM completions.
type LLMClient struct {
	apiKey      string
	baseURL     string
	model       string
	temperature float64
	http        *http.Client
}

// NewLLMClient creates a new LLM client from the given configuration.
// Returns nil if APIKey is empty (LLM features disabled).
func NewLLMClient(cfg LLMConfig) *LLMClient {
	if cfg.APIKey == "" {
		return nil
	}

	baseURL := cfg.BaseURL
	if baseURL == "" {
		baseURL = "https://api.openai.com/v1"
	}
	model := cfg.Model
	if model == "" {
		model = "gpt-4o-mini"
	}
	return &LLMClient{
		apiKey:      cfg.APIKey,
		baseURL:     strings.TrimSuffix(baseURL, "/"),
		model:       model,
		temperature: cfg.Temperature,
		http: &http.Client{
			Timeout: 120 * time.Second,
		},
	}
}

// CompleteJSON sends a chat completion request with system + user prompts
// and returns the assistant's content. It forces JSON output via
// response_format and falls back on HTTP 400 (Ollama/vLLM compatibility).
func (c *LLMClient) CompleteJSON(ctx context.Context, system, user string) (string, error) {
	start := time.Now()
	content, promptTokens, completionTokens, err := c.complete(ctx, system, user, true)
	if err != nil {
		slog.Error("llm request failed",
			"model", c.model,
			"duration_ms", time.Since(start).Milliseconds(),
			"error", err,
		)
		return "", err
	}
	slog.Info("llm request completed",
		"model", c.model,
		"duration_ms", time.Since(start).Milliseconds(),
		"prompt_tokens", promptTokens,
		"completion_tokens", completionTokens,
	)
	return content, nil
}

// complete performs the actual HTTP request. withFormat controls whether
// response_format: json_object is included.
func (c *LLMClient) complete(ctx context.Context, system, user string, withFormat bool) (string, int, int, error) {
	reqBody := chatCompletionRequest{
		Model: c.model,
		Messages: []chatMessage{
			{Role: "system", Content: system},
			{Role: "user", Content: user},
		},
		Temperature: c.temperature,
	}
	if withFormat {
		reqBody.ResponseFormat = &responseFormat{Type: "json_object"}
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return "", 0, 0, fmt.Errorf("marshal request: %w", err)
	}

	url := c.baseURL + "/chat/completions"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return "", 0, 0, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.apiKey)

	resp, err := c.http.Do(req)
	if err != nil {
		return "", 0, 0, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	// HTTP 400 fallback: Ollama/vLLM may not support response_format
	if resp.StatusCode == http.StatusBadRequest && withFormat {
		io.Copy(io.Discard, resp.Body) // drain for connection reuse
		slog.Warn("llm request returned 400 with response_format, retrying without",
			"model", c.model,
		)
		return c.complete(ctx, system, user, false)
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
		return "", 0, 0, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	var result chatCompletionResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", 0, 0, fmt.Errorf("decode response: %w", err)
	}

	if len(result.Choices) == 0 {
		return "", 0, 0, fmt.Errorf("response has no choices")
	}

	return result.Choices[0].Message.Content,
		result.Usage.PromptTokens,
		result.Usage.CompletionTokens,
		nil
}

// ParseJSON parses a JSON string into the generic type T.
func ParseJSON[T any](raw string) (T, error) {
	var t T
	if err := json.Unmarshal([]byte(raw), &t); err != nil {
		return t, fmt.Errorf("parse JSON: %w", err)
	}
	return t, nil
}

// --- internal request/response types ---

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type responseFormat struct {
	Type string `json:"type"`
}

type chatCompletionRequest struct {
	Model          string           `json:"model"`
	Messages       []chatMessage    `json:"messages"`
	Temperature    float64          `json:"temperature"`
	ResponseFormat *responseFormat  `json:"response_format,omitempty"`
}

type chatCompletionResponse struct {
	Choices []struct {
		Message chatMessage `json:"message"`
	} `json:"choices"`
	Usage struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
	} `json:"usage"`
}
