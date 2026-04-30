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
	"sync/atomic"
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
	apiKey             string
	baseURL            string
	model              string
	temperature        float64
	http               *http.Client
	formatNotSupported atomic.Bool // caches 400 response_format failure
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

// CloseIdleConnections closes any idle connections in the underlying
// HTTP client to prevent TCP connection leaks in long-running processes.
func (c *LLMClient) CloseIdleConnections() {
	if c != nil && c.http != nil {
		c.http.CloseIdleConnections()
	}
}

// CompleteJSON sends a chat completion request with system + user prompts
// and returns the assistant's content. It forces JSON output via
// response_format and falls back on HTTP 400 (Ollama/vLLM compatibility).
func (c *LLMClient) CompleteJSON(ctx context.Context, system, user string) (string, error) {
	if c == nil {
		return "", fmt.Errorf("llm client is nil")
	}
	if ctx == nil {
		return "", fmt.Errorf("context must not be nil")
	}
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
	// If we already know the server doesn't support response_format, skip it.
	if withFormat && c.formatNotSupported.Load() {
		return c.complete(ctx, system, user, false)
	}

	var messages []chatMessage
	if system != "" {
		messages = append(messages, chatMessage{Role: "system", Content: system})
	}
	messages = append(messages, chatMessage{Role: "user", Content: user})

	reqBody := chatCompletionRequest{
		Model:       c.model,
		Messages:    messages,
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

	// HTTP 400 fallback: Ollama/vLLM may not support response_format.
	// Read the response body to check whether the error really relates to
	// response_format before permanently caching the failure.
	if resp.StatusCode == http.StatusBadRequest && withFormat {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
		bodyStr := strings.ToLower(string(body))
		if strings.Contains(bodyStr, "response_format") ||
			strings.Contains(bodyStr, "json_object") ||
			strings.Contains(bodyStr, "json_schema") {
			c.formatNotSupported.Store(true)
			slog.Warn("llm server does not support response_format, disabling JSON mode",
				"model", c.model,
			)
			return c.complete(ctx, system, user, false)
		}
		// 400 was for some other reason — treat as a hard error.
		return "", 0, 0, fmt.Errorf("unexpected status 400: %s", bodyStr)
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
