package memory

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestNewLLMClientNil(t *testing.T) {
	if NewLLMClient(LLMConfig{}) != nil {
		t.Error("Expected nil client when APIKey is empty")
	}
}

func TestNewLLMClientDefaults(t *testing.T) {
	c := NewLLMClient(LLMConfig{APIKey: "sk-test"})
	if c == nil {
		t.Fatal("Expected non-nil client")
	}
	if c.baseURL != "https://api.openai.com/v1" {
		t.Errorf("BaseURL default: want %s, got %s", "https://api.openai.com/v1", c.baseURL)
	}
	if c.model != "gpt-4o-mini" {
		t.Errorf("Model default: want %s, got %s", "gpt-4o-mini", c.model)
	}
	if c.temperature != 0.1 {
		t.Errorf("Temperature default: want %f, got %f", 0.1, c.temperature)
	}
}

func TestNewLLMClientCustom(t *testing.T) {
	c := NewLLMClient(LLMConfig{
		APIKey:      "sk-test",
		BaseURL:     "https://custom.example.com/v1/",
		Model:       "gpt-4o",
		Temperature: 0.5,
	})
	if c == nil {
		t.Fatal("Expected non-nil client")
	}
	if c.baseURL != "https://custom.example.com/v1" {
		t.Errorf("BaseURL trim: want %s, got %s", "https://custom.example.com/v1", c.baseURL)
	}
	if c.model != "gpt-4o" {
		t.Errorf("Model: want %s, got %s", "gpt-4o", c.model)
	}
	if c.temperature != 0.5 {
		t.Errorf("Temperature: want %f, got %f", 0.5, c.temperature)
	}
}

func TestCompleteJSONSuccess(t *testing.T) {
	var gotReq chatCompletionRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&gotReq); err != nil {
			t.Errorf("decode request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(chatCompletionResponse{
			Choices: []struct {
				Message chatMessage `json:"message"`
			}{
				{Message: chatMessage{Role: "assistant", Content: `{"answer":"42"}`}},
			},
			Usage: struct {
				PromptTokens     int `json:"prompt_tokens"`
				CompletionTokens int `json:"completion_tokens"`
			}{PromptTokens: 10, CompletionTokens: 5},
		}); err != nil {
			t.Errorf("encode response: %v", err)
		}
	}))
	defer srv.Close()

	client := NewLLMClient(LLMConfig{
		APIKey:  "sk-test",
		BaseURL: srv.URL,
		Model:   "gpt-4o-mini",
	})

	content, err := client.CompleteJSON(context.Background(), "You are a calculator", "What is 40+2?")
	if err != nil {
		t.Fatalf("CompleteJSON failed: %v", err)
	}
	if content != `{"answer":"42"}` {
		t.Errorf("Content: want %q, got %q", `{"answer":"42"}`, content)
	}
	if gotReq.Model != "gpt-4o-mini" {
		t.Errorf("Request model: want gpt-4o-mini, got %s", gotReq.Model)
	}
	if len(gotReq.Messages) != 2 {
		t.Fatalf("Messages count: want 2, got %d", len(gotReq.Messages))
	}
	if gotReq.Messages[0].Role != "system" || gotReq.Messages[0].Content != "You are a calculator" {
		t.Errorf("System message mismatch: %+v", gotReq.Messages[0])
	}
	if gotReq.Messages[1].Role != "user" || gotReq.Messages[1].Content != "What is 40+2?" {
		t.Errorf("User message mismatch: %+v", gotReq.Messages[1])
	}
	if gotReq.ResponseFormat == nil || gotReq.ResponseFormat.Type != "json_object" {
		t.Errorf("ResponseFormat: want json_object, got %+v", gotReq.ResponseFormat)
	}
}

func TestCompleteJSON400Fallback(t *testing.T) {
	callCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		var req chatCompletionRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
		}

		if callCount == 1 {
			if req.ResponseFormat == nil {
				t.Error("First call should have response_format")
			}
			w.WriteHeader(http.StatusBadRequest)
			if _, err := w.Write([]byte(`{"error":"response_format not supported"}`)); err != nil {
				t.Errorf("write response: %v", err)
			}
			return
		}

		if req.ResponseFormat != nil {
			t.Error("Second call should NOT have response_format")
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(chatCompletionResponse{
			Choices: []struct {
				Message chatMessage `json:"message"`
			}{
				{Message: chatMessage{Role: "assistant", Content: `{"result":"ok"}`}},
			},
			Usage: struct {
				PromptTokens     int `json:"prompt_tokens"`
				CompletionTokens int `json:"completion_tokens"`
			}{PromptTokens: 5, CompletionTokens: 3},
		}); err != nil {
			t.Errorf("encode response: %v", err)
		}
	}))
	defer srv.Close()

	client := NewLLMClient(LLMConfig{
		APIKey:  "sk-test",
		BaseURL: srv.URL,
	})

	content, err := client.CompleteJSON(context.Background(), "sys", "user")
	if err != nil {
		t.Fatalf("CompleteJSON failed: %v", err)
	}
	if content != `{"result":"ok"}` {
		t.Errorf("Content: want %q, got %q", `{"result":"ok"}`, content)
	}
	if callCount != 2 {
		t.Errorf("Expected 2 calls, got %d", callCount)
	}
}

func TestCompleteJSON500Error(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`internal error`))
	}))
	defer srv.Close()

	client := NewLLMClient(LLMConfig{
		APIKey:  "sk-test",
		BaseURL: srv.URL,
	})

	_, err := client.CompleteJSON(context.Background(), "sys", "user")
	if err == nil {
		t.Fatal("Expected error for 500 response")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Errorf("Error should mention status 500: %v", err)
	}
}

func TestCompleteJSONNoChoices(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(chatCompletionResponse{
			Choices: []struct {
				Message chatMessage `json:"message"`
			}{},
			Usage: struct {
				PromptTokens     int `json:"prompt_tokens"`
				CompletionTokens int `json:"completion_tokens"`
			}{},
		}); err != nil {
			t.Errorf("encode response: %v", err)
		}
	}))
	defer srv.Close()

	client := NewLLMClient(LLMConfig{
		APIKey:  "sk-test",
		BaseURL: srv.URL,
	})

	_, err := client.CompleteJSON(context.Background(), "sys", "user")
	if err == nil {
		t.Fatal("Expected error for empty choices")
	}
}

func TestParseJSON(t *testing.T) {
	type answer struct {
		Value int `json:"value"`
	}
	got, err := ParseJSON[answer](`{"value":42}`)
	if err != nil {
		t.Fatalf("ParseJSON failed: %v", err)
	}
	if got.Value != 42 {
		t.Errorf("Value: want 42, got %d", got.Value)
	}
}

func TestCompleteJSONContextCancel(t *testing.T) {
	client := NewLLMClient(LLMConfig{APIKey: "sk-test"})
	// Replace with a round-tripper that blocks until context cancellation
	client.http = &http.Client{
		Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			<-req.Context().Done()
			return nil, req.Context().Err()
		}),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := client.CompleteJSON(ctx, "sys", "user")
	if err == nil {
		t.Fatal("Expected error for cancelled context")
	}
	if !strings.Contains(err.Error(), "context") {
		t.Errorf("Error should mention context: %v", err)
	}
}

// roundTripperFunc adapts a function to http.RoundTripper for testing.
type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestParseJSONEmpty(t *testing.T) {
	_, err := ParseJSON[map[string]interface{}]("")
	if err == nil {
		t.Error("Expected error for empty JSON string")
	}
}

func TestParseJSONNull(t *testing.T) {
	got, err := ParseJSON[*struct{ Value int }]("null")
	if err != nil {
		t.Fatalf("ParseJSON null failed: %v", err)
	}
	if got != nil {
		t.Errorf("Expected nil for null JSON, got %v", got)
	}
}

func TestParseJSONInvalid(t *testing.T) {
	_, err := ParseJSON[map[string]interface{}](`not json`)
	if err == nil {
		t.Error("Expected error for invalid JSON")
	}
}
