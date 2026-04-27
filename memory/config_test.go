package memory

import (
	"testing"
)

func TestDefaultConfig(t *testing.T) {
	c := DefaultConfig()

	if c.DataDir != "./vego_memory" {
		t.Errorf("DataDir: want ./vego_memory, got %s", c.DataDir)
	}
	if c.Dimension != 1536 {
		t.Errorf("Dimension: want 1536, got %d", c.Dimension)
	}
	if c.LLMBaseURL != "https://api.openai.com/v1" {
		t.Errorf("LLMBaseURL default mismatch: %s", c.LLMBaseURL)
	}
	if c.LLMModel != "gpt-4o-mini" {
		t.Errorf("LLMModel default mismatch: %s", c.LLMModel)
	}
	if c.LLMTemperature != 0.1 {
		t.Errorf("LLMTemperature: want 0.1, got %f", c.LLMTemperature)
	}
	if c.EmbedModel != "text-embedding-3-small" {
		t.Errorf("EmbedModel default mismatch: %s", c.EmbedModel)
	}
	if c.EmbedDims != 1536 {
		t.Errorf("EmbedDims: want 1536, got %d", c.EmbedDims)
	}
	if c.RRFK != 60.0 {
		t.Errorf("RRFK: want 60.0, got %f", c.RRFK)
	}
	if c.SearchLimit != 10 {
		t.Errorf("SearchLimit: want 10, got %d", c.SearchLimit)
	}
	if c.SearchOverFetch != 5 {
		t.Errorf("SearchOverFetch: want 5, got %d", c.SearchOverFetch)
	}
	if c.MinScore != 0.3 {
		t.Errorf("MinScore: want 0.3, got %f", c.MinScore)
	}
	if c.GapStopRatio != 0.5 {
		t.Errorf("GapStopRatio: want 0.5, got %f", c.GapStopRatio)
	}
	if c.DistanceFunc != "cosine" {
		t.Errorf("DistanceFunc: want cosine, got %s", c.DistanceFunc)
	}
	if c.MaxFacts != 50 {
		t.Errorf("MaxFacts: want 50, got %d", c.MaxFacts)
	}
	if c.MaxConversationRunes != 1_000_000 {
		t.Errorf("MaxConversationRunes: want 1000000, got %d", c.MaxConversationRunes)
	}
	if c.PinnedBoost != 1.5 {
		t.Errorf("PinnedBoost: want 1.5, got %f", c.PinnedBoost)
	}
	if c.SecondHopGate != 0.02 {
		t.Errorf("SecondHopGate: want 0.02, got %f", c.SecondHopGate)
	}
	if c.SecondHopWeight != 0.3 {
		t.Errorf("SecondHopWeight: want 0.3, got %f", c.SecondHopWeight)
	}
	if c.SecondHopTopN != 3 {
		t.Errorf("SecondHopTopN: want 3, got %d", c.SecondHopTopN)
	}
	if c.RecencyBoostWeek != 1.05 {
		t.Errorf("RecencyBoostWeek: want 1.05, got %f", c.RecencyBoostWeek)
	}
	if c.RecencyBoostMonth != 1.02 {
		t.Errorf("RecencyBoostMonth: want 1.02, got %f", c.RecencyBoostMonth)
	}
}

func TestNewConfigWithOptions(t *testing.T) {
	c, err := NewConfig(
		WithDataDir("/tmp/mem"),
		WithDimension(768),
		WithLLM("sk-test", "http://localhost:11434/v1", "llama3", 0.0),
		WithEmbedding("sk-embed", "", "bge-m3", 1024),
		WithDistanceFunc("l2"),
		WithSearchParams(0.5),
		WithRRFK(40.0),
		WithSecondHop(0.6, 0.4, 5),
		WithPinnedBoost(2.0),
		WithRecencyBoost(1.1, 1.03),
		WithGapStop(0.3),
		WithIngestParams(100, 500000),
	)
	if err != nil {
		t.Fatalf("NewConfig error: %v", err)
	}

	if c.DataDir != "/tmp/mem" {
		t.Errorf("DataDir: want /tmp/mem, got %s", c.DataDir)
	}
	if c.Dimension != 768 {
		t.Errorf("Dimension: want 768, got %d", c.Dimension)
	}
	if c.LLMAPIKey != "sk-test" {
		t.Errorf("LLMAPIKey mismatch")
	}
	if c.LLMBaseURL != "http://localhost:11434/v1" {
		t.Errorf("LLMBaseURL: want localhost, got %s", c.LLMBaseURL)
	}
	if c.LLMModel != "llama3" {
		t.Errorf("LLMModel: want llama3, got %s", c.LLMModel)
	}
	if c.LLMTemperature != 0.0 {
		t.Errorf("LLMTemperature: want 0.0, got %f", c.LLMTemperature)
	}
	if c.EmbedAPIKey != "sk-embed" {
		t.Errorf("EmbedAPIKey mismatch")
	}
	// Empty baseURL should keep default
	if c.EmbedBaseURL != "https://api.openai.com/v1" {
		t.Errorf("EmbedBaseURL should keep default when empty, got %s", c.EmbedBaseURL)
	}
	if c.EmbedModel != "bge-m3" {
		t.Errorf("EmbedModel: want bge-m3, got %s", c.EmbedModel)
	}
	if c.EmbedDims != 1024 {
		t.Errorf("EmbedDims: want 1024, got %d", c.EmbedDims)
	}
	if c.DistanceFunc != "l2" {
		t.Errorf("DistanceFunc: want l2, got %s", c.DistanceFunc)
	}
	if c.MinScore != 0.5 {
		t.Errorf("MinScore: want 0.5, got %f", c.MinScore)
	}
	if c.RRFK != 40.0 {
		t.Errorf("RRFK: want 40.0, got %f", c.RRFK)
	}
	if c.SecondHopGate != 0.6 {
		t.Errorf("SecondHopGate: want 0.6, got %f", c.SecondHopGate)
	}
	if c.SecondHopWeight != 0.4 {
		t.Errorf("SecondHopWeight: want 0.4, got %f", c.SecondHopWeight)
	}
	if c.SecondHopTopN != 5 {
		t.Errorf("SecondHopTopN: want 5, got %d", c.SecondHopTopN)
	}
	if c.PinnedBoost != 2.0 {
		t.Errorf("PinnedBoost: want 2.0, got %f", c.PinnedBoost)
	}
	if c.RecencyBoostWeek != 1.1 {
		t.Errorf("RecencyBoostWeek: want 1.1, got %f", c.RecencyBoostWeek)
	}
	if c.RecencyBoostMonth != 1.03 {
		t.Errorf("RecencyBoostMonth: want 1.03, got %f", c.RecencyBoostMonth)
	}
	if c.GapStopRatio != 0.3 {
		t.Errorf("GapStopRatio: want 0.3, got %f", c.GapStopRatio)
	}
	if c.MaxFacts != 100 {
		t.Errorf("MaxFacts: want 100, got %d", c.MaxFacts)
	}
	if c.MaxConversationRunes != 500000 {
		t.Errorf("MaxConversationRunes: want 500000, got %d", c.MaxConversationRunes)
	}
}

func TestConfigValidation(t *testing.T) {
	cases := []struct {
		name    string
		opts    []Option
		wantErr string
	}{
		{
			name:    "invalid distance func",
			opts:    []Option{WithDistanceFunc("hamming")},
			wantErr: "invalid distance func \"hamming\", want cosine/l2/ip",
		},
		{
			name:    "zero dimension",
			opts:    []Option{WithDimension(0)},
			wantErr: "dimension must be > 0, got 0",
		},
		{
			name:    "negative embed dims",
			opts:    []Option{WithEmbedding("", "", "", -1)},
			wantErr: "embed dims must be > 0, got -1",
		},
		{
			name:    "min score out of range",
			opts:    []Option{WithSearchParams(1.5)},
			wantErr: "min score must be in [0,1], got 1.500000",
		},
		{
			name:    "gap stop out of range",
			opts:    []Option{WithGapStop(-0.1)},
			wantErr: "gap stop ratio must be in [0,1], got -0.100000",
		},
		{
			name:    "negative rrf k",
			opts:    []Option{WithRRFK(-1)},
			wantErr: "rrf k must be > 0, got -1.000000",
		},
		{
			name:    "temperature too high",
			opts:    []Option{WithLLM("", "", "", 3.0)},
			wantErr: "llm temperature must be in [0,2], got 3.000000",
		},
		{
			name:    "temperature negative",
			opts:    []Option{WithLLM("", "", "", -0.1)},
			wantErr: "llm temperature must be in [0,2], got -0.100000",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewConfig(tc.opts...)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantErr)
			}
			if err.Error() != tc.wantErr {
				t.Errorf("error message mismatch: want %q, got %q", tc.wantErr, err.Error())
			}
		})
	}
}

func TestConfigToLLMConfig(t *testing.T) {
	c, _ := NewConfig(
		WithLLM("sk-abc", "http://ollama.local/v1", "qwen2.5", 0.0),
	)
	lc := c.ToLLMConfig()
	if lc.APIKey != "sk-abc" {
		t.Errorf("APIKey: want sk-abc, got %s", lc.APIKey)
	}
	if lc.BaseURL != "http://ollama.local/v1" {
		t.Errorf("BaseURL mismatch: %s", lc.BaseURL)
	}
	if lc.Model != "qwen2.5" {
		t.Errorf("Model: want qwen2.5, got %s", lc.Model)
	}
	if lc.Temperature != 0.0 {
		t.Errorf("Temperature: want 0.0, got %f", lc.Temperature)
	}
}

func TestConfigToEmbedConfig(t *testing.T) {
	c, _ := NewConfig(
		WithEmbedding("sk-emb", "http://local.embed/v1", "m3e", 768),
	)
	ec := c.ToEmbedConfig()
	if ec.APIKey != "sk-emb" {
		t.Errorf("APIKey mismatch")
	}
	if ec.BaseURL != "http://local.embed/v1" {
		t.Errorf("BaseURL mismatch: %s", ec.BaseURL)
	}
	if ec.Model != "m3e" {
		t.Errorf("Model: want m3e, got %s", ec.Model)
	}
	if ec.Dims != 768 {
		t.Errorf("Dims: want 768, got %d", ec.Dims)
	}
}

func TestConfigApply(t *testing.T) {
	c, _ := NewConfig()
	if c.DataDir != "./vego_memory" {
		t.Fatalf("unexpected initial DataDir: %s", c.DataDir)
	}

	err := c.Apply(WithDataDir("/updated"))
	if err != nil {
		t.Fatalf("Apply error: %v", err)
	}
	if c.DataDir != "/updated" {
		t.Errorf("DataDir after Apply: want /updated, got %s", c.DataDir)
	}

	// Apply with invalid option should fail. Note: Apply is NOT atomic —
	// options are applied sequentially; if validation fails, earlier options
	// have already modified the Config.
	err = c.Apply(WithDistanceFunc("invalid"))
	if err == nil {
		t.Fatal("expected validation error from Apply")
	}
	// The invalid option was still applied before validation failed.
	if c.DistanceFunc != "invalid" {
		t.Errorf("expected partial modification: DistanceFunc should be 'invalid', got %s", c.DistanceFunc)
	}
}

func TestConfigTemperatureZeroValue(t *testing.T) {
	// This is the key test: temperature=0 must be distinguishable from "unset".
	// With functional options, 0 is an explicit value, not a zero-value sentinel.
	c, err := NewConfig(WithLLM("sk", "", "", 0.0))
	if err != nil {
		t.Fatalf("NewConfig with temp=0 error: %v", err)
	}
	if c.LLMTemperature != 0.0 {
		t.Errorf("temperature=0 should be preserved, got %f", c.LLMTemperature)
	}

	// Default config should still have 0.1
	c2 := DefaultConfig()
	if c2.LLMTemperature != 0.1 {
		t.Errorf("default temperature should be 0.1, got %f", c2.LLMTemperature)
	}
}

func TestConfigEmptyStringNoOverride(t *testing.T) {
	// WithLLM/WithEmbedding with empty baseURL/model should not override defaults.
	c, _ := NewConfig(
		WithLLM("key", "", "", 0.5),
		WithEmbedding("key", "", "", 1536),
	)
	if c.LLMBaseURL != "https://api.openai.com/v1" {
		t.Errorf("LLMBaseURL should keep default, got %s", c.LLMBaseURL)
	}
	if c.LLMModel != "gpt-4o-mini" {
		t.Errorf("LLMModel should keep default, got %s", c.LLMModel)
	}
	if c.EmbedBaseURL != "https://api.openai.com/v1" {
		t.Errorf("EmbedBaseURL should keep default, got %s", c.EmbedBaseURL)
	}
	if c.EmbedModel != "text-embedding-3-small" {
		t.Errorf("EmbedModel should keep default, got %s", c.EmbedModel)
	}
}

func TestConfigFineGrainedOptions(t *testing.T) {
	c, err := NewConfig(
		WithLLMAPIKey("sk-fine"),
		WithLLMBaseURL("http://fine.local/v1"),
		WithLLMModel("fine-model"),
		WithLLMTemperature(0.0),
		WithEmbedAPIKey("emb-fine"),
		WithEmbedBaseURL("http://emb.local/v1"),
		WithEmbedModel("emb-model"),
		WithEmbedDims(512),
	)
	if err != nil {
		t.Fatalf("NewConfig fine-grained error: %v", err)
	}
	if c.LLMAPIKey != "sk-fine" {
		t.Errorf("LLMAPIKey mismatch")
	}
	if c.LLMBaseURL != "http://fine.local/v1" {
		t.Errorf("LLMBaseURL mismatch: %s", c.LLMBaseURL)
	}
	if c.LLMModel != "fine-model" {
		t.Errorf("LLMModel mismatch: %s", c.LLMModel)
	}
	if c.LLMTemperature != 0.0 {
		t.Errorf("LLMTemperature: want 0.0, got %f", c.LLMTemperature)
	}
	if c.EmbedAPIKey != "emb-fine" {
		t.Errorf("EmbedAPIKey mismatch")
	}
	if c.EmbedBaseURL != "http://emb.local/v1" {
		t.Errorf("EmbedBaseURL mismatch: %s", c.EmbedBaseURL)
	}
	if c.EmbedModel != "emb-model" {
		t.Errorf("EmbedModel mismatch: %s", c.EmbedModel)
	}
	if c.EmbedDims != 512 {
		t.Errorf("EmbedDims: want 512, got %d", c.EmbedDims)
	}
}

func TestConfigValidationExtended(t *testing.T) {
	cases := []struct {
		name    string
		opts    []Option
		wantErr string
	}{
		{
			name:    "empty data dir",
			opts:    []Option{WithDataDir("")},
			wantErr: "data dir must not be empty",
		},
		{
			name:    "second hop gate out of range",
			opts:    []Option{WithSecondHop(1.5, 0.3, 3)},
			wantErr: "second hop gate must be in [0,1], got 1.500000",
		},
		{
			name:    "second hop weight out of range",
			opts:    []Option{WithSecondHop(0.5, -0.1, 3)},
			wantErr: "second hop weight must be in [0,1], got -0.100000",
		},
		{
			name:    "second hop topN zero",
			opts:    []Option{WithSecondHop(0.5, 0.3, 0)},
			wantErr: "second hop topN must be > 0, got 0",
		},
		{
			name:    "pinned boost non-positive",
			opts:    []Option{WithPinnedBoost(0)},
			wantErr: "pinned boost must be > 0, got 0.000000",
		},
		{
			name:    "max facts zero",
			opts:    []Option{WithIngestParams(0, 100)},
			wantErr: "max facts must be > 0, got 0",
		},
		{
			name:    "max conversation runes zero",
			opts:    []Option{WithIngestParams(10, 0)},
			wantErr: "max conversation runes must be > 0, got 0",
		},
		{
			name:    "negative recency boost week",
			opts:    []Option{WithRecencyBoost(-0.1, 1.0)},
			wantErr: "recency boost week must be >= 0, got -0.100000",
		},
		{
			name:    "negative recency boost month",
			opts:    []Option{WithRecencyBoost(1.0, -0.1)},
			wantErr: "recency boost month must be >= 0, got -0.100000",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewConfig(tc.opts...)
			if err == nil {
				t.Fatalf("expected error %q, got nil", tc.wantErr)
			}
			if err.Error() != tc.wantErr {
				t.Errorf("error message mismatch: want %q, got %q", tc.wantErr, err.Error())
			}
		})
	}
}
