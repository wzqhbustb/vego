package memory

import (
	"fmt"
	"log/slog"
	"net/http"
)

// Config holds all configuration for the MemoryStore.
// Use functional options (With* functions) to override defaults.
//
// All fields have sensible defaults; a zero-value Config is not usable
// directly — call DefaultConfig() or NewConfig().
type Config struct {
	// Storage
	DataDir   string // Default "./vego_memory"
	Dimension int    // Default 1536

	// LLM
	LLMAPIKey      string
	LLMBaseURL     string
	LLMModel       string
	LLMTemperature float64 // Default 0.1; 0 is a valid value for OpenAI

	// Embedding
	EmbedAPIKey  string
	EmbedBaseURL string
	EmbedModel   string
	EmbedDims    int // Default 1536

	// Search
	SearchLimit       int     // Default 10
	SearchOverFetch   int     // Default 3; overfetch multiplier for SearchWithFilterContext
	RRFK              float64 // Default 60.0
	MinScore          float64 // Default 0.3 (similarity 0-1)
	SecondHopGate     float64 // Default 0.02
	SecondHopWeight   float64 // Default 0.3
	SecondHopTopN     int     // Default 3
	PinnedBoost            float64 // Default 1.5
	RecencyBoostWeek       float64 // Default 1.05 (<=7 days)
	RecencyBoostMonth      float64 // Default 1.02 (<=30 days)
	GapStopRatio           float64 // Default 0.5 (0=disabled)
	DualChannelBonus       float64 // Default 0 (dual-channel hit multiplier bonus; 0=disabled)
	VectorSimilarityWeight float64 // Default 0 (vector similarity weighting; 0=disabled)
	NearDupThreshold       float64 // Default 0 (near-duplicate threshold; 0=disabled; >0 skips LLM)

	// Ingest
	MaxFacts             int // Default 50
	MaxConversationRunes int // Default 1_000_000

	// Input validation
	MaxContentLen int // Default 50_000
	MaxTags       int // Default 20
	MaxBulkSize   int // Default 100

	// Distance function for similarity conversion.
	DistanceFunc string // "cosine" | "l2" | "ip"; default "cosine"

	// Logging
	Logger *slog.Logger // nil means use slog.Default()

	// HTTP
	HTTPRoundTripper http.RoundTripper // nil means use http.DefaultTransport

	// BM25
	BM25K1 float64 // Default 1.2
	BM25B  float64 // Default 0.75

	// Prompts (optional overrides)
	FactExtractionPrompt string // Custom system prompt for fact extraction; empty uses built-in English default
	ReconcilePrompt      string // Custom system prompt for reconcile decisions; empty uses built-in English default
}

// Option is a functional option for Config.
type Option func(*Config)

// DefaultConfig returns a Config with all default values set.
func DefaultConfig() *Config {
	return &Config{
		DataDir:              "./vego_memory",
		Dimension:            1536,
		LLMBaseURL:           "https://api.openai.com/v1",
		LLMModel:             "gpt-4o-mini",
		LLMTemperature:       0.1,
		EmbedBaseURL:         "https://api.openai.com/v1",
		EmbedModel:           "text-embedding-3-small",
		EmbedDims:            1536,
		SearchLimit:          10,
		SearchOverFetch:      3,
		RRFK:                 60.0,
		MinScore:             0.3,
		SecondHopGate:        0.02,
		SecondHopWeight:      0.3,
		SecondHopTopN:        3,
		PinnedBoost:          1.5,
		RecencyBoostWeek:     1.05,
		RecencyBoostMonth:    1.02,
		GapStopRatio:          0.5,
		DualChannelBonus:       0,
		VectorSimilarityWeight: 0,
		NearDupThreshold:       0,
		MaxFacts:              50,
		MaxConversationRunes: 1_000_000,
		MaxContentLen:        50_000,
		MaxTags:              20,
		MaxBulkSize:          100,
		DistanceFunc:         "cosine",
		BM25K1:               1.2,
		BM25B:                0.75,
	}
}

// NewConfig creates a Config with defaults and applies the given options.
func NewConfig(opts ...Option) (*Config, error) {
	c := DefaultConfig()
	for _, opt := range opts {
		opt(c)
	}
	if err := c.validate(); err != nil {
		return nil, err
	}
	return c, nil
}

// Apply applies additional options to an existing Config.
func (c *Config) Apply(opts ...Option) error {
	for _, opt := range opts {
		opt(c)
	}
	return c.validate()
}

func (c *Config) validate() error {
	if c.DataDir == "" {
		return fmt.Errorf("data dir must not be empty")
	}
	switch c.DistanceFunc {
	case "cosine", "l2", "ip":
		// ok
	default:
		return fmt.Errorf("invalid distance func %q, want cosine/l2/ip", c.DistanceFunc)
	}
	if c.Dimension <= 0 {
		return fmt.Errorf("dimension must be > 0, got %d", c.Dimension)
	}
	if c.EmbedDims <= 0 {
		return fmt.Errorf("embed dims must be > 0, got %d", c.EmbedDims)
	}
	if c.Dimension != c.EmbedDims {
		return fmt.Errorf("dimension %d must match embed dims %d", c.Dimension, c.EmbedDims)
	}
	if c.MinScore < 0 || c.MinScore > 1 {
		return fmt.Errorf("min score must be in [0,1], got %f", c.MinScore)
	}
	if c.GapStopRatio < 0 || c.GapStopRatio > 1 {
		return fmt.Errorf("gap stop ratio must be in [0,1], got %f", c.GapStopRatio)
	}
	if c.SearchLimit <= 0 {
		return fmt.Errorf("search limit must be > 0, got %d", c.SearchLimit)
	}
	if c.SearchOverFetch < 1 || c.SearchOverFetch > 20 {
		return fmt.Errorf("search over-fetch must be in [1,20], got %d", c.SearchOverFetch)
	}
	if c.RRFK <= 0 {
		return fmt.Errorf("rrf k must be > 0, got %f", c.RRFK)
	}
	if c.LLMTemperature < 0 || c.LLMTemperature > 2 {
		return fmt.Errorf("llm temperature must be in [0,2], got %f", c.LLMTemperature)
	}
	if c.SecondHopGate < 0 || c.SecondHopGate > 1 {
		return fmt.Errorf("second hop gate must be in [0,1], got %f", c.SecondHopGate)
	}
	if c.SecondHopWeight < 0 || c.SecondHopWeight > 1 {
		return fmt.Errorf("second hop weight must be in [0,1], got %f", c.SecondHopWeight)
	}
	if c.SecondHopTopN <= 0 {
		return fmt.Errorf("second hop topN must be > 0, got %d", c.SecondHopTopN)
	}
	if c.PinnedBoost <= 0 {
		return fmt.Errorf("pinned boost must be > 0, got %f", c.PinnedBoost)
	}
	if c.MaxFacts <= 0 {
		return fmt.Errorf("max facts must be > 0, got %d", c.MaxFacts)
	}
	if c.MaxConversationRunes <= 0 {
		return fmt.Errorf("max conversation runes must be > 0, got %d", c.MaxConversationRunes)
	}
	if c.MaxContentLen <= 0 {
		return fmt.Errorf("max content len must be > 0, got %d", c.MaxContentLen)
	}
	if c.MaxTags <= 0 {
		return fmt.Errorf("max tags must be > 0, got %d", c.MaxTags)
	}
	if c.MaxBulkSize <= 0 {
		return fmt.Errorf("max bulk size must be > 0, got %d", c.MaxBulkSize)
	}
	if c.BM25K1 < 0 {
		return fmt.Errorf("bm25 k1 must be >= 0, got %f", c.BM25K1)
	}
	if c.BM25B < 0 || c.BM25B > 1 {
		return fmt.Errorf("bm25 b must be in [0,1], got %f", c.BM25B)
	}
	if c.RecencyBoostWeek < 0 {
		return fmt.Errorf("recency boost week must be >= 0, got %f", c.RecencyBoostWeek)
	}
	if c.RecencyBoostMonth < 0 {
		return fmt.Errorf("recency boost month must be >= 0, got %f", c.RecencyBoostMonth)
	}
	if c.DualChannelBonus < 0 || c.DualChannelBonus > 1 {
		return fmt.Errorf("dual channel bonus must be in [0,1], got %f", c.DualChannelBonus)
	}
	if c.VectorSimilarityWeight < 0 || c.VectorSimilarityWeight > 5 {
		return fmt.Errorf("vector similarity weight must be in [0,5], got %f", c.VectorSimilarityWeight)
	}
	if c.NearDupThreshold < 0 || c.NearDupThreshold > 1 {
		return fmt.Errorf("near dup threshold must be in [0,1], got %f", c.NearDupThreshold)
	}
	return nil
}

// --- bridge to sub-configs ---

// ToLLMConfig returns an LLMConfig derived from this Config.
func (c *Config) ToLLMConfig() LLMConfig {
	return LLMConfig{
		APIKey:       c.LLMAPIKey,
		BaseURL:      c.LLMBaseURL,
		Model:        c.LLMModel,
		Temperature:  c.LLMTemperature,
		RoundTripper: c.HTTPRoundTripper,
		Logger:       c.Logger,
	}
}

// ToEmbedConfig returns an EmbedConfig derived from this Config.
func (c *Config) ToEmbedConfig() EmbedConfig {
	return EmbedConfig{
		APIKey:       c.EmbedAPIKey,
		BaseURL:      c.EmbedBaseURL,
		Model:        c.EmbedModel,
		Dims:         c.EmbedDims,
		RoundTripper: c.HTTPRoundTripper,
		Logger:       c.Logger,
	}
}

// --- functional options ---

// WithDataDir sets the data directory.
func WithDataDir(dir string) Option {
	return func(c *Config) { c.DataDir = dir }
}

// WithDimension sets the vector dimension.
func WithDimension(dim int) Option {
	return func(c *Config) { c.Dimension = dim }
}

// --- fine-grained LLM options ---

// WithLLMAPIKey sets the LLM API key.
func WithLLMAPIKey(key string) Option {
	return func(c *Config) { c.LLMAPIKey = key }
}

// WithLLMBaseURL sets the LLM base URL (OpenAI-compatible endpoint).
func WithLLMBaseURL(url string) Option {
	return func(c *Config) { c.LLMBaseURL = url }
}

// WithLLMModel sets the LLM model name.
func WithLLMModel(model string) Option {
	return func(c *Config) { c.LLMModel = model }
}

// WithLLMTemperature sets the LLM temperature (0-2). Use 0 for deterministic output.
func WithLLMTemperature(temp float64) Option {
	return func(c *Config) { c.LLMTemperature = temp }
}

// WithLLM sets LLM configuration in one call.
// For incremental changes use the fine-grained WithLLM* options above.
func WithLLM(apiKey, baseURL, model string, temperature float64) Option {
	return func(c *Config) {
		c.LLMAPIKey = apiKey
		if baseURL != "" {
			c.LLMBaseURL = baseURL
		}
		if model != "" {
			c.LLMModel = model
		}
		c.LLMTemperature = temperature
	}
}

// --- fine-grained embedding options ---

// WithEmbedAPIKey sets the embedding API key.
func WithEmbedAPIKey(key string) Option {
	return func(c *Config) { c.EmbedAPIKey = key }
}

// WithEmbedBaseURL sets the embedding API base URL (OpenAI-compatible endpoint).
func WithEmbedBaseURL(url string) Option {
	return func(c *Config) { c.EmbedBaseURL = url }
}

// WithEmbedModel sets the embedding model name.
func WithEmbedModel(model string) Option {
	return func(c *Config) { c.EmbedModel = model }
}

// WithEmbedDims sets the embedding vector dimensions.
func WithEmbedDims(dims int) Option {
	return func(c *Config) { c.EmbedDims = dims }
}

// WithEmbedding sets embedding configuration in one call.
// For incremental changes use the fine-grained WithEmbed* options above.
func WithEmbedding(apiKey, baseURL, model string, dims int) Option {
	return func(c *Config) {
		c.EmbedAPIKey = apiKey
		if baseURL != "" {
			c.EmbedBaseURL = baseURL
		}
		if model != "" {
			c.EmbedModel = model
		}
		c.EmbedDims = dims
	}
}

// WithDistanceFunc sets the distance function for similarity conversion.
func WithDistanceFunc(name string) Option {
	return func(c *Config) { c.DistanceFunc = name }
}

// WithSearchLimit sets the default maximum number of search results.
func WithSearchLimit(limit int) Option {
	return func(c *Config) { c.SearchLimit = limit }
}

// WithSearchOverFetch sets the over-fetch multiplier for
// SearchWithFilterContext. Higher values reduce repeated HNSW searches
// under high archive rates, at the cost of a larger single search.
func WithSearchOverFetch(overFetch int) Option {
	return func(c *Config) { c.SearchOverFetch = overFetch }
}

// WithSearchParams sets the minimum similarity score threshold (0-1).
func WithSearchParams(minScore float64) Option {
	return func(c *Config) { c.MinScore = minScore }
}

// WithRRFK sets the RRF fusion parameter K.
func WithRRFK(k float64) Option {
	return func(c *Config) { c.RRFK = k }
}

// WithSecondHop sets second-hop expansion parameters.
func WithSecondHop(gate, weight float64, topN int) Option {
	return func(c *Config) {
		c.SecondHopGate = gate
		c.SecondHopWeight = weight
		c.SecondHopTopN = topN
	}
}

// WithPinnedBoost sets the pinned memory score multiplier.
func WithPinnedBoost(boost float64) Option {
	return func(c *Config) { c.PinnedBoost = boost }
}

// WithRecencyBoost sets recency score multipliers. Use 1.0 to disable.
func WithRecencyBoost(weekMultiplier, monthMultiplier float64) Option {
	return func(c *Config) {
		c.RecencyBoostWeek = weekMultiplier
		c.RecencyBoostMonth = monthMultiplier
	}
}

// WithGapStop sets the gap-stop ratio. Use 0 to disable.
func WithGapStop(ratio float64) Option {
	return func(c *Config) { c.GapStopRatio = ratio }
}

// WithIngestParams sets ingestion limits.
func WithIngestParams(maxFacts, maxConversationRunes int) Option {
	return func(c *Config) {
		c.MaxFacts = maxFacts
		c.MaxConversationRunes = maxConversationRunes
	}
}

// WithMaxContentLen sets the maximum allowed content length (bytes).
func WithMaxContentLen(n int) Option {
	return func(c *Config) { c.MaxContentLen = n }
}

// WithMaxTags sets the maximum number of tags per memory.
func WithMaxTags(n int) Option {
	return func(c *Config) { c.MaxTags = n }
}

// WithMaxBulkSize sets the maximum number of items in a batch operation.
func WithMaxBulkSize(n int) Option {
	return func(c *Config) { c.MaxBulkSize = n }
}

// WithDualChannelBonus sets the dual-channel hit score multiplier bonus.
// When a result appears in both vector and keyword search, its RRF score
// is multiplied by (1 + bonus). Default is 0 (disabled).
func WithDualChannelBonus(bonus float64) Option {
	return func(c *Config) { c.DualChannelBonus = bonus }
}

// WithVectorSimilarityWeight sets the weight for raw vector similarity in
// the final score. Each result's RRF score is multiplied by
// (1 + vecSimilarity * weight). Default is 0 (disabled).
func WithVectorSimilarityWeight(weight float64) Option {
	return func(c *Config) { c.VectorSimilarityWeight = weight }
}

// WithNearDupThreshold sets the near-duplicate threshold for Reconcile.
// When the top candidate's vector similarity exceeds this threshold,
// the fact is treated as a duplicate and skipped (NOOP) without calling
// the LLM. Use 0 to disable. Range: [0,1].
func WithNearDupThreshold(threshold float64) Option {
	return func(c *Config) { c.NearDupThreshold = threshold }
}

// WithFactExtractionPrompt sets a custom system prompt for LLM fact extraction.
// Use this to localize prompts or tune extraction behavior for your domain.
// If empty, a built-in English prompt is used.
func WithFactExtractionPrompt(prompt string) Option {
	return func(c *Config) { c.FactExtractionPrompt = prompt }
}

// WithReconcilePrompt sets a custom system prompt for reconcile decisions.
// Use this to localize prompts or tune merge/replace logic for your domain.
// If empty, a built-in English prompt is used.
func WithReconcilePrompt(prompt string) Option {
	return func(c *Config) { c.ReconcilePrompt = prompt }
}

// WithLogger sets the structured logger for the MemoryStore.
// Pass nil to use slog.Default(). The logger is used for all
// internal logging (index rebuilds, crash recovery, LLM/embed
// request telemetry, and migration events).
func WithLogger(logger *slog.Logger) Option {
	return func(c *Config) { c.Logger = logger }
}

// WithHTTPRoundTripper sets a custom http.RoundTripper for both
// LLM and embedding HTTP clients. Use this to configure TLS
// certificates, proxies, or connection pooling in enterprise
// environments.
func WithHTTPRoundTripper(rt http.RoundTripper) Option {
	return func(c *Config) { c.HTTPRoundTripper = rt }
}

// WithBM25Params sets the BM25 tuning parameters K1 and B.
// K1 controls term-frequency saturation (typical range [0,3], default 1.2).
// B controls document-length normalization (range [0,1], default 0.75).
func WithBM25Params(k1, b float64) Option {
	return func(c *Config) {
		c.BM25K1 = k1
		c.BM25B = b
	}
}
