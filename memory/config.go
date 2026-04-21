package memory

import (
	"fmt"
)

// Config holds all configuration for the MemoryStore.
// Use functional options (With* functions) to override defaults.
//
// All fields have sensible defaults; a zero-value Config is not usable
// directly — call DefaultConfig() or NewConfig().
type Config struct {
	// Storage
	DataDir   string // 默认 "./vego_memory"
	Dimension int    // 默认 1536

	// LLM
	LLMAPIKey      string
	LLMBaseURL     string
	LLMModel       string
	LLMTemperature float64 // 默认 0.1；0 是 OpenAI 合法值

	// Embedding
	EmbedAPIKey  string
	EmbedBaseURL string
	EmbedModel   string
	EmbedDims    int // 默认 1536

	// Search
	RRFK              float64 // 默认 60.0
	MinScore          float64 // 默认 0.3（相似度 0-1）
	SecondHopGate     float64 // 默认 0.5
	SecondHopWeight   float64 // 默认 0.3
	SecondHopTopN     int     // 默认 3
	PinnedBoost       float64 // 默认 1.5
	RecencyBoostWeek  float64 // 默认 1.05（<=7 天）
	RecencyBoostMonth float64 // 默认 1.02（<=30 天）
	GapStopRatio      float64 // 默认 0.5（0=禁用）

	// Ingest
	MaxFacts             int // 默认 50
	MaxConversationRunes int // 默认 1_000_000

	// Distance function for similarity conversion.
	DistanceFunc string // "cosine" | "l2" | "ip"，默认 "cosine"
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
		RRFK:                 60.0,
		MinScore:             0.3,
		SecondHopGate:        0.5,
		SecondHopWeight:      0.3,
		SecondHopTopN:        3,
		PinnedBoost:          1.5,
		RecencyBoostWeek:     1.05,
		RecencyBoostMonth:    1.02,
		GapStopRatio:         0.5,
		MaxFacts:             50,
		MaxConversationRunes: 1_000_000,
		DistanceFunc:         "cosine",
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
	if c.MinScore < 0 || c.MinScore > 1 {
		return fmt.Errorf("min score must be in [0,1], got %f", c.MinScore)
	}
	if c.GapStopRatio < 0 || c.GapStopRatio > 1 {
		return fmt.Errorf("gap stop ratio must be in [0,1], got %f", c.GapStopRatio)
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
	if c.RecencyBoostWeek < 0 {
		return fmt.Errorf("recency boost week must be >= 0, got %f", c.RecencyBoostWeek)
	}
	if c.RecencyBoostMonth < 0 {
		return fmt.Errorf("recency boost month must be >= 0, got %f", c.RecencyBoostMonth)
	}
	return nil
}

// --- bridge to sub-configs ---

// ToLLMConfig returns an LLMConfig derived from this Config.
func (c *Config) ToLLMConfig() LLMConfig {
	return LLMConfig{
		APIKey:      c.LLMAPIKey,
		BaseURL:     c.LLMBaseURL,
		Model:       c.LLMModel,
		Temperature: c.LLMTemperature,
	}
}

// ToEmbedConfig returns an EmbedConfig derived from this Config.
func (c *Config) ToEmbedConfig() EmbedConfig {
	return EmbedConfig{
		APIKey:  c.EmbedAPIKey,
		BaseURL: c.EmbedBaseURL,
		Model:   c.EmbedModel,
		Dims:    c.EmbedDims,
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

func WithLLMAPIKey(key string) Option {
	return func(c *Config) { c.LLMAPIKey = key }
}

func WithLLMBaseURL(url string) Option {
	return func(c *Config) { c.LLMBaseURL = url }
}

func WithLLMModel(model string) Option {
	return func(c *Config) { c.LLMModel = model }
}

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

func WithEmbedAPIKey(key string) Option {
	return func(c *Config) { c.EmbedAPIKey = key }
}

func WithEmbedBaseURL(url string) Option {
	return func(c *Config) { c.EmbedBaseURL = url }
}

func WithEmbedModel(model string) Option {
	return func(c *Config) { c.EmbedModel = model }
}

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

// WithSearchParams sets search scoring thresholds.
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
