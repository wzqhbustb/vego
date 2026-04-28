# Memory Package — Architecture Improvement Plan

## Overview

The `memory/` package (~10,000 lines, 24 files, 87.3% test coverage) implements an Agent Memory Service on top of Vego (HNSW vector database). The following improvements target architectural weaknesses while preserving the existing public API.

---

## 1. Interface Abstractions — Decouple God Object

### Problem

`MemoryStore` holds concrete types for every dependency, making it impossible to swap backends or mock components in isolation.

```go
// Current
type MemoryStore struct {
    db       *vego.DB
    llm      *LLMClient
    embedder *Embedder
    inverted *InvertedIndex
    ...
}
```

### Proposed

```go
type VectorStore interface {
    Collection(name string) (VectorCollection, error)
    Close() error
}

type VectorCollection interface {
    InsertContext(ctx context.Context, doc *vego.Document) error
    InsertBatchContext(ctx context.Context, docs []*vego.Document) error
    GetContext(ctx context.Context, id string) (*vego.Document, error)
    UpdateContext(ctx context.Context, doc *vego.Document) error
    SearchWithFilterContext(ctx context.Context, vec []float32, limit int, filter vego.Filter, opts ...vego.SearchOption) ([]vego.SearchResult, error)
    ForEach(fn func(*vego.Document) bool) error
    Len() int
}

type TextEmbedder interface {
    Embed(ctx context.Context, text string) ([]float32, error)
    Dims() int
}

type LLMService interface {
    CompleteJSON(ctx context.Context, system, user string) (string, error)
}

type FullTextIndex interface {
    SearchContext(ctx context.Context, query string, limit int) ([]ScoredID, error)
    Add(id string, content string)
    Remove(id string)
    RebuildBatch(entries []RebuildEntry)
    Len() int
}

type MemoryStore struct {
    db       VectorStore
    coll     VectorCollection
    llm      LLMService
    embedder TextEmbedder
    inverted FullTextIndex
    ...
}
```

### Impact

- Unit tests can use mock implementations instead of real HTTP servers
- Vector backend can be swapped (pgvector, Elasticsearch, Qdrant)
- LLM provider can be swapped (Anthropic, local Ollama) without touching core logic
- Each component can be tested and benchmarked independently

---

## 2. Composable Search Pipeline

### Problem

The 10-stage hybrid search is a single 150-line function. Stages cannot be reordered, skipped, extended, or individually tested.

```go
// Current: monolithic
func (s *MemoryStore) hybridSearch(ctx context.Context, query string, filter MemoryFilter) ([]Memory, error) {
    // Stage 1-10 all inline
}
```

### Proposed

```go
type SearchStage func(ctx context.Context, state *SearchState) error

type SearchState struct {
    Query      string
    Now        time.Time
    Vector     []float32
    Candidates map[string]Memory
    Scores     map[string]float64
    Results    []Memory
    Config     SearchConfig
    Filter     MemoryFilter
    Limit      int
}

type Pipeline struct {
    stages []SearchStage
}

func NewPipeline(stages ...SearchStage) *Pipeline {
    return &Pipeline{stages: stages}
}

func (p *Pipeline) Execute(ctx context.Context, state *SearchState) error {
    for i, stage := range p.stages {
        if err := ctx.Err(); err != nil {
            return err
        }
        if err := stage(ctx, state); err != nil {
            return fmt.Errorf("stage %d: %w", i, err)
        }
    }
    return nil
}

// Each stage is a standalone function.
var StageTemporalNormalize SearchStage = func(ctx context.Context, s *SearchState) error {
    s.Query = NormalizeTemporalRecallQuery(s.Query, s.Now)
    return nil
}

var StageVectorSearch SearchStage = func(ctx context.Context, s *SearchState) error { ... }
var StageKeywordSearch SearchStage = func(ctx context.Context, s *SearchState) error { ... }
var StageRRFFusion     SearchStage = func(ctx context.Context, s *SearchState) error { ... }
var StageSecondHop     SearchStage = func(ctx context.Context, s *SearchState) error { ... }
var StagePinnedBoost   SearchStage = func(ctx context.Context, s *SearchState) error { ... }
var StageRecencyBoost  SearchStage = func(ctx context.Context, s *SearchState) error { ... }
var StageGapStop       SearchStage = func(ctx context.Context, s *SearchState) error { ... }

func (s *MemoryStore) buildDefaultPipeline() *Pipeline {
    return NewPipeline(
        StageTemporalNormalize,
        StageVectorSearch,
        StageKeywordSearch,
        StageRRFFusion,
        StageSecondHop,
        StagePinnedBoost,
        StageRecencyBoost,
        StageGapStop,
    )
}
```

### Impact

- Each stage is independently testable with a `SearchState` fixture
- Users can inject custom stages (e.g., authorization filter, business-specific boost)
- Pipeline can be configured per-query or per-deployment
- Performance profiling becomes per-stage

---

## 3. Fine-Grained Locking

### Problem

A single `sync.Mutex` serializes all write operations, including long-running batch inserts.

```go
// Current: all writes contend on s.mu
func (s *MemoryStore) StoreRawMessages(...) {
    s.mu.Lock()         // held for entire insert loop
    defer s.mu.Unlock()
    for _, p := range preparedList {
        s.coll.InsertContext(ctx, doc)
        s.inverted.Add(...)
        s.contentHashIndex.Add(...)
    }
}
```

### Proposed

Separate the inverted index and content hash index into a self-locking composite structure. The HNSW vector store already has internal locking.

```go
// IndexState bundles inverted + content-hash indexes with their own mutex.
type IndexState struct {
    mu       sync.RWMutex
    inverted *InvertedIndex
    hashes   *ContentHashIndex
}

func (is *IndexState) Index(id, content string) {
    is.mu.Lock()
    defer is.mu.Unlock()
    is.inverted.Add(id, content)
}

func (is *IndexState) Deindex(id string) {
    is.mu.Lock()
    defer is.mu.Unlock()
    is.inverted.Remove(id)
}

// MemoryStore retains s.mu for atomicity between HNSW insert and index update,
// but the critical section is now much shorter.
func (s *MemoryStore) Store(ctx context.Context, content string, tags []string) (*Memory, error) {
    vec, _ := s.embed(ctx, content)
    mem := &Memory{ID: vego.DocumentID(), Content: content, ...}
    doc, _ := memoryToDoc(mem, vec)

    // HNSW insert + index update are the only serialized section.
    s.mu.Lock()
    if err := s.coll.InsertContext(ctx, doc); err != nil {
        s.mu.Unlock()
        return nil, err
    }
    s.indexes.Index(mem.ID, mem.Content) // internal lock
    s.mu.Unlock()

    return mem, nil
}
```

### Impact

- Reduced contention for multi-agent ingest scenarios
- `StoreRawMessages` can batch HNSW inserts then batch index updates
- 2-5x throughput improvement with 4+ concurrent writers

---

## 4. Grouped Configuration

### Problem

30+ config fields in a flat struct. Sub-configs (LLM, Embedding, Search) are mixed together.

```go
// Current
type Config struct {
    DataDir, Dimension             // storage
    LLMAPIKey, LLMBaseURL, ...     // llm
    EmbedAPIKey, EmbedBaseURL, ... // embedding
    SearchLimit, RRFK, ...         // search (9 fields)
    MaxFacts, MaxConversationRunes // ingest
    DistanceFunc
}
```

### Proposed

```go
type StorageConfig struct {
    DataDir      string
    Dimension    int
    DistanceFunc string
}

type LLMConfig struct {
    APIKey      string
    BaseURL     string
    Model       string
    Temperature float64
}

type EmbedConfig struct {
    APIKey  string
    BaseURL string
    Model   string
    Dims    int
}

type SearchConfig struct {
    Limit             int
    OverFetch         int
    RRFK              float64
    MinScore          float64
    SecondHopGate     float64
    SecondHopWeight   float64
    SecondHopTopN     int
    PinnedBoost       float64
    RecencyBoostWeek  float64
    RecencyBoostMonth float64
    GapStopRatio      float64
}

type IngestConfig struct {
    MaxFacts             int
    MaxConversationRunes int
}

type Config struct {
    Storage   StorageConfig
    LLM       LLMConfig
    Embedding EmbedConfig
    Search    SearchConfig
    Ingest    IngestConfig
}
```

Functional options remain flat for backward compatibility but internally populate sub-structs:

```go
func WithDimension(dim int) Option {
    return func(c *Config) { c.Storage.Dimension = dim }
}
```

A new grouped option is added for bulk configuration:

```go
func WithSearchConfig(sc SearchConfig) Option {
    return func(c *Config) { c.Search = sc }
}
```

### Impact

- Sub-configs are independently validatable (`func (sc *SearchConfig) validate() error`)
- Sub-configs can be passed directly to sub-components (no bridge functions needed)
- Configuration file (YAML/JSON) maps naturally to the nested structure
- `Config.ToLLMConfig()` and `Config.ToEmbedConfig()` bridge methods are removed

---

## 5. Observability Interface

### Problem

`slog` is called directly throughout the codebase. No metrics, no tracing, no way to plug in production monitoring.

```go
// Current: slog scattered inline
slog.WarnContext(ctx, "vector search failed, continuing with keyword-only results", "err", err)
slog.InfoContext(ctx, "llm request completed", "model", c.model, "duration_ms", ...)
slog.Error("embed request failed", "model", e.model, "error", err)
```

### Proposed

```go
type Observer interface {
    // Search
    OnSearchStage(ctx context.Context, stage string, duration time.Duration)
    OnSearchComplete(ctx context.Context, results int, duration time.Duration)

    // Ingest
    OnIngest(ctx context.Context, mode IngestMode, added, updated, deleted, skipped int, duration time.Duration)

    // LLM
    OnLLMCall(ctx context.Context, model string, promptTokens, completionTokens int, duration time.Duration, err error)

    // Embedding
    OnEmbedCall(ctx context.Context, model string, duration time.Duration, err error)

    // Reconcile
    OnReconcileDecision(ctx context.Context, action string, duration time.Duration)

    // Health
    OnError(ctx context.Context, op string, err error)
}

// Default implementation uses slog.
type SlogObserver struct{}

// NoopObserver for tests.
type NoopObserver struct{}
func (NoopObserver) OnSearchStage(context.Context, string, time.Duration) {}
// ...

type MemoryStore struct {
    ...
    observer Observer
}
```

Key call sites become:

```go
func (s *MemoryStore) hybridSearch(ctx context.Context, ...) ([]Memory, error) {
    start := time.Now()
    defer func() { s.observer.OnSearchComplete(ctx, len(results), time.Since(start)) }()

    // Stage 2: Vector search
    stageStart := time.Now()
    vecResults, err := s.vectorSearch(ctx, vec, ...)
    s.observer.OnSearchStage(ctx, "vector_search", time.Since(stageStart))
    ...
}
```

### Impact

- Production deployments inject an OpenTelemetry Observer for metrics + tracing
- All key operations have latency histograms in Prometheus/Grafana
- Error rates trackable per operation
- Testing uses NoopObserver (zero overhead)

---

## 6. Embedding Cache

### Problem

Identical text is re-embedded multiple times within a single ingest cycle. `findCandidates` and `executeAction(ADD)` each call `embed(ctx, fact.Content)` for the same content.

```go
// Reconcile: same content embedded twice
func (s *MemoryStore) Reconcile(...) {
    // Phase 1: findCandidates → embed(fact.Content)  ← first call
    // Phase 2: executeAction(ADD) → embed(mem.Content) ← same text, second call
}
```

### Proposed

```go
type CachedEmbedder struct {
    inner TextEmbedder
    cache *lru.Cache[string, []float32]
    mu    sync.RWMutex
    hits  uint64
    misses uint64
}

func NewCachedEmbedder(inner TextEmbedder, maxEntries int) *CachedEmbedder {
    cache, _ := lru.New[string, []float32](maxEntries)
    return &CachedEmbedder{inner: inner, cache: cache}
}

func (e *CachedEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
    e.mu.RLock()
    if vec, ok := e.cache.Get(text); ok {
        e.mu.RUnlock()
        atomic.AddUint64(&e.hits, 1)
        return vec, nil
    }
    e.mu.RUnlock()

    vec, err := e.inner.Embed(ctx, text)
    if err != nil {
        return nil, err
    }

    e.mu.Lock()
    e.cache.Add(text, vec)
    e.mu.Unlock()
    atomic.AddUint64(&e.misses, 1)
    return vec, nil
}

func (e *CachedEmbedder) Stats() (hits, misses uint64) {
    return atomic.LoadUint64(&e.hits), atomic.LoadUint64(&e.misses)
}
```

Integration:

```go
func Open(path string, opts ...Option) (*MemoryStore, error) {
    ...
    embedder := NewEmbedder(cfg.ToEmbedConfig())
    if cfg.EmbedCacheSize > 0 {
        embedder = NewCachedEmbedder(embedder, cfg.EmbedCacheSize)
    }
    s.embedder = embedder
    ...
}
```

### Impact

- 30-50% reduction in embedding API calls for typical workloads
- Cache hit rate exposed via `Stats()` for monitoring
- Zero overhead when disabled (`EmbedCacheSize = 0`)

---

## 7. Lifecycle Management

### Problem

No health check, no graceful shutdown beyond `Close()`, no way to inspect internal state programmatically.

### Proposed

```go
type StoreStats struct {
    TotalDocuments  int
    ActiveMemories  int
    ArchivedMemories int
    DeletedMemories int
    InvertedDocCount int
    InvertedTermCount int
    HashIndexEntries int
    EmbedCacheHits   uint64
    EmbedCacheMisses uint64
}

func (s *MemoryStore) Stats() StoreStats { ... }

// Ready returns true if the store is fully initialized and accepting requests.
func (s *MemoryStore) Ready() bool {
    return s.db != nil && s.coll != nil && s.embedder != nil
}

// Healthy performs a lightweight liveness check (no external calls).
func (s *MemoryStore) Healthy(ctx context.Context) error {
    if err := ctx.Err(); err != nil {
        return err
    }
    if s.db == nil {
        return errors.New("database not initialized")
    }
    return nil
}
```

### Impact

- Kubernetes readiness/liveness probes can call `Ready()` / `Healthy()`
- `Stats()` enables dashboards and alert thresholds (e.g., "archived ratio > 50%")
- Graceful shutdown: drain in-flight requests before `Close()`

---

## Implementation Roadmap

| Priority | Item | Effort | Rationale |
|----------|------|--------|-----------|
| **P0** | Observability Interface | 1–2 days | Non-negotiable for production. Enables monitoring, alerting, SLO tracking. |
| **P0** | Embedding Cache | 0.5 days | Immediate cost savings on API calls. Low risk, high impact. |
| **P1** | Interface Abstractions | 2–3 days | Unlocks mock testing and backend portability. Backward-compatible API change. |
| **P1** | Grouped Config | 1 day | Cleaner API surface. Sub-configs become independently validatable. |
| **P1** | Lifecycle (Stats/Health) | 0.5 days | Required for container orchestration and operational visibility. |
| **P2** | Composable Pipeline | 3–4 days | Requires API redesign. Benefits users who need custom search stages. |
| **P2** | Fine-Grained Locking | 2–3 days | Only needed when multi-agent concurrent ingest is a confirmed bottleneck. |

### Migration Notes

- Interface abstractions are backward-compatible: the existing concrete types already satisfy the proposed interfaces.
- Grouped Config can be phased: add sub-structs alongside the existing flat fields; deprecate flat accessors over 2 releases.
- Pipeline composition can be introduced as an opt-in parallel API (`Search` vs `SearchWithPipeline`); the current `Search` remains unchanged.
- All changes can be made incrementally without breaking the existing public API.
