package memory

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"sort"
	"sync"
	"time"

	hnsw "github.com/wzqhbustb/vego/index"
	vego "github.com/wzqhbustb/vego/vego"
	"golang.org/x/sync/semaphore"
)

// testWorkerPanicHook is set by tests to exercise the panic-recovery path
// in rebuildIndexes workers. It is called after a successful docToMemory
// (before tokenize) from multiple concurrent worker goroutines.
//
// Callers MUST provide their own synchronization (e.g., sync.Mutex) if the
// hook accesses shared state. The hook is called once per document, so
// limit side effects (e.g., panic only on the first invocation) to avoid
// skipping all documents.
//
// Parallel tests MUST NOT set this hook concurrently — it is package-level
// and not safe for concurrent assignment. Use t.Cleanup to restore it to nil.
var testWorkerPanicHook func()

// ----------------------------------------------------------------------
// ContentHashIndex
// ----------------------------------------------------------------------

// ContentHashIndex tracks content hashes per session for deduplication
// of raw session messages (ModeRaw).
type ContentHashIndex struct {
	mu     sync.RWMutex
	index  map[string]string // key="sessionID:hash" → memoryID
	maxSeq map[string]int    // sessionID → current max Seq
}

// NewContentHashIndex creates an empty ContentHashIndex.
func NewContentHashIndex() *ContentHashIndex {
	return &ContentHashIndex{
		index:  make(map[string]string),
		maxSeq: make(map[string]int),
	}
}

// Has reports whether the given session already contains this content hash.
func (idx *ContentHashIndex) Has(sessionID, hash string) bool {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	_, ok := idx.index[sessionID+":"+hash]
	return ok
}

// Add records a content hash and updates the session's max Seq.
func (idx *ContentHashIndex) Add(sessionID, hash, memoryID string, seq int) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.index[sessionID+":"+hash] = memoryID
	if seq > idx.maxSeq[sessionID] {
		idx.maxSeq[sessionID] = seq
	}
}

// MaxSeq returns the highest Seq seen for the session, or 0 if none.
func (idx *ContentHashIndex) MaxSeq(sessionID string) int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.maxSeq[sessionID]
}

// Clear removes all entries.
func (idx *ContentHashIndex) Clear() {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.index = make(map[string]string)
	idx.maxSeq = make(map[string]int)
}

// RebuildBatch inserts multiple entries in a single locked operation.
type HashIndexEntry struct {
	SessionID string
	Hash      string
	MemoryID  string
	Seq       int
}

func (idx *ContentHashIndex) RebuildBatch(entries []HashIndexEntry) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if len(idx.index) != 0 {
		return fmt.Errorf("RebuildBatch called on non-empty index: caller must Clear() first")
	}
	if len(entries) == 0 {
		return nil
	}

	// Pre-size maps to avoid repeated rehashing during bulk insert.
	idx.index = make(map[string]string, len(entries))
	idx.maxSeq = make(map[string]int, len(entries)/10+1)

	for _, e := range entries {
		idx.index[e.SessionID+":"+e.Hash] = e.MemoryID
		if e.Seq > idx.maxSeq[e.SessionID] {
			idx.maxSeq[e.SessionID] = e.Seq
		}
	}
	return nil
}

// ----------------------------------------------------------------------
// MemoryStore
// ----------------------------------------------------------------------

// MemoryStore is the main API for the agent memory service.
// It wraps a Vego DB with LLM, embedding, and full-text indexing.
type MemoryStore struct {
	db               *vego.DB
	coll             *vego.Collection
	llm              *LLMClient
	embedder         *Embedder
	inverted         *InvertedIndex
	contentHashIndex *ContentHashIndex
	config           *Config
	mu               sync.Mutex           // guards all write operations (Store/Update/Delete/Bootstrap/StoreBatch)
	llmSem           *semaphore.Weighted  // limits concurrent LLM calls in Reconcile (weight=1 = serialized)
}

// Open opens or creates a MemoryStore.
// The database path is determined as follows:
//   1. If opts includes WithDataDir with a non-default value, that value is used.
//   2. Otherwise, the path argument is used.
//   3. If both are empty, falls back to the default DataDir.
func Open(path string, opts ...Option) (*MemoryStore, error) {
	cfg, err := NewConfig(opts...)
	if err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	// Prefer an explicitly-set DataDir; otherwise use the path argument.
	dbPath := path
	if cfg.DataDir != "" && cfg.DataDir != DefaultConfig().DataDir {
		dbPath = cfg.DataDir
	}
	if dbPath == "" {
		dbPath = cfg.DataDir
	}

	vegoOpts := []vego.Option{vego.WithDimension(cfg.Dimension)}
	// Translate string distance func to Vego option.
	switch cfg.DistanceFunc {
	case "cosine":
		vegoOpts = append(vegoOpts, vego.WithDistanceFunc(hnsw.CosineDistance))
	case "l2":
		vegoOpts = append(vegoOpts, vego.WithDistanceFunc(hnsw.L2Distance))
	case "ip":
		vegoOpts = append(vegoOpts, vego.WithDistanceFunc(hnsw.InnerProductDistance))
	}

	db, err := vego.Open(dbPath, vegoOpts...)
	if err != nil {
		return nil, fmt.Errorf("open vego db: %w", err)
	}

	coll, err := db.Collection("memories")
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("get collection: %w", err)
	}

	s := &MemoryStore{
		db:               db,
		coll:             coll,
		llm:              NewLLMClient(cfg.ToLLMConfig()),
		embedder:         NewEmbedder(cfg.ToEmbedConfig()),
		inverted:         NewInvertedIndex(),
		contentHashIndex: NewContentHashIndex(),
		config:           cfg,
		llmSem:           semaphore.NewWeighted(1),
	}

	if err := s.rebuildIndexes(); err != nil {
		_ = s.Close()
		return nil, fmt.Errorf("rebuild indexes: %w", err)
	}

	return s, nil
}

// Close closes the MemoryStore and its underlying database.
// It also releases idle HTTP connections held by the LLM and embedding clients.
func (s *MemoryStore) Close() error {
	if s == nil {
		return nil
	}
	if s.llm != nil {
		s.llm.CloseIdleConnections()
	}
	if s.embedder != nil {
		s.embedder.CloseIdleConnections()
	}
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// Store creates a new memory.
func (s *MemoryStore) Store(ctx context.Context, content string, tags []string) (*Memory, error) {
	vec, err := s.embed(ctx, content)
	if err != nil {
		return nil, fmt.Errorf("embed: %w", err)
	}

	mem := &Memory{
		ID:         vego.DocumentID(),
		Content:    content,
		MemoryType: TypeInsight,
		State:      StateActive,
		Tags:       append([]string(nil), tags...),
		Version:    1,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}

	doc, err := memoryToDoc(mem, vec)
	if err != nil {
		return nil, fmt.Errorf("convert to doc: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.coll.InsertContext(ctx, doc); err != nil {
		return nil, fmt.Errorf("insert: %w", err)
	}

	s.inverted.Add(mem.ID, mem.Content)
	return mem, nil
}

// Get retrieves a memory by ID.
func (s *MemoryStore) Get(ctx context.Context, id string) (*Memory, error) {
	doc, err := s.coll.GetContext(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("get: %w", err)
	}
	return docToMemory(doc)
}

// getWithoutVector retrieves a memory without reading the vector from
// column storage. Used by hybrid search to cheaply resolve keyword-only
// candidates that need Content/State/Type/UpdatedAt but not the vector.
func (s *MemoryStore) getWithoutVector(ctx context.Context, id string) (*Memory, error) {
	doc, err := s.coll.GetDocumentWithoutVector(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("get: %w", err)
	}
	return docToMemory(doc)
}

// Update updates a memory's content using Archive-and-Create.
// The old memory is archived (state=archived, superseded_by=newID)
// and a new memory with a fresh ID is created.
func (s *MemoryStore) Update(ctx context.Context, id, content string, tags []string) (*Memory, error) {
	return s.update(ctx, id, content, tags, nil)
}

// update is the internal implementation of Update.  If overlay is non-nil,
// its key-value pairs are merged into newMem.Metadata before the atomic
// archive-and-create, ensuring the merge happens under s.mu and eliminating
// the race window between Insert and the metadata patch.
func (s *MemoryStore) update(ctx context.Context, id, content string, tags []string, overlay map[string]interface{}) (*Memory, error) {
	// Fetch old memory.
	oldDoc, err := s.coll.GetContext(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("get old memory: %w", err)
	}
	oldMem, err := docToMemory(oldDoc)
	if err != nil {
		return nil, fmt.Errorf("decode old memory: %w", err)
	}

	if oldMem.State != StateActive {
		return nil, fmt.Errorf("cannot update memory %s: state is %s", id, oldMem.State)
	}

	// Embed new content.
	vec, err := s.embed(ctx, content)
	if err != nil {
		return nil, fmt.Errorf("embed: %w", err)
	}

	newMem := &Memory{
		ID:         vego.DocumentID(),
		Content:    content,
		MemoryType: oldMem.MemoryType,
		State:      StateActive,
		Tags:       append([]string(nil), tags...),
		AgentID:    oldMem.AgentID,
		SessionID:  oldMem.SessionID,
		Seq:        oldMem.Seq,
		Version:    oldMem.Version + 1,
		Metadata:   copyMap(oldMem.Metadata),
		CreatedAt:  oldMem.CreatedAt,
		UpdatedAt:  time.Now(),
	}

	// Merge overlay metadata under the same lock as archiveAndCreate.
	if overlay != nil {
		if newMem.Metadata == nil {
			newMem.Metadata = make(map[string]interface{}, len(overlay))
		}
		for k, v := range overlay {
			newMem.Metadata[k] = v
		}
	}

	if err := s.archiveAndCreate(ctx, id, newMem, vec); err != nil {
		return nil, err
	}
	return newMem, nil
}

// Delete soft-deletes a memory: its state becomes "deleted" but the record
// remains queryable by ID.
func (s *MemoryStore) Delete(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	doc, err := s.coll.GetContext(ctx, id)
	if err != nil {
		return fmt.Errorf("get: %w", err)
	}

	mem, err := docToMemory(doc)
	if err != nil {
		return fmt.Errorf("decode: %w", err)
	}

	// Prevent overwriting SupersededBy/PreviousID chains of
	// already-archived or already-deleted memories.
	if mem.State != StateActive {
		return fmt.Errorf("cannot delete memory %s: state is %s", id, mem.State)
	}

	mem.State = StateDeleted
	mem.UpdatedAt = time.Now()

	updatedDoc, err := memoryToDoc(mem, doc.Vector)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	if err := s.coll.UpdateContext(ctx, updatedDoc); err != nil {
		return fmt.Errorf("update state: %w", err)
	}

	s.inverted.Remove(id)
	return nil
}

// ----------------------------------------------------------------------
// Search
// ----------------------------------------------------------------------

// SearchOption customizes Search behavior.
type SearchOption func(*searchConfig)

type searchConfig struct {
	limit    int
	minScore float64
	hybrid   bool
	filter   MemoryFilter
}

// Limit sets the maximum number of search results for a single call.
func Limit(n int) SearchOption {
	return func(c *searchConfig) { c.limit = n }
}

// MinScore sets the minimum similarity threshold (0-1) for search results.
func MinScore(min float64) SearchOption {
	return func(c *searchConfig) { c.minScore = min }
}

// EnableHybrid controls whether hybrid search (vector + BM25 + RRF) is used.
// When false, falls back to pure vector search.  Default is true.
func EnableHybrid(enabled bool) SearchOption {
	return func(c *searchConfig) { c.hybrid = enabled }
}

// WithFilter injects additional filter criteria (tags, type, agent, session).
func WithFilter(f MemoryFilter) SearchOption {
	return func(c *searchConfig) { c.filter = f }
}

// Search performs hybrid search over active memories.
// By default it runs the full 10-stage pipeline (vector + BM25 + RRF + boosts).
// Pass EnableHybrid(false) to fall back to pure vector search.
func (s *MemoryStore) Search(ctx context.Context, query string, opts ...SearchOption) ([]Memory, error) {
	sc := &searchConfig{
		hybrid:   true,
		minScore: -1, // sentinel: -1 means not set; [0,1] are valid
	}
	for _, opt := range opts {
		opt(sc)
	}

	// Build MemoryFilter from option overrides.
	mf := sc.filter
	mf.Query = query
	if sc.limit > 0 {
		mf.Limit = sc.limit
		mf.LimitSet = true
	}
	if !mf.LimitSet {
		mf.Limit = s.config.SearchLimit
	}
	if sc.minScore >= 0 {
		mf.MinScore = sc.minScore
		mf.MinScoreSet = true
	}
	if !mf.MinScoreSet {
		mf.MinScore = s.config.MinScore
	}

	if !sc.hybrid {
		return s.pureVectorSearch(ctx, query, mf)
	}
	return s.hybridSearch(ctx, query, mf)
}

// pureVectorSearch is the fallback path when hybrid search is disabled.
func (s *MemoryStore) pureVectorSearch(ctx context.Context, query string, filter MemoryFilter) ([]Memory, error) {
	now := time.Now()
	normalizedQuery := NormalizeTemporalRecallQuery(query, now)
	vec, err := s.embed(ctx, normalizedQuery)
	if err != nil {
		return nil, fmt.Errorf("embed query: %w", err)
	}

	limit := filter.Limit
	if limit <= 0 {
		limit = s.config.SearchLimit
	}
	minScore := filter.MinScore
	if !filter.MinScoreSet {
		minScore = s.config.MinScore
	}

	// Over-fetch to compensate for post-search filtering (matchesFilter + GapStop),
	// consistent with hybridSearch's limit*3 strategy.
	results, err := s.vectorSearch(ctx, vec, limit*3, minScore)
	if err != nil {
		if errors.Is(err, hnsw.ErrEmptyIndex) {
			return nil, nil
		}
		return nil, fmt.Errorf("vector search: %w", err)
	}

	// Apply post-processing filters (same as hybrid path).
	var filtered []Memory
	for _, m := range results {
		if matchesFilter(m, filter) {
			filtered = append(filtered, m)
		}
	}
	results = filtered

	// Sort by score descending for consistent ordering before GapStop.
	sort.Slice(results, func(i, j int) bool {
		if results[i].Score != results[j].Score {
			return results[i].Score > results[j].Score
		}
		return results[i].UpdatedAt.After(results[j].UpdatedAt)
	})

	results = applyGapStop(results, s.config.GapStopRatio)

	if filter.Offset > 0 {
		if filter.Offset >= len(results) {
			results = results[:0]
		} else {
			results = results[filter.Offset:]
		}
	}
	if limit > 0 && limit < len(results) {
		results = results[:limit]
	}

	populateRelativeAge(results, now)

	for i := range results {
		results[i].Content = TemporalRecallProjection(results[i].Content, results[i].Metadata, now)
	}
	return results, nil
}

// ----------------------------------------------------------------------
// Batch / Bootstrap
// ----------------------------------------------------------------------

// StoreItem is a single item for StoreBatch.
type StoreItem struct {
	Content string
	Tags    []string
}

// StoreBatch stores multiple memories in one batch.
// Embedding calls run concurrently (up to 4 in parallel) to reduce latency.
func (s *MemoryStore) StoreBatch(ctx context.Context, items []StoreItem) ([]Memory, error) {
	if len(items) == 0 {
		return nil, nil
	}

	// Phase 1: embed all items concurrently with bounded parallelism.
	vecs := make([][]float32, len(items))
	errs := make([]error, len(items))

	var wg sync.WaitGroup
	embedSem := make(chan struct{}, 4)
	for i, item := range items {
		wg.Add(1)
		go func(idx int, content string) {
			defer wg.Done()
			embedSem <- struct{}{}
			defer func() { <-embedSem }()
			vecs[idx], errs[idx] = s.embed(ctx, content)
		}(i, item.Content)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			return nil, fmt.Errorf("embed item %d: %w", i, err)
		}
	}

	// Phase 2: build memories and documents.
	mems := make([]Memory, len(items))
	docs := make([]*vego.Document, len(items))

	for i, item := range items {
		mem := &Memory{
			ID:        vego.DocumentID(),
			Content:   item.Content,
			State:     StateActive,
			Tags:      append([]string(nil), item.Tags...),
			Version:   1,
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}
		mems[i] = *mem

		doc, err := memoryToDoc(mem, vecs[i])
		if err != nil {
			return nil, fmt.Errorf("convert item %d: %w", i, err)
		}
		docs[i] = doc
	}

	// Phase 3: batch insert under lock.
	s.mu.Lock()
	defer s.mu.Unlock()

	// Pre-validate at application layer so that validation failures
	// identify the specific item rather than surfacing a generic
	// Vego error with no per-document context.
	for i, doc := range docs {
		if doc == nil {
			return nil, fmt.Errorf("item %d: document is nil", i)
		}
		if doc.ID == "" {
			return nil, fmt.Errorf("item %d: document ID is empty", i)
		}
		if len(doc.Vector) == 0 {
			return nil, fmt.Errorf("item %d (%s): vector is empty", i, doc.ID)
		}
		if len(doc.Vector) != s.config.Dimension {
			return nil, fmt.Errorf("item %d (%s): vector dimension mismatch: expected %d, got %d",
				i, doc.ID, s.config.Dimension, len(doc.Vector))
		}
	}

	// InsertBatchContext is all-or-nothing: Vego validates all docs first,
	// then inserts them one-by-one into HNSW. If any insertion fails, no
	// documents are persisted. The pre-validation above catches structural
	// problems before reaching Vego, so errors from this call are typically
	// storage-layer issues (disk full, corruption).
	if err := s.coll.InsertBatchContext(ctx, docs); err != nil {
		return nil, fmt.Errorf("insert batch (%d items): %w", len(docs), err)
	}

	for i := range mems {
		s.inverted.Add(mems[i].ID, mems[i].Content)
	}

	return mems, nil
}

// Bootstrap inserts pre-built memories (e.g., from file import).
// If a memory has a non-empty Vector field, it is used directly;
// otherwise the embedder is invoked.
func (s *MemoryStore) Bootstrap(ctx context.Context, memories []*Memory) error {
	if len(memories) == 0 {
		return nil
	}

	docs := make([]*vego.Document, len(memories))
	for i, mem := range memories {
		if mem == nil {
			return fmt.Errorf("memory %d is nil", i)
		}
		// Apply defaults for fields not set by the caller (consistent with
		// Store / StoreBatch which default MemoryType to TypeInsight).
		if mem.State == "" {
			mem.State = StateActive
		}
		if mem.MemoryType == "" {
			mem.MemoryType = TypeInsight
		}
		if mem.Version == 0 {
			mem.Version = 1
		}
		if mem.CreatedAt.IsZero() {
			mem.CreatedAt = time.Now()
		}
		if mem.UpdatedAt.IsZero() {
			mem.UpdatedAt = time.Now()
		}
		var vec []float32
		var err error
		if len(mem.Vector) > 0 {
			vec = mem.Vector
		} else {
			vec, err = s.embed(ctx, mem.Content)
			if err != nil {
				return fmt.Errorf("embed memory %d: %w", i, err)
			}
		}
		doc, err := memoryToDoc(mem, vec)
		if err != nil {
			return fmt.Errorf("convert memory %d: %w", i, err)
		}
		docs[i] = doc
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Pre-validate before calling InsertBatchContext so that
	// validation failures identify the specific item.
	for i, doc := range docs {
		if doc == nil {
			return fmt.Errorf("bootstrap item %d (%s): document is nil",
				i, memories[i].ID)
		}
		if doc.ID == "" {
			return fmt.Errorf("bootstrap item %d: document ID is empty", i)
		}
		if len(doc.Vector) == 0 {
			return fmt.Errorf("bootstrap item %d (%s): vector is empty", i, doc.ID)
		}
		if len(doc.Vector) != s.config.Dimension {
			return fmt.Errorf("bootstrap item %d (%s): vector dimension mismatch: expected %d, got %d",
				i, doc.ID, s.config.Dimension, len(doc.Vector))
		}
	}

	if err := s.coll.InsertBatchContext(ctx, docs); err != nil {
		return fmt.Errorf("insert batch (%d items): %w", len(docs), err)
	}

	for _, mem := range memories {
		s.inverted.Add(mem.ID, mem.Content)
	}
	return nil
}

// ----------------------------------------------------------------------
// Internal helpers
// ----------------------------------------------------------------------

func (s *MemoryStore) embed(ctx context.Context, text string) ([]float32, error) {
	if s.embedder == nil {
		return nil, fmt.Errorf("embedder not configured")
	}
	return s.embedder.Embed(ctx, text)
}

func (s *MemoryStore) embedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	if s.embedder == nil {
		return nil, fmt.Errorf("embedder not configured")
	}
	return s.embedder.EmbedBatch(ctx, texts)
}

// rebuildIndexes rebuilds the inverted index and ContentHashIndex from
// persisted documents. It also runs crash recovery.
func (s *MemoryStore) rebuildIndexes() error {
	// Phase 1: Collect all document pointers (fast pointer copies under RLock).
	var docs []*vego.Document
	err := s.coll.ForEach(func(doc *vego.Document) bool {
		docs = append(docs, doc)
		return true
	})
	if err != nil {
		return err
	}

	// Phase 2: Parallel decode + tokenize.
	type processed struct {
		doc           *vego.Document
		memory        *Memory
		terms         []string
		isOrphan      bool
		hasPreviousID bool
	}

	numWorkers := runtime.GOMAXPROCS(0)
	if numWorkers > 8 {
		numWorkers = 8
	}

	docCh := make(chan *vego.Document, numWorkers*4)
	resultCh := make(chan processed, numWorkers*4)
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for doc := range docCh {
				func() {
					// Prevent a panic in docToMemory/tokenize from hanging the pipeline.
					defer func() {
						if r := recover(); r != nil {
							slog.Error("worker panic during rebuild", "id", doc.ID, "recover", r)
							resultCh <- processed{doc: doc} // sentinel: consumer skips nil memory
						}
					}()
					m, err := docToMemory(doc)
					if err != nil {
						slog.Warn("skip corrupt document during rebuild", "id", doc.ID, "err", err)
						return
					}
					// Test hook: if set, panics to exercise the recover path.
					if testWorkerPanicHook != nil {
						testWorkerPanicHook()
					}
					var terms []string
					if m.State == StateActive {
						terms = tokenize(m.Content)
					}
					resultCh <- processed{
						doc:           doc,
						memory:        m,
						terms:         terms,
						isOrphan:      m.State == StateActive && m.SupersededBy != "",
						hasPreviousID: m.State == StateActive && m.PreviousID != "",
					}
				}()
			}
		}()
	}

	go func() {
		for _, doc := range docs {
			docCh <- doc
		}
		close(docCh)
	}()

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// Phase 3: Serial batch insert + orphan collection.
	var orphans []*vego.Document
	previousIDSet := make(map[string]struct{})
	// Pre-allocate to avoid repeated slice growth during append.
	invertedEntries := make([]RebuildEntry, 0, len(docs))
	hashEntries := make([]HashIndexEntry, 0, len(docs)/10)

	for p := range resultCh {
		m := p.memory
		if m == nil {
			slog.Warn("rebuildIndexes: skipped document due to worker panic", "id", p.doc.ID)
			continue
		}
		if p.isOrphan {
			orphans = append(orphans, p.doc)
			// Orphan will be archived in Phase 4, but its ContentHash must still
			// be indexed to prevent duplicate storage of the same session message
			// before the next Open() rebuild.
			if m.MemoryType == TypeSession && m.ContentHash != "" {
				hashEntries = append(hashEntries, HashIndexEntry{
					SessionID: m.SessionID,
					Hash:      m.ContentHash,
					MemoryID:  m.ID,
					Seq:       m.Seq,
				})
			}
			continue
		}
		if p.hasPreviousID {
			previousIDSet[m.PreviousID] = struct{}{}
		}
		if m.State == StateActive {
			invertedEntries = append(invertedEntries, RebuildEntry{ID: m.ID, Terms: p.terms})
		}
		// Index ALL TypeSession memories for ContentHash deduplication,
		// regardless of state, to prevent re-storing archived/deleted messages.
		if m.MemoryType == TypeSession && m.ContentHash != "" {
			hashEntries = append(hashEntries, HashIndexEntry{
				SessionID: m.SessionID,
				Hash:      m.ContentHash,
				MemoryID:  m.ID,
				Seq:       m.Seq,
			})
		}
	}

	// Release cloned document references so GC can reclaim ~50 MB of Vector
	// data before Phase 4/5 crash recovery (which does not need them).
	docs = nil

	if err := s.inverted.RebuildBatch(invertedEntries); err != nil {
		return fmt.Errorf("rebuild inverted index: %w", err)
	}
	if err := s.contentHashIndex.RebuildBatch(hashEntries); err != nil {
		return fmt.Errorf("rebuild hash index: %w", err)
	}

	// Phase 4: Fix orphans outside of ForEach to avoid RLock -> Lock deadlock.
	for _, doc := range orphans {
		m, err := docToMemory(doc)
		if err != nil {
			slog.Warn("crash recovery: skip corrupt orphan", "id", doc.ID, "err", err)
			continue
		}
		slog.Info("crash recovery: archiving orphaned memory", "id", doc.ID)
		m.State = StateArchived
		m.UpdatedAt = time.Now()
		fixedDoc, err := memoryToDoc(m, doc.Vector)
		if err != nil {
			slog.Warn("failed to marshal fixed memory", "id", doc.ID, "err", err)
			continue
		}
		if err := s.coll.UpdateContext(context.Background(), fixedDoc); err != nil {
			slog.Warn("failed to fix orphaned memory", "id", doc.ID, "err", err)
		} else {
			s.inverted.Remove(doc.ID)
		}
	}

	// Phase 5: archive any active memory whose ID is referenced as PreviousID
	// by another active memory.
	for oldID := range previousIDSet {
		oldDoc, err := s.coll.GetContext(context.Background(), oldID)
		if err != nil {
			continue
		}
		oldMem, err := docToMemory(oldDoc)
		if err != nil {
			slog.Warn("crash recovery: corrupt old memory referenced by PreviousID", "id", oldID, "err", err)
			continue
		}
		if oldMem.State != StateActive {
			continue
		}
		slog.Info("crash recovery: archiving old memory referenced by PreviousID", "id", oldID)
		oldMem.State = StateArchived
		oldMem.UpdatedAt = time.Now()
		fixedDoc, err := memoryToDoc(oldMem, oldDoc.Vector)
		if err != nil {
			slog.Warn("failed to marshal fixed memory", "id", oldID, "err", err)
			continue
		}
		if err := s.coll.UpdateContext(context.Background(), fixedDoc); err != nil {
			slog.Warn("failed to fix old memory referenced by PreviousID", "id", oldID, "err", err)
		} else {
			s.inverted.Remove(oldID)
		}
	}

	return nil
}

// archiveAndCreate performs the two-phase update: insert new memory,
// then archive the old one. Caller must not hold s.mu.
//
// If archiving the old memory fails (e.g. GetContext, decode, or UpdateContext
// errors), the new memory is rolled back via compensateInsert to prevent an
// orphan. Pinned memories and concurrent state changes are not rollback
// triggers — the new memory is intentionally kept as a separate ADD.
func (s *MemoryStore) archiveAndCreate(ctx context.Context, oldID string, newMem *Memory, newVec []float32) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Step 1: insert new memory.
	newMem.PreviousID = oldID
	newDoc, err := memoryToDoc(newMem, newVec)
	if err != nil {
		return fmt.Errorf("marshal new memory: %w", err)
	}
	if err := s.coll.InsertContext(ctx, newDoc); err != nil {
		return fmt.Errorf("insert new memory: %w", err)
	}
	s.inverted.Add(newMem.ID, newMem.Content)

	// Step 2: archive old memory. If any step fails (excluding concurrent
	// state-change and pinned-target cases), compensate by removing the
	// new memory to avoid leaving an orphan.
	oldDoc, err := s.coll.GetContext(ctx, oldID)
	if err != nil {
		slog.Error("archiveAndCreate: get old memory failed, rolling back new memory",
			"old_id", oldID, "new_id", newMem.ID, "err", err)
		s.compensateInsert(newMem.ID)
		return fmt.Errorf("archive old memory: get old: %w", err)
	}

	oldMem, err := docToMemory(oldDoc)
	if err != nil {
		slog.Error("archiveAndCreate: corrupt old memory, rolling back new memory",
			"old_id", oldID, "new_id", newMem.ID, "err", err)
		s.compensateInsert(newMem.ID)
		return fmt.Errorf("archive old memory: decode old: %w", err)
	}

	// Re-verify state under lock: a concurrent Delete/Update may have
	// changed the state between the caller's initial read (outside s.mu)
	// and this re-read under s.mu.
	// If the state is no longer active, the new memory has already been
	// inserted successfully. Returning an error would orphan it, so we
	// log a warning and return nil — the new memory remains searchable
	// as a separate entry (equivalent to ADD).
	if oldMem.State != StateActive {
		slog.Warn("archiveAndCreate: old memory state changed concurrently, new memory kept as separate entry",
			"old_id", oldID, "new_id", newMem.ID, "state", oldMem.State)
		return nil
	}
	// Pinned memories must not be archived. The caller (update) already
	// checked, but a concurrent pin operation may have changed the type.
	// Keep the new memory as a separate ADD rather than rolling back.
	if oldMem.MemoryType == TypePinned {
		slog.Warn("archiveAndCreate: target became pinned concurrently, new memory kept as separate entry",
			"old_id", oldID, "new_id", newMem.ID)
		return nil
	}

	oldMem.State = StateArchived
	oldMem.SupersededBy = newMem.ID
	oldMem.UpdatedAt = time.Now()

	archivedDoc, err := memoryToDoc(oldMem, oldDoc.Vector)
	if err != nil {
		slog.Error("archiveAndCreate: marshal archived memory failed, rolling back new memory",
			"old_id", oldID, "new_id", newMem.ID, "err", err)
		s.compensateInsert(newMem.ID)
		return fmt.Errorf("archive old memory: marshal: %w", err)
	}

	if err := s.coll.UpdateContext(ctx, archivedDoc); err != nil {
		slog.Error("archiveAndCreate: update archived memory failed, rolling back new memory",
			"old_id", oldID, "new_id", newMem.ID, "err", err)
		s.compensateInsert(newMem.ID)
		return fmt.Errorf("archive old memory: update: %w", err)
	}
	s.inverted.Remove(oldID)

	return nil
}

// compensateInsert removes a memory that was just inserted (rollback
// for when archiveAndCreate fails after inserting the new memory).
// Caller must hold s.mu.
func (s *MemoryStore) compensateInsert(id string) {
	s.inverted.Remove(id)
	if err := s.coll.DeleteContext(context.Background(), id); err != nil {
		slog.Warn("compensateInsert: DeleteContext failed, trying soft-delete fallback",
			"id", id, "err", err)
		// Fallback: soft-delete via UpdateContext so the orphan at least
		// becomes invisible to search (state=deleted).
		if err := s.softDelete(context.Background(), id); err != nil {
			slog.Error("compensateInsert: all rollback attempts failed, orphan remains active",
				"id", id, "soft_delete_err", err)
		}
	}
}

// softDelete changes a memory's state to deleted via UpdateContext
// (Vego MarkDeleted + Put with updated metadata). This is used as a
// fallback when DeleteContext fails, ensuring the memory becomes
// invisible to search even if the docToNode mapping could not be
// removed.
func (s *MemoryStore) softDelete(ctx context.Context, id string) error {
	doc, err := s.coll.GetContext(ctx, id)
	if err != nil {
		return fmt.Errorf("get for soft-delete: %w", err)
	}
	m, err := docToMemory(doc)
	if err != nil {
		return fmt.Errorf("decode for soft-delete: %w", err)
	}
	m.State = StateDeleted
	m.UpdatedAt = time.Now()
	delDoc, err := memoryToDoc(m, doc.Vector)
	if err != nil {
		return fmt.Errorf("marshal for soft-delete: %w", err)
	}
	if err := s.coll.UpdateContext(ctx, delDoc); err != nil {
		return fmt.Errorf("UpdateContext for soft-delete: %w", err)
	}
	s.inverted.Remove(id)
	return nil
}
