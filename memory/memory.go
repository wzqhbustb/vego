package memory

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	hnsw "github.com/wzqhbustb/vego/index"
	vego "github.com/wzqhbustb/vego/vego"
)

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
	mu               sync.Mutex // guards all write operations (Store/Update/Delete/Bootstrap/StoreBatch)
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
	}

	if err := s.rebuildIndexes(); err != nil {
		_ = s.Close()
		return nil, fmt.Errorf("rebuild indexes: %w", err)
	}

	return s, nil
}

// Close closes the MemoryStore and its underlying database.
func (s *MemoryStore) Close() error {
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

// Update updates a memory's content using Archive-and-Create.
// The old memory is archived (state=archived, superseded_by=newID)
// and a new memory with a fresh ID is created.
func (s *MemoryStore) Update(ctx context.Context, id, content string, tags []string) (*Memory, error) {
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
		ID:        vego.DocumentID(),
		Content:   content,
		State:     StateActive,
		Tags:      append([]string(nil), tags...),
		Version:   oldMem.Version + 1,
		CreatedAt: oldMem.CreatedAt,
		UpdatedAt: time.Now(),
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
	limit int
}

// Limit sets the maximum number of search results for a single call.
func Limit(n int) SearchOption {
	return func(c *searchConfig) { c.limit = n }
}

// Search performs a vector search over active memories.
// Hybrid search (vector + BM25 + RRF) will be integrated in Task 8.
func (s *MemoryStore) Search(ctx context.Context, query string, opts ...SearchOption) ([]Memory, error) {
	sc := &searchConfig{limit: s.config.SearchLimit}
	for _, opt := range opts {
		opt(sc)
	}

	vec, err := s.embed(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("embed query: %w", err)
	}

	filter := &vego.MetadataFilter{
		Field:    metaKeyState,
		Operator: "eq",
		Value:    string(StateActive),
	}

	results, err := s.coll.SearchWithFilterContext(ctx, vec, sc.limit, filter)
	if err != nil {
		return nil, fmt.Errorf("search: %w", err)
	}

	return s.toMemories(results)
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
func (s *MemoryStore) StoreBatch(ctx context.Context, items []StoreItem) ([]Memory, error) {
	mems := make([]Memory, len(items))
	docs := make([]*vego.Document, len(items))

	for i, item := range items {
		vec, err := s.embed(ctx, item.Content)
		if err != nil {
			return nil, fmt.Errorf("embed item %d: %w", i, err)
		}

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

		doc, err := memoryToDoc(mem, vec)
		if err != nil {
			return nil, fmt.Errorf("convert item %d: %w", i, err)
		}
		docs[i] = doc
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.coll.InsertBatchContext(ctx, docs); err != nil {
		return nil, fmt.Errorf("insert batch: %w", err)
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

	if err := s.coll.InsertBatchContext(ctx, docs); err != nil {
		return fmt.Errorf("insert batch: %w", err)
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

func (s *MemoryStore) toMemories(results []vego.SearchResult) ([]Memory, error) {
	out := make([]Memory, 0, len(results))
	for _, r := range results {
		m, err := docToMemory(r.Document)
		if err != nil {
			slog.Warn("skip corrupt document in search results", "id", r.Document.ID, "err", err)
			continue
		}
		out = append(out, *m)
	}
	return out, nil
}

// rebuildIndexes rebuilds the inverted index and ContentHashIndex from
// persisted documents. It also runs crash recovery.
func (s *MemoryStore) rebuildIndexes() error {
	var orphans []*vego.Document
	previousIDSet := make(map[string]struct{}) // deduplicated IDs referenced by PreviousID

	err := s.coll.ForEach(func(doc *vego.Document) bool {
		m, err := docToMemory(doc)
		if err != nil {
			slog.Warn("skip corrupt document during rebuild", "id", doc.ID, "err", err)
			return true
		}

		// Collect orphaned active memories for later fix-up.
		// We must NOT call UpdateContext inside ForEach because ForEach
		// holds the collection RLock and UpdateContext needs Lock,
		// causing a deadlock.
		if m.State == StateActive && m.SupersededBy != "" {
			orphans = append(orphans, doc)
			return true
		}
		if m.State == StateActive && m.PreviousID != "" {
			previousIDSet[m.PreviousID] = struct{}{}
		}

		// Index active memories for inverted search.
		// Index ALL TypeSession memories for ContentHash deduplication,
		// regardless of state, to prevent re-storing archived/deleted messages.
		if m.State == StateActive {
			s.inverted.Add(m.ID, m.Content)
		}
		if m.MemoryType == TypeSession && m.ContentHash != "" {
			s.contentHashIndex.Add(m.SessionID, m.ContentHash, m.ID, m.Seq)
		}
		return true
	})
	if err != nil {
		return err
	}

	// Fix orphans outside of ForEach to avoid RLock -> Lock deadlock.
	for _, doc := range orphans {
		m, _ := docToMemory(doc)
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

	// Phase 2: archive any active memory whose ID is referenced as PreviousID
	// by another active memory. This catches the case where Step 1 (Insert new)
	// succeeded but Step 2 (archive old) failed before old.SupersededBy was set.
	for oldID := range previousIDSet {
		oldDoc, err := s.coll.GetContext(context.Background(), oldID)
		if err != nil {
			continue // old may have been deleted or not exist
		}
		oldMem, err := docToMemory(oldDoc)
		if err != nil {
			slog.Warn("crash recovery: corrupt old memory referenced by PreviousID", "id", oldID, "err", err)
			continue
		}
		if oldMem.State != StateActive {
			continue // already archived or deleted
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

	// Step 2: archive old memory.
	oldDoc, err := s.coll.GetContext(ctx, oldID)
	if err != nil {
		slog.Warn("archive old memory failed, orphan created", "old_id", oldID, "new_id", newMem.ID, "err", err)
		return fmt.Errorf("archive old memory: get old: %w", err)
	}

	oldMem, err := docToMemory(oldDoc)
	if err != nil {
		slog.Warn("corrupt old memory during archive", "old_id", oldID, "err", err)
		return fmt.Errorf("archive old memory: decode old: %w", err)
	}

	oldMem.State = StateArchived
	oldMem.SupersededBy = newMem.ID
	oldMem.UpdatedAt = time.Now()

	archivedDoc, err := memoryToDoc(oldMem, oldDoc.Vector)
	if err != nil {
		slog.Warn("marshal archived memory failed", "old_id", oldID, "err", err)
		return fmt.Errorf("archive old memory: marshal: %w", err)
	}

	if err := s.coll.UpdateContext(ctx, archivedDoc); err != nil {
		slog.Warn("archive old memory state update failed", "old_id", oldID, "err", err)
		return fmt.Errorf("archive old memory: update: %w", err)
	}
	s.inverted.Remove(oldID)

	return nil
}
