package memory

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"unicode"
)

const (
	defaultK1 = 1.2
	defaultB  = 0.75
)

// ScoredID pairs a document ID with its BM25 relevance score.
type ScoredID struct {
	ID    string
	Score float64
}

// InvertedIndex is a lightweight in-memory inverted index with BM25 scoring.
// All data is held in memory; no persistence is performed.
//
// Thread-safe: all methods are safe for concurrent use.
type InvertedIndex struct {
	mu         sync.RWMutex
	index      map[string]map[string]int // term → docID → tf
	docTerms   map[string][]string       // docID → []term (for cleanup on Remove)
	docLen     map[string]int            // docID → term count
	totalTerms int64                     // sum of all docLens
	docCount   int
	k1         float64
	b          float64
}

// NewInvertedIndex creates an empty in-memory inverted index.
func NewInvertedIndex() *InvertedIndex {
	return &InvertedIndex{
		index:    make(map[string]map[string]int),
		docTerms: make(map[string][]string),
		docLen:   make(map[string]int),
		k1:       defaultK1,
		b:        defaultB,
	}
}

// WithBM25Params sets the BM25 tuning parameters. Not safe for concurrent use;
// call before any Add/Search operations. Invalid values are clamped to defaults.
func (idx *InvertedIndex) WithBM25Params(k1, b float64) *InvertedIndex {
	if k1 < 0 {
		k1 = defaultK1
	}
	if b < 0 || b > 1 {
		b = defaultB
	}
	idx.k1 = k1
	idx.b = b
	return idx
}

// Add indexes a document's content. If the ID already exists, the old entry
// is replaced atomically. Empty id is a no-op; empty content removes the old
// entry without adding a new one.
func (idx *InvertedIndex) Add(id, content string) {
	if id == "" {
		return
	}

	// Tokenize outside the lock — pure function, no shared state.
	terms := tokenize(content)

	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Remove old entry if present (update semantics)
	idx.removeLocked(id)
	if len(terms) == 0 {
		return
	}

	for _, term := range terms {
		if idx.index[term] == nil {
			idx.index[term] = make(map[string]int)
		}
		idx.index[term][id]++
	}
	idx.docTerms[id] = terms
	idx.docLen[id] = len(terms)
	idx.totalTerms += int64(len(terms))
	idx.docCount++
}

// RebuildBatch inserts multiple documents in a single locked operation.
// It is optimized for rebuildIndexes where the index is known to be empty
// (no existing entries to remove). Callers must ensure id uniqueness.
type RebuildEntry struct {
	ID    string
	Terms []string
}

func (idx *InvertedIndex) RebuildBatch(entries []RebuildEntry) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.docCount != 0 {
		return fmt.Errorf("RebuildBatch called on non-empty index: caller must Clear() first")
	}
	if len(entries) == 0 {
		return nil
	}

	// Pre-size maps to avoid repeated rehashing during bulk insert.
	idx.docTerms = make(map[string][]string, len(entries))
	idx.docLen = make(map[string]int, len(entries))
	// Heuristic: mixed CJK/English content yields ~3-5 unique terms per doc
	// on average after stop-word filtering. Over-allocation is harmless.
	idx.index = make(map[string]map[string]int, len(entries)*4)

	for _, e := range entries {
		if e.ID == "" || len(e.Terms) == 0 {
			continue
		}
		for _, term := range e.Terms {
			if idx.index[term] == nil {
				idx.index[term] = make(map[string]int)
			}
			idx.index[term][e.ID]++
		}
		idx.docTerms[e.ID] = e.Terms
		idx.docLen[e.ID] = len(e.Terms)
		idx.totalTerms += int64(len(e.Terms))
		idx.docCount++
	}
	return nil
}

// Remove deletes a document and all its terms from the index.
func (idx *InvertedIndex) Remove(id string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.removeLocked(id)
}

// removeLocked removes a document without acquiring the lock.
// Caller must hold idx.mu.
func (idx *InvertedIndex) removeLocked(id string) {
	oldTerms, ok := idx.docTerms[id]
	if !ok {
		return
	}

	// Count frequency of each unique term to remove all occurrences in one scan.
	termFreq := make(map[string]int, len(oldTerms))
	for _, term := range oldTerms {
		termFreq[term]++
	}

	for term := range termFreq {
		delete(idx.index[term], id)
		if len(idx.index[term]) == 0 {
			delete(idx.index, term)
		}
	}

	idx.totalTerms -= int64(idx.docLen[id])
	delete(idx.docTerms, id)
	delete(idx.docLen, id)
	idx.docCount--
}

// Search performs BM25 scoring over the index and returns the top-N results.
// limit < 0 means return all results; limit == 0 returns an empty slice.
func (idx *InvertedIndex) Search(query string, limit int) []ScoredID {
	results, _ := idx.SearchContext(context.Background(), query, limit)
	return results
}

// SearchContext is like Search but accepts a context for cancellation.
// If the context is cancelled during scoring, a partial result set is not
// returned; instead ctx.Err() is reported.
func (idx *InvertedIndex) SearchContext(ctx context.Context, query string, limit int) ([]ScoredID, error) {
	if limit == 0 {
		return []ScoredID{}, nil
	}

	queryTerms := tokenize(query)
	if len(queryTerms) == 0 {
		return nil, nil
	}

	// --- snapshot under RLock ---
	idx.mu.RLock()
	docCount := idx.docCount
	if docCount == 0 {
		idx.mu.RUnlock()
		return nil, nil
	}
	totalTerms := idx.totalTerms
	avgdl := float64(totalTerms) / float64(docCount)

	// Deduplicate query terms and collect postings.
	seenTerms := make(map[string]struct{}, len(queryTerms))
	type termPostings struct {
		postings map[string]int
	}
	termsData := make([]termPostings, 0, len(queryTerms))
	docLenSnap := make(map[string]int)

	for _, term := range queryTerms {
		if _, seen := seenTerms[term]; seen {
			continue
		}
		seenTerms[term] = struct{}{}
		postings, ok := idx.index[term]
		if !ok {
			continue
		}
		// Deep-copy postings map so we can release the lock before scoring.
		pcopy := make(map[string]int, len(postings))
		for docID, tf := range postings {
			pcopy[docID] = tf
			if _, ok := docLenSnap[docID]; !ok {
				docLenSnap[docID] = idx.docLen[docID]
			}
		}
		termsData = append(termsData, termPostings{postings: pcopy})
	}
	k1 := idx.k1
	b := idx.b
	idx.mu.RUnlock()
	// --- end snapshot ---

	if len(termsData) == 0 {
		return nil, nil
	}

	// BM25 scoring (lock-free).
	scores := make(map[string]float64, len(docLenSnap))
	for _, td := range termsData {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// postings is already map[docID]tf — no counting needed.
		tfMap := td.postings
		df := len(tfMap)
		idf := math.Log((float64(docCount)-float64(df)+0.5)/(float64(df)+0.5) + 1.0)

		for docID, tf := range tfMap {
			dl := float64(docLenSnap[docID])
			numerator := float64(tf) * (k1 + 1.0)
			denominator := float64(tf) + k1*(1.0-b+b*dl/avgdl)
			scores[docID] += idf * numerator / denominator
		}
	}

	results := make([]ScoredID, 0, len(scores))
	for id, score := range scores {
		results = append(results, ScoredID{ID: id, Score: score})
	}
	// Stable tie-breaker by ID to avoid flaky ordering.
	sort.Slice(results, func(i, j int) bool {
		if results[i].Score != results[j].Score {
			return results[i].Score > results[j].Score
		}
		return results[i].ID < results[j].ID
	})

	if limit > 0 && limit < len(results) {
		results = results[:limit]
	}
	return results, nil
}

// Len returns the number of indexed documents.
func (idx *InvertedIndex) Len() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.docCount
}

// DocCount is an alias for Len.
func (idx *InvertedIndex) DocCount() int {
	return idx.Len()
}

// Clear removes all documents from the index.
func (idx *InvertedIndex) Clear() {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.index = make(map[string]map[string]int)
	idx.docTerms = make(map[string][]string)
	idx.docLen = make(map[string]int)
	idx.totalTerms = 0
	idx.docCount = 0
}

// --- tokenization ---

// tokenize splits content into terms.
//   - English/numbers: split by Unicode spaces/punctuation/symbols (except + -),
//     lowercased. Pure numeric tokens are dropped.
//   - Chinese (CJK): character bigram.
func tokenize(content string) []string {
	var terms []string
	current := make([]rune, 0, 32)
	var isCurrChinese bool

	for _, r := range content {
		if isSeparator(r) {
			if len(current) > 0 {
				terms = appendToken(terms, current, isCurrChinese)
				current = current[:0]
			}
			continue
		}

		isRuneChinese := unicode.Is(unicode.Han, r)
		if len(current) > 0 && isRuneChinese != isCurrChinese {
			terms = appendToken(terms, current, isCurrChinese)
			current = current[:0]
		}

		current = append(current, r)
		isCurrChinese = isRuneChinese
	}

	if len(current) > 0 {
		terms = appendToken(terms, current, isCurrChinese)
	}

	return terms
}

func isSeparator(r rune) bool {
	// Preserve '+' and '-' inside tokens for technical terms like "C++" or "co-operate".
	if r == '+' || r == '-' {
		return false
	}
	return unicode.IsSpace(r) || unicode.IsPunct(r) || unicode.IsSymbol(r) || unicode.IsControl(r)
}

func appendToken(terms []string, runes []rune, isChinese bool) []string {
	if isChinese {
		return append(terms, chineseBigram(runes)...)
	}
	s := string(runes)
	s = strings.ToLower(s)
	if isStopWord(s) || isAllDigits(s) {
		return terms
	}
	return append(terms, s)
}

// isAllDigits reports whether s consists solely of Unicode digits.
func isAllDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if !unicode.IsDigit(r) {
			return false
		}
	}
	return true
}

// --- stop words ---

// defaultStopWords is a small English stop-word set.
// Chinese bigrams are too numerous for a static list; we rely on BM25 IDF
// to de-weight common CJK terms naturally.
var defaultStopWords = map[string]struct{}{
	"the": {}, "a": {}, "an": {}, "is": {}, "are": {}, "was": {}, "were": {},
	"be": {}, "been": {}, "being": {}, "have": {}, "has": {}, "had": {},
	"do": {}, "does": {}, "did": {}, "will": {}, "would": {}, "could": {},
	"should": {}, "may": {}, "might": {}, "must": {}, "shall": {},
	"can": {}, "need": {}, "dare": {}, "ought": {}, "used": {},
	"to": {}, "of": {}, "in": {}, "for": {}, "on": {}, "with": {},
	"at": {}, "by": {}, "from": {}, "as": {}, "into": {}, "through": {},
	"during": {}, "before": {}, "after": {}, "above": {}, "below": {},
	"between": {}, "under": {}, "and": {}, "but": {}, "or": {}, "yet": {},
	"so": {}, "if": {}, "because": {}, "although": {}, "though": {},
	"while": {}, "where": {}, "when": {}, "that": {}, "which": {},
	"who": {}, "whom": {}, "whose": {}, "what": {}, "this": {}, "these": {},
	"those": {}, "i": {}, "you": {}, "he": {}, "she": {}, "it": {}, "we": {},
	"they": {}, "me": {}, "him": {}, "her": {}, "us": {}, "them": {},
	"my": {}, "your": {}, "his": {}, "its": {}, "our": {}, "their": {},
	"mine": {}, "yours": {}, "hers": {}, "ours": {}, "theirs": {},
	"myself": {}, "yourself": {}, "himself": {}, "herself": {}, "itself": {},
	"ourselves": {}, "yourselves": {}, "themselves": {},
}

func isStopWord(s string) bool {
	_, ok := defaultStopWords[s]
	return ok
}

// chineseBigram splits a Chinese rune sequence into overlapping character pairs.
// Single rune returns a single-element slice.
func chineseBigram(runes []rune) []string {
	if len(runes) == 1 {
		return []string{string(runes)}
	}
	var terms []string
	for i := 0; i < len(runes)-1; i++ {
		terms = append(terms, string(runes[i:i+2]))
	}
	return terms
}
