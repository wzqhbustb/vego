package memory

import (
	"context"
	"math"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestTokenizeEnglish(t *testing.T) {
	got := tokenize("Hello world, Go!")
	want := []string{"hello", "world", "go"}
	if !slicesEqual(got, want) {
		t.Errorf("tokenize(English): got %v, want %v", got, want)
	}
}

func TestTokenizeChinese(t *testing.T) {
	got := tokenize("我爱编程")
	want := []string{"我爱", "爱编", "编程"}
	if !slicesEqual(got, want) {
		t.Errorf("tokenize(Chinese): got %v, want %v", got, want)
	}
}

func TestTokenizeMixed(t *testing.T) {
	got := tokenize("Hello世界Go")
	want := []string{"hello", "世界", "go"}
	if !slicesEqual(got, want) {
		t.Errorf("tokenize(Mixed): got %v, want %v", got, want)
	}
}

func TestTokenizeSingleChineseChar(t *testing.T) {
	got := tokenize("中")
	want := []string{"中"}
	if !slicesEqual(got, want) {
		t.Errorf("tokenize(SingleChar): got %v, want %v", got, want)
	}
}

func TestTokenizeEmpty(t *testing.T) {
	got := tokenize("")
	if len(got) != 0 {
		t.Errorf("tokenize(Empty): got %v, want empty", got)
	}
}

func TestTokenizeOnlyPunctuation(t *testing.T) {
	got := tokenize("!@#$% , . ;")
	if len(got) != 0 {
		t.Errorf("tokenize(Punctuation): got %v, want empty", got)
	}
}

func TestTokenizeStopWords(t *testing.T) {
	got := tokenize("The cat is on the mat")
	want := []string{"cat", "mat"}
	if !slicesEqual(got, want) {
		t.Errorf("tokenize(StopWords): got %v, want %v", got, want)
	}
}

func TestTokenizeDigits(t *testing.T) {
	got := tokenize("Version 123 and v2")
	want := []string{"version", "v2"}
	if !slicesEqual(got, want) {
		t.Errorf("tokenize(Digits): got %v, want %v", got, want)
	}
}

func TestTokenizeTechSymbols(t *testing.T) {
	// '+' and '-' are preserved inside tokens for technical terms.
	cases := []struct {
		input string
		want  []string
	}{
		{"C++", []string{"c++"}},
		{"co-operate", []string{"co-operate"}},
		{"Rust#", []string{"rust"}},       // '#' is a separator
		{"100%", []string{}},             // '%' splits; "100" is pure digits
		{"X&Y", []string{"x", "y"}},      // '&' is a separator
		{"user@host", []string{"user", "host"}}, // '@' is a separator
	}
	for _, tc := range cases {
		got := tokenize(tc.input)
		if !slicesEqual(got, tc.want) {
			t.Errorf("tokenize(%q): got %v, want %v", tc.input, got, tc.want)
		}
	}
}

func TestTokenizeControlChars(t *testing.T) {
	// Control characters (e.g. NUL, TAB-not-space, BEL) should act as separators.
	got := tokenize("hello\x00world")
	want := []string{"hello", "world"}
	if !slicesEqual(got, want) {
		t.Errorf("tokenize(control chars): got %v, want %v", got, want)
	}
}

func TestIsAllDigits(t *testing.T) {
	if !isAllDigits("123") {
		t.Error("isAllDigits(123) should be true")
	}
	if isAllDigits("abc") {
		t.Error("isAllDigits(abc) should be false")
	}
	if isAllDigits("") {
		t.Error("isAllDigits(empty) should be false")
	}
	if isAllDigits("12a3") {
		t.Error("isAllDigits(12a3) should be false")
	}
}

func TestInvertedIndexAddAndSearch(t *testing.T) {
	idx := NewInvertedIndex()
	idx.Add("doc1", "hello world go")
	idx.Add("doc2", "hello world")

	results := idx.Search("hello", 10)
	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(results))
	}

	// doc2 is shorter (dl=2 vs dl=3) with same tf=1, so it scores slightly higher
	if results[0].ID != "doc2" {
		t.Errorf("Expected doc2 first (shorter doc, higher BM25), got %s", results[0].ID)
	}
	if results[1].ID != "doc1" {
		t.Errorf("Expected doc1 second, got %s", results[1].ID)
	}
	if results[0].Score <= results[1].Score {
		t.Errorf("Expected doc2 score > doc1 score, got %f vs %f", results[0].Score, results[1].Score)
	}
}

func TestInvertedIndexSearchMultipleTerms(t *testing.T) {
	idx := NewInvertedIndex()
	idx.Add("doc1", "hello world")
	idx.Add("doc2", "hello world go")
	idx.Add("doc3", "foo bar")

	results := idx.Search("hello world", 10)
	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(results))
	}

	// Both doc1 and doc2 match both terms, but doc1 is shorter
	if results[0].ID != "doc1" {
		t.Errorf("Expected doc1 first, got %s", results[0].ID)
	}
}

func TestInvertedIndexRemove(t *testing.T) {
	idx := NewInvertedIndex()
	idx.Add("doc1", "hello world")
	idx.Add("doc2", "hello go")

	idx.Remove("doc1")

	results := idx.Search("hello", 10)
	if len(results) != 1 {
		t.Fatalf("Expected 1 result after remove, got %d", len(results))
	}
	if results[0].ID != "doc2" {
		t.Errorf("Expected doc2, got %s", results[0].ID)
	}

	// Search for term only in removed doc
	results = idx.Search("world", 10)
	if len(results) != 0 {
		t.Errorf("Expected 0 results for 'world', got %d", len(results))
	}
}

func TestInvertedIndexUpdate(t *testing.T) {
	idx := NewInvertedIndex()
	idx.Add("doc1", "hello world")
	idx.Add("doc1", "goodbye world") // update

	results := idx.Search("hello", 10)
	if len(results) != 0 {
		t.Errorf("Expected 0 results for 'hello' after update, got %d", len(results))
	}

	results = idx.Search("goodbye", 10)
	if len(results) != 1 || results[0].ID != "doc1" {
		t.Errorf("Expected doc1 for 'goodbye', got %v", results)
	}
}

func TestInvertedIndexSearchLimit(t *testing.T) {
	idx := NewInvertedIndex()
	idx.Add("doc1", "hello")
	idx.Add("doc2", "hello")
	idx.Add("doc3", "hello")

	results := idx.Search("hello", 2)
	if len(results) != 2 {
		t.Errorf("Expected 2 results with limit=2, got %d", len(results))
	}
}

func TestInvertedIndexSearchLimitZero(t *testing.T) {
	idx := NewInvertedIndex()
	idx.Add("doc1", "hello")
	idx.Add("doc2", "hello")

	results := idx.Search("hello", 0)
	if len(results) != 0 {
		t.Errorf("Expected 0 results with limit=0, got %d", len(results))
	}
}

func TestInvertedIndexSearchLimitNegative(t *testing.T) {
	idx := NewInvertedIndex()
	idx.Add("doc1", "hello")
	idx.Add("doc2", "hello")
	idx.Add("doc3", "hello")

	results := idx.Search("hello", -1)
	if len(results) != 3 {
		t.Errorf("Expected 3 results with limit=-1 (all), got %d", len(results))
	}
}

func TestInvertedIndexSearchEmptyIndex(t *testing.T) {
	idx := NewInvertedIndex()
	results := idx.Search("hello", 10)
	if results != nil {
		t.Errorf("Expected nil for empty index, got %v", results)
	}
}

func TestInvertedIndexSearchEmptyQuery(t *testing.T) {
	idx := NewInvertedIndex()
	idx.Add("doc1", "hello")
	results := idx.Search("", 10)
	if results != nil {
		t.Errorf("Expected nil for empty query, got %v", results)
	}
}

func TestInvertedIndexSearchDuplicateTerms(t *testing.T) {
	idx := NewInvertedIndex()
	idx.Add("doc1", "hello world")

	// Query with duplicate terms should produce same score as single term
	resultsDup := idx.Search("hello hello", 10)
	resultsSingle := idx.Search("hello", 10)

	if len(resultsDup) != 1 || len(resultsSingle) != 1 {
		t.Fatalf("Expected 1 result each, got dup=%d single=%d", len(resultsDup), len(resultsSingle))
	}
	if math.Abs(resultsDup[0].Score-resultsSingle[0].Score) > 1e-9 {
		t.Errorf("Duplicate terms should not double score: dup=%f single=%f",
			resultsDup[0].Score, resultsSingle[0].Score)
	}
}

func TestInvertedIndexSearchNoMatch(t *testing.T) {
	idx := NewInvertedIndex()
	idx.Add("doc1", "hello world")
	results := idx.Search("xyz", 10)
	if len(results) != 0 {
		t.Errorf("Expected 0 results for no-match query, got %d", len(results))
	}
}

func TestInvertedIndexAddEmptyID(t *testing.T) {
	idx := NewInvertedIndex()
	idx.Add("", "hello world")
	if idx.Len() != 0 {
		t.Errorf("Empty id should be rejected, got Len=%d", idx.Len())
	}
}

func TestInvertedIndexAddEmptyContent(t *testing.T) {
	idx := NewInvertedIndex()
	idx.Add("doc1", "hello")
	if idx.Len() != 1 {
		t.Fatalf("Expected 1 doc before empty add, got %d", idx.Len())
	}
	idx.Add("doc1", "") // update to empty content → remove
	if idx.Len() != 0 {
		t.Errorf("Update to empty content should remove doc, got Len=%d", idx.Len())
	}
	results := idx.Search("hello", 10)
	if len(results) != 0 {
		t.Errorf("Expected 0 results after empty update, got %d", len(results))
	}
}

func TestInvertedIndexRemoveDuplicateTerms(t *testing.T) {
	idx := NewInvertedIndex()
	idx.Add("doc1", "hello hello hello")
	idx.Add("doc2", "hello world")

	// doc1 has tf=3 for "hello", doc2 has tf=1
	results := idx.Search("hello", 10)
	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(results))
	}
	if results[0].ID != "doc1" {
		t.Errorf("Expected doc1 first (tf=3), got %s", results[0].ID)
	}

	// Remove doc1; doc2 should still be searchable.
	idx.Remove("doc1")
	results = idx.Search("hello", 10)
	if len(results) != 1 || results[0].ID != "doc2" {
		t.Errorf("Expected doc2 after remove, got %v", results)
	}
}

func TestInvertedIndexLenAndDocCount(t *testing.T) {
	idx := NewInvertedIndex()
	if idx.Len() != 0 {
		t.Errorf("Initial Len: want 0, got %d", idx.Len())
	}
	if idx.DocCount() != 0 {
		t.Errorf("Initial DocCount: want 0, got %d", idx.DocCount())
	}

	idx.Add("doc1", "hello")
	if idx.Len() != 1 {
		t.Errorf("After Add: want 1, got %d", idx.Len())
	}

	idx.Add("doc1", "world") // update
	if idx.Len() != 1 {
		t.Errorf("After Update: want 1, got %d", idx.Len())
	}

	idx.Remove("doc1")
	if idx.Len() != 0 {
		t.Errorf("After Remove: want 0, got %d", idx.Len())
	}
}

func TestInvertedIndexClear(t *testing.T) {
	idx := NewInvertedIndex()
	idx.Add("doc1", "hello")
	idx.Add("doc2", "world")
	if idx.Len() != 2 {
		t.Fatalf("Expected 2 docs before clear, got %d", idx.Len())
	}

	idx.Clear()
	if idx.Len() != 0 {
		t.Errorf("After Clear: want 0, got %d", idx.Len())
	}
	results := idx.Search("hello", 10)
	if results != nil {
		t.Errorf("Expected nil after clear, got %v", results)
	}
}

func TestInvertedIndexWithBM25Params(t *testing.T) {
	idx := NewInvertedIndex().WithBM25Params(1.5, 0.5)
	idx.Add("doc1", "hello world go")
	idx.Add("doc2", "hello world")

	results := idx.Search("hello", 10)
	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(results))
	}
	// With custom params, scoring should still be monotonic.
	if results[0].Score <= results[1].Score {
		t.Errorf("Expected decreasing scores, got %f then %f", results[0].Score, results[1].Score)
	}
}

func TestInvertedIndexWithBM25ParamsInvalid(t *testing.T) {
	idx := NewInvertedIndex().WithBM25Params(-1, 2.0)
	if idx.k1 != defaultK1 {
		t.Errorf("Negative k1 should fallback to default, got %f", idx.k1)
	}
	if idx.b != defaultB {
		t.Errorf("Out-of-range b should fallback to default, got %f", idx.b)
	}
}

func TestInvertedIndexSearchContext(t *testing.T) {
	idx := NewInvertedIndex()
	for i := 0; i < 100; i++ {
		idx.Add(string(rune('a'+i%26))+string(rune('0'+i/10)), "hello world common term")
	}

	// Happy path
	ctx := context.Background()
	results, err := idx.SearchContext(ctx, "hello", 10)
	if err != nil {
		t.Fatalf("SearchContext happy path error: %v", err)
	}
	if len(results) != 10 {
		t.Errorf("Expected 10 results, got %d", len(results))
	}

	// Cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // immediately cancel
	results, err = idx.SearchContext(ctx, "hello", 10)
	if err != context.Canceled {
		t.Errorf("Expected context.Canceled, got err=%v results=%v", err, results)
	}
}

func TestInvertedIndexSearchContextTimeout(t *testing.T) {
	idx := NewInvertedIndex()
	// Build a large index so scoring takes non-trivial time.
	for i := 0; i < 5000; i++ {
		idx.Add(string(rune('a'+i%26))+string(rune('a'+i/26%26))+string(rune('0'+i/676%10)),
			"the quick brown fox jumps over the lazy dog hello world")
	}

	// Use a deadline in the past to guarantee the context is already expired,
	// making the test deterministic regardless of scheduling pressure.
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	_, err := idx.SearchContext(ctx, "hello world quick brown", 10)
	if err != context.DeadlineExceeded {
		t.Errorf("Expected context.DeadlineExceeded, got %v", err)
	}
}

func TestInvertedIndexConcurrent(t *testing.T) {
	idx := NewInvertedIndex()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			id := ids[n]
			idx.Add(id, contents[n])
		}(i)
	}
	wg.Wait()

	// Verify all docs are searchable
	results := idx.Search("common", 200)
	if len(results) != 100 {
		t.Errorf("Expected 100 results, got %d", len(results))
	}

	// Concurrent read + write
	wg = sync.WaitGroup{}
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func(n int) {
			defer wg.Done()
			idx.Search("common", 10)
		}(i)
		go func(n int) {
			defer wg.Done()
			idx.Remove(ids[n])
		}(i)
	}
	wg.Wait()
}

func TestInvertedIndexDocCount(t *testing.T) {
	idx := NewInvertedIndex()
	if idx.docCount != 0 {
		t.Errorf("Initial docCount: want 0, got %d", idx.docCount)
	}

	idx.Add("doc1", "hello")
	if idx.docCount != 1 {
		t.Errorf("After Add: want 1, got %d", idx.docCount)
	}

	idx.Add("doc1", "world") // update
	if idx.docCount != 1 {
		t.Errorf("After Update: want 1, got %d", idx.docCount)
	}

	idx.Remove("doc1")
	if idx.docCount != 0 {
		t.Errorf("After Remove: want 0, got %d", idx.docCount)
	}
}

// --- helpers ---

func slicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

var ids = make([]string, 100)
var contents = make([]string, 100)

func init() {
	for i := 0; i < 100; i++ {
		ids[i] = strings.Repeat("x", i%10) + string(rune('a'+i%26))
		contents[i] = "common word " + string(rune('a'+i%26))
	}
}

// ----------------------------------------------------------------------
// RebuildBatch
// ----------------------------------------------------------------------

func TestInvertedIndexRebuildBatch(t *testing.T) {
	idx := NewInvertedIndex()

	entries := []RebuildEntry{
		{ID: "doc1", Terms: []string{"hello", "world"}},
		{ID: "doc2", Terms: []string{"hello", "golang"}},
		{ID: "doc3", Terms: []string{"world"}},
	}

	if err := idx.RebuildBatch(entries); err != nil {
		t.Fatalf("RebuildBatch: %v", err)
	}

	if idx.Len() != 3 {
		t.Errorf("Len: want 3, got %d", idx.Len())
	}

	results := idx.Search("hello", 10)
	if len(results) != 2 {
		t.Fatalf("Search hello: want 2 results, got %d", len(results))
	}
	if results[0].ID != "doc1" && results[0].ID != "doc2" {
		t.Errorf("unexpected top result: %s", results[0].ID)
	}

	// Verify BM25 scoring order: doc2 has fewer terms → higher avgdl weight
	results = idx.Search("world", 10)
	if len(results) != 2 {
		t.Fatalf("Search world: want 2 results, got %d", len(results))
	}
}

func TestInvertedIndexRebuildBatchEmpty(t *testing.T) {
	idx := NewInvertedIndex()
	if err := idx.RebuildBatch(nil); err != nil {
		t.Fatalf("RebuildBatch nil: %v", err)
	}
	if idx.Len() != 0 {
		t.Errorf("nil entries: want 0, got %d", idx.Len())
	}
	if err := idx.RebuildBatch([]RebuildEntry{}); err != nil {
		t.Fatalf("RebuildBatch empty: %v", err)
	}
	if idx.Len() != 0 {
		t.Errorf("empty entries: want 0, got %d", idx.Len())
	}
}

func TestInvertedIndexRebuildBatchSkipEmpty(t *testing.T) {
	idx := NewInvertedIndex()

	entries := []RebuildEntry{
		{ID: "", Terms: []string{"hello"}},        // empty ID → skip
		{ID: "doc2", Terms: nil},                   // nil terms → skip
		{ID: "doc3", Terms: []string{}},            // empty terms → skip
		{ID: "doc4", Terms: []string{"valid"}},     // valid
	}

	if err := idx.RebuildBatch(entries); err != nil {
		t.Fatalf("RebuildBatch: %v", err)
	}
	if idx.Len() != 1 {
		t.Errorf("want 1 valid doc, got %d", idx.Len())
	}

	results := idx.Search("valid", 10)
	if len(results) != 1 || results[0].ID != "doc4" {
		t.Errorf("unexpected result: %+v", results)
	}
}

func TestInvertedIndexRebuildBatchErrorOnNonEmpty(t *testing.T) {
	idx := NewInvertedIndex()
	idx.Add("doc1", "hello")

	err := idx.RebuildBatch([]RebuildEntry{{ID: "doc2", Terms: []string{"world"}}})
	if err == nil {
		t.Error("expected error on non-empty index")
	}
}
