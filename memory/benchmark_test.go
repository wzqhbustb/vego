package memory

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	vego "github.com/wzqhbustb/vego/vego"
)

// generateBenchmarkContent returns a pseudo-random string of approximately
// targetLen runes, mixing Chinese and English to match the expected workload.
func generateBenchmarkContent(r *rand.Rand, targetLen int) string {
	// Mix Chinese sentences and English words.
	fragments := []string{
		"系统架构设计需要考虑高可用性和可扩展性",
		"golang concurrency patterns include goroutines and channels",
		"数据库索引优化可以显著提升查询性能",
		"microservices architecture enables independent deployment",
		"缓存策略选择直接影响应用的响应延迟",
		"RESTful API design should follow idempotency principles",
		"分布式事务处理是微服务架构中的核心挑战",
		"Kubernetes orchestrates containerized applications at scale",
		"代码审查是保障软件质量的重要实践",
		"event-driven architecture decouples producers and consumers",
		"持续集成和持续部署是现代软件开发的标准流程",
		"observability includes metrics logs and distributed tracing",
		"异步消息队列可以削峰填谷缓解系统压力",
		"domain-driven design aligns code structure with business logic",
		"安全性设计需要在开发的每个阶段都予以考虑",
		"load balancing distributes traffic across multiple servers",
	}

	var content string
	for len([]rune(content)) < targetLen {
		content += fragments[r.Intn(len(fragments))]
		if len([]rune(content)) < targetLen {
			content += " "
		}
	}
	runes := []rune(content)
	if len(runes) > targetLen {
		return string(runes[:targetLen])
	}
	return content
}

// BenchmarkRebuildInvertedIndex_100K measures the time to rebuild the
// inverted index from 100K persisted documents. Target: < 1s.
func BenchmarkRebuildInvertedIndex_100K(b *testing.B) {
	const numDocs = 100_000
	const avgContentLen = 500

	ctx := context.Background()
	r := rand.New(rand.NewSource(42))

	// Phase 1: prepare documents with pre-computed vectors (no embed API calls).
	memories := make([]*Memory, numDocs)
	for i := 0; i < numDocs; i++ {
		vec := make([]float32, 128)
		for j := range vec {
			vec[j] = r.Float32()
		}
		memories[i] = &Memory{
			ID:         fmt.Sprintf("bench-%09d", i),
			Content:    generateBenchmarkContent(r, avgContentLen),
			State:      StateActive,
			MemoryType: TypeInsight,
			Tags:       []string{},
			Vector:     vec,
			CreatedAt:  time.Now(),
			UpdatedAt:  time.Now(),
		}
	}

	// Phase 2: open store and bootstrap all documents.
	b.Logf("bootstrapping %d documents...", numDocs)
	s := newTestStore(b)
	if err := s.Bootstrap(ctx, memories); err != nil {
		b.Fatalf("bootstrap: %v", err)
	}

	// Clear the inverted index so we can benchmark rebuildIndexes directly.
	s.inverted.Clear()
	b.Logf("inverted index cleared, starting benchmark...")

	// Phase 3: benchmark rebuildIndexes directly.
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.inverted.Clear()
		if err := s.rebuildIndexes(); err != nil {
			b.Fatalf("rebuildIndexes: %v", err)
		}
	}
	b.StopTimer()

	// Verify.
	if s.inverted.Len() != numDocs {
		b.Errorf("inverted index len: want %d, got %d", numDocs, s.inverted.Len())
	}
}

// BenchmarkSearchHighArchiveRate_80pct measures search latency when
// 80% of memories are archived. Target: reasonable latency (< 100ms per query).
func BenchmarkSearchHighArchiveRate_80pct(b *testing.B) {
	const totalDocs = 10_000
	const activeDocs = 2_000 // 20% active

	ctx := context.Background()
	r := rand.New(rand.NewSource(43))

	memories := make([]*Memory, totalDocs)
	for i := 0; i < totalDocs; i++ {
		vec := make([]float32, 128)
		for j := range vec {
			vec[j] = r.Float32()
		}
		state := StateActive
		if i >= activeDocs {
			state = StateArchived
		}
		memories[i] = &Memory{
			ID:         fmt.Sprintf("search-%09d", i),
			Content:    generateBenchmarkContent(r, 200),
			State:      state,
			MemoryType: TypeInsight,
			Tags:       []string{},
			Vector:     vec,
			CreatedAt:  time.Now(),
			UpdatedAt:  time.Now(),
		}
	}

	s := newTestStore(b)
	setupMockEmbedder(b, s, 128)
	defer s.Close()

	// Bootstrap only active memories (inverted index will contain these).
	if err := s.Bootstrap(ctx, memories[:activeDocs]); err != nil {
		b.Fatalf("bootstrap active: %v", err)
	}

	// Insert archived memories directly into Vego (bypass inverted index).
	archivedDocs := make([]*vego.Document, totalDocs-activeDocs)
	for i := activeDocs; i < totalDocs; i++ {
		doc, err := memoryToDoc(memories[i], memories[i].Vector)
		if err != nil {
			b.Fatalf("marshal archived: %v", err)
		}
		archivedDocs[i-activeDocs] = doc
	}
	if err := s.coll.InsertBatchContext(ctx, archivedDocs); err != nil {
		b.Fatalf("insert archived: %v", err)
	}

	// Sanity check: inverted index only contains active memories.
	if s.inverted.Len() != activeDocs {
		b.Fatalf("inverted index len: want %d, got %d", activeDocs, s.inverted.Len())
	}

	queryVec := make([]float32, 128)
	for i := range queryVec {
		queryVec[i] = r.Float32()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results, err := s.vectorSearch(ctx, queryVec, 10, 0.0)
		if err != nil {
			b.Fatalf("search: %v", err)
		}
		if i == 0 && len(results) == 0 {
			b.Log("warning: search returned 0 results (random vectors may not be close)")
		}
		_ = results
	}
}

// BenchmarkHybridSearch_10K measures hybrid search latency on a
// 10K-document corpus with realistic content length.
func BenchmarkHybridSearch_10K(b *testing.B) {
	const numDocs = 10_000

	ctx := context.Background()
	r := rand.New(rand.NewSource(44))

	memories := make([]*Memory, numDocs)
	for i := 0; i < numDocs; i++ {
		vec := make([]float32, 128)
		for j := range vec {
			vec[j] = r.Float32()
		}
		memories[i] = &Memory{
			ID:         fmt.Sprintf("hybrid-%09d", i),
			Content:    generateBenchmarkContent(r, 300),
			State:      StateActive,
			MemoryType: TypeInsight,
			Tags:       []string{},
			Vector:     vec,
			CreatedAt:  time.Now(),
			UpdatedAt:  time.Now(),
		}
	}

	s := newTestStore(b)
	setupMockEmbedder(b, s, 128)
	defer s.Close()

	if err := s.Bootstrap(ctx, memories); err != nil {
		b.Fatalf("bootstrap: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results, err := s.Search(ctx, "golang concurrency patterns")
		if err != nil {
			b.Fatalf("search: %v", err)
		}
		_ = results
	}
}
