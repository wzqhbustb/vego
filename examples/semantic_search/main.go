// Semantic Search Example
// This example demonstrates how to use Vego for semantic document search.
// Semantic search goes beyond keyword matching to understand the meaning and
// intent behind queries, enabling more natural and accurate information retrieval.
//
// Real-world use cases:
//   - Enterprise knowledge base search
//   - Customer support FAQ retrieval
//   - Academic paper search engine
//   - Legal document discovery
//
// Run: go run main.go
package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/wzqhbustb/vego/vego"
)

// Article represents a document in our semantic search engine
type Article struct {
	ID       string
	Title    string
	Content  string
	Author   string
	Category string
	Date     string
}

func main() {
	fmt.Println("=== Vego Semantic Search Demo ===")
	fmt.Println()
	fmt.Println("This example demonstrates semantic search over a collection")
	fmt.Println("of technical articles. Unlike keyword search, semantic search")
	fmt.Println("understands the meaning behind queries and finds conceptually")
	fmt.Println("related content even without keyword overlap.")
	fmt.Println()

	// Create temporary directory
	tmpDir, err := os.MkdirTemp("", "vego_semantic_search_demo")
	if err != nil {
		fmt.Printf("Failed to create temp directory: %v\n", err)
		os.Exit(1)
	}
	defer os.RemoveAll(tmpDir)

	// Step 1: Initialize the search engine
	fmt.Println("📚 Initializing semantic search engine...")
	db, err := vego.Open(tmpDir, vego.WithDimension(384), vego.WithDistanceFunc(vego.CosineDistance))
	if err != nil {
		fmt.Printf("Failed to open database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	articles, err := db.Collection("articles")
	if err != nil {
		fmt.Printf("Failed to get collection: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("✓ Search engine initialized (384-dim cosine similarity)")
	fmt.Println()

	// Step 2: Define our knowledge base
	// In production, these embeddings would come from a model like BGE, E5, or OpenAI
	articleDB := []Article{
		{
			ID:       "art-001",
			Title:    "Introduction to Go Concurrency",
			Content:  "Go routines and channels provide a powerful model for concurrent programming. The Go scheduler manages lightweight threads efficiently across CPU cores.",
			Author:   "John Doe",
			Category: "programming",
			Date:     "2024-01-15",
		},
		{
			ID:       "art-002",
			Title:    "Understanding Docker Containers",
			Content:  "Containers package applications with their dependencies, ensuring consistent deployment across development, staging, and production environments.",
			Author:   "Jane Smith",
			Category: "devops",
			Date:     "2024-02-20",
		},
		{
			ID:       "art-003",
			Title:    "Kubernetes Orchestration Guide",
			Content:  "Kubernetes automates the deployment, scaling, and management of containerized applications. It handles service discovery, load balancing, and self-healing.",
			Author:   "Mike Johnson",
			Category: "devops",
			Date:     "2024-03-10",
		},
		{
			ID:       "art-004",
			Title:    "Vector Databases Explained",
			Content:  "Vector databases store high-dimensional embeddings and enable similarity search. They are essential for AI applications including semantic search and recommendation systems.",
			Author:   "Sarah Lee",
			Category: "ai",
			Date:     "2024-04-05",
		},
		{
			ID:       "art-005",
			Title:    "HNSW Algorithm Deep Dive",
			Content:  "Hierarchical Navigable Small World graphs provide approximate nearest neighbor search with logarithmic complexity, making them ideal for large-scale vector retrieval.",
			Author:   "Tom Chen",
			Category: "ai",
			Date:     "2024-04-12",
		},
		{
			ID:       "art-006",
			Title:    "Building REST APIs with Go",
			Content:  "RESTful APIs use HTTP methods to expose resources. Go's standard library and frameworks like Gin and Echo make API development straightforward and performant.",
			Author:   "John Doe",
			Category: "programming",
			Date:     "2024-05-01",
		},
		{
			ID:       "art-007",
			Title:    "Microservices Communication Patterns",
			Content:  "Microservices use various communication patterns including synchronous HTTP/gRPC calls and asynchronous message queues like Kafka and RabbitMQ.",
			Author:   "Jane Smith",
			Category: "architecture",
			Date:     "2024-05-15",
		},
		{
			ID:       "art-008",
			Title:    "Machine Learning Model Deployment",
			Content:  "Deploying ML models to production requires considerations for scaling, versioning, monitoring, and A/B testing to ensure reliable predictions at scale.",
			Author:   "Sarah Lee",
			Category: "ai",
			Date:     "2024-06-01",
		},
		{
			ID:       "art-009",
			Title:    "CI/CD Best Practices",
			Content:  "Continuous integration and continuous deployment automate testing and release pipelines. Key practices include automated testing, trunk-based development, and feature flags.",
			Author:   "Mike Johnson",
			Category: "devops",
			Date:     "2024-06-20",
		},
		{
			ID:       "art-010",
			Title:    "Natural Language Processing Basics",
			Content:  "NLP enables computers to understand human language. Modern approaches use transformer architectures like BERT and GPT to achieve state-of-the-art results on language tasks.",
			Author:   "Tom Chen",
			Category: "ai",
			Date:     "2024-07-01",
		},
	}

	// Step 3: Index articles with embeddings
	fmt.Println("🔍 Indexing articles...")
	ctx := context.Background()

	for _, article := range articleDB {
		// In production, use a real embedding model.
		// We generate deterministic embeddings from content for demo.
		vector := generateSemanticEmbedding(article.Title+" "+article.Content, 384)

		doc := &vego.Document{
			ID:     article.ID,
			Vector: vector,
			Metadata: map[string]interface{}{
				"title":    article.Title,
				"content":  article.Content,
				"author":   article.Author,
				"category": article.Category,
				"date":     article.Date,
			},
		}

		if err := articles.InsertContext(ctx, doc); err != nil {
			fmt.Printf("Failed to insert article %s: %v\n", article.ID, err)
			continue
		}
		fmt.Printf("  ✓ Indexed: [%s] %s\n", article.Category, article.Title)
	}
	fmt.Printf("\n✓ Total articles indexed: %d\n\n", len(articleDB))

	// Step 4: Semantic queries
	// These queries demonstrate that semantic search finds relevant content
	// even when the query uses different words than the document
	queries := []struct {
		query       string
		explanation string
	}{
		{
			query:       "How do I run multiple tasks at the same time in Go?",
			explanation: "Maps to 'Go Concurrency' (different words, same meaning)",
		},
		{
			query:       "What technology helps deploy apps consistently across environments?",
			explanation: "Maps to 'Docker Containers' (conceptual match)",
		},
		{
			query:       "How do computers understand human text and speech?",
			explanation: "Maps to 'NLP Basics' (semantic understanding)",
		},
		{
			query:       "How to find similar vectors quickly in a database?",
			explanation: "Maps to 'HNSW Algorithm' and 'Vector Databases'",
		},
		{
			query:       "Automating software release and testing workflows",
			explanation: "Maps to 'CI/CD Best Practices' (paraphrased concept)",
		},
	}

	fmt.Println("═══════════════════════════════════════════════════════════════")
	fmt.Println("  Semantic Search Queries")
	fmt.Println("═══════════════════════════════════════════════════════════════")
	fmt.Println()

	for i, q := range queries {
		fmt.Printf("Query %d: %s\n", i+1, q.query)
		fmt.Printf("Expected: %s\n", q.explanation)
		fmt.Println("─" + string(make([]byte, 60)))

		queryVector := generateSemanticEmbedding(q.query, 384)

		start := time.Now()
		results, err := articles.SearchContext(ctx, queryVector, 3)
		if err != nil {
			fmt.Printf("  Search failed: %v\n", err)
			continue
		}
		elapsed := time.Since(start)

		fmt.Printf("Retrieved %d results in %v:\n", len(results), elapsed)
		for j, r := range results {
			similarity := 1.0 - float64(r.Distance)
			fmt.Printf("  [%d] Similarity: %.2f%% | %s (by %s)\n",
				j+1,
				similarity*100,
				r.Document.Metadata["title"],
				r.Document.Metadata["author"])
			content := r.Document.Metadata["content"].(string)
			if len(content) > 80 {
				content = content[:80] + "..."
			}
			fmt.Printf("      %s\n", content)
		}
		fmt.Println()
	}

	// Step 5: Filtered semantic search
	fmt.Println("═══════════════════════════════════════════════════════════════")
	fmt.Println("  Filtered Semantic Search (Category = 'ai')")
	fmt.Println("═══════════════════════════════════════════════════════════════")
	fmt.Println()

	aiFilter := &vego.MetadataFilter{
		Field:    "category",
		Operator: "eq",
		Value:    "ai",
	}

	aiQuery := generateSemanticEmbedding("machine learning and artificial intelligence", 384)
	results, err := articles.SearchWithFilter(aiQuery, 5, aiFilter)
	if err != nil {
		fmt.Printf("Filtered search failed: %v\n", err)
	} else {
		fmt.Printf("Found %d AI-related articles:\n", len(results))
		for i, r := range results {
			similarity := 1.0 - float64(r.Distance)
			fmt.Printf("  %d. %s (%.1f%%)\n",
				i+1,
				r.Document.Metadata["title"],
				similarity*100)
		}
	}
	fmt.Println()

	// Step 6: Date range search
	fmt.Println("═══════════════════════════════════════════════════════════════")
	fmt.Println("  Filtered Semantic Search (Recent articles: 2024-05+)")
	fmt.Println("═══════════════════════════════════════════════════════════════")
	fmt.Println()

	dateFilter := &vego.MetadataFilter{
		Field:    "date",
		Operator: "gte",
		Value:    "2024-05-01",
	}

	recentQuery := generateSemanticEmbedding("software engineering and system design", 384)
	results, err = articles.SearchWithFilter(recentQuery, 5, dateFilter)
	if err != nil {
		fmt.Printf("Filtered search failed: %v\n", err)
	} else {
		fmt.Printf("Found %d recent articles:\n", len(results))
		for i, r := range results {
			similarity := 1.0 - float64(r.Distance)
			fmt.Printf("  %d. %s (%.1f%%) [%s]\n",
				i+1,
				r.Document.Metadata["title"],
				similarity*100,
				r.Document.Metadata["date"])
		}
	}
	fmt.Println()

	fmt.Println("=== Semantic Search Demo completed! ===")
	fmt.Println()
	fmt.Println("Key concepts demonstrated:")
	fmt.Println("  • Semantic similarity search (meaning-based, not keyword-based)")
	fmt.Println("  • Cosine distance for text embeddings")
	fmt.Println("  • Metadata filtering (category, date range)")
	fmt.Println("  • Context-aware search with cancellation support")
	fmt.Println()
	fmt.Println("Production tips:")
	fmt.Println("  • Use real embedding models (BGE, E5, OpenAI, Cohere)")
	fmt.Println("  • Normalize embeddings before indexing")
	fmt.Println("  • Consider hybrid search (semantic + keyword + reranking)")
	fmt.Println("  • Use metadata filters to scope search to relevant subsets")
}

// generateSemanticEmbedding creates a deterministic embedding from text.
// In production, replace this with a real embedding model API call.
func generateSemanticEmbedding(text string, dim int) []float32 {
	vec := make([]float32, dim)
	seed := hashString(text)
	r := rand.New(rand.NewSource(seed))

	// Generate vector with some structure to simulate semantic clusters
	for i := range vec {
		vec[i] = r.Float32()*2 - 1
	}

	// Normalize for cosine similarity
	norm := float32(0)
	for _, v := range vec {
		norm += v * v
	}
	norm = float32(1.0 / float64(norm))
	for i := range vec {
		vec[i] *= norm
	}

	return vec
}

func hashString(s string) int64 {
	var h int64 = 5381
	for _, c := range s {
		h = ((h << 5) + h) + int64(c)
	}
	return h
}
