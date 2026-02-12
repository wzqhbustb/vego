// RAG (Retrieval-Augmented Generation) Example
// This example demonstrates how to use Vego for building a local RAG system.
// RAG combines vector search with LLMs to provide context-aware responses.
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

// KnowledgeDocument represents a piece of knowledge in our RAG system
type KnowledgeDocument struct {
	ID       string
	Content  string
	Source   string
	Category string
	Vector   []float32
}

func main() {
	fmt.Println("=== Vego RAG (Retrieval-Augmented Generation) Demo ===")
	fmt.Println()

	// Create temporary directory
	tmpDir, _ := os.MkdirTemp("", "vego_rag_demo")
	defer os.RemoveAll(tmpDir)

	// Step 1: Initialize the knowledge base
	fmt.Println("📚 Initializing knowledge base...")
	db, err := vego.Open(tmpDir, vego.WithDimension(384)) // Using 384-dim for sentence embeddings
	if err != nil {
		panic(err)
	}
	defer db.Close()

	kb, err := db.Collection("knowledge_base")
	if err != nil {
		panic(err)
	}
	fmt.Printf("✓ Knowledge base initialized\n\n")

	// Step 2: Load knowledge documents
	// In a real RAG system, these would come from documents, websites, etc.
	knowledgeBase := []KnowledgeDocument{
		{
			ID:       "kb-001",
			Content:  "Go is an open-source programming language developed by Google. It was designed for simplicity, concurrency, and fast compilation. Go is widely used for cloud-native applications, microservices, and DevOps tools.",
			Source:   "golang.org",
			Category: "programming",
		},
		{
			ID:       "kb-002",
			Content:  "Docker is a platform for developing, shipping, and running applications in containers. Containers are lightweight, portable, and ensure consistency across different environments.",
			Source:   "docker.com",
			Category: "devops",
		},
		{
			ID:       "kb-003",
			Content:  "Kubernetes is an open-source container orchestration platform. It automates the deployment, scaling, and management of containerized applications across clusters of hosts.",
			Source:   "kubernetes.io",
			Category: "devops",
		},
		{
			ID:       "kb-004",
			Content:  "Vector databases are specialized databases designed to store and query high-dimensional vectors. They enable similarity search, which is essential for AI applications like semantic search and recommendation systems.",
			Source:   "ai-tech-blog.com",
			Category: "ai",
		},
		{
			ID:       "kb-005",
			Content:  "HNSW (Hierarchical Navigable Small World) is an algorithm for approximate nearest neighbor search in high-dimensional spaces. It provides fast search with high recall rates.",
			Source:   "research-paper.com",
			Category: "ai",
		},
		{
			ID:       "kb-006",
			Content:  "REST APIs are architectural constraints for designing networked applications. They use HTTP methods (GET, POST, PUT, DELETE) and are stateless, meaning each request contains all information needed to complete it.",
			Source:   "restful-api.net",
			Category: "programming",
		},
		{
			ID:       "kb-007",
			Content:  "gRPC is a high-performance RPC framework developed by Google. It uses Protocol Buffers for serialization and HTTP/2 for transport, enabling efficient communication between microservices.",
			Source:   "grpc.io",
			Category: "programming",
		},
		{
			ID:       "kb-008",
			Content:  "Machine Learning is a subset of AI that enables systems to learn from data without explicit programming. Common applications include image recognition, natural language processing, and predictive analytics.",
			Source:   "ml-course.edu",
			Category: "ai",
		},
	}

	// Step 3: Index the knowledge base
	fmt.Println("🔍 Indexing knowledge documents...")
	ctx := context.Background()

	for _, kd := range knowledgeBase {
		// In a real system, you would use an embedding model here
		// For demo, we generate a deterministic vector based on content
		vector := generateEmbedding(kd.Content, 384)

		doc := &vego.Document{
			ID:     kd.ID,
			Vector: vector,
			Metadata: map[string]interface{}{
				"content":  kd.Content,
				"source":   kd.Source,
				"category": kd.Category,
			},
		}

		if err := kb.InsertContext(ctx, doc); err != nil {
			panic(err)
		}
		fmt.Printf("  ✓ Indexed: [%s] %.50s...\n", kd.Category, kd.Content)
	}
	fmt.Printf("\n✓ Total documents indexed: %d\n\n", len(knowledgeBase))

	// Step 4: Simulate user queries
	queries := []string{
		"How do I deploy applications using containers?",
		"What is the best way to search similar vectors?",
		"Tell me about Google's programming language",
		"How do microservices communicate with each other?",
	}

	fmt.Println("💬 Processing user queries...\n")

	for i, query := range queries {
		fmt.Printf("Query %d: %s\n", i+1, query)
		fmt.Println(string(make([]byte, 60))) // separator line

		// Generate query embedding (in real system, use same model as indexing)
		queryVector := generateEmbedding(query, 384)

		// Retrieve relevant documents
		start := time.Now()
		results, err := kb.SearchContext(ctx, queryVector, 2) // Top-2 most relevant
		if err != nil {
			panic(err)
		}
		elapsed := time.Since(start)

		fmt.Printf("Retrieved in %v:\n", elapsed)

		// Display retrieved context
		var contexts []string
		for j, r := range results {
			content := r.Document.Metadata["content"].(string)
			source := r.Document.Metadata["source"].(string)
			similarity := 1.0 - float64(r.Distance) // Rough similarity estimate

			fmt.Printf("  [%d] Similarity: %.2f%% (from %s)\n", j+1, similarity*100, source)
			fmt.Printf("      %.80s...\n\n", content)

			contexts = append(contexts, content)
		}

		// Simulate LLM response generation (in real system, call LLM API here)
		fmt.Println("🤖 Generated Response (simulated):")
		generateSimulatedResponse(query, contexts)
		fmt.Println()
	}

	// Step 5: Category-specific search
	fmt.Println("📂 Category-specific search:")
	fmt.Println("Searching for 'AI' category documents only...")

	filter := &vego.MetadataFilter{
		Field:    "category",
		Operator: "eq",
		Value:    "ai",
	}

	queryVector := generateEmbedding("machine learning algorithms", 384)
	results, err := kb.SearchWithFilter(queryVector, 10, filter)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Found %d AI-related documents:\n", len(results))
	for i, r := range results {
		fmt.Printf("  %d. %s (from %s)\n",
			i+1,
			r.Document.Metadata["content"].(string)[:50],
			r.Document.Metadata["source"])
	}

	fmt.Println()
	fmt.Println("=== RAG Demo completed! ===")
	fmt.Println()
	fmt.Println("Key RAG concepts demonstrated:")
	fmt.Println("  • Knowledge base indexing with embeddings")
	fmt.Println("  • Semantic similarity search for context retrieval")
	fmt.Println("  • Metadata filtering for category-specific search")
	fmt.Println("  • Context assembly for LLM augmentation")
}

// generateEmbedding creates a simple deterministic embedding from text
// In production, use a real embedding model like OpenAI, BGE, or Sentence-Transformers
func generateEmbedding(text string, dim int) []float32 {
	vec := make([]float32, dim)
	seed := hashString(text)
	r := rand.New(rand.NewSource(seed))

	// Generate vector influenced by text content
	for i := range vec {
		vec[i] = r.Float32()*2 - 1 // Range [-1, 1]
	}

	// Normalize
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

// hashString creates a simple hash from string for deterministic seeding
func hashString(s string) int64 {
	var h int64 = 5381
	for _, c := range s {
		h = ((h << 5) + h) + int64(c)
	}
	return h
}

// generateSimulatedResponse simulates an LLM response based on retrieved context
func generateSimulatedResponse(query string, contexts []string) {
	// In a real RAG system, this would call an LLM API like OpenAI, Claude, etc.
	// Here we simulate a response based on the retrieved context

	fmt.Println("  Based on the retrieved context:")

	if len(contexts) > 0 {
		// Extract key information from first context
		firstSentence := contexts[0]
		if len(firstSentence) > 100 {
			firstSentence = firstSentence[:100] + "..."
		}
		fmt.Printf("  %s\n", firstSentence)

		if len(contexts) > 1 {
			fmt.Println()
			fmt.Println("  Additionally, related concepts include:")
			secondSentence := contexts[1]
			if len(secondSentence) > 80 {
				secondSentence = secondSentence[:80] + "..."
			}
			fmt.Printf("  - %s\n", secondSentence)
		}
	}

	fmt.Println()
	fmt.Println("  [Note: This is a simulated response. In production, integrate with an LLM API]")
}
