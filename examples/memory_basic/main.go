// Command memory_basic demonstrates the core API of the memory package.
//
// It walks through the full lifecycle: Open → Store → Search → Update → Delete → Close.
//
// Usage:
//
//	export OPENAI_API_KEY="sk-..."
//	go run .
//
// For non-OpenAI providers, set OPENAI_BASE_URL as well.
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/wzqhbustb/vego/memory"
)

func main() {
	apiKey := os.Getenv("OPENAI_API_KEY")
	if apiKey == "" {
		log.Fatal("OPENAI_API_KEY environment variable is required")
	}
	baseURL := os.Getenv("OPENAI_BASE_URL") // optional; defaults to OpenAI

	ctx := context.Background()

	// ----------------------------------------------------------------
	// 1. Open a MemoryStore
	// ----------------------------------------------------------------
	store, err := memory.Open("./demo_memory",
		memory.WithLLM(apiKey, baseURL, "gpt-4o-mini", 0.1),
		memory.WithEmbedding(apiKey, baseURL, "text-embedding-3-small", 1536),
		memory.WithSearchLimit(10),
		memory.WithGapStop(0.5),
	)
	if err != nil {
		log.Fatalf("Open: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			log.Printf("Close: %v", err)
		}
		// Clean up demo data
		os.RemoveAll("./demo_memory")
	}()

	// ----------------------------------------------------------------
	// 2. Store memories
	// ----------------------------------------------------------------
	mem1, err := store.Store(ctx, "User prefers dark mode with blue accent colors", []string{"preference", "ui"})
	if err != nil {
		log.Fatalf("Store: %v", err)
	}
	fmt.Printf("Stored: id=%s content=%q\n", mem1.ID, mem1.Content)

	mem2, err := store.Store(ctx, "User is working on a Go project called Vego", []string{"project"})
	if err != nil {
		log.Fatalf("Store: %v", err)
	}
	fmt.Printf("Stored: id=%s content=%q\n", mem2.ID, mem2.Content)

	// ----------------------------------------------------------------
	// 3. Search memories
	// ----------------------------------------------------------------
	results, err := store.Search(ctx, "What are the user's UI preferences?",
		memory.Limit(5),
		memory.MinScore(0.2),
	)
	if err != nil {
		log.Fatalf("Search: %v", err)
	}
	fmt.Printf("\nSearch results for 'UI preferences' (%d found):\n", len(results))
	for i, r := range results {
		fmt.Printf("  %d. [%.3f] %s\n", i+1, r.Score, r.Content)
	}

	// ----------------------------------------------------------------
	// 4. Update a memory (archive-and-create)
	// ----------------------------------------------------------------
	updated, err := store.Update(ctx, mem1.ID,
		"User prefers dark mode with purple accent colors (changed from blue)",
		[]string{"preference", "ui"},
	)
	if err != nil {
		log.Fatalf("Update: %v", err)
	}
	fmt.Printf("\nUpdated: id=%s (was %s) content=%q\n", updated.ID, mem1.ID, updated.Content)

	// Verify the old memory is now archived
	old, err := store.Get(ctx, mem1.ID)
	if err != nil {
		log.Fatalf("Get old: %v", err)
	}
	fmt.Printf("Old memory state: %s, superseded_by: %s\n", old.State, old.SupersededBy)

	// ----------------------------------------------------------------
	// 5. Delete a memory (soft-delete)
	// ----------------------------------------------------------------
	if err := store.Delete(ctx, mem2.ID); err != nil {
		log.Fatalf("Delete: %v", err)
	}
	fmt.Printf("\nDeleted: id=%s\n", mem2.ID)

	// Deleted memories are still accessible by ID
	deleted, err := store.Get(ctx, mem2.ID)
	if err != nil {
		log.Fatalf("Get deleted: %v", err)
	}
	fmt.Printf("Deleted memory state: %s\n", deleted.State)

	// ----------------------------------------------------------------
	// 6. Stats
	// ----------------------------------------------------------------
	stats, err := store.Stats(ctx)
	if err != nil {
		log.Fatalf("Stats: %v", err)
	}
	fmt.Printf("\nStats: total=%d active=%d archived=%d deleted=%d\n",
		stats.Total, stats.Active, stats.Archived, stats.Deleted)
}
