package vego

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestCompactConcurrencyPrevention verifies that concurrent triggers don't start multiple compactions
func TestCompactConcurrencyPrevention(t *testing.T) {
	tmpDir := t.TempDir()

	config := DefaultConfig()
	config.AutoCompact = false // Disable auto for manual control

	coll, err := NewCollection("test", tmpDir, config)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}
	defer coll.Close()

	// Insert many documents to make compaction slower
	for i := 1; i <= 200; i++ {
		doc := &Document{
			ID:     fmt.Sprintf("doc-%04d", i),
			Vector: randomVector(128, i),
		}
		if err := coll.Insert(doc); err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}
	flushCollection(t, coll)

	// Delete many documents to trigger compaction
	for i := 1; i <= 100; i++ {
		if err := coll.Delete(fmt.Sprintf("doc-%04d", i)); err != nil {
			t.Fatalf("Failed to delete: %v", err)
		}
	}

	// Concurrently trigger compaction 10 times
	var wg sync.WaitGroup
	compactCount := 0
	var countMu sync.Mutex

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			
			// Small stagger to increase race chance
			time.Sleep(time.Duration(id) * 5 * time.Millisecond)
			
			// Use doAutoCompact directly to test concurrency protection
			coll.compactMu.RLock()
			if !coll.compacting && coll.shouldAutoCompactInternal() {
				coll.compactMu.RUnlock()
				
				// Try to start compaction
				coll.doAutoCompact(fmt.Sprintf("concurrent trigger %d", id))
				
				countMu.Lock()
				compactCount++
				countMu.Unlock()
			} else {
				coll.compactMu.RUnlock()
			}
		}(i)
	}

	wg.Wait()

	// Only one compaction should have executed
	if compactCount > 1 {
		t.Errorf("Expected at most 1 compaction, got %d (concurrent prevention failed)", compactCount)
	} else {
		t.Logf("Concurrent prevention worked: %d compaction(s) executed", compactCount)
	}

	// Verify data is consistent
	if count := coll.Count(); count != 100 {
		t.Errorf("Expected 100 documents, got %d", count)
	}
}

// Helper to check shouldAutoCompact without double lock
func (c *Collection) shouldAutoCompactInternal() bool {
	// Check minimum interval
	if time.Since(c.lastCompactTime) < time.Duration(c.config.CompactMinInterval)*time.Second {
		return false
	}
	// Check deletion rate
	stats := c.Stats()
	return stats.DeletionRate >= c.config.CompactThreshold
}
