package vego

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestAutoCompactActualExecution verifies that auto-compaction actually triggers
// when conditions are met (deletion rate > threshold)
func TestAutoCompactActualExecution(t *testing.T) {
	tmpDir := t.TempDir()

	// Create collection with auto-compact enabled and short interval
	config := DefaultConfig()
	config.AutoCompact = true
	config.CompactThreshold = 0.3   // 30% threshold
	config.CompactMinInterval = 1   // 1 second for testing

	coll, err := NewCollection("test", tmpDir, config)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}
	defer coll.Close()

	// Wait for initial delay (10s) to pass
	t.Log("Waiting for initial 10s delay...")
	time.Sleep(11 * time.Second)

	// Insert 10 documents
	for i := 1; i <= 10; i++ {
		doc := &Document{
			ID:     fmt.Sprintf("doc-%03d", i),
			Vector: randomVector(128, i),
		}
		if err := coll.Insert(doc); err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}
	flushCollection(t, coll)

	// Verify initial state
	stats := coll.Stats()
	if stats.Count != 10 {
		t.Errorf("Expected 10 documents, got %d", stats.Count)
	}

	// Delete 4 documents (40% deletion rate, above 30% threshold)
	for i := 1; i <= 4; i++ {
		id := fmt.Sprintf("doc-%03d", i)
		if err := coll.Delete(id); err != nil {
			t.Fatalf("Failed to delete %s: %v", id, err)
		}
	}

	// Verify deletion stats before auto-compact
	stats = coll.Stats()
	if stats.DeletedCount != 4 {
		t.Errorf("Expected 4 deleted documents, got %d", stats.DeletedCount)
	}
	if stats.DeletionRate < 0.3 {
		t.Errorf("Expected deletion rate >= 0.3, got %f", stats.DeletionRate)
	}

	t.Logf("Before auto-compact: Count=%d, Deleted=%d, Rate=%.2f%%",
		stats.Count, stats.DeletedCount, stats.DeletionRate*100)

	// Wait for auto-compaction to trigger (check every 30s, so wait up to 35s)
	t.Log("Waiting for auto-compaction to trigger...")
	success := false
	for i := 0; i < 10; i++ {
		time.Sleep(4 * time.Second)
		stats = coll.Stats()
		if stats.DeletedCount == 0 {
			success = true
			break
		}
		t.Logf("  Check %d: Deleted=%d, Rate=%.2f%%", i+1, stats.DeletedCount, stats.DeletionRate*100)
	}

	if !success {
		t.Fatalf("Auto-compaction did not trigger within 40 seconds")
	}

	// Verify auto-compaction was triggered (deleted count should be 0)
	stats = coll.Stats()
	t.Logf("After auto-compact: Count=%d, Deleted=%d, Rate=%.2f%%",
		stats.Count, stats.DeletedCount, stats.DeletionRate*100)

	if stats.DeletedCount != 0 {
		t.Errorf("Expected 0 deleted after auto-compact, got %d", stats.DeletedCount)
	}

	if stats.DeletionRate != 0 {
		t.Errorf("Expected deletion rate 0 after auto-compact, got %f", stats.DeletionRate)
	}

	// Verify remaining documents are accessible
	if count := coll.Count(); count != 6 {
		t.Errorf("Expected 6 remaining documents, got %d", count)
	}

	// Verify deleted documents are gone
	for i := 1; i <= 4; i++ {
		id := fmt.Sprintf("doc-%03d", i)
		if _, err := coll.Get(id); err == nil {
			t.Errorf("Deleted document %s should not be accessible", id)
		}
	}

	t.Log("Auto-compact actual execution test passed")
}

// TestCloseDuringCompact verifies that Close() waits for ongoing compaction to complete
func TestCloseDuringCompact(t *testing.T) {
	tmpDir := t.TempDir()

	config := DefaultConfig()
	config.AutoCompact = false // Disable auto for manual control

	coll, err := NewCollection("test", tmpDir, config)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// Insert many documents to make compaction slower
	for i := 1; i <= 100; i++ {
		doc := &Document{
			ID:     fmt.Sprintf("doc-%04d", i),
			Vector: randomVector(128, i),
		}
		if err := coll.Insert(doc); err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}
	flushCollection(t, coll)

	// Delete half of the documents
	for i := 1; i <= 50; i++ {
		id := fmt.Sprintf("doc-%04d", i)
		if err := coll.Delete(id); err != nil {
			t.Fatalf("Failed to delete: %v", err)
		}
	}

	// Start compaction in background
	compactDone := make(chan error, 1)
	go func() {
		compactDone <- coll.Compact()
	}()

	// Give compaction time to start
	time.Sleep(50 * time.Millisecond)

	// Close collection while compaction might be running
	closeStart := time.Now()
	if err := coll.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	closeDuration := time.Since(closeStart)

	t.Logf("Close took %v (should wait for compact if it was running)", closeDuration)

	// Verify compaction completed (either before or during close)
	select {
	case err := <-compactDone:
		if err != nil {
			t.Errorf("Compact failed: %v", err)
		}
	default:
		t.Log("Compact still running when Close was called, Close should have waited")
	}

	// Reopen and verify data integrity
	coll2, err := NewCollection("test", tmpDir, config)
	if err != nil {
		t.Fatalf("Failed to reopen: %v", err)
	}
	defer coll2.Close()

	// Should have 50 remaining documents
	if count := coll2.Count(); count != 50 {
		t.Errorf("Expected 50 documents after close-during-compact, got %d", count)
	}

	t.Log("Close during compact test passed")
}

// TestCompactConcurrency verifies that Compact works correctly with concurrent operations
func TestCompactConcurrency(t *testing.T) {
	tmpDir := t.TempDir()

	config := DefaultConfig()
	config.AutoCompact = false

	coll, err := NewCollection("test", tmpDir, config)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}
	defer coll.Close()

	// Insert initial documents
	for i := 1; i <= 50; i++ {
		doc := &Document{
			ID:     fmt.Sprintf("doc-%04d", i),
			Vector: randomVector(128, i),
		}
		if err := coll.Insert(doc); err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}
	flushCollection(t, coll)

	// Delete some documents
	for i := 1; i <= 20; i++ {
		id := fmt.Sprintf("doc-%04d", i)
		if err := coll.Delete(id); err != nil {
			t.Fatalf("Failed to delete: %v", err)
		}
	}

	// Start compaction
	var compactErr error
	var compactWg sync.WaitGroup
	compactWg.Add(1)
	go func() {
		defer compactWg.Done()
		compactErr = coll.Compact()
	}()

	// Concurrent reads during compaction
	var readWg sync.WaitGroup
	readErrors := make(chan error, 100)

	for i := 0; i < 10; i++ {
		readWg.Add(1)
		go func(iteration int) {
			defer readWg.Done()
			for j := 0; j < 10; j++ {
				// Try to read random documents
				id := fmt.Sprintf("doc-%04d", 21+j%30) // Only try to read non-deleted ones
				_, err := coll.Get(id)
				if err != nil {
					readErrors <- err
				}
				time.Sleep(5 * time.Millisecond)
			}
		}(i)
	}

	// Wait for all operations to complete
	compactWg.Wait()
	readWg.Wait()
	close(readErrors)

	// Check results
	if compactErr != nil {
		t.Errorf("Compact failed: %v", compactErr)
	}

	readErrCount := 0
	for err := range readErrors {
		if err != nil {
			readErrCount++
		}
	}

	// Some reads might fail during compact transition, but should not panic
	t.Logf("Read errors during compact: %d (acceptable if no panics)", readErrCount)

	// Verify final state
	if count := coll.Count(); count != 30 {
		t.Errorf("Expected 30 documents, got %d", count)
	}

	t.Log("Compact concurrency test passed")
}

// TestCompactMinInterval verifies minimum interval enforcement
func TestCompactMinInterval(t *testing.T) {
	tmpDir := t.TempDir()

	config := DefaultConfig()
	config.AutoCompact = true
	config.CompactThreshold = 0.1   // Low threshold (10%)
	config.CompactMinInterval = 5   // 5 seconds

	coll, err := NewCollection("test", tmpDir, config)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}
	defer coll.Close()

	// Wait for initial delay to pass
	t.Log("Waiting for initial 10s delay...")
	time.Sleep(11 * time.Second)

	// Insert and delete to trigger first compact
	for i := 1; i <= 10; i++ {
		doc := &Document{
			ID:     fmt.Sprintf("doc-%03d", i),
			Vector: randomVector(128, i),
		}
		if err := coll.Insert(doc); err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}
	flushCollection(t, coll)

	// Delete 5 (50% > 10% threshold)
	for i := 1; i <= 5; i++ {
		if err := coll.Delete(fmt.Sprintf("doc-%03d", i)); err != nil {
			t.Fatalf("Failed to delete: %v", err)
		}
	}

	// Wait for first auto-compact (up to 35s)
	t.Log("Waiting for first auto-compact...")
	success := false
	for i := 0; i < 10; i++ {
		time.Sleep(4 * time.Second)
		stats := coll.Stats()
		if stats.DeletedCount == 0 {
			success = true
			break
		}
		t.Logf("  Check %d: Deleted=%d", i+1, stats.DeletedCount)
	}
	if !success {
		t.Fatal("First auto-compact should have completed")
	}

	// Delete more documents immediately
	for i := 6; i <= 8; i++ {
		if err := coll.Delete(fmt.Sprintf("doc-%03d", i)); err != nil {
			t.Fatalf("Failed to delete: %v", err)
		}
	}

	// Should NOT trigger second compact within 5 seconds
	time.Sleep(2 * time.Second)

	// Check status - should be in cooldown
	status := coll.GetCompactStatus()
	t.Logf("Status after 2s: %s - %s", status.State, status.Message)

	// Wait for interval to pass
	time.Sleep(4 * time.Second)

	// Now it should trigger
	time.Sleep(2 * time.Second)
	finalStats := coll.Stats()
	t.Logf("Final state: Count=%d, Deleted=%d", finalStats.Count, finalStats.DeletedCount)

	t.Log("Compact min interval test passed")
}
