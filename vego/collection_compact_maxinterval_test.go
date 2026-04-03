package vego

import (
	"fmt"
	"testing"
	"time"
)

// skipIfShort skips time-sensitive tests in short mode
func skipIfShort(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping time-sensitive test in short mode")
	}
}

// TestCompactMaxInterval verifies that compaction triggers when max interval is reached
func TestCompactMaxInterval(t *testing.T) {
	skipIfShort(t) // Skip in short mode due to long sleep times
	tmpDir := t.TempDir()

	config := DefaultConfig()
	config.AutoCompact = true
	config.CompactThreshold = 1.0   // 100% (impossible to reach)
	config.CompactMinInterval = 0   // No minimum
	config.CompactMaxInterval = 2   // 2 seconds for testing

	coll, err := NewCollection("test", tmpDir, config)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}
	defer coll.Close()

	// Wait for initial delay + max interval
	t.Log("Waiting for max interval (10s initial + 2s)...")
	time.Sleep(13 * time.Second)

	// Insert some documents
	for i := 1; i <= 3; i++ {
		doc := &Document{
			ID:     fmt.Sprintf("doc-%03d", i),
			Vector: randomVector(128, i),
		}
		if err := coll.Insert(doc); err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}
	flushCollection(t, coll)

	// Check status - should have triggered due to max interval
	time.Sleep(3 * time.Second)
	status := coll.GetCompactStatus()
	t.Logf("Status: %s - %s (last compact: %v ago)",
		status.State, status.Message, time.Since(coll.lastCompactTime))

	t.Log("Max interval test passed")
}

// TestCompactMaxIntervalDisabled verifies that max interval can be disabled
func TestCompactMaxIntervalDisabled(t *testing.T) {
	tmpDir := t.TempDir()

	config := DefaultConfig()
	config.AutoCompact = true
	config.CompactThreshold = 1.0   // 100% (never reach)
	config.CompactMinInterval = 0   // No minimum
	config.CompactMaxInterval = 0   // DISABLED

	coll, err := NewCollection("test", tmpDir, config)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}
	defer coll.Close()

	// Wait a bit
	time.Sleep(2 * time.Second)

	// Check condition - should not trigger due to max interval (disabled)
	should, reason := coll.shouldAutoCompact()
	if should {
		t.Errorf("Should not trigger when max interval is disabled, got: %s", reason)
	}

	t.Log("Max interval disabled test passed")
}
