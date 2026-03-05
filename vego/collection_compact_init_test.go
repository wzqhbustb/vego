package vego

import (
	"testing"
	"time"
)

// TestLastCompactTimeInitialization verifies that lastCompactTime is properly initialized
// to prevent immediate max interval trigger on startup
func TestLastCompactTimeInitialization(t *testing.T) {
	tmpDir := t.TempDir()

	config := DefaultConfig()
	config.AutoCompact = true
	config.CompactThreshold = 1.0   // 100% (impossible to reach)
	config.CompactMinInterval = 0   // No minimum
	config.CompactMaxInterval = 3600 // 1 hour

	start := time.Now()
	coll, err := NewCollection("test", tmpDir, config)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}
	defer coll.Close()

	// Check that lastCompactTime was initialized (not zero)
	if coll.lastCompactTime.IsZero() {
		t.Error("lastCompactTime should be initialized to time.Now(), not zero")
	}

	// Check that it's recent (within last second)
	if time.Since(coll.lastCompactTime) > time.Second {
		t.Errorf("lastCompactTime should be recent, got %v ago", time.Since(coll.lastCompactTime))
	}

	// Check shouldAutoCompact - should NOT trigger due to max interval
	// because we just initialized lastCompactTime
	should, reason := coll.shouldAutoCompact()
	if should {
		t.Errorf("Should not trigger immediately after creation, got: %s", reason)
	}

	t.Logf("Collection created at %v, lastCompactTime=%v (delta=%v)",
		start, coll.lastCompactTime, time.Since(coll.lastCompactTime))
	t.Log("LastCompactTime initialization test passed")
}
