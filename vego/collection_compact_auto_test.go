package vego

import (
	"fmt"
	"testing"
	"time"
)

// TestAutoCompactEnabled tests that auto-compaction is enabled correctly
func TestAutoCompactEnabled(t *testing.T) {
	tmpDir := t.TempDir()

	// Create collection with auto-compact enabled
	config := DefaultConfig()
	config.AutoCompact = true
	config.CompactThreshold = 0.3
	config.CompactMinInterval = 1 // 1 second for testing

	coll, err := NewCollection("test", tmpDir, config)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}
	defer coll.Close()

	// Check status
	status := coll.GetCompactStatus()
	if status.State != CompactIdle {
		t.Errorf("Expected initial state Idle, got %s", status.State)
	}

	t.Log("Auto-compact enabled test passed")
}

// TestAutoCompactDisabled tests that auto-compaction can be disabled
func TestAutoCompactDisabled(t *testing.T) {
	tmpDir := t.TempDir()

	// Create collection with auto-compact disabled
	config := DefaultConfig()
	config.AutoCompact = false

	coll, err := NewCollection("test", tmpDir, config)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}
	defer coll.Close()

	// Check status - should report disabled
	status := coll.GetCompactStatus()
	if status.State != CompactIdle {
		t.Errorf("Expected state Idle, got %s", status.State)
	}
	if status.Message != "Auto-compaction disabled" {
		t.Errorf("Expected disabled message, got: %s", status.Message)
	}

	// Trigger should fail
	if err := coll.TriggerCompact(); err == nil {
		t.Error("Expected error when triggering disabled auto-compact")
	}

	t.Log("Auto-compact disabled test passed")
}

// TestManualCompactTrigger tests manual compaction trigger
func TestManualCompactTrigger(t *testing.T) {
	tmpDir := t.TempDir()

	config := DefaultConfig()
	config.AutoCompact = true
	config.CompactMinInterval = 3600 // 1 hour to prevent auto-trigger

	coll, err := NewCollection("test", tmpDir, config)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}
	defer coll.Close()

	// Insert and flush documents
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

	// Delete one document
	if err := coll.Delete("doc-002"); err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	// Trigger manual compaction
	if err := coll.TriggerCompact(); err != nil {
		t.Fatalf("Failed to trigger compact: %v", err)
	}

	// Wait for compaction to complete
	time.Sleep(500 * time.Millisecond)

	// Verify
	if count := coll.Count(); count != 2 {
		t.Errorf("Expected count 2, got %d", count)
	}

	t.Log("Manual compact trigger test passed")
}

// TestCompactStatusTransitions tests status transitions
func TestCompactStatusTransitions(t *testing.T) {
	tmpDir := t.TempDir()

	config := DefaultConfig()
	config.AutoCompact = true
	config.CompactMinInterval = 3600

	coll, err := NewCollection("test", tmpDir, config)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}
	defer coll.Close()

	// Initial status
	status := coll.GetCompactStatus()
	if status.State != CompactIdle {
		t.Errorf("Expected initial Idle state, got %s", status.State)
	}

	// Verify state string representations
	states := []CompactState{
		CompactIdle, CompactChecking, CompactCompacting,
		CompactCompleted, CompactFailed,
	}
	expected := []string{"Idle", "Checking", "Compacting", "Completed", "Failed"}

	for i, state := range states {
		if state.String() != expected[i] {
			t.Errorf("State %d: expected %s, got %s", i, expected[i], state.String())
		}
	}

	t.Log("Compact status transitions test passed")
}

// TestCompactStateString tests the String() method
func TestCompactStateString(t *testing.T) {
	tests := []struct {
		state    CompactState
		expected string
	}{
		{CompactIdle, "Idle"},
		{CompactChecking, "Checking"},
		{CompactCompacting, "Compacting"},
		{CompactCompleted, "Completed"},
		{CompactFailed, "Failed"},
		{CompactState(99), "Unknown"},
	}

	for _, tt := range tests {
		if got := tt.state.String(); got != tt.expected {
			t.Errorf("State %d: expected %s, got %s", tt.state, tt.expected, got)
		}
	}

	t.Log("Compact state string test passed")
}
