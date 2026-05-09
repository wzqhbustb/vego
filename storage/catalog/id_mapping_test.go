package catalog

import (
	"testing"
)

func TestIDMappingPutGet(t *testing.T) {
	m := NewIDMapping()
	m.Put("doc1", 100)

	nodeID, ok := m.Map("doc1")
	if !ok || nodeID != 100 {
		t.Fatalf("Map doc1 = (%d, %v), want (100, true)", nodeID, ok)
	}

	docID, ok := m.Reverse(100)
	if !ok || docID != "doc1" {
		t.Fatalf("Reverse 100 = (%s, %v), want (doc1, true)", docID, ok)
	}
}

func TestIDMappingUpdate(t *testing.T) {
	m := NewIDMapping()
	m.Put("doc1", 100)
	m.Put("doc1", 200)

	nodeID, _ := m.Map("doc1")
	if nodeID != 200 {
		t.Fatalf("Map after update = %d, want 200", nodeID)
	}

	// Old reverse mapping should be overwritten
	_, ok := m.Reverse(100)
	if ok {
		t.Error("Reverse of old node ID should not exist")
	}

	docID, ok := m.Reverse(200)
	if !ok || docID != "doc1" {
		t.Fatalf("Reverse 200 = (%s, %v), want (doc1, true)", docID, ok)
	}
}

func TestIDMappingDelete(t *testing.T) {
	m := NewIDMapping()
	m.Put("doc1", 100)
	m.Delete("doc1")

	_, ok := m.Map("doc1")
	if ok {
		t.Error("Map should return false after Delete")
	}

	// Reverse mapping is intentionally preserved (delayed cleanup)
	docID, ok := m.Reverse(100)
	if !ok || docID != "doc1" {
		t.Fatalf("Reverse should be preserved for delayed cleanup: got (%s, %v)", docID, ok)
	}
}

func TestIDMappingCount(t *testing.T) {
	m := NewIDMapping()
	if m.Count() != 0 {
		t.Fatalf("Count = %d, want 0", m.Count())
	}
	m.Put("a", 1)
	m.Put("b", 2)
	if m.Count() != 2 {
		t.Fatalf("Count = %d, want 2", m.Count())
	}
	m.Delete("a")
	if m.Count() != 1 {
		t.Fatalf("Count = %d, want 1", m.Count())
	}
}

func TestIDMappingAll(t *testing.T) {
	m := NewIDMapping()
	m.Put("a", 1)
	m.Put("b", 2)

	all := m.All()
	if len(all) != 2 {
		t.Fatalf("len(All) = %d, want 2", len(all))
	}

	// Modifying the returned map should not affect the store
	delete(all, "a")
	if m.Count() != 2 {
		t.Error("All returned a non-isolated map")
	}
}

func TestIDMappingReplace(t *testing.T) {
	m := NewIDMapping()
	m.Put("a", 1)
	m.Put("b", 2)

	newDocToNode := map[string]int{"c": 3, "d": 4}
	newNodeToDoc := map[int]string{3: "c", 4: "d"}
	m.Replace(newDocToNode, newNodeToDoc)

	if m.Count() != 2 {
		t.Fatalf("Count after Replace = %d, want 2", m.Count())
	}

	_, ok := m.Map("a")
	if ok {
		t.Error("old mapping should be gone after Replace")
	}

	docID, ok := m.Reverse(3)
	if !ok || docID != "c" {
		t.Fatalf("Reverse 3 = (%s, %v), want (c, true)", docID, ok)
	}
}
