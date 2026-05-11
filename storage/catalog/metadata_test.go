package catalog

import (
	"os"
	"path/filepath"
	"testing"
)

func TestHashID(t *testing.T) {
	// HashID should be deterministic
	h1 := HashID("doc1")
	h2 := HashID("doc1")
	if h1 != h2 {
		t.Errorf("HashID not deterministic: %d != %d", h1, h2)
	}

	// Different IDs should (very likely) produce different hashes
	h3 := HashID("doc2")
	if h1 == h3 {
		t.Errorf("HashID collision for different IDs: %d", h1)
	}
}

func TestMetadataStorePutGet(t *testing.T) {
	s := NewMetadataStore("")

	meta := DocMeta{ID: "doc1", RowIndex: 42, Metadata: map[string]interface{}{"key": "value"}}
	s.Put("doc1", HashID("doc1"), meta)

	// GetByID
	got, ok := s.GetByID("doc1")
	if !ok {
		t.Fatal("expected document to exist")
	}
	if got.RowIndex != 42 {
		t.Errorf("RowIndex = %d, want 42", got.RowIndex)
	}

	// GetByHash
	hash := HashID("doc1")
	got2, ok := s.GetByHash(hash)
	if !ok {
		t.Fatal("expected document to exist by hash")
	}
	if got2.ID != "doc1" {
		t.Errorf("ID = %s, want doc1", got2.ID)
	}
}

func TestMetadataStoreDelete(t *testing.T) {
	s := NewMetadataStore("")

	meta := DocMeta{ID: "doc1", RowIndex: 0}
	s.Put("doc1", HashID("doc1"), meta)
	if s.Count() != 1 {
		t.Fatalf("Count = %d, want 1", s.Count())
	}

	s.Delete("doc1", HashID("doc1"))
	if s.Count() != 0 {
		t.Fatalf("Count = %d, want 0", s.Count())
	}

	_, ok := s.GetByID("doc1")
	if ok {
		t.Error("expected document to be deleted")
	}
}

func TestMetadataStoreSaveLoad(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "metadata.json")

	s1 := NewMetadataStore(path)
	s1.Put("doc1", HashID("doc1"), DocMeta{ID: "doc1", RowIndex: 0, Metadata: map[string]interface{}{"a": 1}})
	s1.Put("doc2", HashID("doc2"), DocMeta{ID: "doc2", RowIndex: 1, Metadata: map[string]interface{}{"b": 2}})

	if err := s1.Save(); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// Verify file exists
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("metadata file not created: %v", err)
	}

	s2 := NewMetadataStore(path)
	if err := s2.Load(); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if s2.Count() != 2 {
		t.Fatalf("Count = %d, want 2", s2.Count())
	}

	m1, ok := s2.GetByID("doc1")
	if !ok || m1.RowIndex != 0 {
		t.Errorf("doc1 RowIndex = %d, want 0", m1.RowIndex)
	}
}

func TestMetadataStoreLoadWithRepair(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "metadata.json")

	s1 := NewMetadataStore(path)
	s1.Put("doc1", HashID("doc1"), DocMeta{ID: "doc1", RowIndex: -1})
	s1.Put("doc2", HashID("doc2"), DocMeta{ID: "doc2", RowIndex: 0})
	if err := s1.Save(); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	s2 := NewMetadataStore(path)
	lookup := func(id string) int64 {
		if id == "doc1" {
			return 42
		}
		return -1
	}
	if err := s2.LoadWithRepair(lookup, true, true); err != nil {
		t.Fatalf("LoadWithRepair failed: %v", err)
	}

	m1, _ := s2.GetByID("doc1")
	if m1.RowIndex != 42 {
		t.Errorf("doc1 RowIndex after repair = %d, want 42", m1.RowIndex)
	}

	m2, _ := s2.GetByID("doc2")
	if m2.RowIndex != 0 {
		t.Errorf("doc2 RowIndex = %d, want 0 (old format, no repair)", m2.RowIndex)
	}
}

func TestMetadataStoreAllEntries(t *testing.T) {
	s := NewMetadataStore("")
	s.Put("a", HashID("a"), DocMeta{ID: "a"})
	s.Put("b", HashID("b"), DocMeta{ID: "b"})

	all := s.AllEntries()
	if len(all) != 2 {
		t.Fatalf("len(AllEntries) = %d, want 2", len(all))
	}

	// Modifying the returned map should not affect the store
	for k := range all {
		delete(all, k)
	}
	if s.Count() != 2 {
		t.Error("AllEntries returned a non-isolated map")
	}
}
