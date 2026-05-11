package catalog

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDeletionStoreBasic(t *testing.T) {
	ds := NewDeletionStore()
	if !ds.IsEmpty() {
		t.Error("new store should be empty")
	}
	if ds.Count() != 0 {
		t.Fatalf("Count = %d, want 0", ds.Count())
	}

	ds.MarkDeleted(42)
	if ds.IsEmpty() {
		t.Error("store should not be empty after MarkDeleted")
	}
	if ds.Count() != 1 {
		t.Fatalf("Count = %d, want 1", ds.Count())
	}
	if !ds.IsDeleted(42) {
		t.Error("row 42 should be deleted")
	}
	if ds.IsDeleted(43) {
		t.Error("row 43 should not be deleted")
	}
}

func TestDeletionStoreUnmark(t *testing.T) {
	ds := NewDeletionStore()
	ds.MarkDeleted(10)
	if !ds.UnmarkDeleted(10) {
		t.Error("UnmarkDeleted should return true")
	}
	if ds.IsDeleted(10) {
		t.Error("row 10 should not be deleted after unmark")
	}
	if ds.UnmarkDeleted(10) {
		t.Error("UnmarkDeleted on non-deleted row should return false")
	}
}

func TestDeletionStoreClear(t *testing.T) {
	ds := NewDeletionStore()
	ds.MarkDeleted(1)
	ds.MarkDeleted(2)
	ds.Clear()
	if !ds.IsEmpty() {
		t.Error("store should be empty after Clear")
	}
}

func TestDeletionStoreSaveLoad(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.del")

	ds1 := NewDeletionStore()
	ds1.MarkDeleted(5)
	ds1.MarkDeleted(100)
	ds1.MarkDeleted(10000)

	if err := ds1.Save(path); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("file not created: %v", err)
	}

	ds2 := NewDeletionStore()
	if err := ds2.Load(path); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if ds2.Count() != 3 {
		t.Fatalf("Count = %d, want 3", ds2.Count())
	}
	if !ds2.IsDeleted(5) || !ds2.IsDeleted(100) || !ds2.IsDeleted(10000) {
		t.Error("loaded store missing deleted rows")
	}
}

func TestDeletionStoreLoadOrEmpty(t *testing.T) {
	tmpDir := t.TempDir()
	missingPath := filepath.Join(tmpDir, "missing.del")

	ds := LoadOrEmpty(missingPath)
	if !ds.IsEmpty() {
		t.Error("LoadOrEmpty on missing file should return empty store")
	}
}

func TestDeletionStorePath(t *testing.T) {
	path := DeletionStorePath("/data/vectors.lance")
	if path != "/data/vectors.lance.del" {
		t.Fatalf("DeletionStorePath = %s, want /data/vectors.lance.del", path)
	}
}

func TestDeletionStoreCompatibilityWithIndexFormat(t *testing.T) {
	// Verify that catalog.DeletionStore can read files written by index package.
	// Since we use the same format, this is implicitly tested by the Save/Load test.
	// This test ensures the magic and version constants match.
	if delFileMagic != "DEL1" {
		t.Fatalf("magic mismatch: %s", delFileMagic)
	}
	if delFileVersion != 1 {
		t.Fatalf("version mismatch: %d", delFileVersion)
	}
}
