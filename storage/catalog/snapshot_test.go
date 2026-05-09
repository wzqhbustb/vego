package catalog

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/wzqhbustb/vego/storage/format"
)

func TestNewSnapshot(t *testing.T) {
	tmpDir := t.TempDir()
	s := NewSnapshot(tmpDir, format.V1_2)

	if s.Path != tmpDir {
		t.Errorf("Path = %s, want %s", s.Path, tmpDir)
	}
	if s.DataFile != filepath.Join(tmpDir, DataFileName) {
		t.Errorf("DataFile = %s", s.DataFile)
	}
	if s.MetaFile != filepath.Join(tmpDir, MetaFileName) {
		t.Errorf("MetaFile = %s", s.MetaFile)
	}
	if s.MetaStore == nil {
		t.Error("MetaStore should not be nil")
	}
	if s.DeletionStore == nil {
		t.Error("DeletionStore should not be nil")
	}
}

func TestSnapshotSaveLoadMetaStore(t *testing.T) {
	tmpDir := t.TempDir()
	s1 := NewSnapshot(tmpDir, format.V1_2)
	s1.MetaStore.Put("doc1", HashID("doc1"), DocMeta{ID: "doc1", RowIndex: 0})

	if err := s1.SaveMetaStore(); err != nil {
		t.Fatalf("SaveMetaStore failed: %v", err)
	}

	s2 := NewSnapshot(tmpDir, format.V1_2)
	if err := s2.LoadMetaStore(func(id string) int64 { return -1 }, false, false); err != nil {
		t.Fatalf("LoadMetaStore failed: %v", err)
	}

	if s2.MetaStore.Count() != 1 {
		t.Fatalf("Count = %d, want 1", s2.MetaStore.Count())
	}
}

func TestSnapshotSaveLoadDeletionStore(t *testing.T) {
	tmpDir := t.TempDir()
	s1 := NewSnapshot(tmpDir, format.V1_2)
	s1.DeletionStore.MarkDeleted(5)

	if err := s1.SaveDeletionStore(); err != nil {
		t.Fatalf("SaveDeletionStore failed: %v", err)
	}

	s2 := NewSnapshot(tmpDir, format.V1_2)
	s2.LoadDeletionStore()

	if !s2.DeletionStore.IsDeleted(5) {
		t.Error("row 5 should be deleted after load")
	}
}

func TestSnapshotSaveEmptyDeletionStore(t *testing.T) {
	tmpDir := t.TempDir()
	s1 := NewSnapshot(tmpDir, format.V1_2)
	s1.DeletionStore.MarkDeleted(1)
	if err := s1.SaveDeletionStore(); err != nil {
		t.Fatalf("SaveDeletionStore failed: %v", err)
	}

	// Now clear and save again — file should be removed
	s1.DeletionStore.Clear()
	if err := s1.SaveDeletionStore(); err != nil {
		t.Fatalf("SaveDeletionStore after clear failed: %v", err)
	}

	dvPath := DeletionStorePath(s1.DataFile)
	if _, err := os.Stat(dvPath); err == nil {
		t.Error("DV file should be removed when store is empty")
	}
}

func TestSnapshotFormatVersion(t *testing.T) {
	s := NewSnapshot("/tmp", format.V1_2)

	ver := s.FormatVersion(func() (format.VersionPolicy, error) {
		return format.V1_0, nil
	})
	if ver != format.V1_0.String() {
		t.Errorf("FormatVersion = %s, want %s", ver, format.V1_0.String())
	}

	ver = s.FormatVersion(func() (format.VersionPolicy, error) {
		return format.VersionPolicy{}, os.ErrNotExist
	})
	if ver != format.V1_2.String() {
		t.Errorf("FormatVersion fallback = %s, want %s", ver, format.V1_2.String())
	}
}
