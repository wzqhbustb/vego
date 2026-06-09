package catalog

import (
	"path/filepath"

	"github.com/wzqhbustb/vego/core"
	"github.com/wzqhbustb/vego/storage/format"
	"github.com/wzqhbustb/vego/vfs"
)

const (
	// DataFileName is the default name for the vector data file.
	DataFileName = "vectors.lance"
	// MetaFileName is the default name for the metadata file.
	MetaFileName = "metadata.json"
)

// Snapshot is the collection-level state metadata.
// It holds the current state of a collection's catalog: metadata mappings,
// deletion marks, format version, and file paths.
//
// It is the single source of truth for collection state that needs to be
// persisted atomically. The MetaStore and DeletionStore are the two primary
// stateful components managed by Snapshot.
type Snapshot struct {
	Path          string
	DataFile      string
	MetaFile      string
	Version       format.VersionPolicy
	MetaStore     *MetadataStore
	DeletionStore *DeletionStore
	fs            vfs.VFS // filesystem abstraction; defaults to vfs.Local
}

// NewSnapshot creates a new Snapshot for the given directory using the default local VFS.
func NewSnapshot(dirPath string, version format.VersionPolicy) *Snapshot {
	return NewSnapshotWithVFS(dirPath, version, vfs.Local)
}

// NewSnapshotWithVFS creates a new Snapshot for the given directory with a custom VFS.
func NewSnapshotWithVFS(dirPath string, version format.VersionPolicy, fs vfs.VFS) *Snapshot {
	dataFile := filepath.Join(dirPath, DataFileName)
	return &Snapshot{
		Path:          dirPath,
		DataFile:      dataFile,
		MetaFile:      filepath.Join(dirPath, MetaFileName),
		Version:       version,
		MetaStore:     NewMetadataStoreWithVFS(filepath.Join(dirPath, MetaFileName), fs),
		DeletionStore: NewDeletionStoreWithVFS(fs),
		fs:            fs,
	}
}

// LoadMetaStore loads the metadata store from disk, with optional repair
// for unset RowIndex values.
func (s *Snapshot) LoadMetaStore(lookupRowIndex func(id string) int64, fileExists bool, supportsRowIndex bool) error {
	return s.MetaStore.LoadWithRepair(lookupRowIndex, fileExists, supportsRowIndex)
}

// SaveMetaStore persists the metadata store to disk.
func (s *Snapshot) SaveMetaStore() error {
	return s.MetaStore.Save()
}

// SaveDeletionStore persists the deletion store to disk.
// If the store is empty, any existing file is removed.
func (s *Snapshot) SaveDeletionStore() error {
	dvPath := DeletionStorePath(s.DataFile)
	if s.DeletionStore.IsEmpty() {
		if _, err := s.fs.Stat(dvPath); err == nil {
			return s.fs.Remove(dvPath)
		}
		return nil
	}
	return s.DeletionStore.Save(dvPath)
}

// LoadDeletionStore loads the deletion store from disk if a file exists.
// If no file exists, the DeletionStore remains empty.
func (s *Snapshot) LoadDeletionStore() {
	dvPath := DeletionStorePath(s.DataFile)
	if _, err := s.fs.Stat(dvPath); err == nil {
		s.DeletionStore = LoadOrEmptyWithVFS(dvPath, s.fs)
	}
}

// DataFileSize returns the size of the data file, or 0 if it doesn't exist.
func (s *Snapshot) DataFileSize() int64 {
	if info, err := s.fs.Stat(s.DataFile); err == nil {
		return info.Size()
	}
	return 0
}

// MetaFileSize returns the size of the metadata file, or 0 if it doesn't exist.
func (s *Snapshot) MetaFileSize() int64 {
	if info, err := s.fs.Stat(s.MetaStore.Path()); err == nil {
		return info.Size()
	}
	return 0
}

// FormatVersion returns the effective format version string.
// It prefers the actual file version if available, otherwise falls back to
// the configured version.
func (s *Snapshot) FormatVersion(getFileVersion func() (format.VersionPolicy, error)) string {
	ver := s.Version.String()
	if fileVer, err := getFileVersion(); err == nil {
		ver = fileVer.String()
	}
	return ver
}

// DocumentCount returns the number of documents in the metadata store.
func (s *Snapshot) DocumentCount() int {
	return s.MetaStore.Count()
}

// DeletedCount returns the number of deleted rows.
func (s *Snapshot) DeletedCount() int {
	return s.DeletionStore.Count()
}

// Schema creates the Arrow schema for vector storage based on dimension.
func (s *Snapshot) Schema(dimension int) *core.Schema {
	return core.NewSchema([]core.Field{
		{Name: "id_hash", Type: core.PrimInt64(), Nullable: false},
		{Name: "vector", Type: core.VectorType(dimension), Nullable: false},
		{Name: "timestamp", Type: core.PrimInt64(), Nullable: false},
	}, nil)
}
