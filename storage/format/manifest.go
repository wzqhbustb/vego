package format

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	lerrors "github.com/wzqhbkjdx/vego/storage/errors"
)

// Manifest manages versioning and transaction metadata for Lance files
// This is the foundation for MVCC (Multi-Version Concurrency Control)
type Manifest struct {
	Version       int64             // Monotonically increasing version number
	ParentVersion int64             // Parent version (-1 for initial version)
	Timestamp     int64             // Unix timestamp
	DataFiles     []string          // List of data file paths
	IndexFiles    []string          // List of index file paths (for HNSW)
	Metadata      map[string]string // Transaction metadata
	Committed     bool              // Whether this version is committed
}

// NewManifest creates a new manifest
func NewManifest(version int64) *Manifest {
	return &Manifest{
		Version:       version,
		ParentVersion: version - 1,
		Timestamp:     time.Now().Unix(),
		DataFiles:     make([]string, 0),
		IndexFiles:    make([]string, 0),
		Metadata:      make(map[string]string),
		Committed:     false,
	}
}

// AddDataFile adds a data file to the manifest
func (m *Manifest) AddDataFile(path string) {
	m.DataFiles = append(m.DataFiles, path)
}

// AddIndexFile adds an index file to the manifest
func (m *Manifest) AddIndexFile(path string) {
	m.IndexFiles = append(m.IndexFiles, path)
}

// Commit marks the manifest as committed
func (m *Manifest) Commit() {
	m.Committed = true
	m.Timestamp = time.Now().Unix()
}

// Validate validates the manifest
func (m *Manifest) Validate() error {
	if m.Version < 0 {
		return lerrors.ValidationFailed("validate_manifest", "",
			fmt.Sprintf("invalid version: %d", m.Version))
	}
	if m.ParentVersion >= m.Version {
		return lerrors.New(lerrors.ErrInvalidArgument).
			Op("validate_manifest").
			Context("field", "parent_version").
			Context("parent", m.ParentVersion).
			Context("current", m.Version).
			Context("message", "parent version must be less than current").
			Build()
	}
	if m.Timestamp <= 0 {
		return lerrors.ValidationFailed("validate_manifest", "",
			fmt.Sprintf("invalid timestamp: %d", m.Timestamp))
	}
	return nil
}

// EncodedSize returns the encoded size of the manifest
func (m *Manifest) EncodedSize() int {
	// version(8) + parent_version(8) + timestamp(8) + committed(1)
	size := 8 + 8 + 8 + 1

	// Data files: count(4) + files
	size += 4
	for _, f := range m.DataFiles {
		size += 4 + len(f) // length(4) + string
	}

	// Index files: count(4) + files
	size += 4
	for _, f := range m.IndexFiles {
		size += 4 + len(f)
	}

	// Metadata: count(4) + entries
	size += 4
	for k, v := range m.Metadata {
		size += 4 + len(k) + 4 + len(v)
	}

	return size
}

// WriteTo writes the manifest to a writer
func (m *Manifest) WriteTo(w io.Writer) (int64, error) {
	if err := m.Validate(); err != nil {
		return 0, NewFileError("write manifest", err)
	}

	buf := new(bytes.Buffer)

	// Write fixed fields
	binary.Write(buf, ByteOrder, m.Version)
	binary.Write(buf, ByteOrder, m.ParentVersion)
	binary.Write(buf, ByteOrder, m.Timestamp)

	committedByte := byte(0)
	if m.Committed {
		committedByte = 1
	}
	buf.WriteByte(committedByte)

	// Write data files
	binary.Write(buf, ByteOrder, int32(len(m.DataFiles)))
	for _, file := range m.DataFiles {
		binary.Write(buf, ByteOrder, int32(len(file)))
		buf.WriteString(file)
	}

	// Write index files
	binary.Write(buf, ByteOrder, int32(len(m.IndexFiles)))
	for _, file := range m.IndexFiles {
		binary.Write(buf, ByteOrder, int32(len(file)))
		buf.WriteString(file)
	}

	// Write metadata
	binary.Write(buf, ByteOrder, int32(len(m.Metadata)))
	for k, v := range m.Metadata {
		binary.Write(buf, ByteOrder, int32(len(k)))
		buf.WriteString(k)
		binary.Write(buf, ByteOrder, int32(len(v)))
		buf.WriteString(v)
	}

	n, err := w.Write(buf.Bytes())
	return int64(n), err
}

// ReadFrom reads the manifest from a reader
func (m *Manifest) ReadFrom(r io.Reader) (int64, error) {
	bytesRead := int64(0)

	// Read fixed fields
	if err := binary.Read(r, ByteOrder, &m.Version); err != nil {
		return bytesRead, NewFileError("read manifest version", err)
	}
	bytesRead += 8

	if err := binary.Read(r, ByteOrder, &m.ParentVersion); err != nil {
		return bytesRead, NewFileError("read manifest parent version", err)
	}
	bytesRead += 8

	if err := binary.Read(r, ByteOrder, &m.Timestamp); err != nil {
		return bytesRead, NewFileError("read manifest timestamp", err)
	}
	bytesRead += 8

	committedByte, err := readByte(r)
	if err != nil {
		return bytesRead, err
	}
	m.Committed = committedByte == 1
	bytesRead += 1

	// Read data files
	var dataFileCount int32
	if err := binary.Read(r, ByteOrder, &dataFileCount); err != nil {
		return bytesRead, err
	}
	bytesRead += 4

	m.DataFiles = make([]string, dataFileCount)
	for i := int32(0); i < dataFileCount; i++ {
		file, n, err := readString(r)
		if err != nil {
			return bytesRead, err
		}
		m.DataFiles[i] = file
		bytesRead += n
	}

	// Read index files
	var indexFileCount int32
	if err := binary.Read(r, ByteOrder, &indexFileCount); err != nil {
		return bytesRead, err
	}
	bytesRead += 4

	m.IndexFiles = make([]string, indexFileCount)
	for i := int32(0); i < indexFileCount; i++ {
		file, n, err := readString(r)
		if err != nil {
			return bytesRead, err
		}
		m.IndexFiles[i] = file
		bytesRead += n
	}

	// Read metadata
	var metaCount int32
	if err := binary.Read(r, ByteOrder, &metaCount); err != nil {
		return bytesRead, err
	}
	bytesRead += 4

	m.Metadata = make(map[string]string)
	for i := int32(0); i < metaCount; i++ {
		key, n1, err := readString(r)
		if err != nil {
			return bytesRead, err
		}
		value, n2, err := readString(r)
		if err != nil {
			return bytesRead + n1, err
		}
		m.Metadata[key] = value
		bytesRead += n1 + n2
	}

	// Validate
	if err := m.Validate(); err != nil {
		return bytesRead, err
	}

	return bytesRead, nil
}

// Helper functions
func readByte(r io.Reader) (byte, error) {
	buf := make([]byte, 1)
	_, err := r.Read(buf)
	return buf[0], err
}

func readString(r io.Reader) (string, int64, error) {
	var length int32
	if err := binary.Read(r, ByteOrder, &length); err != nil {
		return "", 4, err
	}

	buf := make([]byte, length)
	n, err := io.ReadFull(r, buf)
	return string(buf), int64(4 + n), err
}

// ManifestManager manages a series of manifests
type ManifestManager struct {
	BasePath       string
	CurrentVersion int64
	VersionHistory []*Manifest
}

// NewManifestManager creates a new manifest manager
func NewManifestManager(basePath string) *ManifestManager {
	return &ManifestManager{
		BasePath:       basePath,
		CurrentVersion: 0,
		VersionHistory: make([]*Manifest, 0),
	}
}

// CreateVersion creates a new version
func (m *ManifestManager) CreateVersion() *Manifest {
	m.CurrentVersion++
	manifest := NewManifest(m.CurrentVersion)
	return manifest
}

// CommitVersion commits a version
func (m *ManifestManager) CommitVersion(manifest *Manifest) error {
	if !manifest.Committed {
		manifest.Commit()
	}
	m.VersionHistory = append(m.VersionHistory, manifest)
	return nil
}

// GetVersion retrieves a specific version
func (m *ManifestManager) GetVersion(version int64) (*Manifest, bool) {
	for _, manifest := range m.VersionHistory {
		if manifest.Version == version {
			return manifest, true
		}
	}
	return nil, false
}

// GetLatestVersion returns the latest committed version
func (m *ManifestManager) GetLatestVersion() *Manifest {
	for i := len(m.VersionHistory) - 1; i >= 0; i-- {
		if m.VersionHistory[i].Committed {
			return m.VersionHistory[i]
		}
	}
	return nil
}
