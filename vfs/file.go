package vfs

import (
	"io"
	"os"
)

// File is the unified file handle interface for all I/O operations.
// *os.File satisfies this interface natively.
type File interface {
	io.Reader
	io.Writer
	io.ReaderAt
	io.WriterAt
	io.Seeker
	io.Closer
	Sync() error
	Stat() (os.FileInfo, error)
	Name() string
}

// VFS is the filesystem abstraction layer.
// All file operations in Vego should go through this interface.
type VFS interface {
	// Create creates a new file with mode 0666 (before umask).
	Create(name string) (File, error)
	// Open opens a file for reading.
	Open(name string) (File, error)
	// OpenFile is the general file opening function.
	OpenFile(name string, flag int, perm os.FileMode) (File, error)
	// Remove removes a file.
	Remove(name string) error
	// RemoveAll removes a path and any children it contains.
	RemoveAll(path string) error
	// Rename renames (moves) a file.
	Rename(oldpath, newpath string) error
	// MkdirAll creates a directory and all necessary parents.
	MkdirAll(path string, perm os.FileMode) error
	// ReadDir reads the named directory, returning all its directory entries.
	ReadDir(name string) ([]os.DirEntry, error)
	// Stat returns file info.
	Stat(name string) (os.FileInfo, error)
}
