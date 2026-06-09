package vfs

import "os"

// Local is the default local filesystem VFS implementation.
// It directly delegates to the standard os package.
var Local VFS = &localVFS{}

type localVFS struct{}

func (l *localVFS) Create(name string) (File, error) {
	return os.Create(name)
}

func (l *localVFS) Open(name string) (File, error) {
	return os.Open(name)
}

func (l *localVFS) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	return os.OpenFile(name, flag, perm)
}

func (l *localVFS) Remove(name string) error {
	return os.Remove(name)
}

func (l *localVFS) Rename(oldpath, newpath string) error {
	return os.Rename(oldpath, newpath)
}

func (l *localVFS) MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}

func (l *localVFS) Stat(name string) (os.FileInfo, error) {
	return os.Stat(name)
}
