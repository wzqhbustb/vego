package vfs

import (
	"fmt"
	"io"
	"os"
)

// ReadFile reads the named file and returns the contents.
// It is the VFS-aware equivalent of os.ReadFile.
func ReadFile(fs VFS, name string) ([]byte, error) {
	f, err := fs.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("read file %s: %w", name, err)
	}
	return data, nil
}

// WriteFile writes data to the named file, creating it if necessary.
// It is the VFS-aware equivalent of os.WriteFile.
func WriteFile(fs VFS, name string, data []byte, perm os.FileMode) error {
	f, err := fs.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}

	_, err = f.Write(data)
	if err1 := f.Close(); err1 != nil && err == nil {
		err = err1
	}
	return err
}
