package vfs

import (
	"os"
	"path/filepath"
	"testing"
)

// 编译期断言：*os.File 满足 vfs.File 接口
var _ File = (*os.File)(nil)

// 编译期断言：localVFS 满足 vfs.VFS 接口
var _ VFS = (*localVFS)(nil)

func TestLocalVFS(t *testing.T) {
	tmpDir := t.TempDir()
	fs := Local

	// Test Create + Write + Close
	path := filepath.Join(tmpDir, "test.txt")
	f, err := fs.Create(path)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	data := []byte("hello vfs")
	if _, err := f.Write(data); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Test Stat
	info, err := fs.Stat(path)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if info.Size() != int64(len(data)) {
		t.Fatalf("Stat size mismatch: got %d, want %d", info.Size(), len(data))
	}

	// Test Open + Read + Close
	f, err = fs.Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	buf := make([]byte, len(data))
	if _, err := f.Read(buf); err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if string(buf) != string(data) {
		t.Fatalf("Read data mismatch: got %q, want %q", buf, data)
	}
	if f.Name() == "" {
		t.Fatalf("Name() returned empty")
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Test Rename
	newPath := filepath.Join(tmpDir, "renamed.txt")
	if err := fs.Rename(path, newPath); err != nil {
		t.Fatalf("Rename failed: %v", err)
	}
	if _, err := fs.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("Old file should not exist after rename")
	}
	if _, err := fs.Stat(newPath); err != nil {
		t.Fatalf("New file should exist after rename: %v", err)
	}

	// Test Remove
	if err := fs.Remove(newPath); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}
	if _, err := fs.Stat(newPath); !os.IsNotExist(err) {
		t.Fatalf("File should not exist after remove")
	}

	// Test MkdirAll
	dir := filepath.Join(tmpDir, "a", "b", "c")
	if err := fs.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	if info, err := fs.Stat(dir); err != nil || !info.IsDir() {
		t.Fatalf("MkdirAll did not create directory")
	}

	// Test OpenFile with O_RDONLY
	path2 := filepath.Join(tmpDir, "readonly.txt")
	if err := os.WriteFile(path2, []byte("ro"), 0644); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	f, err = fs.OpenFile(path2, os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	roBuf := make([]byte, 2)
	if _, err := f.Read(roBuf); err != nil {
		t.Fatalf("Read from OpenFile failed: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestLocalVFSReadAtWriteAt(t *testing.T) {
	tmpDir := t.TempDir()
	fs := Local
	path := filepath.Join(tmpDir, "rw.txt")

	f, err := fs.Create(path)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// WriteAt at offset 4
	if _, err := f.WriteAt([]byte("world"), 4); err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Re-open and ReadAt
	f, err = fs.Open(path)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	buf := make([]byte, 5)
	if _, err := f.ReadAt(buf, 4); err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if string(buf) != "world" {
		t.Fatalf("ReadAt data mismatch: got %q, want %q", buf, "world")
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}
