package vfs

import (
	"path/filepath"
	"testing"
)

func TestReadFile(t *testing.T) {
	tmpDir := t.TempDir()
	fs := Local

	path := filepath.Join(tmpDir, "readfile.txt")
	want := []byte("hello readfile")

	if err := WriteFile(fs, path, want, 0644); err != nil {
		t.Fatalf("WriteFile setup failed: %v", err)
	}

	got, err := ReadFile(fs, path)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("ReadFile data mismatch: got %q, want %q", got, want)
	}
}

func TestReadFileNotFound(t *testing.T) {
	tmpDir := t.TempDir()
	fs := Local

	path := filepath.Join(tmpDir, "nonexistent.txt")
	_, err := ReadFile(fs, path)
	if err == nil {
		t.Fatal("expected error for nonexistent file")
	}
}

func TestWriteFile(t *testing.T) {
	tmpDir := t.TempDir()
	fs := Local

	path := filepath.Join(tmpDir, "writefile.txt")
	data := []byte("hello writefile")

	if err := WriteFile(fs, path, data, 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	got, err := ReadFile(fs, path)
	if err != nil {
		t.Fatalf("ReadFile after WriteFile failed: %v", err)
	}
	if string(got) != string(data) {
		t.Fatalf("WriteFile data mismatch: got %q, want %q", got, data)
	}
}

func TestWriteFileOverwrite(t *testing.T) {
	tmpDir := t.TempDir()
	fs := Local

	path := filepath.Join(tmpDir, "overwrite.txt")

	if err := WriteFile(fs, path, []byte("old"), 0644); err != nil {
		t.Fatalf("first WriteFile failed: %v", err)
	}
	if err := WriteFile(fs, path, []byte("new"), 0644); err != nil {
		t.Fatalf("second WriteFile failed: %v", err)
	}

	got, err := ReadFile(fs, path)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if string(got) != "new" {
		t.Fatalf("expected overwritten content 'new', got %q", got)
	}
}
