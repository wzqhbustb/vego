package io

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestNewFilePool(t *testing.T) {
	fp := NewFilePool()
	defer fp.Close()

	if fp.handles == nil {
		t.Error("handles map should be initialized")
	}
	if fp.openFile == nil {
		t.Error("openFile function should be set")
	}
}

func TestFilePool_Register(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.txt")

	// 创建文件
	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	fp := NewFilePool()
	defer fp.Close()

	// 测试注册
	if err := fp.Register("file1", testFile); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	// 测试重复注册（相同路径，应该成功）
	if err := fp.Register("file1", testFile); err != nil {
		t.Fatalf("Re-register same path should succeed: %v", err)
	}

	// 测试重复注册（不同路径，应该失败）
	otherFile := filepath.Join(tmpDir, "other.txt")
	if err := os.WriteFile(otherFile, []byte("other"), 0644); err != nil {
		t.Fatalf("Failed to create other file: %v", err)
	}

	err := fp.Register("file1", otherFile)
	if err == nil {
		t.Error("Re-register with different path should fail")
	}
}

func TestFilePool_GetPut(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.txt")

	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	fp := NewFilePool()
	defer fp.Close()

	if err := fp.Register("file1", testFile); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	// 测试获取
	file, err := fp.Get("file1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if file == nil {
		t.Fatal("File handle should not be nil")
	}

	// 检查引用计数
	if fp.GetRefCount("file1") != 1 {
		t.Errorf("Expected refCount 1, got %d", fp.GetRefCount("file1"))
	}

	// 测试释放
	fp.Put("file1", file)

	// 检查引用计数
	if fp.GetRefCount("file1") != 0 {
		t.Errorf("Expected refCount 0, got %d", fp.GetRefCount("file1"))
	}
}

func TestFilePool_Get_NotRegistered(t *testing.T) {
	fp := NewFilePool()
	defer fp.Close()

	_, err := fp.Get("nonexistent")
	if err == nil {
		t.Error("Get non-registered file should fail")
	}
}

func TestFilePool_RefCount_NegativeProtection(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.txt")

	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	fp := NewFilePool()
	defer fp.Close()

	if err := fp.Register("file1", testFile); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	// 多次 Put，测试负引用计数保护
	fp.Put("file1", nil) // refCount 已经是 0，不应该变成负数
	fp.Put("file1", nil) // 再次 Put
	fp.Put("file1", nil) // 第三次 Put

	if fp.GetRefCount("file1") != 0 {
		t.Errorf("RefCount should be 0 (protected from negative), got %d", fp.GetRefCount("file1"))
	}
}

func TestFilePool_ConcurrentAccess(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "concurrent.txt")

	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	fp := NewFilePool()
	defer fp.Close()

	if err := fp.Register("concurrent", testFile); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	// 并发获取和释放
	const numGoroutines = 100
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()

			file, err := fp.Get("concurrent")
			if err != nil {
				t.Errorf("Get failed: %v", err)
				return
			}

			// 模拟一些工作
			fp.Put("concurrent", file)
		}()
	}

	wg.Wait()

	// 最终引用计数应该为 0
	if fp.GetRefCount("concurrent") != 0 {
		t.Errorf("Expected final refCount 0, got %d", fp.GetRefCount("concurrent"))
	}
}

func TestFilePool_Close_WithOpenHandles(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.txt")

	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	fp := NewFilePool()

	if err := fp.Register("file1", testFile); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	// 获取但不释放
	file, err := fp.Get("file1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	_ = file // 故意不释放

	// 关闭应该返回警告
	err = fp.Close()
	if err == nil {
		t.Error("Close with open handles should return warning")
	}
}

func TestFilePool_Stats(t *testing.T) {
	tmpDir := t.TempDir()

	fp := NewFilePool()
	defer fp.Close()

	// 创建多个文件
	for i := 0; i < 5; i++ {
		testFile := filepath.Join(tmpDir, "file"+string(rune('0'+i))+".txt")
		if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}

		if err := fp.Register("file"+string(rune('0'+i)), testFile); err != nil {
			t.Fatalf("Register failed: %v", err)
		}
	}

	// 获取一些引用
	file0, _ := fp.Get("file0")
	file1, _ := fp.Get("file1")
	_ = file1

	stats := fp.Stats()

	if stats.TotalFiles != 5 {
		t.Errorf("Expected 5 files, got %d", stats.TotalFiles)
	}
	if stats.TotalReferences != 2 { // file0 和 file1
		t.Errorf("Expected 2 references, got %d", stats.TotalReferences)
	}

	fp.Put("file0", file0)
}
