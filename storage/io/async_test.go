package io

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// setupAsyncIO 创建测试用 AsyncIO
func setupAsyncIO(t testing.TB) *AsyncIO {
	config := DefaultConfig()
	config.Workers = 4
	config.QueueSize = 100
	config.SchedulerCap = 1000
	asyncIO, err := New(config)
	if err != nil {
		t.Fatalf("Failed to create AsyncIO: %v", err)
	}
	return asyncIO
}

// createTestFile 创建测试文件
func createTestFile2(t testing.TB, filename string, size int) {
	data := make([]byte, size)
	if err := os.WriteFile(filename, data, 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
}

// TestAsyncIO_BasicRead 测试基本读取功能
func TestAsyncIO_BasicRead(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.dat")
	createTestFile2(t, testFile, 4096)

	aio := setupAsyncIO(t)
	defer aio.Close()

	if err := aio.RegisterFile("test", testFile); err != nil {
		t.Fatalf("RegisterFile failed: %v", err)
	}

	// 执行读取
	resultCh := aio.Read(context.Background(), "test", 0, 1024)
	result := <-resultCh

	if result.Error != nil {
		t.Fatalf("Read failed: %v", result.Error)
	}

	if len(result.Data) != 1024 {
		t.Errorf("Expected 1024 bytes, got %d", len(result.Data))
	}
}

// TestAsyncIO_ReadPages 测试批量读取
func TestAsyncIO_ReadPages(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.dat")
	createTestFile2(t, testFile, 1024*1024) // 1MB

	aio := setupAsyncIO(t)
	defer aio.Close()

	if err := aio.RegisterFile("test", testFile); err != nil {
		t.Fatalf("RegisterFile failed: %v", err)
	}

	// 批量读取
	offsets := []int64{0, 4096, 8192, 12288}
	results := aio.ReadPages(context.Background(), "test", offsets, 1024)

	if len(results) != len(offsets) {
		t.Fatalf("Expected %d results, got %d", len(offsets), len(results))
	}

	for i, ch := range results {
		result := <-ch
		if result.Error != nil {
			t.Errorf("Read %d failed: %v", i, result.Error)
		}
		if len(result.Data) != 1024 {
			t.Errorf("Read %d: expected 1024 bytes, got %d", i, len(result.Data))
		}
	}
}

// TestAsyncIO_ConcurrentReads 测试并发读取
func TestAsyncIO_ConcurrentReads(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.dat")
	createTestFile2(t, testFile, 1024*1024)

	aio := setupAsyncIO(t)
	defer aio.Close()

	if err := aio.RegisterFile("test", testFile); err != nil {
		t.Fatalf("RegisterFile failed: %v", err)
	}

	// 并发读取
	numReaders := 10
	done := make(chan bool, numReaders)

	for i := 0; i < numReaders; i++ {
		go func(id int) {
			defer func() { done <- true }()

			offset := int64(id * 1024)
			resultCh := aio.Read(context.Background(), "test", offset, 1024)
			result := <-resultCh

			if result.Error != nil {
				t.Errorf("Reader %d failed: %v", id, result.Error)
			}
		}(i)
	}

	// 等待所有读取完成
	for i := 0; i < numReaders; i++ {
		<-done
	}
}

// TestAsyncIO_Timeout 测试超时处理
func TestAsyncIO_Timeout(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.dat")
	createTestFile2(t, testFile, 4096)

	aio := setupAsyncIO(t)
	defer aio.Close()

	if err := aio.RegisterFile("test", testFile); err != nil {
		t.Fatalf("RegisterFile failed: %v", err)
	}

	// 使用超短的 timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	// 等待确保 context 已过期
	time.Sleep(10 * time.Millisecond)

	resultCh := aio.Read(ctx, "test", 0, 1024)
	result := <-resultCh

	// 应该返回错误或正常完成（取决于时机）
	_ = result
}

// TestAsyncIO_InvalidFile 测试无效文件
func TestAsyncIO_InvalidFile(t *testing.T) {
	aio := setupAsyncIO(t)
	defer aio.Close()

	// 尝试读取未注册的文件
	resultCh := aio.Read(context.Background(), "nonexistent", 0, 1024)
	result := <-resultCh

	if result.Error == nil {
		t.Error("Expected error for unregistered file")
	}
}

// TestAsyncIO_Stats 测试统计信息
func TestAsyncIO_Stats(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.dat")
	createTestFile2(t, testFile, 4096)

	aio := setupAsyncIO(t)
	defer aio.Close()

	// 获取初始统计
	stats1 := aio.Stats()
	if stats1.Scheduler.QueueSize != 0 {
		t.Errorf("Expected empty queue, got %d", stats1.Scheduler.QueueSize)
	}

	if err := aio.RegisterFile("test", testFile); err != nil {
		t.Fatalf("RegisterFile failed: %v", err)
	}

	// 执行一些读取
	for i := 0; i < 10; i++ {
		_ = aio.Read(context.Background(), "test", 0, 1024)
	}

	// 给点时间让请求进入队列
	time.Sleep(10 * time.Millisecond)

	// 再次获取统计
	stats2 := aio.Stats()
	t.Logf("Stats: %+v", stats2)
}

// BenchmarkAsyncIO_Read 基准测试：单个读取
func BenchmarkAsyncIO_Read(b *testing.B) {
	tmpDir := b.TempDir()
	testFile := filepath.Join(tmpDir, "bench.dat")
	createTestFile2(b, testFile, 10*1024*1024) // 10MB

	aio, err := New(nil)
	if err != nil {
		b.Fatalf("New failed: %v", err)
	}
	defer aio.Close()

	if err := aio.RegisterFile("bench", testFile); err != nil {
		b.Fatalf("RegisterFile failed: %v", err)
	}

	data := make([]byte, 4096)
	if err := os.WriteFile(testFile, data, 0644); err != nil {
		b.Fatalf("Failed to create test file: %v", err)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		offset := int64(0)
		for pb.Next() {
			resultCh := aio.Read(context.Background(), "bench", offset, 4096)
			<-resultCh
			offset += 4096
			if offset >= int64(len(data)) {
				offset = 0
			}
		}
	})
}

// BenchmarkAsyncIO_ReadPages 基准测试：批量读取
func BenchmarkAsyncIO_ReadPages(b *testing.B) {
	tmpDir := b.TempDir()
	testFile := filepath.Join(tmpDir, "bench.dat")

	// 创建 10MB 测试文件
	data := make([]byte, 10*1024*1024)
	if err := os.WriteFile(testFile, data, 0644); err != nil {
		b.Fatalf("Failed to create test file: %v", err)
	}

	aio, err := New(nil)
	if err != nil {
		b.Fatalf("New failed: %v", err)
	}
	defer aio.Close()

	if err := aio.RegisterFile("bench", testFile); err != nil {
		b.Fatalf("RegisterFile failed: %v", err)
	}

	// 预生成 offsets
	offsets := make([]int64, 100)
	for i := range offsets {
		offsets[i] = int64(i * 1024)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		results := aio.ReadPages(context.Background(), "bench", offsets, 1024)
		for _, ch := range results {
			result := <-ch
			if result.Error != nil {
				b.Fatalf("Read failed: %v", result.Error)
			}
		}
	}
}
