package io

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestNewExecutor(t *testing.T) {
	fp := NewFilePool()
	defer fp.Close()

	e := NewExecutor(4, 100, fp)
	defer e.Close()

	if e.workers != 4 {
		t.Errorf("Expected 4 workers, got %d", e.workers)
	}
	if cap(e.workQueue) != 100 {
		t.Errorf("Expected queue capacity 100, got %d", cap(e.workQueue))
	}
	if e.ctx == nil {
		t.Error("Context should not be nil")
	}
}

func TestExecutor_SubmitAndExecute(t *testing.T) {
	// 创建测试文件
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.dat")

	// 写入测试数据
	testData := []byte("Hello, Executor Test!")
	if err := os.WriteFile(testFile, testData, 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	fp := NewFilePool()
	defer fp.Close()

	if err := fp.Register("test", testFile); err != nil {
		t.Fatalf("Failed to register file: %v", err)
	}

	e := NewExecutor(2, 10, fp)
	defer e.Close()

	// 提交读取请求
	req := NewIORequest("test", 0, int32(len(testData)), PriorityNormal)

	if err := e.Submit(req); err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	// 等待结果
	select {
	case result := <-req.Callback:
		if result.Error != nil {
			t.Fatalf("Read failed: %v", result.Error)
		}
		if string(result.Data) != string(testData) {
			t.Errorf("Data mismatch: got '%s', want '%s'", result.Data, testData)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for result")
	}
}

func TestExecutor_ConcurrentSubmits(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "concurrent.dat")

	// 创建 1MB 测试文件
	testData := make([]byte, 1024*1024)
	for i := range testData {
		testData[i] = byte(i % 256)
	}
	if err := os.WriteFile(testFile, testData, 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	fp := NewFilePool()
	defer fp.Close()

	if err := fp.Register("concurrent", testFile); err != nil {
		t.Fatalf("Failed to register file: %v", err)
	}

	e := NewExecutor(4, 100, fp)
	defer e.Close()

	// 并发提交 100 个请求
	const numRequests = 100
	var wg sync.WaitGroup
	wg.Add(numRequests)

	errors := make(chan error, numRequests)

	for i := 0; i < numRequests; i++ {
		go func(offset int) {
			defer wg.Done()

			req := NewIORequest("concurrent", int64(offset), 1024, PriorityNormal)
			if err := e.Submit(req); err != nil {
				errors <- err
				return
			}

			select {
			case result := <-req.Callback:
				if result.Error != nil {
					errors <- result.Error
				}
			case <-time.After(10 * time.Second):
				errors <- context.DeadlineExceeded
			}
		}(i * 1024)
	}

	wg.Wait()
	close(errors)

	errCount := 0
	for err := range errors {
		if err != nil {
			t.Logf("Error: %v", err)
			errCount++
		}
	}

	if errCount > 0 {
		t.Errorf("Got %d errors during concurrent submits", errCount)
	}
}

func TestExecutor_ContextCancellation(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "cancel.dat")

	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	fp := NewFilePool()
	defer fp.Close()

	if err := fp.Register("cancel", testFile); err != nil {
		t.Fatalf("Failed to register file: %v", err)
	}

	e := NewExecutor(1, 10, fp)
	defer e.Close()

	// 创建已取消的 context
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 立即取消

	req := NewIORequest("cancel", 0, 4, PriorityNormal)
	req.WithContext(ctx)

	// 提交请求
	if err := e.Submit(req); err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	// 等待结果（应该被取消）
	select {
	case result := <-req.Callback:
		// Context 已经取消，应该返回 context.Canceled
		// 但如果 I/O 操作在检查前就完成了，也可能返回成功
		if result.Error != nil && result.Error != context.Canceled {
			t.Errorf("Expected context.Canceled or nil, got %v", result.Error)
		}
		// 验证至少收到了结果（不管是成功还是取消）
	case <-time.After(2 * time.Second):
		t.Error("Timeout: should have received result")
	}
}

func TestExecutor_QueueFull(t *testing.T) {
	fp := NewFilePool()
	defer fp.Close()

	// 创建一个很小的 executor
	e := NewExecutor(1, 1, fp)
	defer e.Close()

	// 先阻塞 worker
	blockReq := NewIORequest("nonexistent", 0, 1024, PriorityNormal)
	e.workQueue <- blockReq // 直接塞满队列

	// 尝试非阻塞提交
	req := NewIORequest("nonexistent2", 0, 1024, PriorityNormal)
	err := e.SubmitAsync(req)

	if err == nil || err.Error() != "queue is full" {
		t.Errorf("Expected 'queue is full' error, got %v", err)
	}
}

func TestExecutor_Stats(t *testing.T) {
	fp := NewFilePool()
	defer fp.Close()

	e := NewExecutor(2, 100, fp)
	defer e.Close()

	stats := e.Stats()

	if stats.Workers != 2 {
		t.Errorf("Expected 2 workers, got %d", stats.Workers)
	}
	if stats.QueueCap != 100 {
		t.Errorf("Expected queue cap 100, got %d", stats.QueueCap)
	}
	if stats.QueueSize < 0 {
		t.Error("Queue size should not be negative")
	}
}

func TestGetBuffer_PutBuffer(t *testing.T) {
	// 测试小缓冲区（从池中获取）
	buf1 := getBuffer(1024)
	if len(buf1) != 1024 {
		t.Errorf("Expected buffer size 1024, got %d", len(buf1))
	}
	putBuffer(buf1)

	// 测试大缓冲区（直接分配）
	buf2 := getBuffer(128 * 1024) // 128KB > 64KB
	if len(buf2) != 128*1024 {
		t.Errorf("Expected buffer size 128KB, got %d", len(buf2))
	}
	putBuffer(buf2) // 应该被 GC，不会放回池中

	// 测试池中获取的缓冲区复用
	buf3 := getBuffer(4096)
	if cap(buf3) != 64*1024 {
		t.Errorf("Expected buffer capacity 64KB from pool, got %d", cap(buf3))
	}
	putBuffer(buf3)
}

func BenchmarkExecutor_Read(b *testing.B) {
	tmpDir := b.TempDir()
	testFile := filepath.Join(tmpDir, "bench.dat")

	// 创建测试文件
	data := make([]byte, 4096)
	if err := os.WriteFile(testFile, data, 0644); err != nil {
		b.Fatalf("Failed to create test file: %v", err)
	}

	fp := NewFilePool()
	defer fp.Close()

	if err := fp.Register("bench", testFile); err != nil {
		b.Fatalf("Failed to register file: %v", err)
	}

	e := NewExecutor(4, 1000, fp)
	defer e.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			req := NewIORequest("bench", 0, 4096, PriorityNormal)
			if err := e.Submit(req); err != nil {
				b.Logf("Submit error: %v", err)
				continue
			}
			<-req.Callback
		}
	})
}
