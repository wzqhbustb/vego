package io

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestStress_1000ConcurrentRequests(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "stress.dat")
	
	// 创建 10MB 测试文件
	data := make([]byte, 10*1024*1024)
	if err := os.WriteFile(testFile, data, 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	
	aio, err := New(nil)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer aio.Close()
	
	if err := aio.RegisterFile("stress", testFile); err != nil {
		t.Fatalf("RegisterFile failed: %v", err)
	}
	
	// 1000 并发请求
	const numRequests = 1000
	var wg sync.WaitGroup
	wg.Add(numRequests)
	
	errors := make(chan error, numRequests)
	start := time.Now()
	
	for i := 0; i < numRequests; i++ {
		go func(offset int) {
			defer wg.Done()
			
			resultCh := aio.Read(context.Background(), "stress", int64(offset), 4096)
			select {
			case result := <-resultCh:
				if result.Error != nil {
					errors <- result.Error
				}
			case <-time.After(30 * time.Second):
				errors <- context.DeadlineExceeded
			}
		}(i * 4096)
	}
	
	wg.Wait()
	close(errors)
	elapsed := time.Since(start)
	
	errCount := 0
	for err := range errors {
		if err != nil {
			errCount++
		}
	}
	
	t.Logf("Processed %d requests in %v, %d errors", numRequests, elapsed, errCount)
	
	if errCount > 0 {
		t.Errorf("Got %d errors during stress test", errCount)
	}
}
