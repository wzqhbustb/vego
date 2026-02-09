package io

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"
)

// AsyncIO 是异步 I/O 的主接口
// 封装了 Scheduler、Executor 和 FilePool
type AsyncIO struct {
	scheduler *Scheduler
	executor  *Executor
	filePool  *FilePool

	mu     sync.RWMutex
	closed bool
}

// Config AsyncIO 配置
type Config struct {
	Workers      int // Worker goroutine 数量
	QueueSize    int // Executor 队列大小
	SchedulerCap int // Scheduler 队列容量
}

// DefaultConfig 返回默认配置
func DefaultConfig() *Config {
	return &Config{
		Workers:      4,
		QueueSize:    1000,
		SchedulerCap: 10000,
	}
}

// New 创建一个新的 AsyncIO 实例
func New(cfg *Config) (*AsyncIO, error) {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	// 1. 创建文件池
	filePool := NewFilePool()

	// 2. 创建 Executor
	executor := NewExecutor(cfg.Workers, cfg.QueueSize, filePool)

	// 3. 创建 Scheduler
	scheduler := NewScheduler(executor, cfg.SchedulerCap)

	return &AsyncIO{
		scheduler: scheduler,
		executor:  executor,
		filePool:  filePool,
	}, nil
}

// RegisterFile 注册文件到 AsyncIO
func (a *AsyncIO) RegisterFile(fileID string, path string) error {
	a.mu.RLock()
	if a.closed {
		a.mu.RUnlock()
		return fmt.Errorf("asyncio is closed")
	}
	a.mu.RUnlock()

	return a.filePool.Register(fileID, path)
}

// Read 异步读取
// 返回的 channel 会收到读取结果
func (a *AsyncIO) Read(ctx context.Context, fileID string, offset int64, size int32) <-chan IOResult {
	a.mu.RLock()
	if a.closed {
		a.mu.RUnlock()
		ch := make(chan IOResult, 1)
		ch <- IOResult{Error: fmt.Errorf("asyncio is closed")}
		close(ch)
		return ch
	}
	a.mu.RUnlock()

	req := NewIORequest(fileID, offset, size, PriorityNormal)
	req.WithContext(ctx)

	// 提交请求
	if err := a.scheduler.Submit(req); err != nil {
		ch := make(chan IOResult, 1)
		ch <- IOResult{Error: err}
		close(ch)
		return ch
	}

	return req.Callback
}

// ReadPages 批量读取多个 Page
// 适用于列式扫描场景
// 修复：使用 SubmitBatch 批量提交
func (a *AsyncIO) ReadPages(ctx context.Context, fileID string, offsets []int64, size int32) []<-chan IOResult {
	results := make([]<-chan IOResult, len(offsets))

	a.mu.RLock()
	if a.closed {
		a.mu.RUnlock()
		// 如果已关闭，返回错误给所有 channel
		for i := range results {
			ch := make(chan IOResult, 1)
			ch <- IOResult{Error: fmt.Errorf("asyncio is closed")}
			close(ch)
			results[i] = ch
		}
		return results
	}
	a.mu.RUnlock()

	// 创建批量请求
	reqs := make([]*IORequest, len(offsets))
	for i, offset := range offsets {
		req := NewIORequest(fileID, offset, size, PriorityNormal)
		req.WithContext(ctx)
		reqs[i] = req
		results[i] = req.Callback
	}

	// 批量提交
	if err := a.scheduler.SubmitBatch(reqs); err != nil {
		// 如果批量提交失败，返回错误给所有 channel
		for i := range results {
			ch := make(chan IOResult, 1)
			ch <- IOResult{Error: err}
			close(ch)
			results[i] = ch
		}
	}

	return results
}

// Write 异步写入
func (a *AsyncIO) Write(ctx context.Context, fileID string, offset int64, data []byte) <-chan IOResult {
	a.mu.RLock()
	if a.closed {
		a.mu.RUnlock()
		ch := make(chan IOResult, 1)
		ch <- IOResult{Error: fmt.Errorf("asyncio is closed")}
		close(ch)
		return ch
	}
	a.mu.RUnlock()

	req := NewIOWriteRequest(fileID, offset, data, PriorityNormal)
	req.WithContext(ctx)

	if err := a.scheduler.Submit(req); err != nil {
		ch := make(chan IOResult, 1)
		ch <- IOResult{Error: err}
		close(ch)
		return ch
	}

	return req.Callback
}

// Stats 返回 AsyncIO 统计信息
func (a *AsyncIO) Stats() AsyncIOStats {
	return AsyncIOStats{
		Scheduler: a.scheduler.Stats(),
		Executor:  a.executor.Stats(),
		FilePool:  a.filePool.Stats(),
	}
}

// Close 关闭 AsyncIO
// 确保优雅关闭：先停止接收新请求，再等待现有任务完成
func (a *AsyncIO) Close() error {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return nil // 幂等操作
	}
	a.closed = true
	a.mu.Unlock()

	// 1. 停止 Scheduler（不再接收新请求，取消队列中的请求）
	if err := a.scheduler.Stop(); err != nil {
		return fmt.Errorf("scheduler stop failed: %w", err)
	}

	// 2. 等待 Executor 完成所有正在执行的任务
	//    设置超时避免无限等待
	done := make(chan struct{})
	go func() {
		a.executor.Close()
		close(done)
	}()

	// 使用 Timer 避免泄漏
	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()

	select {
	case <-done:
		// Executor 正常关闭
	case <-timer.C:
		// 超时强制关闭（后台 goroutine 会继续完成 executor.Close()）
		return fmt.Errorf("executor close timeout after 30s")
	}

	// 3. 关闭文件池
	if err := a.filePool.Close(); err != nil {
		return fmt.Errorf("filepool close failed: %w", err)
	}

	return nil
}

// AsyncIOStats AsyncIO 统计信息
type AsyncIOStats struct {
	Scheduler SchedulerStats
	Executor  ExecutorStats
	FilePool  FilePoolStats
}

// async.go 中添加

// GetRegisteredFilePath 获取已注册文件的完整路径
// 用于 Reader 初始化时将文件注册到 AsyncIO
func (a *AsyncIO) GetRegisteredFilePath(fileID string) (string, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if a.closed {
		return "", fmt.Errorf("asyncio is closed")
	}

	// 直接访问 filePool 的 handles（需要添加方法或导出）
	return a.filePool.GetFilePath(fileID)
}

func (a *AsyncIO) GetFile(fileID string) (*os.File, error) {
	return a.filePool.GetFile(fileID)
}

func (a *AsyncIO) ReleaseFile(fileID string) error {
	return a.filePool.ReleaseFile(fileID)
}
