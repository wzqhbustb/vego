package io

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
)

// bufferPool 用于复用读取缓冲区，减少 GC 压力
// 默认大小 64KB，可以容纳大多数 Page 读取
var bufferPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 64*1024) // 64KB 默认缓冲区
	},
}

// getBuffer 从池中获取缓冲区
func getBuffer(size int32) []byte {
	// 如果请求大小超过池的默认大小，直接分配
	if size > 64*1024 {
		return make([]byte, size)
	}
	// 从池中获取
	buf := bufferPool.Get().([]byte)
	// 确保长度足够
	if int32(len(buf)) < size {
		bufferPool.Put(buf)
		return make([]byte, size)
	}
	return buf[:size]
}

// putBuffer 将缓冲区归还池中
func putBuffer(buf []byte) {
	// 只归还原生大小的缓冲区，避免污染池
	if cap(buf) == 64*1024 {
		bufferPool.Put(buf[:64*1024])
	}
	// 否则让 GC 回收
}

// Executor 负责实际执行 I/O 操作
type Executor struct {
	workers   int
	workQueue chan *IORequest
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
	filePool  *FilePool

	// 原子计数器：当前队列大小（线程安全）
	queueSize atomic.Int64
}

// NewExecutor 创建一个新的 Executor
func NewExecutor(workers int, queueSize int, filePool *FilePool) *Executor {
	if workers <= 0 {
		workers = 4
	}
	if queueSize <= 0 {
		queueSize = 1000
	}

	ctx, cancel := context.WithCancel(context.Background())

	e := &Executor{
		workers:   workers,
		workQueue: make(chan *IORequest, queueSize),
		ctx:       ctx,
		cancel:    cancel,
		filePool:  filePool,
	}

	e.start()

	return e
}

func (e *Executor) start() {
	for i := 0; i < e.workers; i++ {
		e.wg.Add(1)
		go e.worker(i)
	}
}

func (e *Executor) worker(id int) {
	defer e.wg.Done()

	for {
		select {
		case <-e.ctx.Done():
			return
		case req, ok := <-e.workQueue:
			if !ok {
				return
			}
			// 取出请求，队列大小减 1
			e.queueSize.Add(-1)
			e.execute(req)
		}
	}
}

func (e *Executor) execute(req *IORequest) {
	// 检查请求是否已取消
	select {
	case <-req.Context.Done():
		e.sendResult(req, IOResult{
			RequestID: req.ID,
			Error:     req.Context.Err(),
		})
		return
	default:
	}

	// 获取文件句柄
	file, err := e.filePool.Get(req.FileID)
	if err != nil {
		e.sendResult(req, IOResult{
			RequestID: req.ID,
			Error:     fmt.Errorf("get file handle failed: %w", err),
		})
		return
	}
	defer e.filePool.Put(req.FileID, file)

	// 执行 I/O 操作
	var result IOResult
	switch req.Op {
	case OpRead:
		result = e.doRead(file, req)
	case OpWrite:
		result = e.doWrite(file, req)
	default:
		result = IOResult{
			RequestID: req.ID,
			Error:     fmt.Errorf("unknown operation: %v", req.Op),
		}
	}

	e.sendResult(req, result)
}

// doRead 执行读取操作，使用 buffer pool 减少内存分配
func (e *Executor) doRead(file *os.File, req *IORequest) IOResult {
	// 从池中获取缓冲区
	buf := getBuffer(req.Size)

	n, err := file.ReadAt(buf, req.Offset)

	// 处理错误情况
	if err != nil && err != io.EOF {
		putBuffer(buf) // 错误时立即归还
		return IOResult{
			RequestID: req.ID,
			Error:     fmt.Errorf("read failed: %w", err),
		}
	}

	// 如果没有读取到任何数据
	if n == 0 {
		putBuffer(buf)
		if err == io.EOF {
			// 读取超出文件末尾
			return IOResult{
				RequestID: req.ID,
				Error:     fmt.Errorf("read beyond file end: offset=%d, size=%d", req.Offset, req.Size),
			}
		}
		// 其他原因导致的 0 字节读取（罕见）
		return IOResult{
			RequestID: req.ID,
			Data:      []byte{},
		}
	}

	// 读取成功
	// 部分读取（文件尾的 Page）也是成功的情况
	data := make([]byte, n)
	copy(data, buf[:n])
	putBuffer(buf) // 归还缓冲区到池中

	return IOResult{
		RequestID: req.ID,
		Data:      data,
	}
}

func (e *Executor) doWrite(file *os.File, req *IORequest) IOResult {
	n, err := file.WriteAt(req.Data, req.Offset)

	if err != nil {
		return IOResult{
			RequestID: req.ID,
			Error:     fmt.Errorf("write failed: %w", err),
		}
	}

	if n != len(req.Data) {
		return IOResult{
			RequestID: req.ID,
			Error:     fmt.Errorf("short write: wrote %d bytes, expected %d", n, len(req.Data)),
		}
	}

	return IOResult{
		RequestID: req.ID,
		Data:      nil,
	}
}

func (e *Executor) sendResult(req *IORequest, result IOResult) {
	// 因为 Callback channel 有缓冲区 1，且每个请求只发送一次
	// 所以这个发送应该总是成功的（除非用户提前关闭了 channel）
	select {
	case req.Callback <- result:
		// 成功发送
	default:
		// 不应该发生：channel 已满或关闭
		// 这说明用户代码有问题
	}
}

// Submit 提交一个 I/O 请求到执行队列（阻塞直到有空间）
func (e *Executor) Submit(req *IORequest) error {
	select {
	case <-e.ctx.Done():
		return fmt.Errorf("executor is closed")
	case e.workQueue <- req:
		// 成功提交，队列大小加 1
		e.queueSize.Add(1)
		return nil
	}
}

// SubmitAsync 非阻塞提交，如果队列已满返回错误
func (e *Executor) SubmitAsync(req *IORequest) error {
	select {
	case <-e.ctx.Done():
		return fmt.Errorf("executor is closed")
	case e.workQueue <- req:
		e.queueSize.Add(1)
		return nil
	default:
		return fmt.Errorf("queue is full")
	}
}

// Close 关闭 Executor
func (e *Executor) Close() error {
	e.cancel()
	close(e.workQueue)
	e.wg.Wait()
	return nil
}

// Stats 返回 Executor 统计信息（线程安全）
func (e *Executor) Stats() ExecutorStats {
	return ExecutorStats{
		Workers:   e.workers,
		QueueSize: int(e.queueSize.Load()), // 使用原子操作读取
		QueueCap:  cap(e.workQueue),
	}
}

type ExecutorStats struct {
	Workers   int
	QueueSize int
	QueueCap  int
}
