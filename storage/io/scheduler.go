package io

import (
	"container/heap"
	"fmt"
	"sync"
	"time"
)

// Scheduler 负责调度 I/O 请求
type Scheduler struct {
	mu           sync.Mutex
	queue        *priorityQueue
	executor     *Executor
	maxQueueSize int

	submitted uint64
	completed uint64
	errors    uint64

	scheduleChan chan struct{}
	stopChan     chan struct{}
	wg           sync.WaitGroup

	// 条件变量：用于队列满时的阻塞等待
	cond *sync.Cond
}

// NewScheduler 创建一个新的 Scheduler
func NewScheduler(executor *Executor, maxQueueSize int) *Scheduler {
	if maxQueueSize <= 0 {
		maxQueueSize = 10000
	}

	s := &Scheduler{
		queue:        newPriorityQueue(maxQueueSize),
		executor:     executor,
		maxQueueSize: maxQueueSize,
		scheduleChan: make(chan struct{}, 100),
		stopChan:     make(chan struct{}),
	}

	// 初始化条件变量
	s.cond = sync.NewCond(&s.mu)

	s.wg.Add(1)
	go s.scheduleLoop()

	return s
}

// Submit 提交单个请求（如果队列满则阻塞等待）
func (s *Scheduler) Submit(req *IORequest) error {
	s.mu.Lock()

	// 首先检查是否已停止
	select {
	case <-s.stopChan:
		s.mu.Unlock()
		return fmt.Errorf("scheduler is stopped")
	default:
	}

	// 如果队列已满，等待
	for s.queue.Len() >= s.maxQueueSize {
		select {
		case <-s.stopChan:
			s.mu.Unlock()
			return fmt.Errorf("scheduler is stopped")
		default:
			// Wait() 会释放锁，等待被唤醒后重新获取锁
			s.cond.Wait()
		}
	}

	heap.Push(s.queue, req)
	s.submitted++
	s.mu.Unlock()

	// 触发调度（在锁外触发，避免阻塞）
	select {
	case s.scheduleChan <- struct{}{}:
	default:
	}

	return nil
}

// SubmitBatch 批量提交请求（如果队列满则阻塞等待）
func (s *Scheduler) SubmitBatch(reqs []*IORequest) error {
	s.mu.Lock()

	// 首先检查是否已停止
	select {
	case <-s.stopChan:
		s.mu.Unlock()
		return fmt.Errorf("scheduler is stopped")
	default:
	}

	// 如果空间不足，等待
	for s.queue.Len()+len(reqs) > s.maxQueueSize {
		select {
		case <-s.stopChan:
			s.mu.Unlock()
			return fmt.Errorf("scheduler is stopped")
		default:
			s.cond.Wait()
		}
	}

	for _, req := range reqs {
		heap.Push(s.queue, req)
		s.submitted++
	}
	s.mu.Unlock()

	// 触发调度
	select {
	case s.scheduleChan <- struct{}{}:
	default:
	}

	return nil
}

// tryScheduleBatch 尝试批量调度多个请求到 Executor
// 优化：批量取出后再提交，批量更新统计
func (s *Scheduler) tryScheduleBatch() {
	const batchSize = 32 // 每次最多调度 32 个请求

	for {
		// 1. 在持有锁的情况下，批量取出请求
		s.mu.Lock()

		if s.queue.Len() == 0 {
			s.mu.Unlock()
			return // 队列空了，退出
		}

		batch := make([]*IORequest, 0, batchSize)

		for i := 0; i < batchSize && s.queue.Len() > 0; i++ {
			item := heap.Pop(s.queue).(*IORequest)

			// 检查超时
			if !item.Deadline.IsZero() && time.Now().After(item.Deadline) {
				s.sendTimeout(item)
				s.errors++
				continue
			}

			batch = append(batch, item)
			s.cond.Signal() // 通知一个等待的提交者
		}

		hasMore := s.queue.Len() > 0
		s.mu.Unlock()

		// 2. 在锁外提交到 Executor
		successCount := 0
		for _, item := range batch {
			err := s.executor.Submit(item) // 阻塞提交
			if err != nil {
				// Executor 已关闭，放回队列
				s.mu.Lock()
				heap.Push(s.queue, item)
				s.mu.Unlock()
				return // Executor 关闭了，退出循环
			}
			successCount++
		}

		// 3. 批量更新统计
		if successCount > 0 {
			s.mu.Lock()
			s.completed += uint64(successCount)
			s.mu.Unlock()
		}

		// 4. 如果队列中没有更多请求，退出
		if !hasMore {
			return
		}
		// 否则继续处理下一批
	}
}

// scheduleLoop 调度循环
func (s *Scheduler) scheduleLoop() {
	defer s.wg.Done()

	for {
		select {
		case <-s.stopChan:
			return
		case <-s.scheduleChan:
			s.tryScheduleBatch()
		}
	}
}

// sendTimeout 发送超时错误
func (s *Scheduler) sendTimeout(req *IORequest) {
	select {
	case req.Callback <- IOResult{
		RequestID: req.ID,
		Error:     fmt.Errorf("request timeout"),
	}:
	case <-req.Context.Done():
	}
}

// Stop 停止调度器
func (s *Scheduler) Stop() error {
	close(s.stopChan)

	// 唤醒所有等待的 Submit
	s.cond.Broadcast()

	s.wg.Wait()

	s.mu.Lock()
	defer s.mu.Unlock()

	for s.queue.Len() > 0 {
		req := heap.Pop(s.queue).(*IORequest)
		s.sendCancel(req)
	}

	return nil
}

// sendCancel 发送取消错误
func (s *Scheduler) sendCancel(req *IORequest) {
	select {
	case req.Callback <- IOResult{
		RequestID: req.ID,
		Error:     fmt.Errorf("scheduler stopped"),
	}:
	case <-req.Context.Done():
	}
}

// Stats 返回调度器统计信息
func (s *Scheduler) Stats() SchedulerStats {
	s.mu.Lock()
	defer s.mu.Unlock()

	return SchedulerStats{
		QueueSize: s.queue.Len(),
		QueueCap:  s.maxQueueSize, // 添加这一行
		Submitted: s.submitted,
		Completed: s.completed,
		Errors:    s.errors,
	}
}

type SchedulerStats struct {
	QueueSize int
	QueueCap  int
	Submitted uint64
	Completed uint64
	Errors    uint64
}

// priorityQueue 优先级队列实现
type priorityQueue struct {
	items []*IORequest
}

func newPriorityQueue(capacity int) *priorityQueue {
	return &priorityQueue{
		items: make([]*IORequest, 0, capacity),
	}
}

func (pq *priorityQueue) Len() int {
	return len(pq.items)
}

func (pq *priorityQueue) Less(i, j int) bool {
	return pq.items[i].Priority < pq.items[j].Priority
}

func (pq *priorityQueue) Swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
}

func (pq *priorityQueue) Push(x interface{}) {
	pq.items = append(pq.items, x.(*IORequest))
}

func (pq *priorityQueue) Pop() interface{} {
	n := len(pq.items)
	item := pq.items[n-1]
	pq.items = pq.items[0 : n-1]
	return item
}
