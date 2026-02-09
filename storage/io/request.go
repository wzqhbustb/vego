package io

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
)

// OperationType 定义 I/O 操作类型
type OperationType int

const (
	OpRead OperationType = iota
	OpWrite
)

// Priority 定义 I/O 请求优先级
type Priority int

const (
	PriorityHigh   Priority = iota // 用户查询（立即执行）
	PriorityNormal                 // 预取（尽快执行）
	PriorityLow                    // 后台任务（空闲时执行）
)

// IORequest 表示一个 I/O 请求
type IORequest struct {
	ID       uint64          // 唯一标识
	Op       OperationType   // 操作类型
	FileID   string          // 文件标识
	Offset   int64           // 文件偏移
	Size     int32           // 读取/写入大小
	Priority Priority        // 调度优先级
	Deadline time.Time       // 可选的超时时间
	Context  context.Context // 用于取消
	Data     []byte          // 写入时的数据
	Callback chan IOResult   // 结果回调通道, NOTE: 用户必须消费该 Channel，否则可能会导致 worker 阻塞
}

// IOResult 表示 I/O 操作的结果
type IOResult struct {
	RequestID uint64        // 对应的请求 ID
	Data      []byte        // 读取的数据
	Error     error         // 错误信息
	Latency   time.Duration // 实际耗时
}

// 全局原子计数器，用于生成唯一 ID
var globalRequestID uint64 = 0

// generateRequestID 生成唯一的请求 ID
func generateRequestID() uint64 {
	return atomic.AddUint64(&globalRequestID, 1)
}

// NewIORequest 创建一个读取请求
func NewIORequest(fileID string, offset int64, size int32, priority Priority) *IORequest {
	return &IORequest{
		ID:       generateRequestID(),
		Op:       OpRead,
		FileID:   fileID,
		Offset:   offset,
		Size:     size,
		Priority: priority,
		Context:  context.Background(),
		Callback: make(chan IOResult, 1),
	}
}

// NewIOWriteRequest 创建一个写入请求
func NewIOWriteRequest(fileID string, offset int64, data []byte, priority Priority) *IORequest {
	return &IORequest{
		ID:       generateRequestID(),
		Op:       OpWrite,
		FileID:   fileID,
		Offset:   offset,
		Size:     int32(len(data)),
		Data:     data,
		Priority: priority,
		Context:  context.Background(),
		Callback: make(chan IOResult, 1),
	}
}

// WithContext 设置请求的 context
func (r *IORequest) WithContext(ctx context.Context) *IORequest {
	r.Context = ctx
	return r
}

// WithDeadline 设置请求的超时时间
func (r *IORequest) WithDeadline(deadline time.Time) *IORequest {
	r.Deadline = deadline
	return r
}

// String 返回请求的字符串表示（用于调试）
func (r *IORequest) String() string {
	opStr := "Read"
	if r.Op == OpWrite {
		opStr = "Write"
	}
	return fmt.Sprintf("IORequest{id=%d, op=%s, file=%s, offset=%d, size=%d, priority=%d}",
		r.ID, opStr, r.FileID, r.Offset, r.Size, r.Priority)
}
