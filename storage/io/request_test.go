package io

import (
	"context"
	"testing"
	"time"
)

func TestNewIORequest(t *testing.T) {
	req := NewIORequest("file1", 1024, 4096, PriorityHigh)

	if req.ID == 0 {
		t.Error("Request ID should not be zero")
	}
	if req.Op != OpRead {
		t.Errorf("Expected OpRead, got %v", req.Op)
	}
	if req.FileID != "file1" {
		t.Errorf("Expected fileID 'file1', got %s", req.FileID)
	}
	if req.Offset != 1024 {
		t.Errorf("Expected offset 1024, got %d", req.Offset)
	}
	if req.Size != 4096 {
		t.Errorf("Expected size 4096, got %d", req.Size)
	}
	if req.Priority != PriorityHigh {
		t.Errorf("Expected PriorityHigh, got %v", req.Priority)
	}
	if req.Callback == nil {
		t.Error("Callback channel should not be nil")
	}
}

func TestNewIOWriteRequest(t *testing.T) {
	data := []byte("test data")
	req := NewIOWriteRequest("file2", 2048, data, PriorityNormal)

	if req.Op != OpWrite {
		t.Errorf("Expected OpWrite, got %v", req.Op)
	}
	if req.FileID != "file2" {
		t.Errorf("Expected fileID 'file2', got %s", req.FileID)
	}
	if req.Offset != 2048 {
		t.Errorf("Expected offset 2048, got %d", req.Offset)
	}
	if string(req.Data) != string(data) {
		t.Errorf("Expected data '%s', got '%s'", data, req.Data)
	}
	if req.Size != int32(len(data)) {
		t.Errorf("Expected size %d, got %d", len(data), req.Size)
	}
}

func TestIORequest_WithContext(t *testing.T) {
	req := NewIORequest("file1", 0, 1024, PriorityNormal)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req.WithContext(ctx)

	if req.Context != ctx {
		t.Error("Context should be set")
	}

	// 测试取消
	cancel()
	select {
	case <-req.Context.Done():
		// 正确取消
	default:
		t.Error("Context should be cancelled")
	}
}

func TestIORequest_WithDeadline(t *testing.T) {
	req := NewIORequest("file1", 0, 1024, PriorityNormal)

	deadline := time.Now().Add(5 * time.Second)
	req.WithDeadline(deadline)

	if !req.Deadline.Equal(deadline) {
		t.Errorf("Expected deadline %v, got %v", deadline, req.Deadline)
	}
}

func TestIORequest_String(t *testing.T) {
	req := NewIORequest("file1", 1024, 4096, PriorityHigh)
	str := req.String()

	expected := "IORequest{id="
	if len(str) < len(expected) || str[:len(expected)] != expected {
		t.Errorf("String representation unexpected: %s", str)
	}

	// 验证包含关键信息
	if !contains(str, "file1") {
		t.Error("String should contain fileID")
	}
	if !contains(str, "Read") {
		t.Error("String should contain operation type")
	}
}

func TestGenerateRequestID_Unique(t *testing.T) {
	// 重置计数器（注意：这只在测试中有风险，实际生产不这样做）
	globalRequestID = 0

	id1 := generateRequestID()
	id2 := generateRequestID()
	id3 := generateRequestID()

	if id1 == 0 || id2 == 0 || id3 == 0 {
		t.Error("IDs should not be zero")
	}
	if id1 == id2 || id2 == id3 {
		t.Error("IDs should be unique")
	}
	if id2 != id1+1 || id3 != id2+1 {
		t.Error("IDs should be sequential")
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
