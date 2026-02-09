package io

import (
	"fmt"
	"os"
	"strings"
	"sync"
)

// FilePool 管理文件句柄的复用
type FilePool struct {
	mu       sync.Mutex
	handles  map[string]*fileEntry
	openFile func(string) (*os.File, error)
	maxOpen  int
}

type fileEntry struct {
	file     *os.File
	refCount int
	path     string
}

// NewFilePool 创建一个新的文件句柄池
func NewFilePool() *FilePool {
	return &FilePool{
		handles: make(map[string]*fileEntry),
		openFile: func(path string) (*os.File, error) {
			return os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
		},
	}
}

// Register 注册一个文件到池中
func (p *FilePool) Register(fileID string, path string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if entry, exists := p.handles[fileID]; exists {
		if entry.path != path {
			return fmt.Errorf("fileID %s already registered with different path: existing=%s, new=%s",
				fileID, entry.path, path)
		}
		return nil
	}

	file, err := p.openFile(path)
	if err != nil {
		return fmt.Errorf("open file failed: %w", err)
	}

	p.handles[fileID] = &fileEntry{
		file:     file,
		refCount: 0,
		path:     path,
	}

	return nil
}

// Get 获取文件句柄，引用计数 +1
func (p *FilePool) Get(fileID string) (*os.File, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	entry, exists := p.handles[fileID]
	if !exists {
		return nil, fmt.Errorf("file not registered: %s", fileID)
	}

	entry.refCount++
	return entry.file, nil
}

// Put 释放文件句柄，引用计数 -1
func (p *FilePool) Put(fileID string, file *os.File) {
	p.mu.Lock()
	defer p.mu.Unlock()

	entry, exists := p.handles[fileID]
	if !exists {
		return
	}

	// 防止引用计数递减为负
	if entry.refCount > 0 {
		entry.refCount--
	}

	// 注意：这里不关闭文件，保持打开状态以便复用
	// Phase 4 可以实现：当 refCount == 0 时，启动定时器，超时后关闭文件
}

// GetRefCount 获取文件的当前引用计数（用于测试和调试）
func (p *FilePool) GetRefCount(fileID string) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	entry, exists := p.handles[fileID]
	if !exists {
		return -1
	}
	return entry.refCount
}

// Close 关闭所有文件句柄
func (p *FilePool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var warnings []string
	var closeErrors []error

	for fileID, entry := range p.handles {
		// 检查引用计数
		if entry.refCount > 0 {
			warnings = append(warnings,
				fmt.Sprintf("file %s still has %d references", fileID, entry.refCount))
		}

		// 关闭文件
		if entry.file != nil {
			if err := entry.file.Close(); err != nil {
				closeErrors = append(closeErrors,
					fmt.Errorf("close file %s failed: %w", fileID, err))
			}
		}
	}

	p.handles = make(map[string]*fileEntry)

	// 返回错误信息（如果有）
	if len(warnings) > 0 || len(closeErrors) > 0 {
		var sb strings.Builder

		if len(warnings) > 0 {
			sb.WriteString("warnings: [")
			for i, w := range warnings {
				if i > 0 {
					sb.WriteString(", ")
				}
				sb.WriteString(w)
			}
			sb.WriteString("]")
		}

		if len(closeErrors) > 0 {
			if len(warnings) > 0 {
				sb.WriteString("; ")
			}
			sb.WriteString("errors: [")
			for i, e := range closeErrors {
				if i > 0 {
					sb.WriteString(", ")
				}
				sb.WriteString(e.Error())
			}
			sb.WriteString("]")
		}

		return fmt.Errorf("file pool close issues: %s", sb.String())
	}

	return nil
}

// Stats 返回文件池统计信息
func (p *FilePool) Stats() FilePoolStats {
	p.mu.Lock()
	defer p.mu.Unlock()

	totalRefs := 0
	for _, entry := range p.handles {
		totalRefs += entry.refCount
	}

	return FilePoolStats{
		TotalFiles:      len(p.handles),
		TotalReferences: totalRefs,
	}
}

type FilePoolStats struct {
	TotalFiles      int
	TotalReferences int
}

// GetFilePath 获取文件的完整路径
func (p *FilePool) GetFilePath(fileID string) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	entry, exists := p.handles[fileID]
	if !exists {
		return "", fmt.Errorf("file not registered: %s", fileID)
	}

	return entry.path, nil
}

// lance/io/file_pool.go

// GetFile 获取已注册的文件句柄（增加引用计数）
func (p *FilePool) GetFile(fileID string) (*os.File, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	entry, exists := p.handles[fileID] // 改为 handles
	if !exists {
		return nil, fmt.Errorf("file %s not registered", fileID)
	}

	entry.refCount++ // 增加引用计数
	return entry.file, nil
}

// ReleaseFile 减少引用计数（不关闭文件）
func (p *FilePool) ReleaseFile(fileID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	entry, exists := p.handles[fileID] // 改为 handles
	if !exists {
		return nil
	}

	if entry.refCount > 0 {
		entry.refCount--
	}
	return nil
}
