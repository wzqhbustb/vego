# Vego API 审查报告

**审查日期**: 2026-02-11  
**范围**: `vego` 包公共 API  
**状态**: 第 0 阶段 - 实现前审查

---

## 执行摘要

本文档记录了在第 1 阶段实现前审查期间发现的 API 设计问题。标记为 **P0** 的项应在第 1 阶段实现之前或期间解决。

**关键决策**:
- 将逐渐添加 Context 支持（非破坏性）
- 搜索结果保持值类型（非指针）
- 分页使用基于游标的方式（向量搜索不推荐偏移量）
- 迭代器使用 Go 风格的通道模式

---

## 1. 缺少 Context 支持 ⭐ P0

### 问题
所有 API 方法都缺少 `context.Context` 支持，无法：
- 为长时间运行的操作设置超时
- 优雅地取消操作
- 实现分布式追踪
- 传递请求范围值

### 当前 API
```go
func (c *Collection) Search(query []float32, k int, opts ...SearchOption) ([]SearchResult, error)
func (c *Collection) Insert(doc *Document) error
func (c *Collection) Get(id string) (*Document, error)
```

### 推荐的 API（渐进式迁移）

```go
// 第 0 阶段：在现有方法旁边添加 Context 方法
func (c *Collection) Get(id string) (*Document, error)                    // 现有
func (c *Collection) GetContext(ctx context.Context, id string) (*Document, error)  // 新增

func (c *Collection) Insert(doc *Document) error                          // 现有
func (c *Collection) InsertContext(ctx context.Context, doc *Document) error        // 新增

func (c *Collection) Search(query []float32, k int, opts ...SearchOption) (SearchResults, error)             // 现有
func (c *Collection) SearchContext(ctx context.Context, query []float32, k int, opts ...SearchOption) (SearchResults, error)  // 新增

// 实现：现有方法委托给 Context 版本
func (c *Collection) Get(id string) (*Document, error) {
    return c.GetContext(context.Background(), id)
}
```

### 弃用时间表
- **第 0-2 阶段**：两个 API 共存，旧方法标记 `// Deprecated: 使用 GetContext`
- **第 3 阶段 (v1.0)**：移除非 Context 方法

### 影响
- **破坏性变更**: 否（渐进式迁移）
- **工作量**: 中等
- **时间线**: 第 0 阶段

---

## 2. 批量操作不完整 ⭐ P0

### 问题
仅暴露了 `InsertBatch`。其他批处理操作在内部已实现但用户无法访问。

### 缺失的 API
| 方法 | 内部状态 | 优先级 |
|--------|----------------|----------|
| `GetBatch(ids []string)` | 存储层已实现 | P0 |
| `DeleteBatch(ids []string)` | 未实现 | P0 |
| `UpsertBatch(docs []*Document)` | 未实现 | P1 |

### 推荐的 API
```go
// GetBatch 通过 ID 检索多个文档
// 返回 id -> 文档 的映射（缺失的文档将被省略）
func (c *Collection) GetBatch(ids []string) (map[string]*Document, error)
func (c *Collection) GetBatchContext(ctx context.Context, ids []string) (map[string]*Document, error)
```

### 用例
- **GetBatch**: 高效加载搜索结果（第 1 阶段优化）

### 原子性说明
批量操作应尽可能保证原子性。如果任何操作失败，可能会保留部分结果。要完全保证原子性，请使用批量事务 API（见第 9 节）。

---

## 3. 不透明的错误类型 ⭐ P0

### 问题
当前错误是简单的 `fmt.Errorf` 字符串，用户无法：
- 区分"未找到"和"内部错误"
- 基于错误类型实现重试逻辑
- 提供本地化的错误消息

### 推荐的错误类型
```go
package vego

import "errors"

// 常见情况的哨兵错误
var (
    ErrDocumentNotFound   = errors.New("document not found")
    ErrDuplicateID        = errors.New("document already exists")
    ErrDimensionMismatch  = errors.New("vector dimension mismatch")
    ErrCollectionNotFound = errors.New("collection not found")
    ErrCollectionClosed   = errors.New("collection is closed")
    ErrInvalidFilter      = errors.New("invalid filter expression")
    ErrIndexCorrupted     = errors.New("index corrupted")
)

// Error 提供结构化的错误信息
type Error struct {
    Op    string // 操作: "Get", "Insert", "Search"
    Coll  string // 集合名称
    DocID string // 文档 ID（如适用）
    Err   error  // 底层错误
}

func (e *Error) Error() string {
    if e.DocID != "" {
        return fmt.Sprintf("vego: %s on collection %s (doc %s) failed: %v", e.Op, e.Coll, e.DocID, e.Err)
    }
    return fmt.Sprintf("vego: %s on collection %s failed: %v", e.Op, e.Coll, e.Err)
}

func (e *Error) Unwrap() error { return e.Err }

// 用于错误检查的辅助函数
func IsNotFound(err error) bool {
    return errors.Is(err, ErrDocumentNotFound)
}

func IsDuplicate(err error) bool {
    return errors.Is(err, ErrDuplicateID)
}

func IsDimensionMismatch(err error) bool {
    return errors.Is(err, ErrDimensionMismatch)
}
```

### 使用示例
```go
doc, err := coll.GetContext(ctx, "doc-123")
if err != nil {
    if vego.IsNotFound(err) {
        // 优雅地处理未找到的情况
        return nil, nil
    }
    // 处理其他错误
    return nil, err
}
```

---

*本文档应在每个阶段完成后审查和更新。*
