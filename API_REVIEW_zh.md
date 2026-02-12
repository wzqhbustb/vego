# Vego API 审查报告


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
