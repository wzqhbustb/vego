# Deletion Vector 实现方案

> 本文档规划了 Deletion Vector (DV) 框架的完整实现，用于实现逻辑删除而非物理删除。

---

## 一、背景与目标

### 1.1 当前问题

当前删除实现 (`storage.go:294-324`)：
```go
func (s *DocumentStorage) Delete(id string) error {
    // 1. 从 buffer 中移除
    // 2. 从 metadata 中删除
    s.metaStore.mu.Lock()
    delete(s.metaStore.entries, idHash)
    delete(s.metaStore.idToHash, id)
    s.metaStore.mu.Unlock()
    s.dirty = true
    // 问题：物理删除，后续 Flush 会重写整个文件
}
```

**问题**：
- 物理删除导致 Flush 时重写整个文件
- HNSW 索引中的节点无法高效删除
- Update 操作会产生孤儿节点（先 Delete 再 Insert）

### 1.2 DV 解决方案

采用 Lance 风格的 Deletion Vector：
- **逻辑删除**：标记删除而非物理移除
- **O(1) 删除**：位图操作，无需重写
- **搜索过滤**：查询时自动过滤已删除节点
- **后台压缩**：定期清理已删除数据

---

## 二、架构设计

### 2.1 组件关系

```
┌─────────────────────────────────────────────────────────────────┐
│                        Collection                                │
│  ┌─────────────────┐    ┌─────────────────┐                   │
│  │   HNSWIndex     │    │ DeletionVector  │                   │
│  │  (向量索引)      │    │  (逻辑删除标记)  │                   │
│  │                 │    │                 │                   │
│  │  nodes[]       │    │  deletedRows    │                   │
│  │  entryPoint    │    │  (RoaringBitmap)│                   │
│  └────────┬────────┘    └────────┬────────┘                   │
│           │                       │                             │
│           └───────────┬───────────┘                             │
│                       ▼                                        │
│              ┌────────────────┐                                │
│              │ DocumentStorage │                                │
│              │  (持久化存储)   │                                │
│              └────────────────┘                                │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 数据流

```
Delete(id) 流程:
┌──────────────────────────────────────────────────────────────┐
│ 1. Collection.Delete(id)                                     │
│    ├── 获取 nodeID (docToNode)                               │
│    ├── HNSWIndex.MarkDeleted(nodeID)  ← 新增                │
│    └── Storage.MarkDeleted(idHash)     ← 新增                │
└──────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────┐
│ 2. Search/Scan 操作                                         │
│    ├── HNSW 返回候选节点                                    │
│    ├── 过滤 deletedNodes (O(1) 位图查找)                    │
│    └── 返回有效结果                                          │
└──────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────┐
│ 3. Flush 时                                                  │
│    ├── 检查删除率 > 阈值 (默认 30%)                          │
│    ├── 如果超过：执行压缩 (rewrite + 重建索引)               │
│    └── 如果未超过：保持现状 (DV 持久化到 .del 文件)          │
└──────────────────────────────────────────────────────────────┘
```

---

## 三、详细设计

### 3.1 DeletionVector 数据结构

**位置**：`index/deletion_vector.go`

```go
package index

import (
    "github.com/RoaringBitmap/roaring"
)

// DeletionVector marks deleted rows using a bitmap
type DeletionVector struct {
    deleted *roaring.Bitmap  // RoaringBitmap for efficient storage
    mu      sync.RWMutex
}

// NewDeletionVector creates a new DeletionVector
func NewDeletionVector() *DeletionVector {
    return &DeletionVector{
        deleted: roaring.NewBitmap(),
    }
}

// MarkDeleted marks a row ID as deleted
func (dv *DeletionVector) MarkDeleted(rowID uint32) {
    dv.mu.Lock()
    defer dv.mu.Unlock()
    dv.deleted.Add(rowID)
}

// UnmarkDeleted unmarks a row ID (for rollback/undelete)
func (dv *DeletionVector) UnmarkDeleted(rowID uint32) {
    dv.mu.Lock()
    defer dv.mu.Unlock()
    dv.deleted.Remove(rowID)
}

// IsDeleted checks if a row is deleted (O(1))
func (dv *DeletionVector) IsDeleted(rowID uint32) bool {
    dv.mu.RLock()
    defer dv.mu.RUnlock()
    return dv.deleted.Contains(rowID)
}

// Count returns the number of deleted rows
func (dv *DeletionVector) Count() int {
    dv.mu.RLock()
    defer dv.mu.RUnlock()
    return int(dv.deleted.GetCardinality())
}

// DeletedRows returns all deleted row IDs
func (dv *DeletionVector) DeletedRows() *roaring.Bitmap {
    dv.mu.RLock()
    defer dv.mu.RUnlock()
    return dv.deleted.Clone()
}

// Clear removes all deletion marks
func (dv *DeletionVector) Clear() {
    dv.mu.Lock()
    defer dv.mu.Unlock()
    dv.deleted.Clear()
}

// Merge combines another DeletionVector into this one
func (dv *DeletionVector) Merge(other *DeletionVector) {
    dv.mu.Lock()
    defer dv.mu.Unlock()
    other.mu.RLock()
    defer other.mu.RUnlock()
    dv.deleted.Or(other.deleted)
}
```

**为什么选择 RoaringBitmap**：
- 内存高效：对于稀疏删除，内存占用远小于完整位数组
- O(1) 检查：单节点检查极快
- 快速聚合：OR/AND 操作高效
- 序列化高效：紧凑的二进制格式

---

### 3.2 持久化格式

**文件**：`{collection_path}/deletions.del`

```
┌─────────────────────────────────────────┐
│  Header (16 bytes)                      │
│  ┌───────────────────────────────────┐ │
│  │ Magic: "DEL1"      (4 bytes)     │ │
│  │ Version: uint32    (4 bytes)      │ │
│  │ NumDeleted: uint64 (8 bytes)      │ │
│  └───────────────────────────────────┘ │
├─────────────────────────────────────────┤
│  Bitmap Data (Roaring Serialization)   │
│  - Run-length encoding                  │
│  - Container 压缩                       │
└─────────────────────────────────────────┘
```

**实现**：`index/deletion_vector_persist.go`

```go
package index

import (
    "encoding/binary"
    "io"
    "os"
)

const (
    delFileMagic    = "DEL1"
    delFileVersion  = 1
    delFileName     = "deletions.del"
)

// Serialize writes the DeletionVector to a file
func (dv *DeletionVector) Serialize(path string) error {
    f, err := os.Create(path)
    if err != nil {
        return err
    }
    defer f.Close()

    // Write header
    if _, err := f.Write([]byte(delFileMagic)); err != nil {
        return err
    }
    if err := binary.Write(f, binary.LittleEndian, uint32(delFileVersion)); err != nil {
        return err
    }
    numDeleted := dv.deleted.GetCardinality()
    if err := binary.Write(f, binary.LittleEndian, numDeleted); err != nil {
        return err
    }

    // Write bitmap
    if _, err := dv.deleted.WriteTo(f); err != nil {
        return err
    }

    return nil
}

// Deserialize reads a DeletionVector from a file
func Deserialize(path string) (*DeletionVector, error) {
    f, err := os.Open(path)
    if err != nil {
        return nil, err
    }
    defer f.Close()

    // Read header
    magic := make([]byte, 4)
    if _, err := f.Read(magic); err != nil {
        return nil, err
    }
    if string(magic) != delFileMagic {
        return nil, fmt.Errorf("invalid magic: %s", string(magic))
    }

    var version uint32
    if err := binary.Read(f, binary.LittleEndian, &version); err != nil {
        return nil, err
    }
    if version != delFileVersion {
        return nil, fmt.Errorf("unsupported version: %d", version)
    }

    // Read bitmap
    bitmap := roaring.NewBitmap()
    if _, err := bitmap.ReadFrom(f); err != nil {
        return nil, err
    }

    return &DeletionVector{deleted: bitmap}, nil
}
```

---

### 3.3 HNSWIndex 集成

**修改**：`index/hnsw.go`

```go
type HNSWIndex struct {
    // ... 现有字段
    M              int
    dimension      int
    nodes          []*Node
    entryPoint     int32
    maxLevel       int32
    distFunc       DistanceFunc
    globalLock     sync.RWMutex
    rng            *rand.Rand
    mu             sync.Mutex

    // 新增：DeletionVector
    deletionVector *DeletionVector
}

// NewHNSW creates a new HNSW index
func NewHNSW(config Config) *HNSWIndex {
    return &HNSWIndex{
        // ... 现有初始化
        deletionVector: NewDeletionVector(),
    }
}

// MarkDeleted marks a node as deleted (logical deletion)
func (h *HNSWIndex) MarkDeleted(nodeID int) {
    h.globalLock.Lock()
    defer h.globalLock.Unlock()
    h.deletionVector.MarkDeleted(uint32(nodeID))
}

// IsDeleted checks if a node is deleted
func (h *HNSWIndex) IsDeleted(nodeID int) bool {
    return h.deletionVector.IsDeleted(uint32(nodeID))
}

// Search searches for k nearest neighbors, automatically filtering deleted nodes
func (h *HNSWIndex) Search(query []float32, k, ef int) []SearchResult {
    h.globalLock.RLock()
    defer h.globalLock.RUnlock()

    // ... 现有搜索逻辑 ...

    // 过滤已删除节点
    results := filterDeletedResults(results, h.deletionVector)

    return results[:min(k, len(results))]
}

// filterDeletedResults removes deleted nodes from search results
func filterDeletedResults(results []SearchResult, dv *DeletionVector) []SearchResult {
    filtered := results[:0]
    for _, r := range results {
        if !dv.IsDeleted(uint32(r.ID)) {
            filtered = append(filtered, r)
        }
    }
    return filtered
}
```

---

### 3.4 DocumentStorage 集成

**修改**：`vego/storage.go`

```go
type DocumentStorage struct {
    // ... 现有字段
    path           string
    dimension      int
    factory        *encoding.EncoderFactory
    writeBuffer    []*Document
    metaStore      *metadataStore
    blockCache     *format.BlockCache
    version        format.VersionPolicy

    // 新增：DeletionVector
    deletionVector *index.DeletionVector
}

func NewDocumentStorage(path string, dimension int, cache ...*format.BlockCache) (*DocumentStorage, error) {
    s := &DocumentStorage{
        // ... 现有初始化
        deletionVector: index.NewDeletionVector(),
    }

    // 尝试加载已存在的 DV
    dvPath := filepath.Join(path, index.DelFileName)
    if dv, err := index.Deserialize(dvPath); err == nil {
        s.deletionVector = dv
    }

    return s, nil
}

// MarkDeleted marks a document as deleted (logical deletion)
func (s *DocumentStorage) MarkDeleted(id string) error {
    idHash := hashID(id)
    s.deletionVector.MarkDeleted(uint32(idHash))
    return nil
}

// IsDeleted checks if a document is deleted
func (s *DocumentStorage) IsDeleted(id string) bool {
    idHash := hashID(id)
    return s.deletionVector.IsDeleted(uint32(idHash))
}

// Save persists the DeletionVector to disk
func (s *DocumentStorage) saveDeletionVector() error {
    dvPath := filepath.Join(s.path, index.DelFileName)
    return s.deletionVector.Serialize(dvPath)
}

// Flush includes DV persistence
func (s *DocumentStorage) flush() error {
    // ... 现有逻辑 ...

    // 检查是否需要压缩
    totalDocs := len(allDocs)
    deletedCount := s.deletionVector.Count()
    deletionRate := float64(deletedCount) / float64(totalDocs)

    if deletionRate > 0.3 { // 30% 阈值
        // 执行压缩
        if err := s.compact(); err != nil {
            return err
        }
    }

    // 保存 DV
    if err := s.saveDeletionVector(); err != nil {
        return err
    }

    // ... 清理已删除的元数据 ...
}
```

---

### 3.5 Collection 层集成

**修改**：`vego/collection.go`

```go
func (c *Collection) Delete(id string) error {
    return c.DeleteContext(context.Background(), id)
}

func (c *Collection) DeleteContext(ctx context.Context, id string) error {
    c.mu.Lock()
    defer c.mu.Unlock()

    // 检查文档是否存在
    nodeID, exists := c.docToNode[id]
    if !exists {
        return wrapError("DeleteContext", c.name, id, ErrDocumentNotFound)
    }

    // 从 HNSW 标记删除（逻辑删除）
    c.index.MarkDeleted(nodeID)

    // 从存储标记删除
    c.storage.MarkDeleted(id)

    // 更新映射（保留映射以便后续清理）
    // 注意：不立即从 docToNode/nodeToDoc 中删除

    return nil
}

// Update 使用 DV 实现真正的更新
func (c *Collection) Update(doc *Document) error {
    // 1. 检查文档是否存在
    nodeID, exists := c.docToNode[doc.ID]
    if exists {
        // 2. 标记旧版本为已删除（逻辑删除）
        c.index.MarkDeleted(nodeID)
        c.storage.MarkDeleted(doc.ID)
        // 3. 移除映射（防止重复）
        delete(c.docToNode, doc.ID)
        delete(c.nodeToDoc, nodeID)
    }

    // 4. 插入新版本
    return c.Insert(doc)
}
```

---

### 3.6 压缩策略

**触发条件**：
- 删除率 > 30%（可配置）
- 手动触发：`collection.Compact()`

**压缩流程**：

```go
func (c *Collection) Compact() error {
    c.mu.Lock()
    defer c.mu.Unlock()

    // 1. 获取所有有效文档
    validDocs, err := c.storage.GetAllValidDocuments()
    if err != nil {
        return err
    }

    // 2. 重建 HNSW 索引
    newIndex := index.NewHNSW(c.config.toIndexConfig())
    for _, doc := range validDocs {
        newIndex.Add(doc.Vector)
    }

    // 3. 重写存储文件
    if err := c.storage.Rewrite(validDocs); err != nil {
        return err
    }

    // 4. 清除 DV
    c.index.deletionVector.Clear()
    c.storage.ClearDeletionVector()

    // 5. 替换索引
    c.index = newIndex

    return nil
}
```

---

## 四、API 设计

### 4.1 Collection API 扩展

```go
// Collection 新增方法
type Collection interface {
    // ... 现有方法

    // Delete 删除文档（逻辑删除）
    Delete(id string) error
    DeleteContext(ctx context.Context, id string) error

    // Update 更新文档（使用 DV）
    Update(doc *Document) error
    UpdateContext(ctx context.Context, doc *Document) error

    // Compact 手动触发压缩
    Compact() error

    // Stats 包含删除统计
    Stats() CollectionStats
}

type CollectionStats struct {
    // ... 现有字段
    DeletedCount int     `json:"deleted_count"`
    DeletionRate float64 `json:"deletion_rate"`
}
```

### 4.2 Storage API 扩展

```go
type DocumentStorage interface {
    // ... 现有方法

    // MarkDeleted 标记删除
    MarkDeleted(id string) error

    // IsDeleted 检查是否已删除
    IsDeleted(id string) bool

    // GetAllValidDocuments 获取所有有效文档（用于压缩）
    GetAllValidDocuments() ([]*Document, error)

    // ClearDeletionVector 清除删除标记
    ClearDeletionVector() error
}
```

---

## 五、性能影响

### 5.1 操作复杂度

| 操作 | 物理删除（当前） | 逻辑删除（DV） |
|------|------------------|----------------|
| Delete | O(1) | O(1) |
| Search 过滤 | N/A | O(k) - 位图检查 |
| Flush | O(N) - 重写 | O(N) - 重写 |
| Update | O(N) | O(1) |
| 压缩 | N/A | O(N) - 重建 |

### 5.2 内存开销

- RoaringBitmap：每个删除约 2-8 字节
- 10% 删除率：10K 文档 ≈ 8KB
- 50% 删除率：10K 文档 ≈ 40KB

### 5.3 搜索开销

- 每次结果检查：单次 O(1) 位图查找
- Search(k=10)：额外 10 次检查，可忽略

---

## 六、测试计划

### 6.1 单元测试

| 测试 | 描述 |
|------|------|
| `TestDeletionVectorBasic` | 基本标记/检查/计数 |
| `TestDeletionVectorSerialize` | 序列化/反序列化 |
| `TestDeletionVectorMerge` | 合并多个 DV |
| `TestHNSWDelete` | HNSW 逻辑删除 |
| `TestStorageDelete` | Storage 逻辑删除 |

### 6.2 集成测试

| 测试 | 描述 |
|------|------|
| `TestCollectionDelete` | Collection 层删除 |
| `TestCollectionUpdate` | Update 操作（验证无孤儿） |
| `TestSearchWithDeletes` | 搜索结果过滤已删除 |
| `TestCompact` | 压缩功能 |
| `TestPersistence` | DV 持久化和恢复 |

### 6.3 性能测试

| 测试 | 描述 |
|------|------|
| `BenchmarkDelete` | 删除性能 |
| `BenchmarkSearchWithDeletes` | 带删除的搜索性能 |
| `BenchmarkCompact` | 压缩性能 |

---

## 七、实施计划

### Phase 1: 核心实现（1-2 周）

```
Week 1:
├── DeletionVector 数据结构
│   ├── index/deletion_vector.go
│   └── index/deletion_vector_test.go
│
└── 持久化
    ├── index/deletion_vector_persist.go
    └── index/deletion_vector_persist_test.go

Week 2:
├── HNSW 集成
│   ├── index/hnsw.go 修改
│   └── index/delete_test.go
│
└── Storage 集成
    ├── vego/storage.go 修改
    └── vego/delete_test.go
```

### Phase 2: Collection 层（1 周）

```
Week 3:
├── Collection.Delete
├── Collection.Update
└── 集成测试
```

### Phase 3: 压缩与优化（1 周）

```
Week 4:
├── Compact 实现
├── 配置选项
└── 性能测试
```

---

## 八、风险与缓解

| 风险 | 影响 | 缓解措施 |
|------|------|----------|
| 内存泄漏 | 高 | DV 定期压缩清理 |
| 搜索性能退化 | 中 | 位图检查开销极小 |
| 数据不一致 | 中 | 确保 Flush 时 DV 持久化 |
| 压缩阻塞 | 低 | 异步压缩或后台任务 |

---

## 九、依赖

- `github.com/RoaringBitmap/roaring` - 位图实现
- 现有 HNSW 索引
- 现有 DocumentStorage

---

## 十、相关文件

- `index/deletion_vector.go` - DV 数据结构
- `index/deletion_vector_persist.go` - 持久化
- `index/hnsw.go` - HNSW 集成
- `vego/storage.go` - Storage 集成
- `vego/collection.go` - Collection API

---

*文档创建时间：2026-02-28*
