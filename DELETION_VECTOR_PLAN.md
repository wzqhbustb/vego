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
- 使用 idHash 作为删除标记会有冲突风险

### 1.3 设计原则

**关键决策 1：使用物理行号 (rowID) 而非 idHash**

原因：
1. **唯一性**：行号在文件内唯一，无冲突
2. **连续性**：适合 RoaringBitmap 压缩
3. **一致性**：与 RowIndex 使用的 rowIndex 一致
4. **可查找**：通过 ID → metadata → rowID 快速定位

**关键决策 2：扩展 metadata.json 存储 rowIndex**

不创建单独的 idToRow 映射文件，而是扩展现有的 `docMeta` 结构：
- ✅ 单一文件，易于管理
- ✅ 与现有 metadata 生命周期一致
- ✅ 向后兼容（旧数据无 row_index 字段也能读取）

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

### 2.2 关键设计：ID 映射关系

**三层 ID 体系及其映射：**

```
HNSW nodeID          Collection 映射          Storage rowID
   (int)                (string)               (int64)
      │                      │                      │
      │  HNSW.nodes[]        │  docToNode           │  metadata
      │  HNSW 内部索引       │  nodeToDoc           │  RowIndex
      │                      │                      │
      ▼                      ▼                      ▼
   节点数据             文档 ID               文件行号
      │                      │                      │
      └──────────────────────┴──────────────────────┘
                         │
                         │  DV 检查流程
                         ▼
                    ┌────────────────┐
                    │  是否已删除？   │
                    │  Check DV      │
                    └────────────────┘
```

**映射关系说明：**

| ID 类型 | 定义 | 生命周期 | 稳定性 |
|---------|------|----------|--------|
| **nodeID** | HNSW 图中的节点索引 | 创建时分配，Compact 前保留 | ⚠️ 删除后不回收 |
| **docID** | 用户提供的文档标识 | 文档存在期间 | ✅ 稳定 |
| **rowID** | Storage 文件中的行号 | 写入时分配，Compact 前保留 | ⚠️ 删除后不回收 |

**关键问题：nodeID 与 rowID 的解耦**

```
问题场景：Update 操作

初始状态：
  doc-001: nodeID=5, rowID=10

执行 Update(doc-001, newVector):
  1. 标记 rowID=10 为删除
  2. 保留 nodeToDoc[5] = "doc-001"（延迟清理！）
  3. 插入新数据：分配 rowID=20, nodeID=15
  4. 更新映射：docToNode["doc-001"] = 15
            nodeToDoc[15] = "doc-001"
            （nodeToDoc[5] 仍保留！）

结果：
  nodeID=5  → doc-001（旧，已删除）
  nodeID=15 → doc-001（新，有效）

搜索时过滤：
  - nodeID=5: 通过 doc-001 找到 rowID=10，DV 标记删除 → 过滤
  - nodeID=15: 通过 doc-001 找到 rowID=20，未删除 → 保留
```

**设计决策：**
1. **延迟清理**：Delete/Update 时不清理 nodeToDoc 映射
2. **DV 过滤**：通过 Storage DV 过滤旧数据
3. **Compact 统一清理**：重建时只保留有效映射

### 2.3 数据流

```
Delete(id) 流程:
┌──────────────────────────────────────────────────────────────┐
│ 1. Collection.Delete(id)                                     │
│    ├── 获取 nodeID (docToNode)                               │
│    ├── Storage.MarkDeleted(id)     ← 标记 rowID             │
│    └── 保留 nodeToDoc[nodeID]      ← 延迟清理               │
└──────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────┐
│ 2. Search/Scan 操作                                         │
│    ├── HNSW 返回候选 nodeIDs                                │
│    ├── nodeID → docID (nodeToDoc)                           │
│    ├── docID → rowID (metadata)                             │
│    ├── 检查 DV (rowID 是否删除)                             │
│    └── 过滤已删除，返回有效结果                              │
└──────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────┐
│ 3. Flush 时                                                  │
│    ├── 检查删除率 > 阈值 (默认 30%)                          │
│    ├── 如果超过：执行 Compact                                │
│    │   ├── 重写数据文件（移除已删除行）                      │
│    │   ├── 重建 HNSW 索引                                    │
│    │   └── 重建 nodeToDoc 映射（只保留有效）                │
│    └── 如果未超过：持久化 DV 到 .del 文件                    │
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

**文件命名**：`{data_file}.del`

例如：
- 数据文件：`vectors.lance`
- DV 文件：`vectors.lance.del`

这样设计的好处：
1. 一一对应，易于管理
2. 支持多个数据文件（未来分片）
3. 清晰的文件关联

**文件格式**：

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
    "path/filepath"
)

const (
    delFileMagic    = "DEL1"
    delFileVersion  = 1
    delFileExt      = ".del"
)

// GetDeletionVectorPath returns the DV file path for a data file
func GetDeletionVectorPath(dataFilePath string) string {
    return dataFilePath + delFileExt
}

// Example: 
// dataFile = "/path/to/vectors.lance"
// dvFile   = "/path/to/vectors.lance.del"

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

### 3.3 HNSWIndex 与 Storage 集成（简化设计）

**设计变更**：HNSW 不维护独立的 DeletionVector，而是查询 Storage 的 DV

原因：
1. **单一数据源**：避免 HNSW 和 Storage 的 DV 不一致
2. **简化实现**：减少内存占用和同步复杂度
3. **性能可接受**：O(1) 位图检查开销极小

**修改**：`index/hnsw.go`

```go
// Search searches for k nearest neighbors
// Deleted nodes are filtered at Collection layer using Storage's DeletionVector
func (h *HNSWIndex) Search(query []float32, k, ef int) []SearchResult {
    h.globalLock.RLock()
    defer h.globalLock.RUnlock()

    // ... 现有搜索逻辑 ...
    // 注意：此处不过滤，由 Collection 层统一过滤
    return results
}

// SearchWithDV searches and filters using provided DeletionVector
func (h *HNSWIndex) SearchWithDV(query []float32, k, ef int, isDeleted func(int) bool) []SearchResult {
    h.globalLock.RLock()
    defer h.globalLock.RUnlock()

    // 多取一些候选节点以补偿删除
    candidates := h.searchInternal(query, k*2, ef)
    
    // 过滤已删除节点
    filtered := make([]SearchResult, 0, k)
    for _, cand := range candidates {
        if !isDeleted(cand.ID) {
            filtered = append(filtered, cand)
            if len(filtered) >= k {
                break
            }
        }
    }
    
    return filtered
}
```

**Collection 层统一过滤（含容错处理）**：

```go
func (c *Collection) Search(query []float32, k int) ([]SearchResult, error) {
    // 使用 Storage 的 DeletionVector 进行过滤
    isDeleted := func(nodeID int) bool {
        // nodeID -> docID
        docID, ok := c.nodeToDoc[nodeID]
        if !ok {
            // 映射不存在：该 node 可能已被清理或从未存在
            // 视为已删除（容错处理，防止并发问题）
            return true
        }
        
        // docID -> rowID -> check DV
        return c.storage.IsDeleted(docID)
    }
    
    results := c.index.SearchWithDV(query, k, c.config.EfSearch, isDeleted)
    return results, nil
}
```

---

### 3.4 DocumentStorage 集成（使用扩展的 metadata）

**步骤 1：扩展 docMeta 结构**

修改 `vego/storage.go` 中的 `docMeta`：

```go
// docMeta stores metadata for a document
type docMeta struct {
    ID       string                 `json:"id"`
    RowIndex int64                  `json:"row_index"`  // 新增：行号
    Metadata map[string]interface{} `json:"metadata"`
}
```

**步骤 2：DocumentStorage 结构**

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

    // 新增：DeletionVector（使用行号而非 hash）
    deletionVector *index.DeletionVector
}

func NewDocumentStorage(path string, dimension int, cache ...*format.BlockCache) (*DocumentStorage, error) {
    s := &DocumentStorage{
        // ... 现有初始化
        deletionVector: index.NewDeletionVector(),
    }

    // 尝试加载已存在的 DV
    dvPath := filepath.Join(s.path, delFileName)
    if dv, err := index.Deserialize(dvPath); err == nil {
        s.deletionVector = dv
    }

    return s, nil
}
```

**步骤 3：使用 metadata 获取 rowID**

```go
// getRowID returns the row index for a document ID
// 从 metadata 中直接读取（扩展的 docMeta 包含 row_index）
// ⚠️ 注意：rowIndex 必须与物理存储位置保持一致！
// - Insert 时：由 Insert 方法设置
// - Flush 时：由 writeColumnStorage 同步更新
// - Compact 时：由 rewriteStorage 完全重建
func (s *DocumentStorage) getRowID(id string) (int64, bool) {
    idHash := hashID(id)
    
    s.metaStore.mu.RLock()
    defer s.metaStore.mu.RUnlock()
    
    meta, exists := s.metaStore.entries[idHash]
    if !exists {
        return -1, false
    }
    
    // 返回存储的 rowIndex
    return meta.RowIndex, true
}

// MarkDeleted marks a document as deleted using rowID (logical deletion)
func (s *DocumentStorage) MarkDeleted(id string) error {
    rowID, exists := s.getRowID(id)
    if !exists {
        return ErrDocumentNotFound
    }
    
    s.deletionVector.MarkDeleted(uint32(rowID))
    return nil
}

// IsDeleted checks if a document is deleted (by rowID)
func (s *DocumentStorage) IsDeleted(id string) bool {
    rowID, exists := s.getRowID(id)
    if !exists {
        return false
    }
    return s.deletionVector.IsDeleted(uint32(rowID))
}

// IsDeletedByRowID checks if a row is deleted directly by rowID
func (s *DocumentStorage) IsDeletedByRowID(rowID int64) bool {
    return s.deletionVector.IsDeleted(uint32(rowID))
}

// Insert 文档并分配 RowIndex
// 关键点：Insert 返回的 rowIndex 必须保存到 docMeta 中
func (s *DocumentStorage) Insert(doc *Document) (int64, error) {
    s.mu.Lock()
    defer s.mu.Unlock()

    // 1. 计算新文档的 rowIndex（当前文件已有行数）
    var rowIndex int64
    if s.formatWriter != nil {
        rowIndex = int64(s.formatWriter.DocumentCount())
    }

    // 2. 写入文档到存储
    if err := s.writeDocument(doc, rowIndex); err != nil {
        return -1, err
    }

    // 3. ⚠️ 关键：更新 metadata 中的 RowIndex
    idHash := hashID(doc.ID)
    s.metaStore.mu.Lock()
    if meta, exists := s.metaStore.entries[idHash]; exists {
        meta.RowIndex = rowIndex
        s.metaStore.entries[idHash] = meta
    } else {
        // 新文档，创建 metadata
        s.metaStore.entries[idHash] = &docMeta{
            ID:       doc.ID,
            RowIndex: rowIndex,
            Metadata: doc.Metadata,
        }
        s.metaStore.idToHash[doc.ID] = idHash
    }
    s.metaStore.mu.Unlock()

    return rowIndex, nil
}

// writeColumnStorage - 批量写入列存储
// ⚠️ 关键：写入后必须同步更新所有 metadata 的 RowIndex
func (s *DocumentStorage) writeColumnStorage(docs []*Document) error {
    // ... 现有写入逻辑 ...
    
    // 关键步骤：写入后 docs[i] 的行号就是 i，需要同步更新 metadata
    s.metaStore.mu.Lock()
    for i, doc := range docs {
        idHash := hashID(doc.ID)
        if meta, exists := s.metaStore.entries[idHash]; exists {
            meta.RowIndex = int64(i)  // 更新行号
            s.metaStore.entries[idHash] = meta
        }
        // 注意：如果 metadata 不存在，Insert 方法已经处理过了
    }
    s.metaStore.mu.Unlock()
    
    return nil
}

// rewriteStorage - 重写整个存储（Compact 时使用）
// ⚠️ 关键：所有文档的 RowIndex 都会改变，必须完全重建 metadata
func (s *DocumentStorage) rewriteStorage(remainingDocs []*Document) error {
    // 1. 过滤掉已删除的文档
    activeDocs := make([]*Document, 0, len(remainingDocs))
    for _, doc := range remainingDocs {
        if !s.IsDeleted(doc.ID) {
            activeDocs = append(activeDocs, doc)
        }
    }
    
    // 2. 重建 RowIndex 文件（新的行号分配）
    // 使用 RowIndexWriter 写入新文件...
    
    // 3. ⚠️ 关键：完全重建 metadata 的 RowIndex
    // 因为 Compact 后文档顺序完全改变，RowIndex 全部重新分配
    s.metaStore.mu.Lock()
    for i, doc := range activeDocs {
        idHash := hashID(doc.ID)
        if meta, exists := s.metaStore.entries[idHash]; exists {
            meta.RowIndex = int64(i)
            s.metaStore.entries[idHash] = meta
        }
    }
    s.metaStore.mu.Unlock()
    
    // 4. 重置 DV（所有有效文档都是未删除状态）
    s.deletionVector = index.NewDeletionVector()
    
    // 5. 立即保存 metadata 和 DV
    if err := s.saveMetadata(); err != nil {
        return err
    }
    return s.saveDeletionVector()
}

// Save persists the DeletionVector to disk
func (s *DocumentStorage) saveDeletionVector() error {
    dataFile := filepath.Join(s.path, dataFileName)
    dvPath := GetDeletionVectorPath(dataFile)
    return s.deletionVector.Serialize(dvPath)
}

// Flush includes DV persistence and compaction check
func (s *DocumentStorage) flush() error {
    // ... 现有逻辑 ...

    // 检查是否需要压缩
    totalDocs := len(allDocs) + len(s.writeBuffer)
    deletedCount := s.deletionVector.Count()
    deletionRate := float64(deletedCount) / float64(totalDocs)

    if deletionRate > 0.3 { // 30% 阈值
        // 执行压缩
        if err := s.compact(); err != nil {
            return err
        }
    }

    // 保存 DV（metadata.json 已在 saveMetadata 中处理）
    if err := s.saveDeletionVector(); err != nil {
        return err
    }

    return nil
}
```

**步骤 4：向后兼容处理**

旧数据可能没有 `row_index` 字段，需要在加载时重建：

```go
func (s *DocumentStorage) loadMetadata() error {
    // ... 读取 metadata.json ...
    
    for idHash, meta := range stored.Entries {
        // 向后兼容：旧数据没有 row_index，需要从文件重建
        if meta.RowIndex == 0 {
            // 从 RowIndex 文件查找
            rowIdx := s.lookupRowIndexFromFile(meta.ID)
            meta.RowIndex = rowIdx
        }
        s.metaStore.entries[idHash] = meta
        s.metaStore.idToHash[meta.ID] = idHash
    }
    
    return nil
}

// lookupRowIndexFromFile 从 RowIndex 中查找行号（用于向后兼容）
func (s *DocumentStorage) lookupRowIndexFromFile(id string) int64 {
    dataFile := filepath.Join(s.path, dataFileName)
    reader, err := column.NewRowIndexReader(dataFile)
    if err != nil {
        return -1
    }
    defer reader.Close()
    
    rowIdx, err := reader.LookupRowID(id)
    if err != nil {
        return -1
    }
    return rowIdx
}
```
```

---

### 3.5 Collection 层集成（简化版）

**修改**：`vego/collection.go`

```go
func (c *Collection) Delete(id string) error {
    return c.DeleteContext(context.Background(), id)
}

func (c *Collection) DeleteContext(ctx context.Context, id string) error {
    c.mu.Lock()
    defer c.mu.Unlock()

    // 检查文档是否存在
    _, exists := c.docToNode[id]
    if !exists {
        return wrapError("DeleteContext", c.name, id, ErrDocumentNotFound)
    }

    // 从 Storage 标记删除（单一数据源）
    if err := c.storage.MarkDeleted(id); err != nil {
        return err
    }

    // 延迟清理映射（在 Compact 时统一处理）
    // 保留映射以便后续压缩时能找到对应的 node

    return nil
}

// Update 使用 DV 实现真正的更新
func (c *Collection) Update(doc *Document) error {
    c.mu.Lock()
    defer c.mu.Unlock()

    // 1. 检查文档是否存在
    oldNodeID, exists := c.docToNode[doc.ID]
    if exists {
        // 2. ⚠️ 关键时序：必须先 MarkDeleted 再 Insert！
        // 原因：MarkDeleted 使用当前的 docMeta.RowIndex（旧值）
        // 如果先 Insert，docMeta.RowIndex 会被更新为新值
        // 导致旧版本无法被正确标记删除
        if err := c.storage.MarkDeleted(doc.ID); err != nil {
            return err
        }
        // 3. 更新 docToNode 映射（指向新的将创建的 node）
        // 注意：不删除 nodeToDoc[oldNodeID]！
        // 原因：并发搜索可能正在使用旧 nodeID
        // 保留映射直到 Compact 统一清理
        delete(c.docToNode, doc.ID)
        // delete(c.nodeToDoc, oldNodeID)  // 延迟清理！
    }

    // 4. 插入新版本（创建新 node 和新 row）
    // insertInternal 会：
    // - 分配新的 rowIndex（追加到文件末尾）
    // - 更新 docMeta.RowIndex 为新值
    // - 保存 metadata.json
    return c.insertInternal(doc)
}

// Get 自动过滤已删除文档
func (c *Collection) Get(id string) (*Document, error) {
    c.mu.RLock()
    defer c.mu.RUnlock()

    // 检查是否已删除
    if c.storage.IsDeleted(id) {
        return nil, ErrDocumentNotFound
    }

    // ... 原有获取逻辑 ...
}
```

---

### 3.6 压缩策略（并发安全）

> **设计文档**：详细的压缩策略对比见 [COMPACTION.md](COMPACTION.md)，包含 9 种策略的完整分析。

**触发条件**：
- 删除率 > 30%（可配置）
- 手动触发：`collection.Compact()`

**当前实现策略（阻塞式）**：
- 压缩期间阻塞所有读写操作（Insert/Update/Delete/Get/Search）
- 实现简单，数据一致性最强
- 适合维护窗口和批处理场景

**未来优化方向**：
- **轻量锁方案**：读取不阻塞，仅阻塞写入（4-5倍工程复杂度）
- **后台双写**：读写都不阻塞，但实现复杂
- 详见 [COMPACTION.md](COMPACTION.md) 方案对比

**压缩流程**：

```go
func (c *Collection) Compact() error {
    // 1. 获取写锁（阻塞所有修改）
    c.mu.Lock()
    defer c.mu.Unlock()
    
    // 2. 获取所有有效文档（排除已删除）
    validDocs, err := c.storage.GetAllValidDocuments()
    if err != nil {
        return err
    }
    
    // 3. 重建 HNSW 索引
    newIndex := index.NewHNSW(c.config.toIndexConfig())
    newDocToNode := make(map[string]int)
    newNodeToDoc := make(map[int]string)
    
    for _, doc := range validDocs {
        nodeID := newIndex.Add(doc.Vector)
        newDocToNode[doc.ID] = nodeID
        newNodeToDoc[nodeID] = doc.ID
    }

    // 4. 重写存储文件
    if err := c.storage.Rewrite(validDocs); err != nil {
        return err
    }

    // 5. 清除 DV（因为物理数据已重写）
    c.storage.ClearDeletionVector()

    // 6. 原子替换索引和映射
    c.index = newIndex
    c.docToNode = newDocToNode
    c.nodeToDoc = newNodeToDoc

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
| Delete | O(1) 标记 | O(1) 位图标记 |
| Get | O(1) | O(1) + O(1) DV检查 |
| Search 过滤 | N/A | O(k) 位图检查（k=候选数）|
| Flush | O(N) 重写 | O(1) DV持久化 |
| Update | O(N) 重写 | O(1) 标记 + O(1) 插入 |
| 压缩 | N/A | O(N) 重建（后台）|

**关键改进**：
- Delete/Update 不再需要立即重写文件
- Flush 仅持久化 DV 位图（毫秒级）
- 压缩可以后台异步执行

### 5.2 内存开销

**DeletionVector**：
- RoaringBitmap：每个删除约 2-8 字节
- 10% 删除率：10K 文档 ≈ 8KB
- 50% 删除率：10K 文档 ≈ 40KB

**Metadata 扩展（row_index 字段）**：
- 每个 docMeta 增加 8 字节（int64）
- 10K 文档 ≈ 80KB
- 已在 metadata 中，无额外文件开销

**总计**：
- 10K 文档，10% 删除：~88KB
- 100K 文档，10% 删除：~880KB
- 比之前方案（单独 idToRow 映射）节省约 75% 内存
- 压缩后可完全释放已删除文档的 metadata

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
| 内存泄漏 | 高 | DV 定期压缩清理；映射在 Compact 后重建 |
| 搜索性能退化 | 低 | O(1) 位图检查，实测开销 < 1% |
| 数据不一致 | 中 | 单一 DV 数据源；Flush 时原子写入 |
| 压缩阻塞 | 中 | 写锁保护；可考虑异步压缩 |
| nodeID 映射失效 | 中 | 延迟清理策略；搜索容错处理（缺失映射视为已删除） |
| 映射不一致 | 低 | Compact 时统一重建所有映射 |

---

## 九、依赖

- `github.com/RoaringBitmap/roaring` - 位图实现
- 现有 HNSW 索引
- 现有 DocumentStorage

---

## 十、相关文件

- `index/deletion_vector.go` - DV 数据结构
- `index/deletion_vector_persist.go` - 持久化
- `index/hnsw.go` - HNSW 搜索接口（查询 Storage DV）
- `vego/storage.go` - Storage 集成（含扩展的 docMeta）
- `vego/collection.go` - Collection API
- `vego/collection_compact.go` - 压缩功能（可选单独文件）

---

## 附录：设计变更记录

| 版本 | 日期 | 变更 |
|------|------|------|
| v1.0 | 2026-02-28 | 初始设计，使用 idHash 作为 DV 键 |
| v1.1 | 2026-02-28 | 修正：使用行号(rowID)作为 DV 键，避免 hash 冲突 |
| v1.1 | 2026-02-28 | 简化：HNSW 不维护独立 DV，统一查询 Storage |
| v1.1 | 2026-02-28 | 明确：持久化文件命名 `{data}.del` |
| v1.1 | 2026-02-28 | 补充：压缩时并发安全考虑 |
| v1.2 | 2026-02-28 | 优化：使用 metadata.json 存储 rowIndex（而非单独 idToRow 映射） |
| v1.3 | 2026-02-28 | 明确：nodeID/rowID 映射关系，延迟清理策略，搜索容错处理 |

---

*文档创建时间：2026-02-28*
*最后更新：2026-02-28*
