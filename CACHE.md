# 读写全链路缓存的设计和实现

## 完善 BlockCache 的设计

### 一、当前实现状态

**文件位置**: `storage/format/blockcache.go`

**已实现功能**:
- LRU (Least Recently Used) 淘汰策略
- 容量管理 (capacity, size)
- 基本的 Get/Put/Clear/Remove 操作
- 并发安全 (sync.RWMutex)
- 测试覆盖基本功能

---

### 二、需要修改的问题

#### 问题 1: Get() 使用了写锁 [P0 - 严重]

**位置**: `storage/format/blockcache.go:39-41`

**当前代码**:
```go
func (c *BlockCache) Get(key string) ([]byte, bool) {
    c.mu.Lock()         // ❌ 使用了写锁
    defer c.mu.Unlock()
    // ...
}
```

**问题**: 每次读取都要加写锁，高并发下会严重影响性能

**修复方案**:
```go
func (c *BlockCache) Get(key string) ([]byte, bool) {
    c.mu.RLock()        // ✅ 改为读锁
    defer c.mu.RUnlock()
    // ...
}
```

---

#### 问题 2: 缺少缓存命中率统计 [P1]

**当前 Stats 结构体**:
```go
type BlockCacheStats struct {
    ItemCount int
    Size      int64
    Capacity  int64
}
```

**建议扩展**:
```go
type BlockCacheStats struct {
    ItemCount  int     // 缓存项数量
    Size       int64   // 当前缓存大小 (bytes)
    Capacity   int64   // 缓存容量 (bytes)
    Hits       int64  // 缓存命中次数
    Misses     int64  // 缓存未命中次数
    Evictions  int64  // 淘汰次数
}

func (c *BlockCache) Stats() BlockCacheStats {
    c.mu.RLock()
    defer c.mu.RUnlock()
    return BlockCacheStats{
        ItemCount:  len(c.items),
        Size:       c.size,
        Capacity:   c.capacity,
        Hits:       c.hits,
        Misses:     c.misses,
        Evictions:  c.evictions,
    }
}
```

**需要在 BlockCache 结构体中新增字段**:
```go
type BlockCache struct {
    mu        sync.RWMutex
    capacity  int64
    size      int64
    items     map[string]*list.Element
    lru       *list.List

    // 新增统计字段
    hits      int64
    misses    int64
    evictions int64
}
```

---

#### 问题 3: 数据安全问题 [P2]

**问题**: `Put` 直接引用传入的 `[]byte`，如果外部修改，缓存数据也会变

**当前代码**:
```go
func (c *BlockCache) Put(key string, data []byte) {
    // ...
    entry.data = data  // 直接引用
    // ...
}
```

**建议修复**:
```go
func (c *BlockCache) Put(key string, data []byte) {
    // 拷贝数据，避免外部修改影响缓存
    entry.data = make([]byte, len(data))
    copy(entry.data, data)
    // ...
}
```

---

### 三、可选增强功能

#### 功能 1: TTL (过期时间) 支持

**场景**:
- 数据文件被外部修改
- 长时间未访问的数据可能失效

**设计**:
```go
type cacheEntry struct {
    key      string
    data     []byte
    size     int64
    expireAt int64  // 过期时间戳，0 表示永不过期
}

// Put 增加可选参数
type PutOption func(*cacheEntry)

func WithTTL(d time.Duration) PutOption {
    return func(e *cacheEntry) {
        e.expireAt = time.Now().Add(d).UnixNano()
    }
}

func (c *BlockCache) Put(key string, data []byte, opts ...PutOption) {
    entry := &cacheEntry{
        key:  key,
        data: data,
        size: int64(len(data)),
    }
    for _, opt := range opts {
        opt(entry)
    }
    // ...
}

// Get 时检查过期
func (c *BlockCache) Get(key string) ([]byte, bool) {
    c.mu.RLock()
    defer c.mu.RUnlock()

    elem, ok := c.items[key]
    if !ok {
        c.misses++
        return nil, false
    }

    entry := elem.Value.(*cacheEntry)

    // 检查是否过期
    if entry.expireAt > 0 && time.Now().UnixNano() > entry.expireAt {
        // 过期，移除
        delete(c.items, key)
        c.lru.Remove(elem)
        c.size -= entry.size
        c.misses++
        return nil, false
    }

    c.lru.MoveToFront(elem)
    c.hits++
    return entry.data, true
}
```

---

#### 功能 2: 请求合并 (Request Coalescing)

**问题**: 多个 goroutine 同时请求同一个未缓存的 key，会导致重复 I/O（惊群效应）

**设计**:
```go
type BlockCache struct {
    // ... 现有字段

    // 请求合并: 正在加载的 key -> 等待结果的 channel
    pending map[string]chan []byte
}

func (c *BlockCache) GetOrLoad(key string, loader func() ([]byte, error)) ([]byte, error) {
    // 1. 先查缓存
    c.mu.RLock()
    if elem, ok := c.items[key]; ok {
        c.lru.MoveToFront(elem)
        c.mu.RUnlock()
        return elem.Value.(*cacheEntry).data, nil
    }
    c.mu.RUnlock()

    // 2. 检查是否有其他请求正在加载
    c.mu.Lock()
    if ch, ok := c.pending[key]; ok {
        // 有请求正在加载，等待结果
        c.mu.Unlock()
        result := <-ch
        if result == nil {
            return nil, errors.New("load failed")
        }
        return result, nil
    }

    // 3. 自己加载
    ch := make(chan []byte, 1)
    c.pending[key] = ch
    c.mu.Unlock()

    data, err := loader()
    if err != nil {
        ch <- nil
    } else {
        ch <- data
    }

    // 4. 存入缓存
    if err == nil {
        c.mu.Lock()
        c.Put(key, data)
        delete(c.pending, key)
        c.mu.Unlock()
    }

    close(ch)
    return data, err
}
```

---

#### 功能 3: 多级淘汰策略

**当前**: 只有 LRU

**可选**: LFU (Least Frequently Used)

```go
type EvictionPolicy int

const (
    EvictionLRU EvictionPolicy = iota
    EvictionLFU
)

type BlockCache struct {
    // ... 现有字段
    policy EvictionPolicy

    // LFU 需要额外字段
    freq   map[string]int64
}
```

---

### 四、接口设计改进

```go
// BlockCache 配置选项
type BlockCacheOption func(*BlockCache)

// WithCapacity 设置缓存容量
func WithCapacity(capacity int64) BlockCacheOption {
    return func(c *BlockCache) {
        c.capacity = capacity
    }
}

// WithTTL 设置默认 TTL
func WithTTL(ttl time.Duration) BlockCacheOption {
    return func(c *BlockCache) {
        c.ttl = ttl
    }
}

// WithStats 启用统计
func WithStats() BlockCacheOption {
    return func(c *BlockCache) {
        c.enableStats = true
    }
}

// NewBlockCache 创建缓存实例
func NewBlockCache(opts ...BlockCacheOption) *BlockCache {
    c := &BlockCache{
        capacity: 64 * 1024 * 1024, // 默认 64MB
    }
    for _, opt := range opts {
        opt(c)
    }
    c.items = make(map[string]*list.Element)
    c.lru = list.New()
    return c
}
```

### 六、缓存失效策略（已迁移至"读写一致性解决方案"章节）

详见下文 **"加入缓存后的读写一致性问题的解决方案"** 章节。

## BlockCache 在数据读取全链路的集成

### 四个核心模块的集成可行性分析

| 模块 | 当前状态 | 支持可行性 | 复杂度 | 优先级 |
|------|----------|-----------|--------|--------|
| **RowIndexReader** | ✅ 已支持 | 直接使用 | 低 | 已完成 |
| **Reader (base)** | ❌ 未支持 | **完全可行** | 中 | **P0** |
| **DocumentStorage** | ❌ 未支持 | **可行但需设计** | 中 | **P1** |
| **AsyncIO** | ❌ 不适合 | **架构冲突** | 高 | 不推荐 |

---

#### 1. RowIndexReader（已支持，完善即可）

**当前状态**：`storage/column/rowindex_reader.go` 已集成 BlockCache

```go
type RowIndexReader struct {
    *Reader
    blockCache     *format.BlockCache  // ← 已有字段
    ...
}
```

**使用场景**：
- 缓存 RowIndex 元数据页（id → row 映射）
- 避免重复解析索引

**当前局限**：
- 仅缓存索引页，不缓存数据页
- 需要显式传入 BlockCache 实例：`NewRowIndexReaderWithCache(filename, cache)`

**建议**：保持现状，与全局 PageCache 集成后可自动生效

---

#### 2. Reader (base)（强烈推荐支持，这是缓存的核心）

**为什么必须支持**：
`Reader` 是底层数据读取入口，**所有列数据读取都经过这里**，是最关键的缓存集成点。

**改造方案**：

```go
// storage/column/reader.go
type Reader struct {
    file       *os.File
    header     *format.Header
    footer     *format.Footer
    pageReader *PageReader
    
    // 新增：BlockCache 支持
    blockCache *format.BlockCache  // 可选，为 nil 时不使用缓存
    cacheKey   string              // 文件唯一标识（用于 cache key）
    ...
}

// 改造 readPage 方法
func (r *Reader) readPage(pageIdx format.PageIndex) (*format.Page, error) {
    // 1. 先查 BlockCache
    if r.blockCache != nil {
        // 优化：cache key 不需要包含 size（可从 Footer 获取）
        key := fmt.Sprintf("%s:%d", r.cacheKey, pageIdx.Offset)
        if cached, ok := r.blockCache.Get(key); ok {
            page := &format.Page{}
            if err := page.UnmarshalBinary(cached); err == nil {
                return page, nil  // 缓存命中
            }
        }
    }

    // 2. 缓存未命中，读磁盘
    page, err := r.readPageFromDisk(pageIdx)
    if err != nil {
        return nil, err
    }

    // 3. 存入缓存
    if r.blockCache != nil {
        key := fmt.Sprintf("%s:%d", r.cacheKey, pageIdx.Offset)
        if data, err := page.MarshalBinary(); err == nil {
            r.blockCache.Put(key, data)
        }
    }
    return page, nil
}
```

**收益**：
- 所有列数据读取都经过缓存
- 自动支持 Page 级别的 LRU 淘汰
- 多个 Reader 实例可共享同一 BlockCache

**优先级**：**P0** - 这是实现全链路缓存的核心

---

#### 3. DocumentStorage（可行但需间接支持）

**为什么不直接支持**：
`DocumentStorage` 位于 **vego 层**（业务层），而 BlockCache 位于 **storage 层**（数据层）。正确的设计是**通过使用带缓存的 Reader 间接受益**。

```
vego/storage.go (DocumentStorage)
    ↓ 调用
storage/column/reader.go (Reader)  ← BlockCache 在这里
    ↓ 使用
storage/format/blockcache.go (BlockCache)
```

**正确的集成方式**：

DocumentStorage 需要**显式传递 BlockCache 给 Reader**，而不是依赖 Reader 内部创建。

```go
type DocumentStorage struct {
    path      string
    dimension int

    // 共享的 BlockCache 实例（传递给下层 Reader）
    blockCache *format.BlockCache

    // 业务层 Document 缓存（可选，缓存反序列化后的对象）
    docCache *DocumentLRUCache
}

// 初始化时创建共享 BlockCache
func NewDocumentStorage(path string, dimension int) (*DocumentStorage, error) {
    // ...
    s := &DocumentStorage{
        // ...
        blockCache: format.NewBlockCache(format.DefaultBlockCacheSize), // 64MB
        docCache:   NewDocumentLRUCache(10000), // 1万文档
    }
    return s, nil
}

// 读取时显式传递 BlockCache 给 Reader
func (s *DocumentStorage) Get(id string) (*Document, error) {
    // 使用带缓存的 Reader
    reader, _ := column.NewReaderWithCache(s.dataFilePath(), s.blockCache)
    // 现在 readPage 会使用传入的 BlockCache
    // ...
}
```

**为什么需要显式传递**：

```
DocumentStorage (持有 BlockCache 实例)
    │
    ▼ 显式传递 blockCache
Reader (使用传入的 blockCache)
    │
    ▼ 调用 Get/Put
BlockCache (共享的缓存实例)
```

如果不显式传递：
- 每个 Reader 创建自己的 BlockCache → 缓存不共享
- 内存浪费，命中率低

**需要新增的 Reader 构造函数**：

```go
// storage/column/reader.go

// NewReaderWithCache 创建带 BlockCache 的 Reader
func NewReaderWithCache(filename string, cache *format.BlockCache) (*Reader, error) {
    reader, err := NewReader(filename)
    if err != nil {
        return nil, err
    }
    reader.blockCache = cache
    reader.cacheKey = generateCacheKey(filename)
    return reader, nil
}

// generateCacheKey 生成文件唯一标识
func generateCacheKey(filename string) string {
    // 使用文件路径的 hash 作为 key 前缀
    h := fnv.New64a()
    h.Write([]byte(filename))
    return fmt.Sprintf("%x", h.Sum64())
}
```

**建议**：
- DocumentStorage **持有** BlockCache 实例（用于共享）
- 创建 Reader 时**显式传递** BlockCache
- 可额外添加 DocumentCache（业务层缓存，缓存反序列化后的 Document 对象）

**优先级**：**P1** - 需要配合 Reader 改造完成

---

#### 4. AsyncIO（不适合，保持调度器纯粹性）

**为什么不支持**：
AsyncIO 的定位是**异步 I/O 调度器**，不是数据消费者。缓存应该在**数据消费端**（Reader）管理。

**架构分层**：

```
┌─────────────────────────────────────────┐
│           AsyncIO (调度层)               │
│  - 请求排序（按 offset 排序）            │
│  - 调度执行（Worker Pool）               │
│  - 文件句柄管理（FilePool）              │
│                                         │
│  ❌ 不应该关心缓存                        │
└─────────────────────────────────────────┘
              ↓ 返回原始数据
┌─────────────────────────────────────────┐
│         Reader (数据消费层)              │
│  ┌──────────┐    ┌──────────┐          │
│  │ BlockCache│ <- │ readPage │          │
│  │ (缓存)    │    │ (读磁盘) │          │
│  └──────────┘    └──────────┘          │
│                                         │
│  ✅ 管理缓存是 Reader 的职责             │
└─────────────────────────────────────────┘
```

**正确配合方式**：

```go
// Reader 使用 AsyncIO 读取，然后自己管理缓存
func (r *Reader) readPageAsync(pageIdx format.PageIndex) (*format.Page, error) {
    // 1. 先查缓存（Reader 的职责）
    if r.blockCache != nil {
        if cached, ok := r.blockCache.Get(key); ok {
            return cached, nil
        }
    }
    
    // 2. 缓存未命中，使用 AsyncIO 读取
    resultCh := r.asyncIO.Read(ctx, r.fileID, pageIdx.Offset, pageIdx.Size)
    result := <-resultCh
    
    // 3. 存入缓存（Reader 的职责）
    if r.blockCache != nil {
        r.blockCache.Put(key, result.Data)
    }
    return result.Data, nil
}
```

**结论**：
- AsyncIO **不应该**直接支持 BlockCache
- 保持调度器的纯粹性，专注于**高效调度 I/O**
- 缓存由上层 Reader 管理，符合单一职责原则

**优先级**：不推荐改造

---

### 集成优先级建议（修订版）

```
Phase 1 (P0): BlockCache 基础修复
    ├─ 修复 Get() 使用读锁（问题 1）
    ├─ 添加命中率统计（问题 2）
    └─ 数据拷贝避免外部修改（问题 3）

Phase 2 (P0): Reader (base) 支持 BlockCache
    ├─ Reader 添加 blockCache 字段
    ├─ 改造 readPage() 使用缓存
    └─ 新增 NewReaderWithCache() 构造函数

Phase 3 (P1): DocumentStorage 集成
    ├─ 创建共享 BlockCache 实例
    ├─ 使用 NewReaderWithCache() 传递缓存
    └─ 实现缓存失效（flush/Delete 时）

Phase 4 (可选): 增强功能
    ├─ TTL 支持
    ├─ 请求合并
    └─ 多级淘汰策略
```

### 总结

- ✅ **RowIndexReader**：已支持，完善即可
- ✅ **Reader (base)**：**强烈推荐支持**，这是缓存的核心
- ⚠️ **DocumentStorage**：间接支持（通过 Reader），可额外加 DocumentCache
- ❌ **AsyncIO**：**不适合**，保持调度器纯粹性

## 加入缓存后的读写一致性问题的解决方案

### 核心问题

当数据被修改（Insert/Update/Delete/flush）时，BlockCache 中可能缓存了旧数据，导致**读写不一致**。

### 场景分析

| 操作 | 影响范围 | 风险 | 处理方式 |
|------|----------|------|----------|
| Insert() | 新增行 | 无风险 | 追加写入，不影响现有缓存 |
| Update() | 修改单行 | 缓存旧数据 | 清除该行缓存 |
| Delete() | 删除单行 | 缓存已删除数据 | 清除该行缓存 |
| flush() | 整文件重写 | 全部缓存失效 | **Clear 所有缓存** |
| Save() | 全量持久化 | 全部缓存失效 | **Clear 所有缓存** |

### 方案一: 写时失效 (Write-Invalidate) - 推荐

**核心思想**：数据修改时，主动清除受影响的缓存。

```go
func (s *DocumentStorage) flush() error {
    s.mu.Lock()
    defer s.mu.Unlock()

    // 1. 写入磁盘
    err := s.writeColumnStorage()
    if err != nil {
        return err
    }

    // 2. 清除所有缓存（文件已重写，所有旧缓存失效）
    if s.blockCache != nil {
        s.blockCache.Clear()
    }

    // 3. 清除业务层缓存
    if s.docCache != nil {
        s.docCache.Clear()
    }

    // 4. 重建行索引
    return s.rebuildRowIndex()
}

func (s *DocumentStorage) Delete(id string) error {
    s.mu.Lock()
    defer s.mu.Unlock()

    // 1. 获取 idHash 和 rowIdx
    idHash, exists := s.metaStore.idToHash[id]
    if !exists {
        return ErrDocumentNotFound
    }

    rowIdx, found := s.rowIndex.Lookup(idHash)
    if !found {
        return ErrDocumentNotFound
    }

    // 2. 执行删除（标记删除或物理删除）
    // ... 删除逻辑 ...

    // 3. 清除 BlockCache 中该行的缓存
    if s.blockCache != nil {
        cacheKey := fmt.Sprintf("%s:%d", s.dataFileCacheKey, rowIdx)
        s.blockCache.Remove(cacheKey)
    }

    // 4. 清除业务层缓存
    if s.docCache != nil {
        s.docCache.Remove(id)
    }

    // 5. 更新行索引
    s.rowIndex.Delete(idHash)

    return nil
}

func (s *DocumentStorage) Update(doc *Document) error {
    s.mu.Lock()
    defer s.mu.Unlock()

    // 1. 检查文档是否存在
    idHash, exists := s.metaStore.idToHash[doc.ID]
    if !exists {
        return ErrDocumentNotFound
    }

    rowIdx, found := s.rowIndex.Lookup(idHash)
    if !found {
        return ErrDocumentNotFound
    }

    // 2. 清除旧缓存
    if s.blockCache != nil {
        cacheKey := fmt.Sprintf("%s:%d", s.dataFileCacheKey, rowIdx)
        s.blockCache.Remove(cacheKey)
    }
    if s.docCache != nil {
        s.docCache.Remove(doc.ID)
    }

    // 3. 写入缓冲区（稍后 flush 时会再次清除缓存）
    return s.insertToBuffer(doc)
}
```

**优点**：
- 实现简单，立即生效
- 无额外内存开销

**缺点**：
- flush 时全量清除，缓存命中率下降
- 频繁 flush 导致缓存失效频繁

### 方案二: 版本号机制 (Version-Based) - 备选

**核心思想**：缓存 key 包含数据版本号，旧版本数据自然失效。

```go
type DocumentStorage struct {
    path         string
    dimension    int
    blockCache   *format.BlockCache
    dataVersion  int64  // 数据版本号，每次 flush 递增
    fileHash     string // 文件路径 hash，用于 cache key
}

func NewDocumentStorage(path string, dimension int) (*DocumentStorage, error) {
    // ...
    s := &DocumentStorage{
        // ...
        dataVersion: 1,
        fileHash:    generateFileHash(path),
    }
    return s, nil
}

func (s *DocumentStorage) flush() error {
    s.mu.Lock()
    defer s.mu.Unlock()

    // 1. 写入磁盘
    err := s.writeColumnStorage()
    if err != nil {
        return err
    }

    // 2. 递增版本号（旧版本缓存自然失效）
    atomic.AddInt64(&s.dataVersion, 1)

    // 3. 可选：异步清理旧版本缓存（后台任务）
    go s.cleanupOldVersionCache()

    return nil
}

// cache key 包含版本号
func (s *DocumentStorage) getCacheKey(rowIdx int64) string {
    version := atomic.LoadInt64(&s.dataVersion)
    return fmt.Sprintf("%s:v%d:r%d", s.fileHash, version, rowIdx)
}

func (s *DocumentStorage) readWithCache(rowIdx int64) ([]float32, error) {
    cacheKey := s.getCacheKey(rowIdx)

    // 1. 查缓存
    if data, ok := s.blockCache.Get(cacheKey); ok {
        return unmarshalVector(data)
    }

    // 2. 读磁盘
    vector, err := s.readVectorFromDisk(rowIdx)
    if err != nil {
        return nil, err
    }

    // 3. 写入缓存
    if data, err := marshalVector(vector); err == nil {
        s.blockCache.Put(cacheKey, data)
    }

    return vector, nil
}

// 后台清理旧版本缓存（可选优化）
func (s *DocumentStorage) cleanupOldVersionCache() {
    currentVersion := atomic.LoadInt64(&s.dataVersion)
    // 清理版本号 < currentVersion - 2 的缓存
    // 实现略...
}
```

**优点**：
- flush 时无需立即清除缓存，旧版本仍可服务读请求
- 减少缓存抖动

**缺点**：
- 实现复杂，需要版本管理
- 内存占用增加（多版本缓存共存）
- 需要后台清理任务

### 方案对比与推荐

| 方案 | 复杂度 | 内存开销 | 缓存命中率 | 推荐场景 |
|------|--------|----------|------------|----------|
| 写时失效 | 低 | 低 | flush 后下降 | **通用推荐** |
| 版本号机制 | 高 | 中 | 较高 | 频繁 flush 场景 |

**推荐**：使用**方案一（写时失效）**，原因：
1. 实现简单，易于维护
2. flush 操作相对低频（默认 1000 条文档触发一次）
3. 缓存重建成本低（RowIndex 在内存中，可快速重建缓存）

### 缓存失效的粒度控制

```go
// 细粒度：单行失效（Update/Delete）
func (s *DocumentStorage) invalidateRow(rowIdx int64) {
    if s.blockCache != nil {
        cacheKey := fmt.Sprintf("%s:%d", s.fileHash, rowIdx)
        s.blockCache.Remove(cacheKey)
    }
}

// 粗粒度：全量清除（flush/Save）
func (s *DocumentStorage) invalidateAll() {
    if s.blockCache != nil {
        s.blockCache.Clear()
    }
    if s.docCache != nil {
        s.docCache.Clear()
    }
}
```

### 总结

- **flush/Save**：必须清除所有缓存（文件重写）
- **Update/Delete**：清除受影响行的缓存
- **Insert**：无需清除缓存（追加写入）
- **推荐方案**：写时失效（简单可靠）
