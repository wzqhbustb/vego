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

---

### 五、BlockCache 在读取路径的集成

#### 集成点

| 模块 | 文件 | 方法 | 缓存内容 |
|------|------|------|----------|
| Reader | `storage/column/reader.go` | `readPageSync()` | 原始 Page 二进制 |
| RowIndexReader | `storage/column/rowindex_reader.go` | `loadRowIndex()` | RowIndex Page |

#### 集成代码示例

```go
// storage/column/reader.go

type Reader struct {
    // ... 现有字段
    blockCache *format.BlockCache  // 新增
}

func (r *Reader) readPage(pageIdx format.PageIndex) (*format.Page, error) {
    cacheKey := fmt.Sprintf("%s_page_%d_%d", r.file.Name(), pageIdx.Offset, pageIdx.Size)

    // 1. 先查 BlockCache
    if r.blockCache != nil {
        if data, ok := r.blockCache.Get(cacheKey); ok {
            page := &format.Page{}
            if err := page.UnmarshalBinary(data); err == nil {
                return page, nil // 缓存命中
            }
        }
    }

    // 2. 缓存未命中，根据模式读取
    var page *format.Page
    var err error
    if r.useAsync && r.asyncEnabled {
        page, err = r.readPageAsync(pageIdx)
    } else {
        page, err = r.readPageSync(pageIdx)
    }

    // 3. 存入缓存
    if err == nil && r.blockCache != nil {
        if data, err := page.MarshalBinary(); err == nil {
            r.blockCache.Put(cacheKey, data)
        }
    }

    return page, err
}
```

---

### 六、缓存失效策略

#### 场景分析

| 操作 | 影响范围 | 处理方式 |
|------|----------|----------|
| Insert() | 新增行 | 追加，不影响现有缓存 |
| Update() | 修改行 | 清除该行缓存 |
| Delete() | 删除行 | 清除该行缓存 |
| flush() | 整文件重写 | Clear 所有缓存 |
| Save() | 全量持久化 | Clear 所有缓存 |

#### 方案一: 写时失效 (Write-Invalidate)

```go
func (s *DocumentStorage) flush() error {
    // 1. 写入磁盘
    err := s.writeColumnStorage()
    if err != nil { return err }

    // 2. 清除所有缓存
    if s.blockCache != nil {
        s.blockCache.Clear()
    }

    // 3. 重建行索引
    return s.loadRowIndex()
}

func (s *DocumentStorage) Delete(id string) error {
    // ... 执行删除

    // 清除相关缓存
    if s.blockCache != nil {
        idHash := s.metaStore.idToHash[id]
        rowIdx := s.rowIndex.idHashToRow[idHash]
        cacheKey := fmt.Sprintf("%s_row_%d", s.dataPath, rowIdx)
        s.blockCache.Remove(cacheKey)
    }
    return nil
}
```

#### 方案二: 版本号机制

```go
type DocumentStorage struct {
    blockCache   *format.BlockCache
    dataVersion  int64  // 数据版本号
}

func (s *DocumentStorage) flush() error {
    err := s.writeColumnStorage()
    if err != nil { return err }

    // 递增版本号
    atomic.AddInt64(&s.dataVersion, 1)
    return nil
}

// 缓存 key 包含版本号
func (s *DocumentStorage) readWithCache(rowIdx int64) ([]float32, error) {
    version := atomic.LoadInt64(&s.dataVersion)
    cacheKey := fmt.Sprintf("%s_row_%d_v%d", s.dataPath, rowIdx, version)

    if data, ok := s.blockCache.Get(cacheKey); ok {
        return unmarshalVector(data)
    }

    // 读取并缓存
    vector, err := s.readVectorFromDisk(rowIdx)
    if err == nil {
        s.blockCache.Put(cacheKey, marshalVector(vector))
    }
    return vector, err
}
```

---

### 七、AsyncIO 与 BlockCache 的集成

#### 关系说明

**它们是互补的，不是互斥的**:
- **BlockCache**: 避免重复读取（空间换时间）
- **AsyncIO**: 并行化 I/O（并发换时间）

#### 集成流程

```
Get(id)
  │
  ├─→ 1. 查 DocCache (行级缓存)
  │     └─→ 命中 → 返回
  │
  ├─→ 2. 查 BlockCache (Page 级缓存)
  │     └─→ 命中 → 解码 → 返回
  │
  ├─→ 3. 读磁盘 (AsyncIO 或同步)
  │     ├─→ 异步: readPageAsync() → 存入 BlockCache → 解码
  │     └─→ 同步: readPageSync() → 存入 BlockCache → 解码
  │
  └─→ 4. 存入 DocCache → 返回
```

#### AsyncIO 读取后缓存

```go
func (r *Reader) readPageAsync(pageIdx format.PageIndex) (*format.Page, error) {
    cacheKey := r.cacheKey(pageIdx)

    // 再次检查缓存（可能有其他请求刚加载过）
    if r.blockCache != nil {
        if data, ok := r.blockCache.Get(cacheKey); ok {
            page := &format.Page{}
            if err := page.UnmarshalBinary(data); err == nil {
                return page, nil
            }
        }
    }

    // 使用 AsyncIO 读取
    resultCh := r.asyncIO.Read(ctx, r.fileID, pageIdx.Offset, pageIdx.Size)
    // ... 等待结果

    // 存入 BlockCache
    if r.blockCache != nil && page != nil {
        if data, err := page.MarshalBinary(); err == nil {
            r.blockCache.Put(cacheKey, data)
        }
    }
    return page, err
}
```

---

### 八、待完成事项清单

- [ ] 修复 Get() 锁问题 (P0)
- [ ] 添加缓存命中率统计 (P1)
- [ ] 修复数据安全问题 - 拷贝数据 (P2)
- [ ] 集成 BlockCache 到 Reader.readPageSync() (P0)
- [ ] 集成 BlockCache 到 AsyncIO 读取路径 (P1)
- [ ] 实现缓存失效策略 (P0)
- [ ] 可选: TTL 支持 (P3)
- [ ] 可选: 请求合并 (P3)


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
        key := fmt.Sprintf("%s:%d:%d", r.cacheKey, pageIdx.Offset, pageIdx.Size)
        if cached, ok := r.blockCache.Get(key); ok {
            page := &format.Page{}
            if err := page.UnmarshalBinary(cached); err == nil {
                return page, nil  // 缓存命中
            }
        }
    }
    
    // 2. 缓存未命中，读磁盘
    page, err := r.readPageFromDisk(pageIdx)
    
    // 3. 存入缓存
    if r.blockCache != nil && err == nil {
        if data, err := page.MarshalBinary(); err == nil {
            r.blockCache.Put(key, data)
        }
    }
    return page, err
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

```go
type DocumentStorage struct {
    path      string
    dimension int
    
    // 不需要直接持有 BlockCache
    // 但可以有 DocumentCache（业务层缓存）
    docCache *cache.DocumentCache
}

// 读取时自动使用 BlockCache
func (s *DocumentStorage) Get(id string) (*Document, error) {
    // 使用 RowIndexReader（内部使用带 BlockCache 的 Reader）
    reader, _ := column.NewRowIndexReader(s.dataFilePath())
    // readPage 会自动使用 BlockCache
}
```

**建议**：
- DocumentStorage **不直接**使用 BlockCache
- 通过使用带缓存的 Reader（如 RowIndexReader）**间接**支持
- 可额外添加 DocumentCache（业务层缓存，缓存反序列化后的 Document 对象）

**优先级**：**P1** - 通过 Reader 改造自动生效

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

### 集成优先级建议

```
Phase 1 (P0): Reader (base) 支持 BlockCache
    └─ 目标：让所有数据读取都能利用缓存
    
Phase 2 (P1): DocumentStorage 使用带缓存的 Reader
    └─ 目标：vego 层自动享受缓存加速
    
Phase 3 (长期): AsyncIO 保持现状
    └─ 目标：保持调度器纯粹性，不管理缓存
```

### 总结

- ✅ **RowIndexReader**：已支持，完善即可
- ✅ **Reader (base)**：**强烈推荐支持**，这是缓存的核心
- ⚠️ **DocumentStorage**：间接支持（通过 Reader），可额外加 DocumentCache
- ❌ **AsyncIO**：**不适合**，保持调度器纯粹性

## 加入缓存后的读写一致性问题的解决方案

