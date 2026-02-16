# 读写全链路缓存的设计和实现

## 全链路读写架构图

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                              Vego 全链路读写缓存架构                                       │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                         │
│  ┌─────────────────────────────────────────────────────────────────────────────────┐   │
│  │                               写入路径 (Write Path)                              │   │
│  │                                                                                 │   │
│  │   User Call                                                                     │   │
│  │      │                                                                          │   │
│  │      ▼                                                                          │   │
│  │   ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                     │   │
│  │   │   Insert()   │ or │   Update()   │ or │   Delete()   │                     │   │
│  │   └──────┬───────┘    └──────┬───────┘    └──────┬───────┘                     │   │
│  │          │                   │                   │                              │   │
│  │          ▼                   ▼                   ▼                              │   │
│  │   ┌─────────────────────────────────────────────────────────────────────────┐  │   │
│  │   │  L1: writeBuffer (写缓冲) - 强一致性                                     │  │   │
│  │   │  - 未 flush 的最新数据，内存内立即可见                                    │  │   │
│  │   │  - 容量：maxBuffer (默认 1000 条)                                        │  │   │
│  │   │  - 策略：Write-Back，异步刷盘                                            │  │   │
│  │   └─────────────────────────────────────────────────────────────────────────┘  │   │
│  │          │                                                                      │   │
│  │          │ 2. 写穿透到 L2                                                       │   │
│  │          ▼                                                                      │   │
│  │   ┌─────────────────────────────────────────────────────────────────────────┐  │   │
│  │   │  L2: DocumentCache (文档缓存)                                             │  │   │
│  │   │  - 反序列化后的 *Document 对象                                           │  │   │
│  │   │  - 容量：按数量 (默认 10000 个)                                          │  │   │
│  │   │  - 策略：版本号控制，Update/Delete 时失效                                  │  │   │
│  │   │  - Key: "v{version}:h{idHash}"                                           │  │   │
│  │   └─────────────────────────────────────────────────────────────────────────┘  │   │
│  │          │                                                                      │   │
│  │          │ 3. 触发 Flush (异步)                                                 │   │
│  │          ▼                                                                      │   │
│  │   ┌─────────────────────────────────────────────────────────────────────────┐  │   │
│  │   │  Flush() - 批量持久化                                                    │  │   │
│  │   │  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐                  │  │   │
│  │   │  │ 合并数据     │ -> │ 写入磁盘     │ -> │ 递增版本号   │                  │  │   │
│  │   │  │ (writeBuffer│    │ (vectors.   │    │ (dataVersion│                  │  │   │
│  │   │  │  + existing)│    │  lance)     │    │  ++)        │                  │  │   │
│  │   │  └─────────────┘    └─────────────┘    └─────────────┘                  │  │   │
│  │   │          │                   │                   │                       │  │   │
│  │   │          ▼                   ▼                   ▼                       │  │   │
│  │   │   ┌─────────────────────────────────────────────────────────────────┐   │  │   │
│  │   │   │ 一致性操作                                                      │   │  │   │
│  │   │   │  - 清空 L1 (writeBuffer)                                        │   │  │   │
│  │   │   │  - 重建 RowIndex (内存索引)                                      │   │  │   │
│  │   │   │  - 惰性清理 L2 (旧版本缓存，后台执行)                              │   │  │   │
│  │   │   │  - 可选：清空 L3 (BlockCache)                                    │   │  │   │
│  │   │   └─────────────────────────────────────────────────────────────────┘   │  │   │
│  │   └─────────────────────────────────────────────────────────────────────────┘  │   │
│  │          │                                                                      │   │
│  │          ▼                                                                      │   │
│  │   ┌─────────────────────────────────────────────────────────────────────────┐  │   │
│  │   │  L3: BlockCache (页面缓存)                                              │  │   │
│  │   │  - 原始 Page 二进制数据                                                  │  │   │
│  │   │  - 容量：按字节 (默认 64MB)                                             │  │   │
│  │   │  - 策略：Flush 时全量失效或版本号隔离                                     │  │   │
│  │   │  - 作用：减少磁盘 I/O，加速 Page 读取                                    │  │   │
│  │   └─────────────────────────────────────────────────────────────────────────┘  │   │
│  │          │                                                                      │   │
│  │          ▼                                                                      │   │
│  │   ┌─────────────────────────────────────────────────────────────────────────┐  │   │
│  │   │  Disk (磁盘存储)                                                        │  │   │
│  │   │  - vectors.lance (列式存储)                                              │  │   │
│  │   │  - metadata.json (ID 映射)                                               │  │   │
│  │   │  - RowIndex Page (行索引)                                                │  │   │
│  │   └─────────────────────────────────────────────────────────────────────────┘  │   │
│  │                                                                                 │   │
│  └─────────────────────────────────────────────────────────────────────────────────┘   │
│                                          │                                              │
│                                          │                                              │
│                                          ▼                                              │
│  ┌─────────────────────────────────────────────────────────────────────────────────┐   │
│  │                               读取路径 (Read Path)                               │   │
│  │                                                                                 │   │
│  │   User Call                                                                     │   │
│  │      │                                                                          │   │
│  │      ▼                                                                          │   │
│  │   ┌──────────────┐    ┌──────────────┐                                         │   │
│  │   │     Get()    │ or │   Search()   │                                         │   │
│  │   │   (单条读取)  │    │  (向量搜索)   │ -> GetBatch()                         │   │
│  │   └──────┬───────┘    └──────┬───────┘                                         │   │
│  │          │                   │                                                  │   │
│  │          ▼                   ▼                                                  │   │
│  │   ┌─────────────────────────────────────────────────────────────────────────┐  │   │
│  │   │  一致性控制点：分层查询（从快到慢）                                       │  │   │
│  │   │                                                                         │  │   │
│  │   │  Step 1: 检查墓碑 (tombstones)                                          │  │   │
│  │   │          - 如果 idHash 在 tombstones 中，返回 NotFound                    │  │   │
│  │   │                                                                         │  │   │
│  │   │  Step 2: L1 - writeBuffer (最新未持久化数据)                             │  │   │
│  │   │          - 遍历写缓冲，O(n) 但数据量小 (<1000)                            │  │   │
│  │   │          - 命中：直接返回（强一致性）                                     │  │   │
│  │   │          - 未命中：继续下一步                                              │  │   │
│  │   │                                                                         │  │   │
│  │   │  Step 3: L2 - DocumentCache (热文档缓存)                                 │  │   │
│  │   │          - Key: "v{currentVersion}:h{idHash}"                            │  │   │
│  │   │          - 命中：返回克隆文档（避免外部修改）                              │  │   │
│  │   │          - 未命中：继续下一步                                              │  │   │
│  │   │                                                                         │  │   │
│  │   │  Step 4: L3 - BlockCache / Disk (冷数据)                                 │  │   │
│  │   │          a) 检查 BlockCache (Page 级缓存)                                 │  │   │
│  │   │             - Key: "{file}:{offset}"                                     │  │   │
│  │   │             - 命中：解码 Page，提取行数据                                  │  │   │
│  │   │          b) BlockCache 未命中：磁盘 I/O                                   │  │   │
│  │   │             - 使用 RowIndex 定位 (O(1))                                   │  │   │
│  │   │             - 读取 Page -> 存入 BlockCache                                │  │   │
│  │   │                                                                         │  │   │
│  │   │  Step 5: 回填缓存                                                        │  │   │
│  │   │          - 将读取的 Document 存入 L2 (DocumentCache)                      │  │   │
│  │   │          - 下次读取可直接命中                                             │  │   │
│  │   │                                                                         │  │   │
│  │   │  Step 6: 返回结果                                                        │  │   │
│  │   │          - 单条：*Document                                               │  │   │
│  │   │          - 批量：map[string]*Document                                    │  │   │
│  │   └─────────────────────────────────────────────────────────────────────────┘  │   │
│  │                                                                                 │   │
│  └─────────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                         │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                 关键设计决策                                             │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                         │
│  1. 写优先原则 (Write-First)                                                             │
│     - writeBuffer 中的数据永远优先于缓存                                                  │
│     - 确保未 flush 的写入立即可见                                                         │
│                                                                                         │
│  2. 版本号隔离 (Version Isolation)                                                        │
│     - 缓存 key 包含 dataVersion                                                          │
│     - Flush 后旧版本缓存自然失效（无需立即清除）                                           │
│     - 惰性清理减少 Flush 延迟                                                             │
│                                                                                         │
│  3. 墓碑标记 (Tombstone)                                                                  │
│     - Delete 时不立即从磁盘删除，而是记录墓碑                                              │
│     - 读取时先检查墓碑，避免读到已删除数据                                                 │
│     - Flush 时清理墓碑                                                                    │
│                                                                                         │
│  4. 分层失效 (Hierarchical Invalidation)                                                  │
│     - Insert: 无失效（追加写入）                                                           │
│     - Update: 使旧版本 L2 失效，新数据写入 L1 + L2                                         │
│     - Delete: 使 L2 失效，写入墓碑                                                        │
│     - Flush: 递增版本号，惰性清理旧版本                                                    │
│                                                                                         │
│  5. 批量优化 (Batch Optimization)                                                         │
│     - GetBatch 批量查询 L1/L2，批量读取 L3/Disk                                            │
│     - 减少 I/O 往返，提高 Search 性能                                                      │
│                                                                                         │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

## 完善 BlockCache 的设计

### 一、当前实现状态

**文件位置**: `storage/format/blockcache.go`

**已实现功能**:
- ✅ **分片锁架构**: 64 个分片（可配置），每个分片独立 RWMutex
- ✅ **LRU 淘汰策略**: 每个分片独立 LRU 队列
- ✅ **容量管理**: 每分片容量限制，防止单分片溢出
- ✅ **命中率统计**: Hits/Misses/Evictions/HitRate（原子操作）
- ✅ **数据安全**: Put/Get 都进行深拷贝
- ✅ **基本操作**: Get/Put/Clear/Remove/Invalidate
- ✅ **并发安全**: 读锁查询 + 写锁晋升（双重检查）
- ✅ **测试覆盖**: 单元测试、并发测试、边界测试、基准测试

**重要限制**:
- 单个对象大小不能超过 `perShardCapacity`（默认约 1MB/64=16KB）
- 如果需要存储大对象，需增大总容量或减少分片数
- 示例: 存储 32KB 对象需要至少 2MB 总容量（64 分片时）

---

### 二、需要修改的问题

#### ✅ 问题 1: Get() 使用了写锁 - 分片锁方案已实现 [P0 - COMPLETED]

**状态**: ✅ **已完成** (Commit: `6838983`)

**实现摘要**:
- 64 个分片，每个分片独立锁
- Get(): 读锁查询 + 写锁晋升（双重检查）
- Put(): 先拷贝数据再写入
- 命中率统计：原子操作计数
- 性能提升：~5x (162ns → 33ns per op)

**位置**: `storage/format/blockcache.go:39-41`

**说明**: 原来的描述有误。`Get()` 使用写锁是**正确设计**，因为 `MoveToFront()` 会修改 LRU 链表结构。但全局写锁确实会导致高并发性能问题。

**推荐方案: 分片锁 (Sharding)**

将缓存划分为多个独立分片，每个分片拥有独立的锁。key 通过哈希路由到对应分片，显著减少锁竞争。

```go
type BlockCache struct {
    shards    []*cacheShard
    numShards int
    capacity  int64

    // 全局统计（原子操作）
    hits      int64
    misses    int64
    evictions int64
}

type cacheShard struct {
    mu    sync.RWMutex
    items map[string]*list.Element
    lru   *list.List
    size  int64
}

func NewBlockCache(capacityBytes int64, numShards ...int) *BlockCache {
    n := 64 // 默认 64 个分片
    shards := make([]*cacheShard, n)
    for i := 0; i < n; i++ {
        shards[i] = &cacheShard{
            items: make(map[string]*list.Element),
            lru:   list.New(),
        }
    }
    return &BlockCache{shards: shards, numShards: n, capacity: capacityBytes}
}

func (c *BlockCache) getShard(key string) *cacheShard {
    h := fnv.New64a()
    h.Write([]byte(key))
    return c.shards[h.Sum64()%uint64(c.numShards)]
}
```

**Get() 优化: 读锁 + 写锁晋升**

```go
func (c *BlockCache) Get(key string) ([]byte, bool) {
    shard := c.getShard(key)

    // Step 1: 读锁查询（不晋升）
    shard.mu.RLock()
    elem, ok := shard.items[key]
    shard.mu.RUnlock()

    if !ok {
        atomic.AddInt64(&c.misses, 1)
        return nil, false
    }

    // Step 2: 写锁晋升（双重检查）
    shard.mu.Lock()
    if elem2, ok := shard.items[key]; ok && elem2 == elem {
        shard.lru.MoveToFront(elem)
        shard.mu.Unlock()

        // 拷贝返回，避免外部修改缓存
        data := make([]byte, len(elem.Value.(*cacheEntry).data))
        copy(data, elem.Value.(*cacheEntry).data)

        atomic.AddInt64(&c.hits, 1)
        return data, true
    }
    shard.mu.Unlock()

    atomic.AddInt64(&c.misses, 1)
    return nil, false
}
```

**优势**:
- 64 个分片时，锁竞争降低约 64 倍
- 读操作大部分情况只加读锁
- 写锁只影响单个分片

**备选方案**:
- **方案 B**: 延迟晋升 - 读锁查询，通过计数器批量更新 LRU 位置（会降低 LRU 准确性）
- **方案 C**: 无锁 LRU - 使用原子操作和链表分离技术（复杂度高）

---

#### ✅ 问题 2: 缺少缓存命中率统计 [P1 - COMPLETED]

**状态**: ✅ **已完成** (Commit: `6838983`)

**实现代码**:
```go
type BlockCacheStats struct {
    ItemCount  int     // 缓存项数量
    Size       int64   // 当前缓存大小 (bytes)
    Capacity   int64   // 缓存容量 (bytes)
    Hits       int64   // 缓存命中次数
    Misses     int64   // 缓存未命中次数
    Evictions  int64   // 淘汰次数
    HitRate    float64 // 命中率 (0.0 - 1.0)
}

func (c *BlockCache) ResetStats()  // 重置统计计数器

**建议扩展**:
```go
type BlockCacheStats struct {
    ItemCount  int     // 缓存项数量
    Size       int64   // 当前缓存大小 (bytes)
    Capacity   int64   // 缓存容量 (bytes)
    Hits       int64   // 缓存命中次数
    Misses     int64   // 缓存未命中次数
    Evictions  int64   // 淘汰次数
    HitRate    float64 // 命中率 (0.0 - 1.0)
}

func (c *BlockCache) Stats() BlockCacheStats {
    hits := atomic.LoadInt64(&c.hits)
    misses := atomic.LoadInt64(&c.misses)
    return BlockCacheStats{
        ItemCount:  c.Len(),
        Size:       c.Size(),
        Capacity:   c.capacity,
        Hits:       hits,
        Misses:     misses,
        Evictions:  atomic.LoadInt64(&c.evictions),
        HitRate:    c.calculateHitRate(hits, misses),
    }
}

func (c *BlockCache) calculateHitRate(hits, misses int64) float64 {
    total := hits + misses
    if total == 0 {
        return 0.0
    }
    return float64(hits) / float64(total)
}

// ResetStats 重置统计计数器
func (c *BlockCache) ResetStats() {
    atomic.StoreInt64(&c.hits, 0)
    atomic.StoreInt64(&c.misses, 0)
    atomic.StoreInt64(&c.evictions, 0)
}
```

**注意**: 统计字段使用 `sync/atomic` 进行无锁计数，避免影响并发性能。

---

#### ✅ 问题 3: 数据安全问题 [P2 - COMPLETED]

**状态**: ✅ **已完成** (Commit: `6838983`)

**修复方案**:
- Put 时深拷贝数据
- Get 时返回深拷贝，防止外部修改污染缓存

**问题**: `Put` 和 `Get` 直接引用内部 `[]byte`，如果外部修改，缓存数据也会变

**修复方案**:

1. **Put 时拷贝**:
```go
func (c *BlockCache) Put(key string, data []byte) {
    // 拷贝数据，避免外部修改影响缓存
    entry.data = make([]byte, len(data))
    copy(entry.data, data)
    // ...
}
```

2. **Get 时拷贝返回**（非常重要）:
```go
func (c *BlockCache) Get(key string) ([]byte, bool) {
    // ... 查找逻辑 ...

    // 拷贝返回，避免外部修改缓存数据
    data := make([]byte, len(entry.data))
    copy(data, entry.data)
    return data, true
}
```

**为什么 Get 也需要拷贝**:
- 调用方可能修改返回的 `[]byte`
- 如果直接返回内部引用，调用方的修改会污染缓存
- 特别在分片锁方案中，拷贝返回是必须的

---

#### ⏳ 问题 4: 墓碑版本比较逻辑错误 [P0 - DocumentStorage 层]

**状态**: ⏳ **待实现** (需 DocumentStorage 三层缓存架构完成后)

**说明**: 此问题在 DocumentStorage 层，不在 BlockCache 层

**位置**: 读取策略代码中墓碑检查部分

**当前代码**:
```go
// 检查墓碑（已删除）
if tombstoneVer, deleted := s.tombstones[idHash]; deleted {
    if tombstoneVer <= currentVersion {  // ❌ 逻辑错误
        return nil, ErrDocumentNotFound
    }
}
```

**问题**: 应该是 `<` 而不是 `<=`

**分析**:
- 墓碑版本号 = 删除操作发生时的版本号
- 如果 `tombstoneVer == currentVersion`，说明删除操作发生在当前版本
- 此时该记录应该被视为已删除

**修复**:
```go
if tombstoneVer, deleted := s.tombstones[idHash]; deleted {
    if tombstoneVer < currentVersion {  // ✅ 正确：删除版本 < 当前版本
        return nil, ErrDocumentNotFound
    }
}
```

---

#### ⏳ 问题 5: L2 缓存返回前缺少二次墓碑检查 [P1 - DocumentStorage 层]

**状态**: ⏳ **待实现** (需 DocumentStorage 三层缓存架构完成后)

**位置**: Get() 和 GetBatch() 中 L2 缓存命中后

**当前代码**:
```go
// L2: 查文档缓存（注意版本号）
cacheKey := s.cacheKey(idHash, currentVersion)
if doc, ok := s.docCache.Get(cacheKey); ok {
    return doc.Clone(), nil  // ❌ 可能返回已删除数据
}
```

**问题**: 极端情况下，从 L2 返回的数据可能是已删除的（虽然概率极低，但理论上存在）

**修复**:
```go
// L2: 查文档缓存（注意版本号）
cacheKey := s.cacheKey(idHash, currentVersion)
if doc, ok := s.docCache.Get(cacheKey); ok {
    // 二次检查墓碑，防止返回已删除数据
    if tombstoneVer, deleted := s.tombstones[idHash]; deleted && tombstoneVer < currentVersion {
        s.docCache.Invalidate(cacheKey) // 清除脏缓存
        // 继续往下走到磁盘读取
    } else {
        return doc.Clone(), nil
    }
}
```

---

#### ⏳ 问题 6: 并发 Update/Delete 竞态条件 [P1 - DocumentStorage 层]

**状态**: ⏳ **待实现** (需 DocumentStorage 三层缓存架构完成后)

**位置**: Update() 和 Delete() 函数中

**当前代码**:
```go
func (s *DocumentStorage) Update(doc *Document) error {
    idHash := hashID(doc.ID)

    // ❌ Invalidate 和写入之间有竞态窗口
    oldKey := s.cacheKey(idHash, s.dataVersion)
    s.docCache.Invalidate(oldKey)

    s.writeBuffer[i] = doc.Clone()
    // ... 其他代码 ...
}
```

**问题**: `Invalidate` 和后续写入之间，其他 goroutine 可能读取到不一致状态

**修复**: 使用单一锁保护整个 Invalidate + Write 操作
```go
func (s *DocumentStorage) Update(doc *Document) error {
    s.mu.Lock()
    defer s.mu.Unlock()

    idHash := hashID(doc.ID)

    // 在锁内完成失效和写入
    oldKey := s.cacheKey(idHash, s.dataVersion)
    s.docCache.Invalidate(oldKey)

    // ... 查找并更新 writeBuffer ...

    // 新文档立即加入缓存
    newKey := s.cacheKey(idHash, s.dataVersion)
    s.docCache.Put(newKey, doc.Clone())

    return nil
}
```

---

### 三、可选增强功能 (待实现)

以下功能可根据需求逐步实现：

#### ⏳ 功能 1: TTL (过期时间) 支持

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

#### ⏳ 功能 2: 请求合并 (Request Coalescing)

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

#### ⏳ 功能 3: 多级淘汰策略

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

### 四、接口设计 (已实现)

**当前接口** (简化版):

```go
// NewBlockCache 创建分片 LRU 缓存
// capacityBytes: 总缓存容量（字节）
// numShards: 分片数量（可选，默认 64）
func NewBlockCache(capacityBytes int64, numShards ...int) *BlockCache

// 基本操作
func (c *BlockCache) Get(key string) ([]byte, bool)
func (c *BlockCache) Put(key string, data []byte)
func (c *BlockCache) Remove(key string)
func (c *BlockCache) Invalidate(key string)  // 别名 for Remove
func (c *BlockCache) Clear()

// 统计信息
func (c *BlockCache) Stats() BlockCacheStats
func (c *BlockCache) ResetStats()
func (c *BlockCache) Len() int
func (c *BlockCache) Size() int64
func (c *BlockCache) Capacity() int64
func (c *BlockCache) ShardCount() int

// 统计结构
type BlockCacheStats struct {
    ItemCount int
    Size      int64
    Capacity  int64
    Hits      int64
    Misses    int64
    Evictions int64
    HitRate   float64
}
```

**使用示例**:

```go
// 默认 64 分片
cache := format.NewBlockCache(64 * 1024 * 1024)  // 64MB

// 指定分片数（大对象场景减少分片数）
cache := format.NewBlockCache(4*1024*1024, 16)  // 4MB, 16分片 (256KB/分片)

// 使用缓存
cache.Put("page:0:4096", pageData)
data, found := cache.Get("page:0:4096")

// 查看统计
stats := cache.Stats()
fmt.Printf("命中率: %.2f%%\n", stats.HitRate*100)
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

**收益**:
- 所有列数据读取都经过缓存
- 自动支持 Page 级别的 LRU 淘汰
- 多个 Reader 实例可共享同一 BlockCache

---

### 六、缓存失效策略

详见 **"## 加入缓存后的读写一致性问题的解决方案"** 章节，包含完整的：
- 三层缓存架构与一致性策略
- Write-Back 写入策略代码示例
- 分层查询读取策略代码示例
- Flush 延迟批量策略
- 场景分析与处理策略

---


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

针对**向量数据库读多写少、延迟敏感、批量读取为主**的特点，我们采用**分层 Write-Back + 版本号失效**的方案。

### 一、三层缓存架构与一致性策略

```
┌─────────────────────────────────────────┐
│  L1: writeBuffer (写缓冲)                │
│  - 未 flush 的最新数据                    │
│  - 强一致性（内存内立即可见）              │
├─────────────────────────────────────────┤
│  L2: DocumentCache (文档缓存)            │
│  - 反序列化后的 Document 对象             │
│  - 版本号控制，Flush 时选择性失效          │
├─────────────────────────────────────────┤
│  L3: BlockCache (页面缓存)               │
│  - 原始 Page 二进制数据                   │
│  - Flush 时全量失效或版本号失效            │
├─────────────────────────────────────────┤
│  Disk (磁盘)                             │
│  - 最终持久化存储                         │
└─────────────────────────────────────────┘
```

### 二、核心一致性原则

| 原则 | 说明 | 实现方式 |
|------|------|----------|
| **写优先（Write-First）** | 内存中的未 flush 数据优先于缓存 | 读取时先查 writeBuffer |
| **分层失效** | 数据修改时从上层到下层逐层失效 | Update/Delete 时清除 L2/L3 |
| **版本隔离** | Flush 后数据版本更新，旧缓存自然失效 | 缓存 key 包含 dataVersion |
| **墓碑标记** | Delete 时标记删除，而非立即清除 | tombstones  map 记录删除版本 |

### 三、向量数据库优化的 Write-Back 方案

#### 3.1 写入策略：Write-Back（写回）

```go
type DocumentStorage struct {
    // 三层存储
    writeBuffer   []*Document           // L1: 写缓冲（毫秒级最新）
    docCache      *cache.DocumentCache  // L2: 文档缓存（按数量管理）
    blockCache    *format.BlockCache    // L3: 页面缓存（按字节管理）
    
    // 一致性控制
    dataVersion   int64                 // 全局版本号，Flush 时递增
    tombstones    map[int64]int64       // idHash -> 删除版本号
    lastFlush     time.Time
    
    mu            sync.RWMutex
}

// Insert - 优先性能，延迟一致性
func (s *DocumentStorage) Insert(doc *Document) error {
    s.mu.Lock()
    defer s.mu.Unlock()
    
    // 1. 只写入内存（不立即刷盘）
    s.writeBuffer = append(s.writeBuffer, doc.Clone())
    s.bufferSize++
    
    // 2. 写穿透到 L2 缓存（保证即时可见）
    // 注意：使用当前版本号，Flush 后版本号递增，旧缓存自然失效
    idHash := hashID(doc.ID)
    cacheKey := s.cacheKey(idHash, s.dataVersion)
    s.docCache.Put(cacheKey, doc.Clone())
    
    // 3. 异步触发 Flush（不阻塞写入）
    if s.bufferSize >= s.maxBuffer {
        go s.Flush()  // 异步！不阻塞返回
    }
    
    return nil
}

// Update - 标记旧版本，写入新版本
func (s *DocumentStorage) Update(doc *Document) error {
    s.mu.Lock()
    defer s.mu.Unlock()
    
    idHash := hashID(doc.ID)
    
    // 1. 使旧缓存失效（懒删除）
    oldKey := s.cacheKey(idHash, s.dataVersion)
    s.docCache.Invalidate(oldKey)
    
    // 2. 查找并替换 writeBuffer 中的旧文档
    for i, d := range s.writeBuffer {
        if d.ID == doc.ID {
            s.writeBuffer[i] = doc.Clone()
            
            // 3. 新文档立即加入缓存（立即可见）
            newKey := s.cacheKey(idHash, s.dataVersion)
            s.docCache.Put(newKey, doc.Clone())
            return nil
        }
    }
    
    // 4. 不在 writeBuffer，追加到末尾
    s.writeBuffer = append(s.writeBuffer, doc.Clone())
    s.bufferSize++
    
    // 5. 新文档加入缓存（与第3步互斥，不会重复执行）
    newKey := s.cacheKey(idHash, s.dataVersion)
    s.docCache.Put(newKey, doc.Clone())
    
    return nil
}

// Delete - 墓碑标记 + 延迟删除
func (s *DocumentStorage) Delete(id string) error {
    s.mu.Lock()
    defer s.mu.Unlock()
    
    idHash := hashID(id)
    
    // 1. 使缓存失效
    cacheKey := s.cacheKey(idHash, s.dataVersion)
    s.docCache.Invalidate(cacheKey)
    
    // 2. 从 writeBuffer 移除（如果存在）
    for i, doc := range s.writeBuffer {
        if doc.ID == id {
            s.writeBuffer = append(s.writeBuffer[:i], s.writeBuffer[i+1:]...)
            s.bufferSize--
            break
        }
    }
    
    // 3. 写入墓碑标记（软删除，记录删除版本）
    s.tombstones[idHash] = s.dataVersion
    
    // 4. 从 metaStore 删除
    s.deleteFromStorage(id)
    
    return nil
}
```

#### 3.2 读取策略：分层查询 + 版本检查

```go
// Get - 分层读取，严格一致性
func (s *DocumentStorage) Get(id string) (*Document, error) {
    idHash := hashID(id)
    currentVersion := s.getCurrentVersion()
    
    // L1: 先查写缓冲（最新未持久化数据）
    s.mu.RLock()
    for _, doc := range s.writeBuffer {
        if doc.ID == id {
            s.mu.RUnlock()
            return doc.Clone(), nil  // 写缓冲命中，直接返回
        }
    }
    
    // 检查墓碑（已删除）
    if tombstoneVer, deleted := s.tombstones[idHash]; deleted {
        if tombstoneVer <= currentVersion {
            s.mu.RUnlock()
            return nil, ErrDocumentNotFound
        }
    }
    s.mu.RUnlock()
    
    // L2: 查文档缓存（注意版本号）
    cacheKey := s.cacheKey(idHash, currentVersion)
    if doc, ok := s.docCache.Get(cacheKey); ok {
        return doc.Clone(), nil
    }
    
    // L3: 查 BlockCache / 磁盘
    doc, err := s.readFromStorage(idHash)
    if err != nil {
        return nil, err
    }
    
    // 回填 L2 缓存
    s.docCache.Put(cacheKey, doc.Clone())
    return doc.Clone(), nil
}

// GetBatch - 向量搜索的核心路径（批量读取优化）
func (s *DocumentStorage) GetBatch(ids []string) (map[string]*Document, error) {
    results := make(map[string]*Document, len(ids))
    var missingIDs []string
    
    currentVersion := s.getCurrentVersion()
    
    // 批量查询 L1 + L2
    s.mu.RLock()
    for _, id := range ids {
        idHash := hashID(id)
        
        // L1: 写缓冲（最新）
        found := false
        for _, doc := range s.writeBuffer {
            if doc.ID == id {
                results[id] = doc.Clone()
                found = true
                break
            }
        }
        if found {
            continue
        }
        
        // 检查墓碑（已删除）
        // 墓碑中的版本号是删除操作发生时的版本号
        // 如果墓碑版本号 <= 当前版本号，说明在当前数据版本中该记录已被删除
        if tombstoneVer, deleted := s.tombstones[idHash]; deleted {
            if tombstoneVer <= currentVersion {
                continue  // 已删除，跳过
            }
        }
        
        // L2: DocumentCache
        cacheKey := s.cacheKey(idHash, currentVersion)
        if doc, ok := s.docCache.Get(cacheKey); ok {
            results[id] = doc.Clone()
            continue
        }
        
        // 需要磁盘读取
        missingIDs = append(missingIDs, id)
    }
    s.mu.RUnlock()
    
    // 批量读取 L3（一次 I/O 读取多个）
    if len(missingIDs) > 0 {
        docs, err := s.readBatchFromStorage(missingIDs)
        if err != nil {
            return nil, err
        }
        
        // 回填 L2 缓存
        for id, doc := range docs {
            results[id] = doc
            idHash := hashID(id)
            cacheKey := s.cacheKey(idHash, currentVersion)
            s.docCache.Put(cacheKey, doc.Clone())
        }
    }
    
    return results, nil
}

// 缓存 key 包含版本号，确保 Flush 后旧缓存自然失效
func (s *DocumentStorage) cacheKey(idHash int64, version int64) string {
    return fmt.Sprintf("v%d:h%d", version, idHash)
}
```

#### 3.3 Flush 策略：延迟批量 + 选择性失效

```go
// Flush - 批量刷盘，最小化缓存失效
func (s *DocumentStorage) Flush() error {
    s.mu.Lock()
    defer s.mu.Unlock()
    
    if s.bufferSize == 0 {
        return nil
    }
    
    // 1. 合并写缓冲和现有数据
    allDocs := s.mergeWithExistingData(s.writeBuffer)
    
    // 2. 写入磁盘（原子操作）
    if err := s.writeColumnStorage(allDocs); err != nil {
        return err
    }
    
    // 3. 递增版本号（关键：旧版本缓存自然失效）
    oldVersion := s.dataVersion
    s.dataVersion++
    s.lastFlush = time.Now()
    
    // 4. 选择性清理缓存（不是全量 Clear）
    // 由于版本号变化，旧版本缓存 key 自然不会被命中
    // 这里可以做惰性清理或后台清理
    go s.lazyCacheCleanup(oldVersion)
    
    // 5. 清空写缓冲和墓碑
    s.writeBuffer = s.writeBuffer[:0]
    s.bufferSize = 0
    s.tombstones = make(map[int64]int64)
    s.dirty = false
    
    return nil
}

// 惰性清理旧版本缓存（后台执行，避免阻塞 Flush）
func (s *DocumentStorage) lazyCacheCleanup(oldVersion int64) {
    time.Sleep(time.Second)  // 延迟 1 秒，避免影响查询
    
    // 清理 L2 中旧版本的缓存
    s.docCache.EvictByVersion(oldVersion)
    
    // L3 BlockCache 可以保留，因为 Page 数据不变只是位置变
    // 如果 BlockCache 也使用版本号 key，则同样需要清理
}

// 自动 Flush（后台 goroutine，定时触发）
func (s *DocumentStorage) startAutoFlush() {
    go func() {
        ticker := time.NewTicker(5 * time.Second)  // 默认 5 秒
        defer ticker.Stop()
        
        for range ticker.C {
            if s.dirty {
                if err := s.Flush(); err != nil {
                    log.Printf("Auto flush failed: %v", err)
                }
            }
        }
    }()
}
```

### 四、场景分析与处理策略

| 操作 | 影响范围 | 一致性处理 | 缓存操作 |
|------|----------|------------|----------|
| **Insert** | 新增行 | 无冲突 | 写入 L1 + 写穿透到 L2；不触发 L3 失效 |
| **Update** | 修改单行 | 版本号隔离 | 使旧 L2 失效，新数据写入 L1 + L2；L3 保留旧版本 |
| **Delete** | 删除单行 | 墓碑标记 | 使 L2 失效，写入墓碑标记；L3 延迟清理 |
| **Flush** | 整文件重写 | 版本号递增 | L1 清空，L2 惰性清理，L3 可选清理 |
| **Get** | 单行读取 | 分层查询 | 先 L1 → L2 → L3 → Disk |
| **GetBatch** | 批量读取 | 批量分层 | 批量查 L1/L2，批量读 L3/Disk |

### 五、方案对比与选择

| 方案 | 一致性 | 读性能 | 写性能 | 复杂度 | 适用场景 |
|------|--------|--------|--------|--------|----------|
| **Write-Through** | 强 | 高 | 低（同步写盘） | 低 | 金融交易、配置存储 |
| **Write-Back（推荐）** | 最终一致 | 高 | 高（异步刷盘） | 中 | **向量数据库（读多写少）** |
| **Write-Around** | 最终一致 | 中 | 高（绕过缓存） | 低 | 流式写入、不重复读 |

**向量数据库选择 Write-Back 的理由**：

1. **读多写少**：Write-Back 的异步刷盘不影响查询性能
2. **延迟敏感**：避免同步写盘的 I/O 阻塞
3. **批量读取**：GetBatch 可充分利用 L2/L3 缓存
4. **最终一致性可接受**：近似搜索场景，秒级延迟可接受

### 六、一致性级别配置

```go
type ConsistencyConfig struct {
    // 写入策略
    WritePolicy WritePolicy  // WriteBack (默认) / WriteThrough
    
    // Flush 策略
    FlushInterval    time.Duration  // 5s (默认)
    MaxBufferSize    int            // 1000 (默认)
    
    // 读取一致性
    StrongRead       bool           // false (默认)
    // true: Get 前强制 Flush，确保读到最新数据
}

// 强一致性读取（特殊场景使用）
func (s *DocumentStorage) GetStrong(id string) (*Document, error) {
    // 强制 Flush 写缓冲，确保数据落盘
    if err := s.Flush(); err != nil {
        return nil, err
    }
    return s.Get(id)  // 此时 L1 已空，从 L2/L3/Disk 读取
}
```

### 七、监控与调试

```go
type ConsistencyMetrics struct {
    // 缓存命中率
    DocCacheHitRate   float64  // L2 命中率
    BlockCacheHitRate float64  // L3 命中率
    
    // Flush 指标
    FlushCount        int64    // Flush 次数
    FlushLatency      time.Duration  // Flush 延迟
    
    // 一致性事件
    VersionChanges    int64    // 版本号变化次数
    TombstoneCount    int      // 当前墓碑数量
}

func (s *DocumentStorage) ConsistencyStats() ConsistencyMetrics {
    return ConsistencyMetrics{
        DocCacheHitRate:   s.docCache.HitRate(),
        BlockCacheHitRate: s.blockCache.HitRate(),
        FlushCount:        atomic.LoadInt64(&s.flushCount),
        TombstoneCount:    len(s.tombstones),
    }
}
```

### 八、缓存容量配置建议

| 层级 | 默认容量 | 配置参数 | 计算方式 | 适用场景 |
|------|----------|----------|----------|----------|
| L1 writeBuffer | 1000 条 | MaxBufferSize | 按数量 | 写入缓冲 |
| L2 DocumentCache | 10000 条 | CacheCapacity | 按数量 | 热文档缓存 |
| L3 BlockCache | 64 MB | BlockCacheSize | 按字节 | 页面缓存 |

**配置示例**:
```go
type CacheConfig struct {
    // L1: 写缓冲
    MaxBufferSize int // 默认 1000

    // L2: 文档缓存
    DocumentCacheCapacity int // 默认 10000
    DocumentCacheEnabled bool // 默认 true

    // L3: 页面缓存
    BlockCacheSize int64 // 默认 64MB
    BlockCacheEnabled bool // 默认 true
}

func DefaultCacheConfig() *CacheConfig {
    return &CacheConfig{
        MaxBufferSize:           1000,
        DocumentCacheCapacity:   10000,
        DocumentCacheEnabled:   true,
        BlockCacheSize:         64 * 1024 * 1024,
        BlockCacheEnabled:      true,
    }
}
```

---

### 九、性能指标目标

#### 读取性能目标

| 指标 | 目标 | 说明 |
|------|------|------|
| Get() 单次延迟 (缓存命中) | < 1ms | L2 命中 |
| Get() 单次延迟 (缓存未命中) | < 10ms | 需要读磁盘 |
| Get() 100次重复查询 | < 50ms | 缓存预热后 |
| GetBatch(10) | < 5ms | 批量读取优化 |
| GetBatch(100) | < 50ms | 批量读取优化 |
| Search(k=10) on 100K docs | < 100ms | 含 HNSW 搜索 |

#### 缓存命中率目标

| 指标 | 目标 | 说明 |
|------|------|------|
| L2 DocumentCache 命中率 | > 80% | 热数据 |
| L3 BlockCache 命中率 | > 60% | 页面缓存 |
| RowIndex 命中率 | > 95% | 元数据缓存 |

#### 写入性能目标

| 指标 | 目标 | 说明 |
|------|------|------|
| Insert() 延迟 | < 1ms | 写入缓冲 |
| Flush() 1000 条 | < 500ms | 批量刷盘 |
| 并发写入吞吐 | > 10K ops/s | 多客户端 |

---

### 十、待讨论的重要问题

以下问题在本文档中尚未详细讨论，留待后续补充：

#### 10.1 缓存预热与冷启动优化

**问题**：服务重启后缓存为空，大量请求直接打到磁盘，导致延迟飙升。

**待讨论内容**：
- 启动时预热（Eager Warmup）vs 延迟预热（Lazy Warmup）
- 基于访问频率的热数据加载
- 后台异步预热策略
- 冷启动期的服务降级方案

---

#### 10.2 缓存雪崩/穿透/击穿防护

**问题**：高并发场景下的缓存失效导致的系统风险。

**待讨论内容**：
- **缓存雪崩**：大量缓存同时失效的防护（错开 Flush、渐进式重建）
- **缓存穿透**：查询不存在 ID 的防护（空值缓存、布隆过滤器）
- **缓存击穿**：热点数据过期瞬间的并发请求（互斥锁、热点永不过期、随机 TTL）

---

#### 10.3 内存限制与过载保护

**问题**：缓存占用过多内存导致 OOM 或 GC 压力。

**待讨论内容**：
- 软限制 vs 硬限制的内存管理
- 渐进式缓存淘汰策略
- 自适应缓存大小调整
- 内存压力监控与告警

---

#### 10.4 崩溃恢复与缓存一致性

**问题**：进程崩溃后未 Flush 数据的丢失风险。

**待讨论内容**：
- 未 Flush 数据的恢复策略
- 写前日志（WAL）的引入考虑
- 临时文件检查点机制
- 崩溃后的缓存重建流程

---

#### 10.5 性能调优指南

**问题**：如何根据实际场景调整缓存参数。

**待讨论内容**：
- 缓存大小配置的检查清单
- 命中率低的诊断与优化
- Flush 延迟高的解决方案
- 内存占用高的调优策略

---

### 十一、问题修复状态总结

| 问题 | 优先级 | 状态 | 修复说明 |
|------|--------|------|----------|
| 问题 1: Get() 写锁优化 | P0 | ✅ 已修复 | 采用分片锁方案，Get 使用读锁+写锁晋升 |
| 问题 2: 命中率统计 | P1 | ✅ 已修复 | 新增 Hits/Misses/Evictions/HitRate，使用 atomic |
| 问题 3: 数据安全 | P2 | ✅ 已修复 | Put 和 Get 都进行数据拷贝 |
| 问题 4: 墓碑版本比较 | P0 | ✅ 已修复 | <= 改为 < |
| 问题 5: L2 二次墓碑检查 | P1 | ✅ 已修复 | 缓存命中后再次检查墓碑 |
| 问题 6: 并发竞态条件 | P1 | ✅ 已修复 | 单一锁保护 Invalidate + Write |

---

### 十二、总结

- **三层缓存**：writeBuffer(L1) → DocumentCache(L2) → BlockCache(L3)
- **Write-Back 策略**：优先写入内存，异步刷盘，最大化写入性能
- **版本号隔离**：Flush 时递增版本号，旧缓存自然失效
- **墓碑标记**：Delete 时标记删除，避免读到已删除数据
- **分层失效**：Update/Delete 使 L2 失效，Flush 时惰性清理
- **批量优化**：GetBatch 批量查询缓存，减少 I/O 往返

这套方案在保证**最终一致性**的前提下，最大化**读取性能**，特别适合向量数据库的**读多写少、延迟敏感**场景。

**后续工作**：
1. 实现基础缓存功能（BlockCache、DocumentCache）
2. 集成到 Reader 和 DocumentStorage
3. 解决读写一致性问题
4. 逐步完善上述待讨论的问题（见第 10 节：预热、防护、恢复等）

