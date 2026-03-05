# Vego Compaction 策略设计文档

本文档讨论 Vego 向量数据库中 Compaction（数据压缩/整理）的各种实现策略，分析各自的优缺点和适用场景。

## 背景

Compaction 的主要目的是：
1. **回收空间**：清理已删除的文档，减小存储文件大小
2. **重建索引**：清理 HNSW 中的孤儿节点，提高搜索效率
3. **整理碎片**：优化数据布局，提高读取性能

## 方案对比总览

| 方案 | 复杂度 | Compact 耗时 | 读取影响 | 写入影响 | 推荐场景 |
|------|--------|-------------|---------|---------|---------|
| [完全阻塞式](#方案1-完全阻塞式) | ⭐ | 长 | ❌ 阻塞 | ❌ 阻塞 | 离线批处理 |
| [轻量锁](#方案2-轻量锁) | ⭐⭐ | 长 | ✅ 不阻塞 | ❌ 阻塞 | 在线服务 |
| [增量式](#方案3-增量式-compaction) | ⭐⭐⭐ | 短（分批）| ✅ 不阻塞 | ⚠️ 轻微 | 渐进删除 |
| [后台双写](#方案4-后台异步--双写) | ⭐⭐⭐⭐ | 长（后台）| ✅ 不阻塞 | ⚠️ 双倍 | 零停机服务 |
| [分层存储](#方案5-分层-compaction) | ⭐⭐⭐⭐ | 短（分层）| ✅ 不阻塞 | ✅ 不阻塞 | 高写入场景 |
| [Storage优先](#方案6-只清理-storage) | ⭐⭐ | 中 | ✅ 不阻塞 | ⚠️ 轻微 | HNSW 重建成本高 |
| [时间窗口](#方案7-时间窗口-compaction) | ⭐⭐⭐ | 可调 | ✅ 不阻塞 | ⚠️ 轻微 | 时序数据 |
| [外部触发](#方案8-外部触发) | ⭐ | 长 | ❌ 阻塞 | ❌ 阻塞 | 关键业务 |
| [预分配](#方案9-空间换时间) | ⭐⭐ | 极短 | ✅ 不阻塞 | ✅ 不阻塞 | 数量稳定 |

---

## 方案详解

### 方案1: 完全阻塞式

最简单的实现方式，Compact 期间阻塞所有读写操作。

#### 实现原理

```go
func (c *Collection) Compact() error {
    c.mu.Lock()         // 获取独占锁
    defer c.mu.Unlock()
    
    // 1. 获取所有有效文档
    validDocs := c.storage.GetAllValidDocuments()
    
    // 2. 重建 HNSW 索引
    newIndex := hnsw.NewHNSW(config)
    for _, doc := range validDocs {
        nodeID := newIndex.Add(doc.Vector)
        // 更新映射...
    }
    
    // 3. 重写存储文件
    c.storage.Rewrite(validDocs)
    c.storage.ClearDeletionVector()
    
    // 4. 原子替换
    c.index = newIndex
    return nil
}
```

#### 优点
- 实现简单，易于理解和维护
- 数据一致性最强，无并发问题
- 资源占用可控（只有一个索引实例）

#### 缺点
- Compact 期间服务完全不可用
- 对于大集合，阻塞时间可能达数秒到数分钟
- 不适合在线服务

#### 适用场景
- 离线数据处理任务
- 开发测试环境
- 可接受维护窗口的业务

---

### 方案2: 轻量锁（读写分离）

使用读写锁（RWMutex）替代互斥锁，Compact 期间允许读取继续。

#### 实现原理

```go
type Collection struct {
    index    *hnsw.HNSWIndex
    rwmu     sync.RWMutex
}

func (c *Collection) Compact() error {
    // 1. 获取写锁，阻塞新写入
    c.rwmu.Lock()
    c.compacting = true
    c.rwmu.Unlock()
    
    // 2. 后台重建索引（读取继续使用旧索引）
    newIndex := hnsw.NewHNSW(config)
    newDocToNode := make(map[string]int)
    // ... 构建新索引 ...
    
    // 3. 原子切换（短暂阻塞）
    c.rwmu.Lock()
    c.index = newIndex
    c.docToNode = newDocToNode
    c.compacting = false
    c.rwmu.Unlock()
    
    return nil
}

func (c *Collection) Search(...) ([]SearchResult, error) {
    c.rwmu.RLock()          // 获取读锁，不阻塞其他读取
    defer c.rwmu.RUnlock()
    
    // Compact 期间也能读取（DV 会过滤已删除数据）
    return c.index.Search(...)
}
```

#### 优点
- 读取零阻塞，服务可用性高
- 实现相对简单，只需替换锁机制
- 读取性能无损耗

#### 缺点
- 写入仍被阻塞
- Compact 期间可能读到"即将被删除"的数据（但最终一致）
- 内存中同时存在两个索引（瞬时）

#### 适用场景
- 读多写少的在线服务
- 搜索、推荐等查询密集型应用
- 可接受短暂写入延迟的业务

---

### 方案3: 增量式 Compaction

不一次性处理所有数据，而是分批逐步清理。

#### 实现原理

```go
func (c *Collection) CompactIncremental(batchSize int) error {
    // 获取已删除的 rowID 列表
    deletedRows := c.storage.GetDeletedRows()
    
    // 只处理前 batchSize 个
    for i := 0; i < batchSize && i < len(deletedRows); i++ {
        rowID := deletedRows[i]
        
        // 只清理这一行，不移动其他数据
        // 可能需要维护"空洞列表"用于重用
        c.storage.MarkSlotFree(rowID)
    }
    
    // 如果还有剩余，返回特殊错误提示需要继续
    if len(deletedRows) > batchSize {
        return ErrCompactionPartial
    }
    return nil
}

// 周期性调用
func (c *Collection) BackgroundCompact() {
    ticker := time.NewTicker(1 * time.Minute)
    for range ticker.C {
        // 每次只处理 100 个
        c.CompactIncremental(100)
    }
}
```

#### 优点
- 每次工作量小，对性能影响轻微
- 可控制单次处理的数据量
- 适合渐进式删除场景

#### 缺点
- 实现复杂，需要维护碎片/空洞信息
- 整体清理周期较长
- 数据文件可能存在碎片

#### 适用场景
- 删除是渐进式的（如日志过期）
- 不能承受一次性大操作的业务
- 后台持续维护的场景

---

### 方案4: 后台异步 + 双写

Compact 在后台进行，期间新写入同时写入新旧两个索引。

#### 实现原理

```go
type Collection struct {
    index        *hnsw.HNSWIndex  // 当前索引
    newIndex     *hnsw.HNSWIndex  // 重建中的索引
    compacting   bool
    writeQueue   chan *Document   // Compact 期间的写入缓冲
}

func (c *Collection) StartCompact() {
    c.mu.Lock()
    c.compacting = true
    c.newIndex = hnsw.NewHNSW(config)
    c.mu.Unlock()
    
    // 后台迁移现有数据
    go c.migrateExistingData()
}

func (c *Collection) Insert(doc *Document) error {
    c.mu.Lock()
    defer c.mu.Unlock()
    
    if c.compacting {
        // 双写：同时写入新旧索引
        oldNodeID := c.index.Add(doc.Vector)
        newNodeID := c.newIndex.Add(doc.Vector)
        
        c.nodeToDoc[oldNodeID] = doc.ID
        c.newNodeToDoc[newNodeID] = doc.ID
    } else {
        c.index.Add(doc.Vector)
    }
    return nil
}

func (c *Collection) migrateExistingData() {
    validDocs := c.storage.GetAllValidDocuments()
    for _, doc := range validDocs {
        nodeID := c.newIndex.Add(doc.Vector)
        c.newDocToNode[doc.ID] = nodeID
        c.newNodeToDoc[nodeID] = doc.ID
    }
    
    // 完成后原子切换
    c.mu.Lock()
    c.index = c.newIndex
    c.docToNode = c.newDocToNode
    c.nodeToDoc = c.newNodeToDoc
    c.compacting = false
    c.mu.Unlock()
}
```

#### 优点
- 真正零停机，读写都不阻塞
- 用户体验最佳

#### 缺点
- 实现最复杂，需要处理双写一致性
- Compact 期间写入性能下降（双倍操作）
- 内存占用翻倍（两个索引同时存在）

#### 适用场景
- 对可用性要求极高的在线服务
- 金融、电商等关键业务
- 不能接受任何停机的场景

---

### 方案5: 分层 Compaction（LSM-Tree 风格）

将数据分为多层（L0 内存、L1 小文件、L2 大文件），Compact 只合并层间数据。

#### 实现原理

```go
type LayeredStorage struct {
    L0 *DocumentStorage  // 内存层（最新）
    L1 *DocumentStorage  // 小文件（最近 flush）
    L2 *DocumentStorage  // 大文件（历史）
}

// Compact 只是合并 L1 → L2，不涉及 HNSW！
func (s *LayeredStorage) CompactL1ToL2() error {
    // 1. 读取 L1 的所有有效文档
    docs := s.L1.GetAllValidDocuments()
    
    // 2. 合并到 L2（去除已删除）
    for _, doc := range docs {
        s.L2.Put(doc)
    }
    
    // 3. 清空 L1
    s.L1.Clear()
    
    return nil
}

// HNSW 不受影响，因为 docID 不变
// 只是数据从 L1 文件移动到了 L2 文件
```

#### 优点
- Compaction 极快，只涉及文件合并
- HNSW 索引不需要重建
- 读取可能涉及多层，但可通过缓存优化

#### 缺点
- 需要改造 Storage 架构
- 读取可能需要查询多层
- 实现复杂度高

#### 适用场景
- 写入量极高的场景
- 需要类似 LSM-Tree 的写优化
- 可接受读取稍复杂的架构

---

### 方案6: 只清理 Storage，HNSW 懒清理

只重写 Storage 文件，HNSW 索引不重建，而是维护一个"无效 nodeID 集合"。

#### 实现原理

```go
type HNSWIndex struct {
    // 原有字段...
    deletedNodes map[int]bool  // 懒清理的 nodeID 集合
}

func (c *Collection) Compact() error {
    // 1. 只重写 Storage（去掉已删除行）
    validDocs := c.storage.GetAllValidDocuments()
    c.storage.Rewrite(validDocs)
    c.storage.ClearDeletionVector()
    
    // 2. HNSW 不重建！
    // 而是记录哪些 nodeID 已经无效
    for _, doc := range validDocs {
        if doc.IsDeleted {  // 如果之前被删除了
            nodeID := c.docToNode[doc.ID]
            c.index.deletedNodes[nodeID] = true
        }
    }
    
    return nil
}

// Search 时过滤
func (c *Collection) Search(query []float32, k int) ([]SearchResult, error) {
    candidates := c.index.Search(query, k*2)
    
    results := make([]SearchResult, 0, k)
    for _, cand := range candidates {
        // 检查是否在无效集合中
        if c.index.deletedNodes[cand.ID] {
            continue  // 跳过已标记删除的节点
        }
        results = append(results, cand)
    }
    return results, nil
}
```

#### 优点
- Compaction 极快（只需重写 Storage）
- 实现简单，不需要重建 HNSW
- 适合 HNSW 重建成本极高的场景

#### 缺点
- HNSW 会不断膨胀（包含已删除的节点）
- 搜索需要额外过滤步骤
- 需要定期完全重建 HNSW

#### 适用场景
- HNSW 重建非常慢的大数据集
- 可以接受搜索性能轻微下降
- 作为完全重建前的过渡方案

---

### 方案7: 时间窗口 Compaction

只处理某个时间窗口之前的数据，新数据不受影响。

#### 实现原理

```go
func (c *Collection) CompactBefore(cutoffTime time.Time) error {
    // 1. 只获取截止时间之前的文档
    allDocs := c.storage.GetAll()
    oldDocs := filter(allDocs, func(d *Document) bool {
        return d.Timestamp.Before(cutoffTime)
    })
    
    // 2. 只压缩这些旧文档
    // 新数据保持不变
    c.storage.RewritePartial(oldDocs)
    
    // 3. 可能需要维护时间索引
    return nil
}

// 使用示例：只压缩 7 天前的数据
func (c *Collection) WeeklyCompact() {
    cutoff := time.Now().AddDate(0, 0, -7)
    c.CompactBefore(cutoff)
}
```

#### 优点
- 可以控制每次处理的数据量
- 新数据完全不受影响
- 适合时间序列特性明显的数据

#### 缺点
- 需要维护时间索引或扫描
- 旧数据和新数据可能分开存储
- 整体空间回收可能不彻底

#### 适用场景
- 时间序列数据（日志、指标）
- 冷数据需要压缩，热数据保持原样
- 有明显时间访问模式的数据

---

### 方案8: 外部触发 Compaction

不自动 Compact，完全由外部运维控制触发时机。

#### 实现原理

```go
func (c *Collection) Compact() error {
    // 完全手动触发
    // 内部实现可以是任何方案（阻塞式或轻量锁）
}

// 运维脚本
crontab:
# 每天凌晨 2 点执行 Compact
0 2 * * * /usr/local/bin/vego-admin compact --collection=mydb

// 或者通过 API 触发
curl -X POST http://admin-api/compact \
  -d '{"collection": "mydb", "timeout": "30m"}'
```

#### 优点
- 完全可控，风险最低
- 可以选择业务低峰期执行
- 可以结合流量切换（先切到备用节点）

#### 缺点
- 需要人工介入或额外运维系统
- 可能忘记 Compact 导致空间浪费
- 自动化程度低

#### 适用场景
- 关键业务，需要维护窗口
- 已有完善的运维体系
- 对自动化要求不高的内部系统

---

### 方案9: 空间换时间（预分配 + 标记重用）

预分配固定大小文件，删除不释放空间，而是标记为"可重用"。

#### 实现原理

```go
type DocumentStorage struct {
    maxDocs    int64           // 预分配：最大 1000 万文档
    freeSlots  []int64         // 空闲的 RowIndex 列表
    slots      []Slot          // 固定大小数组
}

type Slot struct {
    occupied bool
    doc      *Document
}

func (s *DocumentStorage) Put(doc *Document) (int64, error) {
    // 优先使用空闲槽位
    if len(s.freeSlots) > 0 {
        rowID := s.freeSlots[0]
        s.freeSlots = s.freeSlots[1:]
        s.slots[rowID] = Slot{occupied: true, doc: doc}
        return rowID, nil
    }
    
    // 没有空闲则追加
    rowID := len(s.slots)
    s.slots = append(s.slots, Slot{occupied: true, doc: doc})
    return int64(rowID), nil
}

func (s *DocumentStorage) Delete(id string) error {
    rowID := s.getRowID(id)
    s.slots[rowID].occupied = false
    s.freeSlots = append(s.freeSlots, rowID)  // 加入空闲列表
    return nil
}

// Compact 只是整理 freeSlots，不移动数据
func (s *DocumentStorage) Compact() {
    // 极快！只是整理内存中的空闲列表
    sort.Slice(s.freeSlots, func(i, j int) bool {
        return s.freeSlots[i] < s.freeSlots[j]
    })
}
```

#### 优点
- Compaction 几乎瞬间完成（微秒级）
- 读取写入都不受影响
- 实现简单，无锁竞争

#### 缺点
- 需要预分配，空间利用率低
- 文档数量不能超过预分配上限
- 需要估计最大文档数

#### 适用场景
- 文档数量相对稳定且可预测
- 追求极致性能，不能有任何停顿
- 内存/存储资源充足

---

## 决策建议

### 场景决策树

```
你的应用场景是？
│
├─► 离线批处理/可维护窗口
│   └─► 方案1: 完全阻塞式（简单可靠）
│
├─► 在线服务，读多写少
│   ├─► 能否接受写入阻塞？
│   │   ├─► 能 ──► 方案2: 轻量锁
│   │   └─► 不能 ──► 方案4: 后台双写
│   │
├─► 高写入场景
│   └─► 方案5: 分层 Compaction
│
├─► 时序数据（日志、指标）
│   └─► 方案7: 时间窗口 Compaction
│
├─► 数据量稳定且可预测
│   └─► 方案9: 预分配（极致性能）
│
└─► 关键业务，需要完全控制
    └─► 方案8: 外部触发
```

### Vego 项目建议

对于 Vego 向量数据库，我们建议采用**分阶段实现**：

#### Phase 1: 完全阻塞式（当前 Week 4）
- 实现简单，先满足基本需求
- 适合大多数用户的开发测试场景
- 为后续优化提供基础

#### Phase 2: 轻量锁（Week 5）
- 提升在线服务体验
- 读取零阻塞，写入短暂阻塞
- 平衡实现复杂度和性能

#### Phase 3: 可插拔架构（Week 6+）
- 提供接口让用户选择策略
- 支持自定义 Compaction 实现
- 针对特定场景的高级优化

#### Phase 4: 后台异步自动触发（高级优化）

在 Phase 1-3 的基础上，实现真正的**零人工干预**自动 Compaction。

##### 架构设计

```
┌─────────────────────────────────────────────────────────────┐
│                    Collection                               │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │   写入操作   │  │   读取操作   │  │   后台 Compact     │  │
│  │  (Insert/   │  │  (Get/      │  │      协程          │  │
│  │   Update/    │  │   Search)   │  │                    │  │
│  │   Delete)    │  │             │  │  ┌──────────────┐  │  │
│  └──────┬──────┘  └──────┬──────┘  │  │  1. 监控删除率  │  │  │
│         │                │          │  │  2. 检查间隔   │  │  │
│         ▼                ▼          │  │  3. 触发 Compact│  │  │
│  ┌─────────────────────────────┐   │  │  4. 进度通知   │  │  │
│  │      当前活跃索引            │   │  └──────────────┘  │  │
│  │      (HNSWIndex)            │   │         │          │  │
│  └─────────────────────────────┘   │         ▼          │  │
│                                    │  ┌──────────────┐  │  │
│  ┌─────────────────────────────┐   │  │   任务队列    │  │  │
│  │   Compact 状态通道           │◄──┘  │  (Channel)   │  │  │
│  │  (用于通知前台当前状态)       │      └──────────────┘  │  │
│  └─────────────────────────────┘                         │  │
└─────────────────────────────────────────────────────────────┘
```

##### 核心组件

**1. 自动触发器（AutoCompactor）**

```go
type AutoCompactor struct {
    collection    *Collection
    config        *CompactionConfig
    
    // 控制
    stopCh        chan struct{}      // 停止信号
    triggerCh     chan struct{}      // 手动触发通道
    statusCh      chan CompactStatus // 状态通知
    
    // 状态
    lastCompactTime time.Time
    compacting      bool
    mu              sync.RWMutex
}

type CompactStatus struct {
    State       CompactState  // Idle, Checking, Compacting, Completed, Failed
    Progress    float64       // 0.0 - 1.0
    Message     string        // 描述信息
    LastError   error         // 上次错误
    NextRunTime time.Time     // 下次运行时间
}

type CompactState int

const (
    CompactIdle CompactState = iota
    CompactChecking      // 检查条件
    CompactCompacting    // 正在压缩
    CompactCompleted     // 完成
    CompactFailed        // 失败
)
```

**2. 触发条件检查**

```go
func (ac *AutoCompactor) shouldCompact() (bool, string) {
    ac.mu.RLock()
    defer ac.mu.RUnlock()
    
    // 条件1: 自动压缩关闭
    if !ac.config.AutoCompact {
        return false, "auto-compact disabled"
    }
    
    // 条件2: 正在压缩中
    if ac.compacting {
        return false, "already compacting"
    }
    
    // 条件3: 最小间隔检查
    if time.Since(ac.lastCompactTime) < ac.config.MinInterval {
        return false, "too frequent"
    }
    
    // 条件4: 最大间隔检查（强制压缩）
    if time.Since(ac.lastCompactTime) > ac.config.MaxInterval {
        return true, "max interval reached"
    }
    
    // 条件5: 删除率阈值
    stats := ac.collection.Stats()
    if stats.DeletionRate >= ac.config.CompactThreshold {
        return true, fmt.Sprintf("deletion rate %.2f >= %.2f", 
            stats.DeletionRate, ac.config.CompactThreshold)
    }
    
    return false, "no condition met"
}
```

**3. 后台执行循环**

```go
func (ac *AutoCompactor) Run() {
    ticker := time.NewTicker(ac.config.CheckInterval) // 默认 30 秒检查一次
    defer ticker.Stop()
    
    for {
        select {
        case <-ac.stopCh:
            return // 停止
            
        case <-ticker.C:
            // 定期检查
            if should, reason := ac.shouldCompact(); should {
                ac.doCompact(reason)
            }
            
        case <-ac.triggerCh:
            // 手动触发
            ac.doCompact("manual trigger")
        }
    }
}

func (ac *AutoCompactor) doCompact(reason string) {
    ac.mu.Lock()
    ac.compacting = true
    ac.mu.Unlock()
    
    // 发送开始状态
    ac.statusCh <- CompactStatus{
        State:    CompactCompacting,
        Message:  fmt.Sprintf("Starting compact: %s", reason),
        Progress: 0.0,
    }
    
    // 执行压缩
    err := ac.collection.Compact()
    
    // 更新状态
    ac.mu.Lock()
    ac.compacting = false
    ac.lastCompactTime = time.Now()
    ac.mu.Unlock()
    
    // 发送完成状态
    if err != nil {
        ac.statusCh <- CompactStatus{
            State:     CompactFailed,
            Message:   fmt.Sprintf("Compact failed: %v", err),
            LastError: err,
        }
    } else {
        ac.statusCh <- CompactStatus{
            State:       CompactCompleted,
            Message:     "Compact completed successfully",
            Progress:    1.0,
            NextRunTime: ac.lastCompactTime.Add(ac.config.MinInterval),
        }
    }
}
```

**4. 与 Collection 集成**

```go
type Collection struct {
    // ... 现有字段 ...
    
    autoCompactor *AutoCompactor  // 可选，nil 表示不启用自动压缩
}

func NewCollection(name, path string, config *Config) (*Collection, error) {
    // ... 创建 Collection ...
    
    // 初始化自动压缩器
    if config.AutoCompact {
        coll.autoCompactor = &AutoCompactor{
            collection: coll,
            config:     config,
            stopCh:     make(chan struct{}),
            triggerCh:  make(chan struct{}),
            statusCh:   make(chan CompactStatus, 10),
        }
        go coll.autoCompactor.Run()
    }
    
    return coll, nil
}

func (c *Collection) Close() error {
    // 停止自动压缩器
    if c.autoCompactor != nil {
        close(c.autoCompactor.stopCh)
        // 等待当前 Compact 完成（带超时）
        select {
        case <-c.autoCompactor.waitDone():
        case <-time.After(30 * time.Second):
            log.Println("Warning: AutoCompactor did not stop in time")
        }
    }
    
    return c.saveAndCleanup()
}

// 公开 API：获取 Compact 状态
func (c *Collection) CompactStatus() CompactStatus {
    if c.autoCompactor == nil {
        return CompactStatus{State: CompactIdle, Message: "Auto-compact disabled"}
    }
    return c.autoCompactor.CurrentStatus()
}

// 公开 API：手动触发
func (c *Collection) TriggerCompact() error {
    if c.autoCompactor == nil {
        return errors.New("auto-compact not enabled")
    }
    select {
    case c.autoCompactor.triggerCh <- struct{}{}:
        return nil
    default:
        return errors.New("trigger channel full, try later")
    }
}
```

##### 使用示例

```go
// 启用自动压缩
coll, err := db.Collection("docs", 
    vego.WithAutoCompact(true),
    vego.WithCompactThreshold(0.3),
    vego.WithCompactMinInterval(5*time.Minute),
)

// 查询压缩状态
status := coll.CompactStatus()
fmt.Printf("Compact state: %s, progress: %.1f%%\n", 
    status.State, status.Progress*100)

// 手动触发（如果需要立即压缩）
if err := coll.TriggerCompact(); err != nil {
    log.Printf("Trigger compact failed: %v", err)
}
```

##### 优点
- **真正的自动化**：无需人工干预，后台自动维护
- **可观测性**：状态通道提供实时进度和通知
- **可控性**：支持手动触发、配置调整、优雅停止
- **非侵入性**：不影响现有读写操作（基于 Phase 1 阻塞式）

##### 缺点
- **实现复杂**：需要管理 goroutine 生命周期、状态机、并发安全
- **资源竞争**：后台 Compact 仍会与前台操作竞争资源
- **调试困难**：异步问题难以复现和调试
- **测试复杂**：需要模拟时间、并发触发、优雅关闭等场景

##### 适用场景
- 长期运行的在线服务
- 运维人力有限的场景
- 需要"设置后忘记"的简单运维

##### 实现时机建议

**Phase 4.1（Week 7+）**：基础版本
- 简单的定时检查 + 条件触发
- 基础状态通知
- 单 Collection 独立运行

**Phase 4.2（Week 8+）**：增强版本
- 支持跨 Collection 资源协调
- 添加 Prometheus 指标导出
- Web UI 查看 Compact 状态

**Phase 4.3（Week 9+）**：智能版本
- 基于负载的动态调整
- 预测性 Compact（在高峰期前完成）
- 与集群调度器集成

### 配置建议

```go
// 默认配置（平衡型）
type CompactionConfig struct {
    // 触发条件
    AutoCompact       bool          // 是否启用自动压缩（默认 true）
    DeletionThreshold float64       // 删除率阈值（默认 0.30 = 30%）
    MinInterval       time.Duration // 最小间隔（默认 5 分钟）
    MaxInterval       time.Duration // 最大间隔（默认 7 天）
    
    // 策略选择
    Strategy          CompactStrategy // 压缩策略（默认阻塞式）
    
    // 性能限制
    MaxConcurrent     int           // 最大并发（默认 1）
    BatchSize         int           // 分批大小（默认 0 = 不分批）
}

type CompactStrategy int

const (
    CompactStrategyBlocking CompactStrategy = iota      // 完全阻塞式
    CompactStrategyLightweight                          // 轻量锁
    CompactStrategyIncremental                          // 增量式
    // ... 其他策略
)
```

---

## 总结

没有完美的 Compaction 策略，只有最适合你场景的方案：

- **追求简单**：选择方案1（完全阻塞式）
- **追求可用性**：选择方案2（轻量锁）或方案4（后台双写）
- **追求性能**：选择方案5（分层）或方案9（预分配）
- **追求可控性**：选择方案8（外部触发）

Vego 项目将优先实现**方案1（完全阻塞式）**和**方案2（轻量锁）**，通过配置让用户根据场景选择。
