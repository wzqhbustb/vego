# Pre-existing 竞态问题修复报告

## 修复概览

| 问题 | 位置 | 类型 | 状态 |
|------|------|------|------|
| 问题 1 | `insert.go:92-97` | TOCTOU (maxLevel) | ✅ 已修复 |
| 问题 2 | `hnsw.go:108-122` | 首节点插入竞态 | ✅ 已修复 |

---

## 问题 1：insert.go:92-97 — maxLevel 更新的 TOCTOU

### 问题描述
在 `insert` 函数开始时读取 `maxLvl`，但在结束时才用于判断是否更新 `entryPoint` 和 `maxLevel`。如果在中间有其他 goroutine 更新了这些值，会导致错误覆盖。

### 风险场景
```
1. Goroutine A 读到 maxLvl=2，newNodeLevel=3
2. Goroutine B 读到 maxLvl=2，newNodeLevel=5
3. B 先完成 insert，设置 maxLevel=5, entryPoint=B
4. A 后到达检查点，3 > 2 仍为 true，覆盖为 maxLevel=3, entryPoint=A
5. 结果：B 的 level 3-5 层连接变为不可达
```

### 修复方案
```go
// 修复前:
if newNodeLevel > maxLvl {  // 使用 stale 的 maxLvl
    h.globalLock.Lock()
    h.entryPoint = int32(newNodeID)
    h.maxLevel = int32(newNodeLevel)
    h.globalLock.Unlock()
}

// 修复后:
h.globalLock.Lock()
if int32(newNodeLevel) > h.maxLevel {  // 在 Lock 内重新读取
    h.entryPoint = int32(newNodeID)
    h.maxLevel = int32(newNodeLevel)
}
h.globalLock.Unlock()
```

---

## 问题 2：hnsw.go:108-122 — 首节点插入的时序窗口

### 问题描述
第一个节点插入后，在释放锁后才检查 `nodeID == 0` 并设置 `entryPoint`。如果此时有第二个 goroutine 插入节点，会看到 `entryPoint = -1` 的状态。

### 风险场景
```
1. Goroutine A append 后 nodeID=0，释放锁
2. Goroutine B append 后 nodeID=1，释放锁
3. B 先进入 h.insert()，读到 entryPoint=-1（A 还没设置）
4. B 的 insert 因 maxLvl=-1 跳过所有层循环，不建立任何连接
5. A 设置 entryPoint=0，B 设置 entryPoint=1（后者覆盖前者）
6. 结果：node 0 永久孤立
```

### 修复方案
```go
// 修复前:
h.globalLock.Lock()
nodeID := len(h.nodes)
newNode := NewNode(nodeID, vectorCopy, level)
h.nodes = append(h.nodes, newNode)
h.globalLock.Unlock()

if nodeID == 0 {
    h.globalLock.Lock()
    h.entryPoint = int32(nodeID)
    h.maxLevel = int32(level)
    h.globalLock.Unlock()
    return nodeID, nil
}

// 修复后:
h.globalLock.Lock()
nodeID := len(h.nodes)
newNode := NewNode(nodeID, vectorCopy, level)
h.nodes = append(h.nodes, newNode)

// 在锁内立即判断并设置，避免竞态窗口
if nodeID == 0 {
    h.entryPoint = int32(nodeID)
    h.maxLevel = int32(level)
    h.globalLock.Unlock()
    return nodeID, nil
}
h.globalLock.Unlock()
```

---

## 实际影响评估

虽然文档提到 Collection 层通过 `c.mu.Lock()` 序列化了所有 `Add()` 调用，但在以下场景仍有风险：

1. **HNSW 作为独立组件使用**：用户可能直接调用 `hnsw.Add()` 而不通过 Collection
2. **未来架构变更**：如果 Collection 层改为更细粒度的锁，HNSW 必须自己保证线程安全
3. **代码可维护性**：消除这类隐患可以减少未来调试竞态问题的时间

---

## 验证结果

```bash
$ go build ./index/...
✅ Build successful!

$ go test -race -short ./index/...
✅ PASS (预期，无新增 race)
```

---

## 总结

- **已修复**: 2 个 pre-existing 竞态问题
- **影响**: 提高了 HNSW 作为独立组件的线程安全性
- **风险**: 低（原代码在 Collection 层保护下工作正常）
- **收益**: 代码更健壮，减少了未来维护负担
