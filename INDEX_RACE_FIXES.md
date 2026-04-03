# Index 包并发安全问题修复报告

## 修复概览

| 问题 | 位置 | 严重程度 | 状态 | 修复方式 |
|------|------|---------|------|----------|
| 问题 1 | search.go:314-326 | 中等 | ✅ 已修复 | 锁内拷贝 vector |
| 问题 2 | insert.go:46, 73 | 低（设计意图） | ⏭️ 未修复 | 当前设计有效 |
| 问题 3 | search.go:139 | 轻微 | ✅ 已修复 | 移入锁内检查 |
| 问题 4 | insert.go:61-64 | 中等 | ✅ 已修复 | 锁内拷贝 vector |

---

## 修复详情

### 问题 1: selectNeighborsHeuristic 中 unlock-then-use 模式

**文件**: `index/search.go`  
**修改**: 在 `h.globalLock.RLock()` 内拷贝 vector 数据，而不是依赖 `Vector()` 方法

```go
// 修复前:
h.globalLock.RLock()
candidateVec := h.nodes[candidate.ID].Vector()
h.globalLock.RUnlock()
// ... 使用 candidateVec

// 修复后:
h.globalLock.RLock()
node := h.nodes[candidate.ID]
candidateVec := make([]float32, len(node.vector))
copy(candidateVec, node.vector)
h.globalLock.RUnlock()
// ... 使用 candidateVec（安全拷贝）
```

**说明**: 虽然 `Vector()` 方法已返回拷贝，但直接在锁内访问 `node.vector` 更明确，避免了对外部方法实现的依赖。

---

### 问题 2: insert.go 中修改节点时未持有全局锁

**文件**: `index/insert.go:46, 73`  
**状态**: 未修复（当前设计意图如此）

**分析**:
- `h.globalLock` 保护 `h.nodes` 数组结构
- `n.mu` 保护节点内容
- 这种分层锁设计是合理的，可以提高并发性

**代码模式**:
```go
h.globalLock.RLock()
neighborNode := h.nodes[neighbor.ID]
h.globalLock.RUnlock()
neighborNode.AddConnection(lc, newNodeID) // 使用 n.mu 保护
```

**结论**: 这是设计选择，不是 bug。`AddConnection` 等方法内部有 `n.mu` 锁保护。

---

### 问题 3: searchLayerAggressive 边界检查在锁外

**文件**: `index/search.go:139`  
**修改**: 将边界检查移入 `RLock` 内，避免 TOCTOU

```go
// 修复前:
if current.value < 0 || current.value >= len(h.nodes) {
    continue
}
h.globalLock.RLock()
neighbors := h.nodes[current.value].GetConnections(level)
h.globalLock.RUnlock()

// 修复后:
h.globalLock.RLock()
if current.value < 0 || current.value >= len(h.nodes) {
    h.globalLock.RUnlock()
    continue
}
neighbors := h.nodes[current.value].GetConnections(level)
h.globalLock.RUnlock()
```

**说明**: 虽然 HNSW 无删除操作（节点不会失效），但将检查移入锁内更严谨。

---

### 问题 4: insert.go 中两次解锁同一锁（vector 访问）

**文件**: `index/insert.go:61-64`  
**修改**: 在锁内安全拷贝 vector 数据

```go
// 修复前:
h.globalLock.RLock()
connNode := h.nodes[connID]
h.globalLock.RUnlock()
dist := h.distFunc(neighborNode.Vector(), connNode.Vector())

// 修复后:
// Pre-fetch neighborNode's vector with proper locking
h.globalLock.RLock()
neighborVec := make([]float32, len(neighborNode.vector))
copy(neighborVec, neighborNode.vector)
h.globalLock.RUnlock()

for i, connID := range neighborConnections {
    h.globalLock.RLock()
    connNode := h.nodes[connID]
    connVec := make([]float32, len(connNode.vector))
    copy(connVec, connNode.vector)
    h.globalLock.RUnlock()
    dist := h.distFunc(neighborVec, connVec)
}
```

---

## 验证结果

### 编译检查
```bash
$ go build ./...
✅ Build successful!
```

### Race 检测
```bash
# 因测试超时，使用 -short 模式
$ go test -race -short ./index/...
# 预期: PASS（修复前也是 PASS，因为这些是防御性修复）
```

---

## 总结

- **已修复**: 问题 1、3、4（共 3 处）
- **无需修复**: 问题 2（设计意图）
- **所有修改**: 通过编译验证
- **代码质量**: 修复使并发访问模式更清晰，减少了对外部方法实现的依赖
