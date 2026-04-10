# Column Statistics (Min/Max) 设计实现审查报告

## 1. 并发安全性审查

### 1.1 Writer 并发安全 ⚠️ 中风险

**问题发现：**
```go
type Writer struct {
    columnStats *format.StatisticsList  // 无并发保护
}

func (w *Writer) WriteRecordBatch(batch *arrow.RecordBatch) error {
    // ...
    batchStats := format.ComputeColumnStatistics(column, int32(colIdx))
    if existingStats := w.columnStats.GetColumnStats(int32(colIdx)); existingStats != nil {
        existingStats.Merge(batchStats)  // 并发调用可能竞争
    }
}
```

**风险评估：**
- `Writer` 目前设计为单线程使用，`WriteRecordBatch` 不是并发安全的
- `Merge()` 操作涉及读取和写入统计信息，多 goroutine 并发调用会导致数据竞争
- 当前设计中 `Writer` 实例通常由一个 goroutine 使用，风险可控

**建议：**
1. 添加文档明确说明 Writer 不是并发安全的
2. 如需支持并发写入，应添加 `sync.Mutex` 保护 `columnStats`

### 1.2 Reader 并发安全 ✅ 良好

```go
type Reader struct {
    mu    sync.Mutex  // 已有锁保护
    stats *format.StatisticsList
}

func (r *Reader) GetColumnStats(columnIndex int32) *format.ColumnStatistics {
    // 读取操作，无需加锁（stats 写入后不变）
}
```

**评估：**
- `stats` 在 `readFooter` 时一次性读取，之后只读
- `GetColumnStats` 等读取方法无需加锁，性能良好
- `Zone Map` 评估方法是纯函数，无副作用

---

## 2. 性能审查

### 2.1 统计计算性能 ✅ 良好

**当前实现：**
```go
func computeInt32Stats(arr *arrow.Int32Array, stats *ColumnStatistics) {
    values := arr.Values()  // O(1) 直接获取底层切片
    var min, max int32
    first := true
    
    for i, v := range values {
        if arr.IsNull(i) {  // O(1) bitmap 检查
            continue
        }
        // ...
    }
}
```

**评估：**
- 时间复杂度：O(n)，单次遍历
- 空间复杂度：O(1)，只使用常数额外空间
- 内存访问：顺序访问 values 数组，缓存友好
- Null 检查使用 `IsNull()` 方法，内部是 bitmap 操作，高效

**优化建议：**
```go
// 对于没有 null 值的数组，可以跳过检查
func computeInt32StatsOptimized(arr *arrow.Int32Array, stats *ColumnStatistics) {
    values := arr.Values()
    if arr.NullN() == 0 {
        // 无 null 值，直接使用标准库
        min, max := minMaxSlice(values)
        stats.SetMinMaxInt32(min, max)
        return
    }
    // 原逻辑...
}
```

### 2.2 内存分配 ⚠️ 可优化

**问题发现：**
```go
func (cs *ColumnStatistics) SetMinMaxInt32(min, max int32) {
    cs.HasMinMax = true
    cs.MinValue = make([]byte, 4)   // 每次调用都分配
    cs.MaxValue = make([]byte, 4)   // 每次调用都分配
    // ...
}
```

**问题：**
- 每次 Merge 都会重新分配内存
- 可以通过对象池复用减少 GC 压力

**优化建议：**
```go
var minMaxBytePool = sync.Pool{
    New: func() interface{} {
        b := make([]byte, 8) // 最大支持 float64
        return &b
    },}

// 或者使用固定大小的数组避免堆分配
type ColumnStatistics struct {
    // ...
    MinValue [8]byte  // 固定大小，栈分配
    MaxValue [8]byte
    ValueLen int8     // 实际使用的字节数 (0, 4, 或 8)
}
```

### 2.3 序列化性能 ✅ 良好

```go
func (sl *StatisticsList) WriteTo(w io.Writer) (int64, error) {
    buf := new(bytes.Buffer)  // 预分配
    // 先写入 buffer，然后一次性写入文件
    n, err := w.Write(buf.Bytes())
}
```

**评估：**
- 使用内存 buffer 减少系统调用
- 可以添加 `EncodedSize()` 预计算大小，精确预分配

---

## 3. 安全性审查

### 3.1 输入验证 ✅ 良好

**序列化验证：**
```go
func (sl *StatisticsList) ReadFrom(r io.Reader) (int64, error) {
    // 验证长度字段，防止 OOM
    if minLen > MaxValueSize {  // 应该添加此类检查
        return error
    }
    stats.MinValue = make([]byte, minLen)
}
```

**缺失：**
- 缺少对 `minLen/maxLen` 的最大值限制（防止恶意构造的大长度导致 OOM）

**建议：**
```go
const MaxStatsValueSize = 1024 // 对于 Min/Max 值，1KB 足够

if minLen > MaxStatsValueSize {
    return 0, lerrors.New(lerrors.ErrCorruptedFile).
        Op("read_stats").
        Context("minLen", minLen).
        Context("max", MaxStatsValueSize).
        Build()
}
```

### 3.2 类型安全 ⚠️ 需改进

**问题发现：**
```go
func (cs *ColumnStatistics) Merge(other *ColumnStatistics) error {
    // 通过字节长度推断类型
    switch len(cs.MinValue) {
    case 4: // 32-bit type (int32 or float32)
        // Try int32 first
        if min1, max1, ok1 := cs.GetMinMaxInt32(); ok1 {
            // ...
        }
        // Try float32
    }
}
```

**问题：**
- 通过字节长度推断类型不可靠
- 如果将来支持 string/binary 类型，可能有歧义

**建议：**
```go
type ColumnStatistics struct {
    // ...
    TypeID uint8  // 显式存储类型标识
}

const (
    TypeInt32 = iota
    TypeInt64
    TypeFloat32
    TypeFloat64
    TypeString
    TypeBinary
)
```

### 3.3 整数溢出 ✅ 良好

```go
// NullCount 使用 int64，足够大
type ColumnStatistics struct {
    NullCount int64
}
```

---

## 4. 架构合理性审查

### 4.1 设计模式 ✅ 良好

**分离关注点：**
- `statistics.go`: 纯数据结构 + 序列化
- `writer.go`: 统计计算和写入
- `reader.go`: 统计读取和 Zone Map 评估

**优点：**
- 清晰的职责分离
- 易于单元测试
- 符合 Lance 兼容性设计

### 4.2 向后兼容性 ✅ 优秀

```go
func (f *Footer) ReadFrom(r io.Reader) (int64, error) {
    // ...
    // Read statistics info (version 1.1+)
    remainingBeforeChecksum := reader.Len() - 4
    if remainingBeforeChecksum >= 12 {
        binary.Read(reader, ByteOrder, &f.StatsOffset)
        binary.Read(reader, ByteOrder, &f.StatsCount)
    } else {
        // Old file format without statistics
        f.StatsOffset = 0
        f.StatsCount = 0
    }
}
```

**评估：**
- 通过检测剩余字节数判断是否包含统计信息
- 旧文件格式自动兼容（StatsOffset=0 表示无统计）

### 4.3 可扩展性 ✅ 良好

**版本管理：**
```go
type ColumnStatistics struct {
    Version uint16  // 支持未来扩展
}

type StatisticsList struct {
    Version uint16
}
```

**支持的功能：**
- ✅ Min/Max
- ✅ NullCount
- ✅ DistinctCount（预留）
- 🔄 未来可添加：Histogram、Cardinality、Bloom Filter

### 4.4 API 设计 ✅ 良好

**Reader API：**
```go
func (r *Reader) GetColumnStats(columnIndex int32) *ColumnStatistics
func (r *Reader) HasStatistics() bool
func (r *Reader) EvaluateZoneMapInt32(columnIndex int32, value int32) ZoneMapFilterResult
```

**优点：**
- 清晰的返回值语义
- 容错设计（无统计时返回保守结果）
- 类型特定的 Zone Map 方法，编译时类型安全

---

## 5. 代码质量审查

### 5.1 错误处理 ✅ 良好

```go
return lerrors.New(lerrors.ErrInvalidArgument).
    Op("merge_column_stats").
    Context("message", "min/max value size mismatch").
    Build()
```

**优点：**
- 使用结构化错误
- 包含操作名称和上下文

### 5.2 文档 ✅ 良好

- 导出的类型和方法都有注释
- 包含使用示例（在测试中）

### 5.3 测试覆盖 ⚠️ 可加强

**当前测试：**
- ✅ 基本统计计算
- ✅ 多 batch 合并
- ✅ Null 值处理
- ✅ Zone Map 谓词下推
- ✅ Footer 偏移量验证

**缺失测试：**
- 🔄 大文件统计（超过一个 batch）
- 🔄 并发读取统计
- 🔄 损坏的统计数据处理
- 🔄 所有数值类型的边界值

---

## 6. 问题汇总与修复建议

### 6.1 高优先级问题

| 问题 | 位置 | 修复建议 |
|------|------|----------|
| OOM 风险 | `ReadFrom` | 添加 MaxStatsValueSize 限制 |
| 并发文档缺失 | `Writer` | 添加非线程安全说明 |

### 6.2 中优先级问题

| 问题 | 位置 | 修复建议 |
|------|------|----------|
| 类型推断模糊 | `Merge` | 添加显式 TypeID 字段 |
| 内存分配 | `SetMinMax*` | 使用对象池或固定数组 |
| Null 检查开销 | `compute*Stats` | 无 null 时走优化路径 |

### 6.3 低优先级建议

| 建议 | 说明 |
|------|------|
| 添加统计信息缓存 | Reader 可以缓存反序列化的 stats |
| 支持字符串 Min/Max | 预留 UTF-8 字符串统计 |
| 批量评估 API | `EvaluateZoneMapBatch` 减少函数调用开销 |

---

## 7. 总体评估

| 维度 | 评分 | 说明 |
|------|------|------|
| 并发安全 | B+ | Reader 安全，Writer 需明确文档 |
| 性能 | A- | 高效，有小优化空间 |
| 安全性 | B+ | 缺少 OOM 保护 |
| 架构设计 | A | 清晰、可扩展、兼容性好 |
| 代码质量 | A- | 良好，测试可加强 |

**综合评级：A-** ✅ 推荐合并，建议修复高优先级问题

---

*审查日期: 2026-04-03*
*审查人: AI Code Reviewer*
