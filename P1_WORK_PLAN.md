# P1 阶段详细工作计划

## 阶段目标
完成技术债清理，为 Phase 2 (MVP) 和 Phase 3 (Zone Map) 打下坚实基础。

---

## 任务一：Null 编码统一（主要工作）

### 现状分析

| 编码器 | 当前 null 支持 | 处理方式 |
|--------|---------------|----------|
| Zstd | ✅ 支持 | null bitmap 存储 |
| RLE | ❌ 不支持 | 返回 `ErrNullNotSupported`，回退 Zstd |
| BitPacking | ❌ 不支持 | 返回 `ErrNullNotSupported`，回退 Zstd |
| BSS | ❌ 不支持 | 返回 `ErrNullNotSupported`，回退 Zstd |
| Dictionary | ❌ 不支持 | 返回 `ErrNullNotSupported`，回退 Zstd |

**影响**：含有 null 的数据无法享受特定编码的压缩优势，统一回退到 Zstd。

### 方案设计

采用 **"null bitmap 分离"** 方案（与 Zstd 保持一致）：

```
编码后数据结构：
┌─────────────────┬─────────────────┬─────────────────┐
│  Values Data    │  Null Bitmap    │  Metadata       │
│  (非 null 值)   │  (bitmap 格式)  │  (count 等)     │
└─────────────────┴─────────────────┴─────────────────┘
```

**各编码器处理策略**：

| 编码器 | null 值处理 | 编码内容 |
|--------|------------|----------|
| RLE | 跳过 null，单独存储 bitmap | 非 null 值的 run |
| BitPacking | 跳过 null，单独存储 bitmap | 非 null 值的 bits |
| BSS | 跳过 null，单独存储 bitmap | 非 null 值的 byte streams |
| Dictionary | null 作为特殊 index (0) | 所有值的 dict index |

### 具体实施步骤

#### Step 1: 定义统一的 Nullable 编码接口
```go
// NullableEncodedData 包含编码值和 null bitmap
type NullableEncodedData struct {
    Data      []byte           // 编码后的值数据
    NullBitmap []byte          // null bitmap (bit 0 = null)
    NumValues int              // 总条目数（含 null）
    NullCount int              // null 条目数
}
```

#### Step 2: 实现 NullBitmap 工具包
- 创建 `storage/encoding/nullbitmap.go`
- 提供 `EncodeNullBitmap(nulls []bool) []byte`
- 提供 `DecodeNullBitmap(data []byte, numValues int) []bool`
- 提供 `FilterNulls(values []T, nulls []bool) []T`

#### Step 3: 逐个修改编码器

**3.1 RLE 编码器** (`rle.go`)
```go
func (e *RLEEncoder) Encode(array arrow.Array) (*EncodedData, error) {
    if array.NullN() > 0 {
        // 提取非 null 值
        values := filterNonNullValues(array)
        // 对非 null 值进行 RLE 编码
        rleData := encodeRuns(values)
        // 单独编码 null bitmap
        nullBitmap := encodeNullBitmap(array)
        // 组合返回
        return combineWithNullBitmap(rleData, nullBitmap)
    }
    // 原有逻辑...
}
```

**3.2 BitPacking 编码器** (`bitpacking.go`)
```go
// 类似修改：跳过 null 值，只打包有效值
// 注意：需要记录每个值的位置，解码时根据 bitmap 还原
```

**3.3 BSS 编码器** (`bss.go`)
```go
// 类似修改：只对非 null 值进行 byte-stream-split
```

**3.4 Dictionary 编码器** (`dictionary.go`)
```go
// 策略不同：null 可以作为字典的第 0 个条目
// 或者：单独存储 null bitmap，字典只编码非 null 值
```

#### Step 4: 更新 EncoderFactory
```go
func (f *EncoderFactory) SelectEncoder(dtype arrow.DataType, stats *Statistics) Encoder {
    // 如果数据包含 null，选择支持 null 的编码器
    if stats.NullCount != nil && *stats.NullCount > 0 {
        return f.selectNullableEncoder(dtype, stats)
    }
    // 原有逻辑...
}
```

#### Step 5: 对应的解码器修改
- `rle_decoder.go`: 解码时根据 null bitmap 还原完整数组
- `bitpacking_decoder.go`: 同上
- `bss_decoder.go`: 同上
- `dictionary_decoder.go`: 同上

### 工作量估算

| 步骤 | 工作量 | 复杂度 |
|------|--------|--------|
| Step 1: 接口定义 | 0.5 天 | 低 |
| Step 2: NullBitmap 工具 | 1 天 | 中 |
| Step 3.1: RLE + Decoder | 1 天 | 中 |
| Step 3.2: BitPacking + Decoder | 1 天 | 中 |
| Step 3.3: BSS + Decoder | 1 天 | 中 |
| Step 3.4: Dictionary + Decoder | 1 天 | 高（策略不同）|
| Step 4: Factory 更新 | 0.5 天 | 低 |
| Step 5: 集成测试 | 1 天 | 中 |
| **总计** | **~7 天** | - |

---

## 任务二：页面级 Min/Max 统计（次要工作）

### 现状分析

**当前 Statistics 已有**：
- `NullCount` - null 数量
- `BitWidth` - 最大位宽
- `Cardinality` - 基数
- `RunCount` - run 数量
- `MaxLength` - 最大长度（变长类型）

**缺失**：
- `MinValue` - 最小值
- `MaxValue` - 最大值

### 方案设计

在 `Statistics` 结构体中添加 Min/Max：

```go
type Statistics struct {
    // ... 已有字段 ...
    
    // Min/Max for Zone Map (Phase 3)
    MinValue interface{} // 实际类型根据 DataType 变化
    MaxValue interface{}
}
```

**计算时机**：
- `ComputeStatistics()` 中添加 Min/Max 计算
- 仅对数值类型（Int32, Int64, Float32, Float64）计算

**存储位置**：
- `storage/format/page.go` - PageHeader 添加 Min/Max 字段
- 写入 Page 时存入元数据
- 读取 Page 时可用于过滤（Phase 3 实现）

### 具体实施步骤

#### Step 1: 扩展 Statistics 结构
```go
// 添加 Min/Max 字段
type Statistics struct {
    // ... 已有 ...
    
    MinInt32  *int32   // for Int32
    MaxInt32  *int32
    MinInt64  *int64   // for Int64
    MaxInt64  *int64
    MinFloat32 *float32 // for Float32
    MaxFloat32 *float32
    // ... etc
}
```

#### Step 2: 实现 Min/Max 计算
```go
func computeFixedWidthStats(stats *Statistics, buffer *arrow.Buffer, bitsPerValue int, numValues int) {
    // ... 已有逻辑 ...
    
    // 新增：Min/Max 计算
    switch bitsPerValue {
    case 32:
        values := buffer.Int32()
        min, max := computeMinMax32(values)
        stats.MinInt32 = &min
        stats.MaxInt32 = &max
    case 64:
        values := buffer.Int64()
        min, max := computeMinMax64(values)
        stats.MinInt64 = &min
        stats.MaxInt64 = &max
    }
}
```

#### Step 3: 更新 PageHeader 存储
```go
type PageHeader struct {
    // ... 已有字段 ...
    
    // Statistics for Zone Map
    HasStatistics bool
    MinValue      []byte // 序列化的最小值
    MaxValue      []byte // 序列化的最大值
}
```

#### Step 4: 页面写入时存储 Min/Max
```go
func (w *PageWriter) WritePage(data *EncodedData, stats *Statistics) error {
    // ... 已有逻辑 ...
    
    // 添加 Min/Max 到 header
    if stats != nil {
        header.HasStatistics = true
        header.MinValue = serializeMinValue(stats)
        header.MaxValue = serializeMaxValue(stats)
    }
}
```

#### Step 5: 页面读取时返回 Min/Max
```go
func (r *PageReader) ReadPageHeader() (*PageHeader, error) {
    // ... 已有逻辑 ...
    
    // 读取 Min/Max
    if header.HasStatistics {
        stats.MinValue = deserializeMinValue(header.MinValue)
        stats.MaxValue = deserializeMaxValue(header.MaxValue)
    }
}
```

### 工作量估算

| 步骤 | 工作量 | 复杂度 |
|------|--------|--------|
| Step 1: 扩展 Statistics | 0.5 天 | 低 |
| Step 2: Min/Max 计算 | 0.5 天 | 低 |
| Step 3: PageHeader 扩展 | 0.5 天 | 低 |
| Step 4: 写入时存储 | 0.5 天 | 低 |
| Step 5: 读取时返回 | 0.5 天 | 低 |
| 集成测试 | 0.5 天 | 低 |
| **总计** | **~2.5 天** | - |

---

## 执行顺序建议

```
Week 1: Min/Max 统计（快速完成，为 Zone Map 做准备）
  ├── Day 1: Statistics 扩展 + Min/Max 计算
  ├── Day 2: PageHeader 扩展 + 读写集成
  └── Day 3: 测试 + 文档

Week 2-3: Null 编码统一（主要工作）
  ├── Day 1-2: NullBitmap 工具包
  ├── Day 3-4: RLE + BitPacking 支持
  ├── Day 5-6: BSS + Dictionary 支持
  ├── Day 7: Factory 更新
  └── Day 8: 集成测试
```

**理由**：
1. Min/Max 统计工作量小，先完成可以快速闭环
2. 完成 Min/Max 后，Phase 3 的 Zone Map 可以立即开始
3. Null 编码是长期技术债，需要集中时间解决

---

## 验收标准

### Null 编码统一
- [ ] RLE 编码器支持含 null 的数据
- [ ] BitPacking 编码器支持含 null 的数据
- [ ] BSS 编码器支持含 null 的数据
- [ ] Dictionary 编码器支持含 null 的数据
- [ ] 所有编码器通过含 null 数据的 round-trip 测试
- [ ] 性能测试：含 null 数据的压缩比不低于 Zstd

### Min/Max 统计
- [ ] Statistics 结构体包含 Min/Max 字段
- [ ] ComputeStatistics() 正确计算 Min/Max
- [ ] PageHeader 存储 Min/Max
- [ ] 页面读写正确保留 Min/Max
- [ ] 集成测试通过

---

## 风险与应对

| 风险 | 可能性 | 影响 | 应对 |
|------|--------|------|------|
| Null 编码改动引入 bug | 中 | 高 | 完善的 round-trip 测试 |
| 性能下降 | 低 | 中 | 基准测试对比，必要时优化 |
| 兼容性问题 | 低 | 高 | 版本号管理，旧格式可读 |
| 工作量超预期 | 中 | 中 | 分阶段交付，先核心编码器 |
