# Wave 4: Append-Only Row Group Format — 详细设计文档

> 状态：设计评审中  
> 基于：Vego v0.1.5（Wave 1/2/3 已完成）  
> 目标：解除 `flush()` 的 O(N) 全量重写瓶颈

---

## 1. 当前格式与痛点分析

### 1.1 当前文件布局（`vectors.lance`）

```
Offset 0
┌────────────────────────────────────────┐
│ HEADER (max 8192 bytes)                │
│   [Magic:4][Version:2][Flags:2]        │
│   [NumRows:8][NumColumns:4][PageSize:4]│
│   [Reserved:32]                        │
│   [SchemaLen:4][SchemaJSON:N]          │
│   [padding to 8192]                    │
├────────────────────────────────────────┤  ← 8192
│ PAGES (all columns, all pages)         │
│   [PageHeader:30][Data][NullBitmap?]   │
│   ...                                  │
├────────────────────────────────────────┤
│ STATISTICS (optional, variable)        │
│   [Version:2][NumColumns:4][Stats...]  │
│   [CRC32:4]                            │
├────────────────────────────────────────┤
│ FOOTER (exactly 32768 bytes)           │
│   [Version:2][NumPages:4]              │
│   [CreatedAt:8][ModifiedAt:8]          │
│   [PageIndexList: count+entries]       │
│   [Metadata: count+entries]            │
│   [StatsOffset:8][StatsCount:4]        │
│   [CRC32:4][padding to 32768]          │
└────────────────────────────────────────┘
```

### 1.2 核心痛点

| 痛点 | 根因 | 影响 |
|---|---|---|
| `flush()` 每次全量重写 | 单文件结构，新数据必须和旧数据一起重写 | 100万向量写入耗时随数据量线性增长 |
| `readAllDocuments()` | 为了和 buffer 合并，必须从磁盘读全量 | 每次 flush 都触发大规模读取 |
| BlockCache 全失效 | 文件路径不变但内容全变，缓存键失效 | _flush 后首次查询性能骤降 |
| RowIndex 重建 | 没有增量索引机制 | 索引构建也变成 O(N) |
| Header 预留 8KB | Schema 变大后可能溢出 | 格式扩展性受限 |

### 1.3 Manifest 现状

`storage/format/manifest.go` 已定义 `Manifest` / `ManifestManager` 结构，但**整个代码库零引用**（未使用）。这是 Wave 4 的理想扩展基础。

---

## 2. 目标格式设计：Append-Only Row Group

### 2.1 核心思想

- 每个 **Row Group (RG)** 是独立自包含的 mini-column-file
- `flush()` 新 batch 时，**追加**一个新 RG 到文件末尾
- 用 **Manifest** 记录所有 RG 的偏移、行数、统计信息
- Manifest 是多版本 + 原子写入（tmp + rename）
- **Global Footer 可选**，初期省略以控制复杂度

### 2.2 新文件布局

```
Offset 0
┌────────────────────────────────────────┐
│ GLOBAL HEADER (固定 256 bytes)          │
│   [Magic:4][Version:2][FormatVersion:2]│
│   [NumRowGroups:4][Reserved:244]       │
├────────────────────────────────────────┤  ← 256
│ ROW GROUP 0                            │
│   [RG Header: variable]                │
│   [Pages...]                           │
│   [RG Statistics (optional)]           │
│   [RG Footer: variable]                │
├────────────────────────────────────────┤
│ ROW GROUP 1                            │
│   [RG Header][Pages...][Stats][Footer] │
├────────────────────────────────────────┤
│ ...                                    │
├────────────────────────────────────────┤
│ GLOBAL FOOTER (optional, 未来扩展)      │
│   [RG offset table][Reserved]          │
└────────────────────────────────────────┘
```

**关键决策**：
- **省略 Global Footer（V1）**：Manifest 已包含所有 RG 偏移，足够启动定位
- **Global Header 固定 256 bytes**：只存 magic/version/numRG，Schema 移到 Manifest
- **每个 RG 有独立 Header/Footer**：Reader 可以独立解析每个 RG，无需全局信息

---

## 3. Row Group 详细二进制规格

### 3.1 Row Group Header

```
┌────────────────────────────────────────┐
│ RG Header                              │
│   [Magic:4]          = 0x5247_4C41      │  // "RGLA" (Row Group Lance)
│   [Version:2]        = 0x0001           │
│   [Flags:2]          = feature flags    │
│   [NumRows:8]        = RG 内行数        │
│   [NumColumns:4]     = 列数             │
│   [PageSize:4]       = page size hint   │
│   [SchemaOffset:8]   = Schema 在 RG 内的偏移│
│   [SchemaSize:4]     = Schema 字节数     │
│   [DataOffset:8]     = Pages 起始偏移    │
│   [Reserved:32]      = 预留             │
├────────────────────────────────────────┤
│ Schema JSON (variable)                 │
│   [SchemaLen:4][SchemaJSON:N]          │
├────────────────────────────────────────┤
│ Padding to 8-byte alignment            │
└────────────────────────────────────────┘
```

**设计理由**：
- RG Header 自带 Schema，允许未来跨 RG schema evolution（初期版本要求所有 RG Schema 一致）
- `DataOffset` 指向 Pages 区域起始，便于快速跳过 Header
- Header 总大小通常 < 2KB（Schema 不大的情况下），无需固定预留

### 3.2 Row Group Pages

与当前 Page 格式**完全兼容**，不做任何变更：
- `PageHeader: 30 bytes`
- `[Data: CompressedSize]`
- `[NullBitmap: variable]`

这样 Writer 的 page 编码逻辑可以零改动复用。

### 3.3 Row Group Footer

```
┌────────────────────────────────────────┐
│ RG Footer                              │
│   [NumPages:4]                         │
│   [PageIndexList: count + entries]     │
│   [StatsOffset:8] (0 if none)          │
│   [StatsCount:4]                       │
│   [MetadataCount:4][Metadata entries]  │
│   [CRC32:4]                            │
│   [FooterSize:4]  ← 自身大小（不含 padding）│
├────────────────────────────────────────┤
│ Padding to 8-byte alignment            │
└────────────────────────────────────────┘
```

**与当前 Footer 的区别**：
1. **无固定大小**：`FooterSize` 字段自描述，无需 32KB 预留
2. **无 `Version/CreatedAt/ModifiedAt`**：这些信息在 Manifest 中统一管理
3. **PageIndexList 格式不变**：25 bytes/entry

### 3.4 Global Header（固定 256 bytes）

```go
const GlobalHeaderSize = 256

type GlobalHeader struct {
    Magic        uint32   // 0x4C414E43 ("LANC")
    Version      uint16   // File format version = 0x0200 (V2.0)
    Flags        uint16   // Feature flags
    NumRowGroups uint32   // 当前文件中的 Row Group 数量
    Reserved     [244]byte
}
```

**Version 升级策略**：
- V1.x = 当前单文件格式
- V2.0 = append-only Row Group 格式
- Reader 根据 `Version` 决定解析路径

---

## 4. Manifest 扩展设计

### 4.1 新 Manifest 结构

```go
// RowGroupInfo 描述文件中的一个 Row Group
type RowGroupInfo struct {
    ID            int           // 单调递增，从 0 开始
    FileOffset    int64         // RG 在文件中的起始偏移
    Length        int64         // RG 总字节数（Header + Pages + Stats + Footer）
    NumRows       int64         // RG 内行数
    NumColumns    int32         // 列数（冗余校验）
    SchemaHash    [32]byte      // Schema JSON 的 SHA-256（用于快速一致性校验）
    ColumnStats   []*ColumnStatistics // 每列统计信息（从 RG Stats 汇总）
}

// ManifestV2 扩展自现有 Manifest
type ManifestV2 struct {
    Version       int64             // 单调递增版本号
    ParentVersion int64             // 上一个版本（-1 表示初始）
    Timestamp     int64             // Unix 纳秒时间戳
    FormatVersion uint16            // 2 = Row Group 格式
    
    // Schema（全局，所有 RG 共享）
    Schema        *core.Schema
    SchemaJSON    []byte            // 原始 JSON，用于 hash 校验
    
    // Row Groups
    RowGroups     []RowGroupInfo    // 有序列表
    TotalRows     int64             // 所有 RG 行数之和
    TotalColumns  int32             // 列数
    
    // 数据文件
    DataFiles     []string          // ["vectors.lance"]（保持兼容）
    IndexFiles    []string          // HNSW 索引文件路径
    
    // Deletion Vector（全局，合并所有 RG 的删除标记）
    DeletedRows   []uint32          // 全局删除的行号（逻辑行号）
    
    // 元数据
    Metadata      map[string]string // 事务元数据
    Committed     bool              // 是否已提交
}
```

### 4.2 Manifest 持久化格式

**文件命名**：`manifest-<version>.json`
- 例如：`manifest-000042.json`
- 零填充 6 位，保证字典序 = 数值序

**写入流程**：
```
1. 序列化 ManifestV2 为 JSON（pretty=false，减少体积）
2. 写入临时文件：`manifest-<version>.json.vego-tmp.<nonce>`
3. fsync 临时文件
4. close 文件句柄
5. rename 到最终文件名
6. GC 旧版本（保留最近 3 个）
```

**读取流程**：
```
1. 扫描目录，匹配 `manifest-*.json`
2. 按版本号降序排列
3. 尝试解析最新版本
4. 如果最新损坏（不应发生），回退到上一个版本
5. 加载成功后，删除 version < loaded - 3 的旧文件
```

**为什么用 JSON 而不是二进制？**
- Manifest 通常 < 100KB（即使 1000 个 RG，每个 RGInfo ≈ 200 bytes，总共 200KB）
- JSON 便于调试、手动修复、版本控制
- 写入是 tmp+rename，性能不是瓶颈

### 4.3 Deletion Vector 与 Row Group 的关系

当前 DeletionVector 使用**逻辑行号**（从 0 开始的连续整数）。在 Row Group 格式下：

```
逻辑行号 → (RG ID, RG 内行号)
逻辑行号 = sum(RG[0..ID-1].NumRows) + offset_in_RG
```

**映射函数**：
```go
func (m *ManifestV2) LogicalToPhysical(rowIdx int64) (rgID int, offset int64, ok bool) {
    var cumulative int64
    for i, rg := range m.RowGroups {
        if rowIdx < cumulative+rg.NumRows {
            return i, rowIdx - cumulative, true
        }
        cumulative += rg.NumRows
    }
    return -1, -1, false
}
```

DeletionVector 仍全局管理（一个 `[]uint32`），不需要按 RG 拆分。这样保持与现有 DV 代码的兼容性。

---

## 5. Writer 改造方案

### 5.1 两种 Writer 模式

为了控制变更范围，Writer 支持两种模式：

| 模式 | 用途 | 行为 |
|---|---|---|
| **Legacy 模式**（默认） | 兼容现有代码 | 写单文件格式（V1.2），与当前行为一致 |
| **Append 模式** | Row Group 追加 | 写 V2.0 格式，支持追加 RG |

```go
type Writer struct {
    // 现有字段 ...
    
    // Wave 4: Row Group 模式
    formatVersion FormatVersion  // V1_2 or V2_0
    rowGroupMode  bool           // true = append mode
    
    // V2 模式下新增
    globalHeader  *format.GlobalHeader
    rowGroups     []format.RowGroupDescriptor  // 已写入的 RG 描述
}

type WriterOption func(*Writer)

func WithAppendMode() WriterOption {
    return func(w *Writer) {
        w.rowGroupMode = true
        w.formatVersion = format.V2_0
    }
}
```

### 5.2 Append 模式写入流程

**创建新文件（初始）**：
```
1. 打开临时文件（已有 atomic rename 机制复用）
2. 写入 Global Header（256 bytes，NumRowGroups=0）
3. 记录 currentPos = 256
```

**追加 Row Group（flush 新 batch）**：
```
1. 在当前 currentPos 写入 RG Header（含 Schema）
2. 写入 Pages（复用现有 page 编码逻辑）
3. 写入 Statistics（可选）
4. 写入 RG Footer
5. 更新 globalHeader.NumRowGroups++
6. Seek 到 0，重写 Global Header
7. fsync
8. currentPos = 文件末尾
```

**Close（最终化）**：
```
1. 确保所有 RG 已写入
2. 重写 Global Header（最终 NumRowGroups）
3. fsync
4. close 文件
5. atomic rename（tmp → final）
```

### 5.3 关键实现细节

**Global Header 重写**：
- Global Header 固定 256 bytes，重写时不会导致后续数据偏移变化
- 每次追加 RG 后都重写 Global Header（记录最新 NumRowGroups）
- 如果崩溃时 Global Header 未更新，Manifest 仍然正确（以 Manifest 为准）

**RG Footer 定位**：
- 写入 Pages 后记录 `footerOffset = currentPos`
- 写入 Footer 后从 Footer 尾部读取 `FooterSize` 字段
- `RG.Length = footerOffset + footerSize + padding - rgStartOffset`

---

## 6. Reader 改造方案

### 6.1 版本识别与路由

```go
func NewReaderWithVFS(filename string, fs vfs.VFS) (*Reader, error) {
    file, _ := fs.Open(filename)
    
    // 读取前 4 bytes 判断 magic
    var magic uint32
    binary.Read(file, binary.LittleEndian, &magic)
    
    if magic != format.MagicNumber {
        return nil, ErrInvalidFile
    }
    
    // 读取 version
    var version uint16
    binary.Read(file, binary.LittleEndian, &version)
    
    switch {
    case version < 0x0200:
        return newReaderV1(file, version)
    case version >= 0x0200:
        return newReaderV2(file, version)
    }
}
```

### 6.2 V2 Reader 结构

```go
type ReaderV2 struct {
    file       vfs.File
    fs         vfs.VFS
    globalHdr  *format.GlobalHeader
    
    // Row Groups（从 Manifest 或文件扫描加载）
    rowGroups  []*RowGroupReader  // 每个 RG 一个子 Reader
    
    // 全局信息
    schema     *core.Schema
    totalRows  int64
    
    // 缓存
    blockCache *format.BlockCache
    cacheKey   string
    
    // Range coalescing（复用 Wave 2）
    coalesceGap  int64
    maxMergeSize int64
    
    // Metadata caching（复用 Wave 3）
    footerOnce   sync.Once
    footerErr    error
}

type RowGroupReader struct {
    file       vfs.File      // 同一个文件句柄（共享）
    offset     int64         // RG 起始偏移
    length     int64         // RG 总长度
    header     *format.RGHeader
    footer     *format.RGFooter
    stats      *format.StatisticsList
    
    // 缓存的 page index
    pageIndex  []format.PageIndex
}
```

### 6.3 跨 RG 读取

**ReadRows(start, end)**：
```go
func (r *ReaderV2) ReadRows(start, end int64) (*core.RecordBatch, error) {
    // 1. 确定跨越了哪些 RG
    ranges := r.findRGRanges(start, end)
    
    // 2. 对每个 RG 读取对应行范围
    var batches []*core.RecordBatch
    for _, rr := range ranges {
        rgStart := max(start - rr.rgStartRow, 0)
        rgEnd := min(end - rr.rgStartRow, rr.rg.NumRows)
        batch, err := rr.rgReader.ReadRows(int(rgStart), int(rgEnd))
        batches = append(batches, batch)
    }
    
    // 3. 拼接（按列合并 arrays）
    return concatenateBatches(batches, r.schema)
}
```

**ReadColumn(columnIndex)**：
```go
func (r *ReaderV2) ReadColumn(columnIndex int32) (core.Array, error) {
    var arrays []core.Array
    for _, rg := range r.rowGroups {
        arr, err := rg.ReadColumn(columnIndex)
        arrays = append(arrays, arr)
    }
    return concatenateArrays(arrays)
}
```

### 6.4 RowGroupReader 内部

`RowGroupReader` 内部逻辑与当前 V1 Reader 几乎一致：
- 从 `rgOffset + header.DataOffset` 开始读 Pages
- 用 `footer.PageIndexList` 定位 page
- `coalesceRanges` / `readPagesAsync` 直接复用（Wave 2）

区别只是所有 offset 都是**相对于 RG 起始**的，需要加上 `rg.offset`。

---

## 7. DocumentStorage 改造方案

### 7.1 核心变更：`flush()` 从重写改为追加

**当前流程（V1）**：
```
readAllDocuments() → append(buffer) → rewriteStorage(allDocs)
```

**新流程（V2）**：
```
encodeBufferToRowGroup(buffer) → appendToFile()
→ updateManifest() → saveManifest()
```

**伪代码**：
```go
func (s *DocumentStorage) flush() error {
    if len(s.writeBuffer) == 0 && !s.deletionVectorDirty {
        return nil
    }
    
    // 1. 将 buffer 编码为 RecordBatch
    batch := s.encodeBufferToRecordBatch()
    
    // 2. 追加 Row Group 到 column 文件
    rgOffset, rgLen, err := s.appendRowGroup(batch)
    if err != nil {
        return err
    }
    
    // 3. 更新 Manifest
    s.manifest.RowGroups = append(s.manifest.RowGroups, RowGroupInfo{
        ID:         len(s.manifest.RowGroups),
        FileOffset: rgOffset,
        Length:     rgLen,
        NumRows:    int64(batch.NumRows()),
        // ... ColumnStats from batch stats
    })
    s.manifest.TotalRows += int64(batch.NumRows())
    s.manifest.Version++
    s.manifest.Timestamp = time.Now().UnixNano()
    
    // 4. 保存 Manifest（atomic）
    if err := s.saveManifest(); err != nil {
        // 如果 manifest 保存失败，column 文件已经多出一个"孤儿 RG"
        // 恢复时以 manifest 为准，会忽略这个 RG
        return err
    }
    
    // 5. 清理 buffer
    s.writeBuffer = s.writeBuffer[:0]
    s.deletionVectorDirty = false
    
    // 6. BlockCache：新 RG 的 page 可以预热，旧 RG 的缓存仍然有效
    return nil
}
```

### 7.2 初始化加载

```go
func NewDocumentStorageWithVFS(path string, dimension int, fs vfs.VFS) (*DocumentStorage, error) {
    s := &DocumentStorage{...}
    
    // 1. 尝试加载最新 Manifest
    manifest, err := loadLatestManifest(path, fs)
    if err == nil {
        s.manifest = manifest
        s.formatVersion = manifest.FormatVersion
        
        // 2. 根据 Manifest 打开 column 文件
        columnFile := filepath.Join(path, "vectors.lance")
        s.reader, err = column.NewReaderWithVFS(columnFile, fs)
        
        // 3. 加载 DeletionVector
        s.loadDeletionVector()
        
        return s, nil
    }
    
    // 4. 没有 Manifest → 可能是旧格式（V1）或新库
    // 尝试加载 V1 格式，如果成功则标记需要迁移
    if isV1File(path) {
        s.formatVersion = format.V1_2
        s.reader, _ = column.NewReaderWithVFS(...)
        // ... 现有逻辑
    }
    
    return s, nil
}
```

### 7.3 首次写入（空库 → 新文件）

```go
func (s *DocumentStorage) firstWrite(batch *core.RecordBatch) error {
    // 1. 创建新文件，使用 V2 格式
    filename := filepath.Join(s.path, "vectors.lance")
    writer, err := column.NewWriterWithVFS(filename, s.fs, column.WithAppendMode())
    
    // 2. 写入第一个 Row Group
    err = writer.WriteRecordBatch(batch)
    // ... 添加 RowID
    err = writer.Close()
    
    // 3. 创建初始 Manifest
    s.manifest = &ManifestV2{
        Version:       1,
        FormatVersion: format.V2_0,
        Schema:        batch.Schema(),
        RowGroups:     []RowGroupInfo{{ID: 0, ...}},
        TotalRows:     int64(batch.NumRows()),
    }
    
    // 4. 保存 Manifest
    return s.saveManifest()
}
```

---

## 8. Compaction 改造方案

### 8.1 当前 Compaction（V1）

```
读取所有有效文档 → 重建 HNSW → 重写整个 column 文件 → 清空 DV
```

### 8.2 新 Compaction（V2）

```
1. 选择需要合并的 Row Groups（例如：小文件合并、删除率高的 RG）
2. 读取选中的 RG 的有效行（跳过 DV 标记的删除行）
3. 编码为新的合并后 RG
4. 追加到 column 文件末尾（或写入新临时文件）
5. 更新 Manifest：移除旧 RG，添加新 RG
6. 原子保存 Manifest
7. 旧 RG 的数据变成"孤儿"，等待后台/下次启动时截断清理
```

**为什么不在原文件截断？**
- 截断文件中间的数据需要重写整个文件（回到 O(N)）
- 更简单的策略：让旧 RG 成为"孤儿"，通过 `recoverFromManifest` 在启动时截断到 manifest 记录的最后一个 RG 末尾

### 8.3 孤儿数据清理

```go
func (s *DocumentStorage) cleanupOrphanData() error {
    if len(s.manifest.RowGroups) == 0 {
        return nil
    }
    
    // 最后一个有效 RG 的结束偏移
    lastRG := s.manifest.RowGroups[len(s.manifest.RowGroups)-1]
    expectedEnd := lastRG.FileOffset + lastRG.Length
    
    // 获取文件实际大小
    info, _ := s.fs.Stat(columnFile)
    
    if info.Size() > expectedEnd {
        // 截断到 expectedEnd
        return s.fs.Truncate(columnFile, expectedEnd)
    }
    return nil
}
```

**触发时机**：
- 启动时（`loadLatestManifest` 后）
- Compaction 完成后
- 定期后台任务（低优先级）

---

## 9. 兼容性策略

### 9.1 版本矩阵

| Reader\Writer | V1.2 (旧) | V2.0 (新) |
|---|---|---|
| V1.2 Reader | ✅ 读写 | ❌ 不能读 V2 文件 |
| V2.0 Reader | ✅ 能读 V1 文件（向后兼容） | ✅ 读写 |

### 9.2 V1 → V2 迁移

**方案 A（推荐）：惰性迁移**
- 现有 V1 文件继续用 V1 Reader/Writer 读写
- 当触发 `Compact()` 或显式 `Migrate()` 时，重写为 V2 格式
- 新库默认创建 V2 格式

**方案 B：自动迁移**
- 启动时检测到 V1 文件，自动重写为 V2
- 风险：大文件迁移耗时不可控，启动延迟暴增

**选择方案 A**，理由：
- 不给用户意外惊喜
- 大文件迁移应在用户知情的情况下触发（如显式 Compact）
- 降低上线风险

### 9.3 降级保护

```go
// Writer 选项：允许用户强制使用 V1 格式
type DocumentStorageOption func(*DocumentStorage)

func WithFormatVersion(v FormatVersion) DocumentStorageOption {
    return func(s *DocumentStorage) {
        s.formatVersion = v
    }
}
```

---

## 10. 崩溃恢复流程

### 10.1 正常写入顺序

```
1. 追加新 RG 到 column 文件
2. fsync column 文件
3. 更新 Manifest 内存对象
4. 写 Manifest 临时文件
5. fsync Manifest 临时文件
6. rename Manifest 临时文件 → 正式文件
7. GC 旧 Manifest 版本
```

### 10.2 各阶段崩溃场景

| 崩溃时机 | 状态 | 恢复行为 |
|---|---|---|
| 步骤 1-2 之间 | Column 文件多出孤儿 RG | 启动时加载最新 Manifest，按 Manifest 截断文件 |
| 步骤 3-5 之间 | Column 文件有 RG，但 Manifest 未更新 | 孤儿 RG 被忽略（Manifest 是真理来源） |
| 步骤 6 之后 | 新 Manifest 已生效 | 正常启动，旧 Manifest 被 GC |
| Manifest rename 中途 | 可能同时存在新旧 Manifest | 按版本号取最大，如果最新解析失败回退 |

### 10.3 启动恢复函数

```go
func recoverStorage(path string, fs vfs.VFS) (*ManifestV2, error) {
    // 1. 加载最新有效 Manifest
    manifest, err := loadLatestManifest(path, fs)
    if err != nil {
        return nil, fmt.Errorf("no valid manifest found: %w", err)
    }
    
    // 2. 打开 column 文件
    columnFile := filepath.Join(path, "vectors.lance")
    info, err := fs.Stat(columnFile)
    if err != nil {
        return nil, err
    }
    
    // 3. 验证 column 文件格式版本
    file, _ := fs.Open(columnFile)
    var magic uint32
    binary.Read(file, binary.LittleEndian, &magic)
    if magic != format.MagicNumber {
        return nil, ErrInvalidColumnFile
    }
    
    var version uint16
    binary.Read(file, binary.LittleEndian, &version)
    file.Close()
    
    if version >= 0x0200 {
        // V2: 按 Manifest 截断
        if len(manifest.RowGroups) > 0 {
            lastRG := manifest.RowGroups[len(manifest.RowGroups)-1]
            expectedEnd := lastRG.FileOffset + lastRG.Length
            if info.Size() > expectedEnd {
                log.Warnf("Truncating orphan data: file=%d, expected=%d", info.Size(), expectedEnd)
                fs.Truncate(columnFile, expectedEnd)
            }
        }
    }
    
    // 4. 加载 DeletionVector
    // ...
    
    return manifest, nil
}
```

---

## 11. 测试策略

### 11.1 单元测试

| 测试 | 范围 | 验证点 |
|---|---|---|
| `TestRGHeaderRoundTrip` | format 包 | RG Header 序列化/反序列化正确 |
| `TestRGFooterRoundTrip` | format 包 | RG Footer 自描述大小正确 |
| `TestGlobalHeaderV2` | format 包 | 256 bytes 固定，版本识别正确 |
| `TestWriterAppendMode` | column 包 | 多次 WriteRecordBatch 产生多个 RG |
| `TestWriterCloseFinalizes` | column 包 | Close 后 Global Header NumRG 正确 |
| `TestReaderV1BackwardCompat` | column 包 | V2 Reader 能正确读取 V1 文件 |
| `TestReaderV2MultiRG` | column 包 | 跨 RG 读取行、列结果正确 |
| `TestManifestRoundTrip` | format 包 | JSON 序列化/反序列化 |
| `TestManifestGC` | format 包 | 旧版本正确清理，保留最近 3 个 |
| `TestManifestCrashRecovery` | format 包 | 模拟损坏/缺失 manifest 的回退 |

### 11.2 集成测试

| 测试 | 范围 | 验证点 |
|---|---|---|
| `TestStorageAppendFlush` | vego 包 | 多次 flush 后文件大小增长符合预期 |
| `TestStorageReadAfterAppend` | vego 包 | 追加后读取所有行数据一致 |
| `TestStorageCompaction` | vego 包 | Compact 后文件变小，数据完整 |
| `TestStorageCrashRecovery` | vego 包 | kill -9 模拟，恢复后数据一致 |
| `TestStorageV1ToV2Migration` | vego 包 | V1 文件 Compact 后变为 V2 |

### 11.3 性能基准

| 基准 | 目标 | 对比基线 |
|---|---|---|
| `BenchmarkFlush1MVectors` | < 30s | 当前 V1 的 `BenchmarkFlush` |
| `BenchmarkRead1MVectors` | 与 V1 持平或更好 | 当前 V1 的 `BenchmarkRead` |
| `BenchmarkSequentialAppend` | 线性增长，无突变 | 验证 append 无 O(N) 行为 |

---

## 12. 实施计划（分 4 个阶段）

### Phase A: Format 基础设施（3-4 天）

1. **format 包扩展**
   - 新增 `GlobalHeader`、`RGHeader`、`RGFooter` 结构体
   - 新增 `FormatVersion` 常量（V1_2, V2_0）
   - 序列化/反序列化方法 + 单元测试

2. **Manifest 扩展**
   - 扩展 `Manifest` → `ManifestV2`（或新建 `manifest_v2.go`）
   - JSON 序列化、文件命名、版本管理
   - `loadLatestManifest`、`saveManifest` 函数
   - GC 逻辑

3. **Reader 版本路由**
   - `NewReader` 根据 magic + version 分发到 V1/V2
   - V2 Reader 骨架（能识别文件，还不能读数据）

### Phase B: Writer Append 模式（3-4 天）

1. **Writer 改造**
   - `WithAppendMode()` 选项
   - Append 模式写入流程（Global Header + RG Header + Pages + RG Footer）
   - 重写 Global Header（更新 NumRowGroups）

2. **Writer 测试**
   - 多次 `WriteRecordBatch` 产生多 RG
   - 关闭后文件格式验证
   - 与 V1 Writer 的兼容性对比

### Phase C: Reader V2 实现（3-4 天）

1. **RowGroupReader**
   - 从文件偏移加载 RG Header/Footer
   - 复用现有 page 读取逻辑（offset 加上 RG base）

2. **跨 RG 读取**
   - `ReadRows`、`ReadColumn`、`ReadRowAt`
   - 行号到 RG 的映射
   - Array 拼接（`concatenateArrays`）

3. **Reader 测试**
   - 单 RG 读取（与 V1 结果一致）
   - 多 RG 读取（边界行号正确）
   - V1 向后兼容

### Phase D: Storage 集成与验收（3-4 天）

1. **DocumentStorage 改造**
   - `flush()` 追加模式
   - Manifest 加载/保存集成
   - 启动恢复（`recoverStorage`）

2. **Compaction 改造**
   - 多 RG 合并策略
   - 孤儿数据清理

3. **性能基准**
   - 1M 向量追加写入
   - 与 V1 对比

4. **集成测试 + race detector**

**总工期估算**：12-16 天（约 3 周），与文档中的 3-4 周一致。

---

## 13. 需要确认的关键决策

在开工前，请确认以下几个会影响实现方向的选择：

1. **Global Footer 是否完全省略（V1）？**  
   建议省略，用 Manifest 管理所有 RG 偏移。如果未来 RG 数量达到 1000+ 且启动延迟敏感，再引入 Global Footer 作为优化。

2. **Manifest 格式：JSON 还是二进制？**  
   建议 JSON（便于调试），但如果有性能顾虑可以改为二进制 + proto/msgpack。

3. **Schema 存储位置：每个 RG Header 一份，还是只在 Manifest 存一份？**  
   建议 RG Header 自带 Schema（允许未来 evolution），但初期强制所有 RG Schema 一致，校验不一致时报错。

4. **V1 → V2 迁移策略：惰性（Compact 时触发）还是自动（启动时触发）？**  
   建议惰性迁移，避免大文件启动延迟。

5. **是否在本迭代中实现 Compaction？**  
   Compaction 可以延后到 Wave 4 后续小版本，先保证 append + read + manifest 稳定。

---

*本设计文档基于 Vego v0.1.5 的实际代码结构编写，所有 struct/field/method 名称均参照现有命名风格。*
