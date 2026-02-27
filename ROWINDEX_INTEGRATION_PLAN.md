# DocumentStorage 集成 RowIndex 实施计划

> 本文档规划了将 RowIndex 功能集成到 DocumentStorage 的完整实施路径，实现从 O(N) 到 O(1) 的单条文档查询优化。

---

## 一、背景与目标

### 1.1 当前问题

当前 `DocumentStorage.Get(id)` 流程：
1. 读取整个数据文件 (`readAllDocuments`)
2. 遍历所有行查找匹配 ID
3. 时间复杂度 **O(N)**，数据量增大时性能急剧下降

### 1.2 目标方案

使用 RowIndex 后：
1. 加载 RowIndex（只需一次，可缓存）
2. `Lookup(id)` → rowIndex（O(1) 哈希查找）
3. 直接读取指定行数据
4. 时间复杂度 **O(1)**

### 1.3 RowIndex 简介

- **存储格式**：独立的 Page（PageTypeIndex），V1.1+ 文件格式支持
- **数据结构**：哈希表（IDHash → RowIndex）
- **位置**：Footer 中记录偏移量，文件末尾写入

---

## 二、实施阶段

### 🔴 Phase 1: 写入端改造

**目标**：使用 `RowIndexWriter` 替代普通 `Writer`，写入数据时同步构建 RowIndex。

| 任务 | 目标文件 | 详细说明 |
|------|----------|----------|
| 1.1 添加版本策略字段 | `vego/storage.go` | 在 `DocumentStorage` 结构体添加 `version format.VersionPolicy` 字段，默认 `format.V1_2` |
| 1.2 修改构造函数 | `vego/storage.go` | `NewDocumentStorage` 支持可选版本参数，向后兼容（不传则默认 V1_2） |
| 1.3 替换 Writer | `vego/storage.go:writeColumnStorage` | 将 `column.NewWriter` 替换为 `column.NewRowIndexWriter(dataFile, schema, s.version, s.factory)` |
| 1.4 添加 ID 映射 | `vego/storage.go:writeColumnStorage` | 写入 RecordBatch 后，遍历 docs 调用 `writer.AddRowID(doc.ID, int64(i))` |
| 1.5 关闭写入 | `vego/storage.go:writeColumnStorage` | 调用 `writer.Close()`，自动写入 RowIndex Page 和 Footer 元数据 |

**关键代码示例**：

```go
func (s *DocumentStorage) writeColumnStorage(docs []*Document) error {
    // 使用 RowIndexWriter 替代普通 Writer
    writer, err := column.NewRowIndexWriter(dataFile, schema, s.version, s.factory)
    if err != nil {
        return err
    }
    
    // 写入数据（RowIndexWriter 继承 Writer 的所有功能）
    if err := writer.WriteRecordBatch(batch); err != nil {
        return err
    }
    
    // 添加 RowIndex 映射
    for i, doc := range docs {
        if err := writer.AddRowID(doc.ID, int64(i)); err != nil {
            return err
        }
    }
    
    // Close 自动写入 RowIndex Page
    return writer.Close()
}
```

**验收标准**：
- [ ] 新写入的文件 Footer 中 `HasRowIndex()` 返回 true
- [ ] 文件末尾存在独立的 RowIndex Page
- [ ] V1.0/V1.1/V1.2 版本选择正常工作

---

### 🟡 Phase 2: 读取端改造

**目标**：支持使用 `RowIndexReader` 进行 O(1) 单条查询。

| 任务 | 目标文件 | 详细说明 |
|------|----------|----------|
| 2.1 文件版本检测 | `vego/storage.go` | 添加 `detectFileVersion()` 方法，读取 Footer 判断文件版本 |
| 2.2 条件使用 RowIndexReader | `vego/storage.go:readAllDocuments` | 检测到有 RowIndex 的文件使用 `NewRowIndexReaderWithCache` |
| 2.3 新增单条读取方法 | `vego/storage.go` | 添加 `readDocumentByID(id string) (*Document, error)` 方法 |
| 2.4 随机读取接口 | `storage/column/reader.go` | 扩展 Reader 支持 `ReadRowAt(index int64)` 或类似接口（如需） |
| 2.5 优化 Get 方法 | `vego/storage.go:Get` | 优先使用 RowIndex 路径，fallback 到全表扫描 |

**关键代码示例**：

```go
func (s *DocumentStorage) readDocumentByID(id string) (*Document, error) {
    dataFile := filepath.Join(s.path, dataFileName)
    
    // 使用 RowIndexReader
    reader, err := column.NewRowIndexReaderWithCache(dataFile, s.blockCache)
    if err != nil {
        return nil, err
    }
    defer reader.Close()
    
    // O(1) 查找行号
    rowIdx, err := reader.LookupRowID(id)
    if err != nil {
        return nil, ErrDocumentNotFound
    }
    
    // 读取指定行（需要支持随机读取）
    // batch, err := reader.ReadRecordBatchAt(rowIdx)
    // ...
}

func (s *DocumentStorage) Get(id string) (*Document, error) {
    // 优先检查 buffer（内存中最新数据）
    // ...
    
    // 尝试使用 RowIndex 快速路径
    if s.hasRowIndex() {
        return s.readDocumentByID(id)
    }
    
    // Fallback：全表扫描（兼容旧文件）
    return s.getByFullScan(id)
}
```

**验收标准**：
- [ ] V1.1+ 文件使用 RowIndex 进行 O(1) 查询
- [ ] V1.0 文件自动降级到全表扫描
- [ ] 查询结果正确性验证通过

---

### 🟢 Phase 3: 向后兼容 & 版本管理

**目标**：确保新旧格式无缝过渡，自动升级旧文件。

| 任务 | 目标文件 | 详细说明 |
|------|----------|----------|
| 3.1 版本检测工具 | `vego/storage.go` | 添加 `getFileVersion()` 方法，读取 Footer 获取版本信息 |
| 3.2 能力检测 | `vego/storage.go` | 添加 `supportsRowIndex()` 方法，判断是否可以使用 RowIndex |
| 3.3 读取降级 | `vego/storage.go` | V1.0 文件（无 RowIndex）自动使用原有全表扫描逻辑 |
| 3.4 写入升级 | `vego/storage.go:flush` | 旧版本文件重写时自动升级到配置的版本（如 V1_2） |
| 3.5 版本信息统计 | `vego/storage.go:Stats` | StorageStats 添加 FormatVersion 字段 |

**关键代码示例**：

```go
// 检测文件是否支持 RowIndex
func (s *DocumentStorage) supportsRowIndex() bool {
    // 检查内存状态
    if s.version.HasFeature(format.FeatureRowIndex) {
        // 检查文件实际版本
        fileVer, err := s.getFileVersion()
        if err == nil && fileVer.HasFeature(format.FeatureRowIndex) {
            return true
        }
    }
    return false
}

// flush 时自动升级版本
func (s *DocumentStorage) flush() error {
    // 读取现有数据（兼容任何版本）
    existingDocs, _ := s.readAllDocuments()
    
    // 重写时使用配置的版本（可能升级）
    allDocs := append(existingDocs, s.writeBuffer...)
    return s.rewriteStorage(allDocs) // 使用 s.version
}
```

**验收标准**：
- [ ] V1.0 文件可正常读取
- [ ] V1.0 文件 flush 后升级为 V1.2
- [ ] V1.1 文件可正常使用 RowIndex，但不启用 BlockCache

---

### 🔵 Phase 4: 性能优化

**目标**：充分发挥 RowIndex + BlockCache 的性能优势。

| 任务 | 目标文件 | 详细说明 |
|------|----------|----------|
| 4.1 RowIndex 缓存 | `vego/storage.go` | `RowIndexReader` 已支持 BlockCache 缓存 RowIndex Page |
| 4.2 延迟加载 | `vego/storage.go` | RowIndex 按需加载（首次 Get 时），而非启动时加载 |
| 4.3 缓存预热 | `vego/storage.go` | 可选：启动时调用 `rowIndexReader.WarmupCache()` |
| 4.4 并发优化 | `vego/storage.go` | 考虑读写锁分离（RowIndex 读多写少） |
| 4.5 性能测试 | `vego/storage_bench_test.go` | 添加基准测试对比 O(N) vs O(1) 查询性能 |

**性能优化代码示例**：

```go
// 延迟加载 RowIndex
func (s *DocumentStorage) getRowIndexReader() (*column.RowIndexReader, error) {
    if s.rowIndexReader != nil {
        return s.rowIndexReader, nil
    }
    
    reader, err := column.NewRowIndexReaderWithCache(dataFile, s.blockCache)
    if err != nil {
        return nil, err
    }
    
    s.rowIndexReader = reader
    return reader, nil
}

// 缓存预热（可选）
func (s *DocumentStorage) WarmupCache() error {
    reader, err := s.getRowIndexReader()
    if err != nil {
        return err
    }
    return reader.WarmupCache()
}
```

**验收标准**：
- [ ] 大数据集下 Get 操作性能提升 10x+
- [ ] RowIndex Page 被正确缓存
- [ ] 内存使用合理，无泄漏

---

## 三、关键 API 参考

### 3.1 写入相关

```go
// 创建带 RowIndex 的 Writer
func column.NewRowIndexWriter(
    filename string, 
    schema *arrow.Schema, 
    version format.VersionPolicy,  // V1_1 或 V1_2
    factory *encoding.EncoderFactory,
) (*RowIndexWriter, error)

// 添加 ID → Row 映射
func (w *RowIndexWriter) AddRowID(docID string, rowIndex int64) error

// 版本策略
format.V1_0 // 基础列式存储
format.V1_1 // + RowIndex
format.V1_2 // + BlockCache
```

### 3.2 读取相关

```go
// 创建带 RowIndex 的 Reader
func column.NewRowIndexReader(filename string) (*RowIndexReader, error)
func column.NewRowIndexReaderWithCache(filename string, cache *format.BlockCache) (*RowIndexReader, error)

// O(1) 查找行号
func (r *RowIndexReader) LookupRowID(docID string) (int64, error)

// 检测能力
func (r *RowIndexReader) HasRowIndex() bool
func (r *RowIndexReader) GetVersion() format.VersionPolicy
```

### 3.3 版本相关

```go
// 检测特性支持
func (vp VersionPolicy) HasFeature(feature uint32) bool

// 关键特性标志
format.FeatureRowIndex   // V1.1
format.FeatureBlockCache // V1.2
```

---

## 四、测试计划

### 4.1 单元测试

| 测试项 | 描述 |
|--------|------|
| `TestDocumentStorageRowIndexWrite` | 验证 V1.2 文件正确写入 RowIndex |
| `TestDocumentStorageRowIndexRead` | 验证通过 RowIndex O(1) 读取 |
| `TestDocumentStorageV10Compatibility` | 验证 V1.0 文件读取兼容性 |
| `TestDocumentStorageVersionUpgrade` | 验证 V1.0 → V1.2 升级 |

### 4.2 集成测试

| 测试项 | 描述 |
|--------|------|
| `TestRowIndexWithBlockCache` | RowIndex + BlockCache 联合工作 |
| `TestSharedCacheWithRowIndex` | 多 Storage 共享缓存 + RowIndex |
| `TestConcurrentRowIndexAccess` | 并发读写 RowIndex 文件 |

### 4.3 性能测试

| 测试项 | 描述 |
|--------|------|
| `BenchmarkGetWithRowIndex` | O(1) 查询性能基准 |
| `BenchmarkGetWithoutRowIndex` | O(N) 查询性能基准（对比） |
| `BenchmarkRowIndexMemory` | 内存占用测试 |

---

## 五、风险评估

| 风险 | 影响 | 缓解措施 |
|------|------|----------|
| 随机读取接口缺失 | 高 | 先验证 column.Reader 是否支持 ReadRowAt，如不支持需先实现 |
| 文件格式不兼容 | 高 | 充分测试 V1.0 文件读取兼容性 |
| 性能不达预期 | 中 | 预留优化空间（缓存、预加载等） |
| 内存泄漏 | 中 | 确保 Reader/Writer 正确 Close |

---

## 六、实施顺序建议

```
Week 1: Phase 1（写入端）
  ├── 添加 version 字段
  ├── 替换为 RowIndexWriter
  └── 验证写入的 RowIndex 可读取

Week 2: Phase 2（读取端）+ Phase 3（兼容）
  ├── 实现 RowIndexReader 读取
  ├── 版本检测与降级逻辑
  └── 新旧格式互操作测试

Week 3: Phase 4（优化）+ 测试完善
  ├── 性能优化
  ├── 基准测试
  └── 集成测试
```

---

## 七、相关文件

- `vego/storage.go` - 主要改造文件
- `vego/storage_test.go` - 测试文件
- `storage/column/rowindex_writer.go` - RowIndexWriter 实现
- `storage/column/rowindex_reader.go` - RowIndexReader 实现
- `storage/format/rowindex.go` - RowIndex 数据结构
- `storage/format/version.go` - 版本策略定义

---

*文档创建时间：2026-02-27*  
*关联 Issue: RowIndex Integration for DocumentStorage*
