# Vego 存储文件版本管理

## 1. 概述

本文档定义了 Vego 的 Lance 兼容列式存储格式的文件版本管理方案。它提供了前向/后向兼容性、格式演进和优雅降级的框架。

### 目标

- **后向兼容性**：新版本的读取器可以读取旧版本的文件格式
- **前向兼容性检查**：旧版本的读取器可以拒绝不兼容的新格式
- **特性检测**：运行时检测文件能力
- **优雅降级**：当特性不可用时提供回退策略
- **清晰的迁移路径**：格式演进的结构化方法

### 非目标（推迟）

- 原地文件迁移（需要重写）
- 跨主版本兼容性（V1.x 无法读取 V2.x）
- 自动格式转换（仅显式迁移）

---

## 2. 当前状态

### 2.1 现有版本字段

| 组件 | 类型 | 当前值 | 描述 |
|------|------|--------|------|
| `Header.Version` | uint16 | 1 | 文件格式版本 |
| `Footer.Version` | uint16 | 1 | 冗余验证 |
| `Header.Flags` | uint16 | 位掩码 | 文件级特性标志 |

### 2.2 当前标志（文件级）

```go
const (
    FlagCompressed HeaderFlags = 1 << iota // 数据已压缩
    FlagEncrypted                          // 数据已加密
    FlagIndexed                            // 文件有索引
    FlagVersioned                          // 文件有版本元数据
)
```

**说明**：`Header.Flags` 表示**单个文件**的特性状态（该文件是否压缩、加密等），与格式版本无关。

### 2.3 当前验证

```go
func ValidateVersion(version uint16) error {
    if version < MinSupportedVersion { // 1
        return ErrVersionTooOld
    }
    if version > CurrentVersion { // 1
        return ErrVersionTooNew
    }
    return nil
}
```

---

## 3. 版本策略

### 3.1 版本编码

版本编码为 `uint16`：`(Major << 8) | Minor`

| 版本 | 十六进制 | 主版本 | 次版本 | 描述 |
|------|---------|--------|--------|------|
| 0x0100 | 256 | 1 | 0 | 初始版本（当前） |
| 0x0101 | 257 | 1 | 1 | + 行索引 |
| 0x0102 | 258 | 1 | 2 | + 块缓存元数据 |
| 0x0200 | 512 | 2 | 0 | 未来主版本修订 |

### 3.2 特性标志（格式级）

```go
const (
    FeatureBasicColumnar uint32 = 1 << iota
    FeatureZstdCompression
    FeatureDictionaryEncoding
    FeatureRLE
    FeatureBitPacking
    FeatureRowIndex        // Phase 1 第 3-4 周
    FeatureBlockCache      // Phase 1 第 5-6 周
    FeatureAsyncIO         // Phase 2
    FeatureFullZip         // Phase 3
    FeatureChecksum        // 每页 CRC32
    FeatureEncryption      // AES 加密
)
```

**Flags vs FeatureFlags 区别**：

| 维度 | `Header.Flags` | `FeatureFlags` |
|------|----------------|----------------|
| **层级** | 文件级 | 格式级 |
| **含义** | 该文件是否使用某特性 | 该版本是否支持某特性 |
| **示例** | `FlagCompressed` = 该文件用了压缩 | `FeatureRowIndex` = V1.1 支持行索引 |
| **运行时** | 决定如何解析该文件 | 决定能否读取该文件 |

### 3.3 版本策略定义

```go
type VersionPolicy struct {
    MajorVersion uint8
    MinorVersion uint8
    FeatureFlags uint32  // 该版本支持的特性集合
}

// 编码为 uint16
func (vp VersionPolicy) Encoded() uint16 {
    return (uint16(vp.MajorVersion) << 8) | uint16(vp.MinorVersion)
}

// 从字符串解析 "1.1" → VersionPolicy{1, 1, ...}
func ParseVersion(s string) (VersionPolicy, error)

// 预定义版本
var (
    V1_0 = VersionPolicy{
        MajorVersion: 1,
        MinorVersion: 0,
        FeatureFlags: FeatureBasicColumnar | FeatureZstdCompression,
    }
    
    V1_1 = VersionPolicy{
        MajorVersion: 1,
        MinorVersion: 1,
        FeatureFlags: V1_0.FeatureFlags | FeatureRowIndex,
    }
    
    V1_2 = VersionPolicy{
        MajorVersion: 1,
        MinorVersion: 2,
        FeatureFlags: V1_1.FeatureFlags | FeatureBlockCache,
    }
    
    // 当前实现支持的最新版本
    CurrentVersion = V1_2
    
    // 支持读取的最低版本
    MinReadableVersion = V1_0
)

// 兼容性检查方法（替代原字段）
func (vp VersionPolicy) CanRead(other VersionPolicy) bool {
    // 同主版本，且不低于对方版本
    return vp.MajorVersion == other.MajorVersion && 
           vp.MinorVersion >= other.MinorVersion
}

func (vp VersionPolicy) CanBeReadBy(other VersionPolicy) bool {
    return other.CanRead(vp)
}
```

---

## 4. 文件格式设计

### 4.1 Header 保持不变（向后兼容）

```
┌─────────────────────────────────────────────────────────────┐
│                      Header（可变大小）                       │
├─────────────────────────────────────────────────────────────┤
│ 偏移量 │ 大小 │ 字段                                         │
├────────┼──────┼─────────────────────────────────────────────┤
│   0    │  4   │ Magic (0x4C414E43)                          │
│   4    │  2   │ Version（编码为 uint16）                     │
│   6    │  2   │ Flags（文件级特性，如压缩、加密）             │
│   8    │  8   │ NumRows (int64)                             │
│  16    │  4   │ NumColumns (int32)                          │
│  20    │  4   │ PageSize (int32)                            │
│  24    │  32  │ Reserved（保留，不使用）                     │
│  56    │  4   │ SchemaLength (int32)                        │
│  60    │  var │ SchemaJSON（JSON 编码的 Arrow schema）       │
└─────────────────────────────────────────────────────────────┘
```

**关键决策**：Header 结构**保持不变**，`Reserved [32]byte` 保持未使用状态，确保与现有 V1.0 文件完全兼容。

### 4.2 Footer 扩展（存储版本元数据）

```
┌─────────────────────────────────────────────────────────────┐
│                      Footer（32KB 固定）                     │
├─────────────────────────────────────────────────────────────┤
│ 偏移量 │ 大小 │ 字段                                         │
├────────┼──────┼─────────────────────────────────────────────┤
│   0    │  2   │ Version (uint16) - 与 Header 一致            │
│   2    │  4   │ NumPages (int32)                            │
│   6    │  8   │ CreatedAt (int64)                           │
│  14    │  8   │ ModifiedAt (int64)                          │
│  22    │  var │ PageIndexList                               │
│  var   │  var │ Metadata (map[string]string)                 │
│  ...   │  4   │ Checksum (uint32)                           │
└─────────────────────────────────────────────────────────────┘
```

**Metadata 扩展（V1.1+）**：

Footer 的 `Metadata` 字段存储格式版本信息：

```go
// V1.0 文件（无额外元数据）
metadata := map[string]string{
    // 空或仅有用户自定义元数据
}

// V1.1 文件（含 RowIndex）
metadata := map[string]string{
    "vego.format.version":    "1.1",           // 格式版本（冗余存储）
    "vego.format.features":   "0x00000042",    // FeatureFlags 十六进制
    "vego.rowindex.offset":   "123456",        // RowIndex Page 偏移
    "vego.rowindex.size":     "4096",          // RowIndex Page 大小
    "vego.rowindex.checksum": "0xA1B2C3D4",    // RowIndex 校验和
}

// V1.2 文件（含 BlockCache 提示）
metadata := map[string]string{
    "vego.format.version":     "1.2",
    "vego.format.features":    "0x000000C2",
    "vego.rowindex.offset":    "123456",
    // ...
    "vego.blockcache.enabled": "true",
    "vego.blockcache.block_size": "65536",
}
```

### 4.3 RowIndex 结构（V1.1+）

RowIndex 作为**独立的 Page** 存储在文件中，通过 Footer.Metadata 引用：

```
文件布局（V1.1+）：
┌─────────────────────────────────────────────────────────────┐
│ Header                                                        │
├─────────────────────────────────────────────────────────────┤
│ Page 0 (Column 0, Page 0)                                     │
│ Page 1 (Column 0, Page 1)                                     │
│ ...                                                          │
│ Page N (Column M, Page K)                                     │
├─────────────────────────────────────────────────────────────┤
│ RowIndex Page（类型：PageTypeIndex）                          │
│ ┌─────────────────────────────────────────────────────────┐ │
│ │ Magic (0x52494458 = "RIDX")                             │ │
│ │ NumEntries (int32)                                      │ │
│ │ BucketCount (int32)                                     │ │
│ │ HashTable (bucketCount * 8 bytes)                       │ │
│ │ EntryArray (numEntries * 16 bytes)                      │ │
│ │   - IDHash (uint64)                                     │ │
│ │   - RowIndex (int64)                                    │ │
│ └─────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│ Footer（包含指向 RowIndex Page 的偏移）                       │
└─────────────────────────────────────────────────────────────┘
```

**优势**：
1. **Header 不变**：完全向后兼容现有文件
2. **复用 Page 机制**：RowIndex 可以被压缩、校验，与数据 Page 一致处理
3. **按需加载**：只在需要时读取 RowIndex Page，不增加启动时间
4. **缓存友好**：RowIndex Page 可以被 BlockCache 缓存

---

## 5. 兼容性框架

### 5.1 版本检查器

```go
type VersionChecker struct {
    readerVersion VersionPolicy
}

func NewVersionChecker(readerVersion VersionPolicy) *VersionChecker {
    return &VersionChecker{readerVersion: readerVersion}
}

// CheckReadCompatibility 验证文件是否可以被读取
func (vc *VersionChecker) CheckReadCompatibility(fileVersion uint16) error {
    fileMajor := uint8(fileVersion >> 8)
    fileMinor := uint8(fileVersion & 0xFF)
    fileVP := VersionPolicy{MajorVersion: fileMajor, MinorVersion: fileMinor}
    
    // 检查主版本
    if fileMajor != vc.readerVersion.MajorVersion {
        return &VersionError{
            Op:           "check_compatibility",
            FileVersion:  fileVersion,
            ReaderVersion: vc.readerVersion.Encoded(),
            Reason:       "major version mismatch",
            Suggestion:   fmt.Sprintf("需要 Vego %d.x 来读取此文件", fileMajor),
        }
    }
    
    // 检查次版本
    if fileMinor > vc.readerVersion.MinorVersion {
        return &VersionError{
            Op:           "check_compatibility",
            FileVersion:  fileVersion,
            ReaderVersion: vc.readerVersion.Encoded(),
            Reason:       "file version newer than reader",
            Suggestion:   fmt.Sprintf("请升级至 Vego %d.%d 或更高版本", fileMajor, fileMinor),
        }
    }
    
    return nil
}

// CanUseFeature 检查特定特性是否可用
func (vc *VersionChecker) CanUseFeature(fileFeatures uint32, feature uint32) bool {
    return (fileFeatures & feature) != 0 && (vc.readerVersion.FeatureFlags & feature) != 0
}

// GetReadStrategy 返回适当的读取策略
func (vc *VersionChecker) GetReadStrategy(
    fileVersion uint16, 
    fileFeatures uint32,
) ReadStrategy {
    fileMajor := uint8(fileVersion >> 8)
    fileMinor := uint8(fileVersion & 0xFF)
    
    // 同版本：正常读取
    if fileMajor == vc.readerVersion.MajorVersion && 
       fileMinor == vc.readerVersion.MinorVersion {
        return ReadStrategyNormal
    }
    
    // 旧版本：检查缺失的特性
    if fileMinor < vc.readerVersion.MinorVersion {
        missingFeatures := vc.readerVersion.FeatureFlags & ^fileFeatures
        
        // 检查是否缺少关键特性
        if (missingFeatures & FeatureRowIndex) != 0 && 
           (fileFeatures & FeatureRowIndex) == 0 {
            return ReadStrategyFallbackLinearScan
        }
        
        return ReadStrategyCompatible
    }
    
    return ReadStrategyUnsupported
}
```

### 5.2 读取策略

```go
type ReadStrategy int

const (
    ReadStrategyUnsupported ReadStrategy = iota
    ReadStrategyNormal                   // 全功能支持
    ReadStrategyCompatible               // 忽略可选的新特性
    ReadStrategyFallbackLinearScan       // 无 RowIndex，全表扫描
)

func (s ReadStrategy) String() string {
    switch s {
    case ReadStrategyNormal:
        return "normal"
    case ReadStrategyCompatible:
        return "compatible"
    case ReadStrategyFallbackLinearScan:
        return "fallback_linear_scan"
    default:
        return "unsupported"
    }
}
```

### 5.3 兼容性矩阵

| Reader \ File | V1.0 | V1.1 | V1.2 | V2.0 |
|--------------|------|------|------|------|
| **V1.0** | ✅ 完全 | ❌ 拒绝 | ❌ 拒绝 | ❌ 拒绝 |
| **V1.1** | ✅ 兼容<br>(线性扫描) | ✅ 完全 | ⚠️ 兼容<br>(无块缓存) | ❌ 拒绝 |
| **V1.2** | ✅ 兼容 | ✅ 兼容 | ✅ 完全 | ❌ 拒绝 |

### 5.4 错误处理

```go
// 版本问题的特定错误类型
var (
    ErrVersionTooOld       = errors.New("文件版本太旧，需要迁移")
    ErrVersionTooNew       = errors.New("文件版本太新，请升级读取器")
    ErrFeatureNotSupported = errors.New("文件使用了不支持的特性")
)

// VersionError 包含故障排除的上下文
type VersionError struct {
    Op            string // 操作
    FileVersion   uint16 // 文件中的版本
    ReaderVersion uint16 // 读取器版本
    Reason        string // 失败原因
    Suggestion    string // 用户建议
}

func (e *VersionError) Error() string {
    return fmt.Sprintf("version error: %s (file=%d.%d, reader=%d.%d): %s; %s",
        e.Op,
        e.FileVersion>>8, e.FileVersion&0xFF,
        e.ReaderVersion>>8, e.ReaderVersion&0xFF,
        e.Reason,
        e.Suggestion,
    )
}

func (e *VersionError) Unwrap() error {
    switch {
    case strings.Contains(e.Reason, "newer"):
        return ErrVersionTooNew
    case strings.Contains(e.Reason, "older"):
        return ErrVersionTooOld
    default:
        return ErrFeatureNotSupported
    }
}
```

---

## 6. 格式演进策略

### 6.1 次版本更新（1.0 → 1.1 → 1.2）

**规则：**
- 仅添加变更（新的可选 Metadata 字段）
- 新特性默认禁用
- 旧读取器可以检测但可能拒绝
- 新读取器必须支持所有旧版本

**流程：**
1. 添加特性标志到 `FeatureXxx` 常量
2. 定义新版本 `VersionPolicy`
3. 更新 Footer.Metadata 存储新信息
4. 实现带版本检查的特性
5. 为旧版本添加回退行为

### 6.2 主版本更新（1.x → 2.0）

**规则：**
- 允许破坏性变更
- 可能使用新的 Magic Number
- 需要迁移工具
- 不保证后向兼容性

**流程：**
1. 设计新格式结构
2. 创建迁移工具
3. 维护并行读取器实现
4. 过渡期后弃用旧版本

### 6.3 迁移策略

```go
// Migrator 处理版本升级
type Migrator struct {
    FromVersion VersionPolicy
    ToVersion   VersionPolicy
}

// CanMigrateDirectly 检查是否可以直接迁移（同主版本）
func (m *Migrator) CanMigrateDirectly() bool {
    return m.FromVersion.MajorVersion == m.ToVersion.MajorVersion &&
           m.FromVersion.MinorVersion < m.ToVersion.MinorVersion
}

// Migrate 执行离线迁移（读取旧文件，写入新文件）
func (m *Migrator) Migrate(inputPath, outputPath string) error {
    // 1. 读取旧版本文件
    reader, err := column.NewReader(inputPath)
    if err != nil {
        return fmt.Errorf("open input: %w", err)
    }
    defer reader.Close()
    
    batch, err := reader.ReadRecordBatch()
    if err != nil {
        return fmt.Errorf("read batch: %w", err)
    }
    
    // 2. 使用新版本写入
    writer, err := column.NewWriterWithVersion(outputPath, batch.Schema(), 
        nil, m.ToVersion)
    if err != nil {
        return fmt.Errorf("create writer: %w", err)
    }
    
    if err := writer.WriteRecordBatch(batch); err != nil {
        writer.Close()
        return fmt.Errorf("write batch: %w", err)
    }
    
    return writer.Close()
}
```

**迁移路径：**
```
V1.0 文件 ──┬──► 迁移 ──► V1.1 文件 ──┬──► 迁移 ──► V1.2 文件
            │                        │
            └──► V1.1 Reader (回退)  ─┘──► V1.2 Reader (回退)
```

---

## 7. 旧版本映射

### 7.1 版本号标准化

现有文件的 `Header.Version = 1` 需要映射到新格式：

```go
// NormalizeVersion 将旧版本号映射到新格式
func NormalizeVersion(v uint16) uint16 {
    switch v {
    case 1:           // 旧格式 V1（无前缀）
        return 0x0100  // V1.0
    case 0x0100, 0x0101, 0x0102:
        return v      // 已经是新格式
    default:
        // 未知版本，原样返回让后续检查处理
        return v
    }
}

// 在读取时使用
func (r *Reader) readHeader() error {
    header := &Header{}
    if _, err := header.ReadFrom(r.file); err != nil {
        return err
    }
    
    // 标准化版本号
    header.Version = NormalizeVersion(header.Version)
    
    // 从 Footer 读取 FeatureFlags（如果有）
    if err := r.readFooter(); err != nil {
        return err
    }
    
    // 解析 Footer.Metadata 中的特性标志
    if versionStr, ok := r.footer.Metadata["vego.format.version"]; ok {
        // 使用 Footer 中存储的版本信息
        r.fileVersion = parseVersionString(versionStr)
        r.fileFeatures = parseFeaturesString(r.footer.Metadata["vego.format.features"])
    } else {
        // 旧文件没有 Metadata，使用 Header.Version
        r.fileVersion = VersionPolicy{
            MajorVersion: uint8(header.Version >> 8),
            MinorVersion: uint8(header.Version & 0xFF),
        }
        r.fileFeatures = FeatureBasicColumnar | FeatureZstdCompression
    }
    
    return nil
}
```

---

## 8. 实现计划

### Phase 1, 第 1-2 周：基础

**第 1-2 天：版本类型** ✅ 已完成
- [x] 创建 `storage/format/version.go`
- [x] 定义 `VersionPolicy`、简化后的 `VersionChecker`
- [x] 实现 `FeatureXxx` 常量
- [x] 添加版本编码/解码辅助函数
- [x] 实现 `NormalizeVersion` 用于旧版本映射
- [x] 编写完整单元测试（16个测试用例）

**第 3-4 天：Footer 扩展**
- [ ] 扩展 Footer.Metadata 存储格式版本信息
- [ ] 添加 `MetadataHelpers` 读写工具函数
- [ ] 保持 Header 不变（向后兼容）

**第 5-6 天：兼容性框架**
- [ ] 实现 `VersionChecker.CheckReadCompatibility()`
- [ ] 实现 `GetReadStrategy()`
- [ ] 添加特性检测方法
- [ ] 创建版本问题的错误类型（含 Suggestion）

**第 7 天：测试**
- [ ] 版本编码的单元测试
- [ ] 兼容性矩阵测试
- [ ] 基于属性的测试（往返）
- [ ] 错误情况覆盖
- [ ] 旧版本映射测试

### Phase 1, 第 3-4 周：行索引集成

**依赖：** 版本管理必须完成

- [ ] 实现 `RowIndex` 结构（作为独立 Page）
- [ ] 添加 `FeatureRowIndex` 标志
- [ ] 更新 `Writer` 为 V1.1+ 写入 RowIndex Page 和 Footer.Metadata
- [ ] 更新 `Reader` 从 Footer 读取 RowIndex 偏移并加载
- [ ] 为 V1.0 文件实现 `ReadStrategyFallbackLinearScan`

### Phase 1, 第 5-6 周：块缓存集成

- [ ] 在 Footer.Metadata 中存储 BlockCache 配置
- [ ] 添加 `FeatureBlockCache` 标志
- [ ] 基于版本与 Reader 集成
- [ ] 为 V1.2+ 文件添加缓存预热

---

## 9. API 参考

### 9.1 创建带版本的写入器

```go
// 作为 V1.0 写入（无 RowIndex）
writer, err := column.NewWriterWithVersion(
    "legacy.lance", 
    schema, 
    nil, 
    format.V1_0,
)

// 作为 V1.1 写入（带 RowIndex）
writer, err := column.NewWriterWithVersion(
    "modern.lance", 
    schema, 
    nil, 
    format.V1_1,
)

// 作为 V1.2 写入（带 BlockCache 提示）
writer, err := column.NewWriterWithVersion(
    "optimized.lance", 
    schema, 
    nil, 
    format.V1_2,
)

// 使用默认配置（推荐）
writer, err := column.NewWriter(
    "file.lance", 
    schema, 
    nil,
    // 内部默认使用 CurrentVersion
)
```

### 9.2 带版本检测的读取

```go
// Reader 自动检测版本并适配
reader, err := column.NewReader("file.lance")
if err != nil {
    // 处理版本不匹配错误
    var verr *format.VersionError
    if errors.As(err, &verr) {
        log.Fatalf("版本错误: %s", verr.Suggestion)
    }
    return err
}
defer reader.Close()

// 检查文件能力
capabilities := reader.Capabilities()
fmt.Printf("文件版本: %s\n", capabilities.Version)
fmt.Printf("读取策略: %s\n", capabilities.Strategy)

if capabilities.HasFeature(format.FeatureRowIndex) {
    // 快速路径：O(1) 查找
    doc, err := reader.GetByID(id)
} else {
    // 回退：O(n) 扫描（已记录警告）
    doc, err := reader.GetByID(id)
}
```

### 9.3 版本检查

```go
// 手动版本检查
checker := format.NewVersionChecker(format.V1_2)

// 从文件读取版本
fileVersion, fileFeatures, err := format.InspectFile("file.lance")
if err != nil {
    return err
}

err = checker.CheckReadCompatibility(fileVersion)
if err != nil {
    var verr *format.VersionError
    if errors.As(err, &verr) {
        if errors.Is(err, format.ErrVersionTooOld) {
            // 提供迁移
            migrator := format.NewMigrator(
                format.VersionFromEncoded(fileVersion),
                format.V1_2,
            )
            migrator.Migrate("old.lance", "new.lance")
        } else if errors.Is(err, format.ErrVersionTooNew) {
            // 拒绝并提供有用消息
            log.Fatalf(verr.Suggestion)
        }
    }
}

// 特性检测
if checker.CanUseFeature(fileFeatures, format.FeatureRowIndex) {
    // 使用优化路径
}
```

---

## 10. 测试策略

### 10.1 单元测试

```go
// 测试版本编码
func TestVersionEncoding(t *testing.T) {
    v := format.V1_1.Encoded() // 0x0101
    assert.Equal(t, uint16(0x0101), v)
    assert.Equal(t, uint8(1), v>>8)   // Major
    assert.Equal(t, uint8(1), v&0xFF) // Minor
}

// 测试兼容性方法
func TestVersionCompatibility(t *testing.T) {
    // V1.1 可以读取 V1.0
    assert.True(t, format.V1_1.CanRead(format.V1_0))
    // V1.0 不能读取 V1.1
    assert.False(t, format.V1_0.CanRead(format.V1_1))
    // V1.2 可以读取 V1.0 和 V1.1
    assert.True(t, format.V1_2.CanRead(format.V1_0))
    assert.True(t, format.V1_2.CanRead(format.V1_1))
}

// 测试旧版本映射
func TestNormalizeVersion(t *testing.T) {
    assert.Equal(t, uint16(0x0100), format.NormalizeVersion(1))    // 旧格式
    assert.Equal(t, uint16(0x0100), format.NormalizeVersion(0x0100)) // 新格式
    assert.Equal(t, uint16(0x0101), format.NormalizeVersion(0x0101))
}

// 测试兼容性矩阵
func TestCompatibilityMatrix(t *testing.T) {
    checker := format.NewVersionChecker(format.V1_1)
    
    tests := []struct {
        fileVersion uint16
        wantErr     bool
        errType     error
    }{
        {0x0100, false, nil},           // V1.0 OK
        {0x0101, false, nil},           // V1.1 OK
        {0x0102, true, format.ErrVersionTooNew}, // V1.2 too new
        {0x0200, true, format.ErrVersionTooNew}, // V2.0 major mismatch
    }
    
    for _, tt := range tests {
        err := checker.CheckReadCompatibility(tt.fileVersion)
        if tt.wantErr {
            assert.ErrorIs(t, err, tt.errType)
        } else {
            assert.NoError(t, err)
        }
    }
}
```

### 10.2 集成测试

```go
// 测试不同版本的往返
func TestVersionRoundtrip(t *testing.T) {
    versions := []format.VersionPolicy{format.V1_0, format.V1_1, format.V1_2}
    
    for _, v := range versions {
        t.Run(v.String(), func(t *testing.T) {
            path := filepath.Join(t.TempDir(), "test.lance")
            
            // 用特定版本写入
            writer, err := column.NewWriterWithVersion(path, schema, nil, v)
            require.NoError(t, err)
            require.NoError(t, writer.WriteRecordBatch(batch))
            require.NoError(t, writer.Close())
            
            // 读回
            reader, err := column.NewReader(path)
            require.NoError(t, err)
            batch2, err := reader.ReadRecordBatch()
            require.NoError(t, err)
            reader.Close()
            
            // 验证数据完整性
            assertEqualBatches(t, batch, batch2)
            
            // 验证版本信息
            caps := reader.Capabilities()
            assert.Equal(t, v.MajorVersion, caps.Version.MajorVersion)
            assert.Equal(t, v.MinorVersion, caps.Version.MinorVersion)
        })
    }
}
```

### 10.3 兼容性测试

```go
// 测试跨版本兼容性
func TestCrossVersionCompatibility(t *testing.T) {
    tmpDir := t.TempDir()
    
    // 创建 V1.0 文件
    v10Path := filepath.Join(tmpDir, "v10.lance")
    writeTestFile(t, v10Path, format.V1_0, testData)
    
    // V1.2 reader 应该能读取 V1.0 文件（兼容模式）
    reader, err := column.NewReader(v10Path)
    require.NoError(t, err)
    assert.Equal(t, format.ReadStrategyFallbackLinearScan, reader.Capabilities().Strategy)
    reader.Close()
    
    // 创建 V1.2 文件
    v12Path := filepath.Join(tmpDir, "v12.lance")
    writeTestFile(t, v12Path, format.V1_2, testData)
    
    // V1.0 reader 应该拒绝 V1.2 文件
    checker := format.NewVersionChecker(format.V1_0)
    err = checker.CheckReadCompatibility(0x0102)
    assert.ErrorIs(t, err, format.ErrVersionTooNew)
}

// 测试旧版本映射
func TestLegacyFileSupport(t *testing.T) {
    // 创建模拟的旧格式文件（Version = 1）
    tmpDir := t.TempDir()
    path := filepath.Join(tmpDir, "legacy.lance")
    
    // 手动写入旧格式 Header
    writeLegacyV1File(t, path, testData)
    
    // V1.2 reader 应该能读取
    reader, err := column.NewReader(path)
    require.NoError(t, err)
    
    // 验证版本被正确映射
    caps := reader.Capabilities()
    assert.Equal(t, uint8(1), caps.Version.MajorVersion)
    assert.Equal(t, uint8(0), caps.Version.MinorVersion)
    
    reader.Close()
}
```

---

## 11. 迁移指南

### 从 V1.0 到 V1.1

```bash
# 命令行迁移工具（未来）
vego migrate --from 1.0 --to 1.1 input.lance output.lance

# 或编程方式
migrator := format.NewMigrator(format.V1_0, format.V1_1)
if err := migrator.Migrate("input.lance", "output.lance"); err != nil {
    log.Fatal(err)
}
```

**V1.1 的好处：**
- 按 ID O(1) 文档查找（对比 O(n) 扫描）
- 减少 Get() 操作的内存压力
- 更好地支持大集合（>10K 文档）

**何时迁移：**
- 集合 > 10K 文档
- 频繁的 Get() 操作
- 内存受限环境

---

## 附录 A：版本历史

| 版本 | 日期 | 变更 | 状态 |
|------|------|------|------|
| 1.0 | 当前 | 初始列式格式 | 稳定 |
| 1.1 | 计划中 | + 行索引（Footer Metadata 引用独立 Page） | 设计中 |
| 1.2 | 计划中 | + 块缓存元数据 | 设计中 |
| 2.0 | 未来 | 主版本修订 | 未开始 |

---

## 附录 B：相关文档

- `ARCHITECTURE.md` - 整体系统架构
- `STORAGE.md` - 存储层文档
- `ROADMAP.md` - 开发路线图和优先级
- `FILE_VERSION.md` - 英文版文件版本管理文档

---

## 附录 C：设计变更记录

### 相对于原始设计的修改

1. **VersionPolicy 字段简化**
   - 删除 `BackwardCompatible`/`ForwardCompatible` 字段
   - 改为 `CanRead()`/`CanBeReadBy()` 方法
   - 原因：行为由版本号决定，无需存储冗余字段

2. **RowIndex 位置变更**
   - 原：Header 扩展，Reserved 字段重新分配
   - 新：Footer.Metadata 引用独立 Page
   - 原因：Header 不变保证完全向后兼容，Page 机制可复用压缩/校验

3. **添加 Flags vs FeatureFlags 区分**
   - 明确 `Header.Flags`（文件级）与 `FeatureFlags`（格式级）的区别
   - 添加对比表格说明

4. **错误信息增强**
   - 保留 `Suggestion` 字段
   - 添加 `Unwrap()` 方法支持 `errors.Is()`

5. **读取策略细化**
   - `ReadStrategyFallbackLinearScan` 特指无 RowIndex 时的回退
   - 添加 `String()` 方法便于日志记录

6. **Metadata 命名规范**
   - 使用 `vego.` 前缀避免与用户元数据冲突
   - 标准化字段命名（`vego.format.version`, `vego.rowindex.offset` 等）
