# Vego JSON Metadata 设计分析

> 分析 Vego 选用 JSON 作为 `Document.Metadata` 持久化格式的合理性、问题与改进方向。
>
> 创建日期：2026-04-28

---

## 1. 背景：Vego 的存储架构

Vego 将数据分为三层存储：

| 数据类型 | 存储方式 | 格式 | 设计目标 |
|---------|---------|------|---------|
| **向量 (Vector)** | Lance 兼容列式存储 | 二进制 + 压缩 | 高性能顺序/随机读取 |
| **元数据索引** | `metadata.json` | JSON | ID→RowIndex 映射 + 用户自定义属性 |
| **删除标记** | `deletion_vector.bin` | 自定义二进制 | HNSW 可见性控制 |

`metadata.json` 的内存结构：

```go
type docMeta struct {
    ID       string                 `json:"id"`
    RowIndex int64                  `json:"row_index"`  // 指向列式存储中的行位置
    Metadata map[string]interface{} `json:"metadata"`   // 用户自定义属性
}
```

**关键使用模式：**
- **启动时**：`loadMetadata()` 一次性 `json.Unmarshal` 全量加载到内存 `map[int64]docMeta`
- **运行时**：所有 `Get`/`CheckVisibility` 操作访问的是内存中的 map，**零磁盘 I/O**
- **写入时**：`saveMetadata()` 把整个 map `json.Encode` 到磁盘，**全量覆盖**
- **数据量**：嵌入式场景，<100K 文档，metadata 文件通常 <50MB

---

## 2. JSON 的合理性

### 2.1 为什么 JSON 是合理选择

**1. 零外部依赖**
Vego 的核心卖点是"嵌入式、零外部依赖"。JSON 是 Go 标准库原生支持，不需要引入 protobuf、msgpack、sqlite 等第三方依赖。这对一个声称"单 Go 库即可运行"的项目至关重要。

**2. Schema 动态性**
`map[string]interface{}` 允许用户存储任意 key-value 对。JSON 天然支持这种半结构化数据，无需预定义 schema。如果用 Protobuf/Avro，需要 schema 注册和版本管理，增加复杂度。

**3. 启动时全量加载**
既然 metadata 在启动时就全部读入内存，后续的读写都是内存操作，JSON 的解析开销只发生在启动时。对于 <100K 文档，50MB JSON 的 `Unmarshal` 耗时 <100ms，可接受。

**4. 人类可读**
JSON 是纯文本，调试时 `cat metadata.json` 即可查看。这对于一个相对年轻的项目，降低了排查数据问题的门槛。

**5. 增量写入的替代方案已存在**
Vego 有 `writeBuffer` 机制——新文档先写入内存 buffer，批量 flush 到列式存储，同时重写 metadata JSON。虽然 metadata 是全量覆盖，但触发频率低（buffer 满或显式 Save），不是每条文档都写磁盘。

### 2.2 对 memory 包的正面影响

JSON metadata 的类型退化问题（`int` → `float64`，`[]string` → `[]interface{}`）倒逼 memory 包发明了**单 JSON 字段 `_data` 方案**——将整个 `Memory` 结构体序列化为一个 JSON 字符串存入 `_data` 键，反序列化时一次性 `json.Unmarshal` 还原。

这是一个**优雅的 workaround**：它把 Vego Metadata 的 `map[string]interface{}` 当作透明传输层，数据模型的序列化/反序列化由 memory 包自己控制，绕过了 JSON 类型退化。

同时，`_state`/`_type` 冗余字段作为高频过滤字段提升到 map 的直接键层级，使 HNSW `isExcluded` 回调保持 O(1)，无需每次解析 JSON。

---

## 3. JSON 的深层问题

### 3.1 全量覆盖写入的写放大

```go
func (s *DocumentStorage) saveMetadata() error {
    file, _ := os.Create(s.metaStore.path)
    encoder := json.NewEncoder(file)
    encoder.SetIndent("", "  ")
    encoder.Encode(data)  // 把整个 map 重新写成 JSON
}
```

即使只修改了 1 个文档的 metadata，也要重写整个 JSON 文件。对于 100K 文档，这意味着：
- 每次 `Save()` 写入 50MB+
- `os.Create` 截断旧文件，**无原子性保障**（如果写入中途崩溃，文件损坏）
- 高写入频率场景下，SSD 寿命和性能都受影响

**对比：** SQLite 的 WAL 模式只需追加几 KB 日志即可保证原子性。

### 3.2 JSON number 的类型退化

`map[string]interface{}` + JSON 往返 = `int` 变 `float64`，`[]string` 变 `[]interface{}`。

| 存入时的 Go 类型 | 取出时的 Go 类型 | 示例 |
|-----------------|-----------------|------|
| `int` / `int64` | `float64` | `42` → `42.0` |
| `[]string` | `[]interface{}`（元素为 `string`）| 需类型断言转换 |
| `map[string]string` | `map[string]interface{}` | 嵌套 map 同样退化 |

这迫使上层代码（如 memory 包）要么做防御性逐字段类型断言（代码脆弱、膨胀），要么采用 `_data` 单 JSON 字段方案。

**这本身就说明 JSON 作为通用 metadata 载体是有缺陷的**——如果 format 设计合理，上层不应需要做这种 workaround。

### 3.3 缺乏增量更新能力

假设要实现"只更新文档 A 的 metadata，不碰其他 99999 个文档"——在 JSON 全量覆盖模型下 impossible。必须重写整个文件。

这直接导致：
- `UpdateContext` 实际上是 MarkDeleted(旧) + Insert(新)，同 ID 替换
- 无法高效地只改一个字段
- 高频率更新场景性能极差

### 3.4 无事务保证

JSON 文件写入是：
1. `os.Create` 截断旧文件
2. `json.Encode` 写入新内容
3. `file.Close`

如果步骤 2 中进程崩溃，文件处于半写状态，下次启动 `json.Unmarshal` 会失败。**没有写前日志 (WAL)，没有校验和，没有自动恢复机制。**

---

## 4. 替代方案对比

| 方案 | 优势 | 劣势 | 对 Vego 的适配度 |
|------|------|------|----------------|
| **JSON（当前）** | 零依赖、人类可读、动态 schema | 全量覆盖、无事务、类型退化 | ⭐⭐⭐ 合理 |
| **SQLite（纯 Go）** | 事务、增量更新、WAL、成熟稳定 | 增加 ~2MB 二进制、需 schema 管理 | ⭐⭐⭐⭐ 更优 |
| **BoltDB / bbolt** | 纯 Go、KV 存储、事务、mmap | 无嵌套 map 原生支持、B-tree 非列式 | ⭐⭐⭐ 可行 |
| **Protobuf + 自定义格式** | 紧凑、类型安全、速度快 | 需 schema、非人类可读、增量更新需自行实现 | ⭐⭐ 过度设计 |
| **Parquet（Lance 已用）** | 列式、压缩、高性能 | 只读优化、随机写极差、不适合小文件 | ⭐ 不合适 |

### 4.1 最优替代：SQLite（纯 Go 版）

`modernc.org/sqlite` 是一个纯 Go 实现的 SQLite（无 CGO），与 Vego 的"零依赖"哲学不冲突：

```sql
-- 可能的 metadata 表设计
CREATE TABLE doc_meta (
    id_hash INTEGER PRIMARY KEY,
    id TEXT UNIQUE NOT NULL,
    row_index INTEGER,
    metadata_json TEXT,  -- 仍用 JSON 存动态属性
    state TEXT,
    type TEXT
);
```

**带来的改进：**
- **增量更新**：`UPDATE doc_meta SET metadata_json = ? WHERE id = ?`，只写几 KB
- **事务**：`BEGIN; UPDATE ...; COMMIT;`，崩溃后自动恢复
- **索引**：可在 `state`、`type` 上建索引，加速 `SearchWithFilter` 的过滤
- **WAL 模式**：读写不阻塞，高并发场景下表现更好

**代价：**
- 增加 ~2MB 二进制体积（纯 Go SQLite）
- 需要 SQL schema 管理（但 schema 极简单）
- 不再是"打开文本文件就能看懂"

### 4.2 次优替代：保留 JSON，但改进写入策略

如果必须保留 JSON，最小改进：

**写前日志 + 原子替换：**
```go
func (s *DocumentStorage) saveMetadata() error {
    tmpPath := s.metaStore.path + ".tmp"
    file, _ := os.Create(tmpPath)
    json.NewEncoder(file).Encode(data)
    file.Close()
    return os.Rename(tmpPath, s.metaStore.path)  // 原子替换
}
```

**增量 JSON Lines 格式：**
```json
// metadata.jsonl: 每行一个文档的 JSON
{"id":"a","row_index":0,"metadata":{...}}
{"id":"b","row_index":1,"metadata":{...}}
```
启动时逐行读取，追加写入时只需 `file.Write` 新行。但删除需要 compaction。

---

## 5. 对 memory 包的影响

JSON metadata 的选择直接塑造了 memory 包的设计：

| 影响 | 具体表现 |
|------|---------|
| `_data` 方案被迫发明 | 绕过 JSON 类型退化 |
| `memoryToDoc` 成为唯一写入路径 | 保证 `_state`/`_type` 与 `_data` 同步 |
| `UpdateContext` = MarkDeleted + Insert | 因为 JSON 不支持字段级增量更新 |
| `rebuildIndexes` 必须遍历全量文档 | 因为无法只查询 active 状态的文档 |
| `SearchWithFilterContext` 的 over-fetch | 因为无法从 JSON metadata 建立高效索引 |

**如果 Vego 底层使用 SQLite：**
- memory 包可以直接用 `SELECT * FROM doc_meta WHERE state = 'active'`，无需 over-fetch
- `Update` 可以是真正的 UPDATE，不需要 Archive-and-Create
- `Ingest` 的调和可以依赖 SQL 事务，不需要 `sync.Mutex` + Insert-first 策略

---

## 6. 结论

| 评价维度 | 结论 |
|---------|------|
| **对于 Vego MVP** | JSON 是合理选择。简单、零依赖、够用。 |
| **对于 production 级嵌入式向量数据库** | JSON 是**短板**。应在未来版本替换为 SQLite（纯 Go）或至少引入 WAL + 原子写入。 |
| **对 memory 包的影响** | 正面：倒逼出 `_data` 方案这个优雅的 workaround；负面：限制了搜索性能和更新语义。 |

> **一句话总结：JSON metadata 是 Vego 在"简单优先"哲学下的务实选择，但当项目走向成熟，它会成为第一个需要被替换的组件。**

---

## 7. 建议的演进路径

### 短期（不改变存储格式）
1. 为 `saveMetadata()` 添加原子写入（tmp file + `os.Rename`）
2. 添加 JSON 文件校验和或版本头，检测半写损坏
3. 在文档中明确说明 metadata 的"全有或全无"写入语义

### 中期（保持向后兼容）
1. 引入 SQLite 作为可选 metadata 后端（`WithMetadataBackend("sqlite")`）
2. JSON 后端继续作为默认选项，保证零依赖
3. 提供迁移工具：`vego migrate --to=sqlite`

### 长期（打破兼容）
1. 将 SQLite 作为唯一 metadata 后端
2. 删除 JSON metadata 支持，简化代码路径
3. 利用 SQLite 索引优化 `SearchWithFilterContext`，消除 over-fetch
