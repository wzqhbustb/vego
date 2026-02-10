# Vego 架构设计与数据流文档

## 1. 整体架构概览

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              User Application                                │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              vego (API Layer)                                │
│  ┌──────────────┐  ┌──────────────────────────────────────────────────┐    │
│  │     DB       │  │              Collection                           │    │
│  │  - Open()    │  │  - Insert/Batch  - Get/Delete                    │    │
│  │  - Close()   │  │  - Update/Upsert - Search/WithFilter/Batch       │    │
│  │  - Collection│  │  - Save/Stats/Count                              │    │
│  └──────────────┘  └──────────────────────────────────────────────────┘    │
│                          │                           │                      │
│                          ▼                           ▼                      │
│              ┌──────────────────┐       ┌──────────────────────┐           │
│              │  docToNode map   │       │  DocumentStorage     │           │
│              │  nodeToDoc map   │       │  (Column Storage)    │           │
│              └──────────────────┘       └──────────────────────┘           │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                    ┌─────────────────┴─────────────────┐
                    ▼                                   ▼
┌──────────────────────────────────┐  ┌─────────────────────────────────────┐
│          index (HNSW)            │  │          storage (Lance)            │
│  ┌────────────────────────────┐  │  │  ┌─────────────────────────────┐   │
│  │      HNSWIndex             │  │  │  │      Writer/Reader          │   │
│  │  - nodes []*Node           │  │  │  │  - WriteRecordBatch()       │   │
│  │  - entryPoint int32        │  │  │  │  - ReadRecordBatch()        │   │
│  │  - maxLevel int32          │  │  │  │                             │   │
│  │  - globalLock RWMutex      │  │  │  └─────────────────────────────┘   │
│  └────────────────────────────┘  │  │              │                     │
│                                  │  │              ▼                     │
│  ┌────────────────────────────┐  │  │  ┌─────────────────────────────┐   │
│  │     Search Algorithm       │  │  │  │      Arrow Layer            │   │
│  │  - Greedy nearest search   │  │  │  │  - Schema/RecordBatch       │   │
│  │  - Multi-level navigation  │  │  │  │  - Array Builders           │   │
│  └────────────────────────────┘  │  │  │  - FixedSizeList (vectors)  │   │
└──────────────────────────────────┘  │  └─────────────────────────────┘   │
                                      │              │                     │
                                      │              ▼                     │
                                      │  ┌─────────────────────────────┐   │
                                      │  │      Format Layer           │   │
                                      │  │  - Header/Footer            │   │
                                      │  │  - Page (compressed data)   │   │
                                      │  │  - PageIndex (offsets)      │   │
                                      │  └─────────────────────────────┘   │
                                      └─────────────────────────────────────┘

```

---

## 2. 数据存储流程 (Insert)

```
User Call: coll.Insert(doc)

Step 1: API 层验证
┌─────────────────────────────────────────────────────────────┐
│  collection.Insert(doc)                                      │
│  ├── doc.Validate(dimension)  // 检查向量维度                │
│  └── 检查 docToNode 是否已存在                               │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
Step 2: 写入 HNSW 索引 (内存)
┌─────────────────────────────────────────────────────────────┐
│  index.Add(vector)                                           │
│  ├── 生成随机层数: randomLevel()                             │
│  │   └── level = floor(-ln(U) * ml), U~Uniform(0,1)         │
│  ├── 创建 Node: NewNode(id, vector, level)                  │
│  ├── globalLock.Lock()                                       │
│  │   └── nodeID := len(h.nodes)                             │
│  │   └── h.nodes = append(h.nodes, newNode)                │
│  ├── globalLock.Unlock()                                     │
│  └── insert(newNode)  // 建立层级连接                        │
│      └── 在每一层搜索最近邻居并建立双向连接                   │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
Step 3: 更新映射关系 (内存)
┌─────────────────────────────────────────────────────────────┐
│  collection.docToNode[doc.ID] = nodeID                      │
│  collection.nodeToDoc[nodeID] = doc.ID                      │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
Step 4: 写入 DocumentStorage (缓冲)
┌─────────────────────────────────────────────────────────────┐
│  storage.Put(doc)                                            │
│  ├── 检查 doc.ID 是否已存在 (通过 metaStore.idToHash)       │
│  ├── 如果存在: deleteFromStorage(id)  // 软删除             │
│  └── writeBuffer = append(writeBuffer, doc.Clone())         │
│      └── bufferSize++                                        │
│                                                              │
│  ⚠️ 此时数据还未写入磁盘!                                    │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼ (bufferSize >= 1000 或 Close)
Step 5: Flush 到磁盘
┌─────────────────────────────────────────────────────────────┐
│  storage.flush()                                             │
│  ├── 读取现有文档: readAllDocuments()                        │
│  ├── 合并: allDocs = append(existingDocs, writeBuffer...)   │
│  └── rewriteStorage(allDocs)                                 │
│      ├── 更新 metaStore.entries (内存)                      │
│      ├── 更新 metaStore.idToHash (内存)                     │
│      ├── saveMetadata()  // 写入 metadata.json              │
│      └── writeColumnStorage(allDocs)  // 写入 vectors.lance │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
Step 6: 写入 Lance 格式文件
┌─────────────────────────────────────────────────────────────┐
│  column.Writer                                               │
│  ├── NewWriter(filename, schema, factory)                   │
│  │   ├── 创建文件                                            │
│  │   └── writeHeaderWithPadding()  // 8KB 预留头             │
│  ├── Build Arrow Arrays                                      │
│  │   ├── idBuilder (Int64): hashID(doc.ID)                  │
│  │   ├── vectorBuilder (FixedSizeList<Float32>): doc.Vector │
│  │   └── timestampBuilder (Int64): doc.Timestamp.UnixNano() │
│  ├── Create RecordBatch                                      │
│  ├── WriteRecordBatch()                                      │
│  │   └── 对每个列调用 writeColumn()                         │
│  │       └── pageWriter.WritePages()  // 压缩编码           │
│  │           ├── 选择编码: RLE/Dictionary/BitPacking/Zstd   │
│  │           └── 写入 Page                                   │
│  └── Close()                                                 │
│      ├── 写入 Footer (PageIndex)                            │
│      └── 重写 Header (更新 NumRows)                         │
└─────────────────────────────────────────────────────────────┘

Disk Layout (单 Collection):
collection_path/
├── vectors.lance          # Lance 格式列存储
│   ├── Header (8KB)       # Schema, NumRows
│   ├── Pages (compressed) # id_hash | vector | timestamp
│   └── Footer             # PageIndexList
├── metadata.json          # ID 映射和元数据
│   ├── entries: {id_hash -> {id, metadata}}
│   └── id_to_hash: {id_string -> id_hash}
└── index/                 # HNSW 索引持久化
    └── (待实现)
```

---

## 3. 数据查询流程 (Search)

```
User Call: coll.Search(query, k)

Step 1: API 层参数处理
┌─────────────────────────────────────────────────────────────┐
│  collection.Search(query, k, opts...)                        │
│  ├── 检查 query 维度匹配                                     │
│  └── 解析 SearchOptions (EF, Filter)                        │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
Step 2: HNSW 向量搜索 (内存)
┌─────────────────────────────────────────────────────────────┐
│  index.Search(query, k, ef)                                  │
│  ├── globalLock.RLock()                                      │
│  ├── ep = entryPoint  // 入口点                             │
│  ├── maxLvl = maxLevel                                       │
│  └── globalLock.RUnlock()                                    │
│                                                              │
│  h.search(query, k, ef, ep, maxLvl)                         │
│  ├── Phase 1: 从顶层向下定位                                 │
│  │   for lvl := maxLvl; lvl > 0; lvl-- {                   │
│  │       ep = searchLayer(query, ep, 1, lvl)[0]  // 每层1个 │
│  │   }                                                       │
│  └── Phase 2: 在 Level 0 精确搜索                           │
│        candidates = searchLayer(query, ep, ef, 0)           │
│        return candidates[:k]  // 返回最近k个                 │
│                                                              │
│  searchLayer() 算法:                                         │
│  ├── candidates = minHeap  // 待探索节点                     │
│  ├── visited = set  // 避免重复访问                          │
│  └── while candidates not empty:                            │
│        pop nearest                                           │
│        for each neighbor:                                    │
│            if not visited:                                   │
│               compute distance                               │
│               push to candidates                             │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
Step 3: 映射到文档 (可能触发磁盘读取)
┌─────────────────────────────────────────────────────────────┐
│  遍历 HNSW SearchResult                                      │
│  for _, hr := range hnswResults {                           │
│      docID := nodeToDoc[hr.ID]  // 内存映射                 │
│      doc := storage.Get(docID)  // ⚠️ 可能磁盘读取!        │
│      results = append(results, SearchResult{doc, hr.Distance})
│  }                                                           │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
Step 4: Storage Get (存在性能问题!)
┌─────────────────────────────────────────────────────────────┐
│  storage.Get(id)                                             │
│  ├── 检查 writeBuffer  // O(bufferSize)                     │
│  ├── 检查 metaStore.idToHash  // O(1)                       │
│  └── readVectorByHash(idHash)  // ⚠️ O(n) 全表扫描!        │
│      └── docs := readAllDocuments()  // 读取全部文档!       │
│          ├── column.NewReader(dataFile)                     │
│          ├── reader.ReadRecordBatch()  // 读取整个文件      │
│          └── 线性扫描找到匹配 idHash 的文档                 │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
Step 5: 返回结果
┌─────────────────────────────────────────────────────────────┐
│  return []SearchResult{                                      │
│      {Document, Distance},                                   │
│      ...                                                     │
│  }                                                           │
└─────────────────────────────────────────────────────────────┘

⚠️ 性能瓶颈:
- Search 返回 k=10 个结果
- 每个 Get() 调用 readAllDocuments() = 读取整个 vectors.lance
- 10K 文档时: 10 次全文件读取!
```

---

## 4. 文件格式详解

### 4.1 Lance 文件格式 (vectors.lance)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              vectors.lance                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│  Offset 0                                                                   │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ Header (8KB reserved)                                                │   │
│  │ ├── Magic: "LANC" (4 bytes)                                         │   │
│  │ ├── Version: 1 (4 bytes)                                            │   │
│  │ ├── Schema (Arrow format):                                          │   │
│  │ │   fields: [                                                       │   │
│  │ │       {name: "id_hash", type: Int64},                            │   │
│  │ │       {name: "vector", type: FixedSizeList<Float32>[dim]},       │   │
│  │ │       {name: "timestamp", type: Int64}                           │   │
│  │ │   ]                                                               │   │
│  │ └── NumRows: N (updated on close)                                   │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                          │                                                  │
│  Offset 8192             ▼                                                  │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ Column 0: id_hash (Int64)                                            │   │
│  │ ┌─────────┐ ┌─────────┐ ┌─────────┐                                 │   │
│  │ │ Page 0  │ │ Page 1  │ │ Page 2  │ ... (compressed)                │   │
│  │ │(values) │ │(values) │ │(values) │                                 │   │
│  │ └─────────┘ └─────────┘ └─────────┘                                 │   │
│  │ Encoding: RLE/Dictionary/BitPacking/Zstd                             │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                          │                                                  │
│                          ▼                                                  │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ Column 1: vector (FixedSizeList<Float32>)                            │   │
│  │ ┌─────────┐ ┌─────────┐ ┌─────────┐                                 │   │
│  │ │ Page 0  │ │ Page 1  │ │ Page 2  │ ...                             │   │
│  │ │(vectors)│ │(vectors)│ │(vectors)│                                 │   │
│  │ └─────────┘ └─────────┘ └─────────┘                                 │   │
│  │ Encoding: BSS (Byte-Slice Split) + Zstd                              │   │
│  │   - 对浮点向量特别优化                                                │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                          │                                                  │
│                          ▼                                                  │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ Column 2: timestamp (Int64)                                          │   │
│  │ ┌─────────┐ ┌─────────┐ ┌─────────┐                                 │   │
│  │ │ Page 0  │ │ Page 1  │ │ Page 2  │ ...                             │   │
│  │ └─────────┘ └─────────┘ └─────────┘                                 │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                          │                                                  │
│  Offset EOF-4KB          ▼                                                  │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ Footer (4KB fixed)                                                   │   │
│  │ ├── NumPages: M                                                      │   │
│  │ └── PageIndexList: [                                                 │   │
│  │     {ColumnIdx, PageIdx, Offset, Size, NumValues, Encoding},       │   │
│  │     ...                                                             │   │
│  │ ]                                                                    │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 4.2 Metadata JSON 格式

```json
{
  "entries": {
    "1234567890": {
      "id": "doc-uuid-1",
      "metadata": {
        "title": "Document 1",
        "author": "Alice",
        "tags": ["tech", "go"]
      }
    },
    "9876543210": {
      "id": "doc-uuid-2",
      "metadata": {
        "title": "Document 2",
        "author": "Bob"
      }
    }
  },
  "id_to_hash": {
    "doc-uuid-1": 1234567890,
    "doc-uuid-2": 9876543210
  }
}
```

---

## 5. 关键问题分析

### 5.1 性能瓶颈: Get() O(n) 复杂度

```
当前实现:
Search(k=10) 
  → 10 × Get()
    → 10 × readAllDocuments()
      → 10 × 读取整个 vectors.lance 文件

优化方案:
1. LRU Cache: 缓存最近访问的文档
2. Row Index: 在 flush 时构建 idHash → fileOffset 映射
3. 内存映射: 如果数据集不大，全量加载到内存
```

### 5.2 存储膨胀: Delete 不物理删除

```
当前实现:
Delete(id) 
  → 从 metaStore 删除
  → vectors.lance 中数据仍然存在!

后果:
- 频繁增删后，vectors.lance 包含大量"死数据"
- 读取时需要过滤，浪费 I/O

优化方案:
1. 后台 Compact: 定期重写文件，剔除已删除数据
2. Delete Bitmap: 维护删除标记，延迟清理
```

### 5.3 写入放大: Flush 全量重写

```
当前实现:
Flush() 
  → 读取所有现有文档
  → 追加新文档
  → 重写整个 vectors.lance

后果:
- 100K 文档时，写入 1000 条新文档需要重写 101K 条!

优化方案:
1. Append Mode: Lance 格式支持追加写入
2. 分段存储: 每个 batch 写入单独文件
```

---

## 6. 并发模型

```
┌─────────────────────────────────────────────────────────────┐
│                    锁层次结构                                │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  DB Level (最外层)                                          │
│  ├── db.mu (RWMutex)                                        │
│  │   ├── RLock: Collections()                               │
│  │   └── Lock:  Collection(), DropCollection(), Close()     │
│  │                                                          │
│  └── 保护: collections map                                  │
│                                                              │
│  Collection Level (中间层)                                  │
│  ├── c.mu (RWMutex)                                         │
│  │   ├── RLock: Get(), Search(), SearchWithFilter(),        │
│  │              SearchBatch(), Count(), Stats()              │
│  │   └── Lock:  Insert(), InsertBatch(), Delete(),          │
│  │              Update(), Upsert(), Save()                   │
│  │                                                          │
│  └── 保护: docToNode, nodeToDoc, index operations            │
│                                                              │
│  Index Level (HNSW)                                         │
│  ├── h.globalLock (RWMutex)                                 │
│  │   ├── RLock: Search, Len                                 │
│  │   └── Lock:  Add (insert)                                │
│  │                                                          │
│  └── 保护: nodes, entryPoint, maxLevel                       │
│                                                              │
│  Storage Level (最内层)                                     │
│  ├── s.mu (RWMutex)                                         │
│  │   ├── RLock: Get, GetBatch, Stats                        │
│  │   └── Lock:  Put, PutBatch, Delete, Flush, Close         │
│  │                                                          │
│  ├── metaStore.mu (RWMutex)                                 │
│  │   └── 保护: entries, idToHash                            │
│  │                                                          │
│  └── 保护: writeBuffer, file operations                      │
│                                                              │
└─────────────────────────────────────────────────────────────┘

⚠️ 注意: 存在锁顺序依赖，必须遵循 DB → Collection → Storage
```

---

## 7. 总结

### 架构优点
1. **分层清晰**: API → Collection → Index/Storage 职责明确
2. **格式先进**: Lance 列存储支持高效压缩和向量化读取
3. **功能完整**: 支持 CRUD、向量搜索、元数据过滤
4. **并发安全**: 多层锁保护，无 race condition

### 关键问题
1. **性能**: Get() O(n) 复杂度和 Search 多次磁盘读取
2. **存储**: Delete 不物理删除，Flush 全量重写
3. **扩展**: 缺少缓存层和索引层

### 下一步优化方向
1. 添加 LRU 缓存解决 Get() 性能问题
2. 实现后台 Compact 清理已删除数据
3. 支持 Append 写入减少写入放大
