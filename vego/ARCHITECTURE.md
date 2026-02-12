# Vego 层数据流架构

## 概述

Vego 层提供了一个统一的 API，将 HNSW 向量索引和 Lance 列存储整合在一起，为用户提供简单易用的文档存储和向量搜索能力。

---

## 数据结构关系

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Collection                                     │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │  Memory State                                                          │  │
│  │  ┌─────────────┐    ┌─────────────┐    ┌─────────────────────────┐   │  │
│  │  │  docToNode  │    │  nodeToDoc  │    │      HNSW Index         │   │  │
│  │  │  map[string]│◄──►│  map[int]   │    │  ┌─────────────────┐    │   │  │
│  │  │    int      │    │   string    │    │  │  nodes[]        │    │   │  │
│  │  └─────────────┘    └─────────────┘    │  │  entryPoint     │    │   │  │
│  │                                         │  │  maxLevel       │    │   │  │
│  │  Document ID ↔ Node ID 映射             │  │  globalLock     │    │   │  │
│  │                                         │  └─────────────────┘    │   │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                    │                                        │
│                                    ▼                                        │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                    DocumentStorage                                     │  │
│  │  ┌──────────────────┐    ┌──────────────────────────────────────┐   │  │
│  │  │   Write Buffer   │    │        metadataStore                  │   │  │
│  │  │  []*Document     │    │  ┌─────────────┐  ┌──────────────┐   │   │  │
│  │  │  (未 flush 数据) │    │  │  entries    │  │   idToHash   │   │   │  │
│  │  └──────────────────┘    │  │ map[int64]  │  │ map[string]  │   │   │  │
│  │                          │  │  docMeta    │  │   int64      │   │   │  │
│  │                          │  └─────────────┘  └──────────────┘   │   │  │
│  │                          └──────────────────────────────────────┘   │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 写入流程 (Insert)

```
User: coll.Insert(doc)

┌──────────────────────────────────────────────────────────────────────────────┐
│ Step 1: API 层参数校验                                                        │
│ Collection.Insert(doc)                                                       │
│ ├── doc.Validate(dimension)     // 检查向量维度匹配                          │
│ └── 检查 docToNode 中是否已存在                                               │
└──────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ Step 2: 写入 HNSW 索引 (内存操作)                                             │
│ index.Add(doc.Vector)                                                        │
│ ├── globalLock.Lock()                                                        │
│ ├── nodeID := len(nodes)      // 分配新节点 ID                               │
│ ├── nodes = append(nodes, NewNode(nodeID, vector, level))                   │
│ ├── globalLock.Unlock()                                                      │
│ └── insert(node)              // 建立层级连接 (HNSW 算法)                     │
│     └── 每层搜索最近邻居，建立双向连接                                         │
└──────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ Step 3: 写入 DocumentStorage (缓冲)                                           │
│ storage.Put(doc)                                                             │
│ ├── 检查 writeBuffer 中是否已存在                                             │
│ ├── 如果存在：从 buffer 中删除旧版本                                          │
│ ├── writeBuffer = append(writeBuffer, doc.Clone())                          │
│ └── bufferSize++                                                             │
│                                                                              │
│ ⚠️ 此时数据还未写入磁盘，仅在内存中!                                         │
└──────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ Step 4: 更新映射关系 (内存)                                                   │
│ ├── docToNode[doc.ID] = nodeID                                               │
│ └── nodeToDoc[nodeID] = doc.ID                                               │
└──────────────────────────────────────────────────────────────────────────────┘
                                      │
           ┌──────────────────────────┴──────────────────────────┐
           │ (bufferSize >= 1000 或调用 Close/Save)               │
           ▼                                                      │
┌──────────────────────────────────────────────────────────────────────────────┐
│ Step 5: Flush 到磁盘                                                          │
│ storage.flush()                                                              │
│ ├── 读取现有文档: readAllDocuments()  // 从 vectors.lance 读取               │
│ ├── 合并: allDocs = append(existingDocs, writeBuffer...)                     │
│ ├── rewriteStorage(allDocs)                                                  │
│ │   ├── 更新 metadataStore.entries (idHash → docMeta)                        │
│ │   ├── 更新 metadataStore.idToHash (docID → idHash)                         │
│ │   ├── saveMetadata() → metadata.json                                       │
│ │   └── writeColumnStorage(allDocs) → vectors.lance                          │
│ └── 清空 writeBuffer, bufferSize = 0                                         │
└──────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ Step 6: 写入 Lance 格式文件                                                   │
│ column.Writer                                                                │
│ ├── NewWriter(filename, schema, factory)                                     │
│ ├── 构建 Arrow Arrays:                                                       │
│ │   ├── id_hash:    Int64Array    [hashID(doc1), hashID(doc2), ...]         │
│ │   ├── vector:     FixedSizeListArray  [[v1...], [v2...], ...]             │
│ │   └── timestamp:  Int64Array    [ts1, ts2, ...]                           │
│ ├── Create RecordBatch                                                       │
│ ├── WriteRecordBatch()  // 压缩编码后写入 Pages                               │
│ └── Close()  // 写入 Footer (PageIndex)，重写 Header                          │
└──────────────────────────────────────────────────────────────────────────────┘

磁盘文件结构:
collection_path/
├── vectors.lance          # Lance 格式列存储
│   ├── Header  (8KB)      # Schema, NumRows
│   ├── Page 0: id_hash    # 压缩的 int64 数组
│   ├── Page 1: vector     # 压缩的向量数组
│   ├── Page 2: timestamp  # 压缩的 int64 数组
│   └── Footer             # PageIndex (偏移量索引)
└── metadata.json          # ID 映射和元数据
    └── {entries: {...}, id_to_hash: {...}}
```

---

## 读取流程 (Get)

```
User: coll.Get(docID)

┌──────────────────────────────────────────────────────────────────────────────┐
│ Step 1: Collection 层转发                                                     │
│ Collection.Get(id)                                                           │
│ ├── c.mu.RLock()                                                             │
│ └── 直接转发给 storage.Get(id)                                               │
└──────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ Step 2: 检查 Write Buffer (内存优先)                                          │
│ storage.Get(id)                                                              │
│ └── 遍历 writeBuffer:                                                        │
│     └── if doc.ID == id: return doc.Clone()  // O(bufferSize)               │
└──────────────────────────────────────────────────────────────────────────────┘
                                      │ (未找到)
                                      ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ Step 3: 检查 Metadata Store (内存)                                            │
│ ├── metaStore.mu.RLock()                                                     │
│ ├── idHash := metaStore.idToHash[id]  // O(1) 哈希查找                       │
│ ├── meta := metaStore.entries[idHash] // 获取元数据                          │
│ └── metaStore.mu.RUnlock()                                                   │
└──────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ Step 4: 从 Column Storage 读取向量                                            │
│ storage.readVectorByHash(idHash)                                             │
│ ├── readAllDocuments()  // ⚠️ 读取整个 vectors.lance!                       │
│ │   ├── column.NewReader(dataFile)                                           │
│ │   ├── reader.ReadRecordBatch()  // 加载所有列数据                          │
│ │   └── 从 Arrow Arrays 提取数据                                             │
│ └── 线性扫描找到匹配的 idHash  // O(n)                                       │
│     └── for _, doc := range docs {                                           │
│         if hashID(doc.ID) == idHash: return doc.Vector                       │
│     }                                                                        │
└──────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ Step 5: 组装并返回 Document                                                   │
│ return &Document{                                                            │
│     ID:        meta.ID,                                                      │
│     Vector:    vector,                                                       │
│     Metadata:  meta.Metadata,                                                │
│     Timestamp: time.Unix(0, timestamp),                                      │
│ }                                                                            │
└──────────────────────────────────────────────────────────────────────────────┘

⚠️ 性能问题:
- Get() 调用 readAllDocuments() → 每次读取整个 vectors.lance 文件
- 1000 个文档 = 读取全部 1000 条记录
- 时间复杂度: O(n) - 随数据量线性增长
```

---

## 搜索流程 (Search)

```
User: coll.Search(queryVector, k)

┌──────────────────────────────────────────────────────────────────────────────┐
│ Step 1: 参数处理和选项解析                                                    │
│ Collection.Search(query, k, opts...)                                         │
│ ├── 检查 query 维度与 collection.dimension 匹配                              │
│ └── 解析 SearchOptions (EF, Filter)                                          │
└──────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ Step 2: HNSW 向量搜索 (纯内存操作)                                            │
│ index.Search(query, k, ef)                                                   │
│ ├── globalLock.RLock()                                                       │
│ ├── ep = entryPoint, maxLvl = maxLevel                                       │
│ └── globalLock.RUnlock()                                                     │
│                                                                              │
│ h.search(query, k, ef, ep, maxLvl)                                           │
│ ├── Phase 1: 从顶层向下定位入口                                              │
│ │   for lvl := maxLvl; lvl > 0; lvl-- {                                     │
│ │       ep = searchLayer(query, ep, 1, lvl)[0]  // 每层找最近的 1 个        │
│ │   }                                                                        │
│ └── Phase 2: 在 Level 0 (最底层) 精确搜索                                    │
│       candidates = searchLayer(query, ep, ef, 0)                            │
│       return candidates[:k]  // 返回最近的 k 个                              │
│                                                                              │
│ 返回: []SearchResult{{ID: nodeID, Distance: distance}, ...}                 │
└──────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ Step 3: Node ID → Document ID 映射                                            │
│ 遍历 HNSW 搜索结果:                                                           │
│ for _, hr := range hnswResults {                                             │
│     docID, exists := nodeToDoc[hr.ID]  // 内存 O(1) 查找                     │
│     if !exists:                                                              │
│         log.Printf("Warning: orphaned node %d", hr.ID)                       │
│         continue  // 跳过孤儿节点 (已删除但未清理)                           │
│     ...                                                                      │
│ }                                                                            │
└──────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ Step 4: 加载完整文档 (可能触发磁盘读取)                                       │
│ for each docID from step 3:                                                  │
│     doc, err := storage.Get(docID)  // 同 Get() 流程!                       │
│     ├── 检查 writeBuffer                                                     │
│     ├── 检查 metaStore (idHash, meta)                                        │
│     └── readVectorByHash(idHash)  // ⚠️ 再次全表扫描!                       │
│                                                                              │
│ ⚠️ 性能问题:                                                                 │
│ Search 返回 k=10 个结果                                                      │
│ → 10 次 storage.Get() 调用                                                   │
│ → 10 次 readAllDocuments() (读取整个文件)                                    │
│ → 10 × n 的时间复杂度!                                                       │
└──────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ Step 5: 组装搜索结果                                                          │
│ results := []SearchResult{                                                   │
│     {Document: doc, Distance: hr.Distance},                                  │
│     ...                                                                      │
│ }                                                                            │
│ return results                                                               │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 过滤搜索流程 (SearchWithFilter)

```
User: coll.SearchWithFilter(query, k, filter)

┌──────────────────────────────────────────────────────────────────────────────┐
│ SearchWithFilter 实现策略: 动态扩展搜索范围                                   │
│                                                                              │
│ 初始 batchSize = k * 2                                                       │
│ maxBatchSize = k * 20                                                        │
│ maxAttempts = 5                                                              │
│                                                                              │
│ for attempt := 0; attempt < maxAttempts; attempt++ {                        │
│     results := Search(query, batchSize)  // 获取 batchSize 个候选           │
│                                                                              │
│     // 应用过滤器                                                            │
│     filtered := []SearchResult{}                                             │
│     for _, r := range results {                                              │
│         if filter.Match(r.Document) {                                        │
│             filtered = append(filtered, r)                                  │
│             if len(filtered) >= k:                                           │
│                 return filtered[:k]  // 找到足够的匹配                       │
│         }                                                                    │
│     }                                                                        │
│                                                                              │
│     // 未找到足够匹配，扩大搜索范围                                          │
│     batchSize *= 2  // 指数增长                                              │
│ }                                                                            │
│ return filtered  // 返回所有找到的匹配 (可能 < k)                            │
└──────────────────────────────────────────────────────────────────────────────┘

⚠️ 性能特点:
- 每次 Search() 都会读取完整的 vectors.lance
- 过滤在内存中进行，只过滤 metadata (已从 metadata.json 加载)
- 最坏情况: 5 次 Search() 调用，读取 5 次完整文件
```

---

## 性能瓶颈分析

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                              性能瓶颈                                        │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  1. Get() O(n) 复杂度                                                        │
│  ━━━━━━━━━━━━━━━━━━━━                                                        │
│  storage.Get(id)                                                             │
│      └── readVectorByHash(idHash)                                            │
│          └── readAllDocuments()  // 读取整个文件!                            │
│              └── column.NewReader().ReadRecordBatch()                        │
│                                                                              │
│  影响: 10K 文档时，单次 Get 需要读取 10K 条记录                              │
│                                                                              │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  2. Search() 多次磁盘读取                                                    │
│  ━━━━━━━━━━━━━━━━━━━━━━━━                                                    │
│  Search(k=10)                                                                │
│      ├── index.Search()          // O(log n)，很快                          │
│      └── for i=0; i<10; i++ {                                                │
│            storage.Get(docID)    // 每次都要读完整文件!                     │
│        }                                                                     │
│                                                                              │
│  影响: 返回 10 个结果 = 10 次全文件扫描                                      │
│                                                                              │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  3. Flush 全量重写                                                           │
│  ━━━━━━━━━━━━━━━━━━                                                          │
│  storage.flush()                                                             │
│      ├── readAllDocuments()      // 读全部现有数据                          │
│      ├── append writeBuffer      // 合并新数据                               │
│      └── rewriteStorage(allDocs) // 写全部数据 (100K+1000=101K)             │
│                                                                              │
│  影响: 100K 文档时，写入 1000 条需要重写 101K 条                              │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 优化建议

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                             优化路线图                                       │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  短期 (P0) - 立即实施                                                        │
│  ━━━━━━━━━━━━━━━━━━━━                                                        │
│  1. LRU Cache                                                                │
│     ├── 缓存最近读取的 Document                                              │
│     ├── 命中时避免磁盘读取                                                   │
│     └── 预期提升: 80%+ 命中率，延迟降低 50-100x                             │
│                                                                              │
│  2. Row Index                                                                │
│     ├── Flush 时构建 idHash → rowIndex 映射                                  │
│     ├── Get() 直接定位到指定行                                               │
│     └── 预期提升: O(n) → O(1)                                                │
│                                                                              │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  中期 (P1) - 后续优化                                                        │
│  ━━━━━━━━━━━━━━━━━━━━                                                        │
│  3. 向量缓存                                                                 │
│     └── 将 vectors.lance 中的向量列常驻内存                                  │
│                                                                              │
│  4. 批量读取优化                                                             │
│     └── Search() 时一次性读取所有需要的文档                                  │
│                                                                              │
│  5. Append 写入                                                              │
│     └── 支持追加写入，避免全量重写                                           │
│                                                                              │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  长期 (P2) - 架构升级                                                        │
│  ━━━━━━━━━━━━━━━━━━━━                                                        │
│  6. 内存映射 (MMap)                                                          │
│     └── 大文件内存映射，减少拷贝开销                                         │
│                                                                              │
│  7. 异步 I/O                                                                 │
│     └── 批量并发读取，隐藏 I/O 延迟                                          │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## 总结

Vego 层的核心设计特点：

1. **三层映射关系**
   - `docID ↔ nodeID`：Collection 层内存映射
   - `docID ↔ idHash`：metadataStore 内存映射
   - `idHash → row`：vectors.lance 中的位置 (目前线性扫描)

2. **双存储策略**
   - **HNSW Index**：内存中的向量索引，支持快速近似搜索
   - **Lance Storage**：磁盘列存储，支持持久化和压缩

3. **写缓冲机制**
   - 小批量写入先进入内存 buffer
   - 达到阈值或手动 Flush 时才写入磁盘
   - 减少磁盘 I/O，但增加了数据丢失风险

4. **当前性能瓶颈**
   - Get/Search 都需要全表扫描读取
   - 急需添加缓存和索引优化
