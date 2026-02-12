# Vego 测试计划

## 当前状态

- **当前覆盖率**: 55.3%
- **目标覆盖率 (Phase 0)**: 70%
- **测试文件**: 3 个
- **最后更新**: 2026-02-12

---

## 测试文件清单

| 文件 | 状态 | 说明 |
|------|------|------|
| `collection_test.go` | ✅ 存在 | Collection CRUD 测试 |
| `filter_test.go` | ✅ 存在 | 基础过滤测试 |
| `example_test.go` | ✅ 存在 | 使用示例 |
| `db_test.go` | ❌ 缺失 | DB 层测试 |
| `document_test.go` | ❌ 缺失 | Document 层测试 |
| `config_test.go` | ❌ 缺失 | 配置测试 |
| `benchmark_test.go` | ❌ 缺失 | 性能基准 |

---

## 测试任务清单

### P0: 立即补充（阻塞 Phase 0 完成）

#### 1. DB 层测试 (`db_test.go`)

```go
// 基础功能
func TestOpen(t *testing.T)              // 打开/创建数据库
func TestOpenWithOptions(t *testing.T)   // 使用选项打开
func TestDBClose(t *testing.T)           // 关闭数据库
func TestDBCollection(t *testing.T)      // 获取/创建集合
func TestDBDropCollection(t *testing.T)  // 删除集合
func TestDBCollections(t *testing.T)     // 列出所有集合

// 持久化
func TestDBPersistence(t *testing.T)     // 数据库持久化验证
func TestDBMultipleCollections(t *testing.T) // 多集合操作

// 错误处理
func TestOpenInvalidPath(t *testing.T)   // 无效路径
func TestDBCollectionClosed(t *testing.T) // 已关闭数据库操作
```

#### 2. Document 层测试 (`document_test.go`)

```go
// 基础功能
func TestDocumentID(t *testing.T)         // ID 生成
func TestDocumentValidate(t *testing.T)   // 文档验证
func TestDocumentClone(t *testing.T)      // 文档克隆

// 验证边界
func TestDocumentValidateEmptyID(t *testing.T)        // 空 ID
func TestDocumentValidateWrongDimension(t *testing.T) // 错误维度
func TestDocumentValidateNilVector(t *testing.T)      // 空向量

// 克隆验证
func TestDocumentCloneDeepCopy(t *testing.T) // 深度复制验证
```

#### 3. 完整过滤测试 (补充到 `filter_test.go`)

```go
// 所有操作符
func TestMetadataFilterEq(t *testing.T)      // 等于
func TestMetadataFilterNe(t *testing.T)      // 不等于
func TestMetadataFilterGt(t *testing.T)      // 大于
func TestMetadataFilterGte(t *testing.T)     // 大于等于
func TestMetadataFilterLt(t *testing.T)      // 小于
func TestMetadataFilterLte(t *testing.T)     // 小于等于
func TestMetadataFilterIn(t *testing.T)      // 在列表中
func TestMetadataFilterContains(t *testing.T) // 包含子串

// 复合过滤
func TestAndFilter(t *testing.T)            // AND 组合
func TestOrFilter(t *testing.T)             // OR 组合
func TestNestedFilter(t *testing.T)         // 嵌套组合

// 边界情况
func TestFilterMissingField(t *testing.T)   // 字段不存在
func TestFilterTypeMismatch(t *testing.T)   // 类型不匹配
func TestFilterNilMetadata(t *testing.T)    // 空元数据
```

#### 4. Collection 补充测试 (补充到 `collection_test.go`)

```go
// 批量操作
func TestCollectionGetBatch(t *testing.T)       // 批量获取
func TestCollectionDeleteBatch(t *testing.T)    // 批量删除
func TestCollectionUpsertBatch(t *testing.T)    // 批量 upsert

// 搜索功能
func TestCollectionSearchWithFilter(t *testing.T) // 过滤搜索
func TestCollectionSearchBatch(t *testing.T)      // 批量搜索

// 分页
func TestCollectionSearchPagination(t *testing.T) // 分页搜索
```

#### 5. 配置测试 (`config_test.go`)

```go
// 默认值
func TestDefaultConfig(t *testing.T)       // 默认配置

// 选项函数
func TestWithDimension(t *testing.T)       // 维度选项
func TestWithAdaptive(t *testing.T)        // 自适应选项
func TestWithExpectedSize(t *testing.T)    // 预期大小
func TestWithDistanceFunc(t *testing.T)    // 距离函数
func TestWithM(t *testing.T)               // M 参数
func TestWithEfConstruction(t *testing.T)   // EfConstruction

// 配置验证
func TestConfigValidation(t *testing.T)    // 配置验证
```

---

### P1: 短期补充（Phase 0-1）

#### 6. 持久化测试

```go
// collection_test.go 中添加
func TestCollectionPersistence(t *testing.T) {
    // 1. 创建集合并插入数据
    // 2. 关闭集合
    // 3. 重新打开集合
    // 4. 验证数据完整
    // 5. 验证索引可用
}

func TestCollectionReload(t *testing.T) {
    // 验证重新加载后映射关系正确
}

func TestCollectionSaveConsistency(t *testing.T) {
    // 验证 Save 后数据一致性
}
```

#### 7. 并发测试

```go
// collection_test.go 中添加
func TestCollectionConcurrentInsert(t *testing.T) {
    // 多个 goroutine 同时插入
}

func TestCollectionConcurrentReadWrite(t *testing.T) {
    // 读写并发
}

func TestCollectionConcurrentSearch(t *testing.T) {
    // 并发搜索
}

func TestCollectionRaceCondition(t *testing.T) {
    // 竞态条件检测
}
```

#### 8. 边界测试

```go
// collection_test.go 中添加
func TestCollectionEmpty(t *testing.T) {
    // 空集合操作
}

func TestCollectionLargeMetadata(t *testing.T) {
    // 超大元数据
}

func TestCollectionSpecialCharactersID(t *testing.T) {
    // 特殊字符 ID
    // Unicode、空格、符号等
}

func TestCollectionMaxDimension(t *testing.T) {
    // 最大维度测试 (1536+)
}
```

---

### P2: 中期补充（Phase 1-2）

#### 9. 性能基准测试 (`benchmark_test.go`)

```go
// 插入性能
func BenchmarkInsert(b *testing.B)          // 单条插入
func BenchmarkInsertBatch(b *testing.B)     // 批量插入

// 搜索性能
func BenchmarkSearch(b *testing.B)          // 单次搜索
func BenchmarkSearchBatch(b *testing.B)     // 批量搜索
func BenchmarkSearchWithFilter(b *testing.B) // 过滤搜索

// 获取性能
func BenchmarkGet(b *testing.B)             // 单条获取
func BenchmarkGetBatch(b *testing.B)        // 批量获取

// 不同规模
func BenchmarkSearch1K(b *testing.B)        // 1K 文档
func BenchmarkSearch10K(b *testing.B)       // 10K 文档
func BenchmarkSearch100K(b *testing.B)      // 100K 文档
```

#### 10. 故障注入测试

```go
// collection_test.go 中添加
func TestCollectionDiskFull(t *testing.T) {
    // 磁盘满错误处理
}

func TestCollectionPermissionDenied(t *testing.T) {
    // 权限错误
}

func TestCollectionCorruption(t *testing.T) {
    // 文件损坏检测
}

func TestCollectionRecovery(t *testing.T) {
    // 错误恢复
}
```

#### 11. 大容量测试

```go
// collection_test.go 中添加
func TestCollectionLargeDataset(t *testing.T) {
    // 10万+ 文档
}

func TestCollectionMemoryUsage(t *testing.T) {
    // 内存使用监控
}
```

---

## 测试实现模板

### 基础测试模板

```go
func TestXxx(t *testing.T) {
    // 设置
    coll, cleanup := setupTestCollection(t)
    defer cleanup()
    
    // 测试数据
    doc := createTestDocument("test", 64, map[string]interface{}{
        "key": "value",
    })
    
    // 执行
    err := coll.Insert(doc)
    
    // 验证
    if err != nil {
        t.Errorf("Insert failed: %v", err)
    }
    
    retrieved, err := coll.Get("test")
    if err != nil {
        t.Errorf("Get failed: %v", err)
    }
    
    if retrieved.ID != "test" {
        t.Errorf("Expected ID test, got %s", retrieved.ID)
    }
}
```

### 并发测试模板

```go
func TestConcurrentXxx(t *testing.T) {
    coll, cleanup := setupTestCollection(t)
    defer cleanup()
    
    const numGoroutines = 10
    const numOps = 100
    
    var wg sync.WaitGroup
    wg.Add(numGoroutines)
    
    for i := 0; i < numGoroutines; i++ {
        go func(id int) {
            defer wg.Done()
            
            for j := 0; j < numOps; j++ {
                docID := fmt.Sprintf("goroutine_%d_doc_%d", id, j)
                doc := createTestDocument(docID, 64, nil)
                
                if err := coll.Insert(doc); err != nil {
                    t.Errorf("Insert failed: %v", err)
                }
            }
        }(i)
    }
    
    wg.Wait()
    
    // 验证总数
    stats := coll.Stats()
    if stats.Count != numGoroutines*numOps {
        t.Errorf("Expected %d documents, got %d", numGoroutines*numOps, stats.Count)
    }
}
```

### 基准测试模板

```go
func BenchmarkXxx(b *testing.B) {
    coll, cleanup := setupTestCollection(&testing.T{})
    defer cleanup()
    
    // 准备数据
    for i := 0; i < 1000; i++ {
        doc := createTestDocument(fmt.Sprintf("doc_%d", i), 64, nil)
        coll.Insert(doc)
    }
    
    query := make([]float32, 64)
    for i := range query {
        query[i] = float32(i) * 0.01
    }
    
    b.ResetTimer()
    
    for i := 0; i < b.N; i++ {
        _, err := coll.Search(query, 10)
        if err != nil {
            b.Fatal(err)
        }
    }
}
```

---

## 测试运行命令

```bash
# 运行所有测试
go test ./vego/...

# 运行带覆盖率
go test -coverprofile=coverage.out ./vego/...

# 查看覆盖率报告
go tool cover -html=coverage.out

# 运行基准测试
go test -bench=. ./vego/...

# 运行竞态检测
go test -race ./vego/...

# 运行特定测试
go test -run TestCollectionInsert ./vego/...
```

---

## 覆盖率检查清单

### Phase 0 目标 (70%)

- [ ] `db.go` - 当前 0%，目标 80%
- [ ] `document.go` - 当前 0%，目标 90%
- [ ] `query.go` - 当前 30%，目标 80%
- [ ] `config.go` - 当前 0%，目标 90%
- [ ] `collection.go` - 当前 70%，目标 75%

### Phase 1 目标 (75%)

- [ ] 添加持久化测试
- [ ] 添加并发测试
- [ ] 添加边界测试

### Phase 2 目标 (80%)

- [ ] 添加性能基准
- [ ] 添加故障注入测试
- [ ] 添加大容量测试

---

## 注意事项

1. **测试隔离**: 每个测试使用独立临时目录
2. **资源清理**: 使用 `defer cleanup()` 确保清理
3. **并行执行**: 使用 `t.Parallel()` 加速测试
4. **超时控制**: 大测试设置合理超时
5. **数据验证**: 不要只验证无错误，验证数据正确性

---

*本文档随开发进度更新，最后更新: 2026-02-12*
