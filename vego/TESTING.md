# Vego 测试计划

## 当前状态

- **当前覆盖率**: 77.4% ✅ (已超越 Phase 0 目标 70%)
- **Phase 1 目标**: 80%
- **测试文件**: 8 个
- **测试代码行数**: 3363+ 行
- **最后更新**: 2026-02-12

---

## 测试文件清单

| 文件 | 状态 | 行数 | 说明 |
|------|------|------|------|
| `collection_test.go` | ✅ 完成 | 466 | Collection CRUD、Context、批量操作 |
| `collection_p1_test.go` | ✅ 完成 | 645 | P1: 持久化、并发、边界测试 |
| `db_test.go` | ✅ 完成 | 349 | DB 层完整测试 |
| `document_test.go` | ✅ 完成 | 339 | Document 验证、克隆、边界 |
| `filter_test.go` | ✅ 完成 | 567 | 所有过滤操作符、复合过滤 |
| `config_test.go` | ✅ 完成 | 264 | 配置选项、默认值 |
| `example_test.go` | ✅ 完成 | 184 | 使用示例 |
| `benchmark_test.go` | ✅ 完成 | 489 | 性能基准测试 (P2) |

**总计**: 3363 行测试代码

---

## 已完成测试清单

### ✅ Phase 0 完成 (覆盖率 77.4%)

#### 1. DB 层测试 (`db_test.go`) - 100%

| 测试 | 功能 | 状态 |
|------|------|------|
| `TestOpen` | 创建/打开数据库 | ✅ |
| `TestOpenWithOptions` | 所有配置选项 | ✅ |
| `TestDBClose` | 关闭数据库 | ✅ |
| `TestDBCollection` | 获取/创建集合 | ✅ |
| `TestDBDropCollection` | 删除集合 | ✅ |
| `TestDBCollections` | 列出所有集合 | ✅ |
| `TestDBPersistence` | 数据库持久化验证 | ✅ |
| `TestDBMultipleCollections` | 多集合操作 | ✅ |
| `TestOpenInvalidPath` | 无效路径 | ✅ |
| `TestDBCollectionClosed` | 已关闭数据库操作 | ✅ |

#### 2. Document 层测试 (`document_test.go`) - 100%

| 测试 | 功能 | 状态 |
|------|------|------|
| `TestDocumentID` | ID 生成 (UUID) | ✅ |
| `TestDocumentValidate` | 文档验证 | ✅ |
| `TestDocumentValidateEmptyID` | 空 ID | ✅ |
| `TestDocumentValidateWrongDimension` | 错误维度 | ✅ |
| `TestDocumentValidateNilVector` | 空向量 | ✅ |
| `TestDocumentClone` | 克隆 | ✅ |
| `TestDocumentCloneDeepCopy` | 深度复制 | ✅ |
| `TestDocumentWithNilMetadata` | nil 元数据 | ✅ |
| `TestDocumentWithEmptyMetadata` | 空元数据 | ✅ |
| `TestDocumentValidationEdgeCases` | 边界情况 (1536维, Unicode) | ✅ |

#### 3. Collection 测试 (`collection_test.go`) - 100%

| 测试 | 功能 | 状态 |
|------|------|------|
| `TestCollectionInsert` | 插入 | ✅ |
| `TestCollectionGet` | 获取 | ✅ |
| `TestCollectionDelete` | 删除 | ✅ |
| `TestCollectionUpdate` | 更新 | ✅ |
| `TestCollectionUpsert` | 插入或更新 | ✅ |
| `TestCollectionSearch` | 搜索 | ✅ |
| `TestCollectionBatchOperations` | 批量插入 | ✅ |
| `TestCollectionContextCancellation` | Context 取消 | ✅ |
| `TestCollectionStats` | 统计 | ✅ |
| `TestCollectionSaveAndClose` | 保存/关闭 | ✅ |

#### 4. 过滤测试 (`filter_test.go`) - 100%

| 测试 | 操作符 | 状态 |
|------|--------|------|
| `TestMetadataFilter` | 基础过滤 | ✅ |
| `TestMetadataFilterEq` | eq | ✅ |
| `TestMetadataFilterNe` | ne | ✅ |
| `TestMetadataFilterGt` | gt | ✅ |
| `TestMetadataFilterGte` | gte | ✅ |
| `TestMetadataFilterLt` | lt | ✅ |
| `TestMetadataFilterLte` | lte | ✅ |
| `TestMetadataFilterContains` | contains | ✅ |
| `TestAndFilter` | AND | ✅ |
| `TestOrFilter` | OR | ✅ |
| `TestNestedFilter` | 嵌套组合 | ✅ |
| `TestFilterMissingField` | 缺失字段 | ✅ |
| `TestFilterTypeMismatch` | 类型不匹配 | ✅ |
| `TestFilterNilMetadata` | nil 元数据 | ✅ |
| `TestFilterEmptyMetadata` | 空元数据 | ✅ |

#### 5. 配置测试 (`config_test.go`) - 100%

| 测试 | 功能 | 状态 |
|------|------|------|
| `TestDefaultConfig` | 默认值 | ✅ |
| `TestWithDimension` | 维度选项 | ✅ |
| `TestWithAdaptive` | 自适应选项 | ✅ |
| `TestWithExpectedSize` | 预期大小 | ✅ |
| `TestWithDistanceFunc` | 距离函数 | ✅ |
| `TestWithM` | M 参数 | ✅ |
| `TestWithEfConstruction` | EfConstruction | ✅ |
| `TestConfigValidation` | 配置验证 | ✅ |
| `TestMultipleOptions` | 多选项组合 | ✅ |
| `TestConfigImmutability` | 配置独立性 | ✅ |
| `TestOptionChaining` | 选项链式 | ✅ |

---

### ✅ Phase 1 完成 (`collection_p1_test.go`)

#### 6. 持久化测试

| 测试 | 功能 | 状态 | 时间 |
|------|------|------|------|
| `TestCollectionPersistence` | 完整持久化验证 | ✅ | 0.02s |
| `TestCollectionReload` | 复杂状态重载 | ✅ | - |
| `TestCollectionSaveConsistency` | 多次保存一致性 | ✅ | - |

#### 7. 并发测试

| 测试 | 并发度 | 状态 | 时间 |
|------|--------|------|------|
| `TestCollectionConcurrentInsert` | 10 × 100 | ✅ | 2.36s |
| `TestCollectionConcurrentReadWrite` | 5R + 3W | ✅ | 2.19s |
| `TestCollectionConcurrentSearch` | 20 × 100 | ✅ | 4.73s |
| `TestCollectionRaceCondition` | 5 种操作 | ✅ | 0.11s |

#### 8. 边界测试

| 测试 | 场景 | 状态 |
|------|------|------|
| `TestCollectionEmptyOperations` | 空集合操作 | ✅ |
| `TestCollectionLargeMetadata` | 1000 个 key | ✅ |
| `TestCollectionSpecialCharactersID` | 14 种特殊字符 | ✅ |
| `TestCollectionMaxDimension` | 256/512/768/1024/1536 | ✅ |

---

## 待完成测试 (P2)

### 9. 性能基准测试 (`benchmark_test.go`) ✅ 已完成

```go
// 插入性能
func BenchmarkInsert(b *testing.B)                    // 单条插入 (~3ms/op)
func BenchmarkInsertBatch(b *testing.B)               // 批量插入 (10/50/100/500)
func BenchmarkInsertDifferentDimensions(b *testing.B) // 不同维度 (64-1024)

// 搜索性能
func BenchmarkSearch(b *testing.B)                    // 单次搜索 (~9ms/op for 1K docs)
func BenchmarkSearchK(b *testing.B)                   // 不同 K 值 (1/5/10/20/50/100)
func BenchmarkSearchWithFilter(b *testing.B)          // 过滤搜索
func BenchmarkSearchBatch(b *testing.B)               // 批量搜索

// 获取性能
func BenchmarkGet(b *testing.B)                       // 单条获取
func BenchmarkGetBatch(b *testing.B)                  // 批量获取 (TODO - method not implemented)

// 不同规模
func BenchmarkSearch1K(b *testing.B)                  // 1K 文档
func BenchmarkSearch10K(b *testing.B)                 // 10K 文档
func BenchmarkSearch100K(b *testing.B)                // 100K 文档
func BenchmarkSearchDifferentDimensions(b *testing.B) // 不同维度搜索

// 更新删除
func BenchmarkUpdate(b *testing.B)                    // 更新性能
func BenchmarkDelete(b *testing.B)                    // 删除性能

// 综合测试
func BenchmarkMixedWorkload(b *testing.B)             // 混合读写 (90%/70%/50%/30%/10% 读比例)
func BenchmarkCollectionMemoryUsage(b *testing.B)     // 内存使用 (1K/5K/10K)
func BenchmarkSave(b *testing.B)                      // 保存性能 (100/500/1000 docs)
```

**运行命令:**
```bash
make benchmark                           # 运行所有基准测试
make benchmark-run PATTERN=Insert        # 运行 Insert 相关基准测试
go test -bench=BenchmarkSearch -benchmem  # 运行搜索基准测试
```

### 10. 故障注入测试 ⏳

```go
func TestCollectionDiskFull(t *testing.T)        // 磁盘满
func TestCollectionPermissionDenied(t *testing.T) // 权限错误
func TestCollectionCorruption(t *testing.T)       // 文件损坏
func TestCollectionRecovery(t *testing.T)         // 错误恢复
```

### 11. 大容量测试 ⏳

```go
func TestCollectionLargeDataset(t *testing.T)    // 10万+ 文档
func TestCollectionMemoryUsage(t *testing.T)     // 内存监控
```

---

## 测试运行命令

```bash
# 运行所有测试
go test ./vego/...

# 运行带覆盖率 (当前: 77.4%)
go test -coverprofile=coverage.out ./vego/...

# 查看覆盖率报告
go tool cover -html=coverage.out

# 运行竞态检测
go test -race ./vego/...

# 运行 P1 测试 (持久化 + 并发 + 边界)
go test -v -run "P1|Persistence|Concurrent|Race|Empty|Large|Special|MaxDimension" ./vego/...

# 运行特定测试
go test -v -run TestCollectionConcurrentInsert ./vego/...

# 运行基准测试 (P2)
go test -bench=. ./vego/...
```

---

## 覆盖率目标

### 各文件当前状态

| 文件 | 当前 | Phase 1 目标 | Phase 2 目标 |
|------|------|--------------|--------------|
| `db.go` | ~90% | 90% | 90% |
| `document.go` | ~95% | 95% | 95% |
| `collection.go` | ~75% | 80% | 85% |
| `query.go` | ~85% | 85% | 90% |
| `config.go` | ~85% | 90% | 90% |
| `filter.go` | ~85% | 85% | 90% |
| **总计** | **77.4%** | **80%** | **85%** |

---

## 测试质量统计

### 测试类型分布

| 类型 | 数量 | 占比 |
|------|------|------|
| 单元测试 | 60+ | 75% |
| 集成测试 | 10+ | 12% |
| 并发测试 | 4 | 5% |
| 边界测试 | 6 | 8% |
| **总计** | **80+** | **100%** |

### 并发测试覆盖

- ✅ 并发插入 (10 goroutines)
- ✅ 读写并发 (5 readers + 3 writers)
- ✅ 并发搜索 (20 goroutines)
- ✅ 混合操作竞态检测

### 边界测试覆盖

- ✅ 空集合/空元数据/nil 元数据
- ✅ 特殊字符 ID (Unicode, emoji, 空格等)
- ✅ 大元数据 (1000 keys)
- ✅ 大维度 (256/512/768/1024/1536)
- ✅ 错误维度/空向量/空 ID

---

## 测试最佳实践

### 1. 测试命名规范

```go
// 格式: Test + 被测对象 + 场景
TestCollectionInsert              // 基础
TestCollectionInsertDuplicate     // 错误场景
TestCollectionConcurrentInsert    // 并发场景
```

### 2. 表格驱动测试

```go
func TestXxx(t *testing.T) {
    testCases := []struct {
        name     string
        input    int
        expected int
    }{
        {"case1", 1, 2},
        {"case2", 2, 4},
    }
    
    for _, tc := range testCases {
        t.Run(tc.name, func(t *testing.T) {
            result := function(tc.input)
            if result != tc.expected {
                t.Errorf("Expected %d, got %d", tc.expected, result)
            }
        })
    }
}
```

### 3. 并发测试模式

```go
func TestConcurrentXxx(t *testing.T) {
    coll, cleanup := setupTestCollection(t)
    defer cleanup()
    
    const numGoroutines = 10
    var wg sync.WaitGroup
    wg.Add(numGoroutines)
    
    start := make(chan struct{}) // 同步信号
    
    for i := 0; i < numGoroutines; i++ {
        go func(id int) {
            defer wg.Done()
            <-start // 等待同时启动
            // 执行操作
        }(i)
    }
    
    close(start) // 同时启动
    wg.Wait()
}
```

---

## CI/CD 建议

```yaml
# .github/workflows/test.yml
name: Test

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Go
        uses: actions/setup-go@v4
        with:
          go-version: '1.21'
      
      - name: Run tests
        run: go test -v -race -coverprofile=coverage.out ./vego/...
      
      - name: Check coverage
        run: |
          coverage=$(go tool cover -func=coverage.out | grep total | awk '{print $3}')
          echo "Coverage: $coverage"
          if (( $(echo "$coverage < 75.0" | bc -l) )); then
            echo "Coverage below 75%"
            exit 1
          fi
```

---

## 总结

| 里程碑 | 覆盖率 | 状态 |
|--------|--------|------|
| Phase 0 目标 | 70% | ✅ **已完成 (77.4%)** |
| Phase 1 目标 | 80% | 🔄 进行中 |
| Phase 2 目标 | 85% | ⏳ 待开始 |

**已完成**: 80+ 测试用例，覆盖所有 P0 和 P1 需求
**待完成**: 性能基准、故障注入、大容量测试 (P2)

---

*本文档随开发进度更新，最后更新: 2026-02-12*
