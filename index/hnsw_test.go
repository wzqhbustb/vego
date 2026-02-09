package hnsw

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"sync"
	"testing"
)

func TestHNSWBasic(t *testing.T) {
	// 创建索引
	config := Config{
		M:              16,
		EfConstruction: 200,
		Dimension:      128,
		DistanceFunc:   L2Distance,
		Seed:           42,
	}

	index := NewHNSW(config)

	// 添加一些向量
	numVectors := 1000
	vectors := make([][]float32, numVectors)

	for i := 0; i < numVectors; i++ {
		vector := make([]float32, 128)
		for j := range vector {
			vector[j] = rand.Float32()
		}
		vectors[i] = vector

		id, err := index.Add(vector)
		if err != nil {
			t.Fatalf("Failed to add vector %d: %v", i, err)
		}

		if id != i {
			t.Errorf("Expected ID %d, got %d", i, id)
		}
	}

	// 测试搜索
	query := vectors[0]
	k := 10
	results, err := index.Search(query, k, 0)

	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(results) != k {
		t.Errorf("Expected %d results, got %d", k, len(results))
	}

	// 第一个结果应该是查询向量本身（距离为0）
	if results[0].ID != 0 {
		t.Errorf("Expected first result to be ID 0, got %d", results[0].ID)
	}

	if results[0].Distance > 0.0001 {
		t.Errorf("Expected distance ~0, got %f", results[0].Distance)
	}

	t.Logf("Index size: %d", index.Len())
	t.Logf("Top 3 results: %+v", results[:3])
}

func TestHNSWEmpty(t *testing.T) {
	config := Config{
		Dimension: 128,
	}

	index := NewHNSW(config)

	query := make([]float32, 128)
	_, err := index.Search(query, 10, 0)

	if err != ErrEmptyIndex {
		t.Errorf("Expected ErrEmptyIndex, got %v", err)
	}
}

func TestDistanceFunctions(t *testing.T) {
	a := []float32{1, 2, 3}
	b := []float32{4, 5, 6}

	// L2 距离
	l2 := L2Distance(a, b)
	expected := float32(27) // (1-4)^2 + (2-5)^2 + (3-6)^2 = 9+9+9 = 27
	if l2 != expected {
		t.Errorf("L2Distance: expected %f, got %f", expected, l2)
	}

	// 内积距离
	ip := InnerProductDistance(a, b)
	expectedIP := float32(-32) // -(1*4 + 2*5 + 3*6) = -(4+10+18) = -32
	if ip != expectedIP {
		t.Errorf("InnerProductDistance: expected %f, got %f", expectedIP, ip)
	}
}

func BenchmarkHNSWInsert(b *testing.B) {
	config := Config{
		M:              16,
		EfConstruction: 200,
		Dimension:      128,
	}

	index := NewHNSW(config)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vector := make([]float32, 128)
		for j := range vector {
			vector[j] = rand.Float32()
		}
		index.Add(vector)
	}
}

func BenchmarkHNSWSearch(b *testing.B) {
	config := Config{
		M:              16,
		EfConstruction: 200,
		Dimension:      128,
	}

	index := NewHNSW(config)

	// 预先添加一些向量
	for i := 0; i < 10000; i++ {
		vector := make([]float32, 128)
		for j := range vector {
			vector[j] = rand.Float32()
		}
		index.Add(vector)
	}

	query := make([]float32, 128)
	for j := range query {
		query[j] = rand.Float32()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		index.Search(query, 10, 100)
	}
}

func TestConcurrentInsert(t *testing.T) {
	config := Config{
		M:              16,
		EfConstruction: 200,
		Dimension:      128,
	}

	index := NewHNSW(config)

	numGoroutines := 10
	vectorsPerGoroutine := 100
	totalVectors := numGoroutines * vectorsPerGoroutine

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(start int) {
			defer wg.Done()
			for j := 0; j < vectorsPerGoroutine; j++ {
				vector := make([]float32, 128)
				for k := range vector {
					vector[k] = rand.Float32()
				}
				_, err := index.Add(vector)
				if err != nil {
					errors <- fmt.Errorf("goroutine %d: %v", start, err)
					return
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// 检查是否有错误
	for err := range errors {
		t.Errorf("Concurrent insert error: %v", err)
	}

	// 验证索引大小
	if index.Len() != totalVectors {
		t.Errorf("Expected %d vectors, got %d", totalVectors, index.Len())
	}

	t.Logf("Successfully inserted %d vectors concurrently", totalVectors)
}

func TestConcurrentSearch(t *testing.T) {
	config := Config{
		M:              16,
		EfConstruction: 200,
		Dimension:      128,
	}

	index := NewHNSW(config)

	// 预先添加数据
	numVectors := 1000
	for i := 0; i < numVectors; i++ {
		vector := make([]float32, 128)
		for j := range vector {
			vector[j] = rand.Float32()
		}
		index.Add(vector)
	}

	// 并发搜索
	numGoroutines := 20
	searchesPerGoroutine := 50

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < searchesPerGoroutine; j++ {
				query := make([]float32, 128)
				for k := range query {
					query[k] = rand.Float32()
				}
				results, err := index.Search(query, 10, 0)
				if err != nil {
					errors <- fmt.Errorf("goroutine %d: %v", id, err)
					return
				}
				if len(results) == 0 {
					errors <- fmt.Errorf("goroutine %d: no results", id)
					return
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// 检查是否有错误
	for err := range errors {
		t.Errorf("Concurrent search error: %v", err)
	}

	t.Logf("Successfully performed %d concurrent searches", numGoroutines*searchesPerGoroutine)
}

func TestConcurrentInsertAndSearch(t *testing.T) {
	config := Config{
		M:              16,
		EfConstruction: 200,
		Dimension:      128,
	}

	index := NewHNSW(config)

	// 预先添加一些数据
	for i := 0; i < 500; i++ {
		vector := make([]float32, 128)
		for j := range vector {
			vector[j] = rand.Float32()
		}
		index.Add(vector)
	}

	var wg sync.WaitGroup
	errors := make(chan error, 20)

	// 并发插入
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				vector := make([]float32, 128)
				for k := range vector {
					vector[k] = rand.Float32()
				}
				_, err := index.Add(vector)
				if err != nil {
					errors <- fmt.Errorf("insert goroutine %d: %v", id, err)
					return
				}
			}
		}(i)
	}

	// 并发搜索
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				query := make([]float32, 128)
				for k := range query {
					query[k] = rand.Float32()
				}
				_, err := index.Search(query, 10, 0)
				if err != nil && err != ErrEmptyIndex {
					errors <- fmt.Errorf("search goroutine %d: %v", id, err)
					return
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// 检查是否有错误
	for err := range errors {
		t.Errorf("Concurrent operation error: %v", err)
	}

	t.Logf("Successfully performed concurrent inserts and searches. Final index size: %d", index.Len())
}

// ==================== 数据隔离测试 ====================

func TestVectorIsolation(t *testing.T) {
	config := Config{
		M:              16,
		EfConstruction: 200,
		Dimension:      3,
	}

	index := NewHNSW(config)

	// 测试 Add 的隔离性
	vector := []float32{1, 2, 3}
	id, err := index.Add(vector)
	if err != nil {
		t.Fatalf("Failed to add vector: %v", err)
	}

	// 修改原始向量
	vector[0] = 999

	// 验证索引中的向量未被修改
	storedVec := index.nodes[id].Vector()
	if storedVec[0] != 1 {
		t.Errorf("Vector was modified externally! Expected 1, got %f", storedVec[0])
	}

	// 测试 Vector() 的隔离性
	returnedVec := index.nodes[id].Vector()
	returnedVec[0] = 888

	// 验证内部向量未被修改
	vec2 := index.nodes[id].Vector()
	if vec2[0] != 1 {
		t.Errorf("Internal vector was modified! Expected 1, got %f", vec2[0])
	}

	t.Log("Vector isolation test passed")
}

// ==================== 边界测试 ====================

func TestDimensionMismatch(t *testing.T) {
	config := Config{
		M:              16,
		EfConstruction: 200,
		Dimension:      128,
	}

	index := NewHNSW(config)

	// 测试 Add 时维度不匹配
	wrongVector := make([]float32, 64)
	_, err := index.Add(wrongVector)
	if err != ErrDimensionMismatch {
		t.Errorf("Expected ErrDimensionMismatch, got %v", err)
	}

	// 添加正确维度的向量
	correctVector := make([]float32, 128)
	for i := range correctVector {
		correctVector[i] = rand.Float32()
	}
	index.Add(correctVector)

	// 测试 Search 时维度不匹配
	wrongQuery := make([]float32, 64)
	_, err = index.Search(wrongQuery, 10, 0)
	if err != ErrDimensionMismatch {
		t.Errorf("Expected ErrDimensionMismatch, got %v", err)
	}

	t.Log("Dimension mismatch test passed")
}

func TestSingleVector(t *testing.T) {
	config := Config{
		M:              16,
		EfConstruction: 200,
		Dimension:      128,
	}

	index := NewHNSW(config)

	// 只添加一个向量
	vector := make([]float32, 128)
	for i := range vector {
		vector[i] = rand.Float32()
	}
	id, err := index.Add(vector)
	if err != nil {
		t.Fatalf("Failed to add vector: %v", err)
	}

	// 搜索
	results, err := index.Search(vector, 5, 0)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}

	if results[0].ID != id {
		t.Errorf("Expected ID %d, got %d", id, results[0].ID)
	}

	t.Log("Single vector test passed")
}

func TestLargeK(t *testing.T) {
	config := Config{
		M:              16,
		EfConstruction: 200,
		Dimension:      128,
	}

	index := NewHNSW(config)

	// 添加 100 个向量
	numVectors := 100
	for i := 0; i < numVectors; i++ {
		vector := make([]float32, 128)
		for j := range vector {
			vector[j] = rand.Float32()
		}
		index.Add(vector)
	}

	// 搜索 k > 实际向量数
	query := make([]float32, 128)
	for i := range query {
		query[i] = rand.Float32()
	}

	results, err := index.Search(query, 200, 0)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	// 应该返回所有向量
	if len(results) != numVectors {
		t.Errorf("Expected %d results, got %d", numVectors, len(results))
	}

	t.Logf("Large k test passed: requested 200, got %d", len(results))
}

// ==================== 召回率测试 ====================

func TestRecall(t *testing.T) {
	config := Config{
		M:              16,
		EfConstruction: 200,
		Dimension:      64,
		Seed:           42,
	}

	index := NewHNSW(config)

	// 生成测试数据
	numVectors := 1000
	vectors := make([][]float32, numVectors)
	rand.Seed(42)

	for i := 0; i < numVectors; i++ {
		vector := make([]float32, 64)
		for j := range vector {
			vector[j] = rand.Float32()
		}
		vectors[i] = vector
		index.Add(vector)
	}

	// 生成测试查询
	numQueries := 100
	k := 10

	totalRecall := 0.0

	for q := 0; q < numQueries; q++ {
		query := make([]float32, 64)
		for i := range query {
			query[i] = rand.Float32()
		}

		// HNSW 搜索
		hnswResults, err := index.Search(query, k, 100)
		if err != nil {
			t.Fatalf("HNSW search failed: %v", err)
		}

		// 暴力搜索（ground truth）
		groundTruth := bruteForceSearch(query, vectors, k)

		// 计算召回率
		recall := calculateRecall(hnswResults, groundTruth)
		totalRecall += recall
	}

	avgRecall := totalRecall / float64(numQueries)
	t.Logf("Average Recall@%d: %.2f%%", k, avgRecall*100)

	// 召回率应该 > 90%
	if avgRecall < 0.90 {
		t.Errorf("Recall too low: %.2f%%, expected > 90%%", avgRecall*100)
	}
}

// 暴力搜索（ground truth）
func bruteForceSearch(query []float32, vectors [][]float32, k int) []SearchResult {
	results := make([]SearchResult, len(vectors))
	for i, vec := range vectors {
		dist := L2Distance(query, vec)
		results[i] = SearchResult{ID: i, Distance: dist}
	}

	// 排序
	sort.Slice(results, func(i, j int) bool {
		return results[i].Distance < results[j].Distance
	})

	if len(results) > k {
		return results[:k]
	}
	return results
}

// 计算召回率
func calculateRecall(hnswResults, groundTruth []SearchResult) float64 {
	if len(hnswResults) == 0 || len(groundTruth) == 0 {
		return 0
	}

	// 将 ground truth 转换为 map
	gtMap := make(map[int]bool)
	for _, r := range groundTruth {
		gtMap[r.ID] = true
	}

	// 计算命中数
	hits := 0
	for _, r := range hnswResults {
		if gtMap[r.ID] {
			hits++
		}
	}

	return float64(hits) / float64(len(groundTruth))
}

// ==================== 不同参数测试 ====================

func TestDifferentEf(t *testing.T) {
	config := Config{
		M:              16,
		EfConstruction: 200,
		Dimension:      128,
	}

	index := NewHNSW(config)

	// 添加数据
	numVectors := 1000
	vectors := make([][]float32, numVectors)
	for i := 0; i < numVectors; i++ {
		vector := make([]float32, 128)
		for j := range vector {
			vector[j] = rand.Float32()
		}
		vectors[i] = vector
		index.Add(vector)
	}

	query := vectors[0]
	k := 10

	// 测试不同的 ef 值
	efValues := []int{10, 50, 100, 200}

	for _, ef := range efValues {
		results, err := index.Search(query, k, ef)
		if err != nil {
			t.Fatalf("Search with ef=%d failed: %v", ef, err)
		}

		if len(results) != k {
			t.Errorf("ef=%d: expected %d results, got %d", ef, k, len(results))
		}

		t.Logf("ef=%d: top result distance = %f", ef, results[0].Distance)
	}
}

func TestDifferentM(t *testing.T) {
	mValues := []int{8, 16, 32}

	for _, m := range mValues {
		config := Config{
			M:              m,
			EfConstruction: 200,
			Dimension:      64,
		}

		index := NewHNSW(config)

		// 添加数据
		numVectors := 500
		for i := 0; i < numVectors; i++ {
			vector := make([]float32, 64)
			for j := range vector {
				vector[j] = rand.Float32()
			}
			index.Add(vector)
		}

		// 测试搜索
		query := make([]float32, 64)
		for i := range query {
			query[i] = rand.Float32()
		}

		results, err := index.Search(query, 10, 0)
		if err != nil {
			t.Fatalf("M=%d: search failed: %v", m, err)
		}

		t.Logf("M=%d: found %d results, top distance = %f", m, len(results), results[0].Distance)
	}
}

// ==================== 大规模测试 ====================

func TestLargeScale(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large scale test in short mode")
	}

	config := Config{
		M:              16,
		EfConstruction: 200,
		Dimension:      128,
	}

	index := NewHNSW(config)

	// 添加 10K 向量
	numVectors := 10000
	t.Logf("Adding %d vectors...", numVectors)

	for i := 0; i < numVectors; i++ {
		vector := make([]float32, 128)
		for j := range vector {
			vector[j] = rand.Float32()
		}
		_, err := index.Add(vector)
		if err != nil {
			t.Fatalf("Failed to add vector %d: %v", i, err)
		}

		if (i+1)%1000 == 0 {
			t.Logf("Added %d vectors", i+1)
		}
	}

	t.Logf("Index built with %d vectors", index.Len())

	// 测试搜索性能
	numQueries := 100
	k := 10

	for q := 0; q < numQueries; q++ {
		query := make([]float32, 128)
		for i := range query {
			query[i] = rand.Float32()
		}

		results, err := index.Search(query, k, 100)
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}

		if len(results) != k {
			t.Errorf("Expected %d results, got %d", k, len(results))
		}
	}

	t.Logf("Successfully completed %d searches on %d vectors", numQueries, numVectors)
}

func BenchmarkHNSWSearchDifferentEf(b *testing.B) {
	config := Config{
		M:              16,
		EfConstruction: 200,
		Dimension:      128,
	}

	index := NewHNSW(config)

	// 预先添加数据
	for i := 0; i < 10000; i++ {
		vector := make([]float32, 128)
		for j := range vector {
			vector[j] = rand.Float32()
		}
		index.Add(vector)
	}

	query := make([]float32, 128)
	for j := range query {
		query[j] = rand.Float32()
	}

	efValues := []int{10, 50, 100, 200}

	for _, ef := range efValues {
		b.Run(fmt.Sprintf("ef=%d", ef), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				index.Search(query, 10, ef)
			}
		})
	}
}

func BenchmarkDistanceFunctions(b *testing.B) {
	a := make([]float32, 128)
	vec := make([]float32, 128)
	for i := range a {
		a[i] = rand.Float32()
		vec[i] = rand.Float32()
	}

	b.Run("L2Distance", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			L2Distance(a, vec)
		}
	})

	b.Run("InnerProductDistance", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			InnerProductDistance(a, vec)
		}
	})

	b.Run("CosineDistance", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			CosineDistance(a, vec)
		}
	})
}

// ==================== 特殊距离函数测试 ====================

func TestCosineDistance(t *testing.T) {
	// 测试相同向量
	a := []float32{1, 0, 0}
	dist := CosineDistance(a, a)
	if math.Abs(float64(dist)) > 0.0001 {
		t.Errorf("Cosine distance of same vector should be ~0, got %f", dist)
	}

	// 测试正交向量
	b := []float32{0, 1, 0}
	dist = CosineDistance(a, b)
	if math.Abs(float64(dist-1.0)) > 0.0001 {
		t.Errorf("Cosine distance of orthogonal vectors should be ~1, got %f", dist)
	}

	// 测试反向量
	c := []float32{-1, 0, 0}
	dist = CosineDistance(a, c)
	if math.Abs(float64(dist-2.0)) > 0.0001 {
		t.Errorf("Cosine distance of opposite vectors should be ~2, got %f", dist)
	}
}
