package hnsw

import (
	"os"
	"path/filepath"
	"testing"
)

func TestHNSWStorageBasic(t *testing.T) {
	// 创建临时目录
	tempDir := t.TempDir()

	// 创建测试HNSW索引
	config := Config{
		M:              16,
		EfConstruction: 200,
		Dimension:      4,
		DistanceFunc:   L2Distance,
	}

	hnsw := NewHNSW(config)

	// 添加测试向量
	vectors := [][]float32{
		{1.0, 2.0, 3.0, 4.0},
		{2.0, 3.0, 4.0, 5.0},
		{3.0, 4.0, 5.0, 6.0},
		{4.0, 5.0, 6.0, 7.0},
		{5.0, 6.0, 7.0, 8.0},
	}

	for i, vec := range vectors {
		_, err := hnsw.Add(vec)
		if err != nil {
			t.Fatalf("Failed to add vector %d: %v", i, err)
		}
	}

	// 保存到Lance格式
	if err := hnsw.SaveToLance(tempDir); err != nil {
		t.Fatalf("Failed to save HNSW: %v", err)
	}

	// 验证文件是否创建
	expectedFiles := []string{"nodes.lance", "connections.lance", "metadata.lance"}
	for _, filename := range expectedFiles {
		fullPath := filepath.Join(tempDir, filename)
		if _, err := os.Stat(fullPath); os.IsNotExist(err) {
			t.Errorf("Expected file %s was not created", filename)
		}
	}

	// 从Lance格式加载
	loadedHNSW, err := LoadHNSWFromLance(tempDir)
	if err != nil {
		t.Fatalf("Failed to load HNSW: %v", err)
	}

	// 验证基本属性
	if loadedHNSW.M != hnsw.M {
		t.Errorf("M mismatch: got %d, want %d", loadedHNSW.M, hnsw.M)
	}
	if loadedHNSW.dimension != hnsw.dimension {
		t.Errorf("Dimension mismatch: got %d, want %d", loadedHNSW.dimension, hnsw.dimension)
	}
	if loadedHNSW.entryPoint != hnsw.entryPoint {
		t.Errorf("EntryPoint mismatch: got %d, want %d", loadedHNSW.entryPoint, hnsw.entryPoint)
	}
	if loadedHNSW.maxLevel != hnsw.maxLevel {
		t.Errorf("MaxLevel mismatch: got %d, want %d", loadedHNSW.maxLevel, hnsw.maxLevel)
	}

	// 验证节点数量
	if len(loadedHNSW.nodes) != len(hnsw.nodes) {
		t.Errorf("Node count mismatch: got %d, want %d", len(loadedHNSW.nodes), len(hnsw.nodes))
	}

	// 验证节点内容
	for i, originalNode := range hnsw.nodes {
		if i >= len(loadedHNSW.nodes) {
			t.Errorf("Missing node at index %d", i)
			continue
		}

		loadedNode := loadedHNSW.nodes[i]

		// 验证节点ID
		if loadedNode.ID() != originalNode.ID() {
			t.Errorf("Node %d ID mismatch: got %d, want %d", i, loadedNode.ID(), originalNode.ID())
		}

		// 验证节点级别
		if loadedNode.Level() != originalNode.Level() {
			t.Errorf("Node %d level mismatch: got %d, want %d", i, loadedNode.Level(), originalNode.Level())
		}

		// 验证向量
		originalVec := originalNode.Vector()
		loadedVec := loadedNode.Vector()
		if len(loadedVec) != len(originalVec) {
			t.Errorf("Node %d vector length mismatch: got %d, want %d", i, len(loadedVec), len(originalVec))
			continue
		}

		for j, val := range originalVec {
			if loadedVec[j] != val {
				t.Errorf("Node %d vector[%d] mismatch: got %f, want %f", i, j, loadedVec[j], val)
			}
		}

		// 验证连接关系
		for layer := 0; layer <= originalNode.Level(); layer++ {
			originalConnections := originalNode.GetConnections(layer)
			loadedConnections := loadedNode.GetConnections(layer)

			if len(loadedConnections) != len(originalConnections) {
				t.Errorf("Node %d layer %d connection count mismatch: got %d, want %d",
					i, layer, len(loadedConnections), len(originalConnections))
				continue
			}

			// 转换为map进行比较（顺序可能不同）
			originalSet := make(map[int]bool)
			for _, conn := range originalConnections {
				originalSet[conn] = true
			}

			for _, conn := range loadedConnections {
				if !originalSet[conn] {
					t.Errorf("Node %d layer %d unexpected connection: %d", i, layer, conn)
				}
			}
		}
	}

	// 测试搜索功能是否正常
	queryVector := []float32{2.5, 3.5, 4.5, 5.5}
	results, err := loadedHNSW.Search(queryVector, 3, 50)
	if err != nil {
		t.Errorf("Search failed on loaded HNSW: %v", err)
	}

	if len(results) == 0 {
		t.Error("Search returned no results")
	}

	t.Logf("✓ Basic persistence test passed: saved and loaded %d nodes", len(hnsw.nodes))
}

func TestHNSWStorageEmptyIndex(t *testing.T) {
	// 测试空HNSW的持久化
	tempDir := t.TempDir()

	config := Config{
		M:              8,
		EfConstruction: 100,
		Dimension:      3,
		DistanceFunc:   L2Distance,
	}

	hnsw := NewHNSW(config)

	// 尝试保存空索引应该失败
	err := hnsw.SaveToLance(tempDir)
	if err == nil {
		t.Error("Expected error when saving empty HNSW, but got none")
	}
	if err != nil && err.Error() != "save nodes failed: no nodes to save" {
		t.Errorf("Expected 'no nodes to save' error, got: %v", err)
	}

	t.Logf("✓ Empty index test passed: correctly rejected empty HNSW")
}

func TestHNSWStorageLargeDataset(t *testing.T) {
	// 测试较大数据集的持久化
	tempDir := t.TempDir()

	config := Config{
		M:              16,
		EfConstruction: 200,
		Dimension:      128,
		DistanceFunc:   L2Distance,
	}

	hnsw := NewHNSW(config)

	// 添加100个向量
	numVectors := 100
	for i := 0; i < numVectors; i++ {
		vector := make([]float32, 128)
		for j := 0; j < 128; j++ {
			vector[j] = float32(i*j) * 0.01
		}

		_, err := hnsw.Add(vector)
		if err != nil {
			t.Fatalf("Failed to add vector %d: %v", i, err)
		}
	}

	// 保存和加载
	if err := hnsw.SaveToLance(tempDir); err != nil {
		t.Fatalf("Failed to save large HNSW: %v", err)
	}

	loadedHNSW, err := LoadHNSWFromLance(tempDir)
	if err != nil {
		t.Fatalf("Failed to load large HNSW: %v", err)
	}

	// 基本验证
	if len(loadedHNSW.nodes) != numVectors {
		t.Errorf("Node count mismatch: got %d, want %d", len(loadedHNSW.nodes), numVectors)
	}

	// 验证搜索功能
	queryVector := make([]float32, 128)
	for j := 0; j < 128; j++ {
		queryVector[j] = 0.5
	}

	results, err := loadedHNSW.Search(queryVector, 5, 100)
	if err != nil {
		t.Errorf("Search failed on loaded large HNSW: %v", err)
	}

	if len(results) == 0 {
		t.Error("Search returned no results for large HNSW")
	}

	t.Logf("✓ Large dataset test passed: processed %d vectors with dimension %d",
		numVectors, config.Dimension)
}

func TestHNSWStorageHighDimensional(t *testing.T) {
	// 测试高维向量（模拟真实的嵌入向量）
	tempDir := t.TempDir()

	config := Config{
		M:              16,
		EfConstruction: 200,
		Dimension:      768, // 常见的BERT嵌入维度
		DistanceFunc:   L2Distance,
	}

	hnsw := NewHNSW(config)

	// 添加10个高维向量
	numVectors := 10
	for i := 0; i < numVectors; i++ {
		vector := make([]float32, 768)
		for j := 0; j < 768; j++ {
			vector[j] = float32(i+j) * 0.001
		}

		_, err := hnsw.Add(vector)
		if err != nil {
			t.Fatalf("Failed to add high-dim vector %d: %v", i, err)
		}
	}

	// 保存
	if err := hnsw.SaveToLance(tempDir); err != nil {
		t.Fatalf("Failed to save high-dim HNSW: %v", err)
	}

	// 加载
	loadedHNSW, err := LoadHNSWFromLance(tempDir)
	if err != nil {
		t.Fatalf("Failed to load high-dim HNSW: %v", err)
	}

	// 验证维度
	if loadedHNSW.dimension != 768 {
		t.Errorf("Dimension mismatch: got %d, want 768", loadedHNSW.dimension)
	}

	// 验证向量数据完整性
	for i := 0; i < numVectors; i++ {
		originalVec := hnsw.nodes[i].Vector()
		loadedVec := loadedHNSW.nodes[i].Vector()

		if len(loadedVec) != 768 {
			t.Errorf("Node %d vector dimension mismatch: got %d, want 768", i, len(loadedVec))
		}

		// 抽样验证几个值
		for j := 0; j < 768; j += 100 {
			if originalVec[j] != loadedVec[j] {
				t.Errorf("Node %d vector[%d] mismatch: got %f, want %f",
					i, j, loadedVec[j], originalVec[j])
			}
		}
	}

	t.Logf("✓ High-dimensional test passed: 768-dim vectors preserved correctly")
}

func TestHNSWStorageConnectionIntegrity(t *testing.T) {
	// 专门测试连接关系的完整性
	tempDir := t.TempDir()

	config := Config{
		M:              4, // 较小的M值，更容易测试连接
		EfConstruction: 100,
		Dimension:      3,
		DistanceFunc:   L2Distance,
	}

	hnsw := NewHNSW(config)

	// 添加足够多的向量以产生多层级结构
	vectors := [][]float32{
		{1.0, 0.0, 0.0},
		{0.0, 1.0, 0.0},
		{0.0, 0.0, 1.0},
		{1.0, 1.0, 0.0},
		{1.0, 0.0, 1.0},
		{0.0, 1.0, 1.0},
		{1.0, 1.0, 1.0},
		{2.0, 0.0, 0.0},
		{0.0, 2.0, 0.0},
		{0.0, 0.0, 2.0},
	}

	for i, vec := range vectors {
		_, err := hnsw.Add(vec)
		if err != nil {
			t.Fatalf("Failed to add vector %d: %v", i, err)
		}
	}

	// 计算原始总连接数
	originalTotalConnections := 0
	for _, node := range hnsw.nodes {
		for layer := 0; layer <= node.Level(); layer++ {
			originalTotalConnections += len(node.GetConnections(layer))
		}
	}

	// 保存和加载
	if err := hnsw.SaveToLance(tempDir); err != nil {
		t.Fatalf("Failed to save HNSW: %v", err)
	}

	loadedHNSW, err := LoadHNSWFromLance(tempDir)
	if err != nil {
		t.Fatalf("Failed to load HNSW: %v", err)
	}

	// 计算加载后的总连接数
	loadedTotalConnections := 0
	for _, node := range loadedHNSW.nodes {
		for layer := 0; layer <= node.Level(); layer++ {
			loadedTotalConnections += len(node.GetConnections(layer))
		}
	}

	if loadedTotalConnections != originalTotalConnections {
		t.Errorf("Total connections mismatch: got %d, want %d",
			loadedTotalConnections, originalTotalConnections)
	}

	// 验证每个节点的连接
	for i, originalNode := range hnsw.nodes {
		loadedNode := loadedHNSW.nodes[i]

		for layer := 0; layer <= originalNode.Level(); layer++ {
			originalConns := originalNode.GetConnections(layer)
			loadedConns := loadedNode.GetConnections(layer)

			// 构建集合进行比较
			originalSet := make(map[int]bool)
			for _, conn := range originalConns {
				originalSet[conn] = true
			}

			loadedSet := make(map[int]bool)
			for _, conn := range loadedConns {
				loadedSet[conn] = true
			}

			// 检查是否完全一致
			if len(originalSet) != len(loadedSet) {
				t.Errorf("Node %d layer %d connection set size mismatch: got %d, want %d",
					i, layer, len(loadedSet), len(originalSet))
			}

			for conn := range originalSet {
				if !loadedSet[conn] {
					t.Errorf("Node %d layer %d missing connection: %d", i, layer, conn)
				}
			}

			for conn := range loadedSet {
				if !originalSet[conn] {
					t.Errorf("Node %d layer %d unexpected connection: %d", i, layer, conn)
				}
			}
		}
	}

	t.Logf("✓ Connection integrity test passed: %d total connections preserved",
		originalTotalConnections)
}

func TestHNSWStorageMultipleSaveLoad(t *testing.T) {
	// 测试多次保存和加载
	tempDir := t.TempDir()

	config := Config{
		M:              16,
		EfConstruction: 200,
		Dimension:      4,
		DistanceFunc:   L2Distance,
	}

	// 第一次：创建并保存
	hnsw1 := NewHNSW(config)
	for i := 0; i < 5; i++ {
		vec := []float32{float32(i), float32(i + 1), float32(i + 2), float32(i + 3)}
		hnsw1.Add(vec)
	}

	if err := hnsw1.SaveToLance(tempDir); err != nil {
		t.Fatalf("First save failed: %v", err)
	}

	// 第二次：加载并验证
	hnsw2, err := LoadHNSWFromLance(tempDir)
	if err != nil {
		t.Fatalf("First load failed: %v", err)
	}

	if len(hnsw2.nodes) != 5 {
		t.Errorf("After first load: got %d nodes, want 5", len(hnsw2.nodes))
	}

	// 第三次：再次保存同样的数据到新目录
	tempDir2 := t.TempDir()
	if err := hnsw2.SaveToLance(tempDir2); err != nil {
		t.Fatalf("Second save failed: %v", err)
	}

	// 第四次：加载并验证
	hnsw3, err := LoadHNSWFromLance(tempDir2)
	if err != nil {
		t.Fatalf("Second load failed: %v", err)
	}

	if len(hnsw3.nodes) != 5 {
		t.Errorf("After second load: got %d nodes, want 5", len(hnsw3.nodes))
	}

	// 验证向量数据一致性
	for i := 0; i < 5; i++ {
		vec1 := hnsw1.nodes[i].Vector()
		vec3 := hnsw3.nodes[i].Vector()

		for j := 0; j < 4; j++ {
			if vec1[j] != vec3[j] {
				t.Errorf("Multiple save/load: node %d vector[%d] mismatch", i, j)
			}
		}
	}

	t.Logf("✓ Multiple save/load test passed")
}

func TestHNSWStorageSearchConsistency(t *testing.T) {
	// 测试保存/加载后搜索结果的一致性
	tempDir := t.TempDir()

	config := Config{
		M:              16,
		EfConstruction: 200,
		Dimension:      8,
		DistanceFunc:   L2Distance,
	}

	hnsw := NewHNSW(config)

	// 添加测试向量
	numVectors := 50
	for i := 0; i < numVectors; i++ {
		vector := make([]float32, 8)
		for j := 0; j < 8; j++ {
			vector[j] = float32(i+j) * 0.1
		}
		hnsw.Add(vector)
	}

	// 在保存前执行搜索
	queryVector := []float32{2.5, 2.6, 2.7, 2.8, 2.9, 3.0, 3.1, 3.2}
	originalResults, err := hnsw.Search(queryVector, 5, 100)
	if err != nil {
		t.Fatalf("Original search failed: %v", err)
	}

	// 保存和加载
	if err := hnsw.SaveToLance(tempDir); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	loadedHNSW, err := LoadHNSWFromLance(tempDir)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	// 在加载后执行相同的搜索
	loadedResults, err := loadedHNSW.Search(queryVector, 5, 100)
	if err != nil {
		t.Fatalf("Loaded search failed: %v", err)
	}

	// 验证搜索结果一致性
	if len(loadedResults) != len(originalResults) {
		t.Errorf("Search result count mismatch: got %d, want %d",
			len(loadedResults), len(originalResults))
	}

	// 验证前几个结果的ID和距离
	for i := 0; i < min(len(originalResults), len(loadedResults)); i++ {
		if originalResults[i].ID != loadedResults[i].ID {
			t.Errorf("Result %d ID mismatch: got %d, want %d",
				i, loadedResults[i].ID, originalResults[i].ID)
		}

		// 距离应该非常接近（允许浮点误差）
		distDiff := abs(originalResults[i].Distance - loadedResults[i].Distance)
		if distDiff > 1e-5 {
			t.Errorf("Result %d distance mismatch: got %f, want %f (diff: %f)",
				i, loadedResults[i].Distance, originalResults[i].Distance, distDiff)
		}
	}

	t.Logf("✓ Search consistency test passed: results match after save/load")
}

// 辅助函数
func abs(x float32) float32 {
	if x < 0 {
		return -x
	}
	return x
}
