package hnsw

import (
	"fmt"
	"github.com/wzqhbkjdx/vego/storage/arrow"
	"github.com/wzqhbkjdx/vego/storage/column"
	"github.com/wzqhbkjdx/vego/storage/encoding" // [NEW] 导入 encoding 包
	"os"
	"path/filepath"
)

// [NEW] 辅助函数：创建默认的 EncoderFactory
func defaultEncoderFactory() *encoding.EncoderFactory {
	return encoding.NewEncoderFactory(3) // 默认压缩级别 3
}

// SchemaForNodes 创建节点存储的Schema
func SchemaForNodes(dimension int) *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		arrow.NewField("id", arrow.PrimInt32(), false),
		arrow.NewField("vector", arrow.VectorType(dimension), false),
		arrow.NewField("level", arrow.PrimInt32(), false),
	}, map[string]string{
		"purpose":   "hnsw_nodes",
		"dimension": fmt.Sprintf("%d", dimension),
	})
}

// SchemaForConnections 创建连接关系存储的Schema
func SchemaForConnections() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		arrow.NewField("node_id", arrow.PrimInt32(), false),
		arrow.NewField("layer", arrow.PrimInt32(), false),
		arrow.NewField("neighbor_id", arrow.PrimInt32(), false),
	}, map[string]string{
		"purpose": "hnsw_connections",
	})
}

// SchemaForMetadata 创建元数据存储的Schema（使用Int32数组）
func SchemaForMetadata() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		arrow.NewField("M", arrow.PrimInt32(), false),
		arrow.NewField("Mmax", arrow.PrimInt32(), false),
		arrow.NewField("Mmax0", arrow.PrimInt32(), false),
		arrow.NewField("efConstruction", arrow.PrimInt32(), false),
		arrow.NewField("dimension", arrow.PrimInt32(), false),
		arrow.NewField("entryPoint", arrow.PrimInt32(), false),
		arrow.NewField("maxLevel", arrow.PrimInt32(), false),
		arrow.NewField("numNodes", arrow.PrimInt32(), false),
	}, map[string]string{
		"purpose": "hnsw_metadata",
	})
}

// SaveToLance 将HNSW索引保存到Lance格式文件
func (h *HNSWIndex) SaveToLance(baseDir string) error {
	h.globalLock.RLock()
	defer h.globalLock.RUnlock()

	// 保存节点数据
	if err := h.saveNodes(filepath.Join(baseDir, "nodes.lance")); err != nil {
		return fmt.Errorf("save nodes failed: %w", err)
	}

	// 保存连接数据
	if err := h.saveConnections(filepath.Join(baseDir, "connections.lance")); err != nil {
		return fmt.Errorf("save connections failed: %w", err)
	}

	// 保存元数据
	if err := h.saveMetadata(filepath.Join(baseDir, "metadata.lance")); err != nil {
		return fmt.Errorf("save metadata failed: %w", err)
	}

	return nil
}

// saveNodes 保存所有节点数据
func (h *HNSWIndex) saveNodes(filename string) error {
	if len(h.nodes) == 0 {
		return fmt.Errorf("no nodes to save")
	}

	schema := SchemaForNodes(h.dimension)

	// 准备数据数组
	numNodes := len(h.nodes)

	// ID数组
	ids := make([]int32, numNodes)
	// Vector数组（扁平化）
	vectors := make([]float32, numNodes*h.dimension)
	// Level数组
	levels := make([]int32, numNodes)

	for i, node := range h.nodes {
		ids[i] = int32(node.ID())

		// 复制向量数据
		nodeVector := node.Vector()
		copy(vectors[i*h.dimension:(i+1)*h.dimension], nodeVector)

		levels[i] = int32(node.Level())
	}

	// 创建Arrow数组
	idArray := arrow.NewInt32Array(ids, nil)
	vectorArray := arrow.NewFloat32Array(vectors, nil)
	levelArray := arrow.NewInt32Array(levels, nil)

	// 创建向量的FixedSizeListArray
	vectorType := arrow.VectorType(h.dimension).(*arrow.FixedSizeListType)
	vectorListArray := arrow.NewFixedSizeListArray(vectorType, vectorArray, nil)

	// 创建RecordBatch
	batch, err := arrow.NewRecordBatch(schema, numNodes, []arrow.Array{
		idArray,
		vectorListArray,
		levelArray,
	})
	if err != nil {
		return fmt.Errorf("create record batch failed: %w", err)
	}

	writer, err := column.NewWriter(filename, schema, defaultEncoderFactory())
	if err != nil {
		return fmt.Errorf("create writer failed: %w", err)
	}
	defer writer.Close()

	if err := writer.WriteRecordBatch(batch); err != nil {
		return fmt.Errorf("write nodes failed: %w", err)
	}

	return nil
}

// saveConnections 保存连接关系
func (h *HNSWIndex) saveConnections(filename string) error {
	schema := SchemaForConnections()

	// 收集所有连接关系
	var nodeIDs, layers, neighborIDs []int32

	for _, node := range h.nodes {
		nodeID := int32(node.ID())

		// 遍历该节点的所有层级
		for layer := 0; layer <= node.Level(); layer++ {
			connections := node.GetConnections(layer)

			// 添加该层的所有连接
			for _, neighborID := range connections {
				nodeIDs = append(nodeIDs, nodeID)
				layers = append(layers, int32(layer))
				neighborIDs = append(neighborIDs, int32(neighborID))
			}
		}
	}

	// 如果没有连接关系，不创建文件（避免空数组验证错误）
	if len(nodeIDs) == 0 {
		return nil
	}

	// 创建Arrow数组
	nodeIDArray := arrow.NewInt32Array(nodeIDs, nil)
	layerArray := arrow.NewInt32Array(layers, nil)
	neighborIDArray := arrow.NewInt32Array(neighborIDs, nil)

	// 创建RecordBatch
	batch, err := arrow.NewRecordBatch(schema, len(nodeIDs), []arrow.Array{
		nodeIDArray,
		layerArray,
		neighborIDArray,
	})
	if err != nil {
		return fmt.Errorf("create record batch failed: %w", err)
	}

	writer, err := column.NewWriter(filename, schema, defaultEncoderFactory())
	if err != nil {
		return fmt.Errorf("create writer failed: %w", err)
	}
	defer writer.Close()

	if err := writer.WriteRecordBatch(batch); err != nil {
		return fmt.Errorf("write connections failed: %w", err)
	}

	return nil
}

// saveMetadata 保存HNSW配置元数据
func (h *HNSWIndex) saveMetadata(filename string) error {
	schema := SchemaForMetadata()

	// 准备元数据（单行记录）
	metadata := []int32{
		int32(h.M),
		int32(h.Mmax),
		int32(h.Mmax0),
		int32(h.efConstruction),
		int32(h.dimension),
		h.entryPoint,
		h.maxLevel,
		int32(len(h.nodes)),
	}

	// 创建Arrow数组（每个字段都是长度为1的数组）
	mArray := arrow.NewInt32Array([]int32{metadata[0]}, nil)
	mmaxArray := arrow.NewInt32Array([]int32{metadata[1]}, nil)
	mmax0Array := arrow.NewInt32Array([]int32{metadata[2]}, nil)
	efConstructionArray := arrow.NewInt32Array([]int32{metadata[3]}, nil)
	dimensionArray := arrow.NewInt32Array([]int32{metadata[4]}, nil)
	entryPointArray := arrow.NewInt32Array([]int32{metadata[5]}, nil)
	maxLevelArray := arrow.NewInt32Array([]int32{metadata[6]}, nil)
	numNodesArray := arrow.NewInt32Array([]int32{metadata[7]}, nil)

	// 创建RecordBatch
	batch, err := arrow.NewRecordBatch(schema, 1, []arrow.Array{
		mArray,
		mmaxArray,
		mmax0Array,
		efConstructionArray,
		dimensionArray,
		entryPointArray,
		maxLevelArray,
		numNodesArray,
	})
	if err != nil {
		return fmt.Errorf("create record batch failed: %w", err)
	}

	writer, err := column.NewWriter(filename, schema, defaultEncoderFactory())
	if err != nil {
		return fmt.Errorf("create writer failed: %w", err)
	}
	defer writer.Close()

	if err := writer.WriteRecordBatch(batch); err != nil {
		return fmt.Errorf("write metadata failed: %w", err)
	}

	return nil
}

// LoadFromLance 从Lance格式文件加载HNSW索引
func LoadHNSWFromLance(baseDir string) (*HNSWIndex, error) {
	// 加载元数据，确定HNSW配置
	metadata, err := loadMetadata(filepath.Join(baseDir, "metadata.lance"))
	if err != nil {
		return nil, fmt.Errorf("load metadata failed: %w", err)
	}

	// 创建HNSW实例
	config := Config{
		M:              int(metadata[0]),
		EfConstruction: int(metadata[3]),
		Dimension:      int(metadata[4]),
		DistanceFunc:   L2Distance,
	}

	hnsw := NewHNSW(config)

	// 设置从元数据加载的状态
	hnsw.entryPoint = metadata[5]
	hnsw.maxLevel = metadata[6]

	// 加载节点数据
	if err := hnsw.loadNodes(filepath.Join(baseDir, "nodes.lance")); err != nil {
		return nil, fmt.Errorf("load nodes failed: %w", err)
	}

	// 加载连接数据
	if err := hnsw.loadConnections(filepath.Join(baseDir, "connections.lance")); err != nil {
		return nil, fmt.Errorf("load connections failed: %w", err)
	}

	return hnsw, nil
}

// loadMetadata 加载元数据
func loadMetadata(filename string) ([]int32, error) {
	reader, err := column.NewReader(filename)
	if err != nil {
		return nil, fmt.Errorf("create reader failed: %w", err)
	}
	defer reader.Close()

	batch, err := reader.ReadRecordBatch()
	if err != nil {
		return nil, fmt.Errorf("read metadata failed: %w", err)
	}

	// 提取所有元数据值
	metadata := make([]int32, 8)
	for i := 0; i < 8; i++ {
		array := batch.Column(i).(*arrow.Int32Array)
		metadata[i] = array.Value(0)
	}

	return metadata, nil
}

// loadNodes 加载节点数据
func (h *HNSWIndex) loadNodes(filename string) error {
	reader, err := column.NewReader(filename)
	if err != nil {
		return fmt.Errorf("create reader failed: %w", err)
	}
	defer reader.Close()

	batch, err := reader.ReadRecordBatch()
	if err != nil {
		return fmt.Errorf("read nodes failed: %w", err)
	}

	idArray := batch.Column(0).(*arrow.Int32Array)
	vectorListArray := batch.Column(1).(*arrow.FixedSizeListArray)
	levelArray := batch.Column(2).(*arrow.Int32Array)

	// 获取底层的float数组
	vectorArray := vectorListArray.Values().(*arrow.Float32Array)
	vectorValues := vectorArray.Values()

	// 验证节点ID的连续性
	numNodes := idArray.Len()
	for i := 0; i < numNodes; i++ {
		id := int(idArray.Value(i))
		if id != i {
			return fmt.Errorf("node ID mismatch at index %d: expected %d, got %d", i, i, id)
		}
	}

	// 重构节点
	h.nodes = make([]*Node, numNodes)

	for i := 0; i < numNodes; i++ {
		id := int(idArray.Value(i))
		level := int(levelArray.Value(i))

		// 提取向量
		start := i * h.dimension
		end := start + h.dimension
		vector := make([]float32, h.dimension)
		copy(vector, vectorValues[start:end])

		// 创建节点
		node := NewNode(id, vector, level)
		h.nodes[i] = node
	}

	return nil
}

// loadConnections 加载连接关系
func (h *HNSWIndex) loadConnections(filename string) error {
	// 检查文件是否存在（处理无连接的情况）
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		// 文件不存在，说明保存时没有连接关系，这是合法的
		return nil
	}

	reader, err := column.NewReader(filename)
	if err != nil {
		return fmt.Errorf("create reader failed: %w", err)
	}
	defer reader.Close()

	batch, err := reader.ReadRecordBatch()
	if err != nil {
		return fmt.Errorf("read connections failed: %w", err)
	}

	nodeIDArray := batch.Column(0).(*arrow.Int32Array)
	layerArray := batch.Column(1).(*arrow.Int32Array)
	neighborIDArray := batch.Column(2).(*arrow.Int32Array)

	numConnections := nodeIDArray.Len()

	// 重建连接关系
	for i := 0; i < numConnections; i++ {
		nodeID := int(nodeIDArray.Value(i))
		layer := int(layerArray.Value(i))
		neighborID := int(neighborIDArray.Value(i))

		if nodeID < 0 || nodeID >= len(h.nodes) {
			return fmt.Errorf("invalid node_id %d at connection index %d (valid range: [0, %d])",
				nodeID, i, len(h.nodes))
		}
		if neighborID < 0 || neighborID >= len(h.nodes) {
			return fmt.Errorf("invalid neighbor_id %d at connection index %d (valid range: [0, %d])",
				neighborID, i, len(h.nodes))
		}
		if layer < 0 || layer > h.nodes[nodeID].Level() {
			return fmt.Errorf("invalid layer %d for node %d at connection index %d (valid range: [0, %d])",
				layer, nodeID, i, h.nodes[nodeID].Level())
		}

		node := h.nodes[nodeID]
		node.AddConnection(layer, neighborID)
	}

	return nil
}
