package hnsw

import (
	"fmt"
	"github.com/wzqhbustb/vego/storage/arrow"
	"github.com/wzqhbustb/vego/storage/column"
	"github.com/wzqhbustb/vego/storage/encoding" // [NEW] Import encoding package
	"os"
	"path/filepath"
)

// [NEW] Helper function: create default EncoderFactory
func defaultEncoderFactory() *encoding.EncoderFactory {
	return encoding.NewEncoderFactory(3) // Default compression level 3
}

// SchemaForNodes creates schema for node storage
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

// SchemaForConnections creates schema for connection storage
func SchemaForConnections() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		arrow.NewField("node_id", arrow.PrimInt32(), false),
		arrow.NewField("layer", arrow.PrimInt32(), false),
		arrow.NewField("neighbor_id", arrow.PrimInt32(), false),
	}, map[string]string{
		"purpose": "hnsw_connections",
	})
}

// SchemaForMetadata creates schema for metadata storage (using Int32 arrays)
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

// SaveToLance saves HNSW index to Lance format files
func (h *HNSWIndex) SaveToLance(baseDir string) error {
	h.globalLock.RLock()
	defer h.globalLock.RUnlock()

	// Ensure base directory exists
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return fmt.Errorf("create directory failed: %w", err)
	}

	// Save node data
	if err := h.saveNodes(filepath.Join(baseDir, "nodes.lance")); err != nil {
		return fmt.Errorf("save nodes failed: %w", err)
	}

	// Save connection data
	if err := h.saveConnections(filepath.Join(baseDir, "connections.lance")); err != nil {
		return fmt.Errorf("save connections failed: %w", err)
	}

	// Save metadata
	if err := h.saveMetadata(filepath.Join(baseDir, "metadata.lance")); err != nil {
		return fmt.Errorf("save metadata failed: %w", err)
	}

	return nil
}

// saveNodes saves all node data
func (h *HNSWIndex) saveNodes(filename string) error {
	if len(h.nodes) == 0 {
		return fmt.Errorf("no nodes to save")
	}

	schema := SchemaForNodes(h.dimension)

	// Prepare data arrays
	numNodes := len(h.nodes)

	// ID array
	ids := make([]int32, numNodes)
	// Vector array (flattened)
	vectors := make([]float32, numNodes*h.dimension)
	// Level array
	levels := make([]int32, numNodes)

	for i, node := range h.nodes {
		ids[i] = int32(node.ID())

		// Copy vector data
		nodeVector := node.Vector()
		copy(vectors[i*h.dimension:(i+1)*h.dimension], nodeVector)

		levels[i] = int32(node.Level())
	}

	// Create Arrow arrays
	idArray := arrow.NewInt32Array(ids, nil)
	vectorArray := arrow.NewFloat32Array(vectors, nil)
	levelArray := arrow.NewInt32Array(levels, nil)

	// Create FixedSizeListArray for vectors
	vectorType := arrow.VectorType(h.dimension).(*arrow.FixedSizeListType)
	vectorListArray := arrow.NewFixedSizeListArray(vectorType, vectorArray, nil)

	// Create RecordBatch
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

// saveConnections saves connection relationships
func (h *HNSWIndex) saveConnections(filename string) error {
	schema := SchemaForConnections()

	// Collect all connections
	var nodeIDs, layers, neighborIDs []int32

	for _, node := range h.nodes {
		nodeID := int32(node.ID())

		// Iterate through all layers of this node
		for layer := 0; layer <= node.Level(); layer++ {
			connections := node.GetConnections(layer)

			// Add all connections at this layer
			for _, neighborID := range connections {
				nodeIDs = append(nodeIDs, nodeID)
				layers = append(layers, int32(layer))
				neighborIDs = append(neighborIDs, int32(neighborID))
			}
		}
	}

	// If no connections, don't create file (avoid empty array validation error)
	if len(nodeIDs) == 0 {
		return nil
	}

	// Create Arrow arrays
	nodeIDArray := arrow.NewInt32Array(nodeIDs, nil)
	layerArray := arrow.NewInt32Array(layers, nil)
	neighborIDArray := arrow.NewInt32Array(neighborIDs, nil)

	// Create RecordBatch
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

// saveMetadata saves HNSW configuration metadata
func (h *HNSWIndex) saveMetadata(filename string) error {
	schema := SchemaForMetadata()

	// Prepare metadata (single row record)
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

	// Create Arrow arrays (each field is an array of length 1)
	mArray := arrow.NewInt32Array([]int32{metadata[0]}, nil)
	mmaxArray := arrow.NewInt32Array([]int32{metadata[1]}, nil)
	mmax0Array := arrow.NewInt32Array([]int32{metadata[2]}, nil)
	efConstructionArray := arrow.NewInt32Array([]int32{metadata[3]}, nil)
	dimensionArray := arrow.NewInt32Array([]int32{metadata[4]}, nil)
	entryPointArray := arrow.NewInt32Array([]int32{metadata[5]}, nil)
	maxLevelArray := arrow.NewInt32Array([]int32{metadata[6]}, nil)
	numNodesArray := arrow.NewInt32Array([]int32{metadata[7]}, nil)

	// Create RecordBatch
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

// LoadFromLance loads HNSW index from Lance format files
func LoadHNSWFromLance(baseDir string) (*HNSWIndex, error) {
	// Load metadata to determine HNSW configuration
	metadata, err := loadMetadata(filepath.Join(baseDir, "metadata.lance"))
	if err != nil {
		return nil, fmt.Errorf("load metadata failed: %w", err)
	}

	// Create HNSW instance
	config := Config{
		M:              int(metadata[0]),
		EfConstruction: int(metadata[3]),
		Dimension:      int(metadata[4]),
		DistanceFunc:   L2Distance,
	}

	hnsw := NewHNSW(config)

	// Set state loaded from metadata
	hnsw.entryPoint = metadata[5]
	hnsw.maxLevel = metadata[6]

	// Load node data
	if err := hnsw.loadNodes(filepath.Join(baseDir, "nodes.lance")); err != nil {
		return nil, fmt.Errorf("load nodes failed: %w", err)
	}

	// Load connection data
	if err := hnsw.loadConnections(filepath.Join(baseDir, "connections.lance")); err != nil {
		return nil, fmt.Errorf("load connections failed: %w", err)
	}

	return hnsw, nil
}

// loadMetadata loads metadata
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

	// Extract all metadata values
	metadata := make([]int32, 8)
	for i := 0; i < 8; i++ {
		array := batch.Column(i).(*arrow.Int32Array)
		metadata[i] = array.Value(0)
	}

	return metadata, nil
}

// loadNodes loads node data
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

	// Get underlying float array
	vectorArray := vectorListArray.Values().(*arrow.Float32Array)
	vectorValues := vectorArray.Values()

	// Verify continuity of node IDs
	numNodes := idArray.Len()
	for i := 0; i < numNodes; i++ {
		id := int(idArray.Value(i))
		if id != i {
			return fmt.Errorf("node ID mismatch at index %d: expected %d, got %d", i, i, id)
		}
	}

	// Reconstruct nodes
	h.nodes = make([]*Node, numNodes)

	for i := 0; i < numNodes; i++ {
		id := int(idArray.Value(i))
		level := int(levelArray.Value(i))

		// Extract vector
		start := i * h.dimension
		end := start + h.dimension
		vector := make([]float32, h.dimension)
		copy(vector, vectorValues[start:end])

		// Create node
		node := NewNode(id, vector, level)
		h.nodes[i] = node
	}

	return nil
}

// loadConnections loads connection relationships
func (h *HNSWIndex) loadConnections(filename string) error {
	// Check if file exists (handle case with no connections)
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		// File doesn't exist, meaning no connections were saved, which is valid
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

	// Rebuild connection relationships
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
