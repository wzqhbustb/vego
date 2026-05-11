package hnsw

import (
	"fmt"
	"reflect"

	"github.com/wzqhbustb/vego/core"
)

// SchemaForNodes creates schema for node storage.
func SchemaForNodes(dimension int) *core.Schema {
	return core.NewSchema([]core.Field{
		core.NewField("id", core.PrimInt32(), false),
		core.NewField("vector", core.VectorType(dimension), false),
		core.NewField("level", core.PrimInt32(), false),
	}, map[string]string{
		"purpose":   "hnsw_nodes",
		"dimension": fmt.Sprintf("%d", dimension),
	})
}

// SchemaForConnections creates schema for connection storage.
func SchemaForConnections() *core.Schema {
	return core.NewSchema([]core.Field{
		core.NewField("node_id", core.PrimInt32(), false),
		core.NewField("layer", core.PrimInt32(), false),
		core.NewField("neighbor_id", core.PrimInt32(), false),
	}, map[string]string{
		"purpose": "hnsw_connections",
	})
}

// SchemaForMetadata creates schema for metadata storage.
func SchemaForMetadata() *core.Schema {
	return core.NewSchema([]core.Field{
		core.NewField("M", core.PrimInt32(), false),
		core.NewField("Mmax", core.PrimInt32(), false),
		core.NewField("Mmax0", core.PrimInt32(), false),
		core.NewField("efConstruction", core.PrimInt32(), false),
		core.NewField("dimension", core.PrimInt32(), false),
		core.NewField("entryPoint", core.PrimInt32(), false),
		core.NewField("maxLevel", core.PrimInt32(), false),
		core.NewField("numNodes", core.PrimInt32(), false),
		core.NewField("distanceFunc", core.PrimInt32(), false),
	}, map[string]string{
		"purpose": "hnsw_metadata",
	})
}

// MarshalNodes serializes all nodes in the index into a core.RecordBatch.
// Returns an error if the index contains no nodes.
//
// TODO: For million-scale datasets, switch to streaming callbacks
// (batchSize int, emit func(*core.RecordBatch) error) to avoid multi-GB
// single-batch allocation.
func (h *HNSWIndex) MarshalNodes() (*core.RecordBatch, error) {
	h.globalLock.RLock()
	defer h.globalLock.RUnlock()

	if len(h.nodes) == 0 {
		return nil, fmt.Errorf("no nodes to marshal")
	}

	schema := SchemaForNodes(h.dimension)
	numNodes := len(h.nodes)

	ids := make([]int32, numNodes)
	vectors := make([]float32, numNodes*h.dimension)
	levels := make([]int32, numNodes)

	for i, node := range h.nodes {
		ids[i] = int32(node.ID())
		copy(vectors[i*h.dimension:(i+1)*h.dimension], node.Vector())
		levels[i] = int32(node.Level())
	}

	idArray := core.NewInt32Array(ids, nil)
	vectorArray := core.NewFloat32Array(vectors, nil)
	levelArray := core.NewInt32Array(levels, nil)

	vectorType := core.VectorType(h.dimension).(*core.FixedSizeListType)
	vectorListArray := core.NewFixedSizeListArray(vectorType, vectorArray, nil)

	return core.NewRecordBatch(schema, numNodes, []core.Array{
		idArray,
		vectorListArray,
		levelArray,
	})
}

// MarshalConnections serializes all graph edges into a core.RecordBatch.
// Returns nil, nil when there are no connections.
//
// TODO: For million-scale datasets, switch to streaming callbacks
// (batchSize int, emit func(*core.RecordBatch) error) to avoid multi-GB
// single-batch allocation.
func (h *HNSWIndex) MarshalConnections() (*core.RecordBatch, error) {
	h.globalLock.RLock()
	defer h.globalLock.RUnlock()

	var nodeIDs, layers, neighborIDs []int32

	for _, node := range h.nodes {
		nodeID := int32(node.ID())
		for layer := 0; layer <= node.Level(); layer++ {
			for _, neighborID := range node.GetConnections(layer) {
				nodeIDs = append(nodeIDs, nodeID)
				layers = append(layers, int32(layer))
				neighborIDs = append(neighborIDs, int32(neighborID))
			}
		}
	}

	if len(nodeIDs) == 0 {
		return nil, nil
	}

	schema := SchemaForConnections()
	nodeIDArray := core.NewInt32Array(nodeIDs, nil)
	layerArray := core.NewInt32Array(layers, nil)
	neighborIDArray := core.NewInt32Array(neighborIDs, nil)

	return core.NewRecordBatch(schema, len(nodeIDs), []core.Array{
		nodeIDArray,
		layerArray,
		neighborIDArray,
	})
}

// MetadataResult holds the unmarshaled index configuration.
type MetadataResult struct {
	M              int
	Mmax           int
	Mmax0          int
	EfConstruction int
	Dimension      int
	EntryPoint     int32
	MaxLevel       int32
	NumNodes       int
	DistanceFunc   DistanceFunc
}

// Distance function registry for stable serialization.
// Built-in functions are pre-registered in init(). Custom functions must be
// registered via RegisterDistanceFunc before use, otherwise Marshal will
// silently fallback to L2Distance.
//
// NOTE: The registry uses reflect.ValueOf(f).Pointer() to build the reverse
// mapping. This is stable for package-level functions but undefined for
// closures. Always register custom functions explicitly.
var (
	distanceFuncRegistry = make(map[string]DistanceFunc) // name -> func
	distanceFuncNames    = make(map[uintptr]string)      // func ptr -> name

	distanceFuncNameToID = map[string]int32{
		"l2":           0,
		"l2sqrt":       1,
		"cosine":       2,
		"innerproduct": 3,
	}
	distanceFuncIDToName = map[int32]string{
		0: "l2",
		1: "l2sqrt",
		2: "cosine",
		3: "innerproduct",
	}
)

func init() {
	RegisterDistanceFunc("l2", L2Distance)
	RegisterDistanceFunc("l2sqrt", L2DistanceSqrt)
	RegisterDistanceFunc("cosine", CosineDistance)
	RegisterDistanceFunc("innerproduct", InnerProductDistance)
}

// RegisterDistanceFunc registers a custom distance function so it can be
// serialized and deserialized correctly. Built-in functions are pre-registered.
// Unregistered functions will silently fallback to L2Distance during Marshal.
func RegisterDistanceFunc(name string, df DistanceFunc) {
	if df == nil {
		return
	}
	distanceFuncRegistry[name] = df
	// reflect.ValueOf(f).Pointer() is used here once at registration time.
	// It is stable for package-level functions but undefined for closures.
	distanceFuncNames[reflect.ValueOf(df).Pointer()] = name
}

func funcPtr(f DistanceFunc) uintptr {
	if f == nil {
		return 0
	}
	return reflect.ValueOf(f).Pointer()
}

func distanceFuncToInt32(df DistanceFunc) int32 {
	if df == nil {
		return 0
	}
	name, ok := distanceFuncNames[funcPtr(df)]
	if !ok {
		return 0 // unregistered -> fallback to L2
	}
	id, ok := distanceFuncNameToID[name]
	if !ok {
		return 0
	}
	return id
}

func int32ToDistanceFunc(v int32) DistanceFunc {
	name, ok := distanceFuncIDToName[v]
	if !ok {
		return L2Distance
	}
	df, ok := distanceFuncRegistry[name]
	if !ok {
		return L2Distance
	}
	return df
}

// MarshalMetadata serializes index configuration into a single-row RecordBatch.
func (h *HNSWIndex) MarshalMetadata() (*core.RecordBatch, error) {
	h.globalLock.RLock()
	defer h.globalLock.RUnlock()

	schema := SchemaForMetadata()
	metadata := []int32{
		int32(h.M),
		int32(h.Mmax),
		int32(h.Mmax0),
		int32(h.efConstruction),
		int32(h.dimension),
		h.entryPoint,
		h.maxLevel,
		int32(len(h.nodes)),
		distanceFuncToInt32(h.distFunc),
	}

	return core.NewRecordBatch(schema, 1, []core.Array{
		core.NewInt32Array([]int32{metadata[0]}, nil),
		core.NewInt32Array([]int32{metadata[1]}, nil),
		core.NewInt32Array([]int32{metadata[2]}, nil),
		core.NewInt32Array([]int32{metadata[3]}, nil),
		core.NewInt32Array([]int32{metadata[4]}, nil),
		core.NewInt32Array([]int32{metadata[5]}, nil),
		core.NewInt32Array([]int32{metadata[6]}, nil),
		core.NewInt32Array([]int32{metadata[7]}, nil),
		core.NewInt32Array([]int32{metadata[8]}, nil),
	})
}

// UnmarshalNodes reconstructs nodes from a RecordBatch. The index must already
// have the correct dimension configured.
func (h *HNSWIndex) UnmarshalNodes(batch *core.RecordBatch) error {
	idArray := batch.Column(0).(*core.Int32Array)
	vectorListArray := batch.Column(1).(*core.FixedSizeListArray)
	levelArray := batch.Column(2).(*core.Int32Array)

	vectorArray := vectorListArray.Values().(*core.Float32Array)
	vectorValues := vectorArray.Values()

	numNodes := idArray.Len()
	for i := 0; i < numNodes; i++ {
		id := int(idArray.Value(i))
		if id != i {
			return fmt.Errorf("node ID mismatch at index %d: expected %d, got %d", i, i, id)
		}
	}

	h.globalLock.Lock()
	defer h.globalLock.Unlock()

	h.nodes = make([]*Node, numNodes)
	for i := 0; i < numNodes; i++ {
		id := int(idArray.Value(i))
		level := int(levelArray.Value(i))
		start := i * h.dimension
		end := start + h.dimension
		vector := make([]float32, h.dimension)
		copy(vector, vectorValues[start:end])
		h.nodes[i] = NewNode(id, vector, level)
	}
	return nil
}

// UnmarshalConnections rebuilds graph edges from a RecordBatch.
func (h *HNSWIndex) UnmarshalConnections(batch *core.RecordBatch) error {
	if batch == nil {
		return nil
	}

	nodeIDArray := batch.Column(0).(*core.Int32Array)
	layerArray := batch.Column(1).(*core.Int32Array)
	neighborIDArray := batch.Column(2).(*core.Int32Array)

	numConnections := nodeIDArray.Len()

	h.globalLock.Lock()
	defer h.globalLock.Unlock()

	for i := 0; i < numConnections; i++ {
		nodeID := int(nodeIDArray.Value(i))
		layer := int(layerArray.Value(i))
		neighborID := int(neighborIDArray.Value(i))

		if nodeID < 0 || nodeID >= len(h.nodes) {
			return fmt.Errorf("invalid node_id %d at connection index %d", nodeID, i)
		}
		if neighborID < 0 || neighborID >= len(h.nodes) {
			return fmt.Errorf("invalid neighbor_id %d at connection index %d", neighborID, i)
		}
		if layer < 0 || layer > h.nodes[nodeID].Level() {
			return fmt.Errorf("invalid layer %d for node %d", layer, nodeID)
		}

		h.nodes[nodeID].AddConnection(layer, neighborID)
	}
	return nil
}

// UnmarshalMetadata extracts index configuration from a single-row RecordBatch.
// It is backward-compatible with legacy metadata that lacks the distanceFunc column.
func UnmarshalMetadata(batch *core.RecordBatch) (*MetadataResult, error) {
	if batch.NumRows() != 1 {
		return nil, fmt.Errorf("metadata batch must have exactly 1 row, got %d", batch.NumRows())
	}

	numCols := batch.NumCols()
	if numCols < 8 {
		return nil, fmt.Errorf("metadata batch must have at least 8 columns, got %d", numCols)
	}

	metadata := make([]int32, numCols)
	for i := 0; i < numCols; i++ {
		metadata[i] = batch.Column(i).(*core.Int32Array).Value(0)
	}

	result := &MetadataResult{
		M:              int(metadata[0]),
		Mmax:           int(metadata[1]),
		Mmax0:          int(metadata[2]),
		EfConstruction: int(metadata[3]),
		Dimension:      int(metadata[4]),
		EntryPoint:     metadata[5],
		MaxLevel:       metadata[6],
		NumNodes:       int(metadata[7]),
		DistanceFunc:   L2Distance,
	}

	if numCols >= 9 {
		result.DistanceFunc = int32ToDistanceFunc(metadata[8])
	}

	return result, nil
}
