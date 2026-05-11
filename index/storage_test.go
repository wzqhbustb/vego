package hnsw

import (
	"fmt"
	"testing"
)

// marshalRoundTrip performs a pure in-memory marshal/unmarshal round-trip.
// It returns a new HNSWIndex reconstructed from the marshaled batches.
func marshalRoundTrip(original *HNSWIndex) (*HNSWIndex, error) {
	metaBatch, err := original.MarshalMetadata()
	if err != nil {
		return nil, fmt.Errorf("marshal metadata: %w", err)
	}
	meta, err := UnmarshalMetadata(metaBatch)
	if err != nil {
		return nil, fmt.Errorf("unmarshal metadata: %w", err)
	}

	config := Config{
		M:              meta.M,
		EfConstruction: meta.EfConstruction,
		Dimension:      meta.Dimension,
		DistanceFunc:   meta.DistanceFunc,
	}
	loaded := NewHNSW(config)
	loaded.SetEntryPoint(meta.EntryPoint)
	loaded.SetMaxLevel(meta.MaxLevel)

	nodesBatch, err := original.MarshalNodes()
	if err != nil {
		return nil, fmt.Errorf("marshal nodes: %w", err)
	}
	if err := loaded.UnmarshalNodes(nodesBatch); err != nil {
		return nil, fmt.Errorf("unmarshal nodes: %w", err)
	}

	connBatch, err := original.MarshalConnections()
	if err != nil {
		return nil, fmt.Errorf("marshal connections: %w", err)
	}
	if err := loaded.UnmarshalConnections(connBatch); err != nil {
		return nil, fmt.Errorf("unmarshal connections: %w", err)
	}

	return loaded, nil
}

func TestHNSWStorageBasic(t *testing.T) {
	config := Config{
		M:              16,
		EfConstruction: 200,
		Dimension:      4,
		DistanceFunc:   L2Distance,
	}

	hnsw := NewHNSW(config)

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

	loadedHNSW, err := marshalRoundTrip(hnsw)
	if err != nil {
		t.Fatalf("Round-trip failed: %v", err)
	}

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
	if funcPtr(loadedHNSW.distFunc) != funcPtr(hnsw.distFunc) {
		t.Errorf("DistanceFunc mismatch: got %p, want %p", loadedHNSW.distFunc, hnsw.distFunc)
	}

	if len(loadedHNSW.nodes) != len(hnsw.nodes) {
		t.Errorf("Node count mismatch: got %d, want %d", len(loadedHNSW.nodes), len(hnsw.nodes))
	}

	for i, originalNode := range hnsw.nodes {
		if i >= len(loadedHNSW.nodes) {
			t.Errorf("Missing node at index %d", i)
			continue
		}

		loadedNode := loadedHNSW.nodes[i]

		if loadedNode.ID() != originalNode.ID() {
			t.Errorf("Node %d ID mismatch: got %d, want %d", i, loadedNode.ID(), originalNode.ID())
		}

		if loadedNode.Level() != originalNode.Level() {
			t.Errorf("Node %d level mismatch: got %d, want %d", i, loadedNode.Level(), originalNode.Level())
		}

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

		for layer := 0; layer <= originalNode.Level(); layer++ {
			originalConnections := originalNode.GetConnections(layer)
			loadedConnections := loadedNode.GetConnections(layer)

			if len(loadedConnections) != len(originalConnections) {
				t.Errorf("Node %d layer %d connection count mismatch: got %d, want %d",
					i, layer, len(loadedConnections), len(originalConnections))
				continue
			}

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
	config := Config{
		M:              8,
		EfConstruction: 100,
		Dimension:      3,
		DistanceFunc:   L2Distance,
	}

	hnsw := NewHNSW(config)

	_, err := hnsw.MarshalNodes()
	if err == nil {
		t.Error("Expected error when marshaling empty HNSW, but got none")
	}
	if err != nil && err.Error() != "no nodes to marshal" {
		t.Errorf("Expected 'no nodes to marshal' error, got: %v", err)
	}

	t.Logf("✓ Empty index test passed: correctly rejected empty HNSW")
}

func TestHNSWStorageLargeDataset(t *testing.T) {
	config := Config{
		M:              16,
		EfConstruction: 200,
		Dimension:      128,
		DistanceFunc:   L2Distance,
	}

	hnsw := NewHNSW(config)

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

	loadedHNSW, err := marshalRoundTrip(hnsw)
	if err != nil {
		t.Fatalf("Round-trip failed: %v", err)
	}

	if len(loadedHNSW.nodes) != numVectors {
		t.Errorf("Node count mismatch: got %d, want %d", len(loadedHNSW.nodes), numVectors)
	}

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
	config := Config{
		M:              16,
		EfConstruction: 200,
		Dimension:      768,
		DistanceFunc:   L2Distance,
	}

	hnsw := NewHNSW(config)

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

	loadedHNSW, err := marshalRoundTrip(hnsw)
	if err != nil {
		t.Fatalf("Round-trip failed: %v", err)
	}

	if loadedHNSW.dimension != 768 {
		t.Errorf("Dimension mismatch: got %d, want 768", loadedHNSW.dimension)
	}

	for i := 0; i < numVectors; i++ {
		originalVec := hnsw.nodes[i].Vector()
		loadedVec := loadedHNSW.nodes[i].Vector()

		if len(loadedVec) != 768 {
			t.Errorf("Node %d vector dimension mismatch: got %d, want 768", i, len(loadedVec))
		}

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
	config := Config{
		M:              4,
		EfConstruction: 100,
		Dimension:      3,
		DistanceFunc:   L2Distance,
	}

	hnsw := NewHNSW(config)

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

	originalTotalConnections := 0
	for _, node := range hnsw.nodes {
		for layer := 0; layer <= node.Level(); layer++ {
			originalTotalConnections += len(node.GetConnections(layer))
		}
	}

	loadedHNSW, err := marshalRoundTrip(hnsw)
	if err != nil {
		t.Fatalf("Round-trip failed: %v", err)
	}

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

	for i, originalNode := range hnsw.nodes {
		loadedNode := loadedHNSW.nodes[i]

		for layer := 0; layer <= originalNode.Level(); layer++ {
			originalConns := originalNode.GetConnections(layer)
			loadedConns := loadedNode.GetConnections(layer)

			originalSet := make(map[int]bool)
			for _, conn := range originalConns {
				originalSet[conn] = true
			}

			loadedSet := make(map[int]bool)
			for _, conn := range loadedConns {
				loadedSet[conn] = true
			}

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
	config := Config{
		M:              16,
		EfConstruction: 200,
		Dimension:      4,
		DistanceFunc:   L2Distance,
	}

	hnsw1 := NewHNSW(config)
	for i := 0; i < 5; i++ {
		vec := []float32{float32(i), float32(i + 1), float32(i + 2), float32(i + 3)}
		hnsw1.Add(vec)
	}

	hnsw2, err := marshalRoundTrip(hnsw1)
	if err != nil {
		t.Fatalf("First round-trip failed: %v", err)
	}

	if len(hnsw2.nodes) != 5 {
		t.Errorf("After first round-trip: got %d nodes, want 5", len(hnsw2.nodes))
	}

	hnsw3, err := marshalRoundTrip(hnsw2)
	if err != nil {
		t.Fatalf("Second round-trip failed: %v", err)
	}

	if len(hnsw3.nodes) != 5 {
		t.Errorf("After second round-trip: got %d nodes, want 5", len(hnsw3.nodes))
	}

	for i := 0; i < 5; i++ {
		vec1 := hnsw1.nodes[i].Vector()
		vec3 := hnsw3.nodes[i].Vector()

		for j := 0; j < 4; j++ {
			if vec1[j] != vec3[j] {
				t.Errorf("Multiple round-trip: node %d vector[%d] mismatch", i, j)
			}
		}
	}

	t.Logf("✓ Multiple round-trip test passed")
}

func TestHNSWStorageSearchConsistency(t *testing.T) {
	config := Config{
		M:              16,
		EfConstruction: 200,
		Dimension:      8,
		DistanceFunc:   L2Distance,
	}

	hnsw := NewHNSW(config)

	numVectors := 50
	for i := 0; i < numVectors; i++ {
		vector := make([]float32, 8)
		for j := 0; j < 8; j++ {
			vector[j] = float32(i+j) * 0.1
		}
		hnsw.Add(vector)
	}

	queryVector := []float32{2.5, 2.6, 2.7, 2.8, 2.9, 3.0, 3.1, 3.2}
	originalResults, err := hnsw.Search(queryVector, 5, 100)
	if err != nil {
		t.Fatalf("Original search failed: %v", err)
	}

	loadedHNSW, err := marshalRoundTrip(hnsw)
	if err != nil {
		t.Fatalf("Round-trip failed: %v", err)
	}

	loadedResults, err := loadedHNSW.Search(queryVector, 5, 100)
	if err != nil {
		t.Fatalf("Loaded search failed: %v", err)
	}

	if len(loadedResults) != len(originalResults) {
		t.Errorf("Search result count mismatch: got %d, want %d",
			len(loadedResults), len(originalResults))
	}

	for i := 0; i < min(len(originalResults), len(loadedResults)); i++ {
		if originalResults[i].ID != loadedResults[i].ID {
			t.Errorf("Result %d ID mismatch: got %d, want %d",
				i, loadedResults[i].ID, originalResults[i].ID)
		}

		distDiff := abs(originalResults[i].Distance - loadedResults[i].Distance)
		if distDiff > 1e-5 {
			t.Errorf("Result %d distance mismatch: got %f, want %f (diff: %f)",
				i, loadedResults[i].Distance, originalResults[i].Distance, distDiff)
		}
	}

	t.Logf("✓ Search consistency test passed: results match after round-trip")
}

func TestHNSWStorageDistanceFunc(t *testing.T) {
	for _, df := range []DistanceFunc{CosineDistance, InnerProductDistance, L2DistanceSqrt} {
		name := ""
		switch funcPtr(df) {
		case funcPtr(CosineDistance):
			name = "Cosine"
		case funcPtr(InnerProductDistance):
			name = "InnerProduct"
		case funcPtr(L2DistanceSqrt):
			name = "L2Sqrt"
		}

		t.Run(name, func(t *testing.T) {
			config := Config{
				M:              8,
				EfConstruction: 100,
				Dimension:      4,
				DistanceFunc:   df,
			}

			hnsw := NewHNSW(config)
			for i := 0; i < 10; i++ {
				vec := []float32{float32(i), float32(i + 1), float32(i + 2), float32(i + 3)}
				hnsw.Add(vec)
			}

			loaded, err := marshalRoundTrip(hnsw)
			if err != nil {
				t.Fatalf("Round-trip failed: %v", err)
			}

			if funcPtr(loaded.distFunc) != funcPtr(df) {
				t.Errorf("DistanceFunc mismatch: got %p, want %p", loaded.distFunc, df)
			}
		})
	}
}

func abs(x float32) float32 {
	if x < 0 {
		return -x
	}
	return x
}
