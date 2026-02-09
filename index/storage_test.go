package hnsw

import (
	"os"
	"path/filepath"
	"testing"
)

func TestHNSWStorageBasic(t *testing.T) {
	// Create temporary directory
	tempDir := t.TempDir()

	// Create test HNSW index
	config := Config{
		M:              16,
		EfConstruction: 200,
		Dimension:      4,
		DistanceFunc:   L2Distance,
	}

	hnsw := NewHNSW(config)

	// Add test vectors
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

	// Save to Lance format
	if err := hnsw.SaveToLance(tempDir); err != nil {
		t.Fatalf("Failed to save HNSW: %v", err)
	}

	// Verify files are created
	expectedFiles := []string{"nodes.lance", "connections.lance", "metadata.lance"}
	for _, filename := range expectedFiles {
		fullPath := filepath.Join(tempDir, filename)
		if _, err := os.Stat(fullPath); os.IsNotExist(err) {
			t.Errorf("Expected file %s was not created", filename)
		}
	}

	// Load from Lance format
	loadedHNSW, err := LoadHNSWFromLance(tempDir)
	if err != nil {
		t.Fatalf("Failed to load HNSW: %v", err)
	}

	// Verify basic properties
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

	// Verify node count
	if len(loadedHNSW.nodes) != len(hnsw.nodes) {
		t.Errorf("Node count mismatch: got %d, want %d", len(loadedHNSW.nodes), len(hnsw.nodes))
	}

	// Verify node content
	for i, originalNode := range hnsw.nodes {
		if i >= len(loadedHNSW.nodes) {
			t.Errorf("Missing node at index %d", i)
			continue
		}

		loadedNode := loadedHNSW.nodes[i]

		// Verify node ID
		if loadedNode.ID() != originalNode.ID() {
			t.Errorf("Node %d ID mismatch: got %d, want %d", i, loadedNode.ID(), originalNode.ID())
		}

		// Verify node level
		if loadedNode.Level() != originalNode.Level() {
			t.Errorf("Node %d level mismatch: got %d, want %d", i, loadedNode.Level(), originalNode.Level())
		}

		// Verify vector
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

		// Verify connections
		for layer := 0; layer <= originalNode.Level(); layer++ {
			originalConnections := originalNode.GetConnections(layer)
			loadedConnections := loadedNode.GetConnections(layer)

			if len(loadedConnections) != len(originalConnections) {
				t.Errorf("Node %d layer %d connection count mismatch: got %d, want %d",
					i, layer, len(loadedConnections), len(originalConnections))
				continue
			}

			// Convert to map for comparison (order may differ)
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

	// Test search functionality
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
	// Test persistence of empty HNSW
	tempDir := t.TempDir()

	config := Config{
		M:              8,
		EfConstruction: 100,
		Dimension:      3,
		DistanceFunc:   L2Distance,
	}

	hnsw := NewHNSW(config)

	// Attempting to save empty index should fail
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
	// Test persistence of larger dataset
	tempDir := t.TempDir()

	config := Config{
		M:              16,
		EfConstruction: 200,
		Dimension:      128,
		DistanceFunc:   L2Distance,
	}

	hnsw := NewHNSW(config)

	// Add 100 vectors
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

	// Save and load
	if err := hnsw.SaveToLance(tempDir); err != nil {
		t.Fatalf("Failed to save large HNSW: %v", err)
	}

	loadedHNSW, err := LoadHNSWFromLance(tempDir)
	if err != nil {
		t.Fatalf("Failed to load large HNSW: %v", err)
	}

	// Basic verification
	if len(loadedHNSW.nodes) != numVectors {
		t.Errorf("Node count mismatch: got %d, want %d", len(loadedHNSW.nodes), numVectors)
	}

	// Verify search functionality
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
	// Test high-dimensional vectors (simulating real embeddings)
	tempDir := t.TempDir()

	config := Config{
		M:              16,
		EfConstruction: 200,
		Dimension:      768, // Common BERT embedding dimension
		DistanceFunc:   L2Distance,
	}

	hnsw := NewHNSW(config)

	// Add 10 high-dimensional vectors
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

	// Save
	if err := hnsw.SaveToLance(tempDir); err != nil {
		t.Fatalf("Failed to save high-dim HNSW: %v", err)
	}

	// Load
	loadedHNSW, err := LoadHNSWFromLance(tempDir)
	if err != nil {
		t.Fatalf("Failed to load high-dim HNSW: %v", err)
	}

	// Verify dimension
	if loadedHNSW.dimension != 768 {
		t.Errorf("Dimension mismatch: got %d, want 768", loadedHNSW.dimension)
	}

	// Verify vector data integrity
	for i := 0; i < numVectors; i++ {
		originalVec := hnsw.nodes[i].Vector()
		loadedVec := loadedHNSW.nodes[i].Vector()

		if len(loadedVec) != 768 {
			t.Errorf("Node %d vector dimension mismatch: got %d, want 768", i, len(loadedVec))
		}

		// Sample verify a few values
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
	// Specifically test connection integrity
	tempDir := t.TempDir()

	config := Config{
		M:              4, // Smaller M value, easier to test connections
		EfConstruction: 100,
		Dimension:      3,
		DistanceFunc:   L2Distance,
	}

	hnsw := NewHNSW(config)

	// Add enough vectors to create multi-level structure
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

	// Calculate original total connections
	originalTotalConnections := 0
	for _, node := range hnsw.nodes {
		for layer := 0; layer <= node.Level(); layer++ {
			originalTotalConnections += len(node.GetConnections(layer))
		}
	}

	// Save and load
	if err := hnsw.SaveToLance(tempDir); err != nil {
		t.Fatalf("Failed to save HNSW: %v", err)
	}

	loadedHNSW, err := LoadHNSWFromLance(tempDir)
	if err != nil {
		t.Fatalf("Failed to load HNSW: %v", err)
	}

	// Calculate total connections after loading
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

	// Verify connections of each node
	for i, originalNode := range hnsw.nodes {
		loadedNode := loadedHNSW.nodes[i]

		for layer := 0; layer <= originalNode.Level(); layer++ {
			originalConns := originalNode.GetConnections(layer)
			loadedConns := loadedNode.GetConnections(layer)

			// Build sets for comparison
			originalSet := make(map[int]bool)
			for _, conn := range originalConns {
				originalSet[conn] = true
			}

			loadedSet := make(map[int]bool)
			for _, conn := range loadedConns {
				loadedSet[conn] = true
			}

			// Check if completely consistent
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
	// Test multiple save and load
	tempDir := t.TempDir()

	config := Config{
		M:              16,
		EfConstruction: 200,
		Dimension:      4,
		DistanceFunc:   L2Distance,
	}

	// First: create and save
	hnsw1 := NewHNSW(config)
	for i := 0; i < 5; i++ {
		vec := []float32{float32(i), float32(i + 1), float32(i + 2), float32(i + 3)}
		hnsw1.Add(vec)
	}

	if err := hnsw1.SaveToLance(tempDir); err != nil {
		t.Fatalf("First save failed: %v", err)
	}

	// Second: load and verify
	hnsw2, err := LoadHNSWFromLance(tempDir)
	if err != nil {
		t.Fatalf("First load failed: %v", err)
	}

	if len(hnsw2.nodes) != 5 {
		t.Errorf("After first load: got %d nodes, want 5", len(hnsw2.nodes))
	}

	// Third: save same data to new directory
	tempDir2 := t.TempDir()
	if err := hnsw2.SaveToLance(tempDir2); err != nil {
		t.Fatalf("Second save failed: %v", err)
	}

	// Fourth: load and verify
	hnsw3, err := LoadHNSWFromLance(tempDir2)
	if err != nil {
		t.Fatalf("Second load failed: %v", err)
	}

	if len(hnsw3.nodes) != 5 {
		t.Errorf("After second load: got %d nodes, want 5", len(hnsw3.nodes))
	}

	// Verify vector data consistency
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
	// Test search result consistency after save/load
	tempDir := t.TempDir()

	config := Config{
		M:              16,
		EfConstruction: 200,
		Dimension:      8,
		DistanceFunc:   L2Distance,
	}

	hnsw := NewHNSW(config)

	// Add test vectors
	numVectors := 50
	for i := 0; i < numVectors; i++ {
		vector := make([]float32, 8)
		for j := 0; j < 8; j++ {
			vector[j] = float32(i+j) * 0.1
		}
		hnsw.Add(vector)
	}

	// Execute search before save
	queryVector := []float32{2.5, 2.6, 2.7, 2.8, 2.9, 3.0, 3.1, 3.2}
	originalResults, err := hnsw.Search(queryVector, 5, 100)
	if err != nil {
		t.Fatalf("Original search failed: %v", err)
	}

	// Save and load
	if err := hnsw.SaveToLance(tempDir); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	loadedHNSW, err := LoadHNSWFromLance(tempDir)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	// Execute same search after load
	loadedResults, err := loadedHNSW.Search(queryVector, 5, 100)
	if err != nil {
		t.Fatalf("Loaded search failed: %v", err)
	}

	// Verify search result consistency
	if len(loadedResults) != len(originalResults) {
		t.Errorf("Search result count mismatch: got %d, want %d",
			len(loadedResults), len(originalResults))
	}

	// Verify IDs and distances of first few results
	for i := 0; i < min(len(originalResults), len(loadedResults)); i++ {
		if originalResults[i].ID != loadedResults[i].ID {
			t.Errorf("Result %d ID mismatch: got %d, want %d",
				i, loadedResults[i].ID, originalResults[i].ID)
		}

		// Distances should be very close (allow floating point error)
		distDiff := abs(originalResults[i].Distance - loadedResults[i].Distance)
		if distDiff > 1e-5 {
			t.Errorf("Result %d distance mismatch: got %f, want %f (diff: %f)",
				i, loadedResults[i].Distance, originalResults[i].Distance, distDiff)
		}
	}

	t.Logf("✓ Search consistency test passed: results match after save/load")
}

// Helper function
func abs(x float32) float32 {
	if x < 0 {
		return -x
	}
	return x
}
