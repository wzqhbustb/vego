package vego

import (
	"os"
	"path/filepath"
	"testing"

	hnsw "github.com/wzqhbustb/vego/index"
)

func TestSaveLoadHNSWIndex(t *testing.T) {
	tempDir := t.TempDir()

	config := hnsw.Config{
		M:              8,
		EfConstruction: 100,
		Dimension:      4,
		DistanceFunc:   hnsw.L2Distance,
	}
	idx := hnsw.NewHNSW(config)

	vectors := [][]float32{
		{1.0, 2.0, 3.0, 4.0},
		{2.0, 3.0, 4.0, 5.0},
		{3.0, 4.0, 5.0, 6.0},
	}
	for _, vec := range vectors {
		idx.Add(vec)
	}

	indexPath := filepath.Join(tempDir, "index")
	if err := saveHNSWIndex(idx, indexPath); err != nil {
		t.Fatalf("saveHNSWIndex failed: %v", err)
	}

	// Verify files are created
	for _, name := range []string{"nodes.lance", "connections.lance", "metadata.lance"} {
		p := filepath.Join(indexPath, name)
		if _, err := os.Stat(p); err != nil {
			t.Errorf("expected file %s to exist: %v", name, err)
		}
	}

	loaded, err := loadHNSWIndex(indexPath)
	if err != nil {
		t.Fatalf("loadHNSWIndex failed: %v", err)
	}

	if loaded.Len() != idx.Len() {
		t.Errorf("node count mismatch: got %d, want %d", loaded.Len(), idx.Len())
	}
}

func TestSaveLoadHNSWIndexEmpty(t *testing.T) {
	tempDir := t.TempDir()
	config := hnsw.Config{
		M:            8,
		Dimension:    4,
		DistanceFunc: hnsw.L2Distance,
	}
	idx := hnsw.NewHNSW(config)

	indexPath := filepath.Join(tempDir, "index")
	if err := saveHNSWIndex(idx, indexPath); err == nil {
		t.Error("expected error saving empty index, got nil")
	}
}

func TestSaveLoadHNSWIndexNoConnections(t *testing.T) {
	tempDir := t.TempDir()
	config := hnsw.Config{
		M:            8,
		Dimension:    4,
		DistanceFunc: hnsw.L2Distance,
	}
	idx := hnsw.NewHNSW(config)
	idx.Add([]float32{1.0, 2.0, 3.0, 4.0})

	indexPath := filepath.Join(tempDir, "index")
	if err := saveHNSWIndex(idx, indexPath); err != nil {
		t.Fatalf("save failed: %v", err)
	}

	// connections.lance should not exist for a single-node index
	connPath := filepath.Join(indexPath, "connections.lance")
	if _, err := os.Stat(connPath); !os.IsNotExist(err) {
		t.Error("expected connections.lance to not exist for single-node index")
	}

	loaded, err := loadHNSWIndex(indexPath)
	if err != nil {
		t.Fatalf("load failed: %v", err)
	}
	if loaded.Len() != 1 {
		t.Errorf("expected 1 node, got %d", loaded.Len())
	}
}

func TestSaveLoadHNSWIndexDistanceFunc(t *testing.T) {
	for _, df := range []hnsw.DistanceFunc{hnsw.CosineDistance, hnsw.InnerProductDistance} {
		tempDir := t.TempDir()
		config := hnsw.Config{
			M:              8,
			EfConstruction: 100,
			Dimension:      4,
			DistanceFunc:   df,
		}
		idx := hnsw.NewHNSW(config)
		for i := 0; i < 5; i++ {
			vec := []float32{float32(i), float32(i + 1), float32(i + 2), float32(i + 3)}
			idx.Add(vec)
		}

		indexPath := filepath.Join(tempDir, "index")
		if err := saveHNSWIndex(idx, indexPath); err != nil {
			t.Fatalf("save failed: %v", err)
		}

		loaded, err := loadHNSWIndex(indexPath)
		if err != nil {
			t.Fatalf("load failed: %v", err)
		}

		// Verify search works with restored distance function
		query := []float32{1.5, 2.5, 3.5, 4.5}
		results, err := loaded.Search(query, 3, 50)
		if err != nil {
			t.Fatalf("search failed after load: %v", err)
		}
		if len(results) == 0 {
			t.Error("search returned no results")
		}
	}
}

func TestLoadHNSWIndexMissingMetadata(t *testing.T) {
	tempDir := t.TempDir()
	config := hnsw.Config{
		M:            8,
		Dimension:    4,
		DistanceFunc: hnsw.L2Distance,
	}
	idx := hnsw.NewHNSW(config)
	idx.Add([]float32{1.0, 2.0, 3.0, 4.0})

	indexPath := filepath.Join(tempDir, "index")
	if err := saveHNSWIndex(idx, indexPath); err != nil {
		t.Fatalf("save failed: %v", err)
	}

	// Delete metadata file
	metaPath := filepath.Join(indexPath, "metadata.lance")
	if err := os.Remove(metaPath); err != nil {
		t.Fatalf("failed to remove metadata: %v", err)
	}

	if _, err := loadHNSWIndex(indexPath); err == nil {
		t.Error("expected error loading index with missing metadata, got nil")
	}
}

func TestLoadHNSWIndexMissingNodes(t *testing.T) {
	tempDir := t.TempDir()
	config := hnsw.Config{
		M:            8,
		Dimension:    4,
		DistanceFunc: hnsw.L2Distance,
	}
	idx := hnsw.NewHNSW(config)
	idx.Add([]float32{1.0, 2.0, 3.0, 4.0})

	indexPath := filepath.Join(tempDir, "index")
	if err := saveHNSWIndex(idx, indexPath); err != nil {
		t.Fatalf("save failed: %v", err)
	}

	// Delete nodes file
	nodesPath := filepath.Join(indexPath, "nodes.lance")
	if err := os.Remove(nodesPath); err != nil {
		t.Fatalf("failed to remove nodes: %v", err)
	}

	if _, err := loadHNSWIndex(indexPath); err == nil {
		t.Error("expected error loading index with missing nodes, got nil")
	}
}
