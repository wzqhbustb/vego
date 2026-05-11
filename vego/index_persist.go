package vego

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/wzqhbustb/vego/core"
	hnsw "github.com/wzqhbustb/vego/index"
	"github.com/wzqhbustb/vego/storage/column"
	"github.com/wzqhbustb/vego/storage/encoding"
)

func defaultEncoderFactory() *encoding.EncoderFactory {
	return encoding.NewEncoderFactory(3)
}

// saveHNSWIndex persists an HNSW index to Lance-format files in baseDir.
func saveHNSWIndex(idx *hnsw.HNSWIndex, baseDir string) error {
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return fmt.Errorf("create directory failed: %w", err)
	}

	nodesBatch, err := idx.MarshalNodes()
	if err != nil {
		return fmt.Errorf("marshal nodes failed: %w", err)
	}

	connBatch, err := idx.MarshalConnections()
	if err != nil {
		return fmt.Errorf("marshal connections failed: %w", err)
	}

	metaBatch, err := idx.MarshalMetadata()
	if err != nil {
		return fmt.Errorf("marshal metadata failed: %w", err)
	}

	if err := writeRecordBatch(filepath.Join(baseDir, "nodes.lance"), nodesBatch); err != nil {
		return fmt.Errorf("save nodes failed: %w", err)
	}

	if connBatch != nil {
		if err := writeRecordBatch(filepath.Join(baseDir, "connections.lance"), connBatch); err != nil {
			return fmt.Errorf("save connections failed: %w", err)
		}
	}

	if err := writeRecordBatch(filepath.Join(baseDir, "metadata.lance"), metaBatch); err != nil {
		return fmt.Errorf("save metadata failed: %w", err)
	}

	return nil
}

func writeRecordBatch(filename string, batch *core.RecordBatch) error {
	var writer column.BatchWriter
	writer, err := column.NewWriter(filename, batch.Schema(), defaultEncoderFactory())
	if err != nil {
		return fmt.Errorf("create writer failed: %w", err)
	}
	defer writer.Close()

	if err := writer.WriteRecordBatch(batch); err != nil {
		return fmt.Errorf("write record batch failed: %w", err)
	}
	return nil
}

// loadHNSWIndex restores an HNSW index from Lance-format files in baseDir.
func loadHNSWIndex(baseDir string) (*hnsw.HNSWIndex, error) {
	metaBatch, err := readRecordBatch(filepath.Join(baseDir, "metadata.lance"))
	if err != nil {
		return nil, fmt.Errorf("load metadata failed: %w", err)
	}

	meta, err := hnsw.UnmarshalMetadata(metaBatch)
	if err != nil {
		return nil, fmt.Errorf("unmarshal metadata failed: %w", err)
	}

	config := hnsw.Config{
		M:              meta.M,
		EfConstruction: meta.EfConstruction,
		Dimension:      meta.Dimension,
		DistanceFunc:   meta.DistanceFunc,
	}

	idx := hnsw.NewHNSW(config)
	idx.SetEntryPoint(meta.EntryPoint)
	idx.SetMaxLevel(meta.MaxLevel)

	nodesBatch, err := readRecordBatch(filepath.Join(baseDir, "nodes.lance"))
	if err != nil {
		return nil, fmt.Errorf("load nodes failed: %w", err)
	}

	if err := idx.UnmarshalNodes(nodesBatch); err != nil {
		return nil, fmt.Errorf("unmarshal nodes failed: %w", err)
	}

	connPath := filepath.Join(baseDir, "connections.lance")
	if _, err := os.Stat(connPath); err != nil {
		if os.IsNotExist(err) {
			return idx, nil
		}
		return nil, fmt.Errorf("stat connections failed: %w", err)
	}
	connBatch, err := readRecordBatch(connPath)
	if err != nil {
		return nil, fmt.Errorf("load connections failed: %w", err)
	}
	if err := idx.UnmarshalConnections(connBatch); err != nil {
		return nil, fmt.Errorf("unmarshal connections failed: %w", err)
	}

	return idx, nil
}

func readRecordBatch(filename string) (*core.RecordBatch, error) {
	var reader column.BatchReader
	reader, err := column.NewReader(filename)
	if err != nil {
		return nil, fmt.Errorf("create reader failed: %w", err)
	}
	defer reader.Close()

	batch, err := reader.ReadRecordBatch()
	if err != nil {
		return nil, fmt.Errorf("read record batch failed: %w", err)
	}
	return batch, nil
}
