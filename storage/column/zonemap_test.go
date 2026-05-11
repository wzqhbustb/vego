// Copyright 2024 Vego Authors
// Licensed under the Apache License, Version 2.0

package column

import (
	"math"
	"os"
	"testing"

	"github.com/wzqhbustb/vego/core"
	"github.com/wzqhbustb/vego/storage/format"
)

func TestZoneMapEvaluatorIndependent(t *testing.T) {
	// Create statistics list manually
	sl := format.NewStatisticsList(2)
	
	// Set up column 0: int32 [10, 50]
	sl.Stats[0].SetMinMaxInt32(10, 50)
	
	// Set up column 1: float64 [1.5, 9.5]
	sl.Stats[1].SetMinMaxFloat64(1.5, 9.5)
	
	// Create evaluator directly from statistics
	evaluator := NewZoneMapEvaluator(sl)
	
	// Test integer column evaluation
	result := evaluator.EvaluateZoneMapInt32(0, 30)
	if !result.MayContainData {
		t.Error("Expected MayContainData=true for value=30 in range [10,50]")
	}
	
	result = evaluator.EvaluateZoneMapInt32(0, 5)
	if result.MayContainData {
		t.Error("Expected MayContainData=false for value=5 outside range [10,50]")
	}
	
	// Test float column evaluation
	result = evaluator.EvaluateZoneMapFloat64(1, 5.0)
	if !result.MayContainData {
		t.Error("Expected MayContainData=true for value=5.0 in range [1.5,9.5]")
	}
	
	result = evaluator.EvaluateZoneMapFloat64(1, 15.0)
	if result.MayContainData {
		t.Error("Expected MayContainData=false for value=15.0 outside range [1.5,9.5]")
	}
}

func TestZoneMapEvaluatorNaNQuery(t *testing.T) {
	sl := format.NewStatisticsList(1)
	sl.Stats[0].SetMinMaxFloat64(1.0, 10.0)
	
	evaluator := NewZoneMapEvaluator(sl)
	
	// NaN query should use conservative strategy
	result := evaluator.EvaluateZoneMapFloat64(0, math.NaN())
	if !result.MayContainData {
		t.Error("Expected MayContainData=true for NaN query")
	}
	
	// Range with NaN bounds
	result = evaluator.EvaluateZoneMapRangeFloat64(0, math.NaN(), 5.0)
	if !result.MayContainData {
		t.Error("Expected MayContainData=true when low bound is NaN")
	}
	
	result = evaluator.EvaluateZoneMapRangeFloat64(0, 0.0, math.NaN())
	if !result.MayContainData {
		t.Error("Expected MayContainData=true when high bound is NaN")
	}
}

func TestZoneMapEvaluatorNoStats(t *testing.T) {
	// Evaluator with nil stats
	evaluator := NewZoneMapEvaluator(nil)
	
	// Should return conservative result
	result := evaluator.EvaluateZoneMapInt32(0, 100)
	if !result.MayContainData {
		t.Error("Expected MayContainData=true when no statistics available")
	}
	
	if evaluator.HasStatistics() {
		t.Error("Expected HasStatistics=false")
	}
}

func TestZoneMapEvaluatorReaderDelegation(t *testing.T) {
	// Create a mock scenario using actual file write/read
	tmpFile := "/tmp/test_zonemap_evaluator.lance"
	defer os.Remove(tmpFile)
	
	schema := core.NewSchema([]core.Field{
		core.NewField("id", core.PrimInt32(), false),
	}, nil)
	
	writer, err := NewWriter(tmpFile, schema, nil)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}
	
	batch := createTestRecordBatchInt32(t, schema, []int32{10, 20, 30, 40, 50})
	if err := writer.WriteRecordBatch(batch); err != nil {
		t.Fatalf("Failed to write batch: %v", err)
	}
	
	if err := writer.Close(); err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}
	
	// Read back and use both Reader methods and ZoneMapEvaluator
	reader, err := NewReader(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Close()
	
	// Test Reader delegation
	result1 := reader.EvaluateZoneMapInt32(0, 25)
	
	// Test direct ZoneMapEvaluator
	evaluator := reader.ZoneMapEvaluator()
	result2 := evaluator.EvaluateZoneMapInt32(0, 25)
	
	// Results should be identical
	if result1.MayContainData != result2.MayContainData {
		t.Errorf("Reader and ZoneMapEvaluator results differ: Reader=%v, Evaluator=%v",
			result1.MayContainData, result2.MayContainData)
	}
}
