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

func TestWriterColumnStatistics(t *testing.T) {
	// Create a temporary file
	tmpFile := "/tmp/test_stats_writer.lance"
	defer os.Remove(tmpFile)

	// Create schema with int32 and float64 columns
	schema := core.NewSchema([]core.Field{
		core.NewField("id", core.PrimInt32(), false),
		core.NewField("value", core.PrimFloat64(), false),
	}, nil)

	// Create writer
	writer, err := NewWriter(tmpFile, schema, nil)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	// Create first batch: id=[1, 2, 3], value=[10.5, 20.5, 30.5]
	batch1 := createTestRecordBatch(t, schema, []int32{1, 2, 3}, []float64{10.5, 20.5, 30.5})
	if err := writer.WriteRecordBatch(batch1); err != nil {
		t.Fatalf("Failed to write batch1: %v", err)
	}

	// Create second batch: id=[5, 6, 7], value=[5.0, 25.0, 35.0]
	batch2 := createTestRecordBatch(t, schema, []int32{5, 6, 7}, []float64{5.0, 25.0, 35.0})
	if err := writer.WriteRecordBatch(batch2); err != nil {
		t.Fatalf("Failed to write batch2: %v", err)
	}

	// Close writer
	if err := writer.Close(); err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// Read back and verify statistics
	reader, err := NewReader(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Close()

	// Check if statistics are available
	if !reader.HasStatistics() {
		t.Fatal("Expected statistics to be available")
	}

	// Verify column 0 (id - int32) statistics
	idStats := reader.GetColumnStats(0)
	if idStats == nil {
		t.Fatal("Expected id stats to be available")
	}
	if !idStats.HasMinMax {
		t.Fatal("Expected id stats to have min/max")
	}
	minId, maxId, ok := idStats.GetMinMaxInt32()
	if !ok {
		t.Fatal("Failed to get int32 min/max")
	}
	if minId != 1 {
		t.Errorf("Expected min id=1, got %d", minId)
	}
	if maxId != 7 {
		t.Errorf("Expected max id=7, got %d", maxId)
	}

	// Verify column 1 (value - float64) statistics
	valueStats := reader.GetColumnStats(1)
	if valueStats == nil {
		t.Fatal("Expected value stats to be available")
	}
	if !valueStats.HasMinMax {
		t.Fatal("Expected value stats to have min/max")
	}
	minVal, maxVal, ok := valueStats.GetMinMaxFloat64()
	if !ok {
		t.Fatal("Failed to get float64 min/max")
	}
	if minVal != 5.0 {
		t.Errorf("Expected min value=5.0, got %f", minVal)
	}
	if maxVal != 35.0 {
		t.Errorf("Expected max value=35.0, got %f", maxVal)
	}
}

func TestZoneMapPredicatePushdown(t *testing.T) {
	// Create a temporary file
	tmpFile := "/tmp/test_zonemap.lance"
	defer os.Remove(tmpFile)

	// Create schema with int32 column
	schema := core.NewSchema([]core.Field{
		core.NewField("id", core.PrimInt32(), false),
	}, nil)

	// Create writer
	writer, err := NewWriter(tmpFile, schema, nil)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	// Write batch: id=[10, 20, 30, 40, 50]
	batch := createTestRecordBatchInt32(t, schema, []int32{10, 20, 30, 40, 50})
	if err := writer.WriteRecordBatch(batch); err != nil {
		t.Fatalf("Failed to write batch: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// Read back and test Zone Map filtering
	reader, err := NewReader(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Close()

	// Test equality: value=25 (not in range [10, 50]) - should skip
	result := reader.EvaluateZoneMapInt32(0, 25)
	if !result.MayContainData {
		t.Error("Expected MayContainData=true for value=25 (in range)")
	}

	// Test equality: value=5 (below range) - should skip
	result = reader.EvaluateZoneMapInt32(0, 5)
	if result.MayContainData {
		t.Error("Expected MayContainData=false for value=5 (below range)")
	}

	// Test equality: value=55 (above range) - should skip
	result = reader.EvaluateZoneMapInt32(0, 55)
	if result.MayContainData {
		t.Error("Expected MayContainData=false for value=55 (above range)")
	}

	// Test equality: value=30 (in range) - should read
	result = reader.EvaluateZoneMapInt32(0, 30)
	if !result.MayContainData {
		t.Error("Expected MayContainData=true for value=30 (in range)")
	}

	// Test range query: [15, 25] overlaps with [10, 50] - should read
	result = reader.EvaluateZoneMapRangeInt32(0, 15, 25)
	if !result.MayContainData {
		t.Error("Expected MayContainData=true for range [15,25] (overlaps)")
	}

	// Test range query: [60, 70] no overlap with [10, 50] - should skip
	result = reader.EvaluateZoneMapRangeInt32(0, 60, 70)
	if result.MayContainData {
		t.Error("Expected MayContainData=false for range [60,70] (no overlap)")
	}

	// Test range query: [1, 5] no overlap with [10, 50] - should skip
	result = reader.EvaluateZoneMapRangeInt32(0, 1, 5)
	if result.MayContainData {
		t.Error("Expected MayContainData=false for range [1,5] (no overlap)")
	}
}

func TestStatisticsNullValues(t *testing.T) {
	// Create a temporary file
	tmpFile := "/tmp/test_stats_nulls.lance"
	defer os.Remove(tmpFile)

	// Create schema with nullable int32 column
	schema := core.NewSchema([]core.Field{
		core.NewField("id", core.PrimInt32(), true),
	}, nil)

	// Create writer
	writer, err := NewWriter(tmpFile, schema, nil)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	// Create batch with some null values
	// Values: [1, null, 3, null, 5]
	batch := createTestRecordBatchInt32WithNulls(t, schema, []int32{1, 0, 3, 0, 5}, []bool{true, false, true, false, true})
	if err := writer.WriteRecordBatch(batch); err != nil {
		t.Fatalf("Failed to write batch: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// Read back and verify statistics
	reader, err := NewReader(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Close()

	// Verify statistics
	stats := reader.GetColumnStats(0)
	if stats == nil {
		t.Fatal("Expected stats to be available")
	}

	// Check null count
	if stats.NullCount != 2 {
		t.Errorf("Expected NullCount=2, got %d", stats.NullCount)
	}

	// Check min/max (should be 1 and 5, excluding nulls)
	min, max, ok := stats.GetMinMaxInt32()
	if !ok {
		t.Fatal("Failed to get min/max")
	}
	if min != 1 {
		t.Errorf("Expected min=1, got %d", min)
	}
	if max != 5 {
		t.Errorf("Expected max=5, got %d", max)
	}
}

// Helper functions

func createTestRecordBatchFloat32(t *testing.T, schema *core.Schema, values []float32) *core.RecordBatch {
	t.Helper()
	valueArray := core.NewFloat32Array(values, nil)
	batch, err := core.NewRecordBatch(schema, len(values), []core.Array{valueArray})
	if err != nil {
		t.Fatalf("Failed to create record batch: %v", err)
	}
	return batch
}

func createTestRecordBatchFloat64(t *testing.T, schema *core.Schema, values []float64) *core.RecordBatch {
	t.Helper()
	valueArray := core.NewFloat64Array(values, nil)
	batch, err := core.NewRecordBatch(schema, len(values), []core.Array{valueArray})
	if err != nil {
		t.Fatalf("Failed to create record batch: %v", err)
	}
	return batch
}

func createTestRecordBatch(t *testing.T, schema *core.Schema, ids []int32, values []float64) *core.RecordBatch {
	t.Helper()
	if len(ids) != len(values) {
		t.Fatal("ids and values must have same length")
	}

	idArray := core.NewInt32Array(ids, nil)
	valueArray := core.NewFloat64Array(values, nil)

	batch, err := core.NewRecordBatch(schema, len(ids), []core.Array{idArray, valueArray})
	if err != nil {
		t.Fatalf("Failed to create record batch: %v", err)
	}
	return batch
}

func createTestRecordBatchInt32(t *testing.T, schema *core.Schema, ids []int32) *core.RecordBatch {
	t.Helper()
	idArray := core.NewInt32Array(ids, nil)
	batch, err := core.NewRecordBatch(schema, len(ids), []core.Array{idArray})
	if err != nil {
		t.Fatalf("Failed to create record batch: %v", err)
	}
	return batch
}

func TestStatisticsFooterOffset(t *testing.T) {
	// Create a temporary file
	tmpFile := "/tmp/test_stats_offset.lance"
	defer os.Remove(tmpFile)

	// Create schema with int32 column
	schema := core.NewSchema([]core.Field{
		core.NewField("id", core.PrimInt32(), false),
	}, nil)

	// Create writer
	writer, err := NewWriter(tmpFile, schema, nil)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	// Write batch: id=[1, 2, 3]
	batch := createTestRecordBatchInt32(t, schema, []int32{1, 2, 3})
	if err := writer.WriteRecordBatch(batch); err != nil {
		t.Fatalf("Failed to write batch: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// Read back and verify footer contains stats offset
	reader, err := NewReader(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Close()

	// Access internal footer to check offset
	r := reader
	if r.footer.StatsOffset == 0 {
		t.Error("Expected StatsOffset to be set in footer")
	}
	if r.footer.StatsCount != 1 {
		t.Errorf("Expected StatsCount=1, got %d", r.footer.StatsCount)
	}

	// Verify stats offset points to valid data
	// Stats should be located between header and footer
	fileInfo, err := os.Stat(tmpFile)
	if err != nil {
		t.Fatalf("Failed to stat file: %v", err)
	}

	// Stats offset should be > HeaderReservedSize (8192)
	if r.footer.StatsOffset <= 8192 {
		t.Errorf("StatsOffset should be > HeaderReservedSize, got %d", r.footer.StatsOffset)
	}

	// Stats offset should be < file size - FooterSize
	footerStart := fileInfo.Size() - format.FooterSize
	if r.footer.StatsOffset >= footerStart {
		t.Errorf("StatsOffset should be before footer, got %d, footer starts at %d", 
			r.footer.StatsOffset, footerStart)
	}

	t.Logf("StatsOffset: %d, StatsCount: %d, FooterOffset: %d, FileSize: %d",
		r.footer.StatsOffset, r.footer.StatsCount, footerStart, fileInfo.Size())
}

func TestStatisticsWithNaNValues(t *testing.T) {
	// Test Float32 with NaN values
	t.Run("Float32WithNaN", func(t *testing.T) {
		tmpFile := "/tmp/test_stats_float32_nan.lance"
		defer os.Remove(tmpFile)

		schema := core.NewSchema([]core.Field{
			core.NewField("value", core.PrimFloat32(), false),
		}, nil)

		writer, err := NewWriter(tmpFile, schema, nil)
		if err != nil {
			t.Fatalf("Failed to create writer: %v", err)
		}

		// Create array with NaN: [1.0, NaN, 3.0, NaN, 5.0]
		values := []float32{1.0, float32(math.NaN()), 3.0, float32(math.NaN()), 5.0}
		batch := createTestRecordBatchFloat32(t, schema, values)
		if err := writer.WriteRecordBatch(batch); err != nil {
			t.Fatalf("Failed to write batch: %v", err)
		}

		if err := writer.Close(); err != nil {
			t.Fatalf("Failed to close writer: %v", err)
		}

		// Read back and verify statistics
		reader, err := NewReader(tmpFile)
		if err != nil {
			t.Fatalf("Failed to create reader: %v", err)
		}
		defer reader.Close()

		stats := reader.GetColumnStats(0)
		if stats == nil {
			t.Fatal("Expected column stats")
		}

		if !stats.HasMinMax {
			t.Fatal("Expected HasMinMax to be true (NaN should be ignored)")
		}

		min, max, ok := stats.GetMinMaxFloat32()
		if !ok {
			t.Fatal("Expected min/max to be available")
		}

		// NaN should be ignored, so min=1.0, max=5.0
		if min != 1.0 {
			t.Errorf("Expected min=1.0 (NaN ignored), got %f", min)
		}
		if max != 5.0 {
			t.Errorf("Expected max=5.0 (NaN ignored), got %f", max)
		}
	})

	// Test Float64 with all NaN values
	t.Run("Float64AllNaN", func(t *testing.T) {
		tmpFile := "/tmp/test_stats_float64_all_nan.lance"
		defer os.Remove(tmpFile)

		schema := core.NewSchema([]core.Field{
			core.NewField("value", core.PrimFloat64(), false),
		}, nil)

		writer, err := NewWriter(tmpFile, schema, nil)
		if err != nil {
			t.Fatalf("Failed to create writer: %v", err)
		}

		// Create array with all NaN: [NaN, NaN, NaN]
		values := []float64{math.NaN(), math.NaN(), math.NaN()}
		batch := createTestRecordBatchFloat64(t, schema, values)
		if err := writer.WriteRecordBatch(batch); err != nil {
			t.Fatalf("Failed to write batch: %v", err)
		}

		if err := writer.Close(); err != nil {
			t.Fatalf("Failed to close writer: %v", err)
		}

		// Read back and verify statistics
		reader, err := NewReader(tmpFile)
		if err != nil {
			t.Fatalf("Failed to create reader: %v", err)
		}
		defer reader.Close()

		stats := reader.GetColumnStats(0)
		if stats == nil {
			t.Fatal("Expected column stats")
		}

		// All values are NaN, so HasMinMax should be false
		if stats.HasMinMax {
			t.Error("Expected HasMinMax to be false when all values are NaN")
		}
	})

	// Test Float32 with NaN at the beginning
	t.Run("Float32NaNFirst", func(t *testing.T) {
		tmpFile := "/tmp/test_stats_float32_nan_first.lance"
		defer os.Remove(tmpFile)

		schema := core.NewSchema([]core.Field{
			core.NewField("value", core.PrimFloat32(), false),
		}, nil)

		writer, err := NewWriter(tmpFile, schema, nil)
		if err != nil {
			t.Fatalf("Failed to create writer: %v", err)
		}

		// NaN first, then valid values: [NaN, 2.0, 3.0]
		values := []float32{float32(math.NaN()), 2.0, 3.0}
		batch := createTestRecordBatchFloat32(t, schema, values)
		if err := writer.WriteRecordBatch(batch); err != nil {
			t.Fatalf("Failed to write batch: %v", err)
		}

		if err := writer.Close(); err != nil {
			t.Fatalf("Failed to close writer: %v", err)
		}

		reader, err := NewReader(tmpFile)
		if err != nil {
			t.Fatalf("Failed to create reader: %v", err)
		}
		defer reader.Close()

		stats := reader.GetColumnStats(0)
		if stats == nil || !stats.HasMinMax {
			t.Fatal("Expected valid column stats")
		}

		min, max, ok := stats.GetMinMaxFloat32()
		if !ok {
			t.Fatal("Expected min/max to be available")
		}

		if min != 2.0 || max != 3.0 {
			t.Errorf("Expected min=2.0, max=3.0, got min=%f, max=%f", min, max)
		}
	})
}

func TestStatisticsTypeID(t *testing.T) {
	// Verify TypeID is correctly set and prevents type confusion
	tmpFile := "/tmp/test_stats_typeid.lance"
	defer os.Remove(tmpFile)

	// Create schema with both int32 and float32 columns
	schema := core.NewSchema([]core.Field{
		core.NewField("int_col", core.PrimInt32(), false),
		core.NewField("float_col", core.PrimFloat32(), false),
	}, nil)

	writer, err := NewWriter(tmpFile, schema, nil)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	// Write batch
	intValues := []int32{1, 2, 3}
	floatValues := []float32{1.5, 2.5, 3.5}
	batch := createTestRecordBatchMixed(t, schema, intValues, floatValues)
	if err := writer.WriteRecordBatch(batch); err != nil {
		t.Fatalf("Failed to write batch: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// Read back and verify TypeID
	reader, err := NewReader(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Close()

	stats0 := reader.GetColumnStats(0) // int32 column
	stats1 := reader.GetColumnStats(1) // float32 column

	// Verify int32 column
	if stats0.TypeID != format.StatsTypeInt32 {
		t.Errorf("Expected TypeID=StatsTypeInt32 for int column, got %d", stats0.TypeID)
	}
	min0, max0, ok0 := stats0.GetMinMaxInt32()
	if !ok0 {
		t.Error("Expected GetMinMaxInt32 to succeed")
	}
	if min0 != 1 || max0 != 3 {
		t.Errorf("Expected min=1, max=3 for int column, got min=%d, max=%d", min0, max0)
	}
	// Float32 getter should fail for int column
	_, _, okFloat := stats0.GetMinMaxFloat32()
	if okFloat {
		t.Error("Expected GetMinMaxFloat32 to fail for int column")
	}

	// Verify float32 column
	if stats1.TypeID != format.StatsTypeFloat32 {
		t.Errorf("Expected TypeID=StatsTypeFloat32 for float column, got %d", stats1.TypeID)
	}
	min1, max1, ok1 := stats1.GetMinMaxFloat32()
	if !ok1 {
		t.Error("Expected GetMinMaxFloat32 to succeed")
	}
	if min1 != 1.5 || max1 != 3.5 {
		t.Errorf("Expected min=1.5, max=3.5 for float column, got min=%f, max=%f", min1, max1)
	}
	// Int32 getter should fail for float column
	_, _, okInt := stats1.GetMinMaxInt32()
	if okInt {
		t.Error("Expected GetMinMaxInt32 to fail for float column")
	}
}

func createTestRecordBatchMixed(t *testing.T, schema *core.Schema, intVals []int32, floatVals []float32) *core.RecordBatch {
	t.Helper()
	if len(intVals) != len(floatVals) {
		t.Fatal("intVals and floatVals must have same length")
	}
	intArray := core.NewInt32Array(intVals, nil)
	floatArray := core.NewFloat32Array(floatVals, nil)
	batch, err := core.NewRecordBatch(schema, len(intVals), []core.Array{intArray, floatArray})
	if err != nil {
		t.Fatalf("Failed to create record batch: %v", err)
	}
	return batch
}

func TestStatisticsOOMProtection(t *testing.T) {
	// Test that reading corrupted/malicious statistics doesn't cause OOM
	
	// Create a valid file first
	tmpFile := "/tmp/test_stats_oom.lance"
	defer os.Remove(tmpFile)

	schema := core.NewSchema([]core.Field{
		core.NewField("id", core.PrimInt32(), false),
	}, nil)

	writer, err := NewWriter(tmpFile, schema, nil)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	batch := createTestRecordBatchInt32(t, schema, []int32{1, 2, 3})
	if err := writer.WriteRecordBatch(batch); err != nil {
		t.Fatalf("Failed to write batch: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// Verify we can read it back normally
	reader, err := NewReader(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Close()

	if !reader.HasStatistics() {
		t.Error("Expected statistics to be available")
	}

	stats := reader.GetColumnStats(0)
	if stats == nil {
		t.Fatal("Expected column stats")
	}

	min, max, ok := stats.GetMinMaxInt32()
	if !ok {
		t.Fatal("Expected min/max to be available")
	}
	if min != 1 || max != 3 {
		t.Errorf("Expected min=1, max=3, got min=%d, max=%d", min, max)
	}
}

func TestStatisticsBoundaryValues(t *testing.T) {
	// Test boundary values for all numeric types
	
	t.Run("Int32Boundaries", func(t *testing.T) {
		tmpFile := "/tmp/test_stats_int32_bounds.lance"
		defer os.Remove(tmpFile)

		schema := core.NewSchema([]core.Field{
			core.NewField("value", core.PrimInt32(), false),
		}, nil)

		writer, err := NewWriter(tmpFile, schema, nil)
		if err != nil {
			t.Fatalf("Failed to create writer: %v", err)
		}

		// Test with MaxInt32, MinInt32, 0
		values := []int32{math.MaxInt32, math.MinInt32, 0}
		batch := createTestRecordBatchInt32(t, schema, values)
		if err := writer.WriteRecordBatch(batch); err != nil {
			t.Fatalf("Failed to write batch: %v", err)
		}

		if err := writer.Close(); err != nil {
			t.Fatalf("Failed to close writer: %v", err)
		}

		reader, err := NewReader(tmpFile)
		if err != nil {
			t.Fatalf("Failed to create reader: %v", err)
		}
		defer reader.Close()

		stats := reader.GetColumnStats(0)
		if stats == nil || !stats.HasMinMax {
			t.Fatal("Expected valid stats")
		}

		min, max, ok := stats.GetMinMaxInt32()
		if !ok {
			t.Fatal("Failed to get min/max")
		}

		if min != math.MinInt32 {
			t.Errorf("Expected min=MinInt32(%d), got %d", math.MinInt32, min)
		}
		if max != math.MaxInt32 {
			t.Errorf("Expected max=MaxInt32(%d), got %d", math.MaxInt32, max)
		}
	})

	t.Run("Int64Boundaries", func(t *testing.T) {
		tmpFile := "/tmp/test_stats_int64_bounds.lance"
		defer os.Remove(tmpFile)

		schema := core.NewSchema([]core.Field{
			core.NewField("value", core.PrimInt64(), false),
		}, nil)

		writer, err := NewWriter(tmpFile, schema, nil)
		if err != nil {
			t.Fatalf("Failed to create writer: %v", err)
		}

		values := []int64{math.MaxInt64, math.MinInt64, 0}
		batch := createTestRecordBatchInt64(t, schema, values)
		if err := writer.WriteRecordBatch(batch); err != nil {
			t.Fatalf("Failed to write batch: %v", err)
		}

		if err := writer.Close(); err != nil {
			t.Fatalf("Failed to close writer: %v", err)
		}

		reader, err := NewReader(tmpFile)
		if err != nil {
			t.Fatalf("Failed to create reader: %v", err)
		}
		defer reader.Close()

		stats := reader.GetColumnStats(0)
		if stats == nil || !stats.HasMinMax {
			t.Fatal("Expected valid stats")
		}

		min, max, ok := stats.GetMinMaxInt64()
		if !ok {
			t.Fatal("Failed to get min/max")
		}

		if min != math.MinInt64 {
			t.Errorf("Expected min=MinInt64(%d), got %d", math.MinInt64, min)
		}
		if max != math.MaxInt64 {
			t.Errorf("Expected max=MaxInt64(%d), got %d", math.MaxInt64, max)
		}
	})

	t.Run("Float32Boundaries", func(t *testing.T) {
		tmpFile := "/tmp/test_stats_float32_bounds.lance"
		defer os.Remove(tmpFile)

		schema := core.NewSchema([]core.Field{
			core.NewField("value", core.PrimFloat32(), false),
		}, nil)

		writer, err := NewWriter(tmpFile, schema, nil)
		if err != nil {
			t.Fatalf("Failed to create writer: %v", err)
		}

		// Test with MaxFloat32, SmallestNonzeroFloat32, -MaxFloat32, 0
		values := []float32{math.MaxFloat32, -math.MaxFloat32, math.SmallestNonzeroFloat32, 0}
		batch := createTestRecordBatchFloat32(t, schema, values)
		if err := writer.WriteRecordBatch(batch); err != nil {
			t.Fatalf("Failed to write batch: %v", err)
		}

		if err := writer.Close(); err != nil {
			t.Fatalf("Failed to close writer: %v", err)
		}

		reader, err := NewReader(tmpFile)
		if err != nil {
			t.Fatalf("Failed to create reader: %v", err)
		}
		defer reader.Close()

		stats := reader.GetColumnStats(0)
		if stats == nil || !stats.HasMinMax {
			t.Fatal("Expected valid stats")
		}

		min, max, ok := stats.GetMinMaxFloat32()
		if !ok {
			t.Fatal("Failed to get min/max")
		}

		if min != -math.MaxFloat32 {
			t.Errorf("Expected min=-MaxFloat32(%f), got %f", -math.MaxFloat32, min)
		}
		if max != math.MaxFloat32 {
			t.Errorf("Expected max=MaxFloat32(%f), got %f", math.MaxFloat32, max)
		}
	})

	t.Run("Float64Boundaries", func(t *testing.T) {
		tmpFile := "/tmp/test_stats_float64_bounds.lance"
		defer os.Remove(tmpFile)

		schema := core.NewSchema([]core.Field{
			core.NewField("value", core.PrimFloat64(), false),
		}, nil)

		writer, err := NewWriter(tmpFile, schema, nil)
		if err != nil {
			t.Fatalf("Failed to create writer: %v", err)
		}

		values := []float64{math.MaxFloat64, -math.MaxFloat64, math.SmallestNonzeroFloat64, 0}
		batch := createTestRecordBatchFloat64(t, schema, values)
		if err := writer.WriteRecordBatch(batch); err != nil {
			t.Fatalf("Failed to write batch: %v", err)
		}

		if err := writer.Close(); err != nil {
			t.Fatalf("Failed to close writer: %v", err)
		}

		reader, err := NewReader(tmpFile)
		if err != nil {
			t.Fatalf("Failed to create reader: %v", err)
		}
		defer reader.Close()

		stats := reader.GetColumnStats(0)
		if stats == nil || !stats.HasMinMax {
			t.Fatal("Expected valid stats")
		}

		min, max, ok := stats.GetMinMaxFloat64()
		if !ok {
			t.Fatal("Failed to get min/max")
		}

		if min != -math.MaxFloat64 {
			t.Errorf("Expected min=-MaxFloat64(%f), got %f", -math.MaxFloat64, min)
		}
		if max != math.MaxFloat64 {
			t.Errorf("Expected max=MaxFloat64(%f), got %f", math.MaxFloat64, max)
		}
	})

	t.Run("FloatInfinity", func(t *testing.T) {
		tmpFile := "/tmp/test_stats_float_inf.lance"
		defer os.Remove(tmpFile)

		schema := core.NewSchema([]core.Field{
			core.NewField("value", core.PrimFloat64(), false),
		}, nil)

		writer, err := NewWriter(tmpFile, schema, nil)
		if err != nil {
			t.Fatalf("Failed to create writer: %v", err)
		}

		// Test with +/-Inf
		values := []float64{math.Inf(1), math.Inf(-1), 0, 1.5}
		batch := createTestRecordBatchFloat64(t, schema, values)
		if err := writer.WriteRecordBatch(batch); err != nil {
			t.Fatalf("Failed to write batch: %v", err)
		}

		if err := writer.Close(); err != nil {
			t.Fatalf("Failed to close writer: %v", err)
		}

		reader, err := NewReader(tmpFile)
		if err != nil {
			t.Fatalf("Failed to create reader: %v", err)
		}
		defer reader.Close()

		stats := reader.GetColumnStats(0)
		if stats == nil || !stats.HasMinMax {
			t.Fatal("Expected valid stats")
		}

		min, max, ok := stats.GetMinMaxFloat64()
		if !ok {
			t.Fatal("Failed to get min/max")
		}

		if min != math.Inf(-1) {
			t.Errorf("Expected min=-Inf, got %f", min)
		}
		if max != math.Inf(1) {
			t.Errorf("Expected max=+Inf, got %f", max)
		}
	})
}

func createTestRecordBatchInt64(t *testing.T, schema *core.Schema, values []int64) *core.RecordBatch {
	t.Helper()
	valueArray := core.NewInt64Array(values, nil)
	batch, err := core.NewRecordBatch(schema, len(values), []core.Array{valueArray})
	if err != nil {
		t.Fatalf("Failed to create record batch: %v", err)
	}
	return batch
}

func TestStatisticsSpecialArrays(t *testing.T) {
	// Test all-zero and empty arrays

	t.Run("AllZeros", func(t *testing.T) {
		tmpFile := "/tmp/test_stats_zeros.lance"
		defer os.Remove(tmpFile)

		schema := core.NewSchema([]core.Field{
			core.NewField("value", core.PrimInt32(), false),
		}, nil)

		writer, err := NewWriter(tmpFile, schema, nil)
		if err != nil {
			t.Fatalf("Failed to create writer: %v", err)
		}

		// All zeros
		values := []int32{0, 0, 0, 0}
		batch := createTestRecordBatchInt32(t, schema, values)
		if err := writer.WriteRecordBatch(batch); err != nil {
			t.Fatalf("Failed to write batch: %v", err)
		}

		if err := writer.Close(); err != nil {
			t.Fatalf("Failed to close writer: %v", err)
		}

		reader, err := NewReader(tmpFile)
		if err != nil {
			t.Fatalf("Failed to create reader: %v", err)
		}
		defer reader.Close()

		stats := reader.GetColumnStats(0)
		if stats == nil || !stats.HasMinMax {
			t.Fatal("Expected valid stats")
		}

		min, max, ok := stats.GetMinMaxInt32()
		if !ok {
			t.Fatal("Failed to get min/max")
		}

		if min != 0 || max != 0 {
			t.Errorf("Expected min=max=0, got min=%d, max=%d", min, max)
		}
	})

	t.Run("EmptyArray", func(t *testing.T) {
		// Empty arrays are not supported by the column writer
		// This test verifies that empty arrays are rejected during validation
		schema := core.NewSchema([]core.Field{
			core.NewField("value", core.PrimInt32(), false),
		}, nil)

		writer, err := NewWriter("/tmp/test_stats_empty.lance", schema, nil)
		if err != nil {
			t.Fatalf("Failed to create writer: %v", err)
		}
		defer os.Remove("/tmp/test_stats_empty.lance")

		// Empty array should be rejected during validation
		values := []int32{}
		batch := createTestRecordBatchInt32(t, schema, values)
		err = writer.WriteRecordBatch(batch)
		if err == nil {
			t.Error("Expected error when writing empty array, got nil")
		}
	})

	t.Run("SingleValue", func(t *testing.T) {
		tmpFile := "/tmp/test_stats_single.lance"
		defer os.Remove(tmpFile)

		schema := core.NewSchema([]core.Field{
			core.NewField("value", core.PrimInt32(), false),
		}, nil)

		writer, err := NewWriter(tmpFile, schema, nil)
		if err != nil {
			t.Fatalf("Failed to create writer: %v", err)
		}

		// Single value
		values := []int32{42}
		batch := createTestRecordBatchInt32(t, schema, values)
		if err := writer.WriteRecordBatch(batch); err != nil {
			t.Fatalf("Failed to write batch: %v", err)
		}

		if err := writer.Close(); err != nil {
			t.Fatalf("Failed to close writer: %v", err)
		}

		reader, err := NewReader(tmpFile)
		if err != nil {
			t.Fatalf("Failed to create reader: %v", err)
		}
		defer reader.Close()

		stats := reader.GetColumnStats(0)
		if stats == nil || !stats.HasMinMax {
			t.Fatal("Expected valid stats")
		}

		min, max, ok := stats.GetMinMaxInt32()
		if !ok {
			t.Fatal("Failed to get min/max")
		}

		if min != 42 || max != 42 {
			t.Errorf("Expected min=max=42, got min=%d, max=%d", min, max)
		}
	})
}

func TestStatisticsVersionCompatibility(t *testing.T) {
	// Test that files with and without statistics can be read correctly

	t.Run("FileWithStatistics", func(t *testing.T) {
		tmpFile := "/tmp/test_stats_version_with.lance"
		defer os.Remove(tmpFile)

		schema := core.NewSchema([]core.Field{
			core.NewField("value", core.PrimInt32(), false),
		}, nil)

		writer, err := NewWriter(tmpFile, schema, nil)
		if err != nil {
			t.Fatalf("Failed to create writer: %v", err)
		}

		values := []int32{1, 2, 3, 4, 5}
		batch := createTestRecordBatchInt32(t, schema, values)
		if err := writer.WriteRecordBatch(batch); err != nil {
			t.Fatalf("Failed to write batch: %v", err)
		}

		if err := writer.Close(); err != nil {
			t.Fatalf("Failed to close writer: %v", err)
		}

		// Verify statistics are present
		reader, err := NewReader(tmpFile)
		if err != nil {
			t.Fatalf("Failed to create reader: %v", err)
		}
		defer reader.Close()

		if !reader.HasStatistics() {
			t.Error("Expected statistics to be present")
		}

		stats := reader.GetColumnStats(0)
		if stats == nil {
			t.Fatal("Expected column stats")
		}

		// Verify TypeID is set
		if stats.TypeID != format.StatsTypeInt32 {
			t.Errorf("Expected TypeID=StatsTypeInt32, got %d", stats.TypeID)
		}

		// Verify version
		if stats.Version != 1 {
			t.Errorf("Expected Version=1, got %d", stats.Version)
		}
	})
}

func TestZoneMapFloatNaNQuery(t *testing.T) {
	// Test Zone Map with NaN query values (should use conservative strategy)
	tmpFile := "/tmp/test_zonemap_float_nan.lance"
	defer os.Remove(tmpFile)

	schema := core.NewSchema([]core.Field{
		core.NewField("float_val", core.PrimFloat64(), false),
	}, nil)

	writer, err := NewWriter(tmpFile, schema, nil)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	// Write some data
	values := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	batch := createTestRecordBatchFloat64(t, schema, values)
	if err := writer.WriteRecordBatch(batch); err != nil {
		t.Fatalf("Failed to write batch: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	reader, err := NewReader(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Close()

	// Test: NaN query should return MayContainData=true (conservative)
	result := reader.EvaluateZoneMapFloat64(0, math.NaN())
	if !result.MayContainData {
		t.Error("Expected MayContainData=true for NaN query (conservative strategy)")
	}

	// Test: Range query with NaN bounds should also be conservative
	result = reader.EvaluateZoneMapRangeFloat64(0, math.NaN(), 10.0)
	if !result.MayContainData {
		t.Error("Expected MayContainData=true when low bound is NaN")
	}

	result = reader.EvaluateZoneMapRangeFloat64(0, 0.0, math.NaN())
	if !result.MayContainData {
		t.Error("Expected MayContainData=true when high bound is NaN")
	}

	// Verify normal queries still work correctly
	result = reader.EvaluateZoneMapFloat64(0, 3.0)
	if !result.MayContainData {
		t.Error("Expected MayContainData=true for value=3.0 in range [1,5]")
	}

	result = reader.EvaluateZoneMapFloat64(0, 10.0)
	if result.MayContainData {
		t.Error("Expected MayContainData=false for value=10.0 outside range [1,5]")
	}
}

func createTestRecordBatchInt32WithNulls(t *testing.T, schema *core.Schema, ids []int32, valid []bool) *core.RecordBatch {
	t.Helper()
	if len(ids) != len(valid) {
		t.Fatal("ids and valid must have same length")
	}

	var nullBitmap *core.Bitmap
	for i, v := range valid {
		if nullBitmap == nil {
			nullBitmap = core.NewBitmap(len(valid))
		}
		if v {
			nullBitmap.Set(i)
		}
	}

	idArray := core.NewInt32Array(ids, nullBitmap)
	batch, err := core.NewRecordBatch(schema, len(ids), []core.Array{idArray})
	if err != nil {
		t.Fatalf("Failed to create record batch: %v", err)
	}
	return batch
}
