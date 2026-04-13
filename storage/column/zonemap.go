// Copyright 2024 Vego Authors
// Licensed under the Apache License, Version 2.0

package column

import (
	"math"

	"github.com/wzqhbustb/vego/storage/format"
)

// ZoneMapEvaluator provides column statistics-based predicate pushdown evaluation.
// It uses Min/Max statistics to determine if a file may contain data matching a query.
type ZoneMapEvaluator struct {
	stats *format.StatisticsList
}

// NewZoneMapEvaluator creates a new ZoneMapEvaluator from column statistics.
func NewZoneMapEvaluator(stats *format.StatisticsList) *ZoneMapEvaluator {
	return &ZoneMapEvaluator{stats: stats}
}

// HasStatistics returns true if the evaluator has valid column statistics.
func (z *ZoneMapEvaluator) HasStatistics() bool {
	return z.stats != nil && z.stats.NumColumns > 0
}

// GetColumnStats returns statistics for a specific column.
func (z *ZoneMapEvaluator) GetColumnStats(columnIndex int32) *format.ColumnStatistics {
	if z.stats == nil {
		return nil
	}
	return z.stats.GetColumnStats(columnIndex)
}

// ZoneMapFilterResult is the result of applying Zone Map filtering.
type ZoneMapFilterResult struct {
	MayContainData bool // true if data may satisfy the predicate, false if definitely not
}

// EvaluateZoneMapInt32 checks if a column's min/max range overlaps with a predicate value.
// For equality: returns true if min <= value <= max.
func (z *ZoneMapEvaluator) EvaluateZoneMapInt32(columnIndex int32, value int32) ZoneMapFilterResult {
	stats := z.GetColumnStats(columnIndex)
	if stats == nil || !stats.HasMinMax {
		return ZoneMapFilterResult{MayContainData: true}
	}

	min, max, ok := stats.GetMinMaxInt32()
	if !ok {
		return ZoneMapFilterResult{MayContainData: true}
	}

	return ZoneMapFilterResult{MayContainData: value >= min && value <= max}
}

// EvaluateZoneMapInt64 checks if a column's min/max range overlaps with a predicate value.
func (z *ZoneMapEvaluator) EvaluateZoneMapInt64(columnIndex int32, value int64) ZoneMapFilterResult {
	stats := z.GetColumnStats(columnIndex)
	if stats == nil || !stats.HasMinMax {
		return ZoneMapFilterResult{MayContainData: true}
	}

	min, max, ok := stats.GetMinMaxInt64()
	if !ok {
		return ZoneMapFilterResult{MayContainData: true}
	}

	return ZoneMapFilterResult{MayContainData: value >= min && value <= max}
}

// EvaluateZoneMapFloat32 checks if a column's min/max range overlaps with a predicate value.
// Uses conservative strategy for NaN values.
func (z *ZoneMapEvaluator) EvaluateZoneMapFloat32(columnIndex int32, value float32) ZoneMapFilterResult {
	// NaN query value: use conservative strategy (may contain data)
	if math.IsNaN(float64(value)) {
		return ZoneMapFilterResult{MayContainData: true}
	}

	stats := z.GetColumnStats(columnIndex)
	if stats == nil || !stats.HasMinMax {
		return ZoneMapFilterResult{MayContainData: true}
	}

	min, max, ok := stats.GetMinMaxFloat32()
	if !ok {
		return ZoneMapFilterResult{MayContainData: true}
	}

	// Defensive: if stored min/max is NaN (data corruption), use conservative strategy
	if math.IsNaN(float64(min)) || math.IsNaN(float64(max)) {
		return ZoneMapFilterResult{MayContainData: true}
	}

	return ZoneMapFilterResult{MayContainData: value >= min && value <= max}
}

// EvaluateZoneMapFloat64 checks if a column's min/max range overlaps with a predicate value.
func (z *ZoneMapEvaluator) EvaluateZoneMapFloat64(columnIndex int32, value float64) ZoneMapFilterResult {
	// NaN query value: use conservative strategy (may contain data)
	if math.IsNaN(value) {
		return ZoneMapFilterResult{MayContainData: true}
	}

	stats := z.GetColumnStats(columnIndex)
	if stats == nil || !stats.HasMinMax {
		return ZoneMapFilterResult{MayContainData: true}
	}

	min, max, ok := stats.GetMinMaxFloat64()
	if !ok {
		return ZoneMapFilterResult{MayContainData: true}
	}

	// Defensive: if stored min/max is NaN (data corruption), use conservative strategy
	if math.IsNaN(min) || math.IsNaN(max) {
		return ZoneMapFilterResult{MayContainData: true}
	}

	return ZoneMapFilterResult{MayContainData: value >= min && value <= max}
}

// EvaluateZoneMapRangeInt32 checks if a column's range overlaps with a query range [low, high].
// Used for range queries like "WHERE col BETWEEN low AND high".
func (z *ZoneMapEvaluator) EvaluateZoneMapRangeInt32(columnIndex int32, low, high int32) ZoneMapFilterResult {
	stats := z.GetColumnStats(columnIndex)
	if stats == nil || !stats.HasMinMax {
		return ZoneMapFilterResult{MayContainData: true}
	}

	min, max, ok := stats.GetMinMaxInt32()
	if !ok {
		return ZoneMapFilterResult{MayContainData: true}
	}

	// Range overlap: [min, max] and [low, high] overlap if not disjoint
	// Disjoint if max < low OR min > high
	return ZoneMapFilterResult{MayContainData: !(max < low || min > high)}
}

// EvaluateZoneMapRangeInt64 checks if a column's range overlaps with a query range.
func (z *ZoneMapEvaluator) EvaluateZoneMapRangeInt64(columnIndex int32, low, high int64) ZoneMapFilterResult {
	stats := z.GetColumnStats(columnIndex)
	if stats == nil || !stats.HasMinMax {
		return ZoneMapFilterResult{MayContainData: true}
	}

	min, max, ok := stats.GetMinMaxInt64()
	if !ok {
		return ZoneMapFilterResult{MayContainData: true}
	}

	return ZoneMapFilterResult{MayContainData: !(max < low || min > high)}
}

// EvaluateZoneMapRangeFloat32 checks if a column's range overlaps with a query range.
func (z *ZoneMapEvaluator) EvaluateZoneMapRangeFloat32(columnIndex int32, low, high float32) ZoneMapFilterResult {
	// NaN in range bounds: use conservative strategy
	if math.IsNaN(float64(low)) || math.IsNaN(float64(high)) {
		return ZoneMapFilterResult{MayContainData: true}
	}

	stats := z.GetColumnStats(columnIndex)
	if stats == nil || !stats.HasMinMax {
		return ZoneMapFilterResult{MayContainData: true}
	}

	min, max, ok := stats.GetMinMaxFloat32()
	if !ok {
		return ZoneMapFilterResult{MayContainData: true}
	}

	// Defensive: if stored min/max is NaN (data corruption), use conservative strategy
	if math.IsNaN(float64(min)) || math.IsNaN(float64(max)) {
		return ZoneMapFilterResult{MayContainData: true}
	}

	return ZoneMapFilterResult{MayContainData: !(max < low || min > high)}
}

// EvaluateZoneMapRangeFloat64 checks if a column's range overlaps with a query range.
func (z *ZoneMapEvaluator) EvaluateZoneMapRangeFloat64(columnIndex int32, low, high float64) ZoneMapFilterResult {
	// NaN in range bounds: use conservative strategy
	if math.IsNaN(low) || math.IsNaN(high) {
		return ZoneMapFilterResult{MayContainData: true}
	}

	stats := z.GetColumnStats(columnIndex)
	if stats == nil || !stats.HasMinMax {
		return ZoneMapFilterResult{MayContainData: true}
	}

	min, max, ok := stats.GetMinMaxFloat64()
	if !ok {
		return ZoneMapFilterResult{MayContainData: true}
	}

	// Defensive: if stored min/max is NaN (data corruption), use conservative strategy
	if math.IsNaN(min) || math.IsNaN(max) {
		return ZoneMapFilterResult{MayContainData: true}
	}

	return ZoneMapFilterResult{MayContainData: !(max < low || min > high)}
}
