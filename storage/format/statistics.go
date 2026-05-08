// Copyright 2024 Vego Authors
// Licensed under the Apache License, Version 2.0

package format

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"

	lerrors "github.com/wzqhbustb/vego/storage/errors"
	"github.com/wzqhbustb/vego/core"
)

// MaxStatsValueSize is the maximum size for Min/Max values to prevent OOM attacks.
// For numeric types, 8 bytes is enough. For string/binary types, 1KB should be sufficient
// for statistics purposes (e.g., storing a sample of min/max strings).
const MaxStatsValueSize = 1024

// StatsTypeID identifies the data type of column statistics.
// This is used to correctly interpret Min/Max values during merge operations.
type StatsTypeID uint8

const (
	StatsTypeUnknown StatsTypeID = iota
	StatsTypeInt32
	StatsTypeInt64
	StatsTypeFloat32
	StatsTypeFloat64
	StatsTypeString  // Future use
	StatsTypeBinary  // Future use
)

// ColumnStatistics stores Min/Max and other stats for a column.
// Reference: Lance Manifest column statistics design.
type ColumnStatistics struct {
	Version     uint16  // Format version for future extensions
	ColumnIndex int32   // Column index
	NullCount   int64   // Number of null values
	
	// TypeID identifies the data type for correct Min/Max interpretation
	// Added in version 1 to prevent type confusion during merge
	TypeID StatsTypeID
	
	// Min/Max values stored as raw bytes (type-specific serialization)
	// For numeric types: fixed-size little-endian
	// For string/binary: length-prefixed bytes
	HasMinMax bool   // Whether Min/Max are valid (e.g., all-null column)
	MinValue  []byte 
	MaxValue  []byte
	
	// Optional: distinct count for cardinality estimation
	HasDistinctCount bool
	DistinctCount    uint64
}

// StatisticsList holds statistics for all columns in a file.
type StatisticsList struct {
	Version    uint16             // List format version
	NumColumns uint32             // Number of column statistics
	Stats      []ColumnStatistics // Column statistics array
}

// NewStatisticsList creates a new statistics list for given number of columns.
func NewStatisticsList(numColumns int) *StatisticsList {
	sl := &StatisticsList{
		Version:    1, // Current version
		NumColumns: uint32(numColumns),
		Stats:      make([]ColumnStatistics, numColumns),
	}
	// Initialize each column's index and version
	for i := range sl.Stats {
		sl.Stats[i].ColumnIndex = int32(i)
		sl.Stats[i].Version = 1
	}
	return sl
}

// GetColumnStats returns statistics for a specific column.
func (sl *StatisticsList) GetColumnStats(columnIndex int32) *ColumnStatistics {
	if sl == nil || columnIndex < 0 || int(columnIndex) >= len(sl.Stats) {
		return nil
	}
	return &sl.Stats[columnIndex]
}

// WriteTo serializes the statistics list to a writer.
// Includes CRC32 checksum for data integrity verification.
func (sl *StatisticsList) WriteTo(w io.Writer) (int64, error) {
	buf := new(bytes.Buffer)
	
	// Write header
	if err := binary.Write(buf, binary.LittleEndian, sl.Version); err != nil {
		return 0, lerrors.IO("write_stats_version", "", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, sl.NumColumns); err != nil {
		return 0, lerrors.IO("write_stats_num_columns", "", err)
	}
	
	// Write each column statistics
	for i := range sl.Stats {
		stats := &sl.Stats[i]
		if err := binary.Write(buf, binary.LittleEndian, stats.Version); err != nil {
			return 0, lerrors.IO("write_stats_col_version", "", err)
		}
		if err := binary.Write(buf, binary.LittleEndian, stats.ColumnIndex); err != nil {
			return 0, lerrors.IO("write_stats_col_index", "", err)
		}
		if err := binary.Write(buf, binary.LittleEndian, stats.NullCount); err != nil {
			return 0, lerrors.IO("write_stats_null_count", "", err)
		}
		if err := binary.Write(buf, binary.LittleEndian, stats.TypeID); err != nil {
			return 0, lerrors.IO("write_stats_type_id", "", err)
		}
		if err := binary.Write(buf, binary.LittleEndian, stats.HasMinMax); err != nil {
			return 0, lerrors.IO("write_stats_has_minmax", "", err)
		}
		
		if stats.HasMinMax {
			// Write Min value (length-prefixed)
			if err := binary.Write(buf, binary.LittleEndian, uint32(len(stats.MinValue))); err != nil {
				return 0, lerrors.IO("write_stats_min_len", "", err)
			}
			if _, err := buf.Write(stats.MinValue); err != nil {
				return 0, lerrors.IO("write_stats_min_value", "", err)
			}
			
			// Write Max value (length-prefixed)
			if err := binary.Write(buf, binary.LittleEndian, uint32(len(stats.MaxValue))); err != nil {
				return 0, lerrors.IO("write_stats_max_len", "", err)
			}
			if _, err := buf.Write(stats.MaxValue); err != nil {
				return 0, lerrors.IO("write_stats_max_value", "", err)
			}
		}
		
		// Write distinct count if available
		if err := binary.Write(buf, binary.LittleEndian, stats.HasDistinctCount); err != nil {
			return 0, lerrors.IO("write_stats_has_distinct", "", err)
		}
		if stats.HasDistinctCount {
			if err := binary.Write(buf, binary.LittleEndian, stats.DistinctCount); err != nil {
				return 0, lerrors.IO("write_stats_distinct_count", "", err)
			}
		}
	}
	
	// Calculate and write CRC32 checksum
	data := buf.Bytes()
	checksum := crc32.ChecksumIEEE(data)
	if err := binary.Write(buf, binary.LittleEndian, checksum); err != nil {
		return 0, lerrors.IO("write_stats_checksum", "", err)
	}
	
	n, err := w.Write(buf.Bytes())
	return int64(n), err
}

// trackingReader wraps an io.Reader and records all bytes read for CRC verification.
type trackingReader struct {
	r      io.Reader
	buf    []byte
	total  int64
}

func (tr *trackingReader) Read(p []byte) (int, error) {
	n, err := tr.r.Read(p)
	if n > 0 {
		tr.buf = append(tr.buf, p[:n]...)
		tr.total += int64(n)
	}
	return n, err
}

func (tr *trackingReader) ReadFull(p []byte) error {
	_, err := io.ReadFull(tr.r, p)
	if err == nil {
		tr.buf = append(tr.buf, p...)
		tr.total += int64(len(p))
	}
	return err
}

// ReadFrom deserializes the statistics list from a reader.
// Verifies CRC32 checksum for data integrity.
func (sl *StatisticsList) ReadFrom(r io.Reader) (int64, error) {
	// Use tracking reader to record all data for CRC verification
	tr := &trackingReader{r: r}
	
	// Read header
	headerBuf := make([]byte, 6) // Version(2) + NumColumns(4)
	if err := tr.ReadFull(headerBuf); err != nil {
		return tr.total, err
	}
	
	sl.Version = binary.LittleEndian.Uint16(headerBuf[0:2])
	sl.NumColumns = binary.LittleEndian.Uint32(headerBuf[2:6])
	
	// Validate NumColumns to prevent OOM from malicious/corrupted data
	const MaxColumns = 10000 // Reasonable upper limit for column count
	if sl.NumColumns > MaxColumns {
		return tr.total, lerrors.New(lerrors.ErrCorruptedFile).
			Op("read_stats").
			Context("field", "num_columns").
			Context("value", sl.NumColumns).
			Context("max_allowed", MaxColumns).
			Context("message", "number of columns exceeds maximum allowed").
			Build()
	}
	
	sl.Stats = make([]ColumnStatistics, sl.NumColumns)
	
	// Read each column statistics
	for i := uint32(0); i < sl.NumColumns; i++ {
		stats := &sl.Stats[i]
		
		// Read fixed fields
		// Version(2) + ColumnIndex(4) + NullCount(8) + TypeID(1) + HasMinMax(1) = 16 bytes
		fixedBuf := make([]byte, 16)
		if err := tr.ReadFull(fixedBuf); err != nil {
			return tr.total, err
		}
		
		stats.Version = binary.LittleEndian.Uint16(fixedBuf[0:2])
		stats.ColumnIndex = int32(binary.LittleEndian.Uint32(fixedBuf[2:6]))
		stats.NullCount = int64(binary.LittleEndian.Uint64(fixedBuf[6:14]))
		stats.TypeID = StatsTypeID(fixedBuf[14])
		stats.HasMinMax = fixedBuf[15] != 0
		
		if stats.HasMinMax {
			// Read Min value length and data
			var minLen uint32
			if err := binary.Read(tr, binary.LittleEndian, &minLen); err != nil {
				return tr.total, err
			}
			
			// Validate minLen to prevent OOM
			if minLen > MaxStatsValueSize {
				return tr.total, lerrors.New(lerrors.ErrCorruptedFile).
					Op("read_stats").
					Context("field", "min_value_len").
					Context("value", minLen).
					Context("max_allowed", MaxStatsValueSize).
					Context("message", "min value size exceeds maximum allowed").
					Build()
			}
			
			stats.MinValue = make([]byte, minLen)
			if err := tr.ReadFull(stats.MinValue); err != nil {
				return tr.total, err
			}
			
			// Read Max value length and data
			var maxLen uint32
			if err := binary.Read(tr, binary.LittleEndian, &maxLen); err != nil {
				return tr.total, err
			}
			
			// Validate maxLen to prevent OOM
			if maxLen > MaxStatsValueSize {
				return tr.total, lerrors.New(lerrors.ErrCorruptedFile).
					Op("read_stats").
					Context("field", "max_value_len").
					Context("value", maxLen).
					Context("max_allowed", MaxStatsValueSize).
					Context("message", "max value size exceeds maximum allowed").
					Build()
			}
			
			stats.MaxValue = make([]byte, maxLen)
			if err := tr.ReadFull(stats.MaxValue); err != nil {
				return tr.total, err
			}
		}
		
		// Read distinct count flag
		var hasDistinct uint8
		if err := binary.Read(tr, binary.LittleEndian, &hasDistinct); err != nil {
			return tr.total, err
		}
		
		stats.HasDistinctCount = hasDistinct != 0
		if stats.HasDistinctCount {
			if err := binary.Read(tr, binary.LittleEndian, &stats.DistinctCount); err != nil {
				return tr.total, err
			}
		}
	}
	
	// Read and verify CRC32 checksum
	var storedChecksum uint32
	if err := binary.Read(r, binary.LittleEndian, &storedChecksum); err != nil {
		return tr.total, lerrors.IO("read_stats_checksum", "", err)
	}
	
	// Calculate CRC on all data read (excluding the checksum itself)
	computed := crc32.ChecksumIEEE(tr.buf)
	if computed != storedChecksum {
		return tr.total, lerrors.New(lerrors.ErrCorruptedFile).
			Op("read_stats").
			Context("field", "checksum").
			Context("computed", computed).
			Context("stored", storedChecksum).
			Context("message", "statistics checksum mismatch").
			Build()
	}
	
	return tr.total + 4, nil // +4 for checksum
}

// EncodedSize returns the serialized size in bytes.
func (sl *StatisticsList) EncodedSize() int64 {
	size := int64(6) // Header: Version(2) + NumColumns(4)
	
	for i := range sl.Stats {
		stats := &sl.Stats[i]
		size += 16 // Fixed fields: Version(2) + ColumnIndex(4) + NullCount(8) + TypeID(1) + HasMinMax(1)
		
		if stats.HasMinMax {
			size += 4 + int64(len(stats.MinValue))  // Length prefix + data
			size += 4 + int64(len(stats.MaxValue))
		}
		
		size += 1 // HasDistinctCount flag
		if stats.HasDistinctCount {
			size += 8
		}
	}
	
	return size
}

// Helper functions for reading
func readUint8(r io.Reader, v *uint8) (int, error) {
	buf := make([]byte, 1)
	n, err := io.ReadFull(r, buf)
	if err == nil {
		*v = buf[0]
	}
	return n, err
}

func readUint32(r io.Reader, v *uint32) (int, error) {
	buf := make([]byte, 4)
	n, err := io.ReadFull(r, buf)
	if err == nil {
		*v = binary.LittleEndian.Uint32(buf)
	}
	return n, err
}

func readUint64(r io.Reader, v *uint64) (int, error) {
	buf := make([]byte, 8)
	n, err := io.ReadFull(r, buf)
	if err == nil {
		*v = binary.LittleEndian.Uint64(buf)
	}
	return n, err
}

// SetMinMaxInt32 sets Min/Max for int32 column.
func (cs *ColumnStatistics) SetMinMaxInt32(min, max int32) {
	cs.HasMinMax = true
	cs.TypeID = StatsTypeInt32
	cs.MinValue = make([]byte, 4)
	cs.MaxValue = make([]byte, 4)
	binary.LittleEndian.PutUint32(cs.MinValue, uint32(min))
	binary.LittleEndian.PutUint32(cs.MaxValue, uint32(max))
}

// SetMinMaxInt64 sets Min/Max for int64 column.
func (cs *ColumnStatistics) SetMinMaxInt64(min, max int64) {
	cs.HasMinMax = true
	cs.TypeID = StatsTypeInt64
	cs.MinValue = make([]byte, 8)
	cs.MaxValue = make([]byte, 8)
	binary.LittleEndian.PutUint64(cs.MinValue, uint64(min))
	binary.LittleEndian.PutUint64(cs.MaxValue, uint64(max))
}

// SetMinMaxFloat32 sets Min/Max for float32 column.
func (cs *ColumnStatistics) SetMinMaxFloat32(min, max float32) {
	cs.HasMinMax = true
	cs.TypeID = StatsTypeFloat32
	cs.MinValue = make([]byte, 4)
	cs.MaxValue = make([]byte, 4)
	binary.LittleEndian.PutUint32(cs.MinValue, math.Float32bits(min))
	binary.LittleEndian.PutUint32(cs.MaxValue, math.Float32bits(max))
}

// SetMinMaxFloat64 sets Min/Max for float64 column.
func (cs *ColumnStatistics) SetMinMaxFloat64(min, max float64) {
	cs.HasMinMax = true
	cs.TypeID = StatsTypeFloat64
	cs.MinValue = make([]byte, 8)
	cs.MaxValue = make([]byte, 8)
	binary.LittleEndian.PutUint64(cs.MinValue, math.Float64bits(min))
	binary.LittleEndian.PutUint64(cs.MaxValue, math.Float64bits(max))
}

// GetMinMaxInt32 returns Min/Max for int32 column.
func (cs *ColumnStatistics) GetMinMaxInt32() (min, max int32, ok bool) {
	if !cs.HasMinMax || cs.TypeID != StatsTypeInt32 || len(cs.MinValue) != 4 || len(cs.MaxValue) != 4 {
		return 0, 0, false
	}
	min = int32(binary.LittleEndian.Uint32(cs.MinValue))
	max = int32(binary.LittleEndian.Uint32(cs.MaxValue))
	return min, max, true
}

// GetMinMaxInt64 returns Min/Max for int64 column.
func (cs *ColumnStatistics) GetMinMaxInt64() (min, max int64, ok bool) {
	if !cs.HasMinMax || cs.TypeID != StatsTypeInt64 || len(cs.MinValue) != 8 || len(cs.MaxValue) != 8 {
		return 0, 0, false
	}
	min = int64(binary.LittleEndian.Uint64(cs.MinValue))
	max = int64(binary.LittleEndian.Uint64(cs.MaxValue))
	return min, max, true
}

// GetMinMaxFloat32 returns Min/Max for float32 column.
func (cs *ColumnStatistics) GetMinMaxFloat32() (min, max float32, ok bool) {
	if !cs.HasMinMax || cs.TypeID != StatsTypeFloat32 || len(cs.MinValue) != 4 || len(cs.MaxValue) != 4 {
		return 0, 0, false
	}
	min = math.Float32frombits(binary.LittleEndian.Uint32(cs.MinValue))
	max = math.Float32frombits(binary.LittleEndian.Uint32(cs.MaxValue))
	return min, max, true
}

// GetMinMaxFloat64 returns Min/Max for float64 column.
func (cs *ColumnStatistics) GetMinMaxFloat64() (min, max float64, ok bool) {
	if !cs.HasMinMax || cs.TypeID != StatsTypeFloat64 || len(cs.MinValue) != 8 || len(cs.MaxValue) != 8 {
		return 0, 0, false
	}
	min = math.Float64frombits(binary.LittleEndian.Uint64(cs.MinValue))
	max = math.Float64frombits(binary.LittleEndian.Uint64(cs.MaxValue))
	return min, max, true
}

// ComputeColumnStatistics computes statistics for an Arrow array.
// Currently supports: Int32, Int64, Float32, Float64.
// For unsupported types, returns statistics with only NullCount (HasMinMax=false).
func ComputeColumnStatistics(array core.Array, columnIndex int32) *ColumnStatistics {
	stats := &ColumnStatistics{
		Version:     1,
		ColumnIndex: columnIndex,
		NullCount:   int64(array.NullN()),
	}
	
	if array.Len() == 0 || array.NullN() == array.Len() {
		// Empty or all-null column
		return stats
	}
	
	switch arr := array.(type) {
	case *core.Int32Array:
		computeInt32Stats(arr, stats)
	case *core.Int64Array:
		computeInt64Stats(arr, stats)
	case *core.Float32Array:
		computeFloat32Stats(arr, stats)
	case *core.Float64Array:
		computeFloat64Stats(arr, stats)
	default:
		// Unsupported type: return stats with only NullCount
		// HasMinMax remains false
	}
	
	return stats
}

func computeInt32Stats(arr *core.Int32Array, stats *ColumnStatistics) {
	values := arr.Values()
	
	// Fast path: no null values
	if arr.NullN() == 0 && len(values) > 0 {
		min, max := minMaxInt32Slice(values)
		stats.SetMinMaxInt32(min, max)
		return
	}
	
	var min, max int32
	first := true
	
	for i, v := range values {
		if arr.IsNull(i) {
			continue
		}
		
		if first {
			min, max = v, v
			first = false
		} else {
			if v < min {
				min = v
			}
			if v > max {
				max = v
			}
		}
	}
	
	if !first {
		stats.SetMinMaxInt32(min, max)
	}
}

// minMaxInt32Slice computes min and max in a single pass for int32 slices.
func minMaxInt32Slice(values []int32) (int32, int32) {
	if len(values) == 0 {
		return 0, 0
	}
	min, max := values[0], values[0]
	for _, v := range values[1:] {
		if v < min {
			min = v
		} else if v > max {
			max = v
		}
	}
	return min, max
}

func computeInt64Stats(arr *core.Int64Array, stats *ColumnStatistics) {
	values := arr.Values()
	
	// Fast path: no null values
	if arr.NullN() == 0 && len(values) > 0 {
		min, max := minMaxInt64Slice(values)
		stats.SetMinMaxInt64(min, max)
		return
	}
	
	var min, max int64
	first := true
	
	for i, v := range values {
		if arr.IsNull(i) {
			continue
		}
		if first {
			min, max = v, v
			first = false
		} else {
			if v < min {
				min = v
			}
			if v > max {
				max = v
			}
		}
	}
	
	if !first {
		stats.SetMinMaxInt64(min, max)
	}
}

// minMaxInt64Slice computes min and max in a single pass for int64 slices.
func minMaxInt64Slice(values []int64) (int64, int64) {
	if len(values) == 0 {
		return 0, 0
	}
	min, max := values[0], values[0]
	for _, v := range values[1:] {
		if v < min {
			min = v
		} else if v > max {
			max = v
		}
	}
	return min, max
}

func computeFloat32Stats(arr *core.Float32Array, stats *ColumnStatistics) {
	values := arr.Values()
	
	// Fast path: no null values
	if arr.NullN() == 0 && len(values) > 0 {
		min, max := minMaxFloat32Slice(values)
		if !math.IsNaN(float64(min)) && !math.IsNaN(float64(max)) {
			stats.SetMinMaxFloat32(min, max)
		}
		return
	}
	
	var min, max float32
	first := true
	
	for i, v := range values {
		if arr.IsNull(i) {
			continue
		}
		// Skip NaN values (treat them like nulls for statistics purposes)
		if math.IsNaN(float64(v)) {
			continue
		}
		if first {
			min, max = v, v
			first = false
		} else {
			if v < min {
				min = v
			}
			if v > max {
				max = v
			}
		}
	}
	
	if !first {
		stats.SetMinMaxFloat32(min, max)
	}
}

// minMaxFloat32Slice computes min and max in a single pass for float32 slices.
// NaN values are ignored (treated as invalid for min/max purposes).
func minMaxFloat32Slice(values []float32) (float32, float32) {
	if len(values) == 0 {
		return 0, 0
	}
	
	// Find first non-NaN value
	var min, max float32
	first := true
	for _, v := range values {
		if !math.IsNaN(float64(v)) {
			min, max = v, v
			first = false
			break
		}
	}
	
	// All values are NaN
	if first {
		return math.Float32frombits(0x7FC00000), math.Float32frombits(0x7FC00000) // NaN
	}
	
	for _, v := range values {
		if math.IsNaN(float64(v)) {
			continue
		}
		if v < min {
			min = v
		} else if v > max {
			max = v
		}
	}
	return min, max
}

func computeFloat64Stats(arr *core.Float64Array, stats *ColumnStatistics) {
	values := arr.Values()
	
	// Fast path: no null values
	if arr.NullN() == 0 && len(values) > 0 {
		min, max := minMaxFloat64Slice(values)
		if !math.IsNaN(min) && !math.IsNaN(max) {
			stats.SetMinMaxFloat64(min, max)
		}
		return
	}
	
	var min, max float64
	first := true
	
	for i, v := range values {
		if arr.IsNull(i) {
			continue
		}
		// Skip NaN values (treat them like nulls for statistics purposes)
		if math.IsNaN(v) {
			continue
		}
		if first {
			min, max = v, v
			first = false
		} else {
			if v < min {
				min = v
			}
			if v > max {
				max = v
			}
		}
	}
	
	if !first {
		stats.SetMinMaxFloat64(min, max)
	}
}

// minMaxFloat64Slice computes min and max in a single pass for float64 slices.
// NaN values are ignored (treated as invalid for min/max purposes).
func minMaxFloat64Slice(values []float64) (float64, float64) {
	if len(values) == 0 {
		return 0, 0
	}
	
	// Find first non-NaN value
	var min, max float64
	first := true
	for _, v := range values {
		if !math.IsNaN(v) {
			min, max = v, v
			first = false
			break
		}
	}
	
	// All values are NaN
	if first {
		return math.NaN(), math.NaN()
	}
	
	for _, v := range values {
		if math.IsNaN(v) {
			continue
		}
		if v < min {
			min = v
		} else if v > max {
			max = v
		}
	}
	return min, max
}

// Validate checks if the statistics are valid.
func (cs *ColumnStatistics) Validate() error {
	if cs.Version != 1 {
		return lerrors.New(lerrors.ErrInvalidArgument).
			Op("validate_column_stats").
			Context("version", cs.Version).
			Context("expected", 1).
			Build()
	}
	
	// Validate TypeID is within valid range
	if cs.TypeID > StatsTypeBinary {
		return lerrors.New(lerrors.ErrInvalidArgument).
			Op("validate_column_stats").
			Context("type_id", cs.TypeID).
			Context("max_allowed", StatsTypeBinary).
			Context("message", "invalid type id").
			Build()
	}
	
	// Validate ColumnIndex
	if cs.ColumnIndex < 0 {
		return lerrors.New(lerrors.ErrInvalidArgument).
			Op("validate_column_stats").
			Context("column_index", cs.ColumnIndex).
			Context("message", "column index must be non-negative").
			Build()
	}
	
	return nil
}

// String returns a human-readable representation.
// For typed columns, decodes min/max to actual values for better readability.
func (cs *ColumnStatistics) String() string {
	if !cs.HasMinMax {
		return fmt.Sprintf("Column %d: nulls=%d, no min/max", cs.ColumnIndex, cs.NullCount)
	}
	
	// Decode min/max based on TypeID for readable output
	switch cs.TypeID {
	case StatsTypeInt32:
		if min, max, ok := cs.GetMinMaxInt32(); ok {
			return fmt.Sprintf("Column %d: nulls=%d, min=%d, max=%d (int32)",
				cs.ColumnIndex, cs.NullCount, min, max)
		}
	case StatsTypeInt64:
		if min, max, ok := cs.GetMinMaxInt64(); ok {
			return fmt.Sprintf("Column %d: nulls=%d, min=%d, max=%d (int64)",
				cs.ColumnIndex, cs.NullCount, min, max)
		}
	case StatsTypeFloat32:
		if min, max, ok := cs.GetMinMaxFloat32(); ok {
			return fmt.Sprintf("Column %d: nulls=%d, min=%g, max=%g (float32)",
				cs.ColumnIndex, cs.NullCount, min, max)
		}
	case StatsTypeFloat64:
		if min, max, ok := cs.GetMinMaxFloat64(); ok {
			return fmt.Sprintf("Column %d: nulls=%d, min=%g, max=%g (float64)",
				cs.ColumnIndex, cs.NullCount, min, max)
		}
	}
	
	// Fallback to raw bytes if type unknown or decode failed
	return fmt.Sprintf("Column %d: nulls=%d, min=%v, max=%v (type=%d)",
		cs.ColumnIndex, cs.NullCount, cs.MinValue, cs.MaxValue, cs.TypeID)
}

// Merge updates this statistics by merging with another (for the same column).
// Used when accumulating statistics across multiple RecordBatches during writing.
func (cs *ColumnStatistics) Merge(other *ColumnStatistics) error {
	if other == nil {
		return nil
	}
	
	// Merge Min/Max first (before modifying cs.NullCount, in case of error)
	if !other.HasMinMax {
		// Other has no valid min/max, only accumulate null count
		cs.NullCount += other.NullCount
		return nil
	}
	
	if !cs.HasMinMax {
		// This has no min/max yet, copy from other
		cs.Version = other.Version
		cs.HasMinMax = other.HasMinMax
		cs.TypeID = other.TypeID
		cs.MinValue = make([]byte, len(other.MinValue))
		cs.MaxValue = make([]byte, len(other.MaxValue))
		copy(cs.MinValue, other.MinValue)
		copy(cs.MaxValue, other.MaxValue)
		// Accumulate null count after successful copy
		cs.NullCount += other.NullCount
		// Note: DistinctCount is not currently computed by ComputeColumnStatistics
		// If/when it is implemented, merge logic should be added here
		return nil
	}
	
	// Both have min/max, need to compare and update
	// Use TypeID for explicit type matching (safer than byte length inference)
	if cs.TypeID != other.TypeID {
		return lerrors.New(lerrors.ErrInvalidArgument).
			Op("merge_column_stats").
			Context("message", "type mismatch between statistics").
			Context("expected_type", cs.TypeID).
			Context("actual_type", other.TypeID).
			Build()
	}
	
	switch cs.TypeID {
	case StatsTypeInt32:
		if min1, max1, ok1 := cs.GetMinMaxInt32(); ok1 {
			if min2, max2, ok2 := other.GetMinMaxInt32(); ok2 {
				if min2 < min1 {
					cs.SetMinMaxInt32(min2, max(max1, max2))
				} else if max2 > max1 {
					cs.SetMinMaxInt32(min1, max2)
				}
				// else: other range is within cs range, no update needed
			}
		}
		
	case StatsTypeInt64:
		if min1, max1, ok1 := cs.GetMinMaxInt64(); ok1 {
			if min2, max2, ok2 := other.GetMinMaxInt64(); ok2 {
				if min2 < min1 {
					cs.SetMinMaxInt64(min2, max(max1, max2))
				} else if max2 > max1 {
					cs.SetMinMaxInt64(min1, max2)
				}
				// else: other range is within cs range, no update needed
			}
		}
		
	case StatsTypeFloat32:
		if min1, max1, ok1 := cs.GetMinMaxFloat32(); ok1 {
			if min2, max2, ok2 := other.GetMinMaxFloat32(); ok2 {
				if min2 < min1 {
					cs.SetMinMaxFloat32(min2, max(max1, max2))
				} else if max2 > max1 {
					cs.SetMinMaxFloat32(min1, max2)
				}
				// else: other range is within cs range, no update needed
			}
		}
		
	case StatsTypeFloat64:
		if min1, max1, ok1 := cs.GetMinMaxFloat64(); ok1 {
			if min2, max2, ok2 := other.GetMinMaxFloat64(); ok2 {
				if min2 < min1 {
					cs.SetMinMaxFloat64(min2, max(max1, max2))
				} else if max2 > max1 {
					cs.SetMinMaxFloat64(min1, max2)
				}
				// else: other range is within cs range, no update needed
			}
		}
	}
	
	// Successfully merged min/max, now accumulate null count
	cs.NullCount += other.NullCount
	
	return nil
}
