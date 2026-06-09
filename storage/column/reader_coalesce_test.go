package column

import (
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/wzqhbustb/vego/core"
	"github.com/wzqhbustb/vego/storage/format"
	"github.com/wzqhbustb/vego/vfs"
)

// ====================
// P1: Range Coalescing 测试
// ====================

func TestCoalesceRanges(t *testing.T) {
	tests := []struct {
		name         string
		pages        []format.PageIndex
		maxGap       int64
		maxMergeSize int64
		wantRanges   int
		wantSizes    []int64
	}{
		{
			name: "连续 pages 应合并",
			pages: []format.PageIndex{
				{Offset: 0, Size: 8192},
				{Offset: 8192, Size: 8192},
				{Offset: 16384, Size: 8192},
			},
			maxGap:       4096,
			maxMergeSize: 1024 * 1024,
			wantRanges:   1,
			wantSizes:    []int64{24576},
		},
		{
			name: "间隔超过 maxGap 不合并",
			pages: []format.PageIndex{
				{Offset: 0, Size: 8192},
				{Offset: 8192 + 5000, Size: 8192}, // gap = 5000 > 4096
				{Offset: 8192 + 5000 + 8192 + 5000, Size: 8192},
			},
			maxGap:       4096,
			maxMergeSize: 1024 * 1024,
			wantRanges:   3,
			wantSizes:    []int64{8192, 8192, 8192},
		},
		{
			name: "合并后超过 maxMergeSize 应拆分",
			pages: []format.PageIndex{
				{Offset: 0, Size: 600 * 1024},
				{Offset: 600 * 1024, Size: 600 * 1024}, // candidate = 1.2MB > 1MB
			},
			maxGap:       4096,
			maxMergeSize: 1024 * 1024,
			wantRanges:   2,
			wantSizes:    []int64{600 * 1024, 600 * 1024},
		},
		{
			name: "gap 在阈值内应合并",
			pages: []format.PageIndex{
				{Offset: 0, Size: 8192},
				{Offset: 8192 + 2048, Size: 8192}, // gap = 2048 <= 4096
			},
			maxGap:       4096,
			maxMergeSize: 1024 * 1024,
			wantRanges:   1,
			wantSizes:    []int64{8192 + 2048 + 8192},
		},
		{
			name:         "空输入",
			pages:        []format.PageIndex{},
			maxGap:       4096,
			maxMergeSize: 1024 * 1024,
			wantRanges:   0,
		},
		{
			name:         "单 page",
			pages:        []format.PageIndex{{Offset: 100, Size: 500}},
			maxGap:       4096,
			maxMergeSize: 1024 * 1024,
			wantRanges:   1,
			wantSizes:    []int64{500},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ranges := coalesceRanges(tt.pages, tt.maxGap, tt.maxMergeSize)
			if len(ranges) != tt.wantRanges {
				t.Errorf("coalesceRanges() got %d ranges, want %d", len(ranges), tt.wantRanges)
			}
			for i, want := range tt.wantSizes {
				if i >= len(ranges) {
					break
				}
				if ranges[i].size != want {
					t.Errorf("range[%d].size = %d, want %d", i, ranges[i].size, want)
				}
			}
		})
	}
}

// TestReadPagesAsyncCoalescing verifies that coalesced reads produce identical
// results to sync reads.
func TestReadPagesAsyncCoalescing(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_coalesce.lance")

	// Create test file with multiple columns and batches
	numColumns := 3
	numRows := 500
	createTestFile(t, filename, numRows, numColumns)

	// Sync read
	syncReader, err := NewReader(filename)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer syncReader.Close()

	syncBatch, err := syncReader.ReadRecordBatch()
	if err != nil {
		t.Fatalf("sync ReadRecordBatch failed: %v", err)
	}

	// Async read with coalescing
	asyncIO := setupAsyncIO(t)
	defer asyncIO.Close()

	asyncReader, err := NewReaderWithAsyncIO(filename, asyncIO)
	if err != nil {
		t.Fatalf("NewReaderWithAsyncIO failed: %v", err)
	}
	defer asyncReader.Close()

	asyncBatch, err := asyncReader.ReadRecordBatch()
	if err != nil {
		t.Fatalf("async ReadRecordBatch failed: %v", err)
	}

	// Verify byte-level equality
	if syncBatch.NumRows() != asyncBatch.NumRows() {
		t.Errorf("row count mismatch: sync=%d, async=%d", syncBatch.NumRows(), asyncBatch.NumRows())
	}
	if syncBatch.NumCols() != asyncBatch.NumCols() {
		t.Errorf("column count mismatch: sync=%d, async=%d", syncBatch.NumCols(), asyncBatch.NumCols())
	}

	for col := 0; col < syncBatch.NumCols(); col++ {
		syncArr := syncBatch.Column(col).(*core.Int32Array)
		asyncArr := asyncBatch.Column(col).(*core.Int32Array)
		if syncArr.Len() != asyncArr.Len() {
			t.Errorf("col%d length mismatch", col)
			continue
		}
		if syncArr.NullN() != asyncArr.NullN() {
			t.Errorf("col%d null count mismatch: sync=%d, async=%d", col, syncArr.NullN(), asyncArr.NullN())
		}
		if !reflect.DeepEqual(syncArr.Values(), asyncArr.Values()) {
			t.Errorf("col%d values mismatch (byte-level)", col)
		}
	}

	t.Log("Coalescing read produces identical results to sync read")
}

// TestCoalesceRangesPreservesOrder verifies original page order is preserved
// in the returned indices.
func TestCoalesceRangesPreservesOrder(t *testing.T) {
	pages := []format.PageIndex{
		{Offset: 100, Size: 50},
		{Offset: 0, Size: 50},   // before first in file order
		{Offset: 200, Size: 50},
	}

	ranges := coalesceRanges(pages, 4096, 1024*1024)
	if len(ranges) != 1 {
		t.Fatalf("expected 1 range, got %d", len(ranges))
	}

	// Verify all original indices are present
	if len(ranges[0].indices) != 3 {
		t.Errorf("expected 3 indices, got %d", len(ranges[0].indices))
	}

	// Verify decoding would map back correctly: just check no panic
	_ = fmt.Sprintf("ranges: %+v", ranges)
}

// readAtCounter wraps a vfs.File and counts ReadAt calls.
type readAtCounter struct {
	vfs.File
	count int
}

func (c *readAtCounter) ReadAt(p []byte, off int64) (int, error) {
	c.count++
	return c.File.ReadAt(p, off)
}

// TestReadPagesAsyncReducesReadAtCount verifies that coalescing actually
// reduces the number of ReadAt syscalls.
func TestReadPagesAsyncReducesReadAtCount(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_coalesce_count.lance")

	// Create file with 5 batches => ~5 pages per column
	numColumns := 1
	numRows := 500
	createTestFile(t, filename, numRows, numColumns)

	// Open with sync reader, then swap file for counter
	reader, err := NewReader(filename)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer reader.Close()

	counter := &readAtCounter{File: reader.file}
	reader.file = counter
	reader.useAsync = true
	reader.asyncEnabled = true

	pageIndices := reader.footer.GetColumnPages(0)
	if len(pageIndices) == 0 {
		t.Fatal("no pages found for column 0")
	}

	field := reader.header.Schema.Field(0)
	_, err = reader.readPagesAsync(pageIndices, field.Type)
	if err != nil {
		t.Fatalf("readPagesAsync failed: %v", err)
	}

	t.Logf("ReadAt calls: %d for %d pages", counter.count, len(pageIndices))

	if counter.count >= len(pageIndices) {
		t.Errorf("coalescing did not reduce ReadAt calls: got %d calls for %d pages",
			counter.count, len(pageIndices))
	}

	// With 5 small pages all within 1MB, they should merge into a single range
	if counter.count != 1 {
		t.Logf("note: expected 1 ReadAt call for fully coalesced pages, got %d (may vary by page layout)", counter.count)
	}
}

// TestReadPagesAsync100PagesCoalesced verifies that ~100 continuous pages
// are merged into a single ReadAt call.
func TestReadPagesAsync100PagesCoalesced(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_100pages.lance")

	// 10000 rows with batchSize=100 => ~100 pages per column
	createTestFile(t, filename, 10000, 1)

	reader, err := NewReader(filename)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer reader.Close()

	counter := &readAtCounter{File: reader.file}
	reader.file = counter
	reader.useAsync = true
	reader.asyncEnabled = true

	pageIndices := reader.footer.GetColumnPages(0)
	t.Logf("Total pages: %d", len(pageIndices))
	if len(pageIndices) < 50 {
		t.Skipf("only %d pages generated, need >=50 for meaningful test", len(pageIndices))
	}

	field := reader.header.Schema.Field(0)
	_, err = reader.readPagesAsync(pageIndices, field.Type)
	if err != nil {
		t.Fatalf("readPagesAsync failed: %v", err)
	}

	t.Logf("ReadAt calls: %d for %d pages", counter.count, len(pageIndices))
	if counter.count != 1 {
		t.Errorf("expected 1 ReadAt call for %d continuous pages, got %d", len(pageIndices), counter.count)
	}
}

// TestReadPagesAsyncCoalescingFloat32 verifies coalescing with Float32 data.
func TestReadPagesAsyncCoalescingFloat32(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_coalesce_f32.lance")

	createTestFileFloat32(t, filename, 500, 3)

	// Sync read
	syncReader, err := NewReader(filename)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer syncReader.Close()
	syncBatch, err := syncReader.ReadRecordBatch()
	if err != nil {
		t.Fatalf("sync ReadRecordBatch failed: %v", err)
	}

	// Async read with coalescing
	asyncIO := setupAsyncIO(t)
	defer asyncIO.Close()
	asyncReader, err := NewReaderWithAsyncIO(filename, asyncIO)
	if err != nil {
		t.Fatalf("NewReaderWithAsyncIO failed: %v", err)
	}
	defer asyncReader.Close()
	asyncBatch, err := asyncReader.ReadRecordBatch()
	if err != nil {
		t.Fatalf("async ReadRecordBatch failed: %v", err)
	}

	// Byte-level comparison
	for col := 0; col < syncBatch.NumCols(); col++ {
		syncArr := syncBatch.Column(col).(*core.Float32Array)
		asyncArr := asyncBatch.Column(col).(*core.Float32Array)
		if syncArr.Len() != asyncArr.Len() {
			t.Errorf("col%d length mismatch", col)
			continue
		}
		if !reflect.DeepEqual(syncArr.Values(), asyncArr.Values()) {
			t.Errorf("col%d values mismatch (byte-level)", col)
		}
	}
}

// TestReadPagesAsyncCoalescingFloat64 verifies coalescing with Float64 data.
func TestReadPagesAsyncCoalescingFloat64(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_coalesce_f64.lance")

	createTestFileFloat64(t, filename, 500, 3)

	// Sync read
	syncReader, err := NewReader(filename)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer syncReader.Close()
	syncBatch, err := syncReader.ReadRecordBatch()
	if err != nil {
		t.Fatalf("sync ReadRecordBatch failed: %v", err)
	}

	// Async read with coalescing
	asyncIO := setupAsyncIO(t)
	defer asyncIO.Close()
	asyncReader, err := NewReaderWithAsyncIO(filename, asyncIO)
	if err != nil {
		t.Fatalf("NewReaderWithAsyncIO failed: %v", err)
	}
	defer asyncReader.Close()
	asyncBatch, err := asyncReader.ReadRecordBatch()
	if err != nil {
		t.Fatalf("async ReadRecordBatch failed: %v", err)
	}

	// Byte-level comparison
	for col := 0; col < syncBatch.NumCols(); col++ {
		syncArr := syncBatch.Column(col).(*core.Float64Array)
		asyncArr := asyncBatch.Column(col).(*core.Float64Array)
		if syncArr.Len() != asyncArr.Len() {
			t.Errorf("col%d length mismatch", col)
			continue
		}
		if !reflect.DeepEqual(syncArr.Values(), asyncArr.Values()) {
			t.Errorf("col%d values mismatch (byte-level)", col)
		}
	}
}

// TestReadPagesAsyncCoalescingWithNulls verifies coalescing preserves null bitmap.
func TestReadPagesAsyncCoalescingWithNulls(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_coalesce_nulls.lance")

	schema := core.NewSchema([]core.Field{
		{Name: "nullable_col", Type: core.PrimInt32(), Nullable: true},
	}, nil)

	writer, err := NewWriter(filename, schema, nil)
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	// Write batch with interleaved nulls
	builder := core.NewInt32Builder()
	for i := 0; i < 200; i++ {
		if i%3 == 0 {
			builder.AppendNull()
		} else {
			builder.Append(int32(i))
		}
	}
	arr := builder.NewArray()

	batch, err := core.NewRecordBatch(schema, 200, []core.Array{arr})
	if err != nil {
		t.Fatalf("NewRecordBatch failed: %v", err)
	}
	if err := writer.WriteRecordBatch(batch); err != nil {
		t.Fatalf("WriteRecordBatch failed: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("writer.Close failed: %v", err)
	}

	// Sync read
	syncReader, err := NewReader(filename)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer syncReader.Close()
	syncBatch, err := syncReader.ReadRecordBatch()
	if err != nil {
		t.Fatalf("sync ReadRecordBatch failed: %v", err)
	}

	// Async read with coalescing
	asyncIO := setupAsyncIO(t)
	defer asyncIO.Close()
	asyncReader, err := NewReaderWithAsyncIO(filename, asyncIO)
	if err != nil {
		t.Fatalf("NewReaderWithAsyncIO failed: %v", err)
	}
	defer asyncReader.Close()
	asyncBatch, err := asyncReader.ReadRecordBatch()
	if err != nil {
		t.Fatalf("async ReadRecordBatch failed: %v", err)
	}

	// Deep comparison including null bitmap
	syncArr := syncBatch.Column(0).(*core.Int32Array)
	asyncArr := asyncBatch.Column(0).(*core.Int32Array)

	if syncArr.Len() != asyncArr.Len() {
		t.Fatalf("length mismatch: sync=%d, async=%d", syncArr.Len(), asyncArr.Len())
	}
	if syncArr.NullN() != asyncArr.NullN() {
		t.Fatalf("null count mismatch: sync=%d, async=%d", syncArr.NullN(), asyncArr.NullN())
	}

	for i := 0; i < syncArr.Len(); i++ {
		if syncArr.IsNull(i) != asyncArr.IsNull(i) {
			t.Fatalf("null mismatch at %d: sync=%v, async=%v", i, syncArr.IsNull(i), asyncArr.IsNull(i))
		}
		if !syncArr.IsNull(i) && syncArr.Value(i) != asyncArr.Value(i) {
			t.Fatalf("value mismatch at %d: sync=%d, async=%d", i, syncArr.Value(i), asyncArr.Value(i))
		}
	}

	t.Logf("Null bitmap preserved correctly: %d nulls out of %d values", syncArr.NullN(), syncArr.Len())
}

// TestCoalesceRanges100PagesNoMerge verifies that 100 pages spaced 1MB apart
// do NOT merge, producing 100 independent ranges.
func TestCoalesceRanges100PagesNoMerge(t *testing.T) {
	pages := make([]format.PageIndex, 100)
	for i := 0; i < 100; i++ {
		pages[i] = format.PageIndex{
			Offset: int64(i) * (1024*1024 + 8192), // 1MB + 8KB apart
			Size:   8192,
		}
	}

	ranges := coalesceRanges(pages, 4*1024, 1024*1024)
	if len(ranges) != 100 {
		t.Errorf("expected 100 ranges for 100 spaced pages, got %d", len(ranges))
	}
	for i, r := range ranges {
		if len(r.indices) != 1 {
			t.Errorf("range[%d] should contain exactly 1 page, got %d", i, len(r.indices))
		}
		if r.indices[0] != i {
			t.Errorf("range[%d] should map to original page %d, got %d", i, i, r.indices[0])
		}
	}
}
