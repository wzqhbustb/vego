package column

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/wzqhbustb/vego/storage/arrow"
	lanceio "github.com/wzqhbustb/vego/storage/io"
)

// ====================
// 辅助函数
// ====================

// setupAsyncIO 创建测试用 AsyncIO - 改为接受 testing.TB
func setupAsyncIO(t testing.TB) *lanceio.AsyncIO {
	config := lanceio.DefaultConfig()
	config.Workers = 4
	config.QueueSize = 100
	config.SchedulerCap = 1000
	asyncIO, err := lanceio.New(config)
	if err != nil {
		t.Fatalf("Failed to create AsyncIO: %v", err)
	}
	return asyncIO
}

// createTestFile 创建测试用 Lance 文件
func createTestFile(t testing.TB, filename string, numRows int, numColumns int) {
	fields := make([]arrow.Field, numColumns)
	for i := 0; i < numColumns; i++ {
		fields[i] = arrow.Field{
			Name:     fmt.Sprintf("col%d", i),
			Type:     arrow.PrimInt32(),
			Nullable: false,
		}
	}
	schema := arrow.NewSchema(fields, nil)

	writer, err := NewWriter(filename, schema, defaultEncoderFactory())
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	// 使用匿名函数确保 writer.Close() 总是被调用
	// 即使 t.Fatalf() 被调用也会执行 defer
	defer func() {
		if err := writer.Close(); err != nil {
			t.Errorf("writer.Close() failed: %v", err)
		}
	}()

	// 写入多个 batch
	batchSize := 100
	for start := 0; start < numRows; start += batchSize {
		size := batchSize
		if start+size > numRows {
			size = numRows - start
		}

		columns := make([]arrow.Array, numColumns)
		for col := 0; col < numColumns; col++ {
			builder := arrow.NewInt32Builder()
			for row := 0; row < size; row++ {
				builder.Append(int32(start + row + col*1000))
			}
			columns[col] = builder.NewArray()
			builder.Release()
		}

		batch, err := arrow.NewRecordBatch(schema, size, columns)
		if err != nil {
			t.Fatalf("NewRecordBatch failed: %v", err)
		}

		if err := writer.WriteRecordBatch(batch); err != nil {
			t.Fatalf("WriteRecordBatch failed: %v", err)
		}
	}
}

// ====================
// P0: 基础功能测试
// ====================

func TestReader_WithAsyncIO_Basic(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_async_basic.lance")

	// 创建测试文件
	createTestFile(t, filename, 500, 3)

	// 创建 AsyncIO
	asyncIO := setupAsyncIO(t)
	defer asyncIO.Close()

	// 使用 AsyncIO 创建 Reader
	reader, err := NewReaderWithAsyncIO(filename, asyncIO)
	if err != nil {
		t.Fatalf("NewReaderWithAsyncIO failed: %v", err)
	}
	defer reader.Close()

	// 验证 schema
	if reader.Schema().NumFields() != 3 {
		t.Errorf("Expected 3 fields, got %d", reader.Schema().NumFields())
	}

	// 验证行数
	if reader.NumRows() != 500 {
		t.Errorf("Expected 500 rows, got %d", reader.NumRows())
	}

	// 读取数据
	batch, err := reader.ReadRecordBatch()
	if err != nil {
		t.Fatalf("ReadRecordBatch failed: %v", err)
	}

	// 验证数据
	if batch.NumRows() != 500 {
		t.Errorf("Expected 500 rows in batch, got %d", batch.NumRows())
	}

	if batch.NumCols() != 3 {
		t.Errorf("Expected 3 columns in batch, got %d", batch.NumCols())
	}

	// 验证第一列的前几个值
	col0 := batch.Column(0).(*arrow.Int32Array)
	expectedValues := []int32{0, 1, 2, 3, 4}
	for i, expected := range expectedValues {
		if col0.Value(i) != expected {
			t.Errorf("col0[%d]: expected %d, got %d", i, expected, col0.Value(i))
		}
	}
}

func TestReader_WithAsyncIO_NilAsyncIO(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_async_nil.lance")

	createTestFile(t, filename, 100, 2)

	// 传入 nil AsyncIO 应该回退到同步模式
	reader, err := NewReaderWithAsyncIO(filename, nil)
	if err != nil {
		t.Fatalf("NewReaderWithAsyncIO with nil should succeed: %v", err)
	}
	defer reader.Close()

	// 验证可以正常读取
	batch, err := reader.ReadRecordBatch()
	if err != nil {
		t.Fatalf("ReadRecordBatch failed: %v", err)
	}

	if batch.NumRows() != 100 {
		t.Errorf("Expected 100 rows, got %d", batch.NumRows())
	}
}

func TestReader_WithAsyncIO_MultipleColumns(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_async_multicol.lance")

	// 创建多列文件
	numColumns := 10
	createTestFile(t, filename, 1000, numColumns)

	asyncIO := setupAsyncIO(t)
	defer asyncIO.Close()

	reader, err := NewReaderWithAsyncIO(filename, asyncIO)
	if err != nil {
		t.Fatalf("NewReaderWithAsyncIO failed: %v", err)
	}
	defer reader.Close()

	batch, err := reader.ReadRecordBatch()
	if err != nil {
		t.Fatalf("ReadRecordBatch failed: %v", err)
	}

	// 验证所有列
	if batch.NumCols() != numColumns {
		t.Errorf("Expected %d columns, got %d", numColumns, batch.NumCols())
	}

	// 验证每列的数据
	for col := 0; col < numColumns; col++ {
		colArray := batch.Column(col).(*arrow.Int32Array)
		expected := int32(col * 1000)
		if colArray.Value(0) != expected {
			t.Errorf("col%d[0]: expected %d, got %d", col, expected, colArray.Value(0))
		}
	}
}

func TestReader_WithAsyncIO_LargeFile(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_async_large.lance")

	// 创建大文件（10000 行，5 列）
	createTestFile(t, filename, 10000, 5)

	asyncIO := setupAsyncIO(t)
	defer asyncIO.Close()

	reader, err := NewReaderWithAsyncIO(filename, asyncIO)
	if err != nil {
		t.Fatalf("NewReaderWithAsyncIO failed: %v", err)
	}
	defer reader.Close()

	startTime := time.Now()
	batch, err := reader.ReadRecordBatch()
	duration := time.Since(startTime)

	if err != nil {
		t.Fatalf("ReadRecordBatch failed: %v", err)
	}

	t.Logf("Read 10000 rows x 5 columns in %v", duration)

	if batch.NumRows() != 10000 {
		t.Errorf("Expected 10000 rows, got %d", batch.NumRows())
	}
}

// ====================
// P0: Close 和资源管理测试
// ====================

func TestReader_Close_AsyncMode(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_async_close.lance")

	createTestFile(t, filename, 100, 2)

	asyncIO := setupAsyncIO(t)
	defer asyncIO.Close()

	reader, err := NewReaderWithAsyncIO(filename, asyncIO)
	if err != nil {
		t.Fatalf("NewReaderWithAsyncIO failed: %v", err)
	}

	// 获取 fileID 来检查引用计数（需要访问 FilePool）
	// 注意：这需要 FilePool 暴露 GetRefCount 方法
	stats := asyncIO.Stats()
	initialRefs := stats.FilePool.TotalReferences

	if initialRefs != 1 {
		t.Logf("Warning: Expected initial refCount 1, got %d", initialRefs)
	}

	// Close reader
	if err := reader.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 验证引用计数递减
	stats = asyncIO.Stats()
	afterRefs := stats.FilePool.TotalReferences

	if afterRefs != 0 {
		t.Errorf("Expected refCount 0 after close, got %d", afterRefs)
	}

	// 尝试在关闭后读取
	_, err = reader.ReadRecordBatch()
	if err == nil {
		t.Error("Expected error when reading from closed reader")
	}
}

func TestReader_Close_SyncMode(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_sync_close.lance")

	createTestFile(t, filename, 100, 2)

	reader, err := NewReader(filename)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}

	// Close
	if err := reader.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 尝试读取
	_, err = reader.ReadRecordBatch()
	if err == nil {
		t.Error("Expected error when reading from closed reader")
	}
}

func TestReader_Close_Multiple(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_close_multiple.lance")

	createTestFile(t, filename, 100, 2)

	asyncIO := setupAsyncIO(t)
	defer asyncIO.Close()

	reader, err := NewReaderWithAsyncIO(filename, asyncIO)
	if err != nil {
		t.Fatalf("NewReaderWithAsyncIO failed: %v", err)
	}

	// 第一次 Close
	if err := reader.Close(); err != nil {
		t.Fatalf("First close failed: %v", err)
	}

	// 第二次 Close 应该返回错误
	err = reader.Close()
	if err == nil {
		t.Error("Expected error on second close")
	}
}

func TestReader_MultipleReaders_SharedAsyncIO(t *testing.T) {
	tmpDir := t.TempDir()
	filename1 := filepath.Join(tmpDir, "test_shared1.lance")
	filename2 := filepath.Join(tmpDir, "test_shared2.lance")

	createTestFile(t, filename1, 100, 2)
	createTestFile(t, filename2, 200, 3)

	// 共享 AsyncIO
	asyncIO := setupAsyncIO(t)
	defer asyncIO.Close()

	// 创建两个 Reader
	reader1, err := NewReaderWithAsyncIO(filename1, asyncIO)
	if err != nil {
		t.Fatalf("NewReaderWithAsyncIO 1 failed: %v", err)
	}

	reader2, err := NewReaderWithAsyncIO(filename2, asyncIO)
	if err != nil {
		t.Fatalf("NewReaderWithAsyncIO 2 failed: %v", err)
	}

	// 验证文件池有 2 个文件
	stats := asyncIO.Stats()
	if stats.FilePool.TotalFiles != 2 {
		t.Errorf("Expected 2 files in pool, got %d", stats.FilePool.TotalFiles)
	}

	if stats.FilePool.TotalReferences != 2 {
		t.Errorf("Expected 2 total references, got %d", stats.FilePool.TotalReferences)
	}

	// 并发读取
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		_, err := reader1.ReadRecordBatch()
		if err != nil {
			t.Errorf("Reader1 failed: %v", err)
		}
	}()

	go func() {
		defer wg.Done()
		_, err := reader2.ReadRecordBatch()
		if err != nil {
			t.Errorf("Reader2 failed: %v", err)
		}
	}()

	wg.Wait()

	// 关闭 reader1
	reader1.Close()
	stats = asyncIO.Stats()
	if stats.FilePool.TotalReferences != 1 {
		t.Errorf("Expected 1 reference after closing reader1, got %d", stats.FilePool.TotalReferences)
	}

	// 关闭 reader2
	reader2.Close()
	stats = asyncIO.Stats()
	if stats.FilePool.TotalReferences != 0 {
		t.Errorf("Expected 0 references after closing all readers, got %d", stats.FilePool.TotalReferences)
	}
}

// ====================
// P0: 并发安全测试
// ====================

func TestReader_WithAsyncIO_ConcurrentReads(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_concurrent.lance")

	createTestFile(t, filename, 1000, 5)

	asyncIO := setupAsyncIO(t)
	defer asyncIO.Close()

	reader, err := NewReaderWithAsyncIO(filename, asyncIO)
	if err != nil {
		t.Fatalf("NewReaderWithAsyncIO failed: %v", err)
	}
	defer reader.Close()

	// 多个 goroutine 并发读取
	numReaders := 10
	var wg sync.WaitGroup
	wg.Add(numReaders)

	errors := make(chan error, numReaders)

	for i := 0; i < numReaders; i++ {
		go func(id int) {
			defer wg.Done()

			batch, err := reader.ReadRecordBatch()
			if err != nil {
				errors <- fmt.Errorf("reader %d failed: %w", id, err)
				return
			}

			if batch.NumRows() != 1000 {
				errors <- fmt.Errorf("reader %d: expected 1000 rows, got %d", id, batch.NumRows())
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// 检查错误
	for err := range errors {
		t.Error(err)
	}
}

func TestReader_MultipleReaders_SameFile(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_multiple_readers.lance")

	createTestFile(t, filename, 500, 3)

	asyncIO := setupAsyncIO(t)
	defer asyncIO.Close()

	// 创建多个 Reader 读取同一个文件
	numReaders := 5
	readers := make([]*Reader, numReaders)

	for i := 0; i < numReaders; i++ {
		reader, err := NewReaderWithAsyncIO(filename, asyncIO)
		if err != nil {
			t.Fatalf("NewReaderWithAsyncIO %d failed: %v", i, err)
		}
		readers[i] = reader
	}

	// 验证引用计数
	stats := asyncIO.Stats()
	expectedRefs := numReaders
	if stats.FilePool.TotalReferences != expectedRefs {
		t.Errorf("Expected %d references, got %d", expectedRefs, stats.FilePool.TotalReferences)
	}

	// 并发读取
	var wg sync.WaitGroup
	wg.Add(numReaders)

	for i := 0; i < numReaders; i++ {
		go func(idx int) {
			defer wg.Done()
			_, err := readers[idx].ReadRecordBatch()
			if err != nil {
				t.Errorf("Reader %d failed: %v", idx, err)
			}
		}(i)
	}

	wg.Wait()

	// 关闭所有 Reader
	for i := 0; i < numReaders; i++ {
		readers[i].Close()
	}

	// 验证引用计数归零
	stats = asyncIO.Stats()
	if stats.FilePool.TotalReferences != 0 {
		t.Errorf("Expected 0 references after closing all, got %d", stats.FilePool.TotalReferences)
	}
}

// ====================
// P0: 错误处理测试
// ====================

func TestReader_WithAsyncIO_InvalidFile(t *testing.T) {
	asyncIO := setupAsyncIO(t)
	defer asyncIO.Close()

	// 尝试打开不存在的文件
	_, err := NewReaderWithAsyncIO("/nonexistent/file.lance", asyncIO)
	if err == nil {
		t.Error("Expected error for nonexistent file")
	}
}

func TestReader_WithAsyncIO_CorruptedFile(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_corrupted.lance")

	// 创建一个无效的文件
	if err := os.WriteFile(filename, []byte("invalid data"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	asyncIO := setupAsyncIO(t)
	defer asyncIO.Close()

	// 尝试读取损坏的文件
	_, err := NewReaderWithAsyncIO(filename, asyncIO)
	if err == nil {
		t.Error("Expected error for corrupted file")
	}
}

func TestReader_WithAsyncIO_ContextCancellation(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_context.lance")

	createTestFile(t, filename, 1000, 5)

	asyncIO := setupAsyncIO(t)
	defer asyncIO.Close()

	reader, err := NewReaderWithAsyncIO(filename, asyncIO)
	if err != nil {
		t.Fatalf("NewReaderWithAsyncIO failed: %v", err)
	}
	defer reader.Close()

	// 启动读取
	done := make(chan error)
	go func() {
		_, err := reader.ReadRecordBatch()
		done <- err
	}()

	// 等待结果（应该快速完成）
	select {
	case err := <-done:
		if err != nil {
			t.Logf("Read completed with error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("Read timeout after 5 seconds")
	}
}

func BenchmarkReader_SyncVsAsync(b *testing.B) {
	tmpDir := b.TempDir()
	filename := filepath.Join(tmpDir, "bench_comparison.lance")

	// 创建测试文件 - 使用 b 替代 &testing.T{}
	createTestFile(b, filename, 5000, 10)

	b.Run("Sync", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			reader, err := NewReader(filename)
			if err != nil {
				b.Fatalf("NewReader failed: %v", err)
			}

			_, err = reader.ReadRecordBatch()
			if err != nil {
				b.Fatalf("ReadRecordBatch failed: %v", err)
			}

			reader.Close()
		}
	})

	b.Run("Async", func(b *testing.B) {
		asyncIO := setupAsyncIO(b) // 使用 b
		defer asyncIO.Close()

		for i := 0; i < b.N; i++ {
			reader, err := NewReaderWithAsyncIO(filename, asyncIO)
			if err != nil {
				b.Fatalf("NewReaderWithAsyncIO failed: %v", err)
			}

			_, err = reader.ReadRecordBatch()
			if err != nil {
				b.Fatalf("ReadRecordBatch failed: %v", err)
			}

			reader.Close()
		}
	})
}

func BenchmarkReader_AsyncIO_Concurrency(b *testing.B) {
	tmpDir := b.TempDir()
	filename := filepath.Join(tmpDir, "bench_concurrency.lance")

	createTestFile(b, filename, 10000, 10) // 使用 b

	asyncIO := setupAsyncIO(b) // 使用 b
	defer asyncIO.Close()

	concurrencies := []int{1, 2, 4, 8, 16}

	for _, concurrency := range concurrencies {
		b.Run(fmt.Sprintf("Concurrency%d", concurrency), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				var wg sync.WaitGroup
				wg.Add(concurrency)

				for j := 0; j < concurrency; j++ {
					go func() {
						defer wg.Done()

						reader, _ := NewReaderWithAsyncIO(filename, asyncIO)
						reader.ReadRecordBatch()
						reader.Close()
					}()
				}

				wg.Wait()
			}
		})
	}
}

func BenchmarkReader_AsyncIO_VariableColumns(b *testing.B) {
	tmpDir := b.TempDir()

	columnCounts := []int{1, 5, 10, 20, 50}

	asyncIO := setupAsyncIO(b) // 使用 b
	defer asyncIO.Close()

	for _, numCols := range columnCounts {
		filename := filepath.Join(tmpDir, fmt.Sprintf("bench_%dcols.lance", numCols))
		createTestFile(b, filename, 1000, numCols) // 使用 b

		b.Run(fmt.Sprintf("Columns%d", numCols), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				reader, _ := NewReaderWithAsyncIO(filename, asyncIO)
				reader.ReadRecordBatch()
				reader.Close()
			}
		})
	}
}

// ====================
// P1: 边界条件测试
// ====================

func TestReader_WithAsyncIO_EmptyFile(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_empty.lance")

	// 创建空 schema 文件
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "data", Type: arrow.PrimInt32(), Nullable: false},
	}, nil)

	writer, err := NewWriter(filename, schema, defaultEncoderFactory())
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}
	writer.Close()

	asyncIO := setupAsyncIO(t)
	defer asyncIO.Close()

	reader, err := NewReaderWithAsyncIO(filename, asyncIO)
	if err != nil {
		t.Fatalf("NewReaderWithAsyncIO failed: %v", err)
	}
	defer reader.Close()

	// 应该能读取，但没有数据
	if reader.NumRows() != 0 {
		t.Errorf("Expected 0 rows, got %d", reader.NumRows())
	}
}

func TestReader_WithAsyncIO_SinglePage(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_single_page.lance")

	// 创建只有一个 page 的小文件
	createTestFile(t, filename, 10, 1)

	asyncIO := setupAsyncIO(t)
	defer asyncIO.Close()

	reader, err := NewReaderWithAsyncIO(filename, asyncIO)
	if err != nil {
		t.Fatalf("NewReaderWithAsyncIO failed: %v", err)
	}
	defer reader.Close()

	batch, err := reader.ReadRecordBatch()
	if err != nil {
		t.Fatalf("ReadRecordBatch failed: %v", err)
	}

	if batch.NumRows() != 10 {
		t.Errorf("Expected 10 rows, got %d", batch.NumRows())
	}
}

func TestReader_WithAsyncIO_SingleColumn(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_single_col.lance")

	createTestFile(t, filename, 500, 1)

	asyncIO := setupAsyncIO(t)
	defer asyncIO.Close()

	reader, err := NewReaderWithAsyncIO(filename, asyncIO)
	if err != nil {
		t.Fatalf("NewReaderWithAsyncIO failed: %v", err)
	}
	defer reader.Close()

	batch, err := reader.ReadRecordBatch()
	if err != nil {
		t.Fatalf("ReadRecordBatch failed: %v", err)
	}

	if batch.NumCols() != 1 {
		t.Errorf("Expected 1 column, got %d", batch.NumCols())
	}
}

// ====================
// Float 类型测试辅助函数
// ====================

// createTestFileFloat32 创建 Float32 类型的测试文件
func createTestFileFloat32(t testing.TB, filename string, numRows int, numColumns int) {
	fields := make([]arrow.Field, numColumns)
	for i := 0; i < numColumns; i++ {
		fields[i] = arrow.Field{
			Name:     fmt.Sprintf("float_col%d", i),
			Type:     arrow.PrimFloat32(),
			Nullable: false,
		}
	}
	schema := arrow.NewSchema(fields, nil)

	writer, err := NewWriter(filename, schema, defaultEncoderFactory())
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}
	defer func() {
		if err := writer.Close(); err != nil {
			t.Errorf("writer.Close() failed: %v", err)
		}
	}()

	batchSize := 100
	for start := 0; start < numRows; start += batchSize {
		size := batchSize
		if start+size > numRows {
			size = numRows - start
		}

		columns := make([]arrow.Array, numColumns)
		for col := 0; col < numColumns; col++ {
			builder := arrow.NewFloat32Builder()
			for row := 0; row < size; row++ {
				// 生成一些有趣的 float 值：整数部分 + 小数部分
				value := float32(start+row)*1.5 + float32(col)*0.1
				builder.Append(value)
			}
			columns[col] = builder.NewArray()
			builder.Release()
		}

		batch, err := arrow.NewRecordBatch(schema, size, columns)
		if err != nil {
			t.Fatalf("NewRecordBatch failed: %v", err)
		}

		if err := writer.WriteRecordBatch(batch); err != nil {
			t.Fatalf("WriteRecordBatch failed: %v", err)
		}
	}
}

// createTestFileFloat64 创建 Float64 类型的测试文件
func createTestFileFloat64(t testing.TB, filename string, numRows int, numColumns int) {
	fields := make([]arrow.Field, numColumns)
	for i := 0; i < numColumns; i++ {
		fields[i] = arrow.Field{
			Name:     fmt.Sprintf("double_col%d", i),
			Type:     arrow.PrimFloat64(),
			Nullable: false,
		}
	}
	schema := arrow.NewSchema(fields, nil)

	writer, err := NewWriter(filename, schema, defaultEncoderFactory())
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}
	defer func() {
		if err := writer.Close(); err != nil {
			t.Errorf("writer.Close() failed: %v", err)
		}
	}()

	batchSize := 100
	for start := 0; start < numRows; start += batchSize {
		size := batchSize
		if start+size > numRows {
			size = numRows - start
		}

		columns := make([]arrow.Array, numColumns)
		for col := 0; col < numColumns; col++ {
			builder := arrow.NewFloat64Builder()
			for row := 0; row < size; row++ {
				// 生成高精度浮点数
				value := float64(start+row)*3.14159265359 + float64(col)*0.001
				builder.Append(value)
			}
			columns[col] = builder.NewArray()
			builder.Release()
		}

		batch, err := arrow.NewRecordBatch(schema, size, columns)
		if err != nil {
			t.Fatalf("NewRecordBatch failed: %v", err)
		}

		if err := writer.WriteRecordBatch(batch); err != nil {
			t.Fatalf("WriteRecordBatch failed: %v", err)
		}
	}
}

// createTestFileMixed 创建混合类型（int + float）的测试文件
func createTestFileMixed(t testing.TB, filename string, numRows int) {
	// 混合类型：int32, float32, float64, int64, float32
	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimInt32(), Nullable: false},
		{Name: "score", Type: arrow.PrimFloat32(), Nullable: false},
		{Name: "value", Type: arrow.PrimFloat64(), Nullable: false},
		{Name: "count", Type: arrow.PrimInt64(), Nullable: false},
		{Name: "ratio", Type: arrow.PrimFloat32(), Nullable: false},
	}
	schema := arrow.NewSchema(fields, nil)

	writer, err := NewWriter(filename, schema, defaultEncoderFactory())
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}
	defer func() {
		if err := writer.Close(); err != nil {
			t.Errorf("writer.Close() failed: %v", err)
		}
	}()

	batchSize := 100
	for start := 0; start < numRows; start += batchSize {
		size := batchSize
		if start+size > numRows {
			size = numRows - start
		}

		// int32 column
		idBuilder := arrow.NewInt32Builder()
		for row := 0; row < size; row++ {
			idBuilder.Append(int32(start + row))
		}

		// float32 column
		scoreBuilder := arrow.NewFloat32Builder()
		for row := 0; row < size; row++ {
			scoreBuilder.Append(float32(start+row) * 0.5)
		}

		// float64 column
		valueBuilder := arrow.NewFloat64Builder()
		for row := 0; row < size; row++ {
			valueBuilder.Append(float64(start+row) * 2.718281828)
		}

		// int64 column
		countBuilder := arrow.NewInt64Builder()
		for row := 0; row < size; row++ {
			countBuilder.Append(int64(start+row) * 1000)
		}

		// float32 column
		ratioBuilder := arrow.NewFloat32Builder()
		for row := 0; row < size; row++ {
			ratioBuilder.Append(float32(start+row) / 100.0)
		}

		columns := []arrow.Array{
			idBuilder.NewArray(),
			scoreBuilder.NewArray(),
			valueBuilder.NewArray(),
			countBuilder.NewArray(),
			ratioBuilder.NewArray(),
		}

		// 释放 builders
		idBuilder.Release()
		scoreBuilder.Release()
		valueBuilder.Release()
		countBuilder.Release()
		ratioBuilder.Release()

		batch, err := arrow.NewRecordBatch(schema, size, columns)
		if err != nil {
			t.Fatalf("NewRecordBatch failed: %v", err)
		}

		if err := writer.WriteRecordBatch(batch); err != nil {
			t.Fatalf("WriteRecordBatch failed: %v", err)
		}
	}
}

// ====================
// Float 类型测试用例
// ====================

func TestReader_WithAsyncIO_Float32(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_async_float32.lance")

	// 创建 Float32 测试文件
	createTestFileFloat32(t, filename, 500, 3)

	asyncIO := setupAsyncIO(t)
	defer asyncIO.Close()

	reader, err := NewReaderWithAsyncIO(filename, asyncIO)
	if err != nil {
		t.Fatalf("NewReaderWithAsyncIO failed: %v", err)
	}
	defer reader.Close()

	// 验证 schema
	if reader.Schema().NumFields() != 3 {
		t.Errorf("Expected 3 fields, got %d", reader.Schema().NumFields())
	}

	batch, err := reader.ReadRecordBatch()
	if err != nil {
		t.Fatalf("ReadRecordBatch failed: %v", err)
	}

	if batch.NumRows() != 500 {
		t.Errorf("Expected 500 rows, got %d", batch.NumRows())
	}

	// 验证 Float32 数据
	col0 := batch.Column(0).(*arrow.Float32Array)
	expectedValues := []float32{0, 1.5, 3.0, 4.5, 6.0}
	for i, expected := range expectedValues {
		if col0.Value(i) != expected {
			t.Errorf("col0[%d]: expected %v, got %v", i, expected, col0.Value(i))
		}
	}

	// 验证第二列的数据（带列偏移）
	col1 := batch.Column(1).(*arrow.Float32Array)
	expectedCol1 := float32(0*1.5 + 1*0.1) // 0 + 0.1
	if col1.Value(0) != expectedCol1 {
		t.Errorf("col1[0]: expected %v, got %v", expectedCol1, col1.Value(0))
	}
}

func TestReader_WithAsyncIO_Float64(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_async_float64.lance")

	// 创建 Float64 测试文件
	createTestFileFloat64(t, filename, 500, 3)

	asyncIO := setupAsyncIO(t)
	defer asyncIO.Close()

	reader, err := NewReaderWithAsyncIO(filename, asyncIO)
	if err != nil {
		t.Fatalf("NewReaderWithAsyncIO failed: %v", err)
	}
	defer reader.Close()

	batch, err := reader.ReadRecordBatch()
	if err != nil {
		t.Fatalf("ReadRecordBatch failed: %v", err)
	}

	if batch.NumRows() != 500 {
		t.Errorf("Expected 500 rows, got %d", batch.NumRows())
	}

	// 验证 Float64 数据（高精度）
	col0 := batch.Column(0).(*arrow.Float64Array)
	expectedValue := float64(0) * 3.14159265359
	if col0.Value(0) != expectedValue {
		t.Errorf("col0[0]: expected %v, got %v", expectedValue, col0.Value(0))
	}

	// 验证第10个值
	expectedValue10 := float64(9) * 3.14159265359
	if col0.Value(9) != expectedValue10 {
		t.Errorf("col0[9]: expected %v, got %v", expectedValue10, col0.Value(9))
	}
}

func TestReader_WithAsyncIO_MixedTypes(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_async_mixed.lance")

	// 创建混合类型测试文件
	createTestFileMixed(t, filename, 500)

	asyncIO := setupAsyncIO(t)
	defer asyncIO.Close()

	reader, err := NewReaderWithAsyncIO(filename, asyncIO)
	if err != nil {
		t.Fatalf("NewReaderWithAsyncIO failed: %v", err)
	}
	defer reader.Close()

	// 验证 schema
	schema := reader.Schema()
	if schema.NumFields() != 5 {
		t.Errorf("Expected 5 fields, got %d", schema.NumFields())
	}

	// 验证各列类型
	expectedTypes := []arrow.TypeID{arrow.INT32, arrow.FLOAT32, arrow.FLOAT64, arrow.INT64, arrow.FLOAT32}
	for i, expectedType := range expectedTypes {
		actualType := schema.Field(i).Type.ID()
		if actualType != expectedType {
			t.Errorf("Field %d: expected type %v, got %v", i, expectedType, actualType)
		}
	}

	// 读取数据
	batch, err := reader.ReadRecordBatch()
	if err != nil {
		t.Fatalf("ReadRecordBatch failed: %v", err)
	}

	if batch.NumRows() != 500 {
		t.Errorf("Expected 500 rows, got %d", batch.NumRows())
	}

	// 验证各列数据
	idCol := batch.Column(0).(*arrow.Int32Array)
	if idCol.Value(0) != 0 || idCol.Value(1) != 1 {
		t.Errorf("ID column values incorrect: %d, %d", idCol.Value(0), idCol.Value(1))
	}

	scoreCol := batch.Column(1).(*arrow.Float32Array)
	if scoreCol.Value(0) != 0 || scoreCol.Value(1) != 0.5 {
		t.Errorf("Score column values incorrect: %v, %v", scoreCol.Value(0), scoreCol.Value(1))
	}

	valueCol := batch.Column(2).(*arrow.Float64Array)
	expectedValue := float64(1) * 2.718281828
	if valueCol.Value(1) != expectedValue {
		t.Errorf("Value column[1]: expected %v, got %v", expectedValue, valueCol.Value(1))
	}

	countCol := batch.Column(3).(*arrow.Int64Array)
	if countCol.Value(0) != 0 || countCol.Value(1) != 1000 {
		t.Errorf("Count column values incorrect: %d, %d", countCol.Value(0), countCol.Value(1))
	}

	ratioCol := batch.Column(4).(*arrow.Float32Array)
	if ratioCol.Value(100) != 1.0 {
		t.Errorf("Ratio column[100]: expected 1.0, got %v", ratioCol.Value(100))
	}
}

func TestReader_WithAsyncIO_FloatLargeFile(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_async_float_large.lance")

	// 创建大 Float32 文件
	createTestFileFloat32(t, filename, 10000, 10)

	asyncIO := setupAsyncIO(t)
	defer asyncIO.Close()

	reader, err := NewReaderWithAsyncIO(filename, asyncIO)
	if err != nil {
		t.Fatalf("NewReaderWithAsyncIO failed: %v", err)
	}
	defer reader.Close()

	startTime := time.Now()
	batch, err := reader.ReadRecordBatch()
	duration := time.Since(startTime)

	if err != nil {
		t.Fatalf("ReadRecordBatch failed: %v", err)
	}

	t.Logf("Read 10000 rows x 10 float columns in %v", duration)

	if batch.NumRows() != 10000 {
		t.Errorf("Expected 10000 rows, got %d", batch.NumRows())
	}

	if batch.NumCols() != 10 {
		t.Errorf("Expected 10 columns, got %d", batch.NumCols())
	}
}

func TestReader_WithAsyncIO_FloatConcurrency(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "test_async_float_concurrent.lance")

	// 创建 Float64 测试文件
	createTestFileFloat64(t, filename, 1000, 5)

	asyncIO := setupAsyncIO(t)
	defer asyncIO.Close()

	reader, err := NewReaderWithAsyncIO(filename, asyncIO)
	if err != nil {
		t.Fatalf("NewReaderWithAsyncIO failed: %v", err)
	}
	defer reader.Close()

	// 并发读取验证
	numReaders := 10
	var wg sync.WaitGroup
	wg.Add(numReaders)

	errChan := make(chan error, numReaders)

	for i := 0; i < numReaders; i++ {
		go func(id int) {
			defer wg.Done()

			batch, err := reader.ReadRecordBatch()
			if err != nil {
				errChan <- fmt.Errorf("reader %d failed: %w", id, err)
				return
			}

			// 验证 float 数据精度
			col0 := batch.Column(0).(*arrow.Float64Array)
			expected0 := float64(0) * 3.14159265359
			if col0.Value(0) != expected0 {
				errChan <- fmt.Errorf("reader %d: col0[0] = %v, want %v", id, col0.Value(0), expected0)
			}

			// 验证中间值
			expected500 := float64(500) * 3.14159265359
			if col0.Value(500) != expected500 {
				errChan <- fmt.Errorf("reader %d: col0[500] = %v, want %v", id, col0.Value(500), expected500)
			}
		}(i)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Error(err)
	}
}

// Benchmark Float 类型读取性能
func BenchmarkReader_Float32(b *testing.B) {
	tmpDir := b.TempDir()
	filename := filepath.Join(tmpDir, "bench_float32.lance")

	createTestFileFloat32(b, filename, 10000, 10)

	asyncIO := setupAsyncIO(b)
	defer asyncIO.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader, _ := NewReaderWithAsyncIO(filename, asyncIO)
		reader.ReadRecordBatch()
		reader.Close()
	}
}

func BenchmarkReader_Float64(b *testing.B) {
	tmpDir := b.TempDir()
	filename := filepath.Join(tmpDir, "bench_float64.lance")

	createTestFileFloat64(b, filename, 10000, 10)

	asyncIO := setupAsyncIO(b)
	defer asyncIO.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader, _ := NewReaderWithAsyncIO(filename, asyncIO)
		reader.ReadRecordBatch()
		reader.Close()
	}
}
