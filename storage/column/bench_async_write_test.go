package column

import (
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/wzqhbustb/vego/core"
	"github.com/wzqhbustb/vego/storage/format"
	"github.com/wzqhbustb/vego/vfs"
)

// BenchmarkWriteRecordBatch3Col measures full-file WriteRecordBatch
// with a typical Vego 3-column schema: id_hash(int64), vector(768d float32), timestamp(int64).
func BenchmarkWriteRecordBatch3Col(b *testing.B) {
	batch := build3ColBatch(1000, 768)
	benchmarkWriteRecordBatch(b, batch)
}

// BenchmarkWriteRecordBatch10Col measures a wide-table schema (10 int32 columns).
func BenchmarkWriteRecordBatch10Col(b *testing.B) {
	batch := buildNColBatch(1000, 10, core.PrimInt32())
	benchmarkWriteRecordBatch(b, batch)
}

// BenchmarkWriteRecordBatch20Col measures a very wide-table schema (20 int32 columns).
func BenchmarkWriteRecordBatch20Col(b *testing.B) {
	batch := buildNColBatch(1000, 20, core.PrimInt32())
	benchmarkWriteRecordBatch(b, batch)
}

// BenchmarkWriteRecordBatchVectorOnly measures a schema with only vector columns
// (3 vector columns, 100 rows each). This maximizes CPU-bound encoding work.
func BenchmarkWriteRecordBatchVectorOnly(b *testing.B) {
	batch := buildVectorOnlyBatch(100, 768, 3)
	benchmarkWriteRecordBatch(b, batch)
}

func benchmarkWriteRecordBatch(b *testing.B, batch *core.RecordBatch) {
	for _, async := range []bool{false, true} {
		b.Run(fmt.Sprintf("async=%v", async), func(b *testing.B) {
			f, _ := os.CreateTemp(b.TempDir(), "bench_*.lance")
			writer, _ := NewWriterWithVFS(f.Name(), vfs.Local, batch.Schema(), defaultEncoderFactory(), WithAsyncWrite(async))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = writer.WriteRecordBatch(batch)
			}
			b.StopTimer()
			_ = writer.Close()
			f.Close()
		})
	}
}

// BenchmarkEncodePhase3Col measures only the encode phase for 3 columns.
func BenchmarkEncodePhase3Col(b *testing.B) {
	batch := build3ColBatch(1000, 768)
	benchmarkEncodePhase(b, batch)
}

// BenchmarkEncodePhase10Col measures only the encode phase for 10 columns.
func BenchmarkEncodePhase10Col(b *testing.B) {
	batch := buildNColBatch(1000, 10, core.PrimInt32())
	benchmarkEncodePhase(b, batch)
}

// BenchmarkEncodePhase20Col measures only the encode phase for 20 columns.
func BenchmarkEncodePhase20Col(b *testing.B) {
	batch := buildNColBatch(1000, 20, core.PrimInt32())
	benchmarkEncodePhase(b, batch)
}

// BenchmarkEncodePhaseVectorOnly measures encode phase with 3 vector columns.
func BenchmarkEncodePhaseVectorOnly(b *testing.B) {
	batch := buildVectorOnlyBatch(100, 768, 3)
	benchmarkEncodePhase(b, batch)
}

func benchmarkEncodePhase(b *testing.B, batch *core.RecordBatch) {
	factory := defaultEncoderFactory()
	pw := NewPageWriter(factory)

	b.Run("serial", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for colIdx := 0; colIdx < batch.NumCols(); colIdx++ {
				_ = validateArray(batch.Column(colIdx), batch.Schema().Field(colIdx))
				_ = format.ComputeColumnStatistics(batch.Column(colIdx), int32(colIdx))
				_, _ = pw.WritePages(batch.Column(colIdx), int32(colIdx))
			}
		}
	})

	b.Run("parallel", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var wg sync.WaitGroup
			wg.Add(batch.NumCols())
			for colIdx := 0; colIdx < batch.NumCols(); colIdx++ {
				go func(idx int) {
					defer wg.Done()
					_ = validateArray(batch.Column(idx), batch.Schema().Field(idx))
					_ = format.ComputeColumnStatistics(batch.Column(idx), int32(idx))
					_, _ = pw.WritePages(batch.Column(idx), int32(idx))
				}(colIdx)
			}
			wg.Wait()
		}
	})
}

// build3ColBatch creates a typical Vego 3-column batch.
func build3ColBatch(numRows, dim int) *core.RecordBatch {
	idBuilder := core.NewInt64Builder()
	idBuilder.Reserve(numRows)
	for i := 0; i < numRows; i++ {
		idBuilder.Append(int64(i))
	}
	idArray := idBuilder.NewArray()
	idBuilder.Release()

	childBuilder := core.NewFloat32Builder()
	childBuilder.Reserve(numRows * dim)
	for i := 0; i < numRows*dim; i++ {
		childBuilder.Append(float32(i) * 0.001)
	}
	childArray := childBuilder.NewArray()
	childBuilder.Release()
	listType := core.FixedSizeListOf(core.PrimFloat32(), dim)
	vecArray := core.NewFixedSizeListArray(listType.(*core.FixedSizeListType), childArray, nil)

	tsBuilder := core.NewInt64Builder()
	tsBuilder.Reserve(numRows)
	for i := 0; i < numRows; i++ {
		tsBuilder.Append(int64(1000000 + i))
	}
	tsArray := tsBuilder.NewArray()
	tsBuilder.Release()

	schema := core.NewSchema([]core.Field{
		{Name: "id_hash", Type: core.PrimInt64(), Nullable: false},
		{Name: "vector", Type: core.VectorType(dim), Nullable: false},
		{Name: "timestamp", Type: core.PrimInt64(), Nullable: false},
	}, nil)

	batch, _ := core.NewRecordBatch(schema, numRows, []core.Array{idArray, vecArray, tsArray})
	return batch
}

// buildNColBatch creates a batch with N identical primitive columns.
func buildNColBatch(numRows, n int, dtype core.DataType) *core.RecordBatch {
	fields := make([]core.Field, n)
	columns := make([]core.Array, n)
	for i := 0; i < n; i++ {
		fields[i] = core.Field{Name: fmt.Sprintf("col_%d", i), Type: dtype, Nullable: false}
		b := core.NewInt32Builder()
		b.Reserve(numRows)
		for j := 0; j < numRows; j++ {
			b.Append(int32(j + i*numRows))
		}
		columns[i] = b.NewArray()
		b.Release()
	}
	schema := core.NewSchema(fields, nil)
	batch, _ := core.NewRecordBatch(schema, numRows, columns)
	return batch
}

// buildVectorOnlyBatch creates a batch with only vector columns.
func buildVectorOnlyBatch(numRows, dim, numCols int) *core.RecordBatch {
	fields := make([]core.Field, numCols)
	columns := make([]core.Array, numCols)
	listType := core.FixedSizeListOf(core.PrimFloat32(), dim)
	for i := 0; i < numCols; i++ {
		fields[i] = core.Field{Name: fmt.Sprintf("vec_%d", i), Type: core.VectorType(dim), Nullable: false}
		childBuilder := core.NewFloat32Builder()
		childBuilder.Reserve(numRows * dim)
		for j := 0; j < numRows*dim; j++ {
			childBuilder.Append(float32(j+i*numRows*dim) * 0.001)
		}
		childArray := childBuilder.NewArray()
		childBuilder.Release()
		columns[i] = core.NewFixedSizeListArray(listType.(*core.FixedSizeListType), childArray, nil)
	}
	schema := core.NewSchema(fields, nil)
	batch, _ := core.NewRecordBatch(schema, numRows, columns)
	return batch
}
