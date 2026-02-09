package bench

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/wzqhbkjdx/vego/storage/arrow"
	"github.com/wzqhbkjdx/vego/storage/column"
)

// BenchmarkWriteFile 测试文件写入性能
func BenchmarkWriteFile(b *testing.B) {
	rowCounts := []int{1000, 10000, 100000}
	colCounts := []int{1, 5, 10}

	for _, rows := range rowCounts {
		for _, cols := range colCounts {
			name := fmt.Sprintf("rows=%d/cols=%d", rows, cols)

			b.Run(name, func(b *testing.B) {
				// 构建 schema
				fields := make([]arrow.Field, cols)
				for i := range fields {
					fields[i] = arrow.Field{
						Name:     fmt.Sprintf("col%d", i),
						Type:     arrow.PrimInt32(),
						Nullable: false,
					}
				}
				schema := arrow.NewSchema(fields, nil)

				// 构建数据
				columns := make([]arrow.Array, cols)
				for i := range columns {
					data := generateInt32Data(rows, "sequential")
					builder := arrow.NewInt32Builder()
					for _, v := range data {
						builder.Append(v)
					}
					columns[i] = builder.NewArray()
				}
				batch, _ := arrow.NewRecordBatch(schema, rows, columns)

				filename := filepath.Join(testDataDir, "write_bench.lance")

				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					os.Remove(filename)

					writer, _ := column.NewWriter(filename, schema, encoderFactory)
					writer.WriteRecordBatch(batch)
					writer.Close()
				}

				b.SetBytes(int64(rows * cols * 4))
			})
		}
	}
}

// BenchmarkReadFile 测试文件读取性能
func BenchmarkReadFile(b *testing.B) {
	rowCounts := []int{1000, 10000, 100000}
	colCounts := []int{1, 5, 10}

	for _, rows := range rowCounts {
		for _, cols := range colCounts {
			name := fmt.Sprintf("rows=%d/cols=%d", rows, cols)

			// 准备文件
			filename := filepath.Join(testDataDir, fmt.Sprintf("read_bench_%d_%d.lance", rows, cols))

			fields := make([]arrow.Field, cols)
			for i := range fields {
				fields[i] = arrow.Field{Name: fmt.Sprintf("col%d", i), Type: arrow.PrimInt32(), Nullable: false}
			}
			schema := arrow.NewSchema(fields, nil)

			columns := make([]arrow.Array, cols)
			for i := range columns {
				data := generateInt32Data(rows, "sequential")
				builder := arrow.NewInt32Builder()
				for _, v := range data {
					builder.Append(v)
				}
				columns[i] = builder.NewArray()
			}
			batch, _ := arrow.NewRecordBatch(schema, rows, columns)

			writer, _ := column.NewWriter(filename, schema, encoderFactory)
			writer.WriteRecordBatch(batch)
			writer.Close()

			b.Run(name, func(b *testing.B) {
				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					reader, err := column.NewReader(filename)
					if err != nil {
						b.Fatal(err)
					}

					_, err = reader.ReadRecordBatch()
					if err != nil {
						b.Fatal(err)
					}

					reader.Close()
				}

				b.SetBytes(int64(rows * cols * 4))
			})
		}
	}
}

// BenchmarkRoundtrip 测试写后读完整流程
func BenchmarkRoundtrip(b *testing.B) {
	scenarios := []struct {
		name string
		rows int
		cols int
	}{
		{"Small_1Kx5", 1000, 5},
		{"Medium_10Kx10", 10000, 10},
		{"Large_100Kx5", 100000, 5},
	}

	for _, sc := range scenarios {
		b.Run(sc.name, func(b *testing.B) {
			fields := make([]arrow.Field, sc.cols)
			for i := range fields {
				fields[i] = arrow.Field{Name: fmt.Sprintf("col%d", i), Type: arrow.PrimInt32(), Nullable: false}
			}
			schema := arrow.NewSchema(fields, nil)

			columns := make([]arrow.Array, sc.cols)
			for i := range columns {
				data := generateInt32Data(sc.rows, "sequential")
				builder := arrow.NewInt32Builder()
				for _, v := range data {
					builder.Append(v)
				}
				columns[i] = builder.NewArray()
			}
			batch, _ := arrow.NewRecordBatch(schema, sc.rows, columns)

			filename := filepath.Join(testDataDir, "roundtrip_bench.lance")

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// Write
				os.Remove(filename)
				writer, _ := column.NewWriter(filename, schema, encoderFactory)
				writer.WriteRecordBatch(batch)
				writer.Close()

				// Read
				reader, _ := column.NewReader(filename)
				reader.ReadRecordBatch()
				reader.Close()
			}

			b.SetBytes(int64(sc.rows * sc.cols * 4))
		})
	}
}
