package bench

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/wzqhbustb/vego/storage/arrow"
	"github.com/wzqhbustb/vego/storage/column"
)

// BenchmarkVectorWrite 测试向量写入性能（HNSW场景）
func BenchmarkVectorWrite(b *testing.B) {
	dims := []int{128, 768, 1536}
	numVectors := []int{100, 1000, 10000}

	for _, dim := range dims {
		for _, n := range numVectors {
			name := fmt.Sprintf("dim=%d/vectors=%d", dim, n)

			b.Run(name, func(b *testing.B) {
				vectors := generateVectorData(n, dim)
				listType := arrow.FixedSizeListOf(arrow.PrimFloat32(), dim)

				childBuilder := arrow.NewFloat32Builder()
				for _, vec := range vectors {
					for _, v := range vec {
						childBuilder.Append(v)
					}
				}
				childArray := childBuilder.NewArray()
				array := arrow.NewFixedSizeListArray(listType.(*arrow.FixedSizeListType), childArray, nil)

				idBuilder := arrow.NewInt32Builder()
				for i := 0; i < n; i++ {
					idBuilder.Append(int32(i))
				}
				idArray := idBuilder.NewArray()

				schema := arrow.NewSchema([]arrow.Field{
					{Name: "id", Type: arrow.PrimInt32(), Nullable: false},
					{Name: "embedding", Type: listType, Nullable: false},
				}, nil)

				batch, _ := arrow.NewRecordBatch(schema, n, []arrow.Array{idArray, array})

				filename := filepath.Join(testDataDir, "vector_write_bench.lance")

				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					os.Remove(filename)
					writer, _ := column.NewWriter(filename, schema, encoderFactory)
					writer.WriteRecordBatch(batch)
					writer.Close()
				}

				// 数据量：ID (4 bytes) + vector (dim * 4 bytes) per vector
				b.SetBytes(int64(n * (4 + dim*4)))
			})
		}
	}
}

// BenchmarkVectorSearch 模拟向量搜索读取模式（随机访问）
func BenchmarkVectorSearch(b *testing.B) {
	// 准备 10万条 768维向量
	n := 100000
	dim := 768

	vectors := generateVectorData(n, dim)
	listType := arrow.FixedSizeListOf(arrow.PrimFloat32(), dim)

	childBuilder := arrow.NewFloat32Builder()
	for _, vec := range vectors {
		for _, v := range vec {
			childBuilder.Append(v)
		}
	}
	childArray := childBuilder.NewArray()
	array := arrow.NewFixedSizeListArray(listType.(*arrow.FixedSizeListType), childArray, nil)

	idBuilder := arrow.NewInt32Builder()
	for i := 0; i < n; i++ {
		idBuilder.Append(int32(i))
	}
	idArray := idBuilder.NewArray()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimInt32(), Nullable: false},
		{Name: "embedding", Type: listType, Nullable: false},
	}, nil)

	batch, _ := arrow.NewRecordBatch(schema, n, []arrow.Array{idArray, array})

	filename := filepath.Join(testDataDir, "vector_search_bench.lance")
	os.Remove(filename)

	writer, _ := column.NewWriter(filename, schema, encoderFactory)
	writer.WriteRecordBatch(batch)
	writer.Close()

	b.Run("FullScan", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			reader, _ := column.NewReader(filename)
			reader.ReadRecordBatch()
			reader.Close()
		}
	})
}
