package bench

import (
	"fmt"
	"testing"

	"github.com/wzqhbkjdx/vego/storage/arrow"
	"github.com/wzqhbkjdx/vego/storage/column"
)

// BenchmarkEncoding_Int32 测试 Int32 编码性能
func BenchmarkEncoding_Int32(b *testing.B) {
	sizes := []int{1000, 10000, 100000}
	patterns := []string{"random", "sequential", "repeated"}

	for _, size := range sizes {
		for _, pattern := range patterns {
			name := fmt.Sprintf("size=%d/pattern=%s", size, pattern)

			b.Run(name, func(b *testing.B) {
				data := generateInt32Data(size, pattern)
				builder := arrow.NewInt32Builder()
				for _, v := range data {
					builder.Append(v)
				}
				array := builder.NewArray()

				writer := column.NewPageWriter(encoderFactory)

				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					_, err := writer.WritePages(array, 0)
					if err != nil {
						b.Fatal(err)
					}
				}

				b.SetBytes(int64(size * 4))
			})
		}
	}
}

// BenchmarkEncoding_Float32 测试 Float32 编码性能
func BenchmarkEncoding_Float32(b *testing.B) {
	sizes := []int{1000, 10000, 100000}
	patterns := []string{"random", "small_range"}

	for _, size := range sizes {
		for _, pattern := range patterns {
			name := fmt.Sprintf("size=%d/pattern=%s", size, pattern)

			b.Run(name, func(b *testing.B) {
				data := generateFloat32Data(size, pattern)
				builder := arrow.NewFloat32Builder()
				for _, v := range data {
					builder.Append(v)
				}
				array := builder.NewArray()

				writer := column.NewPageWriter(encoderFactory)

				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					_, err := writer.WritePages(array, 0)
					if err != nil {
						b.Fatal(err)
					}
				}

				b.SetBytes(int64(size * 4))
			})
		}
	}
}

// BenchmarkEncoding_Vector768 测试向量编码性能
func BenchmarkEncoding_Vector768(b *testing.B) {
	numVectors := []int{100, 500, 1000}

	for _, n := range numVectors {
		name := fmt.Sprintf("vectors=%d/dim=768", n)

		b.Run(name, func(b *testing.B) {
			vectors := generateVectorData(n, 768)
			listType := arrow.FixedSizeListOf(arrow.PrimFloat32(), 768)

			childBuilder := arrow.NewFloat32Builder()
			for _, vec := range vectors {
				for _, v := range vec {
					childBuilder.Append(v)
				}
			}
			childArray := childBuilder.NewArray()
			array := arrow.NewFixedSizeListArray(listType.(*arrow.FixedSizeListType), childArray, nil)

			writer := column.NewPageWriter(encoderFactory)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				_, err := writer.WritePages(array, 0)
				if err != nil {
					b.Fatal(err)
				}
			}

			b.SetBytes(int64(n * 768 * 4))
		})
	}
}
