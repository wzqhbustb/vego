package bench

import (
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/wzqhbustb/vego/storage/encoding"
)

var (
	testDataDir    string
	encoderFactory *encoding.EncoderFactory
)

func TestMain(m *testing.M) {
	// 初始化
	encoderFactory = encoding.NewEncoderFactory(3)
	testDataDir = filepath.Join(os.TempDir(), "lance_bench")
	os.MkdirAll(testDataDir, 0755)

	code := m.Run()

	// 清理
	os.RemoveAll(testDataDir)
	os.Exit(code)
}

// generateInt32Data 生成 Int32 测试数据
func generateInt32Data(n int, pattern string) []int32 {
	data := make([]int32, n)
	switch pattern {
	case "random":
		for i := range data {
			data[i] = rand.Int31()
		}
	case "sequential":
		for i := range data {
			data[i] = int32(i)
		}
	case "repeated":
		for i := range data {
			data[i] = int32(i % 10)
		}
	case "sorted":
		for i := range data {
			data[i] = int32(i * 10)
		}
	}
	return data
}

// generateFloat32Data 生成 Float32 测试数据
func generateFloat32Data(n int, pattern string) []float32 {
	data := make([]float32, n)
	switch pattern {
	case "random":
		for i := range data {
			data[i] = rand.Float32()
		}
	case "small_range":
		for i := range data {
			data[i] = float32(i%100) * 0.001
		}
	case "sequential":
		for i := range data {
			data[i] = float32(i) * 0.1
		}
	}
	return data
}

// generateVectorData 生成向量测试数据
func generateVectorData(numVectors, dim int) [][]float32 {
	vectors := make([][]float32, numVectors)
	for i := range vectors {
		vec := make([]float32, dim)
		for j := range vec {
			vec[j] = rand.Float32()
		}
		vectors[i] = vec
	}
	return vectors
}
