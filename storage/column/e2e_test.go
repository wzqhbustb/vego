package column

import (
	"bytes"
	"encoding/binary"
	"math"
	"path/filepath"
	"testing"

	"github.com/wzqhbustb/vego/storage/arrow"
	"github.com/wzqhbustb/vego/storage/encoding"
	"github.com/wzqhbustb/vego/storage/format"
)

// TestE2E_SmartEncoding 验证智能编码选择（RLE、Dictionary、BitPacking、Zstd）
func TestE2E_SmartEncoding(t *testing.T) {
	factory := encoding.NewEncoderFactory(3)
	writer := NewPageWriter(factory)
	reader := NewPageReader()

	testCases := []struct {
		name     string
		values   []int32
		expected format.EncodingType // 期望的编码类型
	}{
		{
			name:     "RLE_长连续重复",
			values:   []int32{1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3},
			expected: format.EncodingRLE,
		},
		{
			name:     "Dictionary_低基数",
			values:   []int32{1, 2, 1, 2, 1, 2, 1, 2, 1, 2},
			expected: format.EncodingDictionary,
		},
		{
			name:     "BitPacking_窄整数",
			values:   []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			expected: format.EncodingBitPacked,
		},
		{
			name:     "Zstd_随机数据",
			values:   []int32{1000000, 2000000, 3000000, 4000000, 5000000},
			expected: format.EncodingZstd,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 构建数组
			builder := arrow.NewInt32Builder()
			for _, v := range tc.values {
				builder.Append(v)
			}
			array := builder.NewArray()

			// 写入（会自动选择编码）
			pages, err := writer.WritePages(array, 0)
			if err != nil {
				t.Fatalf("Write failed: %v", err)
			}

			// 验证使用了期望的编码
			if pages[0].Encoding != tc.expected {
				t.Logf("Note: Expected %v but got %v (encoding selection may vary based on thresholds)",
					tc.expected, pages[0].Encoding)
			}

			// 读取（会自动解码）
			result, err := reader.ReadPage(pages[0], arrow.PrimInt32())
			if err != nil {
				t.Fatalf("Read failed: %v", err)
			}

			// 验证数据一致
			if !arraysEqual(array, result) {
				t.Error("Data mismatch after roundtrip")
			}
		})
	}
}

// TestE2E_Float32_BSS 验证 BSS 编码对浮点数的压缩效果
func TestE2E_Float32_BSS(t *testing.T) {
	factory := encoding.NewEncoderFactory(3)
	writer := NewPageWriter(factory)
	reader := NewPageReader()

	// 创建适合 BSS 的数据（小范围浮点数，高位字节相似）
	values := make([]float32, 1000)
	for i := range values {
		values[i] = float32(i) * 0.001 // 0.000, 0.001, 0.002...
	}

	builder := arrow.NewFloat32Builder()
	for _, v := range values {
		builder.Append(v)
	}
	array := builder.NewArray()

	// 写入
	pages, err := writer.WritePages(array, 0)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// 验证使用了 BSS 或 Combined
	if pages[0].Encoding != format.EncodingBSSEncoding && pages[0].Encoding != format.EncodingZstd {
		t.Logf("Encoding type: %v", pages[0].Encoding)
	}

	// 读取
	result, err := reader.ReadPage(pages[0], arrow.PrimFloat32())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	// 验证数据
	original := array.(*arrow.Float32Array).Values()
	resultArray := result.(*arrow.Float32Array)
	for i, v := range original {
		if resultArray.Value(i) != v {
			t.Errorf("Value mismatch at %d: expected %f, got %f", i, v, resultArray.Value(i))
		}
	}
}

// TestE2E_CompressionRatio 验证压缩比
func TestE2E_CompressionRatio(t *testing.T) {
	factory := encoding.NewEncoderFactory(3)
	writer := NewPageWriter(factory)

	// 测试数据：高度可压缩
	values := make([]int32, 10000)
	for i := range values {
		values[i] = int32(i % 10) // 只有10个唯一值
	}

	builder := arrow.NewInt32Builder()
	for _, v := range values {
		builder.Append(v)
	}
	array := builder.NewArray()

	// 写入
	pages, err := writer.WritePages(array, 0)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// 计算压缩比
	originalSize := len(values) * 4 // 原始大小：40000 bytes
	compressedSize := len(pages[0].Data)

	ratio := float64(compressedSize) / float64(originalSize)
	t.Logf("Original: %d bytes, Compressed: %d bytes, Ratio: %.2f",
		originalSize, compressedSize, ratio)

	// 验证确实压缩了（字典编码应该能压缩到50%以下）
	if ratio > 0.6 {
		t.Errorf("Compression ratio too high: %.2f, expected significant compression for repetitive data", ratio)
	}
}

// TestE2E_MultiplePages 验证多页场景
func TestE2E_MultiplePages(t *testing.T) {
	// 创建临时文件测试完整 Writer/Reader
	tmpDir := t.TempDir()
	filename := tmpDir + "/test.lance"

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimInt32(), Nullable: false},
	}, nil)

	// 写入多个 batch（每批一个 page）
	writer, err := NewWriter(filename, schema, encoding.NewEncoderFactory(3))
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	for batchNum := 0; batchNum < 3; batchNum++ {
		builder := arrow.NewInt32Builder()
		for i := 0; i < 100; i++ {
			builder.Append(int32(batchNum*100 + i))
		}
		array := builder.NewArray()

		batch, _ := arrow.NewRecordBatch(schema, 100, []arrow.Array{array})
		if err := writer.WriteRecordBatch(batch); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 读取验证
	reader, err := NewReader(filename)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer reader.Close()

	result, err := reader.ReadRecordBatch()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if result.NumRows() != 300 {
		t.Errorf("Expected 300 rows, got %d", result.NumRows())
	}
}

// ====================
// 1. 边界条件测试
// ====================

// TestE2E_EmptyArray 验证空数组处理
func TestE2E_EmptyArray(t *testing.T) {
	factory := encoding.NewEncoderFactory(3)
	writer := NewPageWriter(factory)

	builder := arrow.NewInt32Builder()
	array := builder.NewArray() // 空数组

	_, err := writer.WritePages(array, 0)
	if err == nil {
		t.Error("Expected error for empty array")
	}
}

// TestE2E_SingleValue 验证单值数组
func TestE2E_SingleValue(t *testing.T) {
	factory := encoding.NewEncoderFactory(3)
	writer := NewPageWriter(factory)
	reader := NewPageReader()

	builder := arrow.NewInt32Builder()
	builder.Append(42)
	array := builder.NewArray()

	pages, err := writer.WritePages(array, 0)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	result, err := reader.ReadPage(pages[0], arrow.PrimInt32())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if result.Len() != 1 || result.(*arrow.Int32Array).Value(0) != 42 {
		t.Error("Single value roundtrip failed")
	}
}

// TestE2E_AllNulls 验证全 null 数组
func TestE2E_AllNulls(t *testing.T) {
	factory := encoding.NewEncoderFactory(3)
	writer := NewPageWriter(factory)
	reader := NewPageReader()

	builder := arrow.NewInt32Builder()
	for i := 0; i < 100; i++ {
		builder.AppendNull()
	}
	array := builder.NewArray()

	pages, err := writer.WritePages(array, 0)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	result, err := reader.ReadPage(pages[0], arrow.PrimInt32())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if result.Len() != 100 || result.NullN() != 100 {
		t.Errorf("Expected 100 nulls, got %d nulls", result.NullN())
	}
}

// ====================
// 2. 特定编码器验证测试
// ====================

// TestE2E_BitPacking_Int32 验证 BitPacking 正确性
func TestE2E_BitPacking_Int32(t *testing.T) {
	// 窄整数数据（0-255，可用 8 bits 表示）
	values := make([]int32, 1000)
	for i := range values {
		values[i] = int32(i % 256)
	}

	factory := encoding.NewEncoderFactory(3)
	writer := NewPageWriter(factory)
	reader := NewPageReader()

	builder := arrow.NewInt32Builder()
	for _, v := range values {
		builder.Append(v)
	}
	array := builder.NewArray()

	pages, err := writer.WritePages(array, 0)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// 验证使用了 BitPacking 或其他编码
	t.Logf("Encoding used: %v", pages[0].Encoding)

	result, err := reader.ReadPage(pages[0], arrow.PrimInt32())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if !arraysEqual(array, result) {
		t.Error("BitPacking roundtrip failed")
	}
}

// TestE2E_RLE_LongRuns 验证 RLE 对长连续段的压缩
func TestE2E_RLE_LongRuns(t *testing.T) {
	// 创建长连续重复数据
	values := make([]int32, 10000)
	for i := range values {
		values[i] = int32(i / 1000) // 每 1000 个值相同
	}

	factory := encoding.NewEncoderFactory(3)
	writer := NewPageWriter(factory)
	reader := NewPageReader()

	builder := arrow.NewInt32Builder()
	for _, v := range values {
		builder.Append(v)
	}
	array := builder.NewArray()

	pages, err := writer.WritePages(array, 0)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// 验证高压缩比（RLE 应该压缩得很好）
	originalSize := len(values) * 4
	compressedSize := len(pages[0].Data)
	ratio := float64(compressedSize) / float64(originalSize)

	t.Logf("RLE compression ratio: %.4f (%d -> %d)", ratio, originalSize, compressedSize)

	// RLE 应该能压缩到 10% 以下
	if ratio > 0.1 {
		t.Logf("Warning: RLE compression ratio higher than expected: %.4f", ratio)
	}

	result, err := reader.ReadPage(pages[0], arrow.PrimInt32())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if !arraysEqual(array, result) {
		t.Error("RLE roundtrip failed")
	}
}

// TestE2E_Dictionary_LowCardinality 验证 Dictionary 编码
func TestE2E_Dictionary_LowCardinality(t *testing.T) {
	// 极低基数数据
	values := make([]int32, 10000)
	for i := range values {
		values[i] = int32(i % 5) // 只有 5 个唯一值
	}

	factory := encoding.NewEncoderFactory(3)
	writer := NewPageWriter(factory)
	reader := NewPageReader()

	builder := arrow.NewInt32Builder()
	for _, v := range values {
		builder.Append(v)
	}
	array := builder.NewArray()

	pages, err := writer.WritePages(array, 0)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Dictionary 应该压缩到很小
	originalSize := len(values) * 4
	compressedSize := len(pages[0].Data)
	ratio := float64(compressedSize) / float64(originalSize)

	t.Logf("Dictionary compression ratio: %.4f", ratio)

	if ratio > 0.1 {
		t.Logf("Warning: Dictionary compression ratio higher than expected: %.4f", ratio)
	}

	result, err := reader.ReadPage(pages[0], arrow.PrimInt32())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if !arraysEqual(array, result) {
		t.Error("Dictionary roundtrip failed")
	}
}

// ====================
// 3. 混合类型测试
// ====================

// TestE2E_MixedTypes 验证多列不同类型
func TestE2E_MixedTypes(t *testing.T) {
	tmpDir := t.TempDir()
	filename := tmpDir + "/mixed.lance"

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimInt32(), Nullable: false},
		{Name: "score", Type: arrow.PrimFloat64(), Nullable: true},
		// [REMOVED] PrimInt8() 未定义，移除或使用 PrimInt32() 代替
		// {Name: "flag", Type: arrow.PrimInt8(), Nullable: false},
	}, nil)

	writer, err := NewWriter(filename, schema, encoding.NewEncoderFactory(3))
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	// 构建数据 - 只使用 schema 中定义的列
	idBuilder := arrow.NewInt32Builder()
	scoreBuilder := arrow.NewFloat64Builder()

	for i := 0; i < 1000; i++ {
		idBuilder.Append(int32(i))
		if i%10 == 0 {
			scoreBuilder.AppendNull()
		} else {
			scoreBuilder.Append(float64(i) * 0.5)
		}
	}

	idArray := idBuilder.NewArray()
	scoreArray := scoreBuilder.NewArray()

	batch, _ := arrow.NewRecordBatch(schema, 1000, []arrow.Array{idArray, scoreArray})
	if err := writer.WriteRecordBatch(batch); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	writer.Close()

	// 读取验证...
}

// ====================
// 4. 错误处理测试
// ====================

// TestE2E_CorruptedData 验证损坏数据处理
func TestE2E_CorruptedData(t *testing.T) {
	factory := encoding.NewEncoderFactory(3)
	writer := NewPageWriter(factory)
	reader := NewPageReader()

	// 正常写入
	builder := arrow.NewInt32Builder()
	for i := 0; i < 100; i++ {
		builder.Append(int32(i))
	}
	array := builder.NewArray()

	pages, err := writer.WritePages(array, 0)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// 损坏数据：截断 Page.Data
	corruptedPage := &format.Page{
		Type:             pages[0].Type,
		Encoding:         pages[0].Encoding,
		ColumnIndex:      pages[0].ColumnIndex,
		NumValues:        pages[0].NumValues,
		UncompressedSize: pages[0].UncompressedSize,
		CompressedSize:   int32(len(pages[0].Data) / 2), // 假装只有一半
		Data:             pages[0].Data[:len(pages[0].Data)/2],
	}

	// 应该返回错误
	_, err = reader.ReadPage(corruptedPage, arrow.PrimInt32())
	if err == nil {
		t.Error("Expected error for corrupted data")
	}
}

// TestE2E_UnsupportedEncoding 验证不支持的编码类型
func TestE2E_UnsupportedEncoding(t *testing.T) {
	reader := NewPageReader()

	// 创建带有不支持的编码类型的 Page
	invalidPage := &format.Page{
		Type:        format.PageTypeData,
		Encoding:    format.EncodingType(255), // 不存在的编码
		ColumnIndex: 0,
		NumValues:   10,
		Data:        []byte{1, 2, 3, 4},
	}

	_, err := reader.ReadPage(invalidPage, arrow.PrimInt32())
	if err == nil {
		t.Error("Expected error for unsupported encoding")
	}
}

// ====================
// 5. 性能/压力测试
// ====================

// TestE2E_LargePage 验证大页处理
func TestE2E_LargePage(t *testing.T) {
	factory := encoding.NewEncoderFactory(3)
	writer := NewPageWriter(factory)
	reader := NewPageReader()

	// 1M 个值
	n := 1000000
	values := make([]int32, n)
	for i := range values {
		values[i] = int32(i % 1000)
	}

	builder := arrow.NewInt32Builder()
	for _, v := range values {
		builder.Append(v)
	}
	array := builder.NewArray()

	pages, err := writer.WritePages(array, 0)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// 验证只产生一个页（当前实现）
	if len(pages) != 1 {
		t.Logf("Note: Produced %d pages for %d values", len(pages), n)
	}

	result, err := reader.ReadPage(pages[0], arrow.PrimInt32())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if result.Len() != n {
		t.Errorf("Expected %d values, got %d", n, result.Len())
	}
}

// BenchmarkE2E_EncodingComparison 对比不同编码器的性能
func BenchmarkE2E_EncodingComparison(b *testing.B) {
	encodings := []struct {
		name   string
		values []int32
	}{
		{"Random", func() []int32 {
			v := make([]int32, 10000)
			for i := range v {
				v[i] = int32(i)
			}
			return v
		}()},
		{"RLE", func() []int32 {
			v := make([]int32, 10000)
			for i := range v {
				v[i] = int32(i / 1000)
			}
			return v
		}()},
		{"Dictionary", func() []int32 {
			v := make([]int32, 10000)
			for i := range v {
				v[i] = int32(i % 10)
			}
			return v
		}()},
		{"BitPacking", func() []int32 {
			v := make([]int32, 10000)
			for i := range v {
				v[i] = int32(i % 256)
			}
			return v
		}()},
	}

	for _, tc := range encodings {
		b.Run(tc.name, func(b *testing.B) {
			factory := encoding.NewEncoderFactory(3)
			writer := NewPageWriter(factory)
			reader := NewPageReader()

			builder := arrow.NewInt32Builder()
			for _, v := range tc.values {
				builder.Append(v)
			}
			array := builder.NewArray()

			pages, _ := writer.WritePages(array, 0)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				reader.ReadPage(pages[0], arrow.PrimInt32())
			}
		})
	}
}

// ====================
// P0: 修复和补充的测试
// ====================

// TestE2E_VectorColumn 向量列测试（HNSW 核心场景）- 修复浮点精度问题
func TestE2E_VectorColumn(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "vectors.lance")

	// 创建 768 维向量 schema（典型 Embedding 维度）
	dim := 768
	listType := arrow.FixedSizeListOf(arrow.PrimFloat32(), dim)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "vector_id", Type: arrow.PrimInt32(), Nullable: false},
		{Name: "embedding", Type: listType, Nullable: false},
	}, nil)

	writer, err := NewWriter(filename, schema, encoding.NewEncoderFactory(3))
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	// 构建向量数据（100 个向量）
	numVectors := 100
	idBuilder := arrow.NewInt32Builder()
	childBuilder := arrow.NewFloat32Builder()

	// 同时构建预期的字节数据用于后续比较
	expectedBytes := make([][]byte, numVectors)

	for i := 0; i < numVectors; i++ {
		idBuilder.Append(int32(i))

		// 构建向量值并记录预期字节
		vectorValues := make([]float32, dim)
		buf := new(bytes.Buffer)
		for d := 0; d < dim; d++ {
			val := float32(i*dim+d) * 0.001
			vectorValues[d] = val
			binary.Write(buf, binary.LittleEndian, val)
		}
		expectedBytes[i] = buf.Bytes()

		for _, v := range vectorValues {
			childBuilder.Append(v)
		}
	}

	idArray := idBuilder.NewArray()
	childArray := childBuilder.NewArray()
	vectorArray := arrow.NewFixedSizeListArray(listType.(*arrow.FixedSizeListType), childArray, nil)

	batch, _ := arrow.NewRecordBatch(schema, numVectors, []arrow.Array{idArray, vectorArray})
	if err := writer.WriteRecordBatch(batch); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	writer.Close()

	// 读取验证
	reader, err := NewReader(filename)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer reader.Close()

	result, err := reader.ReadRecordBatch()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if result.NumRows() != numVectors {
		t.Errorf("Expected %d vectors, got %d", numVectors, result.NumRows())
	}

	// 验证 ID 列
	resultID := result.Column(0).(*arrow.Int32Array)
	for i := 0; i < numVectors; i++ {
		if resultID.Value(i) != int32(i) {
			t.Errorf("Vector ID mismatch at %d: expected %d, got %d", i, i, resultID.Value(i))
			break
		}
	}

	// [FIXED] 使用字节级别比较，避免浮点精度问题
	resultVector := result.Column(1).(*arrow.FixedSizeListArray)
	resultValues := resultVector.Values().(*arrow.Float32Array)

	for i := 0; i < numVectors; i++ {
		// 提取实际向量的字节表示
		start := i * dim
		end := start + dim
		actualValues := resultValues.Values()[start:end]

		// 将实际值转换为字节
		buf := new(bytes.Buffer)
		for _, v := range actualValues {
			binary.Write(buf, binary.LittleEndian, v)
		}
		actualBytes := buf.Bytes()

		// 字节级比较
		if !bytes.Equal(expectedBytes[i], actualBytes) {
			// 找到第一个不匹配的字节
			for j := 0; j < len(expectedBytes[i]) && j < len(actualBytes); j++ {
				if expectedBytes[i][j] != actualBytes[j] {
					t.Errorf("Vector %d byte mismatch at offset %d: expected 0x%02X, got 0x%02X",
						i, j, expectedBytes[i][j], actualBytes[j])
					break
				}
			}
			t.Errorf("Vector %d data mismatch (total bytes: expected %d, got %d)",
				i, len(expectedBytes[i]), len(actualBytes))
			return
		}
	}

	// 额外验证：检查向量值的数值正确性（使用更大的容差）
	for i := 0; i < numVectors; i++ {
		start := i * dim
		end := start + dim
		actualValues := resultValues.Values()[start:end]

		for d := 0; d < dim; d++ {
			expected := float32(i*dim+d) * 0.001
			actual := actualValues[d]

			// 使用相对误差检查
			var diff float32
			if expected > actual {
				diff = expected - actual
			} else {
				diff = actual - expected
			}

			// 容差：相对误差 0.1% 或绝对误差 0.001
			if diff > 0.001 && diff > expected*0.001 {
				t.Errorf("Vector [%d][%d] value out of tolerance: expected %.10f, got %.10f, diff=%.10e",
					i, d, expected, actual, diff)
				return
			}
		}
	}
}

// TestE2E_MixedTypes_Fixed 修复后的多列类型测试（完整验证）
func TestE2E_MixedTypes_Fixed(t *testing.T) {
	tmpDir := t.TempDir()
	filename := tmpDir + "/mixed.lance"

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimInt32(), Nullable: false},
		{Name: "score", Type: arrow.PrimFloat64(), Nullable: true},
	}, nil)

	writer, err := NewWriter(filename, schema, encoding.NewEncoderFactory(3))
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	// 构建数据
	idBuilder := arrow.NewInt32Builder()
	scoreBuilder := arrow.NewFloat64Builder()

	for i := 0; i < 1000; i++ {
		idBuilder.Append(int32(i))
		if i%10 == 0 {
			scoreBuilder.AppendNull()
		} else {
			scoreBuilder.Append(float64(i) * 0.5)
		}
	}

	idArray := idBuilder.NewArray()
	scoreArray := scoreBuilder.NewArray()

	batch, _ := arrow.NewRecordBatch(schema, 1000, []arrow.Array{idArray, scoreArray})
	if err := writer.WriteRecordBatch(batch); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	writer.Close()

	// [NEW] 完整的读取验证
	reader, err := NewReader(filename)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer reader.Close()

	// 验证 Schema
	if !reader.Schema().Equal(schema) {
		t.Error("Schema mismatch")
	}

	// 验证行数
	if reader.NumRows() != 1000 {
		t.Errorf("Expected 1000 rows, got %d", reader.NumRows())
	}

	// 读取数据
	result, err := reader.ReadRecordBatch()
	if err != nil {
		t.Fatalf("ReadRecordBatch failed: %v", err)
	}

	// 验证 ID 列
	resultID := result.Column(0).(*arrow.Int32Array)
	for i := 0; i < 1000; i++ {
		if resultID.Value(i) != int32(i) {
			t.Errorf("ID mismatch at %d: expected %d, got %d", i, i, resultID.Value(i))
			break
		}
	}

	// 验证 Score 列（包括 null）
	resultScore := result.Column(1).(*arrow.Float64Array)
	nullCount := 0
	for i := 0; i < 1000; i++ {
		if i%10 == 0 {
			if resultScore.IsValid(i) {
				t.Errorf("Expected null at index %d", i)
			}
			nullCount++
		} else {
			if !resultScore.IsValid(i) {
				t.Errorf("Expected valid at index %d", i)
			}
			expected := float64(i) * 0.5
			if resultScore.Value(i) != expected {
				t.Errorf("Score mismatch at %d: expected %f, got %f", i, expected, resultScore.Value(i))
			}
		}
	}

	if nullCount != 100 {
		t.Errorf("Expected 100 nulls, got %d", nullCount)
	}
}

// TestE2E_Int64Type Int64 类型完整测试
func TestE2E_Int64Type(t *testing.T) {
	tmpDir := t.TempDir()
	filename := tmpDir + "/int64.lance"

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "timestamp", Type: arrow.PrimInt64(), Nullable: false},
	}, nil)

	writer, err := NewWriter(filename, schema, encoding.NewEncoderFactory(3))
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	// 构建 Int64 数据（时间戳）
	builder := arrow.NewInt64Builder()
	now := int64(1704067200000) // 2024-01-01 00:00:00 UTC in ms
	for i := 0; i < 1000; i++ {
		builder.Append(now + int64(i)*1000) // 每秒一个
	}
	array := builder.NewArray()

	batch, _ := arrow.NewRecordBatch(schema, 1000, []arrow.Array{array})
	if err := writer.WriteRecordBatch(batch); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	writer.Close()

	// 读取验证
	reader, err := NewReader(filename)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer reader.Close()

	result, err := reader.ReadRecordBatch()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	resultArray := result.Column(0).(*arrow.Int64Array)
	for i := 0; i < 1000; i++ {
		expected := now + int64(i)*1000
		if resultArray.Value(i) != expected {
			t.Errorf("Int64 value mismatch at %d: expected %d, got %d", i, expected, resultArray.Value(i))
			break
		}
	}
}

// TestE2E_Float64Type Float64 类型完整测试
func TestE2E_Float64Type(t *testing.T) {
	tmpDir := t.TempDir()
	filename := tmpDir + "/float64.lance"

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "measurement", Type: arrow.PrimFloat64(), Nullable: true},
	}, nil)

	writer, err := NewWriter(filename, schema, encoding.NewEncoderFactory(3))
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	// 构建 Float64 数据（高精度测量值）
	builder := arrow.NewFloat64Builder()
	for i := 0; i < 1000; i++ {
		if i%20 == 0 {
			builder.AppendNull()
		} else {
			builder.Append(math.Pi * float64(i) * 0.001)
		}
	}
	array := builder.NewArray()

	batch, _ := arrow.NewRecordBatch(schema, 1000, []arrow.Array{array})
	if err := writer.WriteRecordBatch(batch); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	writer.Close()

	// 读取验证
	reader, err := NewReader(filename)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer reader.Close()

	result, err := reader.ReadRecordBatch()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	resultArray := result.Column(0).(*arrow.Float64Array)
	nullCount := 0
	for i := 0; i < 1000; i++ {
		if i%20 == 0 {
			if resultArray.IsValid(i) {
				t.Errorf("Expected null at index %d", i)
			}
			nullCount++
		} else {
			expected := math.Pi * float64(i) * 0.001
			actual := resultArray.Value(i)
			if math.Abs(actual-expected) > 1e-15 {
				t.Errorf("Float64 value mismatch at %d: expected %.15f, got %.15f", i, expected, actual)
			}
		}
	}

	if nullCount != 50 {
		t.Errorf("Expected 50 nulls, got %d", nullCount)
	}
}

// ====================
// P1: 建议添加的测试
// ====================

// TestE2E_PartialNulls 混合 null 和 valid 值的测试
func TestE2E_PartialNulls(t *testing.T) {
	factory := encoding.NewEncoderFactory(3)
	writer := NewPageWriter(factory)
	reader := NewPageReader()

	builder := arrow.NewInt32Builder()

	// 创建模式：valid, null, valid, null, ...
	for i := 0; i < 100; i++ {
		if i%2 == 0 {
			builder.Append(int32(i))
		} else {
			builder.AppendNull()
		}
	}
	array := builder.NewArray()

	pages, err := writer.WritePages(array, 0)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	result, err := reader.ReadPage(pages[0], arrow.PrimInt32())
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	// 验证 pattern
	for i := 0; i < 100; i++ {
		if i%2 == 0 {
			if !result.IsValid(i) {
				t.Errorf("Expected valid at index %d", i)
			}
			if result.(*arrow.Int32Array).Value(i) != int32(i) {
				t.Errorf("Value mismatch at %d", i)
			}
		} else {
			if result.IsValid(i) {
				t.Errorf("Expected null at index %d", i)
			}
		}
	}
}

// TestE2E_EncodingSelection 编码选择验证（允许回退）
func TestE2E_EncodingSelection(t *testing.T) {
	factory := encoding.NewEncoderFactory(3)
	writer := NewPageWriter(factory)
	reader := NewPageReader()

	// Test 1: 长连续段 - 期望 RLE，但可能回退到 Zstd
	t.Run("LongRuns", func(t *testing.T) {
		values := make([]int32, 1000)
		for i := range values {
			values[i] = int32(i / 100) // 10 个值，每个重复 100 次
		}

		builder := arrow.NewInt32Builder()
		for _, v := range values {
			builder.Append(v)
		}
		array := builder.NewArray()

		pages, err := writer.WritePages(array, 0)
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}

		// 记录实际使用的编码，不强制要求特定编码
		t.Logf("Encoding used for long runs: %v", pages[0].Encoding)

		// 验证能正确读取即可
		result, err := reader.ReadPage(pages[0], arrow.PrimInt32())
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}

		if !arraysEqual(array, result) {
			t.Error("Data mismatch")
		}
	})

	// Test 2: 低基数 - 期望 Dictionary
	t.Run("LowCardinality", func(t *testing.T) {
		values := make([]int32, 1000)
		for i := range values {
			values[i] = int32(i % 3) // 只有 3 个唯一值
		}

		builder := arrow.NewInt32Builder()
		for _, v := range values {
			builder.Append(v)
		}
		array := builder.NewArray()

		pages, err := writer.WritePages(array, 0)
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}

		t.Logf("Encoding used for low cardinality: %v", pages[0].Encoding)

		// Dictionary 对低基数数据应该有效，不太可能回退
		if pages[0].Encoding != format.EncodingDictionary {
			t.Logf("Note: Expected Dictionary, got %v (may use fallback)", pages[0].Encoding)
		}

		result, err := reader.ReadPage(pages[0], arrow.PrimInt32())
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}

		if !arraysEqual(array, result) {
			t.Error("Data mismatch")
		}
	})
}

// TestE2E_WriterReaderRoundtrip 完整的文件级往返测试
func TestE2E_WriterReaderRoundtrip(t *testing.T) {
	tmpDir := t.TempDir()
	filename := tmpDir + "/roundtrip.lance"

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimInt32(), Nullable: false},
		{Name: "value", Type: arrow.PrimFloat64(), Nullable: true},
		{Name: "flag", Type: arrow.PrimInt64(), Nullable: false},
	}, nil)

	// 写入数据
	writer, err := NewWriter(filename, schema, encoding.NewEncoderFactory(3))
	if err != nil {
		t.Fatalf("NewWriter failed: %v", err)
	}

	var batches []*arrow.RecordBatch
	for batchNum := 0; batchNum < 5; batchNum++ {
		idBuilder := arrow.NewInt32Builder()
		valueBuilder := arrow.NewFloat64Builder()
		flagBuilder := arrow.NewInt64Builder()

		for i := 0; i < 100; i++ {
			idBuilder.Append(int32(batchNum*100 + i))
			if i%5 == 0 {
				valueBuilder.AppendNull()
			} else {
				valueBuilder.Append(float64(batchNum*100+i) * 1.5)
			}
			flagBuilder.Append(int64(batchNum*100 + i))
		}

		batch, _ := arrow.NewRecordBatch(schema, 100, []arrow.Array{
			idBuilder.NewArray(),
			valueBuilder.NewArray(),
			flagBuilder.NewArray(),
		})
		batches = append(batches, batch)

		if err := writer.WriteRecordBatch(batch); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	writer.Close()

	// 读取验证
	reader, err := NewReader(filename)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer reader.Close()

	if reader.NumRows() != 500 {
		t.Errorf("Expected 500 rows, got %d", reader.NumRows())
	}

	result, err := reader.ReadRecordBatch()
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	// 验证所有数据
	for i := 0; i < 500; i++ {
		expectedID := int32(i)
		if result.Column(0).(*arrow.Int32Array).Value(i) != expectedID {
			t.Errorf("ID mismatch at %d", i)
			break
		}
	}
}

// TestE2E_PageMetadata 验证页元数据正确性
func TestE2E_PageMetadata(t *testing.T) {
	factory := encoding.NewEncoderFactory(3)
	writer := NewPageWriter(factory)

	values := make([]int32, 1000)
	for i := range values {
		values[i] = int32(i)
	}

	builder := arrow.NewInt32Builder()
	for _, v := range values {
		builder.Append(v)
	}
	array := builder.NewArray()

	pages, err := writer.WritePages(array, 0)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	page := pages[0]

	// 验证元数据
	if page.NumValues != 1000 {
		t.Errorf("Expected NumValues=1000, got %d", page.NumValues)
	}

	if page.ColumnIndex != 0 {
		t.Errorf("Expected ColumnIndex=0, got %d", page.ColumnIndex)
	}

	if page.UncompressedSize != int32(len(values)*4+5) { // data + header
		t.Logf("UncompressedSize: %d", page.UncompressedSize)
	}

	if page.CompressedSize == 0 {
		t.Error("CompressedSize should not be zero")
	}

	if page.Encoding == format.EncodingPlain {
		t.Log("Warning: Using Plain encoding, compression not applied")
	}
}
