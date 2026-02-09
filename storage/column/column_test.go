package column

import (
	"encoding/binary"
	"github.com/wzqhbustb/vego/storage/arrow"
	"github.com/wzqhbustb/vego/storage/encoding"
	"os"
	"path/filepath"
	"testing"
)

// ====================
// Test 1: calculateNumPages
// ====================

func TestCalculateNumPages(t *testing.T) {
	tests := []struct {
		name        string
		arrayLen    int
		pageSize    int32
		bytesPerVal int32 // 模拟不同数据类型的每值字节数
		expected    int32
		useInt64    bool // 是否使用Int64Builder
	}{
		{
			name:        "小数组_小于一页",
			arrayLen:    100,
			pageSize:    1024 * 1024, // 1MB
			bytesPerVal: 4,           // Int32
			expected:    1,
		},
		{
			name:        "大数组_需要多页",
			arrayLen:    1000000,     // 100万个Int32 = 4MB
			pageSize:    1024 * 1024, // 1MB per page
			bytesPerVal: 4,
			expected:    4,
		},
		{
			name:        "边界值_刚好整除",
			arrayLen:    262144, // 262144 * 4 = 1MB exactly
			pageSize:    1024 * 1024,
			bytesPerVal: 4,
			expected:    1,
		},
		{
			name:        "边界值_多一页",
			arrayLen:    262145, // 1 byte over 1MB
			pageSize:    1024 * 1024,
			bytesPerVal: 4,
			expected:    2,
		},
		{
			name:        "超大数组_Int64类型",
			arrayLen:    1000000, // 100万个Int64 = 8MB
			pageSize:    1024 * 1024,
			bytesPerVal: 8,
			expected:    8,
			useInt64:    true, // 标记使用Int64Builder
		},
		{
			name:        "空数组边界",
			arrayLen:    1,
			pageSize:    1024,
			bytesPerVal: 4,
			expected:    1,
		},
		{
			name:        "极小pageSize",
			arrayLen:    100,
			pageSize:    100, // 很小的page size
			bytesPerVal: 4,
			expected:    4, // 400 bytes / 100 = 4 pages
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 构建测试数组
			var array arrow.Array
			if tt.useInt64 {
				builder := arrow.NewInt64Builder()
				for i := 0; i < tt.arrayLen; i++ {
					builder.Append(int64(i))
				}
				array = builder.NewArray()
			} else {
				builder := arrow.NewInt32Builder()
				for i := 0; i < tt.arrayLen; i++ {
					builder.Append(int32(i))
				}
				array = builder.NewArray()
			}

			// 调用被测函数
			result := calculateNumPages(array, tt.pageSize)

			if result != tt.expected {
				t.Errorf("calculateNumPages() = %v, expected %v (arrayLen=%d, pageSize=%d)",
					result, tt.expected, tt.arrayLen, tt.pageSize)
			}
		})
	}
}

// ====================
// Test 2: splitArrayIntoRanges
// ====================

func TestSplitArrayIntoRanges(t *testing.T) {
	tests := []struct {
		name           string
		arrayLen       int
		pageSize       int32
		bytesPerValue  int32
		expectedRanges []struct{ Start, End int }
	}{
		{
			name:          "小数组_单页",
			arrayLen:      100,
			pageSize:      1024,
			bytesPerValue: 4,
			expectedRanges: []struct{ Start, End int }{
				{0, 100},
			},
		},
		{
			name:          "大数组_多页分割",
			arrayLen:      1000,
			pageSize:      1024, // 每页约256个Int32
			bytesPerValue: 4,
			expectedRanges: []struct{ Start, End int }{
				{0, 256},
				{256, 512},
				{512, 768},
				{768, 1000},
			},
		},
		{
			name:          "边界_刚好整除",
			arrayLen:      256, // 256 * 4 = 1024 = pageSize
			pageSize:      1024,
			bytesPerValue: 4,
			expectedRanges: []struct{ Start, End int }{
				{0, 256},
			},
		},
		{
			name:          "边界_多一个元素",
			arrayLen:      257,
			pageSize:      1024,
			bytesPerValue: 4,
			expectedRanges: []struct{ Start, End int }{
				{0, 256},
				{256, 257},
			},
		},
		{
			name:          "极大pageSize_单页",
			arrayLen:      100,
			pageSize:      1024 * 1024, // 1MB
			bytesPerValue: 4,
			expectedRanges: []struct{ Start, End int }{
				{0, 100},
			},
		},
		{
			name:          "极小pageSize_每元素一页",
			arrayLen:      5,
			pageSize:      4, // 只能容纳1个Int32
			bytesPerValue: 4,
			expectedRanges: []struct{ Start, End int }{
				{0, 1},
				{1, 2},
				{2, 3},
				{3, 4},
				{4, 5},
			},
		},
		{
			name:          "零保护_pageSize小于bytesPerValue",
			arrayLen:      10,
			pageSize:      2, // 小于4，应该保护为每页1个
			bytesPerValue: 4,
			expectedRanges: []struct{ Start, End int }{
				{0, 1},
				{1, 2},
				{2, 3},
				{3, 4},
				{4, 5},
				{5, 6},
				{6, 7},
				{7, 8},
				{8, 9},
				{9, 10},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ranges := splitArrayIntoRanges(tt.arrayLen, tt.pageSize, tt.bytesPerValue)

			if len(ranges) != len(tt.expectedRanges) {
				t.Errorf("Expected %d ranges, got %d: %v",
					len(tt.expectedRanges), len(ranges), ranges)
				return
			}

			for i, r := range ranges {
				if r.Start != tt.expectedRanges[i].Start || r.End != tt.expectedRanges[i].End {
					t.Errorf("Range %d: expected {%d,%d}, got {%d,%d}",
						i, tt.expectedRanges[i].Start, tt.expectedRanges[i].End,
						r.Start, r.End)
				}
			}
		})
	}
}

// ====================
// Test 3: mergeFixedSizeListArrays (HNSW核心)
// ====================

func TestMergeFixedSizeListArrays(t *testing.T) {
	// 创建测试用的FSL类型 (768维向量，类似Embedding)
	dim := 768
	listType := arrow.FixedSizeListOf(arrow.PrimFloat32(), dim)

	tests := []struct {
		name          string
		numArrays     int
		vectorsPerArr int
		withNulls     bool
	}{
		{
			name:          "单个数组_无null",
			numArrays:     1,
			vectorsPerArr: 10,
			withNulls:     false,
		},
		{
			name:          "多个数组_无null",
			numArrays:     3,
			vectorsPerArr: 5,
			withNulls:     false,
		},
		{
			name:          "多个数组_含null",
			numArrays:     3,
			vectorsPerArr: 5,
			withNulls:     true,
		},
		{
			name:          "大量小数组",
			numArrays:     10,
			vectorsPerArr: 2,
			withNulls:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 创建Reader实例 (用于调用mergeFixedSizeListArrays)
			reader := &Reader{}

			// 构建多个FSL数组
			var arrays []arrow.Array
			expectedNullCount := 0

			for arrIdx := 0; arrIdx < tt.numArrays; arrIdx++ {
				// 使用FixedSizeListBuilder创建数组
				builder := arrow.NewFixedSizeListBuilder(listType.(*arrow.FixedSizeListType))

				for vecIdx := 0; vecIdx < tt.vectorsPerArr; vecIdx++ {
					if tt.withNulls && vecIdx%3 == 0 {
						// 每第3个向量为null
						builder.AppendNull()
						expectedNullCount++
					} else {
						// 创建向量值
						values := make([]float32, dim)
						for d := 0; d < dim; d++ {
							values[d] = float32(arrIdx*10000 + vecIdx*100 + d)
						}
						builder.AppendValues(values)
					}
				}

				fslArray := builder.NewArray().(*arrow.FixedSizeListArray)
				arrays = append(arrays, fslArray)
			}

			// 调用被测函数
			merged, err := reader.mergeFixedSizeListArrays(arrays, listType.(*arrow.FixedSizeListType))
			if err != nil {
				t.Fatalf("mergeFixedSizeListArrays failed: %v", err)
			}

			// 验证结果
			mergedFsl := merged.(*arrow.FixedSizeListArray)
			expectedLen := tt.numArrays * tt.vectorsPerArr
			if mergedFsl.Len() != expectedLen {
				t.Errorf("Expected length %d, got %d", expectedLen, mergedFsl.Len())
			}

			// 验证null count
			if tt.withNulls {
				if mergedFsl.NullN() != expectedNullCount {
					t.Errorf("Expected %d nulls, got %d", expectedNullCount, mergedFsl.NullN())
				}
			} else {
				if mergedFsl.NullN() != 0 {
					t.Errorf("Expected 0 nulls, got %d", mergedFsl.NullN())
				}
			}

			// 验证向量值正确性 (只检查非null向量)
			values := mergedFsl.Values().(*arrow.Float32Array)
			for arrIdx := 0; arrIdx < tt.numArrays; arrIdx++ {
				for vecIdx := 0; vecIdx < tt.vectorsPerArr; vecIdx++ {
					if tt.withNulls && vecIdx%3 == 0 {
						continue // 跳过null向量
					}

					// 计算该向量在values数组中的起始位置
					vecOffset := (arrIdx*tt.vectorsPerArr + vecIdx) * dim

					for d := 0; d < dim; d++ {
						expectedVal := float32(arrIdx*10000 + vecIdx*100 + d)
						actualVal := values.Value(vecOffset + d)
						if actualVal != expectedVal {
							t.Errorf("Value mismatch at array %d, vector %d, dim %d: expected %f, got %f",
								arrIdx, vecIdx, d, expectedVal, actualVal)
							break
						}
					}
				}
			}
		})
	}
}

// ====================
// Test 4: EstimatePageSize
// ====================

func TestEstimatePageSize(t *testing.T) {
	factory := encoding.NewEncoderFactory(3)
	writer := NewPageWriter(factory)

	tests := []struct {
		name        string
		buildArray  func() arrow.Array
		expectError bool
		description string
	}{
		{
			name: "Int32数组_无null",
			buildArray: func() arrow.Array {
				builder := arrow.NewInt32Builder()
				for i := 0; i < 1000; i++ {
					builder.Append(int32(i))
				}
				return builder.NewArray()
			},
			expectError: false,
			description: "基础Int32数组",
		},
		{
			name: "Int32数组_有null_应该回退到Zstd",
			buildArray: func() arrow.Array {
				builder := arrow.NewInt32Builder()
				for i := 0; i < 1000; i++ {
					if i%5 == 0 {
						builder.AppendNull()
					} else {
						builder.Append(int32(i))
					}
				}
				return builder.NewArray()
			},
			expectError: false,
			description: "含null的Int32",
		},
		{
			name: "Int64数组",
			buildArray: func() arrow.Array {
				builder := arrow.NewInt64Builder()
				for i := 0; i < 500; i++ {
					builder.Append(int64(i * 1000000))
				}
				return builder.NewArray()
			},
			expectError: false,
			description: "Int64时间戳",
		},
		{
			name: "Float32数组",
			buildArray: func() arrow.Array {
				builder := arrow.NewFloat32Builder()
				for i := 0; i < 1000; i++ {
					builder.Append(float32(i) * 0.5)
				}
				return builder.NewArray()
			},
			expectError: false,
			description: "Float32数组",
		},
		{
			name: "Float64数组",
			buildArray: func() arrow.Array {
				builder := arrow.NewFloat64Builder()
				for i := 0; i < 500; i++ {
					builder.Append(float64(i) * 1.5)
				}
				return builder.NewArray()
			},
			expectError: false,
			description: "Float64数组",
		},
		{
			name: "FSL向量数组_强制Zstd",
			buildArray: func() arrow.Array {
				dim := 768
				listType := arrow.FixedSizeListOf(arrow.PrimFloat32(), dim)
				childBuilder := arrow.NewFloat32Builder()

				for i := 0; i < 10; i++ { // 10个向量
					for d := 0; d < dim; d++ {
						childBuilder.Append(float32(i*dim + d))
					}
				}
				childArray := childBuilder.NewArray()
				return arrow.NewFixedSizeListArray(
					listType.(*arrow.FixedSizeListType),
					childArray,
					nil,
				)
			},
			expectError: false,
			description: "768维向量",
		},
		{
			name: "空数组_应该报错",
			buildArray: func() arrow.Array {
				builder := arrow.NewInt32Builder()
				return builder.NewArray() // 空
			},
			expectError: true,
			description: "空数组边界",
		},
		{
			name: "nil数组_应该报错",
			buildArray: func() arrow.Array {
				return nil
			},
			expectError: true,
			description: "nil数组边界",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			array := tt.buildArray()

			size, err := writer.EstimatePageSize(array)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error for %s, got nil", tt.description)
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error for %s: %v", tt.description, err)
				return
			}

			// 验证估算值合理 (应该为正且不太离谱)
			if size <= 0 {
				t.Errorf("Expected positive size for %s, got %d", tt.description, size)
			}

			// 粗略检查：估算值不应超过原始数据的10倍
			var originalSize int
			if array != nil {
				switch arr := array.(type) {
				case *arrow.Int32Array:
					originalSize = arr.Len() * 4
				case *arrow.Int64Array:
					originalSize = arr.Len() * 8
				case *arrow.Float32Array:
					originalSize = arr.Len() * 4
				case *arrow.Float64Array:
					originalSize = arr.Len() * 8
				case *arrow.FixedSizeListArray:
					originalSize = arr.Len() * arr.ListSize() * 4
				}
			}

			if originalSize > 0 && size > originalSize*10 {
				t.Errorf("Estimated size %d seems too large for original %d bytes",
					size, originalSize)
			}

			t.Logf("%s: estimated size = %d bytes (original ~%d bytes)",
				tt.description, size, originalSize)
		})
	}
}

// TestEstimatePageSize_WithNullsFallback 专门测试null回退逻辑
func TestEstimatePageSize_WithNullsFallback(t *testing.T) {
	factory := encoding.NewEncoderFactory(3)
	writer := NewPageWriter(factory)

	// 创建有null的数组 (会触发回退到Zstd)
	builder := arrow.NewInt32Builder()
	for i := 0; i < 1000; i++ {
		if i%10 == 0 {
			builder.AppendNull()
		} else {
			builder.Append(int32(i % 100)) // 低基数，本来会用Dictionary
		}
	}
	array := builder.NewArray()

	size, err := writer.EstimatePageSize(array)
	if err != nil {
		t.Fatalf("EstimatePageSize failed: %v", err)
	}

	// 验证估算值合理
	if size <= 0 {
		t.Errorf("Expected positive size, got %d", size)
	}

	// 由于有null，应该回退到Zstd估算
	// Zstd通常能压缩到50%左右
	originalSize := 1000 * 4 // 4000 bytes
	if size > originalSize {
		t.Logf("Note: Estimated size %d > original %d (compression not effective for this data)",
			size, originalSize)
	}
}

// ====================
// Test 8: DefaultSerializationOptions
// ====================

// ====================
// Test 9: 边界条件 - 空文件、损坏footer等
// ====================

func TestBoundary_InvalidSchema(t *testing.T) {
	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "invalid.lance")

	// 创建一个文件，写入无效的header
	file, _ := os.Create(filename)

	// 写入magic
	file.Write([]byte("LANC"))

	// 写入错误的version
	binary.Write(file, binary.LittleEndian, uint32(99999))

	// 写入一些垃圾数据作为schema长度
	binary.Write(file, binary.LittleEndian, uint32(1000000))

	// 填充垃圾数据
	file.Write(make([]byte, 100))

	file.Close()

	// 尝试读取应该报错
	_, err := NewReader(filename)
	if err == nil {
		t.Error("Expected error for invalid schema")
	}
}
