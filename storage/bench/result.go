// lance/bench/result.go
package bench

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// BenchmarkResult 单次基准测试结果
type BenchmarkResult struct {
	Name      string    `json:"name"`
	Timestamp time.Time `json:"timestamp"`

	// 性能指标
	OpsPerSec   float64 `json:"ops_per_sec"`   // 每秒操作数
	NsPerOp     int64   `json:"ns_per_op"`     // 每次操作纳秒
	BytesPerSec float64 `json:"bytes_per_sec"` // 每秒字节数
	AllocBytes  int64   `json:"alloc_bytes"`   // 每次操作分配字节
	Allocs      int64   `json:"allocs"`        // 每次操作分配次数

	// 数据规模
	DataSize  int   `json:"data_size"`  // 数据行数/元素数
	DataBytes int64 `json:"data_bytes"` // 原始数据大小

	// 压缩指标（编码相关）
	CompressedSize   int64   `json:"compressed_size,omitempty"`   // 压缩后大小
	CompressionRatio float64 `json:"compression_ratio,omitempty"` // 压缩比

	// 元数据
	Encoding    string `json:"encoding,omitempty"`    // 使用的编码
	Concurrency int    `json:"concurrency,omitempty"` // 并发度
	Columns     int    `json:"columns,omitempty"`     // 列数

	// 环境信息
	GoVersion string `json:"go_version"`
	Platform  string `json:"platform"`
	CPU       string `json:"cpu"`
}

// ResultSet 基准测试集合
type ResultSet struct {
	Timestamp time.Time          `json:"timestamp"`
	Commit    string             `json:"commit"`  // git commit hash
	Version   string             `json:"version"` // lance 版本
	Results   []*BenchmarkResult `json:"results"`
}

// NewResultSet 创建新的结果集
func NewResultSet() *ResultSet {
	return &ResultSet{
		Timestamp: time.Now(),
		Results:   make([]*BenchmarkResult, 0),
	}
}

// Add 添加结果
func (rs *ResultSet) Add(r *BenchmarkResult) {
	rs.Results = append(rs.Results, r)
}

// SaveToFile 保存结果到文件
func (rs *ResultSet) SaveToFile(filename string) error {
	dir := filepath.Dir(filename)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	data, err := json.MarshalIndent(rs, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(filename, data, 0644)
}

// LoadFromFile 从文件加载结果
func LoadFromFile(filename string) (*ResultSet, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	var rs ResultSet
	if err := json.Unmarshal(data, &rs); err != nil {
		return nil, err
	}
	return &rs, nil
}

// Compare 比较两个结果集，返回性能变化
func (rs *ResultSet) Compare(baseline *ResultSet) *ComparisonReport {
	return CompareResults(baseline, rs)
}

// BenchmarkResultFromTesting 从 testing.BenchmarkResult 转换
func BenchmarkResultFromTesting(name string, r testing.BenchmarkResult, dataBytes int64) *BenchmarkResult {
	return &BenchmarkResult{
		Name:        name,
		Timestamp:   time.Now(),
		OpsPerSec:   float64(r.N) / r.T.Seconds(),
		NsPerOp:     r.NsPerOp(),
		BytesPerSec: float64(dataBytes) * float64(r.N) / r.T.Seconds(),
		AllocBytes:  r.AllocedBytesPerOp(),
		Allocs:      r.AllocsPerOp(),
		DataBytes:   dataBytes,
		GoVersion:   runtime.Version(),
		Platform:    fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH),
		CPU:         fmt.Sprintf("%d cores", runtime.NumCPU()),
	}
}
