// lance/bench/runner.go
package bench

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

// Runner 基准测试运行器
type Runner struct {
	Config    *BenchmarkConfig
	ResultSet *ResultSet
	OutputDir string
}

// NewRunner 创建新的运行器
func NewRunner(cfg *BenchmarkConfig) *Runner {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	return &Runner{
		Config:    cfg,
		ResultSet: NewResultSet(),
		OutputDir: cfg.OutputDir,
	}
}

// RunAll 运行所有基准测试
func (r *Runner) RunAll() error {
	timestamp := time.Now().Format("20060102_150405")
	outputFile := filepath.Join(r.OutputDir, fmt.Sprintf("bench_%s.json", timestamp))

	// 运行 column 包基准测试
	if err := r.runPackage("lance/column", "Benchmark"); err != nil {
		return err
	}

	// 运行 encoding 包基准测试
	if err := r.runPackage("lance/encoding", "Benchmark"); err != nil {
		return err
	}

	// 运行 io 包基准测试
	if err := r.runPackage("lance/io", "Benchmark"); err != nil {
		return err
	}

	// 保存结果
	if err := r.ResultSet.SaveToFile(outputFile); err != nil {
		return fmt.Errorf("failed to save results: %w", err)
	}

	fmt.Printf("Benchmark results saved to: %s\n", outputFile)
	return nil
}

// runPackage 运行指定包的基准测试
func (r *Runner) runPackage(pkg, benchPattern string) error {
	fmt.Printf("Running benchmarks for %s...\n", pkg)

	cmd := exec.Command("go", "test",
		"-bench="+benchPattern,
		"-benchmem",
		"-run=^$",
		"-count=3", // 运行3次取平均
		"-timeout=30m",
		"./"+pkg,
	)

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	return cmd.Run()
}

// CompareWithBaseline 与基线比较
func (r *Runner) CompareWithBaseline(baselineFile string) (*ComparisonReport, error) {
	baseline, err := LoadFromFile(baselineFile)
	if err != nil {
		return nil, err
	}

	return r.ResultSet.Compare(baseline), nil
}
