package bench

import (
	"fmt"
	"math"
)

// Comparison 单个测试的比较结果
type Comparison struct {
	Name           string  `json:"name"`
	BaselineOps    float64 `json:"baseline_ops"`
	CurrentOps     float64 `json:"current_ops"`
	ChangePercent  float64 `json:"change_percent"` // 正数表示提升，负数表示下降
	BaselineBytes  int64   `json:"baseline_bytes"`
	CurrentBytes   int64   `json:"current_bytes"`
	BytesChangePct float64 `json:"bytes_change_pct"`
}

// ComparisonReport 比较报告
type ComparisonReport struct {
	Comparisons  []*Comparison `json:"comparisons"`
	Improved     int           `json:"improved"`
	Regressed    int           `json:"regressed"`
	Unchanged    int           `json:"unchanged"`
	ThresholdPct float64       `json:"threshold_pct"` // 变化阈值，低于此值视为无变化
}

// CompareResults 比较两个结果集
func CompareResults(baseline, current *ResultSet) *ComparisonReport {
	report := &ComparisonReport{
		Comparisons:  make([]*Comparison, 0),
		ThresholdPct: 5.0, // 5% 变化阈值
	}

	// 建立 baseline 索引
	baselineMap := make(map[string]*BenchmarkResult)
	for _, r := range baseline.Results {
		baselineMap[r.Name] = r
	}

	// 比较每个当前结果
	for _, curr := range current.Results {
		base, ok := baselineMap[curr.Name]
		if !ok {
			continue // baseline 中没有，跳过
		}

		comp := &Comparison{
			Name:          curr.Name,
			BaselineOps:   base.OpsPerSec,
			CurrentOps:    curr.OpsPerSec,
			BaselineBytes: base.AllocBytes,
			CurrentBytes:  curr.AllocBytes,
		}

		// 计算性能变化
		if base.OpsPerSec > 0 {
			comp.ChangePercent = (curr.OpsPerSec - base.OpsPerSec) / base.OpsPerSec * 100
		}

		// 计算内存变化
		if base.AllocBytes > 0 {
			comp.BytesChangePct = float64(curr.AllocBytes-base.AllocBytes) / float64(base.AllocBytes) * 100
		}

		// 分类
		if math.Abs(comp.ChangePercent) < report.ThresholdPct {
			report.Unchanged++
		} else if comp.ChangePercent > 0 {
			report.Improved++
		} else {
			report.Regressed++
		}

		report.Comparisons = append(report.Comparisons, comp)
	}

	return report
}

// Print 打印比较报告
func (r *ComparisonReport) Print() {
	fmt.Println("========================================")
	fmt.Println("       Benchmark Comparison Report")
	fmt.Println("========================================")
	fmt.Printf("Threshold: ±%.1f%%\n", r.ThresholdPct)
	fmt.Printf("Improved:  %d\n", r.Improved)
	fmt.Printf("Regressed: %d\n", r.Regressed)
	fmt.Printf("Unchanged: %d\n", r.Unchanged)
	fmt.Println("----------------------------------------")

	// 打印回归的测试
	if r.Regressed > 0 {
		fmt.Println("\n⚠️  Performance Regressions:")
		for _, c := range r.Comparisons {
			if c.ChangePercent < -r.ThresholdPct {
				fmt.Printf("  %-50s %6.1f%%  (%.0f → %.0f ops/s)\n",
					c.Name, c.ChangePercent, c.BaselineOps, c.CurrentOps)
			}
		}
	}

	// 打印提升的测试
	if r.Improved > 0 {
		fmt.Println("\n✅ Performance Improvements:")
		for _, c := range r.Comparisons {
			if c.ChangePercent > r.ThresholdPct {
				fmt.Printf("  %-50s +%5.1f%%  (%.0f → %.0f ops/s)\n",
					c.Name, c.ChangePercent, c.BaselineOps, c.CurrentOps)
			}
		}
	}
}
