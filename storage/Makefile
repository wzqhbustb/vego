# langgraphdemogo/lance/Makefile

# 模块配置
MODULE_ROOT := ..
MODULE_NAME := github.com/wzqhbustb/vego/storage
LANCE_PKG := $(MODULE_NAME)

# 子包列表
PACKAGES := \
	$(MODULE_NAME)/arrow \
	$(MODULE_NAME)/column \
	$(MODULE_NAME)/encoding \
	$(MODULE_NAME)/format \
	$(MODULE_NAME)/io \
	$(MODULE_NAME)/errors

# 测试配置
TEST_TIMEOUT := 300s
RACE_DETECTOR := -race
COVERAGE_OUT := coverage.out
COVERAGE_HTML := coverage.html

# 基准测试配置（相对于 MODULE_ROOT 的相对路径）
BENCH_RESULTS_DIR := bench_results
BENCH_BASELINE_FILE := $(BENCH_RESULTS_DIR)/baseline.txt

# 默认目标：运行所有测试
.PHONY: all
all: test

# =============================================================================
# 测试命令
# =============================================================================

# 运行所有测试（快速模式）
.PHONY: test
test:
	@echo "Running all tests..."
	@cd $(MODULE_ROOT) && go test $(PACKAGES) -timeout $(TEST_TIMEOUT)

# 运行所有测试（带竞态检测）
.PHONY: test-race
test-race:
	@echo "Running tests with race detector..."
	@cd $(MODULE_ROOT) && go test $(PACKAGES) $(RACE_DETECTOR) -timeout $(TEST_TIMEOUT)

# 运行所有测试（详细输出）
.PHONY: test-v
test-v:
	@echo "Running tests with verbose output..."
	@cd $(MODULE_ROOT) && go test $(PACKAGES) -v -timeout $(TEST_TIMEOUT)

# 运行测试并生成覆盖率报告
.PHONY: test-coverage
test-coverage:
	@echo "Running tests with coverage..."
	@cd $(MODULE_ROOT) && go test $(PACKAGES) -coverprofile=$(COVERAGE_OUT) -covermode=atomic -timeout $(TEST_TIMEOUT)
	@echo "Coverage report saved to: $(MODULE_ROOT)/$(COVERAGE_OUT)"
	@cd $(MODULE_ROOT) && go tool cover -html=$(COVERAGE_OUT) -o $(COVERAGE_HTML)
	@echo "HTML coverage report saved to: $(MODULE_ROOT)/$(COVERAGE_HTML)"

# 查看覆盖率摘要（不生成 HTML）
.PHONY: cover
cover:
	@cd $(MODULE_ROOT) && go test $(PACKAGES) -cover -timeout $(TEST_TIMEOUT)

# =============================================================================
# 单包测试
# =============================================================================

.PHONY: test-arrow
test-arrow:
	@cd $(MODULE_ROOT) && go test $(LANCE_PKG)/arrow -v

.PHONY: test-column
test-column:
	@cd $(MODULE_ROOT) && go test $(LANCE_PKG)/column -v

.PHONY: test-encoding
test-encoding:
	@cd $(MODULE_ROOT) && go test $(LANCE_PKG)/encoding -v

.PHONY: test-format
test-format:
	@cd $(MODULE_ROOT) && go test $(LANCE_PKG)/format -v

.PHONY: test-io
test-io:
	@cd $(MODULE_ROOT) && go test $(LANCE_PKG)/io -v

.PHONY: test-errors
test-errors:
	@cd $(MODULE_ROOT) && go test $(LANCE_PKG)/errors -v

# =============================================================================
# 性能测试（基础版）
# =============================================================================

.PHONY: bench
bench:
	@echo "Running benchmarks..."
	@cd $(MODULE_ROOT) && go test $(PACKAGES) -bench=. -benchmem -run=^$$

.PHONY: bench-arrow
bench-arrow:
	@cd $(MODULE_ROOT) && go test $(LANCE_PKG)/arrow -bench=. -benchmem -run=^$$

.PHONY: bench-encoding
bench-encoding:
	@cd $(MODULE_ROOT) && go test $(LANCE_PKG)/encoding -bench=. -benchmem -run=^$$

# =============================================================================
# Phase0: 高级性能基准测试（性能基线建立）
# =============================================================================

# 运行完整基准测试并保存结果（运行3次取平均）
.PHONY: bench-full
bench-full:
	@echo "Running full benchmark suite (3 iterations)..."
	@mkdir -p $(MODULE_ROOT)/$(BENCH_RESULTS_DIR)
	@cd $(MODULE_ROOT) && go test $(PACKAGES) \
		-bench=Benchmark \
		-benchmem \
		-run=^$$ \
		-count=3 \
		-timeout=30m \
		2>&1 | tee $(BENCH_RESULTS_DIR)/bench_$$(date +%Y%m%d_%H%M%S).txt

# 快速基准测试（单次运行，开发时使用）
.PHONY: bench-quick
bench-quick:
	@echo "Running quick benchmarks..."
	@cd $(MODULE_ROOT) && go test $(LANCE_PKG)/column $(LANCE_PKG)/encoding \
		-bench=. \
		-benchmem \
		-run=^$$ \
		-timeout=5m

# 保存当前结果为基线（运行5次取稳定值）
.PHONY: bench-save-baseline
bench-save-baseline:
	@echo "Saving current benchmark as baseline..."
	@mkdir -p $(MODULE_ROOT)/$(BENCH_RESULTS_DIR)
	@cd $(MODULE_ROOT) && go test $(PACKAGES) \
		-bench=Benchmark \
		-benchmem \
		-run=^$$ \
		-count=5 \
		-timeout=30m \
		2>&1 | tee $(BENCH_BASELINE_FILE)
	@echo "========================================"
	@echo "✅ Baseline saved to: $(BENCH_BASELINE_FILE)"
	@echo "========================================"

# 与基线比较（需要安装 benchstat: go install golang.org/x/perf/cmd/benchstat@latest）
.PHONY: bench-compare
bench-compare:
	@if [ ! -f "$(BENCH_BASELINE_FILE)" ]; then \
		echo "❌ Baseline not found. Run 'make bench-save-baseline' first."; \
		exit 1; \
	fi
	@echo "Running new benchmark for comparison..."
	@cd $(MODULE_ROOT) && go test $(PACKAGES) \
		-bench=Benchmark \
		-benchmem \
		-run=^$$ \
		-count=5 \
		-timeout=30m \
		2>&1 | tee $(BENCH_RESULTS_DIR)/bench_new.txt
	@echo ""
	@echo "========================================"
	@echo "      Benchmark Comparison Report"
	@echo "========================================"
	@benchstat $(BENCH_BASELINE_FILE) $(BENCH_RESULTS_DIR)/bench_new.txt 2>/dev/null || \
		(echo ""; \
		 echo "⚠️  benchstat not found or comparison failed."; \
		 echo "   Install with: go install golang.org/x/perf/cmd/benchstat@latest"; \
		 echo ""; \
		 echo "Raw results saved to:"; \
		 echo "  Baseline: $(BENCH_BASELINE_FILE)"; \
		 echo "  Current:  $(BENCH_RESULTS_DIR)/bench_new.txt")

# 编码器专项基准测试
.PHONY: bench-encoding-all
bench-encoding-all:
	@echo "Running encoding benchmarks..."
	@mkdir -p $(MODULE_ROOT)/$(BENCH_RESULTS_DIR)
	@cd $(MODULE_ROOT) && go test $(LANCE_PKG)/encoding \
		-bench=Benchmark \
		-benchmem \
		-run=^$$ \
		-count=3 \
		-timeout=10m \
		2>&1 | tee $(BENCH_RESULTS_DIR)/encoding_$$(date +%Y%m%d_%H%M%S).txt

# IO/异步性能专项测试
.PHONY: bench-io
bench-io:
	@echo "Running IO benchmarks..."
	@mkdir -p $(MODULE_ROOT)/$(BENCH_RESULTS_DIR)
	@cd $(MODULE_ROOT) && go test $(LANCE_PKG)/io \
		-bench=Benchmark \
		-benchmem \
		-run=^$$ \
		-count=3 \
		-timeout=10m \
		2>&1 | tee $(BENCH_RESULTS_DIR)/io_$$(date +%Y%m%d_%H%M%S).txt

# 列存储端到端基准测试
.PHONY: bench-column
bench-column:
	@echo "Running column store benchmarks..."
	@mkdir -p $(MODULE_ROOT)/$(BENCH_RESULTS_DIR)
	@cd $(MODULE_ROOT) && go test $(LANCE_PKG)/column \
		-bench=Benchmark \
		-benchmem \
		-run=^$$ \
		-count=3 \
		-timeout=15m \
		2>&1 | tee $(BENCH_RESULTS_DIR)/column_$$(date +%Y%m%d_%H%M%S).txt

# 列出所有可用的基准测试
.PHONY: bench-list
bench-list:
	@echo "Available benchmarks:"
	@cd $(MODULE_ROOT) && go test $(PACKAGES) -bench=. -run=^$$ 2>&1 | grep "^Benchmark" | cut -d' ' -f1 | sort | uniq

# 清理基准测试结果
.PHONY: bench-clean
bench-clean:
	@rm -rf $(BENCH_RESULTS_DIR)
	@echo "Cleaned benchmark results from: $(BENCH_RESULTS_DIR)"

# =============================================================================
# 代码质量
# =============================================================================

# 运行 go vet 静态检查
.PHONY: vet
vet:
	@cd $(MODULE_ROOT) && go vet $(PACKAGES)

# 格式化代码
.PHONY: fmt
fmt:
	@cd $(MODULE_ROOT) && go fmt $(PACKAGES)

# 检查代码风格（需要安装 golint）
.PHONY: lint
lint:
	@cd $(MODULE_ROOT) && golint $(PACKAGES)

# 下载依赖
.PHONY: deps
deps:
	@cd $(MODULE_ROOT) && go mod download
	@cd $(MODULE_ROOT) && go mod tidy

# 清理测试缓存和覆盖率文件
.PHONY: clean
clean:
	@cd $(MODULE_ROOT) && go clean -testcache
	@rm -f $(MODULE_ROOT)/$(COVERAGE_OUT) $(MODULE_ROOT)/$(COVERAGE_HTML)
	@echo "Cleaned test cache and coverage files"

# =============================================================================
# CI/自动化
# =============================================================================

# CI 完整检查（测试 + 竞态检测 + 覆盖率 + vet）
.PHONY: ci
ci: fmt vet test-race test-coverage
	@echo "CI checks completed"

# 快速检查（用于本地提交前）
.PHONY: pre-commit
pre-commit: fmt vet test
	@echo "Pre-commit checks passed"

# Phase0 里程碑检查（测试 + 基准测试 + 保存基线）
.PHONY: phase0-milestone
phase0-milestone: test-race bench-save-baseline
	@echo "========================================"
	@echo "✅ Phase0 milestone completed!"
	@echo "   - All tests passed with race detector"
	@echo "   - Performance baseline saved"
	@echo "========================================"

# =============================================================================
# 帮助
# =============================================================================

.PHONY: help
help:
	@echo "Lance Storage Engine - Test Makefile"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Test Commands:"
	@echo "  make test          - Run all tests (default)"
	@echo "  make test-race     - Run tests with race detector"
	@echo "  make test-v        - Run tests with verbose output"
	@echo "  make test-coverage - Run tests and generate coverage report"
	@echo "  make cover         - Quick coverage summary"
	@echo ""
	@echo "Single Package Tests:"
	@echo "  make test-arrow    - Test arrow package"
	@echo "  make test-column   - Test column package"
	@echo "  make test-encoding - Test encoding package"
	@echo "  make test-format   - Test format package"
	@echo "  make test-io       - Test io package"
	@echo "  make test-errors   - Test errors package"
	@echo ""
	@echo "Basic Benchmarks:"
	@echo "  make bench         - Run all benchmarks (quick)"
	@echo "  make bench-arrow   - Run arrow benchmarks"
	@echo "  make bench-encoding- Run encoding benchmarks"
	@echo ""
	@echo "Advanced Benchmarks (Phase0):"
	@echo "  make bench-full         - Full benchmark with 3 iterations"
	@echo "  make bench-quick        - Quick benchmarks for development"
	@echo "  make bench-save-baseline- Save current results as baseline"
	@echo "  make bench-compare      - Compare current with baseline (needs benchstat)"
	@echo "  make bench-encoding-all - Encoding-only benchmarks"
	@echo "  make bench-io           - IO/Async benchmarks"
	@echo "  make bench-column       - Column store E2E benchmarks"
	@echo "  make bench-list         - List all available benchmarks"
	@echo "  make bench-clean        - Clean benchmark results"
	@echo ""
	@echo "Code Quality:"
	@echo "  make vet           - Run go vet"
	@echo "  make fmt           - Format code"
	@echo "  make lint          - Run golint"
	@echo "  make deps          - Download and tidy dependencies"
	@echo ""
	@echo "Automation:"
	@echo "  make ci              - Full CI checks"
	@echo "  make pre-commit      - Quick pre-commit checks"
	@echo "  make phase0-milestone- Phase0 milestone (tests + baseline)"
	@echo "  make clean           - Clean test cache and coverage files"
	@echo ""
	@echo "Help:"
	@echo "  make help          - Show this help message"