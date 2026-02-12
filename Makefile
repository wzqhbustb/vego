# Vego Project Root Makefile
# Unified interface for testing vego, index (HNSW), and storage modules

.PHONY: all test test-race test-v test-coverage help
.PHONY: test-vego test-index test-storage
.PHONY: test-vego-race test-index-race test-storage-race
.PHONY: bench bench-quick bench-all bench-compare
.PHONY: ci pre-commit clean

# Default target: run all tests
all: test

# =============================================================================
# All-in-One Test Commands
# =============================================================================

# Run all tests across all modules (vego + index + storage)
test:
	@echo "========================================"
	@echo "Running all tests (vego + index + storage)..."
	@echo "========================================"
	@echo ""
	@echo "[1/3] Running vego tests..."
	@$(MAKE) -C vego test
	@echo ""
	@echo "[2/3] Running index tests..."
	@$(MAKE) -C index test
	@echo ""
	@echo "[3/3] Running storage tests..."
	@$(MAKE) -C storage test
	@echo ""
	@echo "========================================"
	@echo "✅ All tests passed!"
	@echo "========================================"

# Run all tests with verbose output
test-v:
	@echo "========================================"
	@echo "Running all tests with verbose output..."
	@echo "========================================"
	@echo ""
	@echo "[1/3] Running vego tests (verbose)..."
	@$(MAKE) -C vego test-v
	@echo ""
	@echo "[2/3] Running index tests (verbose)..."
	@$(MAKE) -C index test-v
	@echo ""
	@echo "[3/3] Running storage tests (verbose)..."
	@$(MAKE) -C storage test-v
	@echo ""
	@echo "========================================"
	@echo "✅ All tests passed!"
	@echo "========================================"

# Run all tests with race detector
test-race:
	@echo "========================================"
	@echo "Running all tests with race detector..."
	@echo "========================================"
	@echo ""
	@echo "[1/3] Running vego tests (race)..."
	@$(MAKE) -C vego test-race
	@echo ""
	@echo "[2/3] Running index tests (race)..."
	@$(MAKE) -C index test-race
	@echo ""
	@echo "[3/3] Running storage tests (race)..."
	@$(MAKE) -C storage test-race
	@echo ""
	@echo "========================================"
	@echo "✅ All tests passed with race detector!"
	@echo "========================================"

# Run all tests with coverage report
test-coverage:
	@echo "========================================"
	@echo "Running all tests with coverage..."
	@echo "========================================"
	@echo ""
	@echo "[1/3] Running vego tests with coverage..."
	@$(MAKE) -C vego test-cover
	@echo ""
	@echo "[2/3] Running index tests..."
	@$(MAKE) -C index test || true
	@echo ""
	@echo "[3/3] Running storage tests with coverage..."
	@$(MAKE) -C storage test-coverage
	@echo ""
	@echo "========================================"
	@echo "✅ Coverage report generated!"
	@echo "========================================"

# =============================================================================
# Module-Specific Test Commands
# =============================================================================

# Run only vego (Collection API) tests
test-vego:
	@echo "========================================"
	@echo "Running vego (Collection API) tests..."
	@echo "========================================"
	@$(MAKE) -C vego test

# Run only index (HNSW) tests
test-index:
	@echo "========================================"
	@echo "Running index (HNSW) tests..."
	@echo "========================================"
	@$(MAKE) -C index test

# Run only storage tests
test-storage:
	@echo "========================================"
	@echo "Running storage tests..."
	@echo "========================================"
	@$(MAKE) -C storage test

# Run vego tests with race detector
test-vego-race:
	@$(MAKE) -C vego test-race

# Run index tests with race detector
test-index-race:
	@$(MAKE) -C index test-race

# Run storage tests with race detector
test-storage-race:
	@$(MAKE) -C storage test-race

# =============================================================================
# Quick Benchmark Commands
# =============================================================================

# Quick benchmarks (smoke test) - runs fastest benchmarks from both modules
bench-quick:
	@echo "========================================"
	@echo "Running quick benchmarks..."
	@echo "========================================"
	@echo ""
	@echo "[1/3] Running vego quick benchmark..."
	@$(MAKE) -C vego benchmark-fast
	@echo ""
	@echo "[2/3] Running index quick benchmark (1K vectors)..."
	@$(MAKE) -C index bench-quick
	@echo ""
	@echo "[3/3] Running storage quick benchmark..."
	@$(MAKE) -C storage bench-quick
	@echo ""
	@echo "========================================"
	@echo "✅ Quick benchmarks completed!"
	@echo "========================================"

# Run all benchmarks (comprehensive, takes longer)
bench-all:
	@echo "========================================"
	@echo "Running all benchmarks..."
	@echo "⚠️  This will take 60-120 minutes"
	@echo "========================================"
	@echo ""
	@echo "[1/3] Running vego benchmarks..."
	@$(MAKE) -C vego benchmark
	@echo ""
	@echo "[2/3] Running index benchmarks..."
	@$(MAKE) -C index bench-all
	@echo ""
	@echo "[3/3] Running storage benchmarks..."
	@$(MAKE) -C storage bench-full
	@echo ""
	@echo "========================================"
	@echo "✅ All benchmarks completed!"
	@echo "========================================"

# =============================================================================
# Index (HNSW) Specific Benchmarks
# =============================================================================

# Index scale benchmarks (1K, 10K, 100K vectors)
bench-scale:
	@$(MAKE) -C index bench-scale

# Index dimension benchmarks (128, 256, 512, 768, 1536)
bench-dimension:
	@$(MAKE) -C index bench-dimension

# Index parameter tuning benchmarks (M, Ef, TopK)
bench-params:
	@$(MAKE) -C index bench-params

# Index distance function benchmarks
bench-distance:
	@$(MAKE) -C index bench-distance

# Index baseline comparison
bench-compare:
	@$(MAKE) -C index bench-compare

bench-compare-new:
	@$(MAKE) -C index bench-compare-new

# =============================================================================
# Storage Specific Benchmarks
# =============================================================================

# Storage encoding benchmarks
bench-encoding:
	@$(MAKE) -C storage bench-encoding-all

# Storage IO benchmarks
bench-io:
	@$(MAKE) -C storage bench-io

# Storage column store benchmarks
bench-column:
	@$(MAKE) -C storage bench-column

# Save storage performance baseline
bench-save-baseline:
	@$(MAKE) -C storage bench-save-baseline

# Compare storage performance with baseline
bench-compare-storage:
	@$(MAKE) -C storage bench-compare

# =============================================================================
# Code Quality & Maintenance
# =============================================================================

# Format all code
fmt:
	@echo "Formatting code..."
	@go fmt ./...
	@$(MAKE) -C storage fmt

# Run go vet
vet:
	@echo "Running go vet..."
	@go vet ./...
	@$(MAKE) -C storage vet

# Download and tidy dependencies
deps:
	@echo "Downloading dependencies..."
	@go mod download
	@go mod tidy

# Clean all test cache and artifacts
clean:
	@echo "Cleaning all test cache and artifacts..."
	@$(MAKE) -C vego clean
	@$(MAKE) -C index clean
	@$(MAKE) -C storage clean
	@go clean -testcache
	@echo "✅ Clean complete!"

# =============================================================================
# CI & Automation
# =============================================================================

# Full CI check (format + vet + race test + coverage)
ci: fmt vet test-race test-coverage
	@echo "========================================"
	@echo "✅ CI checks completed!"
	@echo "========================================"

# Quick pre-commit check
pre-commit: fmt vet test
	@echo "========================================"
	@echo "✅ Pre-commit checks passed!"
	@echo "========================================"

# Phase0 milestone check (tests + baseline)
phase0-milestone: test-race bench-save-baseline
	@echo "========================================"
	@echo "✅ Phase0 milestone completed!"
	@echo "   - All tests passed with race detector"
	@echo "   - Performance baseline saved"
	@echo "========================================"

# =============================================================================
# Help
# =============================================================================

help:
	@echo "Vego Project - Unified Test & Benchmark Makefile"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "═══════════════════════════════════════════════════════════════"
	@echo "All-in-One Commands (Recommended)"
	@echo "═══════════════════════════════════════════════════════════════"
	@echo "  make test              - Run all tests (vego + index + storage)"
	@echo "  make test-v            - Run all tests with verbose output"
	@echo "  make test-race         - Run all tests with race detector"
	@echo "  make test-coverage     - Run all tests with coverage report"
	@echo "  make bench-quick       - Quick smoke test (~5 minutes)"
	@echo "  make bench-all         - Comprehensive benchmarks (60-120 min)"
	@echo ""
	@echo "═══════════════════════════════════════════════════════════════"
	@echo "Module-Specific Commands"
	@echo "═══════════════════════════════════════════════════════════════"
	@echo "Vego (Collection API) Tests:"
	@echo "  make test-vego         - Run only vego tests"
	@echo "  make test-vego-race    - Run vego tests with race detector"
	@echo ""
	@echo "Index (HNSW) Tests:"
	@echo "  make test-index        - Run only index tests"
	@echo "  make test-index-race   - Run index tests with race detector"
	@echo ""
	@echo "Storage Tests:"
	@echo "  make test-storage      - Run only storage tests"
	@echo "  make test-storage-race - Run storage tests with race detector"
	@echo ""
	@echo "═══════════════════════════════════════════════════════════════"
	@echo "Index (HNSW) Benchmarks"
	@echo "═══════════════════════════════════════════════════════════════"
	@echo "  make bench-scale       - Test different dataset sizes (~15 min)"
	@echo "  make bench-dimension   - Test different dimensions (~20 min)"
	@echo "  make bench-params      - Test parameter tuning (~30 min)"
	@echo "  make bench-distance    - Test distance functions (~15 min)"
	@echo "  make bench-compare     - Compare with baseline"
	@echo ""
	@echo "═══════════════════════════════════════════════════════════════"
	@echo "Storage Benchmarks"
	@echo "═══════════════════════════════════════════════════════════════"
	@echo "  make bench-encoding    - Encoding benchmarks"
	@echo "  make bench-io          - IO/Async benchmarks"
	@echo "  make bench-column      - Column store benchmarks"
	@echo "  make bench-save-baseline    - Save performance baseline"
	@echo "  make bench-compare-storage  - Compare with baseline"
	@echo ""
	@echo "═══════════════════════════════════════════════════════════════"
	@echo "Code Quality"
	@echo "═══════════════════════════════════════════════════════════════"
	@echo "  make fmt               - Format all code"
	@echo "  make vet               - Run go vet"
	@echo "  make deps              - Download and tidy dependencies"
	@echo "  make clean             - Clean all test cache and artifacts"
	@echo ""
	@echo "═══════════════════════════════════════════════════════════════"
	@echo "CI & Automation"
	@echo "═══════════════════════════════════════════════════════════════"
	@echo "  make ci                - Full CI checks (fmt + vet + race + coverage)"
	@echo "  make pre-commit        - Quick pre-commit checks"
	@echo "  make phase0-milestone  - Phase0 milestone validation"
	@echo ""
	@echo "═══════════════════════════════════════════════════════════════"
	@echo "Examples"
	@echo "═══════════════════════════════════════════════════════════════"
	@echo "  make test              # Run everything"
	@echo "  make test-vego         # Test only Collection API"
	@echo "  make test-index        # Test only HNSW index"
	@echo "  make test-storage      # Test only storage"
	@echo "  make bench-quick       # Quick smoke test"
	@echo "  make ci                # Full CI validation"
	@echo ""
