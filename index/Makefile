.PHONY: test test-v test-race bench bench-quick bench-scale bench-dimension bench-params bench-distance bench-all bench-baseline bench-compare bench-compare-new clean help

# Run all tests
test:
	go test -v -timeout=30m

# Run tests with verbose output
test-v:
	go test -v -timeout=30m

# Run tests with race detector
test-race:
	go test -race -v -timeout=30m

# Quick benchmark (1K vectors, ~1 minute)
bench-quick:
	@echo "Running quick benchmarks (1K vectors, ~1 minute)..."
	go test -bench=BenchmarkHNSW_E2E_1K -benchmem -benchtime=1x -timeout=30m

# Scale benchmarks (1K, 10K, 100K)
bench-scale:
	@echo "Running scale benchmarks (1K, 10K, 100K vectors, ~15 minutes)..."
	go test -bench="BenchmarkHNSW_E2E.*K_D128$$" -benchmem -benchtime=1x -timeout=60m

# Dimension benchmarks (128, 256, 512, 768, 1536)
bench-dimension:
	@echo "Running dimension benchmarks (128-1536, ~20 minutes)..."
	go test -bench="BenchmarkHNSW_E2E_10K_D" -benchmem -benchtime=1x -timeout=60m

# Parameter tuning benchmarks (M, Ef, TopK)
bench-params:
	@echo "Running parameter tuning benchmarks (~30 minutes)..."
	go test -bench="BenchmarkHNSW_E2E_10K_M|BenchmarkHNSW_E2E_10K_Ef|BenchmarkHNSW_E2E_10K_Top" -benchmem -benchtime=1x -timeout=60m

# Distance function benchmarks (L2, Cosine, InnerProduct)
bench-distance:
	@echo "Running distance function benchmarks (~15 minutes)..."
	go test -bench="BenchmarkHNSW_E2E_10K.*Distance|BenchmarkHNSW_E2E_10K_D128$$" -benchmem -benchtime=1x -timeout=60m

# Run all benchmarks (60-120 minutes)
bench-all:
	@echo "Running all benchmarks (60-120 minutes)..."
	go test -bench=BenchmarkHNSW_E2E -benchmem -benchtime=1x -timeout=120m

# Generate baseline benchmark results
bench-baseline:
	@echo "Generating baseline benchmark results..."
	@mkdir -p benchmarks
	@echo "This will take 60-120 minutes..."
	@go test -bench=BenchmarkHNSW_E2E -benchmem -benchtime=1x -timeout=120m | tee benchmarks/baseline-$$(date +%Y%m%d-%H%M%S).txt
	@echo "Baseline saved!"

# Start benchmark comparison
bench-compare:
	@echo "Running baseline benchmarks for comparison..."
	@mkdir -p benchmarks
	@echo "Running standard 10K D128 benchmark (~5 minutes)..."
	@go test -bench=BenchmarkHNSW_E2E_10K_D128$$ -benchmem -benchtime=1x -timeout=30m > benchmarks/old.txt
	@echo "Baseline saved to benchmarks/old.txt"
	@echo "Now make your changes, then run: make bench-compare-new"

# Compare with new changes
bench-compare-new:
	@echo "Running updated benchmarks..."
	@go test -bench=BenchmarkHNSW_E2E_10K_D128$$ -benchmem -benchtime=1x -timeout=30m > benchmarks/new.txt
	@echo ""
	@echo "Comparing results with benchstat:"
	@benchstat benchmarks/old.txt benchmarks/new.txt || echo ""
	@echo "Install benchstat with: go install golang.org/x/perf/cmd/benchstat@latest"

# Run specific benchmark (example: make bench-one NAME=BenchmarkHNSW_E2E_10K_D128)
bench-one:
	@if [ -z "$(NAME)" ]; then \
		echo "Usage: make bench-one NAME=BenchmarkHNSW_E2E_10K_D128"; \
		exit 1; \
	fi
	go test -bench=$(NAME) -benchmem -benchtime=1x -timeout=30m

# Clean benchmark artifacts
clean:
	@rm -rf benchmarks/
	@echo "Cleaned benchmark artifacts"

# Show help
help:
	@echo "Vego Index Benchmark Commands:"
	@echo ""
	@echo "Testing:"
	@echo "  make test             - Run all tests"
	@echo "  make test-v           - Run tests with verbose output"
	@echo "  make test-race        - Run tests with race detector"
	@echo ""
	@echo "Quick Benchmarks:"
	@echo "  make bench-quick      - Quick smoke test (1K vectors, ~1 min)"
	@echo "  make bench-one        - Run specific benchmark (use NAME=...)"
	@echo ""
	@echo "Comprehensive Benchmarks:"
	@echo "  make bench-scale      - Test different dataset sizes (~15 min)"
	@echo "  make bench-dimension  - Test different dimensions (~20 min)"
	@echo "  make bench-params     - Test parameter tuning (~30 min)"
	@echo "  make bench-distance   - Test distance functions (~15 min)"
	@echo "  make bench-all        - Run all benchmarks (60-120 min)"
	@echo ""
	@echo "Baseline & Comparison:"
	@echo "  make bench-baseline   - Generate full baseline (60-120 min)"
	@echo "  make bench-compare    - Capture baseline for comparison"
	@echo "  make bench-compare-new - Compare with new changes"
	@echo ""
	@echo "Maintenance:"
	@echo "  make clean            - Clean benchmark artifacts"
	@echo ""
	@echo "Examples:"
	@echo "  make bench-quick                              # Fast validation"
	@echo "  make bench-scale                              # Compare scales"
	@echo "  make bench-one NAME=BenchmarkHNSW_E2E_10K_D128 # Run specific test"
	@echo ""
	@echo "Optimization workflow:"
	@echo "  1. make bench-compare        # Capture baseline"
	@echo "  2. [Make your changes]"
	@echo "  3. make bench-compare-new    # See improvements"
	@echo ""
