# Vego Test Makefile
# Provides convenient commands for testing the vego package

.PHONY: all test test-verbose test-race test-cover test-cover-html benchmark clean help

# Default target
all: test

# Run all tests
test:
	go test ./...

# Run tests with verbose output
test-verbose:
	go test -v ./...

# Run tests with race detection
test-race:
	go test -race ./...

# Run tests with coverage
test-cover:
	go test -coverprofile=coverage.out ./...
	@echo "Coverage summary:"
	@go tool cover -func=coverage.out | grep total

# Run tests with coverage and generate HTML report
test-cover-html: test-cover
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# Run benchmarks
benchmark:
	go test -bench=. -benchmem ./...

# Run benchmarks with specific pattern (e.g., make benchmark-run PATTERN=Insert)
benchmark-run:
	go test -bench=$(PATTERN) -benchmem -run=^$$ ./...

# Run specific test (e.g., make test-run TEST=TestCollectionInsert)
test-run:
	go test -v -run $(TEST) ./...

# Run tests for specific file pattern (e.g., make test-file FILE=Collection)
test-file:
	go test -v -run Test$(FILE) ./...

# Run all tests including examples
test-all:
	go test -v -run . ./...

# Run short tests only (skips long-running tests)
test-short:
	go test -short ./...

# Run tests with timeout (for CI)
test-ci:
	go test -v -race -coverprofile=coverage.out -timeout 10m ./...

# Format code
fmt:
	go fmt ./...

# Run linter (requires golangci-lint)
lint:
	@which golangci-lint > /dev/null || (echo "golangci-lint not installed, run: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest" && exit 1)
	golangci-lint run ./...

# Vet code
vet:
	go vet ./...

# Check for issues (fmt + vet + test)
check: fmt vet test

# Generate coverage report in multiple formats
coverage: test-cover
	@mkdir -p coverage
	go tool cover -html=coverage.out -o coverage/coverage.html
	go tool cover -func=coverage.out > coverage/coverage.txt
	@echo "Coverage reports generated in coverage/"

# Clean generated files
clean:
	rm -f coverage.out coverage.html
	rm -rf coverage/
	rm -rf /tmp/vego_*

# Display help
help:
	@echo "Vego Test Makefile"
	@echo ""
	@echo "Available targets:"
	@echo "  make test              - Run all tests"
	@echo "  make test-verbose      - Run tests with verbose output"
	@echo "  make test-race         - Run tests with race detection"
	@echo "  make test-cover        - Run tests with coverage summary"
	@echo "  make test-cover-html   - Generate HTML coverage report"
	@echo "  make test-run TEST=xxx - Run specific test (e.g., TEST=TestCollectionInsert)"
	@echo "  make test-file FILE=xxx- Run tests matching pattern (e.g., FILE=Collection)"
	@echo "  make benchmark         - Run all benchmarks"
	@echo "  make benchmark-run PATTERN=xxx - Run specific benchmark"
	@echo "  make test-ci           - Run CI test suite (race + coverage)"
	@echo "  make fmt               - Format code"
	@echo "  make vet               - Run go vet"
	@echo "  make lint              - Run golangci-lint"
	@echo "  make check             - Run fmt + vet + test"
	@echo "  make coverage          - Generate coverage reports"
	@echo "  make clean             - Clean generated files"
	@echo "  make help              - Show this help message"
