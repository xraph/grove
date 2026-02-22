.PHONY: help build run test clean fmt lint lint-fix vet tidy deps install dev hot check coverage b r t c f l lf v check-deps

# Default target
.DEFAULT_GOAL := help

# Variables
BINARY_NAME=grove
CMD_DIR=./cmd/grove
BUILD_DIR=./bin
GO=go
GOFLAGS=-v
LDFLAGS=-ldflags "-s -w"

# Colors for output
RED=\033[0;31m
GREEN=\033[0;32m
YELLOW=\033[0;33m
BLUE=\033[0;34m
NC=\033[0m # No Color

## help: Display this help message
help:
	@echo "$(BLUE)Available targets:$(NC)"
	@echo ""
	@echo "$(GREEN)Build & Run:$(NC)"
	@echo "  make build (b)      - Build the application"
	@echo "  make run (r)        - Run the application"
	@echo "  make dev (d)        - Run in development mode with live reload"
	@echo "  make install (i)    - Install the binary to GOPATH/bin"
	@echo "  make clean (c)      - Remove build artifacts"
	@echo ""
	@echo "$(GREEN)Code Quality:$(NC)"
	@echo "  make fmt (f)        - Format code with gofmt and goimports"
	@echo "  make lint (l)       - Run linter (golangci-lint)"
	@echo "  make lint-fix (lf)  - Run linter with auto-fix"
	@echo "  make vet (v)        - Run go vet"
	@echo "  make check          - Run fmt, vet, and lint"
	@echo ""
	@echo "$(GREEN)Testing:$(NC)"
	@echo "  make test (t)       - Run tests"
	@echo "  make test-verbose   - Run tests with verbose output"
	@echo "  make test-race      - Run tests with race detector"
	@echo "  make coverage       - Generate test coverage report"
	@echo "  make coverage-html  - Generate HTML coverage report"
	@echo ""
	@echo "$(GREEN)Dependencies:$(NC)"
	@echo "  make deps           - Install development dependencies"
	@echo "  make tidy           - Tidy and verify go modules"
	@echo "  make mod-download   - Download go modules"
	@echo "  make mod-verify     - Verify go modules"
	@echo ""
	@echo "$(GREEN)Documentation:$(NC)"
	@echo "  make docs           - Serve documentation locally"
	@echo "  make docs-build     - Build documentation"
	@echo ""
	@echo "$(GREEN)Other:$(NC)"
	@echo "  make all            - Run check, test, and build"
	@echo "  make help (h)       - Show this help message"

## build (b): Build the application
build b:
	@echo "$(BLUE)Building $(BINARY_NAME)...$(NC)"
	@mkdir -p $(BUILD_DIR)
	$(GO) build $(GOFLAGS) $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME) $(CMD_DIR)
	@echo "$(GREEN)✓ Build complete: $(BUILD_DIR)/$(BINARY_NAME)$(NC)"

## run (r): Run the application
run r:
	@echo "$(BLUE)Running $(BINARY_NAME)...$(NC)"
	$(GO) run $(CMD_DIR)/main.go

## dev (d): Run in development mode
dev d:
	@echo "$(BLUE)Running in development mode...$(NC)"
	@command -v air >/dev/null 2>&1 || { echo "$(YELLOW)Air not found, installing...$(NC)"; go install github.com/cosmtrek/air@latest; }
	@mkdir -p tmp
	@chmod +x tmp 2>/dev/null || true
	air

## hot: Alias for dev
hot: dev

## install (i): Install binary to GOPATH/bin
install i: build
	@echo "$(BLUE)Installing $(BINARY_NAME)...$(NC)"
	$(GO) install $(CMD_DIR)
	@echo "$(GREEN)✓ Installed to $(shell go env GOPATH)/bin/$(BINARY_NAME)$(NC)"

## clean (c): Remove build artifacts
clean c:
	@echo "$(BLUE)Cleaning build artifacts...$(NC)"
	@rm -rf $(BUILD_DIR)
	@rm -rf tmp
	@rm -f coverage.out coverage.html
	@rm -f build-errors.log
	@$(GO) clean
	@echo "$(GREEN)✓ Clean complete$(NC)"

## fmt (f): Format code
fmt f:
	@echo "$(BLUE)Formatting code...$(NC)"
	@gofmt -s -w .
	@command -v goimports >/dev/null 2>&1 && goimports -w -local github.com/xraph/grove . || echo "$(YELLOW)goimports not found, skipping (run: go install golang.org/x/tools/cmd/goimports@latest)$(NC)"
	@echo "$(GREEN)✓ Formatting complete$(NC)"

## lint (l): Run linter
lint l:
	@echo "$(BLUE)Running linter...$(NC)"
	@command -v golangci-lint >/dev/null 2>&1 || { echo "$(RED)golangci-lint not found. Install: https://golangci-lint.run/usage/install/$(NC)"; exit 1; }
	golangci-lint run ./...
	@echo "$(GREEN)✓ Linting complete$(NC)"

## lint-fix (lf): Run linter with auto-fix
lint-fix lf:
	@echo "$(BLUE)Running linter with auto-fix...$(NC)"
	@command -v golangci-lint >/dev/null 2>&1 || { echo "$(RED)golangci-lint not found. Install: https://golangci-lint.run/usage/install/$(NC)"; exit 1; }
	golangci-lint run --fix ./...
	@echo "$(GREEN)✓ Linting with fixes complete$(NC)"

## vet (v): Run go vet
vet v:
	@echo "$(BLUE)Running go vet...$(NC)"
	$(GO) vet ./...
	@echo "$(GREEN)✓ Vet complete$(NC)"

## check: Run fmt, vet, and lint
check:
	@echo "$(BLUE)Running all checks...$(NC)"
	@$(MAKE) fmt
	@$(MAKE) vet
	@$(MAKE) lint
	@echo "$(GREEN)✓ All checks passed$(NC)"

## test (t): Run tests
test t:
	@echo "$(BLUE)Running tests...$(NC)"
	$(GO) test -v ./...
	@echo "$(GREEN)✓ Tests complete$(NC)"

## test-verbose: Run tests with verbose output
test-verbose:
	@echo "$(BLUE)Running tests (verbose)...$(NC)"
	$(GO) test -v -count=1 ./...

## test-race: Run tests with race detector
test-race:
	@echo "$(BLUE)Running tests with race detector...$(NC)"
	$(GO) test -race -v ./...
	@echo "$(GREEN)✓ Race tests complete$(NC)"

## coverage: Generate test coverage
coverage:
	@echo "$(BLUE)Generating coverage report...$(NC)"
	$(GO) test -coverprofile=coverage.out ./...
	$(GO) tool cover -func=coverage.out
	@echo "$(GREEN)✓ Coverage report generated: coverage.out$(NC)"

## coverage-html: Generate HTML coverage report
coverage-html: coverage
	@echo "$(BLUE)Generating HTML coverage report...$(NC)"
	$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "$(GREEN)✓ HTML coverage report: coverage.html$(NC)"
	@command -v open >/dev/null 2>&1 && open coverage.html || echo "Open coverage.html in your browser"

## tidy: Tidy and verify modules
tidy:
	@echo "$(BLUE)Tidying modules...$(NC)"
	$(GO) mod tidy
	$(GO) mod verify
	@echo "$(GREEN)✓ Modules tidied$(NC)"

## deps: Install development dependencies
deps:
	@echo "$(BLUE)Installing development dependencies...$(NC)"
	@echo "Installing goimports..."
	@go install golang.org/x/tools/cmd/goimports@latest
	@echo "Installing air (hot reload)..."
	@go install github.com/cosmtrek/air@latest
	@echo "Installing golangci-lint..."
	@command -v golangci-lint >/dev/null 2>&1 || curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(shell go env GOPATH)/bin
	@echo "$(GREEN)✓ Development dependencies installed$(NC)"

## check-deps: Check if required tools are installed
check-deps:
	@echo "$(BLUE)Checking development dependencies...$(NC)"
	@command -v goimports >/dev/null 2>&1 && echo "$(GREEN)✓ goimports$(NC)" || echo "$(YELLOW)✗ goimports (run: make deps)$(NC)"
	@command -v golangci-lint >/dev/null 2>&1 && echo "$(GREEN)✓ golangci-lint$(NC)" || echo "$(YELLOW)✗ golangci-lint (run: make deps)$(NC)"
	@command -v air >/dev/null 2>&1 && echo "$(GREEN)✓ air$(NC)" || echo "$(YELLOW)✗ air (run: make deps)$(NC)"

## mod-download: Download modules
mod-download:
	@echo "$(BLUE)Downloading modules...$(NC)"
	$(GO) mod download
	@echo "$(GREEN)✓ Modules downloaded$(NC)"

## mod-verify: Verify modules
mod-verify:
	@echo "$(BLUE)Verifying modules...$(NC)"
	$(GO) mod verify
	@echo "$(GREEN)✓ Modules verified$(NC)"

## docs: Serve documentation locally
docs:
	@echo "$(BLUE)Serving documentation...$(NC)"
	@cd docs && pnpm install && pnpm dev

## docs-build: Build documentation
docs-build:
	@echo "$(BLUE)Building documentation...$(NC)"
	@cd docs && pnpm install && pnpm build
	@echo "$(GREEN)✓ Documentation built$(NC)"

## all: Run check, test, and build
all: check test build
	@echo "$(GREEN)✓ All tasks complete$(NC)"

# Short aliases
h: help
b: build
r: run
t: test
c: clean
f: fmt
l: lint
lf: lint-fix
v: vet
d: dev
i: install
