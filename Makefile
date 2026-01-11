.PHONY: proto build test clean all build-all

# Go 参数
GOCMD=go
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test
GOCLEAN=$(GOCMD) clean
GOMOD=$(GOCMD) mod

# 版本信息
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
BUILD_TIME ?= $(shell date -u '+%Y-%m-%d_%H:%M:%S')
LDFLAGS=-ldflags "-X main.Version=$(VERSION) -X main.BuildTime=$(BUILD_TIME)"

# RocksDB CGO 配置 (自动检测 Homebrew 路径)
# macOS: 检测 Homebrew 前缀 (Apple Silicon: /opt/homebrew, Intel: /usr/local)
# Linux: 使用标准系统路径
UNAME_S := $(shell uname -s)
export CGO_ENABLED := 1
ifeq ($(UNAME_S),Darwin)
    HOMEBREW_PREFIX := $(shell brew --prefix 2>/dev/null || echo "/opt/homebrew")
    export CGO_CFLAGS := -I$(HOMEBREW_PREFIX)/include
    export CGO_LDFLAGS := -L$(HOMEBREW_PREFIX)/lib
endif

# 二进制输出目录
BINDIR=bin

# 服务二进制
STORAGE_BINARY=$(BINDIR)/rockskv-storage
COMPUTE_BINARY=$(BINDIR)/rockskv-compute
METADATA_BINARY=$(BINDIR)/rockskv-metadata
CLI_BINARY=$(BINDIR)/rockskv-cli

# 支持的平台
PLATFORMS=linux/amd64 linux/arm64 darwin/amd64 darwin/arm64

# Protobuf
PROTO_DIR=proto
PROTO_OUT=pkg/proto

all: proto build

# 生成 protobuf 代码
proto:
	@echo "Generating protobuf code..."
	@mkdir -p $(PROTO_OUT)
	protoc --go_out=$(PROTO_OUT) --go_opt=paths=source_relative \
		--go-grpc_out=$(PROTO_OUT) --go-grpc_opt=paths=source_relative \
		-I$(PROTO_DIR) $(PROTO_DIR)/*.proto

# 构建所有服务 (当前平台)
build: build-storage build-compute build-metadata build-cli

build-storage:
	@echo "Building storage service..."
	@mkdir -p $(BINDIR)
	$(GOBUILD) $(LDFLAGS) -o $(STORAGE_BINARY) ./cmd/storage

build-compute:
	@echo "Building compute service..."
	@mkdir -p $(BINDIR)
	$(GOBUILD) $(LDFLAGS) -o $(COMPUTE_BINARY) ./cmd/compute

build-metadata:
	@echo "Building metadata service..."
	@mkdir -p $(BINDIR)
	$(GOBUILD) $(LDFLAGS) -o $(METADATA_BINARY) ./cmd/metadata

build-cli:
	@echo "Building CLI..."
	@mkdir -p $(BINDIR)
	$(GOBUILD) $(LDFLAGS) -o $(CLI_BINARY) ./cmd/cli

# ========================================
# IMPORTANT: Cross-platform binary distribution
# ========================================
# RocksKV Storage requires RocksDB (CGO dependency).
# Binaries MUST be built on the target platform with CGO_ENABLED=1.
#
# For distribution, use ONE of the following approaches:
#   1. Docker images (recommended) - see deploy/Dockerfile.*
#   2. Build on target platform (Linux: use CI/CD on Linux runners)
#   3. Platform-specific package managers (apt, yum, homebrew)
#
# Cross-platform build targets (build-all, build-linux-*, etc.) have been
# removed because CGO binaries cannot be cross-compiled without the target
# platform's RocksDB libraries.
# ========================================

# 运行测试
test:
	@echo "Running tests..."
	CGO_ENABLED=1 $(GOTEST) -v -race -cover ./...

# 运行特定包的测试
test-storage:
	CGO_ENABLED=1 $(GOTEST) -v -race -cover ./pkg/storage/...

test-compute:
	CGO_ENABLED=1 $(GOTEST) -v -race -cover ./pkg/compute/...

test-metadata:
	CGO_ENABLED=1 $(GOTEST) -v -race -cover ./pkg/metadata/...

# 运行集成测试（自动启动和清理 etcd）
test-integration:
	@echo "Starting etcd for integration tests..."
	@./scripts/start-etcd.sh > /tmp/etcd-test.log 2>&1 & echo $$! > /tmp/etcd-test.pid
	@sleep 3
	@if ! lsof -ti:2379 > /dev/null 2>&1; then \
		echo "❌ etcd failed to start"; \
		cat /tmp/etcd-test.log; \
		exit 1; \
	fi
	@echo "✅ etcd started (PID: $$(cat /tmp/etcd-test.pid))"
	@echo "Running integration tests..."
	@CGO_ENABLED=1 $(GOTEST) -v -tags integration ./pkg/storage/... ./pkg/metadata/... || (make test-integration-cleanup && exit 1)
	@make test-integration-cleanup

# 清理集成测试环境
test-integration-cleanup:
	@echo "Cleaning up integration test environment..."
	@if [ -f /tmp/etcd-test.pid ]; then \
		kill $$(cat /tmp/etcd-test.pid) 2>/dev/null || true; \
		rm -f /tmp/etcd-test.pid; \
	fi
	@lsof -ti:2379,2380 | xargs kill -9 2>/dev/null || true
	@rm -rf ./data/etcd /tmp/etcd-test.log
	@echo "✅ Cleanup complete"

# 清理
clean:
	@echo "Cleaning..."
	$(GOCLEAN)
	rm -rf $(BINDIR)
	rm -rf $(PROTO_OUT)/*.go

# 依赖管理
deps:
	$(GOMOD) download
	$(GOMOD) tidy

# 格式化代码
fmt:
	@echo "Formatting code..."
	gofmt -s -w .

# 代码检查
lint:
	@echo "Running linter..."
	golangci-lint run ./...

# 生成模拟文件 (用于测试)
mocks:
	@echo "Generating mocks..."
	mockgen -source=pkg/storage/rocksdb.go -destination=pkg/storage/mocks/rocksdb_mock.go
	mockgen -source=pkg/metadata/store.go -destination=pkg/metadata/mocks/store_mock.go

# Docker 构建
docker-build:
	docker build -t rockskv-storage -f deploy/Dockerfile.storage .
	docker build -t rockskv-compute -f deploy/Dockerfile.compute .
	docker build -t rockskv-metadata -f deploy/Dockerfile.metadata .

# 帮助信息
help:
	@echo "RocksKV Makefile targets:"
	@echo ""
	@echo "Build targets (current platform only):"
	@echo "  build            - Build all services with CGO_ENABLED=1 (requires RocksDB)"
	@echo "  build-storage    - Build storage service"
	@echo "  build-compute    - Build compute service"
	@echo "  build-metadata   - Build metadata service"
	@echo "  build-cli        - Build CLI client"
	@echo ""
	@echo "Distribution:"
	@echo "  docker-build     - Build Docker images (recommended for distribution)"
	@echo "  NOTE: Cross-platform binaries removed - use Docker or build on target platform"
	@echo ""
	@echo "Testing:"
	@echo "  test                    - Run all unit tests (CGO_ENABLED=1)"
	@echo "  test-integration        - Run integration tests with etcd (auto cleanup)"
	@echo "  test-integration-cleanup - Manually cleanup integration test environment"
	@echo ""
	@echo "Other targets:"
	@echo "  proto          - Generate protobuf code"
	@echo "  clean          - Clean build artifacts"
	@echo "  deps           - Download dependencies"
	@echo "  fmt            - Format code"
	@echo "  lint           - Run linter"
