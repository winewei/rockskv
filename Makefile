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

# 构建所有平台的二进制文件
build-all:
	@echo "Building for all platforms..."
	@for platform in $(PLATFORMS); do \
		GOOS=$$(echo $$platform | cut -d'/' -f1); \
		GOARCH=$$(echo $$platform | cut -d'/' -f2); \
		output_dir=$(BINDIR)/$$GOOS-$$GOARCH; \
		mkdir -p $$output_dir; \
		echo "Building for $$GOOS/$$GOARCH..."; \
		GOOS=$$GOOS GOARCH=$$GOARCH CGO_ENABLED=0 $(GOBUILD) $(LDFLAGS) -o $$output_dir/rockskv-storage ./cmd/storage; \
		GOOS=$$GOOS GOARCH=$$GOARCH CGO_ENABLED=0 $(GOBUILD) $(LDFLAGS) -o $$output_dir/rockskv-compute ./cmd/compute; \
		GOOS=$$GOOS GOARCH=$$GOARCH CGO_ENABLED=0 $(GOBUILD) $(LDFLAGS) -o $$output_dir/rockskv-metadata ./cmd/metadata; \
		GOOS=$$GOOS GOARCH=$$GOARCH CGO_ENABLED=0 $(GOBUILD) $(LDFLAGS) -o $$output_dir/rockskv-cli ./cmd/cli; \
	done
	@echo "Build complete. Binaries are in $(BINDIR)/"

# 构建特定平台
build-linux-amd64:
	@echo "Building for linux/amd64..."
	@mkdir -p $(BINDIR)/linux-amd64
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 $(GOBUILD) $(LDFLAGS) -o $(BINDIR)/linux-amd64/rockskv-storage ./cmd/storage
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 $(GOBUILD) $(LDFLAGS) -o $(BINDIR)/linux-amd64/rockskv-compute ./cmd/compute
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 $(GOBUILD) $(LDFLAGS) -o $(BINDIR)/linux-amd64/rockskv-metadata ./cmd/metadata
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 $(GOBUILD) $(LDFLAGS) -o $(BINDIR)/linux-amd64/rockskv-cli ./cmd/cli

build-linux-arm64:
	@echo "Building for linux/arm64..."
	@mkdir -p $(BINDIR)/linux-arm64
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 $(GOBUILD) $(LDFLAGS) -o $(BINDIR)/linux-arm64/rockskv-storage ./cmd/storage
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 $(GOBUILD) $(LDFLAGS) -o $(BINDIR)/linux-arm64/rockskv-compute ./cmd/compute
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 $(GOBUILD) $(LDFLAGS) -o $(BINDIR)/linux-arm64/rockskv-metadata ./cmd/metadata
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 $(GOBUILD) $(LDFLAGS) -o $(BINDIR)/linux-arm64/rockskv-cli ./cmd/cli

build-darwin-amd64:
	@echo "Building for darwin/amd64..."
	@mkdir -p $(BINDIR)/darwin-amd64
	GOOS=darwin GOARCH=amd64 CGO_ENABLED=0 $(GOBUILD) $(LDFLAGS) -o $(BINDIR)/darwin-amd64/rockskv-storage ./cmd/storage
	GOOS=darwin GOARCH=amd64 CGO_ENABLED=0 $(GOBUILD) $(LDFLAGS) -o $(BINDIR)/darwin-amd64/rockskv-compute ./cmd/compute
	GOOS=darwin GOARCH=amd64 CGO_ENABLED=0 $(GOBUILD) $(LDFLAGS) -o $(BINDIR)/darwin-amd64/rockskv-metadata ./cmd/metadata
	GOOS=darwin GOARCH=amd64 CGO_ENABLED=0 $(GOBUILD) $(LDFLAGS) -o $(BINDIR)/darwin-amd64/rockskv-cli ./cmd/cli
	@if command -v codesign >/dev/null 2>&1; then \
		echo "Signing darwin/amd64 binaries..."; \
		codesign -s - $(BINDIR)/darwin-amd64/rockskv-*; \
	fi

build-darwin-arm64:
	@echo "Building for darwin/arm64 (Apple Silicon)..."
	@mkdir -p $(BINDIR)/darwin-arm64
	GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 $(GOBUILD) $(LDFLAGS) -o $(BINDIR)/darwin-arm64/rockskv-storage ./cmd/storage
	GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 $(GOBUILD) $(LDFLAGS) -o $(BINDIR)/darwin-arm64/rockskv-compute ./cmd/compute
	GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 $(GOBUILD) $(LDFLAGS) -o $(BINDIR)/darwin-arm64/rockskv-metadata ./cmd/metadata
	GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 $(GOBUILD) $(LDFLAGS) -o $(BINDIR)/darwin-arm64/rockskv-cli ./cmd/cli
	@if command -v codesign >/dev/null 2>&1; then \
		echo "Signing darwin/arm64 binaries..."; \
		codesign -s - $(BINDIR)/darwin-arm64/rockskv-*; \
	fi

# 运行测试
test:
	@echo "Running tests..."
	$(GOTEST) -v -race -cover ./...

# 运行特定包的测试
test-storage:
	$(GOTEST) -v -race -cover ./pkg/storage/...

test-compute:
	$(GOTEST) -v -race -cover ./pkg/compute/...

test-metadata:
	$(GOTEST) -v -race -cover ./pkg/metadata/...

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
	@echo "Build targets:"
	@echo "  build            - Build all services for current platform"
	@echo "  build-all        - Build all services for all platforms (linux/darwin, amd64/arm64)"
	@echo "  build-storage    - Build storage service"
	@echo "  build-compute    - Build compute service"
	@echo "  build-metadata   - Build metadata service"
	@echo "  build-cli        - Build CLI client"
	@echo ""
	@echo "Cross-compile targets:"
	@echo "  build-linux-amd64   - Build for Linux x86_64"
	@echo "  build-linux-arm64   - Build for Linux ARM64"
	@echo "  build-darwin-amd64  - Build for macOS x86_64"
	@echo "  build-darwin-arm64  - Build for macOS ARM64 (Apple Silicon M1/M2)"
	@echo ""
	@echo "Other targets:"
	@echo "  proto          - Generate protobuf code"
	@echo "  test           - Run all tests"
	@echo "  clean          - Clean build artifacts"
	@echo "  deps           - Download dependencies"
	@echo "  fmt            - Format code"
	@echo "  lint           - Run linter"
