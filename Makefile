.PHONY: proto build test clean all

# Go 参数
GOCMD=go
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test
GOCLEAN=$(GOCMD) clean
GOMOD=$(GOCMD) mod

# 二进制输出目录
BINDIR=bin

# 服务二进制
STORAGE_BINARY=$(BINDIR)/rockskv-storage
COMPUTE_BINARY=$(BINDIR)/rockskv-compute
METADATA_BINARY=$(BINDIR)/rockskv-metadata
CLI_BINARY=$(BINDIR)/rockskv-cli

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

# 构建所有服务
build: build-storage build-compute build-metadata build-cli

build-storage:
	@echo "Building storage service..."
	@mkdir -p $(BINDIR)
	$(GOBUILD) -o $(STORAGE_BINARY) ./cmd/storage

build-compute:
	@echo "Building compute service..."
	@mkdir -p $(BINDIR)
	$(GOBUILD) -o $(COMPUTE_BINARY) ./cmd/compute

build-metadata:
	@echo "Building metadata service..."
	@mkdir -p $(BINDIR)
	$(GOBUILD) -o $(METADATA_BINARY) ./cmd/metadata

build-cli:
	@echo "Building CLI..."
	@mkdir -p $(BINDIR)
	$(GOBUILD) -o $(CLI_BINARY) ./cmd/cli

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
	@echo "  proto          - Generate protobuf code"
	@echo "  build          - Build all services"
	@echo "  build-storage  - Build storage service"
	@echo "  build-compute  - Build compute service"
	@echo "  build-metadata - Build metadata service"
	@echo "  build-cli      - Build CLI client"
	@echo "  test           - Run all tests"
	@echo "  clean          - Clean build artifacts"
	@echo "  deps           - Download dependencies"
	@echo "  fmt            - Format code"
	@echo "  lint           - Run linter"
