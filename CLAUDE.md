# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

RocksKV is a compute-storage separated distributed KV database built on RocksDB, designed as a low-cost alternative to DynamoDB.

## Architecture

```
                    ┌─────────────┐
                    │   Client    │
                    └──────┬──────┘
                           │
              ┌────────────┴────────────┐
              ▼                         ▼
       ┌────────────┐            ┌────────────┐
       │ Compute-1  │            │ Compute-2  │
       │  (:8000)   │            │  (:8001)   │
       └─────┬──────┘            └──────┬─────┘
             │                          │
             └────────────┬─────────────┘
                          │
         ┌────────────────┼────────────────┐
         ▼                ▼                ▼
  ┌────────────┐   ┌────────────┐   ┌────────────┐
  │ Storage-1  │   │ Storage-2  │   │  Metadata  │
  │  (:9001)   │   │  (:9002)   │   │  (:9000)   │
  └─────┬──────┘   └─────┬──────┘   └──────┬─────┘
        │                │                 │
        ▼                ▼                 ▼
     RocksDB          RocksDB            etcd
```

**Three-tier architecture:**
- **Compute Layer** (`pkg/compute/`): Stateless routing layer. Routes requests using xxhash, coordinates dual-replica sync writes. Horizontally scalable.
- **Storage Layer** (`pkg/storage/`): Stateful storage layer. RocksDB with WAL on EBS, data on NVMe. Subscribes to route table for partition assignment.
- **Metadata Service** (`pkg/metadata/`): Route table management, node registration, failover detection (backed by etcd)

**Key design decisions:**
- Fixed 4096 partitions: `partition_id = xxhash(key) % 4096`
- Each partition has Primary + Replica (dual-replica sync write - both must succeed)
- Partition keys in RocksDB: `p:<partition_id_4bytes>:<key>`
- All gRPC calls use 5s timeout
- Storage nodes subscribe to metadata for route table updates

## Dependencies

### RocksDB 10.7.5+ (Required for CGO mode)

This project uses [grocksdb](https://github.com/linxGnu/grocksdb) v1.10.4 which requires **RocksDB 10.x** (specifically 10.7.5 or higher recommended).

**Check your RocksDB version:**
```bash
# Linux
ldconfig -p | grep rocksdb

# macOS
brew list rocksdb --versions
```

#### Install on macOS (Homebrew)

```bash
brew install rocksdb
```

#### Install on Linux (Build from source)

```bash
# Install build dependencies
sudo apt-get update
sudo apt-get install -y build-essential libgflags-dev libsnappy-dev \
    zlib1g-dev libbz2-dev liblz4-dev libzstd-dev

# Clone RocksDB 10.7.5
cd /tmp
git clone --depth 1 --branch v10.7.5 https://github.com/facebook/rocksdb.git rocksdb-10.7.5
cd rocksdb-10.7.5

# Build shared library (takes 5-10 minutes)
make shared_lib -j$(nproc)

# Install to /usr/local
sudo make install-shared INSTALL_PATH=/usr/local
sudo ldconfig

# Verify installation
ldconfig -p | grep rocksdb
# Should show: librocksdb.so.10.7 => /usr/local/lib/librocksdb.so.10.7
```

#### Troubleshooting RocksDB

If you encounter linking errors, ensure old versions are removed:
```bash
# Remove old apt packages (if any)
sudo apt-get remove -y librocksdb-dev librocksdb8.9

# Check for conflicting libraries
ls -la /usr/lib/x86_64-linux-gnu/librocksdb* 2>/dev/null
# Remove any old versions if found

# Refresh library cache
sudo ldconfig
```

### Build Modes

- **CGO mode** (default): Uses real RocksDB, full functionality
- **nocgo mode**: Uses stub implementation, for CI/cross-compilation

```bash
# CGO mode (requires RocksDB installed)
CGO_ENABLED=1 go build ./...

# nocgo mode (no RocksDB required)
CGO_ENABLED=0 go build -tags=nocgo ./...
```

## Build Commands

```bash
make build              # Build all services for current platform
make build-storage      # Build only storage service
make build-compute      # Build only compute service
make build-metadata     # Build only metadata service
make build-cli          # Build only CLI client
make build-darwin-arm64 # Cross-compile for Apple Silicon
make build-linux-amd64  # Cross-compile for Linux x86_64 (nocgo mode)
```

## Test Commands

```bash
make test               # Run all tests with race detection and coverage
make test-storage       # Test only pkg/storage/
make test-compute       # Test only pkg/compute/
make test-metadata      # Test only pkg/metadata/

# Run a single test
go test -v -race ./pkg/compute/... -run TestCalculatePartition

# Run smoke tests (requires running services)
./scripts/smoke-test.sh
```

## Code Generation

```bash
make proto              # Regenerate protobuf code (requires protoc)
```

Proto definitions are in `proto/rockskv.proto`, generated Go code goes to `pkg/proto/`.

## Running Locally

```bash
# Start etcd first (via Docker)
docker-compose up -d etcd

# Start all services
./scripts/start-all.sh

# Check status
./scripts/status.sh

# Stop all services
./scripts/stop-all.sh
```

**Service ports:**

| Service | Port | Metrics Port |
|---------|------|--------------|
| etcd | 2379 | - |
| Metadata | 9000 | 9090 |
| Storage-1 | 9001 | 9091 |
| Storage-2 | 9002 | 9092 |
| Compute-1 | 8000 | 8090 |
| Compute-2 | 8001 | 8091 |

## CLI Usage

```bash
# Basic operations (default: localhost:8000)
./bin/darwin-arm64/rockskv-cli put key value
./bin/darwin-arm64/rockskv-cli get key
./bin/darwin-arm64/rockskv-cli delete key

# Connect to specific compute node
./bin/darwin-arm64/rockskv-cli -addr localhost:8001 get key

# Batch operations
./bin/darwin-arm64/rockskv-cli mset k1 v1 k2 v2
./bin/darwin-arm64/rockskv-cli mget k1 k2
```

## Code Patterns

- Logging: Use `common.NewLogger("component-name")` (zap-based, outputs to stderr)
- Metrics: Use `common.RequestCounter`, `common.RequestLatency`, etc. (prometheus)
- Configuration: viper-based YAML configs in `config/local/`
- gRPC services implement generated interfaces from `pkg/proto/`
- Storage has stub files (`*_stub.go`) for CGO-free builds

## Adding New Nodes

To add a new compute or storage node:

1. Create config file in `config/local/` (copy existing and modify node_id, ports)
2. Create start script in `scripts/` (copy existing and update config path)
3. Update `scripts/start-all.sh`, `scripts/stop-all.sh`, `scripts/status.sh`
4. Rebuild and restart: `make build-darwin-arm64 && ./scripts/start-all.sh`

# 开发规范

## ⚠️ 强制规则

**每次修改代码后，必须立即运行 `make test`，如果失败则继续修复，循环直到测试全部通过。不要询问，直接执行。**

## 测试循环
```
修改代码 → make test → 失败 → 修复 → make test → ... → 通过 → 完成
```

## 常用命令

- `make test` - 单元测试
- `make build` - 编译
- `./scripts/smoke-test.sh` - 集成测试