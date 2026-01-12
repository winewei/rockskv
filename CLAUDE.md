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

#### IDE Configuration (VS Code / GoLand)

IDE language servers (gopls) may show false compilation errors because they don't inherit Makefile's CGO environment variables. Add these to your shell profile:

**macOS (add to ~/.zshrc or ~/.bashrc):**
```bash
# RocksDB CGO configuration for IDE
export CGO_ENABLED=1
export CGO_CFLAGS="-I$(brew --prefix)/include"
export CGO_LDFLAGS="-L$(brew --prefix)/lib"
```

**Linux (add to ~/.bashrc):**
```bash
# RocksDB CGO configuration for IDE
export CGO_ENABLED=1
export CGO_CFLAGS="-I/usr/local/include"
export CGO_LDFLAGS="-L/usr/local/lib"
```

After adding, restart your terminal and IDE. Note: `make build` and `make test` work correctly without this - it only affects IDE diagnostics.

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

### Build Requirements

⚠️ **IMPORTANT**: RocksKV Storage service REQUIRES CGO and RocksDB.

All builds (development, testing, production) use real RocksDB with `CGO_ENABLED=1`.

**Prerequisites:**
- RocksDB 10.7.5+ must be installed on your system
- See installation instructions above for your platform

```bash
# All builds require RocksDB
make build              # Uses CGO_ENABLED=1 by default
go build ./cmd/storage  # Requires RocksDB libraries
```

## Build Commands

```bash
make build              # Build all services for current platform (CGO_ENABLED=1)
make build-storage      # Build only storage service
make build-compute      # Build only compute service
make build-metadata     # Build only metadata service
make build-cli          # Build only CLI client
```

**Note**: Cross-platform build targets have been removed. For distribution:
- Use Docker images (`make docker-build`) - recommended
- Build on target platform (Linux CI runners, macOS builders)
- Platform packages (apt, yum, homebrew)

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

## Quick Start with Docker

The fastest way to try RocksKV is using Docker Compose, which starts a complete multi-node cluster:

```bash
# Start the full cluster (1 etcd + 1 metadata + 3 storage + 2 compute nodes)
docker-compose up -d

# Wait for all services to be healthy (about 30 seconds)
docker-compose ps

# Use the CLI to interact with the cluster
docker-compose exec cli rockskv-cli put hello world
docker-compose exec cli rockskv-cli get hello
# Output: world

docker-compose exec cli rockskv-cli put user:1 '{"name":"Alice","age":30}'
docker-compose exec cli rockskv-cli get user:1

# View cluster status
docker-compose exec cli rockskv-cli routes

# View service logs
docker-compose logs -f metadata
docker-compose logs -f storage-1

# Stop and cleanup
docker-compose down -v
```

**Docker Cluster Architecture:**

```
┌─────────────────────────────────────────────────────────────────┐
│                        Docker Network                            │
│                                                                  │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐     │
│  │ compute-1│   │ compute-2│   │   cli    │   │   etcd   │     │
│  │  :8000   │   │  :8001   │   │          │   │  :2379   │     │
│  └────┬─────┘   └────┬─────┘   └──────────┘   └────┬─────┘     │
│       │              │                              │           │
│       └──────────────┼──────────────────────────────┼───────┐   │
│                      │                              │       │   │
│  ┌───────────────────┴───────────────────┐    ┌─────┴─────┐ │   │
│  │              metadata:9000             │◄───│   etcd    │ │   │
│  └───────────────────┬───────────────────┘    └───────────┘ │   │
│                      │                                       │   │
│       ┌──────────────┼──────────────┐                       │   │
│       ▼              ▼              ▼                       │   │
│  ┌─────────┐   ┌─────────┐   ┌─────────┐                   │   │
│  │storage-1│   │storage-2│   │storage-3│                   │   │
│  │  :9001  │   │  :9002  │   │  :9003  │                   │   │
│  └─────────┘   └─────────┘   └─────────┘                   │   │
└─────────────────────────────────────────────────────────────────┘
```

## Running Locally (Without Docker)

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
| Storage-3 | 9003 | 9093 |
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

## SDK Integration

RocksKV provides SDKs for multiple languages via gRPC. All SDKs use `proto/rockskv.proto`.

### Python

```bash
pip install grpcio grpcio-tools
cd sdk/python
python -m grpc_tools.protoc -I../../proto --python_out=rockskv --grpc_python_out=rockskv ../../proto/rockskv.proto
```

```python
from rockskv import RocksKVClient

with RocksKVClient(["localhost:8000"]) as client:
    client.put("hello", "world")
    print(client.get("hello"))  # world
```

### Go

```go
import "github.com/winewei/rockskv/sdk/go/rockskv"

client, _ := rockskv.NewClient(rockskv.WithAddrs("localhost:8000"))
defer client.Close()

client.Put(ctx, "hello", "world")
value, _ := client.Get(ctx, "hello")
```

### Java

```java
RocksKVClient client = RocksKVClient.builder()
    .addAddress("localhost:8000")
    .build();

client.put("hello", "world");
String value = client.get("hello").orElse(null);
```

### Other Languages

Generate gRPC client from `proto/rockskv.proto` using protoc.

See `sdk/` directory for complete documentation and examples.

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

## Metadata High Availability (HA)

The metadata service supports high availability through leader election using etcd.

### HA Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                       Metadata Cluster (HA)                       │
│                                                                   │
│  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐            │
│  │ Metadata-1  │   │ Metadata-2  │   │ Metadata-3  │            │
│  │  (Leader)   │   │ (Follower)  │   │ (Follower)  │            │
│  │   :9000     │   │   :9010     │   │   :9020     │            │
│  └──────┬──────┘   └──────┬──────┘   └──────┬──────┘            │
│         │                 │                 │                    │
│         └─────────────────┼─────────────────┘                    │
│                           │                                      │
│                    ┌──────┴──────┐                               │
│                    │    etcd     │                               │
│                    │   :2379     │                               │
│                    └─────────────┘                               │
└─────────────────────────────────────────────────────────────────┘
```

### Features

- **Leader Election**: Uses etcd concurrency.Election for leader election
- **Automatic Failover**: If leader fails, a new leader is elected automatically
- **Write Forwarding**: Only the leader can process write operations (RegisterNode, InitCluster, TriggerRebalance, etc.)
- **Read Scaling**: All nodes can serve read operations (GetRouteTable, GetClusterInfo, etc.)
- **Leader Discovery**: Clients can use `GetLeaderInfo` RPC to discover the current leader

### Configuration

Enable HA mode in the metadata configuration:

```yaml
# config/local/metadata.yaml
node_id: "metadata-1"
listen_addr: ":9000"

etcd:
  endpoints:
    - "localhost:2379"
  dial_timeout: "5s"

# Enable HA mode
ha_enabled: true
```

### Deploying Multiple Metadata Nodes

1. Create separate config files for each node with unique `node_id` and `listen_addr`
2. Start each node with its respective config
3. All nodes connect to the same etcd cluster for coordination

Example for 3-node HA cluster:

```bash
# Node 1 (metadata-1.yaml)
node_id: "metadata-1"
listen_addr: ":9000"
ha_enabled: true

# Node 2 (metadata-2.yaml)
node_id: "metadata-2"
listen_addr: ":9010"
ha_enabled: true

# Node 3 (metadata-3.yaml)
node_id: "metadata-3"
listen_addr: ":9020"
ha_enabled: true
```

### API Behavior in HA Mode

| Operation | Leader Only | All Nodes |
|-----------|-------------|-----------|
| RegisterNode | Yes | - |
| InitCluster | Yes | - |
| TriggerRebalance | Yes | - |
| CancelMigration | Yes | - |
| ShutdownNode | Yes | - |
| GetRouteTable | - | Yes |
| GetClusterInfo | - | Yes |
| GetMigrationStatus | - | Yes |
| GetLeaderInfo | - | Yes |
| Heartbeat | - | Yes |
| SubscribeRouteUpdates | - | Yes |

When a non-leader node receives a write request, it returns `FailedPrecondition` error with the current leader ID.

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