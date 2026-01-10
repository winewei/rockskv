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

## Build Commands

```bash
make build              # Build all services for current platform
make build-storage      # Build only storage service
make build-compute      # Build only compute service
make build-metadata     # Build only metadata service
make build-cli          # Build only CLI client
make build-darwin-arm64 # Cross-compile for Apple Silicon
make build-linux-amd64  # Cross-compile for Linux x86_64
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
