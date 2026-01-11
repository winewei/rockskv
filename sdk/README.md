# RocksKV SDK

RocksKV provides SDKs for multiple programming languages using gRPC.

## Quick Start

### Generate Client Code from Proto

All SDKs are generated from the same proto file: `proto/rockskv.proto`

```bash
# Install protoc and language-specific plugins first
# Then generate client code:

# Python
python -m grpc_tools.protoc -I../proto --python_out=. --grpc_python_out=. ../proto/rockskv.proto

# Go
protoc -I../proto --go_out=. --go-grpc_out=. ../proto/rockskv.proto

# Java
protoc -I../proto --java_out=. --grpc-java_out=. ../proto/rockskv.proto
```

## SDK Overview

| Language | Directory | Package Name | Status |
|----------|-----------|--------------|--------|
| Python | `sdk/python/` | `rockskv` | Ready |
| Go | `sdk/go/` | `github.com/winewei/rockskv/sdk/go/rockskv` | Ready |
| Java | `sdk/java/` | `com.rockskv.client` | Ready |

## Features

All SDKs provide:
- Connection pooling
- Automatic retry with backoff
- Timeout configuration
- Batch operations (mget/mset)
- Thread-safe operations

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Your Application                         │
├─────────────────────────────────────────────────────────────────┤
│  Python SDK  │   Go SDK    │   Java SDK   │   Other (gRPC)     │
├──────────────┴─────────────┴──────────────┴─────────────────────┤
│                          gRPC Protocol                           │
├─────────────────────────────────────────────────────────────────┤
│                    RocksKV Compute Layer                         │
│                      (Load Balancing)                            │
└─────────────────────────────────────────────────────────────────┘
```

## Connection String Format

```
rockskv://host1:8000,host2:8001?timeout=5s&pool_size=10
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `timeout` | `5s` | Request timeout |
| `pool_size` | `10` | Connection pool size per host |
| `retry` | `3` | Max retry attempts |

## Examples

See the `examples/` directory in each SDK for complete working examples.
