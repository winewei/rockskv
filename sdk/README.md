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

## Connection URI Format

All SDKs support MongoDB-style connection URIs:

```
rockskv://host1:port1,host2:port2,host3:port3[?options]
```

### Examples

```
# Single node
rockskv://localhost:8000

# Multiple nodes (recommended for production)
rockskv://node1:8000,node2:8000,node3:8000

# With options
rockskv://localhost:8000,localhost:8001?timeout=5000&retryCount=3&poolSize=20
```

### URI Options

| Parameter | Default | Description |
|-----------|---------|-------------|
| `timeout` | `5000` | Request timeout in milliseconds |
| `retryCount` | `3` | Max retry attempts |
| `retryDelay` | `100` | Initial retry delay in milliseconds |
| `maxRetryDelay` | `2000` | Max retry delay in milliseconds |
| `poolSize` | `10` | Connection pool size per host |

### Usage by Language

**Go:**
```go
client, err := rockskv.NewClientFromURI("rockskv://localhost:8000,localhost:8001")
```

**Python:**
```python
from rockskv import create_client_from_uri
client = create_client_from_uri("rockskv://localhost:8000,localhost:8001")
```

**Java:**
```java
RocksKVClient client = ConnectionUri.createClient("rockskv://localhost:8000,localhost:8001");
```

## Examples

See the `examples/` directory in each SDK for complete working examples.
