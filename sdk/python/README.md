# RocksKV Python SDK

A high-performance Python client for RocksKV distributed key-value store.

## Installation

```bash
pip install grpcio grpcio-tools

# Generate proto files (one-time setup)
cd sdk/python
python -m grpc_tools.protoc -I../../proto --python_out=. --grpc_python_out=. ../../proto/rockskv.proto
```

## Quick Start

```python
from rockskv import RocksKVClient

# Connect to cluster
client = RocksKVClient(["localhost:8000", "localhost:8001"])

# Basic operations
client.put("user:1", '{"name": "Alice", "age": 30}')
value = client.get("user:1")
print(value)  # {"name": "Alice", "age": 30}

# Batch operations
client.mset({
    "key1": "value1",
    "key2": "value2",
    "key3": "value3",
})
results = client.mget(["key1", "key2", "key3"])

# Delete
client.delete("user:1")

# Close connection
client.close()
```

## With Context Manager

```python
from rockskv import RocksKVClient

with RocksKVClient(["localhost:8000"]) as client:
    client.put("hello", "world")
    print(client.get("hello"))
```

## Configuration

```python
from rockskv import RocksKVClient, ClientConfig

config = ClientConfig(
    timeout=5.0,           # Request timeout in seconds
    pool_size=10,          # Connection pool size per host
    retry_count=3,         # Max retry attempts
    retry_delay=0.1,       # Initial retry delay in seconds
)

client = RocksKVClient(
    addrs=["localhost:8000", "localhost:8001"],
    config=config,
)
```

## Async Support

```python
import asyncio
from rockskv import AsyncRocksKVClient

async def main():
    async with AsyncRocksKVClient(["localhost:8000"]) as client:
        await client.put("key", "value")
        value = await client.get("key")
        print(value)

asyncio.run(main())
```

## Error Handling

```python
from rockskv import RocksKVClient, RocksKVError, KeyNotFoundError

client = RocksKVClient(["localhost:8000"])

try:
    value = client.get("nonexistent")
except KeyNotFoundError:
    print("Key not found")
except RocksKVError as e:
    print(f"RocksKV error: {e}")
```
