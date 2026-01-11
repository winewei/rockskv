#!/usr/bin/env python3
"""
Async usage example for RocksKV Python SDK.

Before running, generate proto files:
    cd sdk/python
    python -m grpc_tools.protoc -I../../proto --python_out=rockskv --grpc_python_out=rockskv ../../proto/rockskv.proto

Then run:
    python examples/async_usage.py
"""

import asyncio
import sys
import os

# Add parent directory to path for local development
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from rockskv import AsyncRocksKVClient, ClientConfig


async def main():
    config = ClientConfig(timeout=5.0)

    async with AsyncRocksKVClient(["localhost:8000"], config=config) as client:
        print("Connected to RocksKV cluster (async)")

        # Basic async operations
        await client.put("async_key", "async_value")
        print("PUT async_key = 'async_value'")

        value = await client.get("async_key")
        print(f"GET async_key = {value.decode('utf-8')}")

        # Concurrent operations
        print("\nRunning concurrent operations...")

        async def write_key(key: str, value: str):
            await client.put(key, value)
            return key

        async def read_key(key: str):
            value = await client.get(key)
            return (key, value.decode('utf-8') if value else None)

        # Write 100 keys concurrently
        keys = [f"concurrent_key_{i}" for i in range(100)]
        write_tasks = [write_key(k, f"value_{i}") for i, k in enumerate(keys)]
        await asyncio.gather(*write_tasks)
        print(f"Written {len(keys)} keys concurrently")

        # Read all keys concurrently
        read_tasks = [read_key(k) for k in keys]
        results = await asyncio.gather(*read_tasks)
        print(f"Read {len(results)} keys concurrently")

        # Verify
        success_count = sum(1 for k, v in results if v is not None)
        print(f"Verified {success_count}/{len(keys)} keys")

        # Cleanup
        for key in keys:
            await client.delete(key)
        await client.delete("async_key")
        print("Cleaned up test keys")

        print("\nDone!")


if __name__ == "__main__":
    asyncio.run(main())
