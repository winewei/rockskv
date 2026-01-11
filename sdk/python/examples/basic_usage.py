#!/usr/bin/env python3
"""
Basic usage example for RocksKV Python SDK.

Before running, generate proto files:
    cd sdk/python
    python -m grpc_tools.protoc -I../../proto --python_out=rockskv --grpc_python_out=rockskv ../../proto/rockskv.proto

Then run:
    python examples/basic_usage.py
"""

import sys
import os

# Add parent directory to path for local development
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from rockskv import RocksKVClient, ClientConfig


def main():
    # Configuration (optional)
    config = ClientConfig(
        timeout=5.0,      # Request timeout
        retry_count=3,    # Retry attempts
        pool_size=10,     # Connection pool size
    )

    # Connect to RocksKV cluster
    # Using context manager for automatic cleanup
    with RocksKVClient(["localhost:8000"], config=config) as client:
        print("Connected to RocksKV cluster")

        # ========== Basic Operations ==========
        print("\n--- Basic Operations ---")

        # Put a value
        client.put("greeting", "Hello, RocksKV!")
        print("PUT greeting = 'Hello, RocksKV!'")

        # Get a value
        value = client.get("greeting")
        print(f"GET greeting = {value.decode('utf-8')}")

        # Delete a value
        client.delete("greeting")
        print("DELETE greeting")

        # Get non-existent key
        value = client.get("greeting")
        print(f"GET greeting (after delete) = {value}")

        # ========== JSON Data ==========
        print("\n--- JSON Data ---")

        import json

        user = {
            "id": 1,
            "name": "Alice",
            "email": "alice@example.com",
            "age": 30,
        }

        # Store JSON
        client.put("user:1", json.dumps(user))
        print(f"PUT user:1 = {user}")

        # Retrieve JSON
        value = client.get("user:1")
        retrieved_user = json.loads(value.decode('utf-8'))
        print(f"GET user:1 = {retrieved_user}")

        # ========== Batch Operations ==========
        print("\n--- Batch Operations ---")

        # Batch put (mset)
        items = {
            "key1": "value1",
            "key2": "value2",
            "key3": "value3",
            "key4": "value4",
            "key5": "value5",
        }
        count = client.mset(items)
        print(f"MSET {len(items)} keys, stored {count}")

        # Batch get (mget)
        keys = ["key1", "key2", "key3", "key4", "key5", "nonexistent"]
        results = client.mget(keys)
        print(f"MGET {len(keys)} keys:")
        for key, value in results.items():
            if value is not None:
                print(f"  {key.decode()} = {value.decode()}")
            else:
                print(f"  {key.decode()} = (nil)")

        # ========== Cleanup ==========
        print("\n--- Cleanup ---")
        for key in ["user:1", "key1", "key2", "key3", "key4", "key5"]:
            client.delete(key)
        print("Cleaned up test keys")

        print("\nDone!")


if __name__ == "__main__":
    main()
