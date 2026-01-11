"""
RocksKV Python SDK

A high-performance Python client for RocksKV distributed key-value store.

Example:
    from rockskv import RocksKVClient

    client = RocksKVClient(["localhost:8000"])
    client.put("key", "value")
    value = client.get("key")
"""

from .client import RocksKVClient, AsyncRocksKVClient
from .config import ClientConfig
from .exceptions import RocksKVError, KeyNotFoundError, ConnectionError, TimeoutError

__version__ = "0.1.0"
__all__ = [
    "RocksKVClient",
    "AsyncRocksKVClient",
    "ClientConfig",
    "RocksKVError",
    "KeyNotFoundError",
    "ConnectionError",
    "TimeoutError",
]
