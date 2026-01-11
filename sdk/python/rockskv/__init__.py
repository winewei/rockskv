"""
RocksKV Python SDK

A high-performance Python client for RocksKV distributed key-value store.

Example:
    from rockskv import RocksKVClient

    # Using addresses list
    client = RocksKVClient(["localhost:8000"])
    client.put("key", "value")
    value = client.get("key")

    # Using connection URI
    from rockskv import create_client_from_uri
    client = create_client_from_uri("rockskv://localhost:8000,localhost:8001")
"""

from .client import RocksKVClient, AsyncRocksKVClient
from .config import ClientConfig
from .exceptions import RocksKVError, KeyNotFoundError, ConnectionError, TimeoutError
from .uri import parse_uri, create_client_from_uri, create_async_client_from_uri

__version__ = "0.1.0"
__all__ = [
    "RocksKVClient",
    "AsyncRocksKVClient",
    "ClientConfig",
    "RocksKVError",
    "KeyNotFoundError",
    "ConnectionError",
    "TimeoutError",
    "parse_uri",
    "create_client_from_uri",
    "create_async_client_from_uri",
]
