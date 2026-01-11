"""RocksKV client implementation."""

from __future__ import annotations

import random
import time
from typing import Dict, List, Optional, Union

import grpc

from .config import ClientConfig
from .exceptions import (
    RocksKVError,
    KeyNotFoundError,
    ConnectionError,
    TimeoutError,
)

# Import generated proto files
try:
    from . import rockskv_pb2
    from . import rockskv_pb2_grpc
except ImportError:
    # Proto files not generated yet
    rockskv_pb2 = None
    rockskv_pb2_grpc = None


class RocksKVClient:
    """Synchronous RocksKV client.

    Example:
        client = RocksKVClient(["localhost:8000", "localhost:8001"])
        client.put("key", "value")
        value = client.get("key")
        client.close()

    With context manager:
        with RocksKVClient(["localhost:8000"]) as client:
            client.put("key", "value")
    """

    def __init__(
        self,
        addrs: List[str],
        config: Optional[ClientConfig] = None,
    ):
        """Initialize RocksKV client.

        Args:
            addrs: List of compute node addresses (e.g., ["localhost:8000"])
            config: Client configuration (optional)
        """
        if not addrs:
            raise ValueError("At least one address is required")

        if rockskv_pb2 is None:
            raise ImportError(
                "Proto files not found. Please generate them first:\n"
                "python -m grpc_tools.protoc -I../../proto "
                "--python_out=. --grpc_python_out=. ../../proto/rockskv.proto"
            )

        self._addrs = addrs
        self._config = config or ClientConfig()
        self._channels: Dict[str, grpc.Channel] = {}
        self._stubs: Dict[str, rockskv_pb2_grpc.KVServiceStub] = {}
        self._closed = False

        # Initialize connections
        for addr in addrs:
            self._connect(addr)

    def _connect(self, addr: str) -> None:
        """Create connection to a compute node."""
        options = self._config.to_grpc_options()
        channel = grpc.insecure_channel(addr, options=options)
        self._channels[addr] = channel
        self._stubs[addr] = rockskv_pb2_grpc.KVServiceStub(channel)

    def _get_stub(self) -> rockskv_pb2_grpc.KVServiceStub:
        """Get a random stub for load balancing."""
        if not self._stubs:
            raise ConnectionError("No available connections")
        addr = random.choice(list(self._stubs.keys()))
        return self._stubs[addr]

    def _call_with_retry(self, method, request):
        """Execute a gRPC call with retry logic."""
        last_error = None
        delay = self._config.retry_delay

        for attempt in range(self._config.retry_count + 1):
            try:
                stub = self._get_stub()
                return method(stub)(request, timeout=self._config.timeout)
            except grpc.RpcError as e:
                last_error = e
                if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                    raise TimeoutError(f"Request timed out: {e.details()}")
                if e.code() == grpc.StatusCode.UNAVAILABLE:
                    if attempt < self._config.retry_count:
                        time.sleep(delay)
                        delay = min(delay * 2, self._config.max_retry_delay)
                        continue
                raise RocksKVError(f"gRPC error: {e.code()} - {e.details()}")

        raise ConnectionError(f"All retries failed: {last_error}")

    def get(self, key: Union[str, bytes]) -> Optional[bytes]:
        """Get value by key.

        Args:
            key: The key to get

        Returns:
            The value as bytes, or None if not found

        Raises:
            RocksKVError: If the operation fails
        """
        if isinstance(key, str):
            key = key.encode('utf-8')

        request = rockskv_pb2.GetRequest(key=key)
        response = self._call_with_retry(lambda s: s.Get, request)

        if not response.found:
            return None
        return response.value

    def put(self, key: Union[str, bytes], value: Union[str, bytes]) -> bool:
        """Put a key-value pair.

        Args:
            key: The key to set
            value: The value to set

        Returns:
            True if successful

        Raises:
            RocksKVError: If the operation fails
        """
        if isinstance(key, str):
            key = key.encode('utf-8')
        if isinstance(value, str):
            value = value.encode('utf-8')

        request = rockskv_pb2.PutRequest(key=key, value=value)
        response = self._call_with_retry(lambda s: s.Put, request)
        return response.success

    def delete(self, key: Union[str, bytes]) -> bool:
        """Delete a key.

        Args:
            key: The key to delete

        Returns:
            True if successful

        Raises:
            RocksKVError: If the operation fails
        """
        if isinstance(key, str):
            key = key.encode('utf-8')

        request = rockskv_pb2.DeleteRequest(key=key)
        response = self._call_with_retry(lambda s: s.Delete, request)
        return response.success

    def mget(self, keys: List[Union[str, bytes]]) -> Dict[bytes, Optional[bytes]]:
        """Get multiple values.

        Args:
            keys: List of keys to get

        Returns:
            Dict mapping keys to values (None for missing keys)

        Raises:
            RocksKVError: If the operation fails
        """
        encoded_keys = []
        for key in keys:
            if isinstance(key, str):
                key = key.encode('utf-8')
            encoded_keys.append(key)

        request = rockskv_pb2.BatchGetRequest(keys=encoded_keys)
        response = self._call_with_retry(lambda s: s.BatchGet, request)

        result = {}
        for item in response.items:
            result[item.key] = item.value if item.found else None
        return result

    def mset(self, items: Dict[Union[str, bytes], Union[str, bytes]]) -> int:
        """Set multiple key-value pairs.

        Args:
            items: Dict of key-value pairs

        Returns:
            Number of keys set

        Raises:
            RocksKVError: If the operation fails
        """
        kv_items = []
        for key, value in items.items():
            if isinstance(key, str):
                key = key.encode('utf-8')
            if isinstance(value, str):
                value = value.encode('utf-8')
            kv_items.append(rockskv_pb2.KeyValue(key=key, value=value))

        request = rockskv_pb2.BatchPutRequest(items=kv_items)
        response = self._call_with_retry(lambda s: s.BatchPut, request)
        return response.count

    def close(self) -> None:
        """Close all connections."""
        if self._closed:
            return
        self._closed = True
        for channel in self._channels.values():
            channel.close()
        self._channels.clear()
        self._stubs.clear()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


class AsyncRocksKVClient:
    """Asynchronous RocksKV client.

    Example:
        async with AsyncRocksKVClient(["localhost:8000"]) as client:
            await client.put("key", "value")
            value = await client.get("key")
    """

    def __init__(
        self,
        addrs: List[str],
        config: Optional[ClientConfig] = None,
    ):
        """Initialize async RocksKV client."""
        if not addrs:
            raise ValueError("At least one address is required")

        if rockskv_pb2 is None:
            raise ImportError(
                "Proto files not found. Please generate them first."
            )

        self._addrs = addrs
        self._config = config or ClientConfig()
        self._channels: Dict[str, grpc.aio.Channel] = {}
        self._stubs: Dict[str, rockskv_pb2_grpc.KVServiceStub] = {}
        self._closed = False

    async def _connect(self, addr: str) -> None:
        """Create async connection to a compute node."""
        options = self._config.to_grpc_options()
        channel = grpc.aio.insecure_channel(addr, options=options)
        self._channels[addr] = channel
        self._stubs[addr] = rockskv_pb2_grpc.KVServiceStub(channel)

    def _get_stub(self) -> rockskv_pb2_grpc.KVServiceStub:
        """Get a random stub for load balancing."""
        if not self._stubs:
            raise ConnectionError("No available connections")
        addr = random.choice(list(self._stubs.keys()))
        return self._stubs[addr]

    async def get(self, key: Union[str, bytes]) -> Optional[bytes]:
        """Get value by key (async)."""
        if isinstance(key, str):
            key = key.encode('utf-8')

        request = rockskv_pb2.GetRequest(key=key)
        stub = self._get_stub()
        response = await stub.Get(request, timeout=self._config.timeout)

        if not response.found:
            return None
        return response.value

    async def put(self, key: Union[str, bytes], value: Union[str, bytes]) -> bool:
        """Put a key-value pair (async)."""
        if isinstance(key, str):
            key = key.encode('utf-8')
        if isinstance(value, str):
            value = value.encode('utf-8')

        request = rockskv_pb2.PutRequest(key=key, value=value)
        stub = self._get_stub()
        response = await stub.Put(request, timeout=self._config.timeout)
        return response.success

    async def delete(self, key: Union[str, bytes]) -> bool:
        """Delete a key (async)."""
        if isinstance(key, str):
            key = key.encode('utf-8')

        request = rockskv_pb2.DeleteRequest(key=key)
        stub = self._get_stub()
        response = await stub.Delete(request, timeout=self._config.timeout)
        return response.success

    async def close(self) -> None:
        """Close all connections."""
        if self._closed:
            return
        self._closed = True
        for channel in self._channels.values():
            await channel.close()
        self._channels.clear()
        self._stubs.clear()

    async def __aenter__(self):
        for addr in self._addrs:
            await self._connect(addr)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        return False
