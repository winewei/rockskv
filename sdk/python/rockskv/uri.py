"""RocksKV connection URI parsing.

URI format: rockskv://host1:port1,host2:port2[?options]

Supported options:
    - timeout: request timeout in seconds (e.g., "5", "0.5")
    - retry_count: max retry attempts (e.g., "3")
    - retry_delay: initial retry delay in seconds (e.g., "0.1")
    - max_retry_delay: max retry delay in seconds (e.g., "2")
    - pool_size: connection pool size (e.g., "10")

Examples:
    rockskv://localhost:8000
    rockskv://localhost:8000,localhost:8001
    rockskv://node1:8000,node2:8000?timeout=5&retry_count=3
"""

from typing import List, Tuple
from urllib.parse import urlparse, parse_qs

from .config import ClientConfig


def parse_uri(uri: str) -> Tuple[List[str], ClientConfig]:
    """Parse a RocksKV connection URI.

    Args:
        uri: Connection URI string

    Returns:
        Tuple of (addresses, config)

    Raises:
        ValueError: If URI is invalid

    Examples:
        >>> addrs, config = parse_uri("rockskv://localhost:8000,localhost:8001")
        >>> addrs
        ['localhost:8000', 'localhost:8001']
    """
    # Handle different URI formats
    if uri.startswith("rockskv://"):
        uri = "http://" + uri[len("rockskv://"):]
    elif "://" not in uri:
        # Plain host:port format
        uri = "http://" + uri

    parsed = urlparse(uri)

    # Parse hosts
    if not parsed.netloc:
        raise ValueError("No hosts specified in URI")

    hosts = parsed.netloc.split(",")
    addrs = []
    for host in hosts:
        host = host.strip()
        if not host:
            raise ValueError("Empty address in URI")
        addrs.append(host)

    # Parse query options
    query = parse_qs(parsed.query)
    config = ClientConfig()

    if "timeout" in query:
        config.timeout = float(query["timeout"][0])

    if "retry_count" in query:
        config.retry_count = int(query["retry_count"][0])

    if "retry_delay" in query:
        config.retry_delay = float(query["retry_delay"][0])

    if "max_retry_delay" in query:
        config.max_retry_delay = float(query["max_retry_delay"][0])

    if "pool_size" in query:
        config.pool_size = int(query["pool_size"][0])

    return addrs, config


def create_client_from_uri(uri: str):
    """Create a RocksKV client from a connection URI.

    Args:
        uri: Connection URI string

    Returns:
        RocksKVClient instance

    Examples:
        >>> client = create_client_from_uri("rockskv://localhost:8000,localhost:8001")
        >>> client = create_client_from_uri("rockskv://node1:8000?timeout=5&retry_count=3")
    """
    from .client import RocksKVClient

    addrs, config = parse_uri(uri)
    return RocksKVClient(addrs, config)


def create_async_client_from_uri(uri: str):
    """Create an async RocksKV client from a connection URI.

    Args:
        uri: Connection URI string

    Returns:
        AsyncRocksKVClient instance

    Examples:
        >>> client = create_async_client_from_uri("rockskv://localhost:8000")
    """
    from .client import AsyncRocksKVClient

    addrs, config = parse_uri(uri)
    return AsyncRocksKVClient(addrs, config)
