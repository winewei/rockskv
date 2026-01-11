"""RocksKV client configuration."""

from dataclasses import dataclass
from typing import Optional


@dataclass
class ClientConfig:
    """Configuration for RocksKV client.

    Attributes:
        timeout: Request timeout in seconds (default: 5.0)
        pool_size: Connection pool size per host (default: 10)
        retry_count: Maximum retry attempts (default: 3)
        retry_delay: Initial retry delay in seconds (default: 0.1)
        max_retry_delay: Maximum retry delay in seconds (default: 2.0)
        keepalive_time: Keepalive ping interval in seconds (default: 30)
        keepalive_timeout: Keepalive ping timeout in seconds (default: 5)
    """
    timeout: float = 5.0
    pool_size: int = 10
    retry_count: int = 3
    retry_delay: float = 0.1
    max_retry_delay: float = 2.0
    keepalive_time: int = 30
    keepalive_timeout: int = 5

    def to_grpc_options(self) -> list:
        """Convert to gRPC channel options."""
        return [
            ('grpc.keepalive_time_ms', self.keepalive_time * 1000),
            ('grpc.keepalive_timeout_ms', self.keepalive_timeout * 1000),
            ('grpc.keepalive_permit_without_calls', True),
            ('grpc.http2.max_pings_without_data', 0),
        ]
