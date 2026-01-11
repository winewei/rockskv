"""RocksKV exceptions."""


class RocksKVError(Exception):
    """Base exception for RocksKV errors."""
    pass


class KeyNotFoundError(RocksKVError):
    """Raised when a key is not found."""
    pass


class ConnectionError(RocksKVError):
    """Raised when connection fails."""
    pass


class TimeoutError(RocksKVError):
    """Raised when request times out."""
    pass


class PartitionNotFoundError(RocksKVError):
    """Raised when partition is not available."""
    pass
