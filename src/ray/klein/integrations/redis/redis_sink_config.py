# SPDX-License-Identifier: Apache-2.0
"""Write-shape and buffering settings for the Redis sink."""

from dataclasses import dataclass
from datetime import timedelta

from ray.klein.integrations.redis._validation import positive_seconds
from ray.klein.integrations.redis.data_type import RedisDataType


@dataclass(frozen=True, slots=True)
class RedisSinkConfig:
    """Write shape and buffering policy for :class:`RedisSink`."""

    data_type: RedisDataType = RedisDataType.STRING
    key_prefix: str | None = None
    delimiter: str = ":"
    ttl: timedelta | None = None
    batch_size: int = 100
    flush_interval: timedelta = timedelta(seconds=1)
    buffer_capacity: int = 5000

    def __post_init__(self) -> None:
        if not isinstance(self.data_type, RedisDataType):
            raise TypeError("data_type must be a RedisDataType")
        if self.key_prefix is not None and not self.key_prefix.strip():
            raise ValueError("key_prefix must be None or a non-empty string")
        if not self.delimiter:
            raise ValueError("delimiter must be non-empty")
        if self.ttl is not None:
            positive_seconds(self.ttl, "ttl")
        if self.batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if self.buffer_capacity < self.batch_size:
            raise ValueError("buffer_capacity must be greater than or equal to batch_size")
        positive_seconds(self.flush_interval, "flush_interval")
