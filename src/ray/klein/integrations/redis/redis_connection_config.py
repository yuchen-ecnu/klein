# SPDX-License-Identifier: Apache-2.0
"""Connection settings for Redis integrations."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

import redis
from redis import RedisError
from redis.backoff import ExponentialBackoff
from redis.retry import Retry

from ray.klein._internal.frozen_mapping import FrozenMapping
from ray.klein.integrations.redis._validation import positive_seconds


@dataclass(frozen=True, slots=True)
class RedisConnectionConfig:
    """Connection and retry settings shared by Redis operators."""

    host: str
    port: int = 6379
    database: int = 0
    username: str | None = None
    password: str | None = field(default=None, repr=False)
    max_connections: int = 10
    timeout: timedelta = timedelta(seconds=5)
    max_retries: int = 5
    max_retry_delay: timedelta = timedelta(seconds=10)
    connection_options: Mapping[str, Any] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.host, str) or not self.host.strip():
            raise ValueError("host must be a non-empty string")
        if not 1 <= self.port <= 65535:
            raise ValueError("port must be between 1 and 65535")
        if self.database < 0:
            raise ValueError("database must be non-negative")
        if self.max_connections <= 0:
            raise ValueError("max_connections must be positive")
        if self.max_retries < 0:
            raise ValueError("max_retries must be non-negative")
        positive_seconds(self.timeout, "timeout")
        positive_seconds(self.max_retry_delay, "max_retry_delay")
        object.__setattr__(self, "connection_options", FrozenMapping(self.connection_options))

    def create_pool(self, *, retries: int | None = None) -> redis.BlockingConnectionPool:
        """Create a pool owned by one operator instance."""

        retry_count = self.max_retries if retries is None else retries
        if retry_count < 0:
            raise ValueError("retries must be non-negative")

        timeout = self.timeout.total_seconds()
        options = dict(self.connection_options)
        options.update(
            host=self.host,
            port=self.port,
            db=self.database,
            username=self.username,
            password=self.password,
            max_connections=self.max_connections,
            socket_timeout=timeout,
            socket_connect_timeout=timeout,
            retry=Retry(
                ExponentialBackoff(self.max_retry_delay.total_seconds(), 0.1),
                retry_count,
                (RedisError,),
            ),
        )
        return redis.BlockingConnectionPool(**options)
