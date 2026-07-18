# SPDX-License-Identifier: Apache-2.0
"""Redis missing-key filter transform."""

import time

from redis import RedisError

from ray.klein._internal.logging import get_logger
from ray.klein.api.runtime_context import RuntimeContext
from ray.klein.integrations.redis._lookup_client import LookupKeyExtractor, Record, _RedisLookupClient
from ray.klein.integrations.redis.redis_connection_config import RedisConnectionConfig

logger = get_logger(__name__)


class RedisMissingKeyFilter:
    """Keep records whose extracted key does not exist in Redis."""

    def __init__(
        self,
        connection: RedisConnectionConfig,
        key: LookupKeyExtractor,
        *,
        key_prefix: str | None = None,
        delimiter: str = ":",
        runtime_context: RuntimeContext | None = None,
    ) -> None:
        self._lookup = _RedisLookupClient(
            connection,
            key,
            key_prefix=key_prefix,
            delimiter=delimiter,
            runtime_context=runtime_context,
        )

    def __call__(self, record: Record) -> bool | list[bool]:
        keys = self._lookup.resolve_keys(record)
        if isinstance(keys, list):
            return self._filter_batch(keys)
        return self._filter_one(keys)

    def _filter_one(self, key: str) -> bool:
        started_at = time.monotonic()
        try:
            with self._lookup.client() as client:
                exists = client.exists(key)
        except (RedisError, OSError):
            self._lookup.record_failure()
            logger.exception("Redis existence lookup failed for key %r", key)
            raise
        self._lookup.record_success(started_at)
        return not bool(exists)

    def _filter_batch(self, keys: list[str]) -> list[bool]:
        started_at = time.monotonic()
        try:
            with (
                self._lookup.client() as client,
                client.pipeline(transaction=False) as pipeline,
            ):
                for key in keys:
                    pipeline.exists(key)
                responses = pipeline.execute()
        except (RedisError, OSError):
            self._lookup.record_failure()
            logger.exception("Redis existence lookup failed for %d keys", len(keys))
            raise
        self._lookup.record_success(started_at, len(keys))
        return [not bool(response) for response in responses]

    def close(self) -> None:
        self._lookup.close()
