# SPDX-License-Identifier: Apache-2.0
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.support.waiting import wait_until

_EXTERNAL_ROOT = Path(__file__).resolve().parent


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    marker = pytest.mark.external
    for item in items:
        if item.path.is_relative_to(_EXTERNAL_ROOT):
            item.add_marker(marker)


@pytest.fixture(scope="module")
def redis_service():
    from redis import Redis
    from testcontainers.redis import RedisContainer

    container = RedisContainer("redis:7.2-alpine")
    container.start()
    client = Redis(
        host=container.get_container_host_ip(),
        port=int(container.get_exposed_port(6379)),
    )
    wait_until(client.ping, timeout=30, interval=0.2, description="Redis test container to accept connections")
    service = SimpleNamespace(
        host=client.connection_pool.connection_kwargs["host"],
        port=client.connection_pool.connection_kwargs["port"],
        client=client,
    )
    try:
        yield service
    finally:
        client.close()
        container.stop()


@pytest.fixture()
def clean_redis(redis_service):
    redis_service.client.flushdb()
    return redis_service
