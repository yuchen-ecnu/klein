# SPDX-License-Identifier: Apache-2.0
import os
import socket
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

    if os.environ.get("KLEIN_TEST_EMBEDDED_SERVICES") == "1":
        from redislite import Redis as EmbeddedRedis

        probe = socket.socket()
        probe.bind(("127.0.0.1", 0))
        port = int(probe.getsockname()[1])
        probe.close()
        owner = EmbeddedRedis(serverconfig={"bind": "127.0.0.1", "port": str(port)})
        client = Redis(host="127.0.0.1", port=port)
        wait_until(client.ping, timeout=30, interval=0.2, description="embedded Redis to accept connections")
        try:
            yield SimpleNamespace(host="127.0.0.1", port=port, client=client)
        finally:
            client.close()
            owner.shutdown()
            owner.close()
        return

    from testcontainers.core.container import DockerContainer
    from testcontainers.core.wait_strategies import LogMessageWaitStrategy

    container = (
        DockerContainer("redis:7.2-alpine")
        .with_exposed_ports(6379)
        .waiting_for(LogMessageWaitStrategy("Ready to accept connections"))
    )
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
