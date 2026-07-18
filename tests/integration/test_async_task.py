# SPDX-License-Identifier: Apache-2.0
import asyncio
from typing import Any

import aiohttp
import orjson
import pytest

from ray import serve
from ray.klein.api.data_stream import DataStream
from ray.klein.api.job_handle import JobHandle
from ray.klein.api.klein_context import KleinContext
from ray.klein.runtime.serve import numpy_encoder
from tests.support.streaming import LoopSourceFunction


@serve.deployment(name="mock_http_server", max_queued_requests=20000)
class MockHttpServer:
    async def __call__(self, request):
        data = await request.json()
        await asyncio.sleep(1)
        return data


app = MockHttpServer.options(name="mock_http_server").bind()


class AsyncMap:
    async def __call__(self, batch: dict[str, Any]) -> dict[str, Any]:
        async with (
            aiohttp.ClientSession() as session,
            session.post(
                "http://0.0.0.0:8000/",
                data=orjson.dumps(batch, default=numpy_encoder),
                headers={"Content-Type": "application/json"},
            ) as resp,
        ):
            return await resp.json()


@pytest.fixture(scope="module", autouse=True)
def mock_http_server(ray_cluster):
    serve.run(app, name="mock_http_server")
    yield
    serve.shutdown()


def test_async_task() -> None:
    """A finite source can call a real async Ray Serve deployment in batches."""

    context = KleinContext()
    stream: DataStream = context.source(
        LoopSourceFunction,
        num_cpus=0.1,
        fn_constructor_kwargs={"sleep_interval": 0, "record_num": 6},
    )
    stream = stream.map_batches(AsyncMap, num_cpus=0.1, concurrency=1, batch_size=2, async_buffer_size=1)
    stream.show()

    client: JobHandle = context.execute()
    client.wait()
