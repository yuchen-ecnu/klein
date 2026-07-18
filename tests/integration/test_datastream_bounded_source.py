# SPDX-License-Identifier: Apache-2.0
import os
from datetime import timedelta
from typing import Any
from unittest import TestCase
from unittest.mock import patch

import numpy as np

from ray.klein._internal.logging import get_logger
from ray.klein.api.job_handle import JobHandle
from ray.klein.api.klein_context import KleinContext
from ray.klein.api.sink_function import SinkFunction
from ray.klein.config.checkpoint_trigger_options import (
    CheckpointTriggerOptions,
)
from ray.klein.config.configuration import Configuration
from ray.klein.config.environment_variables import EnvironmentVariables

logger = get_logger(__name__)


class CollectionSinkFunction(SinkFunction):
    """
    CollectionSinkFunction.
    """

    def __init__(self, batch_size: int):
        self._batch_size = batch_size
        self.datasets = []

    def flush(self) -> None:
        logger.info("Force Flushing...")

    def write(self, value: dict[str, Any]) -> None:
        if self._batch_size == 1:
            assert not isinstance(value["id"], list)
        else:
            assert isinstance(value["id"], np.ndarray)
        logger.info("Received: %s", value)
        self.datasets.append(value)


class DatastreamBoundedSourceTest(TestCase):
    """
    DatastreamBatchTest.
    """

    @patch.dict(os.environ, {EnvironmentVariables.DEBUG: "1"})
    def test_datastream(self) -> None:
        config = Configuration()
        # Deterministic per-record barriers: count threshold 1, time disabled.
        config.set(CheckpointTriggerOptions.INTERVAL_RECORDS, 1)
        config.set(CheckpointTriggerOptions.INTERVAL_DURATION, timedelta(0))
        ctx = KleinContext(config)

        (
            ctx.from_values({"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}, {"id": 5})
            .map(
                lambda x: {"id": x["id"] * 2},
                num_cpus=0.1,
                concurrency=2,
                name="MapOperator",
            )
            .write(
                CollectionSinkFunction,
                fn_constructor_kwargs={"batch_size": 3},
                batch_size=2,
            )
        )

        client: JobHandle = ctx.execute("Demo PoC Job")

        client.wait()
