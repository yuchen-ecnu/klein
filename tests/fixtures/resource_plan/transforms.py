# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import Any

import numpy as np

from ray.klein.api.data_stream import DataStream


class BatchIdentity:
    def __call__(self, batch: Any) -> Any:
        return batch


def identity_batch(batch: dict[str, np.ndarray]) -> dict[str, Any]:
    return batch


def transform_stream(stream: DataStream) -> DataStream:
    return stream.map(lambda row: row, num_cpus=2.0, concurrency=6)
