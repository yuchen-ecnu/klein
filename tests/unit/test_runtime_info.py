# SPDX-License-Identifier: Apache-2.0
from dataclasses import FrozenInstanceError

import pytest

from ray.klein.api.functions.logical_function import LogicalFunction
from ray.klein.api.runtime_info import RuntimeInfo


def test_runtime_info_is_immutable_after_validation() -> None:
    info = RuntimeInfo(batch_size=8, batch_timeout=3, batch_format="default")

    with pytest.raises(FrozenInstanceError):
        info.batch_size = 16


def test_runtime_info_requires_a_format_when_batching() -> None:
    with pytest.raises(ValueError, match="batch_format"):
        RuntimeInfo(batch_size=8, batch_timeout=3)


def test_logical_function_runtime_overrides_are_revalidated() -> None:
    logical = LogicalFunction(lambda value: value, batch_timeout=3)

    tuned = logical.with_runtime_overrides(batch_size=8)

    assert tuned.runtime_info.batch_size == 8
    assert logical.runtime_info.batch_size is None
    with pytest.raises(ValueError, match="async_buffer_size"):
        logical.with_runtime_overrides(async_buffer_size=0)
