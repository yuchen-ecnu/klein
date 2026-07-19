# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Callable
from typing import Any

from ray.data import Dataset

from ray.klein.api.data_stream import DataStream
from ray.klein.api.klein_context import KleinContext
from ray.klein.api.ray_data.discovery import public_module_function
from ray.klein.api.ray_data.metadata import copy_callable_metadata


def dataset_factory(name: str) -> Callable[..., Any]:
    """Bind an installed ``ray.data`` source function to the current context."""

    target = public_module_function(name)

    def invoke(*args: Any, **kwargs: Any) -> DataStream:
        return KleinContext.current().data.source(name, *args, **kwargs)

    return copy_callable_metadata(invoke, target, drop_first=False)


def _context_method(name: str) -> Callable[..., Any]:
    target = getattr(KleinContext, name)

    def invoke(*args: Any, **kwargs: Any) -> DataStream:
        return getattr(KleinContext.current(), name)(*args, **kwargs)

    return copy_callable_metadata(invoke, target, drop_first=True)


from_items = _context_method("from_items")
from_values = _context_method("from_values")
read_kafka = _context_method("read_kafka")
read_canal = _context_method("read_canal")
read_rocketmq = _context_method("read_rocketmq")


def source(operation: str | Callable[..., Any], /, *args: Any, **kwargs: Any) -> DataStream:
    return KleinContext.current().data.source(operation, *args, **kwargs)


def from_ray_dataset(dataset: Dataset) -> DataStream:
    return KleinContext.current().data.from_dataset(dataset)
