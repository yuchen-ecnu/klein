# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from ray.data import Dataset

from ray.klein.api.ray_data.call import RayDataCall
from ray.klein.api.ray_data.discovery import public_dataset_factories, public_module_function
from ray.klein.api.ray_data.metadata import copy_callable_metadata

if TYPE_CHECKING:
    from ray.klein.api.data_stream import DataStream
    from ray.klein.api.klein_context import KleinContext


class RayDataContextAdapter:
    """Dynamic namespace for public ``ray.data`` Dataset factories."""

    def __init__(self, owner: KleinContext) -> None:
        self._owner = owner

    def source(
        self,
        operation: str | Callable[..., Dataset],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> DataStream:
        if isinstance(operation, str):
            public_module_function(operation)
            call = RayDataCall.module_function(operation, args, kwargs)
        elif callable(operation):
            call = RayDataCall.source_callable(operation, args, kwargs)
        else:
            raise TypeError("Ray Data source must be a public function name or callable")
        return self._owner._from_ray_data(call)

    def from_dataset(self, dataset: Dataset) -> DataStream:
        if not isinstance(dataset, Dataset):
            raise TypeError(f"dataset must be ray.data.Dataset, got {type(dataset).__name__}")
        return self._owner._from_ray_data(RayDataCall.dataset_value(dataset))

    @property
    def available(self) -> tuple[str, ...]:
        return public_dataset_factories()

    def __getattr__(self, name: str) -> Callable[..., DataStream]:
        target = public_module_function(name)

        def invoke(*args: Any, **kwargs: Any) -> DataStream:
            return self.source(name, *args, **kwargs)

        return copy_callable_metadata(invoke, target, drop_first=False)

    def __dir__(self) -> list[str]:
        return sorted(set(super().__dir__()) | set(public_dataset_factories()))
