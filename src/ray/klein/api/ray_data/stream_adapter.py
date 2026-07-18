# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from ray.data import Dataset

from ray.klein.api.ray_data.binding import bind_stream_arguments
from ray.klein.api.ray_data.call import RayDataCall
from ray.klein.api.ray_data.discovery import (
    classify_dataset_method,
    dataset_method_binds_instance,
    public_dataset_method,
    public_dataset_methods,
)
from ray.klein.api.ray_data.metadata import copy_callable_metadata
from ray.klein.api.ray_data.method_kind import RayDataMethodKind

if TYPE_CHECKING:
    from ray.klein.api.data_stream import DataStream
    from ray.klein.api.stream_sink import StreamSink


class RayDataStreamAdapter:
    """Dynamic namespace for public ``ray.data.Dataset`` operations."""

    def __init__(self, owner: DataStream) -> None:
        self._owner = owner

    def transform(
        self,
        operation: str | Callable[..., Dataset],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> DataStream:
        if isinstance(operation, str):
            public_dataset_method(operation)
            factory = RayDataCall.dataset_method
        elif callable(operation):
            factory = RayDataCall.dataset_callable
        else:
            raise TypeError("Ray Data transform must be a public method name or callable")
        dependencies, bound_args, bound_kwargs = bind_stream_arguments(self._owner, args, kwargs)
        call = factory(operation, bound_args, bound_kwargs, expects_dataset=True)
        return self._owner._apply_ray_data(call, dependencies)

    def consume(
        self,
        operation: str | Callable[..., Any],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> StreamSink | Any:
        if isinstance(operation, str):
            public_dataset_method(operation)
            factory = RayDataCall.dataset_method
        elif callable(operation):
            factory = RayDataCall.dataset_callable
        else:
            raise TypeError("Ray Data consumer must be a public method name or callable")
        dependencies, bound_args, bound_kwargs = bind_stream_arguments(self._owner, args, kwargs)
        call = factory(operation, bound_args, bound_kwargs, expects_dataset=False)
        return self._owner._consume_ray_data(call, dependencies)

    @property
    def available(self) -> tuple[str, ...]:
        return public_dataset_methods()

    def kind(self, name: str) -> RayDataMethodKind:
        return classify_dataset_method(name)

    def __getattr__(self, name: str) -> Callable[..., Any]:
        target = public_dataset_method(name)
        kind = classify_dataset_method(name)

        def invoke(*args: Any, **kwargs: Any) -> Any:
            if kind == RayDataMethodKind.TRANSFORM:
                return self.transform(name, *args, **kwargs)
            return self.consume(name, *args, **kwargs)

        return copy_callable_metadata(
            invoke,
            target,
            drop_first=dataset_method_binds_instance(name),
        )

    def __dir__(self) -> list[str]:
        return sorted(set(super().__dir__()) | set(public_dataset_methods()))
