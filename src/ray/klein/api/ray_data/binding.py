# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any

from ray.klein.api.ray_data.call import _DatasetInput

if TYPE_CHECKING:
    from ray.klein.api.data_stream import DataStream
    from ray.klein.api.stream import Stream


def bind_stream_arguments(
    primary: DataStream,
    args: Iterable[Any],
    kwargs: Mapping[str, Any],
) -> tuple[tuple[Stream, ...], tuple[Any, ...], dict[str, Any]]:
    """Replace nested Klein streams with stable upstream Dataset references."""

    from ray.klein.api.stream import Stream

    dependencies: list[Stream] = [primary]
    indices = {id(primary): 0}

    def bind(value: Any) -> Any:
        if isinstance(value, Stream):
            identity = id(value)
            if identity not in indices:
                indices[identity] = len(dependencies)
                dependencies.append(value)
            return _DatasetInput(indices[identity])
        if isinstance(value, list):
            return [bind(item) for item in value]
        if isinstance(value, tuple):
            return tuple(bind(item) for item in value)
        if isinstance(value, dict):
            return {key: bind(item) for key, item in value.items()}
        if isinstance(value, set):
            return {bind(item) for item in value}
        if isinstance(value, frozenset):
            return frozenset(bind(item) for item in value)
        return value

    bound_args = tuple(bind(item) for item in args)
    bound_kwargs = {key: bind(value) for key, value in kwargs.items()}
    return tuple(dependencies), bound_args, bound_kwargs
