# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import Enum, auto
from functools import singledispatch
from typing import Any

from ray.data import Dataset

from ray.klein._internal.frozen_mapping import FrozenMapping
from ray.klein.api.functions.lowering_context import LoweringContext
from ray.klein.api.ray_data.discovery import public_module_function
from ray.klein.api.ray_data.error import RayDataAPIError


class _TargetKind(Enum):
    MODULE_FUNCTION = auto()
    DATASET_METHOD = auto()
    SOURCE_CALLABLE = auto()
    DATASET_CALLABLE = auto()
    DATASET_VALUE = auto()


@dataclass(frozen=True, slots=True)
class _DatasetInput:
    index: int


@singledispatch
def resolve_inputs(value: Any, datasets: tuple[Dataset, ...]) -> Any:
    return value


@resolve_inputs.register
def _(value: _DatasetInput, datasets: tuple[Dataset, ...]) -> Dataset:
    try:
        return datasets[value.index]
    except IndexError as exc:
        raise RayDataAPIError(
            f"Ray Data call references input {value.index}, but only {len(datasets)} inputs were compiled"
        ) from exc


@resolve_inputs.register
def _(value: list, datasets: tuple[Dataset, ...]) -> list:
    return [resolve_inputs(item, datasets) for item in value]


@resolve_inputs.register
def _(value: tuple, datasets: tuple[Dataset, ...]) -> tuple:
    return tuple(resolve_inputs(item, datasets) for item in value)


@resolve_inputs.register
def _(value: dict, datasets: tuple[Dataset, ...]) -> dict:
    return {key: resolve_inputs(item, datasets) for key, item in value.items()}


@resolve_inputs.register
def _(value: set, datasets: tuple[Dataset, ...]) -> set:
    return {resolve_inputs(item, datasets) for item in value}


@resolve_inputs.register
def _(value: frozenset, datasets: tuple[Dataset, ...]) -> frozenset:
    return frozenset(resolve_inputs(item, datasets) for item in value)


@dataclass(frozen=True, slots=True)
class RayDataCall:
    """A lazy call resolved against the Ray version installed at execution."""

    target_kind: _TargetKind
    target: str | Callable[..., Any] | Dataset
    args: tuple[Any, ...] = ()
    kwargs: Mapping[str, Any] | None = None
    expects_dataset: bool = True

    @classmethod
    def module_function(cls, target: str, args: tuple[Any, ...], kwargs: Mapping[str, Any]) -> RayDataCall:
        return cls(_TargetKind.MODULE_FUNCTION, target, args, kwargs)

    @classmethod
    def source_callable(
        cls, target: Callable[..., Any], args: tuple[Any, ...], kwargs: Mapping[str, Any]
    ) -> RayDataCall:
        return cls(_TargetKind.SOURCE_CALLABLE, target, args, kwargs)

    @classmethod
    def dataset_value(cls, dataset: Dataset) -> RayDataCall:
        return cls(_TargetKind.DATASET_VALUE, dataset)

    @classmethod
    def dataset_method(
        cls,
        target: str,
        args: tuple[Any, ...],
        kwargs: Mapping[str, Any],
        *,
        expects_dataset: bool,
    ) -> RayDataCall:
        return cls(_TargetKind.DATASET_METHOD, target, args, kwargs, expects_dataset)

    @classmethod
    def dataset_callable(
        cls,
        target: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: Mapping[str, Any],
        *,
        expects_dataset: bool,
    ) -> RayDataCall:
        return cls(_TargetKind.DATASET_CALLABLE, target, args, kwargs, expects_dataset)

    def __post_init__(self) -> None:
        object.__setattr__(self, "args", tuple(self.args))
        object.__setattr__(self, "kwargs", FrozenMapping(self.kwargs or {}))

    @property
    def display_name(self) -> str:
        if isinstance(self.target, str):
            return self.target
        return getattr(self.target, "__name__", self.target.__class__.__name__)

    def _callable_target(self) -> Callable[..., Any]:
        if not callable(self.target):
            raise RayDataAPIError(f"Ray Data call target {self.target!r} is not callable")
        return self.target

    def __call__(self, ctx: LoweringContext) -> Any:
        datasets = tuple(ctx.upstream_ds)
        args = resolve_inputs(self.args, datasets)
        kwargs = resolve_inputs(dict(self.kwargs), datasets)

        if self.target_kind == _TargetKind.MODULE_FUNCTION:
            result = public_module_function(str(self.target))(*args, **kwargs)
        elif self.target_kind == _TargetKind.DATASET_METHOD:
            if not datasets:
                raise RayDataAPIError(f"Dataset.{self.target} requires an upstream Dataset")
            result = getattr(datasets[0], str(self.target))(*args, **kwargs)
        elif self.target_kind == _TargetKind.SOURCE_CALLABLE:
            result = self._callable_target()(*args, **kwargs)
        elif self.target_kind == _TargetKind.DATASET_CALLABLE:
            if not datasets:
                raise RayDataAPIError("A Ray Data transform callable requires an upstream Dataset")
            result = self._callable_target()(datasets[0], *args, **kwargs)
        else:
            result = self.target

        if self.expects_dataset and not isinstance(result, Dataset):
            raise RayDataAPIError(
                f"Ray Data operation {self.display_name!r} returned {type(result).__name__}, "
                "but a Dataset-producing operation was required. Use stream.data.consume(...) "
                "for a terminal result, or complete intermediate objects such as GroupedData "
                "inside stream.data.transform(lambda ds: ...)."
            )
        return result
