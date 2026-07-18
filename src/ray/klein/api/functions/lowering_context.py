# SPDX-License-Identifier: Apache-2.0
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from ray.data import Dataset

from ray.klein._internal.frozen_mapping import FrozenMapping
from ray.klein.api.runtime_context import RuntimeContext
from ray.klein.api.runtime_info import RuntimeInfo
from ray.klein.runtime.resources import Resources


@dataclass(frozen=True, slots=True)
class LoweringContext:
    """Values assembled when a logical function is lowered to Ray Data."""

    upstream_ds: tuple[Dataset, ...]
    resources: Resources
    runtime_info: RuntimeInfo
    user_fn: Any = None
    fn_constructor_args: tuple[Any, ...] = ()
    fn_constructor_kwargs: Mapping[str, Any] = field(default_factory=FrozenMapping)
    runtime_context: RuntimeContext | None = None
    needs_runtime_context: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "upstream_ds", tuple(self.upstream_ds))
        object.__setattr__(self, "fn_constructor_args", tuple(self.fn_constructor_args))
        object.__setattr__(self, "fn_constructor_kwargs", FrozenMapping(self.fn_constructor_kwargs))

    @property
    def user_fn_ctor_kwargs(self) -> dict[str, Any]:
        kwargs = dict(self.fn_constructor_kwargs)
        if self.needs_runtime_context:
            kwargs["runtime_context"] = self.runtime_context
        return kwargs

    @property
    def user_fn_ctor_kwargs_for_ray_data(self) -> dict[str, Any]:
        """Return constructor kwargs only when the user function is a class."""

        if not isinstance(self.user_fn, type):
            return {}
        return {
            "fn_constructor_args": self.fn_constructor_args,
            "fn_constructor_kwargs": self.user_fn_ctor_kwargs,
        }
