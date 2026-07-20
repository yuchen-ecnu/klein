# SPDX-License-Identifier: Apache-2.0
import inspect
from collections.abc import Callable, Iterable
from copy import copy
from dataclasses import replace
from typing import Any

from ray.data import Dataset
from ray.data.block import UserDefinedFunction
from ray.util.queue import Queue

from ray.klein._internal.frozen_mapping import FrozenMapping
from ray.klein.api.collect_function import CollectFunction
from ray.klein.api.functions.function_kind import FunctionKind
from ray.klein.api.functions.lowering_context import LoweringContext
from ray.klein.api.runtime_context import RuntimeContext
from ray.klein.api.runtime_info import RuntimeInfo
from ray.klein.api.sink_function import SinkFunction
from ray.klein.api.source_function import SourceFunction
from ray.klein.runtime.resources import Resources


class LogicalFunction:
    """Wraps a user fn and knows how to materialize it for stream or batch.

    Note: users never construct this directly — the DataStream API builds it
    when you call ``.map(fn)`` / ``.sink(fn)`` etc. Stream and batch are two
    independent execution backends; only one is used per job mode.
    """

    def __init__(
        self,
        fn: UserDefinedFunction | type[SinkFunction] | type[SourceFunction],
        *,
        fn_constructor_args: Iterable[Any] | None = None,
        fn_constructor_kwargs: dict[str, Any] | None = None,
        lowering: Callable[[LoweringContext], Any] | None = None,
        resources: Resources | None = None,
        batch_size: int | None = None,
        batch_timeout: int | None = None,
        batch_format: str = "default",
        async_buffer_size: int | None = None,
    ) -> None:
        if isinstance(fn, (CollectFunction, SourceFunction, SinkFunction)):
            raise TypeError("Lifecycle functions must be passed as classes so each subtask owns its instance")
        self._function = fn
        self._constructor_args = tuple(fn_constructor_args or ())
        self._constructor_kwargs = FrozenMapping(fn_constructor_kwargs or {})
        # The ray.data backend is described by a single lowering callable: either
        # a named transform fn (lower_map_batches…) or a RayDataCall.
        # ``to_batch`` builds a LoweringContext and hands it over; the resources
        # are resolved lazily there so a ResourcePlan override is reflected.
        self._lowering = lowering
        self._resources = resources if resources is not None else Resources()
        self._runtime_info = RuntimeInfo(
            batch_size=batch_size,
            batch_timeout=batch_timeout,
            batch_format=batch_format,
            async_buffer_size=async_buffer_size,
        )
        # Classify the fn once, here, instead of via runtime inspect in to_stream.
        self._function_kind = self._classify(fn)
        self._needs_runtime_context = self._compute_needs_runtime_context(fn)

    @property
    def function(self) -> UserDefinedFunction | type[SinkFunction] | type[SourceFunction]:
        """The user callable or lifecycle class described by this wrapper."""
        return self._function

    @property
    def constructor_args(self) -> tuple[Any, ...]:
        return tuple(self._constructor_args)

    @property
    def constructor_kwargs(self) -> dict[str, Any]:
        return dict(self._constructor_kwargs)

    @property
    def runtime_info(self) -> RuntimeInfo:
        return self._runtime_info

    @property
    def supports_concurrent_rescale(self) -> bool:
        """Whether a pending runtime may be constructed beside the active one.

        Plain functions have no open/close lifecycle and remain dormant until
        the actor-local commit. Callable/lifecycle classes may acquire external
        resources in construction or ``open`` and must opt in with a class
        attribute of the same name.
        """

        if self._function_kind == FunctionKind.STATELESS:
            return True
        return bool(getattr(self._function, "supports_concurrent_rescale", False))

    def with_runtime_overrides(
        self,
        *,
        batch_size: int | None = None,
        async_buffer_size: int | None = None,
    ) -> "LogicalFunction":
        """Return an independent function recipe with validated runtime tuning."""

        changes: dict[str, int] = {}
        if batch_size is not None:
            changes["batch_size"] = batch_size
        if async_buffer_size is not None:
            changes["async_buffer_size"] = async_buffer_size
        cloned = copy(self)
        if changes:
            cloned._runtime_info = replace(self._runtime_info, **changes)
        return cloned

    def with_resources(self, resources: Resources) -> "LogicalFunction":
        """Return an independent function recipe using ``resources`` for batch lowering."""

        if not isinstance(resources, Resources):
            raise TypeError("resources must be a Resources instance")
        cloned = copy(self)
        cloned._resources = resources
        return cloned

    @staticmethod
    def _classify(fn) -> FunctionKind:
        if inspect.isfunction(fn):
            return FunctionKind.STATELESS
        if isinstance(fn, type) and issubclass(fn, CollectFunction):
            return FunctionKind.COLLECT
        if isinstance(fn, type) and issubclass(fn, (SourceFunction, SinkFunction)):
            return FunctionKind.LIFECYCLE
        return FunctionKind.CALLABLE_CLASS

    @staticmethod
    def _compute_needs_runtime_context(fn) -> bool:
        # Only meaningful for callable classes; computed once instead of per call.
        if inspect.isfunction(fn):
            return False
        try:
            return "runtime_context" in inspect.signature(fn.__init__).parameters
        except (TypeError, ValueError):
            return False

    def to_stream(self, runtime_context: RuntimeContext, output_queue: Queue | None = None) -> Any:
        if self._function_kind == FunctionKind.STATELESS:
            return self._function

        if self._function_kind in {FunctionKind.COLLECT, FunctionKind.LIFECYCLE}:
            kwargs = dict(self._constructor_kwargs)
            if self._function_kind == FunctionKind.COLLECT:
                kwargs["output_queue"] = output_queue
            function = self._function(*self._constructor_args, **kwargs)
            function.open(runtime_context)
            return function

        # CALLABLE_CLASS
        kwargs = dict(self._constructor_kwargs)
        if self._needs_runtime_context:
            kwargs["runtime_context"] = runtime_context
        return self._function(*self._constructor_args, **kwargs)

    def to_batch(
        self,
        upstream_ds: list[Dataset],
        *,
        runtime_context: RuntimeContext | None = None,
    ) -> Dataset:
        if self._lowering is None:
            raise ValueError("Batch function is not defined.")

        ctx = LoweringContext(
            upstream_ds=upstream_ds,
            resources=self._resources,
            runtime_info=self._runtime_info,
            user_fn=self._function,
            fn_constructor_args=self._constructor_args,
            fn_constructor_kwargs=self._constructor_kwargs,
            runtime_context=runtime_context,
            needs_runtime_context=self._needs_runtime_context,
        )
        return self._lowering(ctx)

    @property
    def batch_supported(self) -> bool:
        """Whether batch lowering preserves this function's runtime semantics.

        ``async_buffer_size`` describes Klein's ordered native-streaming
        execution window.  Ray Data lowerings do not receive that option and
        invoke ordinary ``map`` callables synchronously, so the presence of a
        lowering alone is not sufficient for AUTO mode to choose batch.
        """

        return self._lowering is not None and not self._runtime_info.async_enabled

    @property
    def batch_lowering(self) -> Callable[[LoweringContext], Any] | None:
        """Return the immutable description of this function's Ray Data lowering."""

        return self._lowering
