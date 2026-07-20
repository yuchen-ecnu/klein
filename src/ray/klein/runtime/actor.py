# SPDX-License-Identifier: Apache-2.0
"""Ray actor handles and the in-process debug actor runtime."""

from __future__ import annotations

import asyncio
import copy
import inspect
import threading
from collections.abc import Coroutine
from concurrent.futures import TimeoutError as FutureTimeoutError
from contextlib import suppress
from typing import Any

from ray.util.scheduling_strategies import (
    NodeAffinitySchedulingStrategy,
    PlacementGroupSchedulingStrategy,
)

import ray
import ray.klein
from ray.klein._internal.constants import ComponentName

_DEBUG_LOOP_ATTR = "_klein_debug_loop"
_DEBUG_LOOP_THREAD_ATTR = "_klein_debug_loop_thread"
_DEBUG_LOOP_REGISTRY: dict[int, asyncio.AbstractEventLoop] = {}
_DEBUG_LOOP_THREADS: dict[int, threading.Thread] = {}


def _run_debug_loop(loop: asyncio.AbstractEventLoop) -> None:
    asyncio.set_event_loop(loop)
    try:
        loop.run_forever()
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()


def start_debug_loop_for(actor: Any) -> asyncio.AbstractEventLoop:
    """Start and attach one isolated event loop to a local debug actor."""
    existing = _loop_for_actor(actor, create=False)
    if existing is not None:
        return existing

    loop = asyncio.new_event_loop()
    thread = threading.Thread(
        target=_run_debug_loop,
        args=(loop,),
        name=f"klein-debug-loop-{id(actor)}",
        daemon=True,
    )
    thread.start()
    try:
        setattr(actor, _DEBUG_LOOP_ATTR, loop)
        setattr(actor, _DEBUG_LOOP_THREAD_ATTR, thread)
    except (AttributeError, TypeError):
        # Slot-based actor classes may forbid runtime attributes.
        _DEBUG_LOOP_REGISTRY[id(actor)] = loop
        _DEBUG_LOOP_THREADS[id(actor)] = thread
    return loop


def _loop_for_actor(
    actor: Any,
    *,
    create: bool = True,
) -> asyncio.AbstractEventLoop | None:
    loop = getattr(actor, _DEBUG_LOOP_ATTR, None) or _DEBUG_LOOP_REGISTRY.get(id(actor))
    if loop is None and create:
        return start_debug_loop_for(actor)
    return loop


def run_on_actor_loop(
    actor: Any,
    coroutine: Coroutine[Any, Any, Any],
    *,
    timeout: float | None = None,
) -> Any:
    """Run a coroutine on the local actor's dedicated event loop.

    ``timeout`` is primarily used by lifecycle code. A failed actor must not
    make best-effort teardown wait forever for a loop that can no longer make
    progress.
    """
    loop = _loop_for_actor(actor)
    if loop is None:  # pragma: no cover - ``create=True`` guarantees a loop.
        raise RuntimeError("Debug actor loop could not be created")
    try:
        future = asyncio.run_coroutine_threadsafe(coroutine, loop)
    except BaseException:
        coroutine.close()
        raise
    try:
        return future.result(timeout=timeout)
    except FutureTimeoutError as error:
        # Since Python 3.11 concurrent.futures.TimeoutError aliases the
        # built-in TimeoutError. Preserve a TimeoutError raised by the actor
        # coroutine itself; only translate an unfinished wait that expired.
        if future.done():
            raise
        future.cancel()
        raise TimeoutError("Debug actor call exceeded its timeout") from error


def stop_debug_loop_for(actor: Any, *, timeout: float = 2.0) -> None:
    """Stop and release a local actor's event-loop thread.

    Real Ray actors own their process lifetime. Debug actors are in-process, so
    Klein must explicitly release their loop to avoid leaking one daemon thread
    per actor across jobs and tests.
    """
    actor_id = id(actor)
    loop = _loop_for_actor(actor, create=False)
    thread = getattr(actor, _DEBUG_LOOP_THREAD_ATTR, None) or _DEBUG_LOOP_THREADS.pop(actor_id, None)
    _DEBUG_LOOP_REGISTRY.pop(actor_id, None)
    for attribute in (_DEBUG_LOOP_ATTR, _DEBUG_LOOP_THREAD_ATTR):
        with suppress(AttributeError, TypeError):
            delattr(actor, attribute)

    if loop is None or not loop.is_running():
        return
    with suppress(RuntimeError):
        loop.call_soon_threadsafe(loop.stop)
    if thread is not None and thread is not threading.current_thread():
        thread.join(timeout)


def _run_unbound_coroutine(coroutine: Coroutine[Any, Any, Any]) -> Any:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coroutine)

    result: list[Any] = []
    error: list[BaseException] = []

    def run() -> None:
        try:
            result.append(asyncio.run(coroutine))
        except BaseException as exc:
            error.append(exc)

    thread = threading.Thread(target=run, name="klein-debug-coroutine", daemon=True)
    thread.start()
    thread.join()
    if error:
        raise error[0]
    return result[0]


class _LocalFunctionProxy:
    """Deferred local call with the same shape as a Ray ``ObjectRef``."""

    def __init__(self, function: Any, *args: Any, **kwargs: Any) -> None:
        self._function = function
        self._args = args
        self._kwargs = kwargs

    def __call__(self, *, timeout: float | None = None) -> Any:
        result = self._function(*self._args, **self._kwargs)
        if not inspect.iscoroutine(result):
            return result
        actor = getattr(self._function, "__self__", None)
        if actor is not None:
            return run_on_actor_loop(actor, result, timeout=timeout)
        return _run_unbound_coroutine(result)

    async def resolve_async(self) -> Any:
        """Resolve without blocking the caller's event loop."""

        result = self._function(*self._args, **self._kwargs)
        if not inspect.iscoroutine(result):
            return result
        actor = getattr(self._function, "__self__", None)
        if actor is None:
            return await result
        loop = _loop_for_actor(actor)
        if loop is asyncio.get_running_loop():
            return await result
        try:
            future = asyncio.run_coroutine_threadsafe(result, loop)
        except BaseException:
            result.close()
            raise
        return await asyncio.wrap_future(future)

    def __deepcopy__(self, _memo: dict[int, Any]) -> _LocalFunctionProxy:
        return self


class _KleinActorMethod:
    """Callable method facade shared by Ray and debug actor handles."""

    def __init__(self, inner_actor: Any, method_name: str, debug_mode: bool) -> None:
        self._inner_actor = inner_actor
        self._method_name = method_name
        self._debug_mode = debug_mode

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        method = getattr(self._inner_actor, self._method_name)
        if self._debug_mode:
            # Local actors share the caller's process and need an explicit copy to
            # emulate Ray's process isolation. A real Ray call already serializes
            # its arguments; copying here only duplicates payload memory and can
            # block the actor loop before the RPC is submitted.
            args = copy.deepcopy(args)
            kwargs = copy.deepcopy(kwargs)
            return _LocalFunctionProxy(method, *args, **kwargs)
        return method.remote(*args, **kwargs)

    def __reduce__(self) -> tuple[Any, tuple[Any, str, bool]]:
        return _KleinActorMethod, (self._inner_actor, self._method_name, self._debug_mode)

    def __deepcopy__(self, _memo: dict[int, Any]) -> _KleinActorMethod:
        return self


class KleinActorHandle:
    """Uniform handle for a Ray actor or an in-process debug actor."""

    def __init__(self, inner_actor: Any, debug_mode: bool = False) -> None:
        self.inner_actor = inner_actor
        self.debug_mode = debug_mode

    def __getattr__(self, item: str) -> _KleinActorMethod:
        return _KleinActorMethod(self.inner_actor, item, self.debug_mode)

    @property
    def actor_id(self) -> str | None:
        """Ray Actor ID used by the native Dashboard actor detail route."""

        if self.debug_mode:
            return None
        actor_id = getattr(self.inner_actor, "_actor_id", None)
        if actor_id is None:
            actor_id = getattr(self.inner_actor, "_ray_actor_id", None)
        to_hex = getattr(actor_id, "hex", None)
        return to_hex() if callable(to_hex) else None

    def __reduce__(self) -> tuple[Any, tuple[Any, bool]]:
        return KleinActorHandle, (self.inner_actor, self.debug_mode)

    def __deepcopy__(self, _memo: dict[int, Any]) -> KleinActorHandle:
        return self


def is_local_function_proxy(value: Any) -> bool:
    """Return whether ``value`` is a deferred debug-actor call."""
    return isinstance(value, _LocalFunctionProxy)


def resolve_local_function_proxy(value: Any, *, timeout: float | None = None) -> Any:
    """Resolve a deferred debug-actor call."""
    if not isinstance(value, _LocalFunctionProxy):
        raise TypeError(f"Expected a local function proxy, got {type(value).__name__}")
    return value(timeout=timeout)


async def resolve_local_function_proxy_async(value: Any) -> Any:
    """Resolve a deferred debug-actor call without blocking the caller loop."""

    if not isinstance(value, _LocalFunctionProxy):
        raise TypeError(f"Expected a local function proxy, got {type(value).__name__}")
    return await value.resolve_async()


def _create_remote_actor(actor_class: type[Any], **ray_remote_args: Any) -> Any:
    return ray.remote(**ray_remote_args)(actor_class)


def _runtime_options(ray_remote_args: dict[str, Any] | None) -> dict[str, Any]:
    options = dict(ray_remote_args) if ray_remote_args is not None else {"num_cpus": 1}
    runtime_env = dict(options.get("runtime_env") or {})
    env_vars = dict(runtime_env.get("env_vars") or {})
    env_vars.setdefault("RAY_WARN_BLOCKING_GET_INSIDE_ASYNC", "0")
    runtime_env["env_vars"] = env_vars
    options["runtime_env"] = runtime_env
    return options


def _apply_scheduling_strategy(
    actor_class: type[Any],
    options: dict[str, Any],
    *,
    schedule_node_id: str | None,
    placement_group: Any,
    placement_group_bundle_index: int,
) -> None:
    if actor_class.__name__ in {
        ComponentName.KLEIN_JOB_MANAGER,
        ComponentName.KLEIN_CHECKPOINT_COORDINATOR,
    }:
        options["scheduling_strategy"] = NodeAffinitySchedulingStrategy(
            node_id=ray.get_runtime_context().get_node_id(),
            soft=False,
        )
    elif placement_group is not None:
        options["scheduling_strategy"] = PlacementGroupSchedulingStrategy(
            placement_group=placement_group,
            placement_group_bundle_index=placement_group_bundle_index,
        )
    elif schedule_node_id is not None:
        options["scheduling_strategy"] = NodeAffinitySchedulingStrategy(
            node_id=schedule_node_id,
            soft=False,
        )


def create_remote_actor(
    actor_class: type[Any],
    construct_args: dict[str, Any] | None = None,
    ray_remote_args: dict[str, Any] | None = None,
    local_mode: bool = False,
    schedule_node_id: str | None = None,
    placement_group: Any = None,
    placement_group_bundle_index: int = -1,
) -> KleinActorHandle:
    """Create a Ray actor, or an isolated local actor in debug mode."""
    debug_mode = local_mode or ray.klein.is_debug_mode()
    constructor = dict(construct_args or {})
    if debug_mode:
        actor = actor_class(**copy.deepcopy(constructor))
    else:
        options = _runtime_options(ray_remote_args)
        _apply_scheduling_strategy(
            actor_class,
            options,
            schedule_node_id=schedule_node_id,
            placement_group=placement_group,
            placement_group_bundle_index=placement_group_bundle_index,
        )
        actor = _create_remote_actor(actor_class, **options).remote(**constructor)

    handle = KleinActorHandle(actor, debug_mode)
    actor_name = ray_remote_args.get("name") if debug_mode and ray_remote_args else None
    if actor_name is not None:
        ray.klein.register_debug_actor(actor_name, handle)
    return handle
