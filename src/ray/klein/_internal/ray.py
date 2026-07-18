# SPDX-License-Identifier: Apache-2.0
import asyncio
import inspect
import os
from typing import Any

import ray
from ray import ObjectRef
from ray.klein._internal.logging import get_logger
from ray.klein.api.stream_task_status import StreamTaskStatus
from ray.klein.config.environment_variables import EnvironmentVariables
from ray.klein.runtime.actor import (
    KleinActorHandle,
    is_local_function_proxy,
    resolve_local_function_proxy,
    run_on_actor_loop,
    stop_debug_loop_for,
)

KLEIN_DEBUG_OBJECT_STORE: dict[str, KleinActorHandle] = {}

# Short bound for the liveness ping in get_actor_status: long enough to ride out
# a momentary RPC hiccup, short enough that a being-rebuilt actor is classified
# DEAD (recoverable) promptly rather than stalling the health loop.
PING_TIMEOUT_SECONDS = 2.0
_DEBUG_ACTOR_STOP_TIMEOUT_SECONDS = 2.0


logger = get_logger(__name__)


def _get_list(obj_list: list[Any], timeout: float | None = None) -> list[Any]:
    if not obj_list:
        return []
    if all(isinstance(obj, ObjectRef) for obj in obj_list):
        return ray.get(obj_list, timeout=timeout)
    return [
        ray.get(obj, timeout=timeout)
        if isinstance(obj, ObjectRef)
        else resolve_local_function_proxy(obj)
        if is_local_function_proxy(obj)
        else obj
        for obj in obj_list
    ]


def get(obj, timeout: float | None = None) -> Any:
    if isinstance(obj, list):
        return _get_list(obj, timeout)
    return _get_list([obj], timeout)[0]


async def _aget_one(obj: Any) -> Any:
    """Await one object ref or resolve a debug-mode local call without
    blocking the event loop.

    Inside an async Ray actor, ``ray.get`` is forbidden because it blocks the
    actor's event loop. Use this to ``await`` the result instead. ObjectRefs are
    wrapped as asyncio futures (the portable form across Python 3.9-3.11). In
    debug mode the proxy is just called directly; if it returns a coroutine we
    await it.
    """
    if isinstance(obj, ObjectRef):
        return await asyncio.wrap_future(obj.future())
    if is_local_function_proxy(obj):
        result = resolve_local_function_proxy(obj)
        if inspect.isawaitable(result):
            return await result
        return result
    return obj


async def aget(obj, timeout: float | None = None, return_exceptions: bool = False) -> Any:
    """Async counterpart of :func:`get` for use inside async actor methods.

    Accepts a single ref/proxy or a list. With a timeout, raises
    asyncio.TimeoutError on expiry (mirroring ray.get's GetTimeoutError intent).

    ``return_exceptions`` (list form only) mirrors ``asyncio.gather``: instead of
    the whole call failing when one ref errors (e.g. an actor Ray is mid-restart),
    each slot independently resolves to its result or the raised exception. The
    caller filters those out. Without it, a single dead actor fails the entire
    batch — which is rarely what a best-effort reader wants.
    """
    if isinstance(obj, list):
        if not obj:
            return []
        coro = asyncio.gather(*[_aget_one(item) for item in obj], return_exceptions=return_exceptions)
    else:
        coro = _aget_one(obj)
    if timeout is not None:
        return await asyncio.wait_for(coro, timeout=timeout)
    return await coro


def kill(actor_handle: KleinActorHandle | None) -> None:
    if actor_handle is None:
        return
    try:
        if actor_handle.debug_mode:
            if hasattr(actor_handle.inner_actor, "stop"):
                result = actor_handle.inner_actor.stop()
                if inspect.iscoroutine(result):
                    run_on_actor_loop(
                        actor_handle.inner_actor,
                        result,
                        timeout=_DEBUG_ACTOR_STOP_TIMEOUT_SECONDS,
                    )
        else:
            ray.kill(actor_handle.inner_actor)
    except Exception as error:
        logger.debug("Failed to kill actor: %s", error)
    finally:
        if actor_handle.debug_mode:
            stop_debug_loop_for(actor_handle.inner_actor)
            for name, handle in list(KLEIN_DEBUG_OBJECT_STORE.items()):
                if handle.inner_actor is actor_handle.inner_actor:
                    KLEIN_DEBUG_OBJECT_STORE.pop(name, None)


def kill_actor_by_name(name: str, namespace: str | None = None) -> None:
    """Kill a named actor.

    ``namespace`` scopes the lookup to a specific Ray namespace so Klein's
    per-job namespace isolation can target the right actor when multiple
    Klein jobs run in the same cluster. ``None`` uses Ray's current namespace;
    debug mode has no Ray actor namespaces.
    """
    if is_debug_mode():
        kill(KLEIN_DEBUG_OBJECT_STORE.pop(name, None))
        return
    try:
        ray.kill(ray.get_actor(name, namespace=namespace))
    except Exception as error:
        logger.debug("Failed to kill actor %s by name: %s", name, error)


def get_actor_by_name(name: str, namespace: str | None = None) -> KleinActorHandle | None:
    """Look up a named actor handle.

    Args:
        name: The Ray named-actor name.
        namespace: Optional Ray namespace to scope the lookup. Klein uses a
            per-job namespace (``klein-{job_name}-{uuid8}``) to isolate
            JobManager / CheckpointCoordinator / StreamTask actors so multiple
            Klein jobs can coexist in one cluster. ``None`` uses Ray's current
            namespace.

    Returns:
        A :class:`KleinActorHandle` wrapping the live actor, or ``None`` if
        the name (in the given namespace) doesn't resolve.
    """
    if is_debug_mode():
        # Debug mode is in-process: namespaces are a Ray concept, so we just
        # look up by raw name in the local registry. The caller's intent
        # ("find the actor with this name") is preserved as long as the
        # registered name itself is already unique-per-job (which it is —
        # JobManager / CheckpointCoordinator are singletons within a single
        # JobClient instance, and StreamTask names embed the job_name).
        return KLEIN_DEBUG_OBJECT_STORE.get(name)
    try:
        actor = ray.get_actor(name, namespace=namespace)
        return KleinActorHandle(actor)
    except Exception as error:
        logger.debug(
            "Failed to look up actor by name %s in namespace %s: %s",
            name,
            namespace,
            error,
        )
    return None


def register_debug_actor(name: str, actor: KleinActorHandle) -> None:
    previous = KLEIN_DEBUG_OBJECT_STORE.get(name)
    if previous is not None and previous.inner_actor is not actor.inner_actor:
        kill(previous)
    KLEIN_DEBUG_OBJECT_STORE[name] = actor


def get_actor_status(task_name: str, namespace: str | None = None) -> StreamTaskStatus:
    """Classify an actor as ALIVE / DEAD / NOT_EXIST for the health loop.

    Uses only the cheap control-plane calls instead of the rate-limited
    ``ray.util.state`` observability API (which is meant for the dashboard, not
    a hot health-poll path):
      * ``ray.get_actor`` raises -> the named actor doesn't exist -> NOT_EXIST.
      * a short ``ping()`` returns -> the actor is up and serving -> ALIVE.
      * ping times out / errors -> the actor exists by name but isn't serving
        (Ray is rebuilding it) -> DEAD, which the recovery loop treats as
        "still coming back, wait for a later tick".

    ``namespace`` scopes the lookup to the per-job Ray namespace used by
    Klein's namespace isolation; ``None`` uses Ray's current namespace.
    """
    if is_debug_mode():
        return StreamTaskStatus.ALIVE
    try:
        actor = ray.get_actor(task_name, namespace=namespace)
    except Exception:
        return StreamTaskStatus.NOT_EXIST
    if actor is None:
        return StreamTaskStatus.NOT_EXIST
    try:
        ray.get(actor.ping.remote(), timeout=PING_TIMEOUT_SECONDS)
        return StreamTaskStatus.ALIVE
    except Exception:
        # Exists by name but not answering -> being rebuilt; recoverable later.
        return StreamTaskStatus.DEAD


def exit_actor() -> None:
    try:
        if not is_debug_mode():
            ray.actor.exit_actor()
    except Exception as error:
        logger.debug("Failed to exit actor: %s", error)


def is_debug_mode() -> bool:
    return str(os.getenv(EnvironmentVariables.DEBUG, "0")).lower() in {
        "true",
        "1",
        "yes",
    }
