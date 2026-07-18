# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the in-process actor lifecycle."""

import asyncio
import time
from types import SimpleNamespace

from ray.klein._internal import ray as klein_ray
from ray.klein._internal.constants import ComponentName
from ray.klein.runtime.actor import create_remote_actor


class _AsyncActor:
    async def ping(self) -> str:
        return "pong"


class _StuckActor:
    async def ping(self) -> str:
        return "pong"

    async def stop(self) -> None:
        await asyncio.Event().wait()


def test_debug_actor_loop_is_released_on_kill() -> None:
    handle = create_remote_actor(_AsyncActor, local_mode=True)

    assert klein_ray.get(handle.ping()) == "pong"
    loop = handle.inner_actor._klein_debug_loop
    thread = handle.inner_actor._klein_debug_loop_thread
    assert loop.is_running()
    assert thread.is_alive()

    klein_ray.kill(handle)

    assert not thread.is_alive()
    assert not hasattr(handle.inner_actor, "_klein_debug_loop")


def test_named_debug_actor_is_removed_and_stopped(monkeypatch) -> None:
    monkeypatch.setenv("RAY_KLEIN_DEBUG", "1")
    handle = create_remote_actor(
        _AsyncActor,
        ray_remote_args={"name": "test-debug-actor"},
    )
    assert klein_ray.get(handle.ping()) == "pong"
    thread = handle.inner_actor._klein_debug_loop_thread

    klein_ray.kill_actor_by_name("test-debug-actor")

    assert "test-debug-actor" not in klein_ray.KLEIN_DEBUG_OBJECT_STORE
    assert not thread.is_alive()


def test_kill_of_stuck_debug_actor_is_bounded(monkeypatch) -> None:
    monkeypatch.setattr(klein_ray, "_DEBUG_ACTOR_STOP_TIMEOUT_SECONDS", 0.01)
    handle = create_remote_actor(_StuckActor, local_mode=True)
    assert klein_ray.get(handle.ping()) == "pong"
    loop = handle.inner_actor._klein_debug_loop
    thread = handle.inner_actor._klein_debug_loop_thread

    started = time.monotonic()
    klein_ray.kill(handle)

    assert time.monotonic() - started < 1
    assert not loop.is_running()
    assert not thread.is_alive()


def test_actor_options_do_not_mutate_callers(monkeypatch) -> None:
    options = {"runtime_env": {"env_vars": {"EXISTING": "1"}}}
    captured = {}

    class _Remote:
        @staticmethod
        def remote(**kwargs):
            return object()

    def create(_actor_class, **kwargs):
        captured.update(kwargs)
        return _Remote()

    monkeypatch.setattr("ray.klein.runtime.actor._create_remote_actor", create)
    monkeypatch.setattr("ray.klein.runtime.actor.ray.klein.is_debug_mode", lambda: False)

    create_remote_actor(_AsyncActor, ray_remote_args=options)

    assert options == {"runtime_env": {"env_vars": {"EXISTING": "1"}}}
    assert captured["runtime_env"]["env_vars"] == {
        "EXISTING": "1",
        "RAY_WARN_BLOCKING_GET_INSIDE_ASYNC": "0",
    }


def test_control_plane_actor_uses_public_runtime_context_api(monkeypatch) -> None:
    captured = {}

    class _Remote:
        @staticmethod
        def remote(**kwargs):
            return object()

    def create(_actor_class, **kwargs):
        captured.update(kwargs)
        return _Remote()

    actor_class = type(ComponentName.KLEIN_JOB_MANAGER, (), {})
    node_id = "1" * 56
    runtime_context = SimpleNamespace(get_node_id=lambda: node_id)
    monkeypatch.setattr("ray.klein.runtime.actor._create_remote_actor", create)
    monkeypatch.setattr("ray.klein.runtime.actor.ray.klein.is_debug_mode", lambda: False)
    monkeypatch.setattr("ray.klein.runtime.actor.ray.get_runtime_context", lambda: runtime_context)

    create_remote_actor(actor_class)

    strategy = captured["scheduling_strategy"]
    assert strategy.node_id == node_id
    assert strategy.soft is False
