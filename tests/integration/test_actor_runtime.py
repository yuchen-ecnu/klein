# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import ray.klein as klein
from ray.klein.runtime.actor import create_remote_actor


class GreetingActor:
    def greet(self) -> str:
        return "hello"


class NamedWorker:
    def __init__(self, name: str):
        self.name = name

    def greet(self, message: str) -> str:
        return f"{self.name}: {message}"


def test_actor_call() -> None:
    actor = create_remote_actor(GreetingActor)

    assert klein.get(actor.greet()) == "hello"


def test_local_actor_mode() -> None:
    actor = create_remote_actor(NamedWorker, construct_args={"name": "local-worker"}, local_mode=True)

    assert klein.get(actor.greet("hello")) == "local-worker: hello"


def test_remote_actor_mode() -> None:
    actor = create_remote_actor(NamedWorker, construct_args={"name": "remote-worker"})

    assert klein.get(actor.greet("hello")) == "remote-worker: hello"


def test_kill_remote_actor() -> None:
    actor = create_remote_actor(NamedWorker, construct_args={"name": "remote-worker"})

    klein.kill(actor)
