# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from ray.klein._internal import ray as klein_ray


def reset_debug_runtime() -> None:
    """Stop and forget every in-process actor created by Klein debug mode."""

    handles = list({id(handle): handle for handle in klein_ray.KLEIN_DEBUG_OBJECT_STORE.values()}.values())
    for handle in handles:
        klein_ray.kill(handle)
    klein_ray.KLEIN_DEBUG_OBJECT_STORE.clear()
