# SPDX-License-Identifier: Apache-2.0
"""Helpers for executing lazy terminal sinks in integration tests."""

from typing import Any

from ray.klein.api.stream_sink import StreamSink


def execute_terminal(sink: StreamSink, *, job_name: str) -> Any:
    """Execute exactly one terminal sink and return its result."""
    handle = sink.context.execute(job_name, sinks=(sink,))
    handle.wait()
    return handle.get()
