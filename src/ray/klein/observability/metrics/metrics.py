# SPDX-License-Identifier: Apache-2.0
import time
from abc import ABC, abstractmethod
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import ray.util.metrics

from ray.klein.observability.metrics.metric_spec import MetricKind


class Metrics(ABC):
    """Common interface for Klein's small Ray metric wrappers."""

    @property
    @abstractmethod
    def info(self) -> dict[str, Any]:
        """Metric metadata exposed by Ray."""


class Counter(Metrics):
    def __init__(self, counter: ray.util.metrics.Counter) -> None:
        self._counter = counter
        self._value: int | float = 0

    def inc(self, value: int | float = 1.0, tags: dict[str, str] | None = None) -> None:
        if value < 0:
            raise ValueError("counter increments must be non-negative")
        if value == 0:
            return
        self._counter.inc(value, tags)
        self._value += value

    @property
    def value(self) -> int | float:
        """Task-local readable mirror; Ray's metric handle is write-only."""

        return self._value

    @property
    def info(self) -> dict[str, Any]:
        return self._counter.info


class Gauge(Metrics):
    def __init__(self, gauge: ray.util.metrics.Gauge) -> None:
        self._gauge = gauge
        self._value: int | float = 0

    def set(self, value: int | float, tags: dict[str, str] | None = None) -> None:
        self._gauge.set(value, tags)
        self._value = value

    def mark_idle(self, tags: dict[str, str] | None = None) -> None:
        """Reset a data-driven gauge so its last sample is not read as live data."""
        self._gauge.set(0, tags)
        self._value = 0

    @property
    def value(self) -> int | float:
        return self._value

    @property
    def info(self) -> dict[str, Any]:
        return self._gauge.info


class Histogram(Metrics):
    def __init__(self, histogram: ray.util.metrics.Histogram) -> None:
        self._histogram = histogram
        self._count = 0
        self._sum = 0.0
        self._last = 0.0
        self._max = 0.0

    def observe(self, value: int | float, tags: dict[str, str] | None = None) -> None:
        if value < 0:
            raise ValueError("histogram observations must be non-negative")
        self._histogram.observe(value, tags)
        numeric = float(value)
        self._count += 1
        self._sum += numeric
        self._last = numeric
        self._max = max(self._max, numeric)

    @property
    def count(self) -> int:
        return self._count

    @property
    def last(self) -> float:
        return self._last

    @property
    def maximum(self) -> float:
        return self._max

    @property
    def mean(self) -> float:
        return 0.0 if self._count == 0 else self._sum / self._count

    def observe_elapsed(self, started_at: float, tags: dict[str, str] | None = None) -> float:
        """Observe elapsed monotonic time and return the duration in milliseconds."""

        elapsed_ms = (time.monotonic() - started_at) * 1000
        self.observe(elapsed_ms, tags)
        return elapsed_ms

    @contextmanager
    def time(self, tags: dict[str, str] | None = None) -> Iterator[None]:
        """Context manager for synchronous operations measured in milliseconds."""

        started_at = time.monotonic()
        try:
            yield
        finally:
            self.observe_elapsed(started_at, tags)

    @property
    def info(self) -> dict[str, Any]:
        return self._histogram.info


def create_metric(
    kind: MetricKind,
    name: str,
    labels: dict[str, str],
    boundaries: tuple[float, ...] = (),
    description: str = "",
) -> Counter | Gauge | Histogram:
    """Create one Ray native metric with a stable default label set."""

    tag_keys = tuple(labels)
    if kind is MetricKind.COUNTER:
        raw = ray.util.metrics.Counter(name=name, description=description, tag_keys=tag_keys)
        raw.set_default_tags(labels)
        return Counter(raw)
    if kind is MetricKind.GAUGE:
        raw = ray.util.metrics.Gauge(name=name, description=description, tag_keys=tag_keys)
        raw.set_default_tags(labels)
        return Gauge(raw)
    raw = ray.util.metrics.Histogram(
        name=name,
        description=description,
        boundaries=list(boundaries),
        tag_keys=tag_keys,
    )
    raw.set_default_tags(labels)
    return Histogram(raw)
