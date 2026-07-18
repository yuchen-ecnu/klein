# SPDX-License-Identifier: Apache-2.0
"""Declarative definitions for Klein's built-in metrics."""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum


class MetricKind(str, Enum):
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"


_METRIC_NAME = re.compile(r"^[a-z][a-z0-9_]*$")


@dataclass(frozen=True, slots=True)
class MetricSpec:
    """The stable contract of one built-in metric.

    Metric names deliberately omit Prometheus suffixes such as ``_total``.
    Ray's exporter adds those suffixes for counters.
    """

    name: str
    kind: MetricKind
    description: str
    boundaries: tuple[float, ...] = ()

    def __post_init__(self) -> None:
        if not _METRIC_NAME.fullmatch(self.name):
            raise ValueError(f"invalid built-in metric name: {self.name!r}")
        if not self.description.strip():
            raise ValueError(f"metric {self.name!r} must have a description")
        if self.kind is MetricKind.HISTOGRAM:
            if not self.boundaries:
                raise ValueError(f"histogram {self.name!r} must define boundaries")
            if tuple(sorted(set(self.boundaries))) != self.boundaries:
                raise ValueError(f"histogram {self.name!r} boundaries must be unique and increasing")
        elif self.boundaries:
            raise ValueError(f"only histograms may define boundaries: {self.name!r}")
