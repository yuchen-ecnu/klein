# SPDX-License-Identifier: Apache-2.0
"""Immutable resource requirements shared by logical and physical plans."""

import math
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Resources:
    """CPU, GPU, and concurrency requirements for one logical operator."""

    num_cpus: float | None = None
    num_gpus: float | None = None
    concurrency: int | tuple[int, int] | None = None

    def __post_init__(self) -> None:
        self._validate_resource("num_cpus", self.num_cpus)
        self._validate_resource("num_gpus", self.num_gpus)
        self._validate_concurrency()

    @staticmethod
    def _validate_resource(name: str, value: float | None) -> None:
        if value is None:
            return
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise TypeError(f"{name} must be a finite number or None")
        if not math.isfinite(value) or value < 0:
            raise ValueError(f"{name} must be finite and >= 0, got {value}")

    def _validate_concurrency(self) -> None:
        if self.concurrency is None:
            return
        if isinstance(self.concurrency, tuple):
            if len(self.concurrency) != 2 or any(
                isinstance(value, bool) or not isinstance(value, int) or value <= 0 for value in self.concurrency
            ):
                raise ValueError(f"concurrency tuple must be (min, max) with values > 0, got {self.concurrency}")
            if self.concurrency[0] > self.concurrency[1]:
                raise ValueError(f"concurrency minimum exceeds maximum: {self.concurrency}")
            return
        if isinstance(self.concurrency, bool) or not isinstance(self.concurrency, int):
            raise TypeError("concurrency must be an integer, a (min, max) integer tuple, or None")
        if self.concurrency <= 0:
            raise ValueError(f"concurrency must be > 0, got {self.concurrency}")

    @property
    def cpus(self) -> float:
        return self.num_cpus if self.num_cpus is not None else 1.0

    @property
    def gpus(self) -> float:
        return self.num_gpus if self.num_gpus is not None else 0.0

    @property
    def effective_concurrency(self) -> int | tuple[int, int]:
        """Return raw concurrency, preserving autoscaling ranges."""
        return self.concurrency if self.concurrency is not None else 1

    @property
    def scalar_concurrency(self) -> int:
        """Return the initial parallelism for physical graph expansion."""
        if isinstance(self.concurrency, tuple):
            return self.concurrency[0]
        return self.concurrency if self.concurrency is not None else 1
