# SPDX-License-Identifier: Apache-2.0
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RuntimeInfo:
    """Runtime batching and asynchronous buffering settings."""

    batch_size: int | None = None
    batch_format: str | None = None
    batch_timeout: int | None = None
    async_buffer_size: int | None = None

    def __post_init__(self) -> None:
        _validate_positive_integer(self.batch_size, "batch_size")
        _validate_positive_integer(self.batch_timeout, "batch_timeout")
        _validate_positive_integer(self.async_buffer_size, "async_buffer_size")
        if self.batch_format is not None and not isinstance(self.batch_format, str):
            raise TypeError("batch_format must be a string or None")
        if self.batch_enabled and not self.batch_format:
            raise ValueError("batch_format must be a non-empty string when batching is enabled")
        if (
            self.batch_size is not None
            and self.batch_size > 1
            and (self.batch_timeout is None or self.batch_timeout <= 0)
        ):
            raise ValueError("batch_timeout must be positive when batch_size is greater than one")

    @property
    def batch_enabled(self) -> bool:
        return self.batch_size is not None and self.batch_size > 0

    @property
    def async_enabled(self) -> bool:
        return self.async_buffer_size is not None and self.async_buffer_size > 0

    def __str__(self) -> str:
        if self.batch_enabled:
            fields = [
                f"batch_size={self.batch_size}",
                f"batch_format={self.batch_format}",
                f"batch_timeout={self.batch_timeout}",
            ]
            if self.async_enabled:
                fields.append(f"async_buffer_size={self.async_buffer_size}")
            return f"RuntimeInfo({', '.join(fields)})"
        if self.async_enabled:
            return f"RuntimeInfo(async_buffer_size={self.async_buffer_size})"
        return "RuntimeInfo(batch_disabled)"


def _validate_positive_integer(value: int | None, name: str) -> None:
    if value is None:
        return
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer or None")
    if value <= 0:
        raise ValueError(f"{name} must be positive")
