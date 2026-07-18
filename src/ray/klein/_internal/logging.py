# SPDX-License-Identifier: Apache-2.0
"""Klein logging built on the standard Python and Ray logging pipeline.

Operational logs, terminal UI output, and records written by a user sink have
different contracts.  This module owns only operational logs.  Ray captures
those records in the normal worker and actor log files, so Klein deliberately
does not reach into Ray's private session directory or replace the root logger.
"""

from __future__ import annotations

import json
import logging
import logging.config
import os
import re
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

DEFAULT_CONFIG_PATH = Path(__file__).with_name("logging.yaml")
TRACE_LEVEL = logging.DEBUG - 1
_LOG_CONTEXT: ContextVar[dict[str, Any] | None] = ContextVar("ray_klein_log_context", default=None)
_CONTEXT_KEYS = (
    "job_id",
    "job_name",
    "operator_id",
    "operator_name",
    "task_id",
    "task_name",
    "subtask_index",
    "checkpoint_id",
)
_SECRET_FIELD = re.compile(
    r"(?:^|[.\-_])(password|passwd|secret|token|credential|api[.\-_]?key)(?:$|[.\-_])",
    re.IGNORECASE,
)

logging.addLevelName(TRACE_LEVEL, "TRACE")


def get_logger(name: str | None = None) -> logging.Logger:
    """Return a child of the ``ray.klein`` logger.

    Passing ``__name__`` follows the convention used throughout Ray and keeps
    the component that emitted a record queryable in both text and JSON logs.
    """

    if not name or name == "ray.klein":
        return logging.getLogger("ray.klein")
    if name.startswith("ray.klein."):
        return logging.getLogger(name)
    return logging.getLogger(f"ray.klein.{name}")


@contextmanager
def log_context(**fields: Any) -> Iterator[None]:
    """Bind structured fields to every log record in the current task/thread."""

    clean_fields = _safe_fields(fields)
    token = _LOG_CONTEXT.set({**(_LOG_CONTEXT.get() or {}), **clean_fields})
    try:
        yield
    finally:
        _LOG_CONTEXT.reset(token)


def log_event(
    target: logging.Logger,
    level: int,
    event: str,
    message: str,
    *args: Any,
    exc_info: Any = None,
    **fields: Any,
) -> None:
    """Emit a named operational event with structured context.

    Event names use a stable dotted vocabulary such as ``job.status.changed``
    or ``checkpoint.completed``.  Human-readable text remains concise while
    JSON consumers and the dashboard can filter on the event and context.
    """

    target.log(
        level,
        message,
        *args,
        exc_info=exc_info,
        extra={
            "klein_event": event,
            "klein_fields": _safe_fields(fields),
        },
    )


class _KleinContextFilter(logging.Filter):
    """Add normalized component and context fields to a LogRecord."""

    def filter(self, record: logging.LogRecord) -> bool:
        if getattr(record, "hide", False):
            return False
        record.klein_event = getattr(record, "klein_event", "-")
        record.klein_component = _component_name(record)
        explicit = getattr(record, "klein_fields", {})
        context = _safe_fields({**_ray_log_context(), **(_LOG_CONTEXT.get() or {}), **explicit})
        record.klein_fields = context
        record.klein_context = _context_text(context)
        for key in _CONTEXT_KEYS:
            setattr(record, f"klein_{key}", context.get(key))
        return True


class _KleinJsonFormatter(logging.Formatter):
    """Serialize one operational event as a single JSON object."""

    def format(self, record: logging.LogRecord) -> str:
        if not hasattr(record, "klein_fields"):
            _KleinContextFilter().filter(record)
        payload: dict[str, Any] = {
            **record.klein_fields,
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "component": record.klein_component,
            "event": record.klein_event,
            "message": record.getMessage(),
            "process_id": record.process,
            "thread_name": record.threadName,
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str, separators=(",", ":"))


def configure_logging(
    config_path: str | os.PathLike[str] | None = None,
    *,
    level: str | int | None = None,
    log_format: str | None = None,
) -> None:
    """Configure Klein's logger without modifying application/root handlers.

    ``RAY_KLEIN_LOGGING_CONFIG`` selects a custom dictConfig YAML file.
    ``RAY_KLEIN_LOG_LEVEL`` overrides the logger level and
    ``RAY_KLEIN_LOG_FORMAT`` accepts ``text`` or ``json``.  Operational logs go
    to stderr; stdout remains available for data sinks and interactive output.
    """

    selected_path = Path(config_path or os.environ.get("RAY_KLEIN_LOGGING_CONFIG", DEFAULT_CONFIG_PATH)).expanduser()
    with selected_path.open(encoding="utf-8") as file:
        config = yaml.safe_load(file)
    if not isinstance(config, dict):
        raise TypeError(f"Logging configuration must be a mapping: {selected_path}")

    selected_level = level or os.environ.get("RAY_KLEIN_LOG_LEVEL")
    if selected_level is not None:
        config.setdefault("loggers", {}).setdefault("ray.klein", {})["level"] = _normalize_level(selected_level)

    selected_format = (log_format or os.environ.get("RAY_KLEIN_LOG_FORMAT", "text")).lower()
    if selected_format not in {"text", "json"}:
        raise ValueError("RAY_KLEIN_LOG_FORMAT must be 'text' or 'json'")
    formatter_name = "json" if selected_format == "json" else "text"
    for handler in config.get("handlers", {}).values():
        handler["formatter"] = formatter_name
    logging.config.dictConfig(config)


def reset_logging() -> None:
    """Remove Klein handlers and restore ordinary library-log propagation."""

    root = get_logger()
    for handler in list(root.handlers):
        handler.close()
        root.removeHandler(handler)
    root.setLevel(logging.NOTSET)
    root.propagate = True
    _install_fallback_handler()


def _normalize_level(level: str | int) -> str | int:
    if isinstance(level, int):
        return level
    normalized = level.upper()
    if normalized == "TRACE":
        return TRACE_LEVEL
    if normalized not in logging.getLevelNamesMapping():
        raise ValueError(f"Unknown Klein log level: {level}")
    return normalized


def _component_name(record: logging.LogRecord) -> str:
    if record.name.startswith("ray.klein."):
        return record.name.removeprefix("ray.klein.")
    marker = f"{os.sep}ray{os.sep}klein{os.sep}"
    if marker in record.pathname:
        relative = record.pathname.split(marker, 1)[1]
        return relative.removesuffix(".py").replace(os.sep, ".")
    return record.module


def _context_text(context: dict[str, Any]) -> str:
    if not context:
        return ""
    fields = " ".join(f"{key}={context[key]}" for key in sorted(context))
    return f" {fields}"


def _safe_fields(fields: dict[str, Any]) -> dict[str, Any]:
    """Drop nulls and redact credential-like structured fields."""

    return {
        key: "<redacted>" if _SECRET_FIELD.search(key) else value for key, value in fields.items() if value is not None
    }


def _ray_log_context() -> dict[str, Any]:
    """Best-effort actor/job context without making logging depend on Ray init."""

    try:
        import ray

        if not ray.is_initialized():
            return {}
        runtime_context = ray.get_runtime_context()
        namespace = runtime_context.namespace
        actor_name = runtime_context.get_actor_name()
        result = {}
        if namespace and namespace.startswith("klein-"):
            result["job_id"] = namespace
        if actor_name:
            result["task_name"] = actor_name
        return result
    except Exception:
        return {}


def _install_fallback_handler() -> None:
    # Libraries must be quiet until the application opts in.  A NullHandler is
    # still useful when this module is imported without ray.klein.__init__.
    root = get_logger()
    if not root.handlers:
        root.addHandler(logging.NullHandler())


_install_fallback_handler()
