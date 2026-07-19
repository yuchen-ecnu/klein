# SPDX-License-Identifier: Apache-2.0
"""Small, dependency-free web dashboard for published Klein jobs.

The server intentionally sits on top of :mod:`ray.klein.observability.state_api`
instead of importing Ray Dashboard internals.  This keeps it usable across Ray
patch releases and gives operators one stable HTTP boundary for read and
rescale actions.
"""

from __future__ import annotations

import asyncio
import ipaddress
import json
import socket
from collections.abc import Callable, Collection
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib.resources import files
from typing import Any, Protocol
from urllib.parse import unquote, urlsplit

_MAX_REQUEST_BYTES = 64 * 1024
_STATIC_PACKAGE = "ray.klein.observability.dashboard"
_ASSETS = {
    "/": ("static/index.html", "text/html; charset=utf-8"),
    "/assets/dashboard.css": ("static/dashboard.css", "text/css; charset=utf-8"),
    "/assets/dashboard.js": ("static/dashboard.js", "text/javascript; charset=utf-8"),
}


class _DashboardState(Protocol):
    def list_jobs(self) -> list[dict[str, Any]]: ...

    def get_job(self, job_id: str) -> dict[str, Any] | None: ...

    def rescale_operator(self, job_id: str, operator_id: int, parallelism: int) -> Any: ...


class _PublishedState:
    """Lazy adapter so importing the HTTP server does not initialize Ray."""

    @staticmethod
    def list_jobs() -> list[dict[str, Any]]:
        from ray.klein.observability.state_api import list_job_snapshots

        return list_job_snapshots()

    @staticmethod
    def get_job(job_id: str) -> dict[str, Any] | None:
        from ray.klein.observability.state_api import get_job_snapshot

        return get_job_snapshot(job_id)

    @staticmethod
    def rescale_operator(job_id: str, operator_id: int, parallelism: int) -> Any:
        from ray.klein.observability.state_api import rescale_operator

        return rescale_operator(job_id, operator_id, parallelism)


class _DashboardHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self,
        server_address: tuple[str, int],
        state: _DashboardState,
        trusted_hosts: Collection[str] | None = None,
    ) -> None:
        self.state = state
        self.trusted_hosts, self.allow_ip_hosts = _trusted_hosts_for_listener(
            server_address[0],
            trusted_hosts,
        )
        super().__init__(server_address, _DashboardRequestHandler)

    def is_trusted_host(self, authority: str | None) -> bool:
        host = _authority_hostname(authority)
        if host is None:
            return False
        if host in self.trusted_hosts:
            return True
        if not self.allow_ip_hosts:
            return False
        try:
            ipaddress.ip_address(host)
        except ValueError:
            return False
        return True


class _IPv6DashboardHTTPServer(_DashboardHTTPServer):
    address_family = socket.AF_INET6


class _DashboardRequestHandler(BaseHTTPRequestHandler):
    server: _DashboardHTTPServer

    def do_GET(self) -> None:
        if not self._require_trusted_host():
            return
        path = urlsplit(self.path).path
        if path in _ASSETS:
            resource_name, content_type = _ASSETS[path]
            self._send_asset(resource_name, content_type)
            return

        segments = _path_segments(path)
        if segments == ["api", "jobs"]:
            self._state_call(lambda: self._send_json(HTTPStatus.OK, {"jobs": self.server.state.list_jobs()}))
            return
        if len(segments) == 3 and segments[:2] == ["api", "jobs"]:
            job_id = segments[2]

            def _get_job() -> None:
                snapshot = self.server.state.get_job(job_id)
                if snapshot is None:
                    self._send_error_json(HTTPStatus.NOT_FOUND, f"Unknown Klein job: {job_id}")
                    return
                self._send_json(HTTPStatus.OK, snapshot)

            self._state_call(_get_job)
            return
        self._send_error_json(HTTPStatus.NOT_FOUND, "Not found")

    def do_POST(self) -> None:
        if not self._require_trusted_host():
            return
        self._do_trusted_post()

    def _do_trusted_post(self) -> None:
        segments = _path_segments(urlsplit(self.path).path)
        if (
            len(segments) != 6
            or segments[:2] != ["api", "jobs"]
            or segments[3] != "operators"
            or segments[5] != "rescale"
        ):
            self._send_error_json(HTTPStatus.NOT_FOUND, "Not found")
            return
        if not self._same_origin_request():
            self._send_error_json(HTTPStatus.FORBIDDEN, "Cross-origin control requests are not allowed")
            return

        job_id = segments[2]
        try:
            operator_id = int(segments[4])
        except ValueError:
            self._send_error_json(HTTPStatus.BAD_REQUEST, "operator_id must be an integer")
            return
        if operator_id < 0:
            self._send_error_json(HTTPStatus.BAD_REQUEST, "operator_id must be non-negative")
            return

        body = self._read_json_body()
        if body is None:
            return
        parallelism = body.get("parallelism")
        if type(parallelism) is not int or parallelism < 1:
            self._send_error_json(HTTPStatus.BAD_REQUEST, "parallelism must be a positive integer")
            return

        def _rescale() -> None:
            result = self.server.state.rescale_operator(job_id, operator_id, parallelism)
            if result is False or result is None:
                self._send_error_json(HTTPStatus.NOT_FOUND, "The job or operator is no longer available")
                return
            if isinstance(result, dict):
                response = result
            else:
                response = {
                    "accepted": bool(result),
                    "job_id": job_id,
                    "operator_id": operator_id,
                    "parallelism": parallelism,
                }
            self._send_json(HTTPStatus.OK, response)

        self._state_call(_rescale)

    def _state_call(self, action: Callable[[], None]) -> None:
        try:
            action()
        except Exception as error:
            timeout_error = _find_timeout_error(error)
            if timeout_error is not None:
                self._send_error_json(
                    HTTPStatus.GATEWAY_TIMEOUT,
                    str(timeout_error) or "Klein control request timed out",
                )
                return
            self._send_error_json(
                HTTPStatus.SERVICE_UNAVAILABLE,
                f"Klein state service unavailable: {type(error).__name__}: {error}",
            )

    def _require_trusted_host(self) -> bool:
        if self.server.is_trusted_host(self.headers.get("Host")):
            return True
        self._send_error_json(HTTPStatus.FORBIDDEN, "Untrusted Host header")
        return False

    def _read_json_body(self) -> dict[str, Any] | None:
        content_type = self.headers.get("Content-Type", "").partition(";")[0].strip().lower()
        if content_type != "application/json":
            self._send_error_json(HTTPStatus.UNSUPPORTED_MEDIA_TYPE, "Content-Type must be application/json")
            return None
        try:
            length = int(self.headers.get("Content-Length", ""))
        except ValueError:
            self._send_error_json(HTTPStatus.BAD_REQUEST, "A valid Content-Length header is required")
            return None
        if length < 1 or length > _MAX_REQUEST_BYTES:
            self._send_error_json(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, "Request body size is invalid")
            return None
        try:
            value = json.loads(self.rfile.read(length))
        except (UnicodeDecodeError, json.JSONDecodeError):
            self._send_error_json(HTTPStatus.BAD_REQUEST, "Request body must be valid JSON")
            return None
        if not isinstance(value, dict):
            self._send_error_json(HTTPStatus.BAD_REQUEST, "Request body must be a JSON object")
            return None
        return value

    def _same_origin_request(self) -> bool:
        origin = self.headers.get("Origin")
        if not origin:
            return True
        authority = self.headers.get("Host")
        if not authority:
            return False
        parsed = urlsplit(origin)
        return parsed.scheme in {"http", "https"} and parsed.netloc == authority

    def _send_asset(self, resource_name: str, content_type: str) -> None:
        try:
            payload = files(_STATIC_PACKAGE).joinpath(resource_name).read_bytes()
        except (FileNotFoundError, ModuleNotFoundError):
            self._send_error_json(HTTPStatus.NOT_FOUND, "Dashboard asset not found")
            return
        self._send_bytes(HTTPStatus.OK, payload, content_type)

    def _send_json(self, status: HTTPStatus, value: Any) -> None:
        payload = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self._send_bytes(status, payload, "application/json; charset=utf-8")

    def _send_error_json(self, status: HTTPStatus, message: str) -> None:
        self._send_json(status, {"error": message, "status": int(status)})

    def _send_bytes(self, status: HTTPStatus, payload: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header(
            "Content-Security-Policy",
            "default-src 'self'; connect-src 'self'; img-src 'self' data:; script-src 'self'; style-src 'self'",
        )
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, _format: str, *_args: Any) -> None:
        # The CLI already announces the listener. Avoid leaking job identifiers
        # through the base class' unconditional stderr access log.
        return


def _path_segments(path: str) -> list[str]:
    return [unquote(segment) for segment in path.split("/") if segment]


def create_dashboard_server(
    host: str = "127.0.0.1",
    port: int = 8266,
    *,
    state: _DashboardState | None = None,
    trusted_hosts: Collection[str] | None = None,
) -> ThreadingHTTPServer:
    """Create a Klein Dashboard server without starting its blocking loop."""

    if not host:
        raise ValueError("host cannot be empty")
    if not 0 <= port <= 65535:
        raise ValueError("port must be between 0 and 65535")
    server_type = _IPv6DashboardHTTPServer if _is_ipv6_literal(host) else _DashboardHTTPServer
    return server_type(
        (host, port),
        state if state is not None else _PublishedState(),
        trusted_hosts,
    )


def serve_dashboard(host: str = "127.0.0.1", port: int = 8266) -> None:
    """Serve the Klein Dashboard until interrupted."""

    server = create_dashboard_server(host, port)
    try:
        server.serve_forever()
    finally:
        server.server_close()


def _trusted_hosts_for_listener(
    listener_host: str,
    configured_hosts: Collection[str] | None,
) -> tuple[frozenset[str], bool]:
    normalized_listener = _normalize_host(listener_host)
    allow_ip_hosts = normalized_listener in {"0.0.0.0", "::"}
    trusted = set()
    if not allow_ip_hosts:
        trusted.add(normalized_listener)
    if allow_ip_hosts or normalized_listener == "localhost" or _is_loopback_ip(normalized_listener):
        trusted.add("localhost")
    if configured_hosts is not None:
        if isinstance(configured_hosts, str):
            raise TypeError("trusted_hosts must be a collection of host names")
        for configured_host in configured_hosts:
            hostname = _authority_hostname(configured_host)
            if hostname is None:
                hostname = _normalize_host(configured_host)
            trusted.add(hostname)
    return frozenset(trusted), allow_ip_hosts


def _authority_hostname(authority: str | None) -> str | None:
    if not authority:
        return None
    try:
        parsed = urlsplit(f"//{authority}")
        if parsed.username is not None or parsed.password is not None or parsed.path:
            return None
        host = parsed.hostname
        # Accessing port validates malformed/non-numeric port spellings.
        _ = parsed.port
    except (TypeError, ValueError):
        return None
    return None if host is None else _normalize_host(host)


def _normalize_host(host: str) -> str:
    candidate = host.strip().strip("[]").rstrip(".").lower()
    if not candidate:
        raise ValueError("host cannot be empty")
    try:
        return ipaddress.ip_address(candidate).compressed
    except ValueError:
        return candidate


def _is_loopback_ip(host: str) -> bool:
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _is_ipv6_literal(host: str) -> bool:
    try:
        return ipaddress.ip_address(host.strip().strip("[]")).version == 6
    except ValueError:
        return False


def _find_timeout_error(error: BaseException) -> BaseException | None:
    pending = [error]
    seen: set[int] = set()
    while pending:
        candidate = pending.pop()
        if id(candidate) in seen:
            continue
        seen.add(id(candidate))
        nested_errors = [
            nested
            for attribute in ("cause", "__cause__", "__context__")
            if isinstance((nested := getattr(candidate, attribute, None)), BaseException)
        ]
        if isinstance(candidate, (TimeoutError, asyncio.TimeoutError)):
            return next(
                (nested for nested in nested_errors if isinstance(nested, (TimeoutError, asyncio.TimeoutError))),
                candidate,
            )
        pending.extend(nested_errors)
    return None
