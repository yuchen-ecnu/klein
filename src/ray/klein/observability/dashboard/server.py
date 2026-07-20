# SPDX-License-Identifier: Apache-2.0
"""Small, dependency-free web server for the bundled Klein Dashboard.

The server intentionally sits on top of :mod:`ray.klein.observability.state_api`
instead of importing Ray Dashboard internals.  This keeps it usable across Ray
patch releases and gives operators one stable HTTP boundary for read and
rescale actions. The Klein React application is packaged with ``ray-klein``;
Ray-owned navigation remains an external link to the native Ray Dashboard.
"""

from __future__ import annotations

import asyncio
import ipaddress
import json
import mimetypes
import socket
from collections.abc import Callable, Collection
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib.resources import files
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import unquote, urlsplit, urlunsplit
from urllib.request import Request as URLRequest
from urllib.request import urlopen

_MAX_REQUEST_BYTES = 64 * 1024


class _DashboardState(Protocol):
    def list_jobs(self) -> list[dict[str, Any]]: ...

    def get_job(self, job_id: str) -> dict[str, Any] | None: ...

    def cancel_job(self, job_id: str, timeout: int = 60) -> bool: ...

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
    def cancel_job(job_id: str, timeout: int = 60) -> bool:
        from ray.klein.observability.state_api import cancel_job

        return cancel_job(job_id, timeout=timeout)

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
        ray_dashboard_url: str,
        frontend_url: str | None,
        trusted_hosts: Collection[str] | None = None,
    ) -> None:
        self.state = state
        self.ray_dashboard_url = _normalize_ray_dashboard_url(ray_dashboard_url)
        self.frontend_url = None if frontend_url is None else _normalize_frontend_url(frontend_url)
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
        if path == "/__klein/navigation.js":
            self._send_bytes(
                HTTPStatus.OK,
                _navigation_bridge(self.server.ray_dashboard_url),
                "text/javascript; charset=utf-8",
            )
            return
        if self._do_klein_get(path):
            return
        if self.server.frontend_url is not None:
            self._proxy_get(self.server.frontend_url, inject_navigation=path == "/")
            return
        if self._serve_embedded_frontend(path):
            return
        self._send_error_json(HTTPStatus.NOT_FOUND, "Not found")

    def _serve_embedded_frontend(self, path: str) -> bool:
        relative_path = _embedded_frontend_path(path)
        if relative_path is None:
            return False
        try:
            payload = files("ray.klein.observability.dashboard").joinpath("static").joinpath(relative_path).read_bytes()
        except (FileNotFoundError, IsADirectoryError):
            return False
        if relative_path == "index.html":
            payload = _inject_navigation_bridge(payload)
        content_type = mimetypes.guess_type(relative_path)[0] or "application/octet-stream"
        if content_type.startswith("text/") or content_type in {"application/javascript", "image/svg+xml"}:
            content_type = f"{content_type}; charset=utf-8"
        self._send_bytes(HTTPStatus.OK, payload, content_type)
        return True

    def _do_klein_get(self, path: str) -> bool:
        segments = _path_segments(path)
        if segments == ["api", "config"]:
            self._send_json(
                HTTPStatus.OK,
                {"ray_dashboard_url": self.server.ray_dashboard_url},
            )
            return True
        if segments in (["api", "jobs"], ["api", "klein", "jobs"]):
            self._state_call(lambda: self._send_json(HTTPStatus.OK, {"jobs": self.server.state.list_jobs()}))
            return True
        is_legacy_detail = len(segments) == 3 and segments[:2] == ["api", "jobs"]
        is_ray_detail = len(segments) == 4 and segments[:3] == ["api", "klein", "jobs"]
        if not (is_legacy_detail or is_ray_detail):
            return False
        job_id = segments[-1]

        def _get_job() -> None:
            snapshot = self.server.state.get_job(job_id)
            if snapshot is None:
                self._send_error_json(HTTPStatus.NOT_FOUND, f"Unknown Klein job: {job_id}")
                return
            payload: Any = {"job": snapshot} if is_ray_detail else snapshot
            self._send_json(HTTPStatus.OK, payload)

        self._state_call(_get_job)
        return True

    def do_POST(self) -> None:
        if not self._require_trusted_host():
            return
        self._do_trusted_post()

    def _do_trusted_post(self) -> None:
        segments = _path_segments(urlsplit(self.path).path)
        if self._do_klein_post(segments):
            return
        if (
            len(segments) != 6
            or segments[:2] != ["api", "jobs"]
            or segments[3] != "operators"
            or segments[5] != "rescale"
        ):
            self._send_error_json(HTTPStatus.NOT_FOUND, "Not found")
            return
        self._rescale_operator(segments[2], segments[4])

    def _rescale_operator(self, job_id: str, raw_operator_id: str) -> None:
        if not self._same_origin_request():
            self._send_error_json(HTTPStatus.FORBIDDEN, "Cross-origin control requests are not allowed")
            return

        try:
            operator_id = int(raw_operator_id)
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

    def _do_klein_post(self, segments: list[str]) -> bool:
        if len(segments) == 5 and segments[:3] == ["api", "klein", "jobs"] and segments[4] == "cancel":
            self._cancel_job(segments[3])
            return True
        if (
            len(segments) == 7
            and segments[:3] == ["api", "klein", "jobs"]
            and segments[4] == "operators"
            and segments[6] == "rescale"
        ):
            self._rescale_operator(segments[3], segments[5])
            return True
        return False

    def _cancel_job(self, job_id: str) -> None:
        if not self._same_origin_request():
            self._send_error_json(HTTPStatus.FORBIDDEN, "Cross-origin control requests are not allowed")
            return

        def _cancel() -> None:
            cancelled = self.server.state.cancel_job(job_id)
            if not cancelled:
                self._send_error_json(HTTPStatus.NOT_FOUND, f"Unknown Klein job: {job_id}")
                return
            self._send_json(HTTPStatus.OK, {"job_id": job_id, "cancelled": True})

        self._state_call(_cancel)

    def _proxy_get(self, base_url: str, *, inject_navigation: bool = False) -> None:
        upstream_url = _join_upstream_url(base_url, self.path)
        request = URLRequest(
            upstream_url,
            headers={
                "Accept": self.headers.get("Accept", "*/*"),
                "Accept-Language": self.headers.get("Accept-Language", "en"),
                "Accept-Encoding": "identity",
                "User-Agent": self.headers.get("User-Agent", "ray-klein-dashboard"),
            },
        )
        try:
            with urlopen(request, timeout=10) as response:
                payload = response.read()
                content_type = response.headers.get_content_type()
                if inject_navigation and content_type == "text/html":
                    payload = _inject_navigation_bridge(payload)
                self._send_proxy_response(response.status, payload, response.headers.get("Content-Type"))
        except HTTPError as error:
            self._send_proxy_response(error.code, error.read(), error.headers.get("Content-Type"))
        except (OSError, URLError) as error:
            self._send_error_json(
                HTTPStatus.BAD_GATEWAY,
                f"Klein Dashboard frontend is unavailable: {error}",
            )

    def _send_proxy_response(self, status: int, payload: bytes, content_type: str | None) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type or "application/octet-stream")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(payload)

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
            "default-src 'self'; connect-src 'self'; font-src 'self' data:; img-src 'self' data:; "
            "script-src 'self'; style-src 'self' 'unsafe-inline'",
        )
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, _format: str, *_args: Any) -> None:
        # The CLI already announces the listener. Avoid leaking job identifiers
        # through the base class' unconditional stderr access log.
        return


def _path_segments(path: str) -> list[str]:
    return [unquote(segment) for segment in path.split("/") if segment]


def _embedded_frontend_path(path: str) -> str | None:
    if path in {"/", "/index.html"}:
        return "index.html"
    if not path.startswith("/assets/"):
        return None
    asset_name = unquote(path.removeprefix("/assets/"))
    if not asset_name or "/" in asset_name or "\\" in asset_name or asset_name in {".", ".."}:
        return None
    return f"assets/{asset_name}"


def create_dashboard_server(
    host: str = "127.0.0.1",
    port: int = 8266,
    *,
    state: _DashboardState | None = None,
    trusted_hosts: Collection[str] | None = None,
    ray_dashboard_url: str = "http://127.0.0.1:8265",
    frontend_url: str | None = None,
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
        ray_dashboard_url,
        frontend_url,
        trusted_hosts,
    )


def serve_dashboard(
    host: str = "127.0.0.1",
    port: int = 8266,
    *,
    ray_dashboard_url: str = "http://127.0.0.1:8265",
    frontend_url: str | None = None,
) -> None:
    """Serve the Klein Dashboard until interrupted."""

    server = create_dashboard_server(
        host,
        port,
        ray_dashboard_url=ray_dashboard_url,
        frontend_url=frontend_url,
    )
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


def _normalize_ray_dashboard_url(value: str) -> str:
    if not isinstance(value, str):
        raise TypeError("ray_dashboard_url must be a string")
    candidate = value.strip()
    parsed = urlsplit(candidate)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("ray_dashboard_url must be an absolute HTTP(S) URL without credentials, query, or fragment")
    try:
        _ = parsed.port
    except ValueError as error:
        raise ValueError("ray_dashboard_url contains an invalid port") from error
    path = parsed.path.rstrip("/")
    return urlunsplit((parsed.scheme, parsed.netloc, path, "", ""))


def _normalize_frontend_url(value: str) -> str:
    try:
        return _normalize_ray_dashboard_url(value)
    except (TypeError, ValueError) as error:
        raise type(error)(str(error).replace("ray_dashboard_url", "frontend_url")) from error


def _join_upstream_url(base_url: str, request_target: str) -> str:
    base = urlsplit(base_url)
    request = urlsplit(request_target)
    base_path = base.path.rstrip("/")
    request_path = request.path if request.path.startswith("/") else f"/{request.path}"
    return urlunsplit((base.scheme, base.netloc, f"{base_path}{request_path}", request.query, ""))


def _inject_navigation_bridge(payload: bytes) -> bytes:
    marker = b"</head>"
    script = b'<script src="__klein/navigation.js"></script>'
    if marker in payload.lower():
        index = payload.lower().index(marker)
        return payload[:index] + script + payload[index:]
    return payload + script


def _navigation_bridge(ray_dashboard_url: str) -> bytes:
    encoded_url = json.dumps(ray_dashboard_url, ensure_ascii=True).replace("<", "\\u003c")
    return f"""(() => {{
  const rayDashboardUrl = {encoded_url}.replace(/\\/+$/, "");
  const kleinPrefix = "#/klein";
  if (!window.location.hash || window.location.hash === "#/" || window.location.hash === "#") {{
    window.location.replace(`${{window.location.pathname}}${{window.location.search}}#/klein`);
  }}
  document.addEventListener("click", (event) => {{
    const target = event.target instanceof Element ? event.target.closest("a") : null;
    if (!target) return;
    const href = target.getAttribute("href");
    if (!href) return;
    const destination = new URL(href, window.location.href);
    const route = destination.hash;
    if (destination.origin === window.location.origin && route.startsWith("#/") && !route.startsWith(kleinPrefix)) {{
      event.preventDefault();
      event.stopImmediatePropagation();
      window.location.assign(`${{rayDashboardUrl}}/${{route}}`);
    }}
  }}, true);
}})();
""".encode()


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
