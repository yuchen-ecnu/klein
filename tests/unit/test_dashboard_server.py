# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import importlib
import json
import logging
import re
import socket
import threading
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from urllib.parse import quote, urlsplit

import pytest
from click.testing import CliRunner
from ray.exceptions import RayTaskError

from ray.klein.observability.dashboard.server import _proxy_content_type, create_dashboard_server
from tests.support.waiting import wait_until

cli = importlib.import_module("ray.klein.cli")


class _FakeState:
    def __init__(self) -> None:
        self.job_id = "orders / east"
        self.snapshot = {
            "job_id": self.job_id,
            "job_name": "Orders",
            "status": "RUNNING",
            "operators": [
                {
                    "op_id": 7,
                    "name": "enrich",
                    "parallelism": 2,
                    "max_busy_percent": 80,
                    "max_backpressure_percent": 5,
                }
            ],
            "edges": [],
        }
        self.rescale_calls: list[tuple[str, int, int]] = []
        self.cancel_calls: list[tuple[str, int]] = []
        self.rescale_error: Exception | None = None

    def list_jobs(self):
        return [self.snapshot]

    def get_job(self, job_id):
        return self.snapshot if job_id == self.job_id else None

    def rescale_operator(self, job_id, operator_id, parallelism):
        return self.submit_operator_rescale(job_id, operator_id, parallelism)

    def submit_operator_rescale(self, job_id, operator_id, parallelism):
        self.rescale_calls.append((job_id, operator_id, parallelism))
        if self.rescale_error is not None:
            raise self.rescale_error
        return {
            "operation_id": "resize-1",
            "job_id": job_id,
            "operator_id": operator_id,
            "previous_parallelism": 2,
            "parallelism": parallelism,
            "target_parallelism": parallelism,
            "status": "ACCEPTED",
            "phase": "QUEUED",
        }

    def cancel_job(self, job_id, timeout=60):
        self.cancel_calls.append((job_id, timeout))
        return job_id == self.job_id


class _FakeFrontendHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/":
            payload = b'<html><head><title>Ray Dashboard</title></head><body><main id="root"></main></body></html>'
            content_type = "text/html; charset=utf-8"
            status = 200
        elif self.path == "/static/js/bundle.js":
            payload = b"window.__RAY_DASHBOARD__ = true;"
            content_type = "text/javascript; charset=utf-8"
            status = 200
        elif self.path == "/oversized":
            payload = b"12345"
            content_type = "application/octet-stream"
            status = 200
        elif self.path == "/oversized-error":
            payload = b"12345"
            content_type = "text/plain"
            status = 500
        else:
            self.send_error(404)
            return
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        if self.path != "/oversized-error":
            self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, _format, *_args):
        return


@pytest.fixture
def dashboard_server():
    state = _FakeState()
    server = create_dashboard_server("127.0.0.1", 0, state=state)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server, state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


@pytest.fixture
def frontend_server():
    server = ThreadingHTTPServer(("127.0.0.1", 0), _FakeFrontendHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


@pytest.fixture
def dashboard_log_records():
    records: list[logging.LogRecord] = []

    class _RecordHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    target = logging.getLogger("ray.klein.observability.dashboard.server")
    previous_level = target.level
    previous_disabled = target.disabled
    handler = _RecordHandler()
    target.addHandler(handler)
    target.setLevel(logging.INFO)
    target.disabled = False
    try:
        yield records
    finally:
        target.removeHandler(handler)
        target.setLevel(previous_level)
        target.disabled = previous_disabled
        handler.close()


def _request(server, method, path, *, body=None, headers=None):
    connection = HTTPConnection("127.0.0.1", server.server_port, timeout=2)
    connection.request(method, path, body=body, headers=headers or {})
    response = connection.getresponse()
    payload = response.read()
    response_headers = dict(response.getheaders())
    connection.close()
    return response.status, response_headers, payload


def test_dashboard_serves_the_bundled_frontend(dashboard_server) -> None:
    server, _ = dashboard_server

    status, headers, page = _request(server, "GET", "/")

    assert status == 200
    assert headers["Content-Type"] == "text/html; charset=utf-8"
    assert "connect-src 'self';" in headers["Content-Security-Policy"]
    assert " ws:" not in headers["Content-Security-Policy"]
    assert b'<div id="root"></div>' in page
    assert b'src="__klein/navigation.js"' in page

    pending = [asset.decode() for asset in re.findall(rb'(?:src|href)="\./(assets/[^"]+\.(?:js|css))"', page)]
    assert pending
    served_assets: dict[str, bytes] = {}
    while pending:
        asset_path = pending.pop()
        if asset_path in served_assets:
            continue
        asset_status, asset_headers, asset = _request(server, "GET", f"/{asset_path}")
        assert asset_status == 200
        assert asset_headers["Cache-Control"] == "public, max-age=31536000, immutable"
        if asset_path.endswith(".css"):
            assert asset_headers["Content-Type"] == "text/css; charset=utf-8"
        else:
            assert asset_headers["Content-Type"] in {
                "application/javascript; charset=utf-8",
                "text/javascript; charset=utf-8",
            }
        assert asset
        served_assets[asset_path] = asset
        if asset_path.endswith(".js"):
            pending.extend(
                f"assets/{linked.decode()}" for linked in re.findall(rb'["`]\./([^"`]+\.(?:js|css))["`]', asset)
            )

    javascript = b"".join(asset for path, asset in served_assets.items() if path.endswith(".js"))
    assert all(
        token in javascript
        for token in (
            b"data-busy-percent",
            b"max_busy_percent",
            b"#eaf3fc",
            b"#fadbd8",
            b"#fff0cc",
        )
    )


def test_dashboard_keeps_index_and_navigation_uncached(dashboard_server) -> None:
    server, _ = dashboard_server

    _, page_headers, _ = _request(server, "GET", "/")
    _, navigation_headers, _ = _request(server, "GET", "/__klein/navigation.js")

    assert page_headers["Cache-Control"] == "no-store"
    assert navigation_headers["Cache-Control"] == "no-store"


@pytest.mark.parametrize(
    "path",
    ["/assets/foo%00bar.js", f"/assets/{'a' * 300}.js"],
)
def test_dashboard_returns_not_found_for_invalid_asset_names(
    dashboard_server,
    path,
) -> None:
    server, _ = dashboard_server

    status, _, _ = _request(server, "GET", path)

    assert status == 404


def test_dashboard_exposes_ray_navigation_configuration() -> None:
    server = create_dashboard_server(
        "127.0.0.1",
        0,
        state=_FakeState(),
        ray_dashboard_url="https://ray.example.com/cluster/",
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        status, _, payload = _request(server, "GET", "/api/config")
        assert status == 200
        assert json.loads(payload) == {"ray_dashboard_url": "https://ray.example.com/cluster"}
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_dashboard_health_and_readiness_have_correlated_request_ids(dashboard_server) -> None:
    server, _ = dashboard_server

    health_status, health_headers, health_payload = _request(
        server,
        "GET",
        "/healthz",
        headers={"X-Request-ID": "probe-123"},
    )
    ready_status, ready_headers, ready_payload = _request(server, "GET", "/readyz")

    assert health_status == ready_status == 200
    assert re.fullmatch(r"[0-9a-f]{32}", health_headers["X-Request-ID"])
    assert health_headers["X-Request-ID"] != "probe-123"
    assert re.fullmatch(r"[0-9a-f]{32}", ready_headers["X-Request-ID"])
    assert health_headers["X-Request-ID"] != ready_headers["X-Request-ID"]
    assert json.loads(health_payload) == {"status": "ok"}
    assert json.loads(ready_payload) == {"status": "ready"}


def test_dashboard_readiness_fails_closed_while_liveness_stays_healthy(dashboard_server) -> None:
    server, state = dashboard_server

    def unavailable():
        raise RuntimeError("state actor unavailable")

    state.list_jobs = unavailable
    health_status, _, _ = _request(server, "GET", "/healthz")
    ready_status, _, ready_payload = _request(server, "GET", "/readyz")

    assert health_status == 200
    assert ready_status == 503
    assert json.loads(ready_payload)["request_id"]


def test_dashboard_emits_sanitized_access_and_control_events(dashboard_server, dashboard_log_records) -> None:
    server, state = dashboard_server
    body = json.dumps({"parallelism": 5})

    status, headers, _ = _request(
        server,
        "POST",
        f"/api/jobs/{quote(state.job_id, safe='')}/operators/7/rescale",
        body=body,
        headers={
            "Content-Type": "application/json",
            "Content-Length": str(len(body)),
            "X-Request-ID": "control-123",
        },
    )

    assert status == 202
    assert re.fullmatch(r"[0-9a-f]{32}", headers["X-Request-ID"])
    assert headers["X-Request-ID"] != "control-123"
    wait_until(
        lambda: any(
            getattr(record, "klein_event", None) == "dashboard.http.request" for record in dashboard_log_records
        ),
        timeout=1,
        interval=0.001,
        description="Dashboard access log",
    )
    events = {getattr(record, "klein_event", None): record for record in dashboard_log_records}
    assert "dashboard.http.request" in events
    assert "dashboard.control.rescale.requested" in events
    assert "dashboard.control.rescale.completed" in events
    access_record = events["dashboard.http.request"]
    assert state.job_id not in access_record.getMessage()
    assert access_record.klein_fields["route"] == "operator.rescale"
    assert access_record.klein_fields["request_id"] == headers["X-Request-ID"]


def test_dashboard_reuses_ray_frontend_and_injects_external_navigation(frontend_server) -> None:
    state = _FakeState()
    server = create_dashboard_server(
        "127.0.0.1",
        0,
        state=state,
        frontend_url=frontend_server,
        ray_dashboard_url="https://ray.example.com/base",
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        status, page_headers, page = _request(server, "GET", "/")
        script_status, script_headers, script = _request(server, "GET", "/static/js/bundle.js")
        bridge_status, _, bridge = _request(server, "GET", "/__klein/navigation.js")
        jobs_status, _, jobs = _request(server, "GET", "/api/klein/jobs")
        job_status, _, job = _request(server, "GET", f"/api/klein/jobs/{quote(state.job_id, safe='')}")

        assert status == script_status == bridge_status == jobs_status == job_status == 200
        assert b"Ray Dashboard" in page
        assert b'src="__klein/navigation.js"' in page
        assert script == b"window.__RAY_DASHBOARD__ = true;"
        assert b"https://ray.example.com/base" in bridge
        assert b'"jobs"' in jobs
        assert json.loads(job) == {"job": state.snapshot}
        for headers in (page_headers, script_headers):
            assert headers["X-Frame-Options"] == "DENY"
            assert headers["Referrer-Policy"] == "no-referrer"
            assert headers["Content-Security-Policy"].startswith("default-src 'self'")
            frontend_authority = urlsplit(frontend_server).netloc
            assert f"connect-src 'self' ws://{frontend_authority};" in headers["Content-Security-Policy"]
            assert " ws: " not in headers["Content-Security-Policy"]
            assert " wss: " not in headers["Content-Security-Policy"]
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


@pytest.mark.parametrize(
    ("upstream", "expected"),
    [
        ("text/html", "text/html"),
        ("application/javascript", "text/javascript; charset=utf-8"),
        ("image/png", "image/png"),
        ("text/html\r\nX-Injected: true", "application/octet-stream"),
        ("application/example", "application/octet-stream"),
    ],
)
def test_dashboard_maps_upstream_content_types_to_safe_constants(upstream, expected) -> None:
    headers = SimpleNamespace(get_content_type=lambda: upstream)

    assert _proxy_content_type(headers) == expected


def test_dashboard_bounds_success_and_error_frontend_proxy_bodies(
    frontend_server,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "ray.klein.observability.dashboard.server._MAX_PROXY_RESPONSE_BYTES",
        4,
    )
    server = create_dashboard_server(
        "127.0.0.1",
        0,
        state=_FakeState(),
        frontend_url=frontend_server,
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        success_status, _, success = _request(server, "GET", "/oversized")
        error_status, _, error = _request(server, "GET", "/oversized-error")

        assert success_status == error_status == 502
        assert "proxy limit" in json.loads(success)["error"]
        assert "proxy limit" in json.loads(error)["error"]
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_dashboard_supports_ray_frontend_cancel_endpoint(dashboard_server) -> None:
    server, state = dashboard_server

    status, _, payload = _request(
        server,
        "POST",
        f"/api/klein/jobs/{quote(state.job_id, safe='')}/cancel",
        body=b"",
        headers={"Content-Length": "0"},
    )

    assert status == 200
    assert json.loads(payload) == {"job_id": state.job_id, "cancelled": True}
    assert state.cancel_calls == [(state.job_id, 60)]


@pytest.mark.parametrize(
    "url",
    [
        "",
        "127.0.0.1:8265",
        "ftp://ray.example.com",
        "https://user@ray.example.com",
        "https://ray.example.com/#/jobs",
        "http://ray.example.com; script-src *",
        "http://ray.example.com other",
        "http://ray.example.com,https://other.example",
        "http://ray.example.com%0aX-Test:1",
    ],
)
def test_dashboard_rejects_invalid_ray_dashboard_url(url) -> None:
    with pytest.raises((TypeError, ValueError)):
        create_dashboard_server("127.0.0.1", 0, state=_FakeState(), ray_dashboard_url=url)


@pytest.mark.parametrize("url", ["", "127.0.0.1:3001", "file:///tmp/dashboard", "https://user@ray.example.com"])
def test_dashboard_rejects_invalid_frontend_url(url) -> None:
    with pytest.raises((TypeError, ValueError)):
        create_dashboard_server("127.0.0.1", 0, state=_FakeState(), frontend_url=url)


def test_dashboard_lists_jobs_and_reads_url_encoded_job_id(dashboard_server) -> None:
    server, state = dashboard_server

    list_status, _, list_payload = _request(server, "GET", "/api/jobs")
    detail_status, _, detail_payload = _request(server, "GET", f"/api/jobs/{quote(state.job_id, safe='')}")

    assert list_status == detail_status == 200
    assert json.loads(list_payload) == {"jobs": [state.snapshot]}
    assert json.loads(detail_payload) == state.snapshot


@pytest.mark.parametrize(
    "path_template",
    (
        "/api/jobs/{job_id}/operators/7/rescale",
        "/api/klein/jobs/{job_id}/operators/7/rescale",
    ),
)
def test_dashboard_forwards_operator_rescale_and_returns_operation(
    dashboard_server,
    path_template,
) -> None:
    server, state = dashboard_server
    body = json.dumps({"parallelism": 5})

    status, _, payload = _request(
        server,
        "POST",
        path_template.format(job_id=quote(state.job_id, safe="")),
        body=body,
        headers={"Content-Type": "application/json", "Content-Length": str(len(body))},
    )

    assert status == 202
    assert state.rescale_calls == [(state.job_id, 7, 5)]
    assert json.loads(payload)["status"] == "ACCEPTED"
    assert json.loads(payload)["operation_id"] == "resize-1"
    assert json.loads(payload)["parallelism"] == 5


def test_dashboard_returns_conflict_for_concurrent_rescale(dashboard_server) -> None:
    server, state = dashboard_server

    def reject(job_id, operator_id, parallelism):
        return {
            "operation_id": "resize-2",
            "active_operation_id": "resize-1",
            "job_id": job_id,
            "operator_id": operator_id,
            "parallelism": parallelism,
            "status": "REJECTED",
            "error": "another operator rescale is already in progress",
        }

    state.submit_operator_rescale = reject
    body = json.dumps({"parallelism": 5})
    status, _, payload = _request(
        server,
        "POST",
        f"/api/jobs/{quote(state.job_id, safe='')}/operators/7/rescale",
        body=body,
        headers={"Content-Type": "application/json", "Content-Length": str(len(body))},
    )

    assert status == 409
    assert json.loads(payload)["status"] == "REJECTED"
    assert json.loads(payload)["active_operation_id"] == "resize-1"


@pytest.mark.parametrize("parallelism", [0, -1, 1.5, True, "2"])
def test_dashboard_rejects_invalid_parallelism_without_control_call(dashboard_server, parallelism) -> None:
    server, state = dashboard_server
    body = json.dumps({"parallelism": parallelism})

    status, _, payload = _request(
        server,
        "POST",
        f"/api/jobs/{quote(state.job_id, safe='')}/operators/7/rescale",
        body=body,
        headers={"Content-Type": "application/json", "Content-Length": str(len(body))},
    )

    assert status == 400
    assert "positive integer" in json.loads(payload)["error"]
    assert state.rescale_calls == []


def test_dashboard_rejects_cross_origin_control_request(dashboard_server) -> None:
    server, state = dashboard_server
    body = json.dumps({"parallelism": 3})

    status, _, _ = _request(
        server,
        "POST",
        f"/api/jobs/{quote(state.job_id, safe='')}/operators/7/rescale",
        body=body,
        headers={
            "Content-Type": "application/json",
            "Content-Length": str(len(body)),
            "Origin": "https://attacker.example",
        },
    )

    assert status == 403
    assert state.rescale_calls == []


def test_dashboard_rejects_cross_site_fetch_metadata_without_origin(dashboard_server) -> None:
    server, state = dashboard_server

    status, _, _ = _request(
        server,
        "POST",
        f"/api/klein/jobs/{quote(state.job_id, safe='')}/cancel",
        body=b"",
        headers={"Content-Length": "0", "Sec-Fetch-Site": "cross-site"},
    )

    assert status == 403
    assert state.cancel_calls == []


def test_dashboard_rejects_dns_rebinding_host_before_control_call(dashboard_server) -> None:
    server, state = dashboard_server
    body = json.dumps({"parallelism": 3})
    attacker_authority = f"attacker.example:{server.server_port}"

    status, _, payload = _request(
        server,
        "POST",
        f"/api/jobs/{quote(state.job_id, safe='')}/operators/7/rescale",
        body=body,
        headers={
            "Content-Type": "application/json",
            "Content-Length": str(len(body)),
            "Host": attacker_authority,
            # A DNS-rebinding request is same-origin from the browser's point of
            # view, so Origin-vs-Host comparison alone cannot reject it.
            "Origin": f"http://{attacker_authority}",
        },
    )

    assert status == 403
    assert json.loads(payload)["error"] == "Untrusted Host header"
    assert state.rescale_calls == []


def test_dashboard_maps_backend_type_error_to_service_unavailable(dashboard_server, dashboard_log_records) -> None:
    server, state = dashboard_server
    state.rescale_error = TypeError("JobManager returned an invalid result")
    body = json.dumps({"parallelism": 3})

    status, _, payload = _request(
        server,
        "POST",
        f"/api/jobs/{quote(state.job_id, safe='')}/operators/7/rescale",
        body=body,
        headers={"Content-Type": "application/json", "Content-Length": str(len(body))},
    )

    assert status == 503
    assert "TypeError" in json.loads(payload)["error"]
    completed = next(
        record
        for record in dashboard_log_records
        if getattr(record, "klein_event", None) == "dashboard.control.rescale.completed"
    )
    assert completed.klein_fields["result"] == "ERROR"


def test_dashboard_maps_ray_wrapped_asyncio_timeout_to_gateway_timeout(dashboard_server) -> None:
    server, state = dashboard_server
    state.rescale_error = RayTaskError(
        "rescale_operator",
        "remote traceback",
        asyncio.TimeoutError("operator rescale timed out"),
    ).as_instanceof_cause()
    body = json.dumps({"parallelism": 3})

    status, _, payload = _request(
        server,
        "POST",
        f"/api/jobs/{quote(state.job_id, safe='')}/operators/7/rescale",
        body=body,
        headers={"Content-Type": "application/json", "Content-Length": str(len(body))},
    )

    assert status == 504
    assert "operator rescale timed out" in json.loads(payload)["error"]


def test_dashboard_maps_ray_get_timeout_to_gateway_timeout(dashboard_server) -> None:
    server, state = dashboard_server
    get_timeout_type = type("GetTimeoutError", (Exception,), {"__module__": "ray.exceptions"})

    def timed_out():
        raise get_timeout_type("state actor response timed out")

    state.list_jobs = timed_out
    status, _, payload = _request(server, "GET", "/api/jobs")

    assert status == 504
    assert "state actor response timed out" in json.loads(payload)["error"]


def test_dashboard_times_out_slow_request_bodies(monkeypatch) -> None:
    monkeypatch.setattr("ray.klein.observability.dashboard.server._REQUEST_SOCKET_TIMEOUT_SECONDS", 0.05)
    server = create_dashboard_server("127.0.0.1", 0, state=_FakeState())
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    client = socket.create_connection(("127.0.0.1", server.server_port), timeout=2)
    try:
        client.sendall(
            b"POST /api/jobs/job/operators/7/rescale HTTP/1.1\r\n"
            b"Host: 127.0.0.1\r\n"
            b"Content-Type: application/json\r\n"
            b"Content-Length: 17\r\n\r\n{"
        )
        chunks = []
        while chunk := client.recv(4096):
            chunks.append(chunk)
        response = b"".join(chunks)
        assert b"408 Request Timeout" in response
        assert b"Request body timed out" in response
    finally:
        client.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_dashboard_rejects_requests_over_the_concurrency_limit(monkeypatch) -> None:
    monkeypatch.setattr("ray.klein.observability.dashboard.server._MAX_CONCURRENT_REQUESTS", 1)
    state = _FakeState()
    started = threading.Event()
    release = threading.Event()

    def blocked_list_jobs():
        started.set()
        assert release.wait(timeout=2)
        return []

    state.list_jobs = blocked_list_jobs
    server = create_dashboard_server("127.0.0.1", 0, state=state)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    first_result = []
    first_thread = threading.Thread(target=lambda: first_result.append(_request(server, "GET", "/readyz")))
    server_thread.start()
    first_thread.start()
    try:
        assert started.wait(timeout=1)
        status, _, payload = _request(server, "GET", "/healthz")
        assert status == 503
        assert json.loads(payload)["error"] == "Dashboard request concurrency limit reached"
    finally:
        release.set()
        first_thread.join(timeout=2)
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=2)
    assert first_result[0][0] == 200


def test_cli_dashboard_starts_bound_server(monkeypatch) -> None:
    events = []
    server = SimpleNamespace(
        server_port=8765,
        ray_dashboard_url="https://ray.example.com",
        frontend_url="http://127.0.0.1:3001",
        serve_forever=lambda: events.append("served"),
        server_close=lambda: events.append("closed"),
    )
    monkeypatch.setattr(cli, "_ensure_ray_init", lambda: events.append("connected"))
    monkeypatch.setattr(
        "ray.klein.observability.dashboard.server.create_dashboard_server",
        lambda host, port, *, ray_dashboard_url, frontend_url, allow_unauthenticated: (
            events.append((host, port, ray_dashboard_url, frontend_url, allow_unauthenticated)) or server
        ),
    )

    result = CliRunner().invoke(
        cli.klein_cli_group,
        [
            "dashboard",
            "--host",
            "0.0.0.0",
            "--port",
            "8765",
            "--ray-dashboard-url",
            "https://ray.example.com",
            "--frontend-url",
            "http://127.0.0.1:3001",
            "--allow-unauthenticated",
        ],
    )

    assert result.exit_code == 0
    assert result.output.splitlines() == [
        "WARNING: the Dashboard control endpoint is unauthenticated; protect it with a trusted proxy.",
        "Klein Dashboard is running at http://127.0.0.1:8765/",
        "Klein UI is reused from http://127.0.0.1:3001",
        "Ray navigation opens https://ray.example.com",
        "Press Ctrl+C to stop it.",
    ]
    assert events == [
        "connected",
        ("0.0.0.0", 8765, "https://ray.example.com", "http://127.0.0.1:3001", True),
        "served",
        "closed",
    ]


def test_cli_dashboard_uses_bundled_frontend_by_default(monkeypatch) -> None:
    events = []
    server = SimpleNamespace(
        server_port=8266,
        ray_dashboard_url="http://127.0.0.1:8265",
        frontend_url=None,
        serve_forever=lambda: events.append("served"),
        server_close=lambda: events.append("closed"),
    )
    monkeypatch.setattr(cli, "_ensure_ray_init", lambda: events.append("connected"))
    monkeypatch.setattr(
        "ray.klein.observability.dashboard.server.create_dashboard_server",
        lambda host, port, *, ray_dashboard_url, frontend_url, allow_unauthenticated: (
            events.append((host, port, ray_dashboard_url, frontend_url, allow_unauthenticated)) or server
        ),
    )

    result = CliRunner().invoke(cli.klein_cli_group, ["dashboard"])

    assert result.exit_code == 0
    assert "http://127.0.0.1:8266/" in result.output
    assert "Klein UI is reused" not in result.output
    assert events == [
        "connected",
        ("127.0.0.1", 8266, "http://127.0.0.1:8265", None, False),
        "served",
        "closed",
    ]


def test_cli_dashboard_refuses_unauthenticated_non_loopback_listener(monkeypatch) -> None:
    monkeypatch.setattr(
        cli,
        "_ensure_ray_init",
        lambda: pytest.fail("an unsafe listener should be rejected before connecting to Ray"),
    )

    result = CliRunner().invoke(
        cli.klein_cli_group,
        ["dashboard", "--host", "0.0.0.0", "--frontend-url", "http://127.0.0.1:3001"],
    )

    assert result.exit_code == 1
    assert "Refusing to expose" in result.output
    assert "--allow-unauthenticated" in result.output


def test_programmatic_dashboard_binding_requires_the_same_non_loopback_opt_in() -> None:
    with pytest.raises(ValueError, match="non-loopback"):
        create_dashboard_server("0.0.0.0", 0, state=_FakeState())

    server = create_dashboard_server(
        "0.0.0.0",
        0,
        state=_FakeState(),
        allow_unauthenticated=True,
    )
    server.server_close()


@pytest.mark.skipif(not socket.has_ipv6, reason="IPv6 is unavailable on this host")
def test_dashboard_supports_an_ipv6_loopback_listener() -> None:
    try:
        server = create_dashboard_server("::1", 0, state=_FakeState())
    except OSError as error:
        pytest.skip(f"IPv6 loopback is unavailable: {error}")
    try:
        assert server.address_family == socket.AF_INET6
    finally:
        server.server_close()
