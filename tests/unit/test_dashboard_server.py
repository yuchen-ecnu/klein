# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import asyncio
import importlib
import json
import re
import socket
import threading
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from urllib.parse import quote

import pytest
from click.testing import CliRunner
from ray.exceptions import RayTaskError

from ray.klein.observability.dashboard.server import create_dashboard_server

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
        self.rescale_calls.append((job_id, operator_id, parallelism))
        if self.rescale_error is not None:
            raise self.rescale_error
        return {
            "job_id": job_id,
            "operator_id": operator_id,
            "previous_parallelism": 2,
            "parallelism": parallelism,
            "status": "COMPLETED",
        }

    def cancel_job(self, job_id, timeout=60):
        self.cancel_calls.append((job_id, timeout))
        return job_id == self.job_id


class _FakeFrontendHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/":
            payload = b'<html><head><title>Ray Dashboard</title></head><body><main id="root"></main></body></html>'
            content_type = "text/html; charset=utf-8"
        elif self.path == "/static/js/bundle.js":
            payload = b"window.__RAY_DASHBOARD__ = true;"
            content_type = "text/javascript; charset=utf-8"
        else:
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Type", content_type)
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
    asset_path = re.search(rb'src="(/assets/[^"]+\.js)"', page)

    assert status == 200
    assert headers["Content-Type"] == "text/html; charset=utf-8"
    assert b'<div id="root"></div>' in page
    assert b"/__klein/navigation.js" in page
    assert asset_path is not None
    asset_status, asset_headers, asset = _request(server, "GET", asset_path.group(1).decode())
    assert asset_status == 200
    assert asset_headers["Content-Type"] in {
        "application/javascript; charset=utf-8",
        "text/javascript; charset=utf-8",
    }
    assert len(asset) > 1_000


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
        status, _, page = _request(server, "GET", "/")
        script_status, _, script = _request(server, "GET", "/static/js/bundle.js")
        bridge_status, _, bridge = _request(server, "GET", "/__klein/navigation.js")
        jobs_status, _, jobs = _request(server, "GET", "/api/klein/jobs")
        job_status, _, job = _request(server, "GET", f"/api/klein/jobs/{quote(state.job_id, safe='')}")

        assert status == script_status == bridge_status == jobs_status == job_status == 200
        assert b"Ray Dashboard" in page
        assert b"/__klein/navigation.js" in page
        assert script == b"window.__RAY_DASHBOARD__ = true;"
        assert b"https://ray.example.com/base" in bridge
        assert b'"jobs"' in jobs
        assert json.loads(job) == {"job": state.snapshot}
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
    ["", "127.0.0.1:8265", "ftp://ray.example.com", "https://user@ray.example.com", "https://ray.example.com/#/jobs"],
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

    assert status == 200
    assert state.rescale_calls == [(state.job_id, 7, 5)]
    assert json.loads(payload)["status"] == "COMPLETED"
    assert json.loads(payload)["parallelism"] == 5


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


def test_dashboard_maps_backend_type_error_to_service_unavailable(dashboard_server) -> None:
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
        lambda host, port, *, ray_dashboard_url, frontend_url: (
            events.append((host, port, ray_dashboard_url, frontend_url)) or server
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
        ("0.0.0.0", 8765, "https://ray.example.com", "http://127.0.0.1:3001"),
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
        lambda host, port, *, ray_dashboard_url, frontend_url: (
            events.append((host, port, ray_dashboard_url, frontend_url)) or server
        ),
    )

    result = CliRunner().invoke(cli.klein_cli_group, ["dashboard"])

    assert result.exit_code == 0
    assert "http://127.0.0.1:8266/" in result.output
    assert "Klein UI is reused" not in result.output
    assert events == [
        "connected",
        ("127.0.0.1", 8266, "http://127.0.0.1:8265", None),
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
