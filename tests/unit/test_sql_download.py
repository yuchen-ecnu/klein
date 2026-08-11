# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from ray.klein._internal.sql import download_runtime
from ray.klein._internal.sql.download_runtime import (
    DOWNLOAD_BATCH_SIZE,
    DownloadRejectedError,
    SQLDownloadPolicy,
    _ApplyDownloadsBatch,
    download_uri,
)
from ray.klein.config.configuration import Configuration


class _DownloadHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path == "/redirect":
            self.send_response(302)
            self.send_header("Location", "/body")
            self.end_headers()
            return
        payload = b"body" if self.path == "/body" else b"oversized"
        self.send_response(200)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, _format: str, *_args) -> None:
        return


@pytest.fixture
def download_server():
    server = ThreadingHTTPServer(("127.0.0.1", 0), _DownloadHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_default_policy_rejects_private_resolution_even_for_allowlisted_host(monkeypatch) -> None:
    monkeypatch.setattr(
        socket,
        "getaddrinfo",
        lambda *_args, **_kwargs: [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 80))],
    )
    policy = SQLDownloadPolicy(allowed_hosts=("assets.example.com",))

    with pytest.raises(DownloadRejectedError, match="globally routable"):
        download_runtime._validated_addresses("assets.example.com", 80, policy)


def test_explicit_ip_range_can_authorize_an_internal_download_target(monkeypatch) -> None:
    monkeypatch.setattr(
        socket,
        "getaddrinfo",
        lambda *_args, **_kwargs: [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.1.2.3", 80))],
    )
    policy = SQLDownloadPolicy(
        allowed_hosts=("assets.internal",),
        allowed_ip_ranges=("10.1.0.0/16",),
    )

    assert download_runtime._validated_addresses("assets.internal", 80, policy) == ("10.1.2.3",)


def test_denied_ip_range_takes_precedence_over_an_allow_range(monkeypatch) -> None:
    monkeypatch.setattr(
        socket,
        "getaddrinfo",
        lambda *_args, **_kwargs: [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.1.2.3", 80))],
    )
    policy = SQLDownloadPolicy(
        allowed_ip_ranges=("10.0.0.0/8",),
        denied_ip_ranges=("10.1.0.0/16",),
    )

    with pytest.raises(DownloadRejectedError, match="denied"):
        download_runtime._validated_addresses("assets.internal", 80, policy)


@pytest.mark.parametrize(
    "uri",
    (
        "ftp://files.example.com/body",
        "http://" + "user:secret@files.example.com/body",
        "http://files.example.com/body#fragment",
    ),
)
def test_download_rejects_disallowed_or_credentialed_uris(uri: str) -> None:
    with pytest.raises(DownloadRejectedError):
        download_uri(uri, None, SQLDownloadPolicy())


def test_http_redirects_are_revalidated_and_reads_are_bounded(download_server) -> None:
    policy = SQLDownloadPolicy(
        allowed_hosts=("127.0.0.1",),
        allowed_ip_ranges=("127.0.0.1/32",),
        max_bytes=4,
    )

    assert download_uri(f"{download_server}/redirect", None, policy) == b"body"
    with pytest.raises(DownloadRejectedError, match="max-bytes"):
        download_uri(f"{download_server}/large", None, policy)


def test_redirect_cannot_escape_the_configured_host_allowlist(monkeypatch) -> None:
    request = Mock(return_value=(302, "http://169.254.169.254/latest/meta-data", b""))
    monkeypatch.setattr(download_runtime, "_request_http_once", request)
    policy = SQLDownloadPolicy(allowed_hosts=("assets.example.com",))

    with pytest.raises(DownloadRejectedError, match="allowlisted"):
        download_runtime._download_http("https://assets.example.com/body", policy)
    request.assert_called_once()


def test_http_request_refreshes_the_deadline_for_each_socket_phase(monkeypatch) -> None:
    socket_timeouts = []
    connections = []

    class FakeSocket:
        def settimeout(self, timeout):
            socket_timeouts.append(timeout)

    class FakeResponse:
        status = 200

        @staticmethod
        def getheader(_name):
            return None

        @staticmethod
        def read(_size):
            return b""

    class FakeConnection:
        def __init__(self, address, *, port, timeout):
            self.address = address
            self.port = port
            self.timeout = timeout
            self.sock = FakeSocket()
            connections.append(self)

        def connect(self):
            return None

        def request(self, *_args, **_kwargs):
            return None

        def getresponse(self):
            return FakeResponse()

        def close(self):
            return None

    remaining = Mock(side_effect=[4.0, 3.0, 2.0, 1.0])
    monkeypatch.setattr(download_runtime, "_validated_addresses", Mock(return_value=("203.0.113.1",)))
    monkeypatch.setattr(download_runtime, "_remaining_timeout", remaining)
    monkeypatch.setattr(download_runtime.http.client, "HTTPConnection", FakeConnection)
    parsed, _scheme = download_runtime._validate_uri(
        "http://assets.example.com/body",
        SQLDownloadPolicy(allowed_hosts=("assets.example.com",)),
    )

    assert download_runtime._request_http_once(parsed, SQLDownloadPolicy(), 10.0) == (200, None, b"")
    assert connections[0].timeout == 4.0
    assert socket_timeouts == [3.0, 2.0, 1.0]
    assert remaining.call_count == 4


def test_batch_downloads_share_one_per_row_byte_budget(monkeypatch) -> None:
    budgets: list[int] = []

    def fake_download(uri, _filesystem, _column, policy):
        budgets.append(policy.max_bytes)
        return str(uri).encode()[: policy.max_bytes]

    monkeypatch.setattr(download_runtime, "download_uri_soft", fake_download)
    worker = _ApplyDownloadsBatch(
        (("first", "uri1", None), ("second", "uri2", None)),
        SQLDownloadPolicy(max_bytes=6),
    )

    result = worker({"uri1": ["abcd"], "uri2": ["wxyz"]})

    assert DOWNLOAD_BATCH_SIZE == 1
    assert result["first"].tolist() == [b"abcd"]
    assert result["second"].tolist() == [b"wx"]
    assert budgets == [6, 2]


def test_policy_is_captured_from_typed_configuration() -> None:
    config = Configuration(
        {
            "sql.download.allowed-hosts": ["*.example.com"],
            "sql.download.allowed-ip-ranges": ["203.0.113.0/24"],
            "sql.download.max-bytes": 1024,
            "sql.download.timeout": "2s",
            "sql.download.max-redirects": 1,
        },
        include_environment=False,
    )

    policy = SQLDownloadPolicy.from_configuration(config)

    assert policy.allowed_hosts == ("*.example.com",)
    assert policy.allowed_ip_ranges == ("203.0.113.0/24",)
    assert policy.max_bytes == 1024
    assert policy.timeout_seconds == 2
    assert policy.max_redirects == 1


def test_streaming_runtime_context_policy_defaults_are_serializable() -> None:
    policy = SQLDownloadPolicy.from_configuration(
        getattr(SimpleNamespace(), "config", None),
    )

    assert policy.max_bytes == 64 * 1024 * 1024
