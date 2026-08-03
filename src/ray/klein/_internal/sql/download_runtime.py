# SPDX-License-Identifier: Apache-2.0
"""Bounded, SSRF-aware runtime for SQL ``DOWNLOAD``."""

from __future__ import annotations

import http.client
import ipaddress
import math
import socket
import ssl
import time
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import timedelta
from typing import TYPE_CHECKING, Any
from urllib.parse import SplitResult, urljoin, urlsplit

from ray.klein._internal.logging import get_logger
from ray.klein.config.configuration import Configuration
from ray.klein.config.sql_download_options import SQLDownloadOptions

if TYPE_CHECKING:
    from ray.data import Dataset


logger = get_logger(__name__)

DOWNLOAD_BATCH_SIZE = 1
_REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})
_READ_CHUNK_BYTES = 64 * 1024


class DownloadRejectedError(OSError):
    """A URI or response rejected by the configured download boundary."""


@dataclass(frozen=True, slots=True)
class SQLDownloadPolicy:
    """Serializable download policy captured with a SQL query graph."""

    allowed_schemes: tuple[str, ...] = ("file", "local", "memory", "s3", "gs", "http", "https")
    allowed_hosts: tuple[str, ...] = ()
    denied_hosts: tuple[str, ...] = ()
    allowed_ip_ranges: tuple[str, ...] = ()
    denied_ip_ranges: tuple[str, ...] = ()
    allow_private_network: bool = False
    max_bytes: int = 64 * 1024 * 1024
    timeout_seconds: float = 30.0
    max_redirects: int = 5

    def __post_init__(self) -> None:
        object.__setattr__(self, "allowed_schemes", _normalize_schemes(self.allowed_schemes))
        object.__setattr__(self, "allowed_hosts", _normalize_host_patterns(self.allowed_hosts, "allowed_hosts"))
        object.__setattr__(self, "denied_hosts", _normalize_host_patterns(self.denied_hosts, "denied_hosts"))
        object.__setattr__(
            self,
            "allowed_ip_ranges",
            _normalize_ip_ranges(self.allowed_ip_ranges, "allowed_ip_ranges"),
        )
        object.__setattr__(
            self,
            "denied_ip_ranges",
            _normalize_ip_ranges(self.denied_ip_ranges, "denied_ip_ranges"),
        )
        if type(self.allow_private_network) is not bool:
            raise TypeError("allow_private_network must be a boolean")
        if type(self.max_bytes) is not int or self.max_bytes <= 0:
            raise ValueError("max_bytes must be a positive integer")
        if (
            isinstance(self.timeout_seconds, bool)
            or not isinstance(self.timeout_seconds, int | float)
            or not math.isfinite(self.timeout_seconds)
            or self.timeout_seconds <= 0
        ):
            raise ValueError("timeout_seconds must be a finite positive number")
        object.__setattr__(self, "timeout_seconds", float(self.timeout_seconds))
        if type(self.max_redirects) is not int or self.max_redirects < 0:
            raise ValueError("max_redirects must be a non-negative integer")

    @classmethod
    def from_configuration(cls, configuration: Configuration | None) -> SQLDownloadPolicy:
        config = configuration or Configuration(include_environment=False)
        timeout = _config_value(config, SQLDownloadOptions.TIMEOUT)
        if not isinstance(timeout, timedelta):
            raise TypeError("sql.download.timeout must resolve to a duration")
        return cls(
            allowed_schemes=tuple(_config_value(config, SQLDownloadOptions.ALLOWED_SCHEMES)),
            allowed_hosts=tuple(_config_value(config, SQLDownloadOptions.ALLOWED_HOSTS)),
            denied_hosts=tuple(_config_value(config, SQLDownloadOptions.DENIED_HOSTS)),
            allowed_ip_ranges=tuple(_config_value(config, SQLDownloadOptions.ALLOWED_IP_RANGES)),
            denied_ip_ranges=tuple(_config_value(config, SQLDownloadOptions.DENIED_IP_RANGES)),
            allow_private_network=_config_value(config, SQLDownloadOptions.ALLOW_PRIVATE_NETWORK),
            max_bytes=_config_value(config, SQLDownloadOptions.MAX_BYTES),
            timeout_seconds=timeout.total_seconds(),
            max_redirects=_config_value(config, SQLDownloadOptions.MAX_REDIRECTS),
        )


class _PinnedHTTPSConnection(http.client.HTTPSConnection):
    """HTTPS connection pinned to a validated IP while retaining host TLS checks."""

    _context: ssl.SSLContext

    def __init__(self, address: str, port: int, server_hostname: str, timeout: float) -> None:
        super().__init__(address, port=port, timeout=timeout, context=ssl.create_default_context())
        self._validated_server_hostname = server_hostname

    def connect(self) -> None:
        http.client.HTTPConnection.connect(self)
        if self.sock is None:
            raise OSError("DOWNLOAD failed to establish a TLS socket")
        self.sock = self._context.wrap_socket(
            self.sock,
            server_hostname=self._validated_server_hostname,
        )


def apply_batch_downloads(
    dataset: Dataset,
    downloads: Sequence[tuple[str, str, Any]],
    *,
    policy: SQLDownloadPolicy,
    num_cpus: float | None,
    **ray_options: Any,
) -> Dataset:
    """Append one or more bounded download columns in one Ray batch operator."""

    if not downloads:
        return dataset
    options = dict(ray_options)
    if num_cpus is not None:
        options["num_cpus"] = num_cpus
    return dataset.map_batches(
        _ApplyDownloadsBatch,
        fn_constructor_args=(tuple(downloads), policy),
        batch_size=DOWNLOAD_BATCH_SIZE,
        batch_format="numpy",
        udf_modifying_row_count=False,
        **options,
    )


def apply_download_expression(
    dataset: Dataset,
    column_name: str,
    expression: Any,
    policy: SQLDownloadPolicy,
    **ray_options: Any,
) -> Dataset:
    """Secure batch lowering for public ``Dataset.with_column(DownloadExpr)``."""

    from ray.data.expressions import DownloadExpr

    if not isinstance(column_name, str) or not isinstance(expression, DownloadExpr):
        raise TypeError("apply_download_expression requires a column name and DownloadExpr")
    num_cpus = ray_options.pop("num_cpus", None)
    return apply_batch_downloads(
        dataset,
        ((column_name, expression.uri_column_name, expression.filesystem),),
        policy=policy,
        num_cpus=num_cpus,
        **ray_options,
    )


class _ApplyDownloadsBatch:
    def __init__(self, downloads: Sequence[tuple[str, str, Any]], policy: SQLDownloadPolicy) -> None:
        self._downloads = tuple(downloads)
        self._policy = policy

    def __call__(self, batch: Mapping[str, Any]) -> dict[str, Any]:
        from ray.klein._internal.values import create_ragged_ndarray

        output = dict(batch)
        row_count = len(next(iter(batch.values()))) if batch else 0
        remaining_bytes = [self._policy.max_bytes] * row_count
        for output_name, input_name, filesystem in self._downloads:
            try:
                uris = batch[input_name]
            except KeyError as error:
                raise KeyError(f"DOWNLOAD references missing URI column {input_name!r}") from error
            values: list[bytes | None] = [None] * row_count
            for index in range(row_count):
                uri = _python_scalar(uris[index])
                if uri is None or remaining_bytes[index] <= 0:
                    continue
                request_policy = replace(
                    self._policy,
                    max_bytes=remaining_bytes[index],
                )
                result = download_uri_soft(uri, filesystem, input_name, request_policy)
                values[index] = result
                if result is not None:
                    remaining_bytes[index] -= len(result)
            output[output_name] = create_ragged_ndarray(values)
        return output


def download_uri_soft(
    uri: Any,
    filesystem: Any,
    column_name: str,
    policy: SQLDownloadPolicy,
) -> bytes | None:
    """Read one URI with Ray's per-row soft-failure contract."""

    try:
        return download_uri(str(uri), filesystem, policy)
    except DownloadRejectedError as error:
        logger.warning("DOWNLOAD rejected a URI from column %r: %s", column_name, error)
    except OSError:
        logger.debug("DOWNLOAD failed for column %r", column_name, exc_info=True)
    except Exception as error:
        logger.warning(
            "Unexpected DOWNLOAD failure for column %r: %s",
            column_name,
            type(error).__name__,
            exc_info=True,
        )
    return None


def download_uri(uri: str, filesystem: Any, policy: SQLDownloadPolicy) -> bytes | None:
    """Validate and read one URI without retaining more than ``max_bytes``."""

    parsed, scheme = _validate_uri(uri, policy)
    if scheme in {"http", "https"}:
        if filesystem is not None:
            raise DownloadRejectedError("an explicit filesystem cannot be combined with HTTP(S)")
        return _download_http(uri, policy)

    from ray.klein._compat.ray_data_expression import read_uri

    return read_uri(uri, filesystem, max_bytes=policy.max_bytes)


def _download_http(uri: str, policy: SQLDownloadPolicy) -> bytes:
    deadline = time.monotonic() + policy.timeout_seconds
    current = uri
    previous_scheme: str | None = None
    for redirect_count in range(policy.max_redirects + 1):
        parsed, scheme = _validate_uri(current, policy)
        if scheme not in {"http", "https"}:
            raise DownloadRejectedError("HTTP redirects cannot leave HTTP(S)")
        if previous_scheme == "https" and scheme == "http":
            raise DownloadRejectedError("HTTPS redirects cannot downgrade to HTTP")
        status, location, payload = _request_http_once(parsed, policy, deadline)
        if status not in _REDIRECT_STATUSES:
            if not 200 <= status < 300:
                raise OSError(f"DOWNLOAD HTTP response status {status}")
            return payload
        if redirect_count >= policy.max_redirects:
            raise DownloadRejectedError("DOWNLOAD exceeded its redirect limit")
        if not location:
            raise DownloadRejectedError("DOWNLOAD redirect omitted Location")
        previous_scheme = scheme
        current = urljoin(current, location)
    raise AssertionError("unreachable redirect state")


def _request_http_once(
    parsed: SplitResult,
    policy: SQLDownloadPolicy,
    deadline: float,
) -> tuple[int, str | None, bytes]:
    host = parsed.hostname
    if host is None:
        raise DownloadRejectedError("HTTP(S) URI requires a host")
    port = parsed.port or (443 if parsed.scheme.casefold() == "https" else 80)
    addresses = _validated_addresses(host, port, policy)
    target = parsed.path or "/"
    if parsed.query:
        target = f"{target}?{parsed.query}"
    host_header = _host_header(host, port, parsed.scheme.casefold())
    last_error: OSError | None = None

    for address in addresses:
        connection: http.client.HTTPConnection | None = None
        try:
            timeout = _remaining_timeout(deadline)
            connection = (
                _PinnedHTTPSConnection(address, port, host, timeout)
                if parsed.scheme.casefold() == "https"
                else http.client.HTTPConnection(address, port=port, timeout=timeout)
            )
            connection.connect()
            _refresh_socket_timeout(connection, deadline)
            connection.request(
                "GET",
                target,
                headers={
                    "Accept": "*/*",
                    "Accept-Encoding": "identity",
                    "Host": host_header,
                    "User-Agent": "ray-klein-download/1",
                },
            )
            _refresh_socket_timeout(connection, deadline)
            response = connection.getresponse()
            if response.status in _REDIRECT_STATUSES:
                return response.status, response.getheader("Location"), b""
            length = _content_length(response.getheader("Content-Length"))
            if length is not None and length > policy.max_bytes:
                raise DownloadRejectedError("DOWNLOAD response exceeds sql.download.max-bytes")
            return response.status, None, _read_bounded(response, connection, policy.max_bytes, deadline)
        except DownloadRejectedError:
            raise
        except OSError as error:
            last_error = error
        finally:
            if connection is not None:
                connection.close()
    if last_error is not None:
        raise last_error
    raise OSError("DOWNLOAD could not connect to any validated address")


def _refresh_socket_timeout(
    connection: http.client.HTTPConnection,
    deadline: float,
) -> None:
    if connection.sock is None:
        raise OSError("DOWNLOAD connection closed before the response completed")
    connection.sock.settimeout(_remaining_timeout(deadline))


def _read_bounded(
    response: http.client.HTTPResponse,
    connection: http.client.HTTPConnection,
    max_bytes: int,
    deadline: float,
) -> bytes:
    chunks: list[bytes] = []
    retained = 0
    while True:
        remaining = _remaining_timeout(deadline)
        if connection.sock is not None:
            connection.sock.settimeout(remaining)
        chunk = response.read(min(_READ_CHUNK_BYTES, max_bytes - retained + 1))
        if not chunk:
            return b"".join(chunks)
        retained += len(chunk)
        if retained > max_bytes:
            raise DownloadRejectedError("DOWNLOAD response exceeds sql.download.max-bytes")
        chunks.append(chunk)


def _remaining_timeout(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("DOWNLOAD exceeded sql.download.timeout")
    return remaining


def _content_length(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        length = int(value)
    except ValueError as error:
        raise DownloadRejectedError("DOWNLOAD response has an invalid Content-Length") from error
    if length < 0:
        raise DownloadRejectedError("DOWNLOAD response has an invalid Content-Length")
    return length


def _validate_uri(uri: str, policy: SQLDownloadPolicy) -> tuple[SplitResult, str]:
    if not isinstance(uri, str):
        raise TypeError("DOWNLOAD URI must be a string")
    if not uri or any(ord(character) < 0x20 for character in uri):
        raise DownloadRejectedError("DOWNLOAD URI is empty or contains control characters")
    try:
        parsed = urlsplit(uri)
        scheme = parsed.scheme.casefold() or "local"
        _ = parsed.port
    except ValueError as error:
        raise DownloadRejectedError("DOWNLOAD URI is malformed") from error
    if scheme not in policy.allowed_schemes:
        raise DownloadRejectedError(f"DOWNLOAD scheme {scheme!r} is not allowed")
    if parsed.username is not None or parsed.password is not None:
        raise DownloadRejectedError("DOWNLOAD URI credentials are not allowed")
    if parsed.fragment:
        raise DownloadRejectedError("DOWNLOAD URI fragments are not allowed")
    if scheme in {"http", "https"}:
        _validate_http_host(parsed, policy)
    return parsed, scheme


def _validate_http_host(parsed: SplitResult, policy: SQLDownloadPolicy) -> None:
    host = parsed.hostname
    if host is None:
        raise DownloadRejectedError("HTTP(S) URI requires a host")
    normalized_host = _normalize_hostname(host)
    if _matches_host(normalized_host, policy.denied_hosts):
        raise DownloadRejectedError("DOWNLOAD host is denied")
    if policy.allowed_hosts and not _matches_host(normalized_host, policy.allowed_hosts):
        raise DownloadRejectedError("DOWNLOAD host is not allowlisted")


def _validated_addresses(host: str, port: int, policy: SQLDownloadPolicy) -> tuple[str, ...]:
    normalized_host = _normalize_hostname(host)
    try:
        results = socket.getaddrinfo(normalized_host, port, type=socket.SOCK_STREAM)
    except OSError as error:
        raise OSError("DOWNLOAD host resolution failed") from error
    addresses = tuple(dict.fromkeys(str(result[4][0]) for result in results))
    if not addresses:
        raise OSError("DOWNLOAD host resolution returned no addresses")

    allowed_networks = tuple(ipaddress.ip_network(value) for value in policy.allowed_ip_ranges)
    denied_networks = tuple(ipaddress.ip_network(value) for value in policy.denied_ip_ranges)
    for address in addresses:
        ip = ipaddress.ip_address(address)
        if any(ip in network for network in denied_networks):
            raise DownloadRejectedError("DOWNLOAD destination address is denied")
        if allowed_networks:
            if not any(ip in network for network in allowed_networks):
                raise DownloadRejectedError("DOWNLOAD destination address is not allowlisted")
        elif not policy.allow_private_network and not ip.is_global:
            raise DownloadRejectedError("DOWNLOAD destination address is not globally routable")
    return addresses


def _host_header(host: str, port: int, scheme: str) -> str:
    try:
        is_ipv6 = ipaddress.ip_address(host).version == 6
    except ValueError:
        is_ipv6 = False
    authority = f"[{host}]" if is_ipv6 else host
    default_port = 443 if scheme == "https" else 80
    return authority if port == default_port else f"{authority}:{port}"


def _normalize_schemes(values: Iterable[Any]) -> tuple[str, ...]:
    if isinstance(values, str):
        raise TypeError("allowed_schemes must be a collection")
    normalized: list[str] = []
    for value in values:
        scheme = str(value).strip().casefold()
        if (
            not scheme
            or not scheme[0].isalpha()
            or not all(character.isalnum() or character in "+-." for character in scheme)
        ):
            raise ValueError(f"invalid DOWNLOAD scheme {value!r}")
        if scheme not in normalized:
            normalized.append(scheme)
    if not normalized:
        raise ValueError("allowed_schemes cannot be empty")
    return tuple(normalized)


def _normalize_host_patterns(values: Iterable[Any], name: str) -> tuple[str, ...]:
    if isinstance(values, str):
        raise TypeError(f"{name} must be a collection")
    normalized: list[str] = []
    for value in values:
        pattern = str(value).strip().casefold().rstrip(".")
        wildcard = pattern.startswith("*.")
        hostname = pattern[2:] if wildcard else pattern
        hostname = _normalize_hostname(hostname)
        if "*" in hostname:
            raise ValueError(f"{name} contains an invalid wildcard")
        rendered = f"*.{hostname}" if wildcard else hostname
        if rendered not in normalized:
            normalized.append(rendered)
    return tuple(normalized)


def _normalize_ip_ranges(values: Iterable[Any], name: str) -> tuple[str, ...]:
    if isinstance(values, str):
        raise TypeError(f"{name} must be a collection")
    normalized: list[str] = []
    for value in values:
        try:
            network = ipaddress.ip_network(str(value).strip(), strict=False)
        except ValueError as error:
            raise ValueError(f"{name} contains an invalid IP range") from error
        rendered = network.with_prefixlen
        if rendered not in normalized:
            normalized.append(rendered)
    return tuple(normalized)


def _normalize_hostname(host: str) -> str:
    candidate = host.strip().strip("[]").rstrip(".").casefold()
    if not candidate:
        raise DownloadRejectedError("DOWNLOAD host is empty")
    try:
        return ipaddress.ip_address(candidate).compressed
    except ValueError:
        try:
            return candidate.encode("idna").decode("ascii")
        except UnicodeError as error:
            raise DownloadRejectedError("DOWNLOAD host is malformed") from error


def _matches_host(host: str, patterns: Sequence[str]) -> bool:
    for pattern in patterns:
        if pattern.startswith("*."):
            suffix = pattern[2:]
            if host != suffix and host.endswith(f".{suffix}"):
                return True
        elif host == pattern:
            return True
    return False


def _python_scalar(value: Any) -> Any:
    return value.item() if hasattr(value, "item") else value


def _config_value(config: Configuration, option: Any) -> Any:
    return config.get(option)
