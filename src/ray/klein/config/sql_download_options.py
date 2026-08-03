# SPDX-License-Identifier: Apache-2.0
"""Security and resource limits for SQL ``DOWNLOAD``."""

from datetime import timedelta

from ray.klein.config.config_option import ConfigOption


class SQLDownloadOptions:
    ALLOWED_SCHEMES = ConfigOption(
        "sql.download.allowed-schemes",
        ("file", "local", "memory", "s3", "gs", "http", "https"),
        tuple,
        description="URI schemes accepted by DOWNLOAD. A path without a scheme is treated as local.",
    )

    ALLOWED_HOSTS = ConfigOption(
        "sql.download.allowed-hosts",
        (),
        tuple,
        description="Optional exact or *.suffix HTTP(S) host allowlist. Empty allows any otherwise-safe host.",
    )

    DENIED_HOSTS = ConfigOption(
        "sql.download.denied-hosts",
        (),
        tuple,
        description="Exact or *.suffix HTTP(S) hosts rejected before DNS resolution.",
    )

    ALLOWED_IP_RANGES = ConfigOption(
        "sql.download.allowed-ip-ranges",
        (),
        tuple,
        description="Optional IP/CIDR allowlist applied to every resolved HTTP(S) address.",
    )

    DENIED_IP_RANGES = ConfigOption(
        "sql.download.denied-ip-ranges",
        (),
        tuple,
        description="IP/CIDR denylist applied to every resolved HTTP(S) address.",
    )

    ALLOW_PRIVATE_NETWORK = ConfigOption(
        "sql.download.allow-private-network",
        False,
        bool,
        description="Allow HTTP(S) destinations that resolve outside globally routable address space.",
    )

    MAX_BYTES = ConfigOption(
        "sql.download.max-bytes",
        64 * 1024 * 1024,
        int,
        description="Maximum bytes retained from one DOWNLOAD response.",
    )

    TIMEOUT = ConfigOption(
        "sql.download.timeout",
        timedelta(seconds=30),
        timedelta,
        description="HTTP(S) connection, redirect, and response-read budget; synchronous system DNS "
        "resolution cannot be interrupted by this timer.",
    )

    MAX_REDIRECTS = ConfigOption(
        "sql.download.max-redirects",
        5,
        int,
        description="Maximum HTTP(S) redirects; each destination is validated again.",
    )
