# SPDX-License-Identifier: Apache-2.0
"""Configuration options for Klein Serve clients and deployments."""

from ray.klein.config.config_option import ConfigOption


class ServeOptions:
    """Ray Serve deployment and embedded proxy-client settings."""

    PROXY_ENDPOINTS = ConfigOption(
        "serve.proxy-endpoints",
        None,
        str,
        description="Comma-separated HTTP base URLs for the Serve proxy.",
    )
    DEPLOYMENT_NAME = ConfigOption(
        "serve.deployment-name",
        None,
        str,
        description="Ray Serve deployment name.",
    )
    ROUTE_PREFIX = ConfigOption(
        "serve.route-prefix",
        "/",
        str,
        description="Ray Serve deployment route prefix.",
    )
    CLIENT_NUM_CPUS = ConfigOption(
        "serve.client.num-cpus",
        1.0,
        float,
        description="CPU allocation for the embedded proxy client.",
    )
    CLIENT_CONCURRENCY = ConfigOption(
        "serve.client.concurrency",
        1,
        int,
        description="Concurrency of the embedded proxy client.",
    )
    CLIENT_ASYNC_BUFFER_SIZE = ConfigOption(
        "serve.client.async-buffer-size",
        100,
        int,
        description="Async buffer capacity of the embedded proxy client.",
    )
    CLIENT_BATCH_TIMEOUT = ConfigOption(
        "serve.client.batch-timeout",
        5,
        int,
        description="Proxy batching timeout in seconds.",
    )
    CLIENT_BATCH_SIZE = ConfigOption(
        "serve.client.batch-size",
        2,
        int,
        description="Proxy batch size.",
    )
    CLIENT_MAX_ATTEMPTS = ConfigOption(
        "serve.client.max-attempts",
        30,
        int,
        description="Maximum proxy request attempts.",
    )
    CLIENT_MAX_REQUEST_BYTES = ConfigOption(
        "serve.client.max-request-bytes",
        16 * 1024 * 1024,
        int,
        description="Maximum encoded request body size for an embedded proxy client.",
    )
    CLIENT_MAX_RESPONSE_BYTES = ConfigOption(
        "serve.client.max-response-bytes",
        16 * 1024 * 1024,
        int,
        description="Maximum decoded response body size for an embedded proxy client.",
    )
    CLIENT_MAX_ROWS = ConfigOption(
        "serve.client.max-rows",
        100_000,
        int,
        description="Maximum rows in one embedded proxy client request.",
    )
    CLIENT_MAX_RESULT_ROWS = ConfigOption(
        "serve.client.max-result-rows",
        100_000,
        int,
        description="Maximum rows accepted in one embedded proxy client result.",
    )
    CLIENT_SLOW_REQUEST_WARNING = ConfigOption(
        "serve.client.slow-request-warning",
        60,
        int,
        description="Elapsed seconds before logging a slow-request warning.",
    )
    HTTP_TIMEOUT = ConfigOption(
        "serve.client.http-timeout",
        300,
        int,
        description="Total logical proxy-call timeout across retries and backoff, in seconds.",
    )
    HTTP_CONNECT_TIMEOUT = ConfigOption(
        "serve.client.http-connect-timeout",
        5,
        int,
        description="HTTP connection and pool-acquisition timeout in seconds.",
    )
    HTTP_LIMIT_PER_HOST = ConfigOption(
        "serve.client.http-limit-per-host",
        1000,
        int,
        description="Maximum HTTP connections per host.",
    )
    HTTP_CONNECTION_LIMIT = ConfigOption(
        "serve.client.http-connection-limit",
        1000,
        int,
        description="Maximum total HTTP connections.",
    )
    RETRY_BACKOFF_MAX = ConfigOption(
        "serve.client.retry-backoff-max",
        3.0,
        float,
        description="Maximum exponential retry backoff in seconds.",
    )
    DEPLOYMENT_MAX_REQUEST_BYTES = ConfigOption(
        "serve.deployment.max-request-bytes",
        16 * 1024 * 1024,
        int,
        description="Maximum request body size accepted by a Klein Serve deployment.",
    )
    DEPLOYMENT_MAX_RESPONSE_BYTES = ConfigOption(
        "serve.deployment.max-response-bytes",
        16 * 1024 * 1024,
        int,
        description="Maximum response body size emitted by a Klein Serve deployment.",
    )
    DEPLOYMENT_MAX_ROWS = ConfigOption(
        "serve.deployment.max-rows",
        100_000,
        int,
        description="Maximum rows accepted in one Klein Serve deployment request.",
    )
    DEPLOYMENT_MAX_RESULT_ROWS = ConfigOption(
        "serve.deployment.max-result-rows",
        100_000,
        int,
        description="Maximum rows emitted by one Klein Serve deployment request.",
    )
