# SPDX-License-Identifier: Apache-2.0
"""Ray Serve deployment for extracted Klein operator chains."""

from __future__ import annotations

import asyncio
import hashlib
import os
from collections.abc import Callable
from pathlib import Path
from threading import RLock
from typing import Any

import orjson
from starlette.responses import JSONResponse, Response

from ray import serve
from ray.klein._internal.logging import get_logger
from ray.klein.config.config_option import ConfigOption
from ray.klein.config.environment_variables import EnvironmentVariables
from ray.klein.config.serve_options import ServeOptions
from ray.klein.runtime.serve_functions import close_operators
from ray.klein.runtime.serve_serialization import decode_batch, enforce_row_limit, numpy_encoder

logger = get_logger(__name__)


class _RequestLimitError(ValueError):
    pass


@serve.deployment
class KleinServeDeployment:
    """Execute an extracted Klein operator chain behind Ray Serve."""

    def __init__(self) -> None:
        self.operators: list[Callable] = []
        self.service_name = os.environ.get(EnvironmentVariables.SERVICE_NAME)
        self.ready = False
        self._loaded_key: tuple[str, str] | None = None
        self.max_request_bytes = int(ServeOptions.DEPLOYMENT_MAX_REQUEST_BYTES.default)
        self.max_response_bytes = int(ServeOptions.DEPLOYMENT_MAX_RESPONSE_BYTES.default)
        self.max_rows = int(ServeOptions.DEPLOYMENT_MAX_ROWS.default)
        self.max_result_rows = int(ServeOptions.DEPLOYMENT_MAX_RESULT_ROWS.default)
        # Serve can run sync user methods in a thread pool. Fence reconfigure and
        # request execution so one replica never invokes or closes the same UDF
        # instance concurrently.
        self._operator_lock = RLock()
        self._reconfigure_lock = RLock()

    def reconfigure(self, config: dict[str, Any]) -> None:
        limits = (
            self._configured_limit(
                config,
                ServeOptions.DEPLOYMENT_MAX_REQUEST_BYTES,
                "max_request_bytes",
            ),
            self._configured_limit(
                config,
                ServeOptions.DEPLOYMENT_MAX_RESPONSE_BYTES,
                "max_response_bytes",
            ),
            self._configured_limit(config, ServeOptions.DEPLOYMENT_MAX_ROWS, "max_rows"),
            self._configured_limit(
                config,
                ServeOptions.DEPLOYMENT_MAX_RESULT_ROWS,
                "max_result_rows",
            ),
        )
        entrypoint = config["workflow"]
        workflow = Path(entrypoint).resolve()

        from ray.klein.runtime.serve_extract import run_extraction

        with self._reconfigure_lock:
            content_digest = hashlib.sha256(workflow.read_bytes()).hexdigest()
            key = (str(workflow), content_digest)
            if key == self._loaded_key and self.operators:
                with self._operator_lock:
                    self._install_limits(limits)
                logger.info("Workflow %s unchanged; reusing loaded operators", workflow)
                return
            try:
                operators = run_extraction(str(workflow))
            except Exception as error:
                logger.exception("Failed to extract serve operators from workflow %s", workflow)
                raise RuntimeError(f"Failed to extract serve operators from workflow {workflow}: {error}") from error
            if not operators:
                raise RuntimeError("No operators found in the deployment")
            installed = False
            try:
                extracted_digest = hashlib.sha256(workflow.read_bytes()).hexdigest()
                if extracted_digest != content_digest:
                    raise RuntimeError(f"Workflow {workflow} changed while Serve operators were being extracted; retry")

                # Extraction happens without blocking requests. Acquiring the request
                # lock here waits for every old-chain invocation before the atomic swap.
                with self._operator_lock:
                    previous = self.operators
                    self.operators = operators
                    self._loaded_key = key
                    self._install_limits(limits)
                    self.ready = True
                    installed = True
            finally:
                if not installed:
                    close_operators(operators)
            close_operators(previous, excluding=operators)
            logger.info("Initialized %s operators", len(operators))

    async def __call__(self, request: Any) -> Response:
        service_error = self._validate_service(request)
        if service_error is not None:
            return service_error
        if not self.ready:
            return JSONResponse({"error": "Service not ready"}, status_code=503)
        try:
            request_value = await self._read_request(request)
            data = decode_batch(request_value)
            try:
                enforce_row_limit(data, self.max_rows, "Serve request")
            except ValueError as error:
                raise _RequestLimitError(str(error)) from error
        except _RequestLimitError as error:
            return JSONResponse({"error": str(error)}, status_code=413)
        except (TypeError, ValueError, orjson.JSONDecodeError) as error:
            return JSONResponse({"error": str(error)}, status_code=400)
        result = await asyncio.get_running_loop().run_in_executor(None, self._run_operators, data)
        try:
            enforce_row_limit(result, self.max_result_rows, "Serve result")
            content = orjson.dumps(result, default=numpy_encoder)
            if len(content) > self.max_response_bytes:
                raise ValueError(f"Serve response exceeds the {self.max_response_bytes}-byte limit")
        except (TypeError, ValueError) as error:
            return JSONResponse({"error": str(error)}, status_code=422)
        return Response(content=content, media_type="application/json")

    async def _read_request(self, request: Any) -> Any:
        raw_length = request.headers.get("content-length")
        if raw_length is not None:
            try:
                declared_length = int(raw_length)
            except ValueError as error:
                raise ValueError("Serve request has an invalid Content-Length") from error
            if declared_length < 0:
                raise ValueError("Serve request has an invalid Content-Length")
            if declared_length > self.max_request_bytes:
                raise _RequestLimitError(f"Serve request exceeds the {self.max_request_bytes}-byte limit")

        stream = getattr(request, "stream", None)
        if not callable(stream):
            value = await request.json()
            encoded = orjson.dumps(value)
            if len(encoded) > self.max_request_bytes:
                raise _RequestLimitError(f"Serve request exceeds the {self.max_request_bytes}-byte limit")
            return value

        chunks: list[bytes] = []
        retained = 0
        async for chunk in stream():
            retained += len(chunk)
            if retained > self.max_request_bytes:
                raise _RequestLimitError(f"Serve request exceeds the {self.max_request_bytes}-byte limit")
            chunks.append(chunk)
        return orjson.loads(b"".join(chunks))

    @staticmethod
    def _configured_limit(config: dict[str, Any], option: ConfigOption[int], alias: str) -> int:
        value = config.get(option.key, config.get(alias, option.default))
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"{option.key} must be a positive integer")
        return int(value)

    def _install_limits(self, limits: tuple[int, int, int, int]) -> None:
        (
            self.max_request_bytes,
            self.max_response_bytes,
            self.max_rows,
            self.max_result_rows,
        ) = limits

    def _validate_service(self, request: Any) -> JSONResponse | None:
        if self.service_name is None:
            return None
        actual_service = request.headers.get("rayservice")
        if self.service_name == actual_service:
            return None
        message = f"Expected service {self.service_name!r}, got {actual_service!r}"
        logger.error(message)
        return JSONResponse({"error": message}, status_code=421)

    def _run_operators(self, data: Any) -> Any:
        with self._operator_lock:
            for operator in self.operators:
                data = operator(data)
            return data

    def __del__(self) -> None:
        try:
            reconfigure_lock = getattr(self, "_reconfigure_lock", None)
            lock = getattr(self, "_operator_lock", None)
            if lock is None or reconfigure_lock is None:
                return
            with reconfigure_lock:
                with lock:
                    operators, self.operators = getattr(self, "operators", []), []
                    self.ready = False
                    self._loaded_key = None
                close_operators(operators)
        except Exception:
            # Destructors must remain best-effort during interpreter/replica teardown.
            pass


app = KleinServeDeployment.bind()
