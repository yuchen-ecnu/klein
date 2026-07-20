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
from starlette.responses import JSONResponse

from ray import serve
from ray.klein._internal.logging import get_logger
from ray.klein.config.environment_variables import EnvironmentVariables
from ray.klein.runtime.serve_functions import close_operators
from ray.klein.runtime.serve_serialization import decode_batch, numpy_encoder

logger = get_logger(__name__)


@serve.deployment
class KleinServeDeployment:
    """Execute an extracted Klein operator chain behind Ray Serve."""

    def __init__(self) -> None:
        self.operators: list[Callable] = []
        self.service_name = os.environ.get(EnvironmentVariables.SERVICE_NAME)
        self.ready = False
        self._loaded_key: tuple[str, str] | None = None
        # Serve can run sync user methods in a thread pool. Fence reconfigure and
        # request execution so one replica never invokes or closes the same UDF
        # instance concurrently.
        self._operator_lock = RLock()
        self._reconfigure_lock = RLock()

    def reconfigure(self, config: dict[str, Any]) -> None:
        entrypoint = config["workflow"]
        workflow = Path(entrypoint).resolve()

        from ray.klein.runtime.serve_extract import run_extraction

        with self._reconfigure_lock:
            content_digest = hashlib.sha256(workflow.read_bytes()).hexdigest()
            key = (str(workflow), content_digest)
            if key == self._loaded_key and self.operators:
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
                    self.ready = True
                    installed = True
            finally:
                if not installed:
                    close_operators(operators)
            close_operators(previous, excluding=operators)
            logger.info("Initialized %s operators", len(operators))

    async def __call__(self, request) -> JSONResponse:
        service_error = self._validate_service(request)
        if service_error is not None:
            return service_error
        if not self.ready:
            return JSONResponse({"error": "Service not ready"}, status_code=503)
        data = decode_batch(await request.json())
        result = await asyncio.get_running_loop().run_in_executor(None, self._run_operators, data)
        content = orjson.loads(orjson.dumps(result, default=numpy_encoder))
        return JSONResponse(content=content)

    def _validate_service(self, request) -> JSONResponse | None:
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
