# SPDX-License-Identifier: Apache-2.0
"""Ray Serve deployment for extracted Klein operator chains."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Callable
from pathlib import Path
from typing import Any

import orjson
from starlette.responses import JSONResponse

from ray import serve
from ray.klein._internal.logging import get_logger
from ray.klein.config.environment_variables import EnvironmentVariables
from ray.klein.runtime.serve_serialization import decode_batch, numpy_encoder

logger = get_logger(__name__)


@serve.deployment
class KleinServeDeployment:
    """Execute an extracted Klein operator chain behind Ray Serve."""

    def __init__(self) -> None:
        self.operators: list[Callable] = []
        self.service_name = os.environ.get(EnvironmentVariables.SERVICE_NAME)
        self.ready = False
        self._loaded_key: tuple[str, float] | None = None

    def reconfigure(self, config: dict[str, Any]) -> None:
        entrypoint = config["workflow"]
        key = (entrypoint, Path(entrypoint).stat().st_mtime)
        if key == self._loaded_key and self.operators:
            logger.info("Workflow %s unchanged; reusing loaded operators", entrypoint)
            return

        from ray.klein.runtime.serve_extract import run_extraction

        try:
            operators = run_extraction(entrypoint)
        except Exception as error:
            logger.exception("Failed to extract serve operators from workflow %s", entrypoint)
            raise RuntimeError(f"Failed to extract serve operators from workflow {entrypoint}: {error}") from error
        if not operators:
            raise RuntimeError("No operators found in the deployment")
        self.operators = operators
        self._loaded_key = key
        self.ready = True
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
        return JSONResponse({"error": message}, status_code=499)

    def _run_operators(self, data: Any) -> Any:
        for operator in self.operators:
            data = operator(data)
        return data


app = KleinServeDeployment.bind()
