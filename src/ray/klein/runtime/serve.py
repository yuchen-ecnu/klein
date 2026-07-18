# SPDX-License-Identifier: Apache-2.0
"""Stable import surface used by Ray Serve YAML deployments."""

from ray.klein.runtime.serve_client import EmbeddedProxyClient
from ray.klein.runtime.serve_deployment import KleinServeDeployment, app
from ray.klein.runtime.serve_functions import instantiate_logical_functions
from ray.klein.runtime.serve_serialization import decode_batch, numpy_encoder

__all__ = [
    "EmbeddedProxyClient",
    "KleinServeDeployment",
    "app",
    "decode_batch",
    "instantiate_logical_functions",
    "numpy_encoder",
]
