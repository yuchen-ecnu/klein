# SPDX-License-Identifier: Apache-2.0
"""Version-adaptive, lazy Ray Data integration."""

from ray.klein.api.ray_data.call import RayDataCall
from ray.klein.api.ray_data.context_adapter import RayDataContextAdapter
from ray.klein.api.ray_data.discovery import (
    classify_dataset_method,
    has_public_dataset_factory,
    has_public_dataset_method,
    public_dataset_factories,
    public_dataset_methods,
)
from ray.klein.api.ray_data.error import RayDataAPIError
from ray.klein.api.ray_data.method_kind import RayDataMethodKind
from ray.klein.api.ray_data.stream_adapter import RayDataStreamAdapter

__all__ = [
    "RayDataAPIError",
    "RayDataCall",
    "RayDataContextAdapter",
    "RayDataMethodKind",
    "RayDataStreamAdapter",
    "classify_dataset_method",
    "has_public_dataset_factory",
    "has_public_dataset_method",
    "public_dataset_factories",
    "public_dataset_methods",
]
