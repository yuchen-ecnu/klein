# SPDX-License-Identifier: Apache-2.0
"""Version-adaptive discovery of the installed public Ray Data API."""

import inspect
from collections.abc import Callable
from typing import Any, get_args

import ray.data
from ray.data import Dataset

from ray.klein.api.ray_data.method_kind import RayDataMethodKind


def _is_single_dataset_annotation(annotation: Any) -> bool:
    if isinstance(annotation, type):
        try:
            return issubclass(annotation, Dataset)
        except TypeError:
            return False
    if isinstance(annotation, str):
        normalized = annotation.replace(" ", "").strip("'\"")
        return normalized in {
            "Dataset",
            "MaterializedDataset",
            "ray.data.Dataset",
            "ray.data.dataset.Dataset",
            "ray.data.dataset.MaterializedDataset",
        }
    return False


def _is_dataset_factory_annotation(annotation: Any) -> bool:
    return _is_single_dataset_annotation(annotation) or any(
        _is_single_dataset_annotation(item) for item in get_args(annotation)
    )


def public_module_function(name: str) -> Callable[..., Any]:
    if name.startswith("_"):
        raise AttributeError(name)
    candidate = getattr(ray.data, name, None)
    if not inspect.isfunction(candidate):
        raise AttributeError(f"ray.data has no public function {name!r}")
    annotation = inspect.signature(candidate).return_annotation
    if not _is_dataset_factory_annotation(annotation) and not name.startswith(("read_", "from_", "range")):
        raise AttributeError(f"ray.data.{name} is not a Dataset factory")
    return candidate


def public_dataset_method(name: str) -> Callable[..., Any]:
    if name.startswith("_"):
        raise AttributeError(name)
    descriptor = inspect.getattr_static(Dataset, name, None)
    if not (inspect.isfunction(descriptor) or isinstance(descriptor, (staticmethod, classmethod))):
        raise AttributeError(f"ray.data.Dataset has no public method {name!r}")
    candidate = getattr(Dataset, name, None)
    if not callable(candidate):
        raise AttributeError(f"ray.data.Dataset has no public method {name!r}")
    return candidate


def dataset_method_binds_instance(name: str) -> bool:
    return inspect.isfunction(inspect.getattr_static(Dataset, name))


def public_dataset_factories() -> tuple[str, ...]:
    names = []
    for name, _ in inspect.getmembers(ray.data, inspect.isfunction):
        try:
            public_module_function(name)
        except AttributeError:
            continue
        names.append(name)
    return tuple(names)


def public_dataset_methods() -> tuple[str, ...]:
    names = []
    for name in dir(Dataset):
        try:
            public_dataset_method(name)
        except AttributeError:
            continue
        names.append(name)
    return tuple(names)


def has_public_dataset_factory(name: str) -> bool:
    try:
        public_module_function(name)
    except AttributeError:
        return False
    return True


def has_public_dataset_method(name: str) -> bool:
    try:
        public_dataset_method(name)
    except AttributeError:
        return False
    return True


def classify_dataset_method(name: str) -> RayDataMethodKind:
    """Classify a Dataset method from its return contract, without an API table."""

    annotation = inspect.signature(public_dataset_method(name)).return_annotation
    if _is_single_dataset_annotation(annotation):
        return RayDataMethodKind.TRANSFORM
    if annotation is not inspect.Signature.empty:
        return RayDataMethodKind.CONSUME
    if name.startswith("write_") or name == "explain":
        return RayDataMethodKind.CONSUME
    return RayDataMethodKind.TRANSFORM
