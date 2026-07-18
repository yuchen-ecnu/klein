# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import os
import posixpath
from collections.abc import Mapping
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import urlparse
from uuid import uuid4


class CheckpointFileSystem:
    """Small URI-aware facade over public PyArrow filesystem APIs.

    Plain paths and ``file://`` use the local filesystem. Object-store URIs
    such as ``s3://`` and ``gs://`` use the corresponding PyArrow backend.
    """

    def __init__(
        self,
        root_uri: str,
        storage_options: Mapping[str, Any] | None = None,
    ) -> None:
        if not isinstance(root_uri, str) or not root_uri.strip():
            raise ValueError("checkpoint root URI must be a non-empty string")
        self._root_uri = _normalize_root_uri(root_uri)
        self._storage_options = dict(storage_options or {})
        self._filesystem, self._root_path = self._resolve(self._root_uri, self._storage_options)

    @staticmethod
    def _resolve(
        root_uri: str,
        storage_options: Mapping[str, Any],
    ) -> tuple[Any, str]:
        import pyarrow.fs as pafs

        parsed = urlparse(root_uri)
        scheme = parsed.scheme.lower()
        if not storage_options:
            return pafs.FileSystem.from_uri(root_uri)
        if scheme == "s3":
            return pafs.S3FileSystem(**storage_options), f"{parsed.netloc}{parsed.path}".strip("/")
        if scheme in {"gs", "gcs"}:
            return pafs.GcsFileSystem(**storage_options), f"{parsed.netloc}{parsed.path}".strip("/")
        raise ValueError(f"storage_options are not supported for checkpoint URI scheme {scheme or 'file'!r}")

    @property
    def root_uri(self) -> str:
        return self._root_uri

    def uri(self, relative_path: str = "") -> str:
        relative_path = _relative(relative_path)
        if not relative_path:
            return self._root_uri
        separator = "" if self._root_uri.endswith("/") else "/"
        return f"{self._root_uri}{separator}{relative_path}"

    def relative_path(self, uri: str) -> str:
        """Return a path below this root, rejecting foreign checkpoint URIs."""

        if uri == self._root_uri:
            return ""
        separator = "" if self._root_uri.endswith("/") else "/"
        prefix = f"{self._root_uri}{separator}"
        if not uri.startswith(prefix):
            raise ValueError(f"checkpoint URI {uri!r} is outside root {self._root_uri!r}")
        return _relative(uri[len(prefix) :])

    def create_dir(self, relative_path: str) -> None:
        self._filesystem.create_dir(self._path(relative_path), recursive=True)

    def write_bytes(self, relative_path: str, value: bytes, *, atomic: bool = False) -> str:
        import pyarrow.fs as pafs

        relative_path = _relative(relative_path)
        target = self._path(relative_path)
        parent = posixpath.dirname(target)
        if parent:
            self._filesystem.create_dir(parent, recursive=True)
        if atomic and isinstance(self._filesystem, pafs.LocalFileSystem):
            temporary = Path(f"{target}.tmp-{uuid4().hex}")
            try:
                with temporary.open("wb") as stream:
                    stream.write(value)
                    stream.flush()
                    os.fsync(stream.fileno())
                temporary.replace(target)
            finally:
                temporary.unlink(missing_ok=True)
        else:
            # A single object PUT is the publication boundary on S3/GCS. Do not
            # emulate rename there: rename is a non-atomic copy+delete.
            self._write(target, value)
        return self.uri(relative_path)

    def open_output_stream(self, relative_path: str) -> Any:
        """Open a binary output stream below this filesystem root."""

        target = self._path(relative_path)
        parent = posixpath.dirname(target)
        if parent:
            self._filesystem.create_dir(parent, recursive=True)
        return self._filesystem.open_output_stream(target)

    def _write(self, path: str, value: bytes) -> None:
        with self._filesystem.open_output_stream(path) as stream:
            stream.write(value)

    def read_bytes(self, relative_path: str) -> bytes:
        with self._filesystem.open_input_file(self._path(relative_path)) as stream:
            return stream.read()

    def exists(self, relative_path: str) -> bool:
        import pyarrow.fs as pafs

        return self._filesystem.get_file_info(self._path(relative_path)).type != pafs.FileType.NotFound

    def move_file(self, source_path: str, target_path: str) -> str:
        """Idempotently publish one file within this filesystem root."""

        source_path = _relative(source_path)
        target_path = _relative(target_path)
        if self.exists(target_path):
            if self.exists(source_path):
                self._filesystem.delete_file(self._path(source_path))
            return self.uri(target_path)
        if not self.exists(source_path):
            raise FileNotFoundError(f"source file does not exist: {self.uri(source_path)}")
        target = self._path(target_path)
        parent = posixpath.dirname(target)
        if parent:
            self._filesystem.create_dir(parent, recursive=True)
        self._filesystem.move(self._path(source_path), target)
        return self.uri(target_path)

    def delete_file(self, relative_path: str) -> None:
        """Delete a file if it exists."""

        if self.exists(relative_path):
            self._filesystem.delete_file(self._path(relative_path))

    def list_directories(self, relative_path: str) -> tuple[str, ...]:
        import pyarrow.fs as pafs

        base = self._path(relative_path)
        try:
            infos = self._filesystem.get_file_info(pafs.FileSelector(base, recursive=False))
        except FileNotFoundError:
            return ()
        return tuple(PurePosixPath(info.path).name for info in infos if info.type == pafs.FileType.Directory)

    def delete_dir(self, relative_path: str) -> None:
        path = self._path(relative_path)
        if self.exists(relative_path):
            self._filesystem.delete_dir(path)

    def _path(self, relative_path: str) -> str:
        relative_path = _relative(relative_path)
        return self._root_path if not relative_path else posixpath.join(self._root_path, relative_path)


def _relative(path: str) -> str:
    normalized = str(path).replace("\\", "/").strip("/")
    if normalized in {"", "."}:
        return ""
    if ".." in PurePosixPath(normalized).parts:
        raise ValueError("checkpoint paths must not escape their root")
    return normalized


def _normalize_root_uri(root_uri: str) -> str:
    value = root_uri.strip()
    if not urlparse(value).scheme:
        # Keep the caller-visible path spelling stable (notably macOS
        # ``/var``); resolving symlinks here would change checkpoint URIs.
        value = str(Path(value).absolute())
    if value in {"/", "file:///"}:
        return value
    return value.rstrip("/")
