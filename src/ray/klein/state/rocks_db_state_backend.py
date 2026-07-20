# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import io
import pickle
import shutil
import tarfile
import tempfile
import time
from collections.abc import Callable, Iterable, Mapping
from pathlib import Path, PurePosixPath
from typing import Any

from ray.klein.state.key_group_range import key_group_for_key
from ray.klein.state.managed_state_backend import ManagedStateBackend
from ray.klein.state.state_codec import (
    decode_expiry_key,
    decode_state_key,
    decode_state_namespace,
    decode_state_value,
    decode_timer,
    encode_expiry_key,
    encode_state_key,
    encode_state_value,
    encode_timer_key,
    encode_timer_value,
    state_key_prefix,
    timer_prefix,
)
from ray.klein.state.state_descriptor import StateDescriptor
from ray.klein.state.state_ttl_update_type import StateTTLUpdateType
from ray.klein.state.state_visibility import StateVisibility
from ray.klein.state.timer_domain import TimerDomain
from ray.klein.state.timer_event import TimerEvent

_COLUMN_FAMILIES = ("state", "expiry", "timers", "metadata")


class RocksDBStateBackend(ManagedStateBackend):
    """Task-local RocksDB managed state using byte-level column families."""

    def __init__(
        self,
        path: str,
        *,
        reset: bool = False,
        clock: Callable[[], int] | None = None,
    ) -> None:
        if not path:
            raise ValueError("RocksDB state path must not be empty")
        self._path = str(Path(path).resolve())
        self._clock = clock or (lambda: int(time.time() * 1000))
        self._current_key = None
        self._key_set = False
        self._db = None
        self._families: dict[str, Any] = {}
        self._handles: dict[str, Any] = {}
        if reset:
            _remove_tree_if_present(self._path)
        self._open_db()

    @property
    def path(self) -> str:
        return self._path

    @property
    def current_key(self) -> Any:
        self._require_current_key()
        return self._current_key

    @current_key.setter
    def current_key(self, key: Any) -> None:
        self._current_key = key
        self._key_set = True

    def get(self, descriptor: StateDescriptor, namespace: Any = None) -> Any:
        state_key = self._state_key(descriptor, namespace)
        encoded = self._families["state"].get(state_key)
        if encoded is None:
            return None
        expires_at, payload = decode_state_value(encoded)
        now = self._clock()
        if (
            expires_at is not None
            and expires_at <= now
            and (
                descriptor.ttl_config is None
                or descriptor.ttl_config.visibility == StateVisibility.NEVER_RETURN_EXPIRED
            )
        ):
            self._delete_state_key(state_key)
            return None
        value = descriptor.serializer.loads(payload)
        if (
            descriptor.ttl_config is not None
            and descriptor.ttl_config.update_type == StateTTLUpdateType.ON_READ_AND_WRITE
        ):
            self.put(descriptor, value, namespace)
        return value

    def put(self, descriptor: StateDescriptor, value: Any, namespace: Any = None) -> None:
        from rocksdict import WriteBatch

        state_key = self._state_key(descriptor, namespace)
        expires_at = self._expiry_for(descriptor)
        encoded = encode_state_value(descriptor.serializer.dumps(value), expires_at)
        batch = WriteBatch(raw_mode=True)
        batch.put(state_key, encoded, self._handles["state"])
        if expires_at is not None:
            batch.put(encode_expiry_key(expires_at, state_key), b"", self._handles["expiry"])
        self._db.write(batch)

    def delete(self, descriptor: StateDescriptor, namespace: Any = None) -> None:
        self._delete_state_key(self._state_key(descriptor, namespace))

    def namespaces(self, descriptor: StateDescriptor) -> tuple[Any, ...]:
        prefix = state_key_prefix(descriptor, self.current_key)
        result = []
        for encoded_key in self._families["state"].keys(from_key=prefix):
            if not encoded_key.startswith(prefix):
                break
            result.append(decode_state_namespace(encoded_key))
        return tuple(result)

    def register_timer(self, timestamp: int, namespace: Any, domain: TimerDomain) -> None:
        if timestamp < 0:
            raise ValueError("timer timestamp must be non-negative")
        key = self.current_key
        self._families["timers"][encode_timer_key(timestamp, key, namespace, domain)] = encode_timer_value(
            key,
            namespace,
        )

    def delete_timer(self, timestamp: int, namespace: Any, domain: TimerDomain) -> None:
        encoded_key = encode_timer_key(timestamp, self.current_key, namespace, domain)
        self._delete_cf_key("timers", encoded_key)

    def pop_due_timers(
        self,
        timestamp: int,
        domain: TimerDomain,
        limit: int | None = None,
    ) -> tuple[TimerEvent, ...]:
        prefix = timer_prefix(domain)
        result: list[TimerEvent] = []
        for encoded_key, encoded_value in self._families["timers"].items(from_key=prefix):
            if not encoded_key.startswith(prefix):
                break
            timer_timestamp, key, namespace = decode_timer(encoded_key, encoded_value)
            if timer_timestamp > timestamp:
                break
            result.append(TimerEvent(timer_timestamp, key, namespace, domain))
            if limit is not None and len(result) >= limit:
                break
        for event in result:
            self._delete_cf_key(
                "timers",
                encode_timer_key(event.timestamp, event.key, event.namespace, event.domain),
            )
        return tuple(result)

    def cleanup_expired(self, now_ms: int | None = None, limit: int | None = None) -> int:
        from rocksdict import WriteBatch

        now_ms = self._clock() if now_ms is None else now_ms
        batch = WriteBatch(raw_mode=True)
        processed = 0
        removed = 0
        # Raw-mode Rdict iteration treats values as mapping keys and fails;
        # keys() is the rocksdict API for scanning byte keys.
        for expiry_key in self._families["expiry"].keys():  # noqa: SIM118
            expires_at, state_key = decode_expiry_key(expiry_key)
            if expires_at > now_ms or (limit is not None and processed >= limit):
                break
            encoded = self._families["state"].get(state_key)
            if encoded is not None and decode_state_value(encoded)[0] == expires_at:
                batch.delete(state_key, self._handles["state"])
                removed += 1
            batch.delete(expiry_key, self._handles["expiry"])
            processed += 1
        if processed:
            self._db.write(batch)
        return removed

    def snapshot(self) -> bytes:
        from rocksdict import Checkpoint

        with tempfile.TemporaryDirectory(prefix="klein-rocks-checkpoint-") as temporary:
            checkpoint_path = Path(temporary) / "db"
            Checkpoint(self._db).create_checkpoint(str(checkpoint_path))
            output = io.BytesIO()
            with tarfile.open(fileobj=output, mode="w") as archive:
                for path in sorted(checkpoint_path.iterdir(), key=lambda item: item.name):
                    archive.add(path, arcname=path.name, recursive=False)
            return output.getvalue()

    def restore(self, snapshot: bytes) -> None:
        # Decode into an isolated directory first. A corrupt checkpoint must not
        # destroy the currently open database, and archive members must never be
        # allowed to create links or escape the destination.
        with tempfile.TemporaryDirectory(prefix="klein-rocks-restore-") as temporary:
            staged_path = Path(temporary) / "db"
            staged_path.mkdir()
            with tarfile.open(fileobj=io.BytesIO(snapshot), mode="r:") as archive:
                _extract_checkpoint_archive(archive, staged_path)

            self._close_db()
            _remove_tree_if_present(self._path)
            shutil.move(str(staged_path), self._path)
        self._open_db()

    def snapshot_key_groups(
        self,
        max_parallelism: int,
        key_groups: Iterable[int],
    ) -> Mapping[int, bytes]:
        requested = frozenset(key_groups)
        buckets: dict[int, dict[str, list[tuple[bytes, bytes]]]] = {}

        def bucket(key_group: int) -> dict[str, list[tuple[bytes, bytes]]]:
            return buckets.setdefault(
                key_group,
                {"state": [], "expiry": [], "timers": [], "metadata": []},
            )

        for encoded_key, encoded_value in self._families["state"].items():
            _name, key, _namespace = decode_state_key(encoded_key)
            key_group = key_group_for_key(key, max_parallelism)
            if key_group in requested:
                bucket(key_group)["state"].append((encoded_key, encoded_value))
        for encoded_key, encoded_value in self._families["expiry"].items():
            _expires_at, state_key = decode_expiry_key(encoded_key)
            _name, key, _namespace = decode_state_key(state_key)
            key_group = key_group_for_key(key, max_parallelism)
            if key_group in requested:
                bucket(key_group)["expiry"].append((encoded_key, encoded_value))
        for encoded_key, encoded_value in self._families["timers"].items():
            _timestamp, key, _namespace = decode_timer(encoded_key, encoded_value)
            key_group = key_group_for_key(key, max_parallelism)
            if key_group in requested:
                bucket(key_group)["timers"].append((encoded_key, encoded_value))

        return {
            key_group: pickle.dumps(
                {"format_version": 1, **contents},
                protocol=pickle.HIGHEST_PROTOCOL,
            )
            for key_group, contents in buckets.items()
        }

    def restore_key_groups(self, snapshots: Mapping[int, bytes]) -> None:
        from rocksdict import WriteBatch

        self._close_db()
        _remove_tree_if_present(self._path)
        self._open_db()
        batch = WriteBatch(raw_mode=True)
        entries = 0
        for snapshot in snapshots.values():
            payload = pickle.loads(snapshot)
            if payload.get("format_version") != 1:
                raise ValueError("unsupported RocksDB key-group state format")
            for family in _COLUMN_FAMILIES:
                for encoded_key, encoded_value in payload.get(family, ()):
                    batch.put(encoded_key, encoded_value, self._handles[family])
                    entries += 1
        if entries:
            self._db.write(batch)

    def close(self) -> None:
        self._close_db()

    def _open_db(self) -> None:
        from rocksdict import Rdict

        path = Path(self._path)
        path.parent.mkdir(parents=True, exist_ok=True)
        options = self._options()
        existing = (path / "CURRENT").exists()
        if existing:
            names = set(Rdict.list_cf(self._path, options))
            missing = set(_COLUMN_FAMILIES) - names
            if missing:
                raise ValueError(f"RocksDB managed state is missing column families: {sorted(missing)}")
            column_families = {name: self._options() for name in _COLUMN_FAMILIES}
            self._db = Rdict(self._path, options, column_families)
            self._families = {name: self._db.get_column_family(name) for name in _COLUMN_FAMILIES}
        else:
            self._db = Rdict(self._path, options)
            self._families = {name: self._db.create_column_family(name, self._options()) for name in _COLUMN_FAMILIES}
        self._handles = {name: self._db.get_column_family_handle(name) for name in _COLUMN_FAMILIES}

    @staticmethod
    def _options() -> Any:
        from rocksdict import Options

        options = Options(raw_mode=True)
        options.create_if_missing(True)
        options.create_missing_column_families(True)
        return options

    def _close_db(self) -> None:
        for family in self._families.values():
            family.close()
        self._families = {}
        self._handles = {}
        if self._db is not None:
            self._db.close()
            self._db = None
    def _delete_state_key(self, state_key: bytes) -> None:
        self._delete_cf_key("state", state_key)

    def _delete_cf_key(self, family: str, encoded_key: bytes) -> None:
        from rocksdict import WriteBatch

        batch = WriteBatch(raw_mode=True)
        batch.delete(encoded_key, self._handles[family])
        self._db.write(batch)

    def _state_key(self, descriptor: StateDescriptor, namespace: Any) -> bytes:
        return encode_state_key(descriptor, self.current_key, namespace)

    def _expiry_for(self, descriptor: StateDescriptor) -> int | None:
        if descriptor.ttl_config is None:
            return None
        return self._clock() + descriptor.ttl_config.ttl_milliseconds

    def _require_current_key(self) -> None:
        if not self._key_set:
            raise RuntimeError("current key is not set")


def _remove_tree_if_present(path: str) -> None:
    """Remove a backend directory idempotently without hiding I/O failures."""

    try:
        shutil.rmtree(path)
    except FileNotFoundError:
        return


def _extract_checkpoint_archive(
    archive: tarfile.TarFile,
    destination: Path,
) -> None:
    """Extract regular checkpoint files without trusting tar member paths."""

    for member in archive.getmembers():
        relative_path = PurePosixPath(member.name)
        if relative_path.is_absolute() or ".." in relative_path.parts:
            raise ValueError("RocksDB checkpoint archive escapes its destination")
        target = destination.joinpath(*relative_path.parts)
        if member.isdir():
            target.mkdir(parents=True, exist_ok=True)
            continue
        if not member.isfile():
            raise ValueError("RocksDB checkpoint archive contains a non-file member")
        source = archive.extractfile(member)
        if source is None:
            raise ValueError(f"RocksDB checkpoint archive member {member.name!r} has no content")
        target.parent.mkdir(parents=True, exist_ok=True)
        with source, target.open("wb") as output:
            shutil.copyfileobj(source, output)
