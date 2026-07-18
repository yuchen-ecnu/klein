# SPDX-License-Identifier: Apache-2.0
from unittest.mock import MagicMock, patch

import pyarrow.fs as pafs

from ray.klein.state.checkpoint_file_system import CheckpointFileSystem


def test_custom_s3_options_are_forwarded_to_pyarrow(monkeypatch):
    backend = MagicMock()
    captured = {}

    def create_s3_filesystem(**options):
        captured.update(options)
        return backend

    monkeypatch.setattr(pafs, "S3FileSystem", create_s3_filesystem)

    filesystem = CheckpointFileSystem(
        "s3://bucket/checkpoints",
        {"anonymous": True, "endpoint_override": "minio:9000"},
    )

    assert captured == {"anonymous": True, "endpoint_override": "minio:9000"}
    assert filesystem.uri("job/chk-1/_metadata") == ("s3://bucket/checkpoints/job/chk-1/_metadata")


def test_object_store_atomic_write_uses_one_put_without_rename(monkeypatch):
    backend = MagicMock()
    stream = MagicMock()
    backend.open_output_stream.return_value.__enter__.return_value = stream
    monkeypatch.setattr(
        CheckpointFileSystem,
        "_resolve",
        staticmethod(lambda _uri, _options: (backend, "bucket/checkpoints")),
    )
    filesystem = CheckpointFileSystem("s3://bucket/checkpoints")

    with patch("ray.klein.state.checkpoint_file_system.os.replace") as replace:
        result = filesystem.write_bytes("job/chk-1/_metadata", b"manifest", atomic=True)

    assert result == "s3://bucket/checkpoints/job/chk-1/_metadata"
    backend.open_output_stream.assert_called_once_with("bucket/checkpoints/job/chk-1/_metadata")
    stream.write.assert_called_once_with(b"manifest")
    replace.assert_not_called()
