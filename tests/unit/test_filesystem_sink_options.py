# SPDX-License-Identifier: Apache-2.0
from datetime import timedelta

import pytest

from ray.klein._internal.sql.filesystem_sink_options import FilesystemSinkOptions
from ray.klein.api.sql_query_error import SQLQueryError


def test_flink_filesystem_sink_options_are_typed() -> None:
    options = FilesystemSinkOptions.from_mapping(
        {
            "sink.filename-prefix": "events",
            "sink.max-rows-per-file": "1000",
            "sink.parallelism": "3",
            "sink.rolling-policy.file-size": "128 MiB",
            "sink.rolling-policy.rollover-interval": "15 min",
            "sink.rolling-policy.inactivity-interval": "30s",
            "sink.storage-options": '{"anonymous": true}',
            "sink.ray-data-options": '{"compression": "gzip"}',
        }
    )

    assert options.filename_prefix == "events"
    assert options.max_rows_per_file == 1000
    assert options.parallelism == 3
    assert options.max_bytes_per_file == 128 * (1 << 20)
    assert options.rollover_interval == timedelta(minutes=15)
    assert options.inactivity_interval == timedelta(seconds=30)
    assert options.storage_options == {"anonymous": True}
    assert options.ray_data_options == {"compression": "gzip"}


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("sink.parallelism", "0"),
        ("sink.max-rows-per-file", "many"),
        ("sink.rolling-policy.file-size", "12 elephants"),
        ("sink.rolling-policy.rollover-interval", "soon"),
        ("sink.storage-options", "[]"),
    ],
)
def test_invalid_filesystem_sink_options_are_rejected(name: str, value: str) -> None:
    with pytest.raises(SQLQueryError, match=name):
        FilesystemSinkOptions.from_mapping({name: value})
