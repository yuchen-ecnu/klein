# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from transforms import BatchIdentity, identity_batch, transform_stream

from ray.klein.api.data_stream import DataStream
from ray.klein.api.klein_context import KleinContext


def prepare_source(stream: DataStream) -> DataStream:
    return (
        stream.flat_map(lambda row: row, num_cpus=1.2, num_gpus=0.25, concurrency=2)
        .map_batches(identity_batch, num_cpus=0.25, batch_size=16)
        .map(BatchIdentity, num_gpus=1.4)
    )


def build_pipeline() -> None:
    context = KleinContext()
    source = context.data.read_csv("input.csv", ray_remote_args={"num_cpus": 1.0})
    prepared = prepare_source(source)

    primary = transform_stream(prepared).flat_map(
        lambda row: row,
        num_cpus=1,
        num_gpus=0.5,
        name="ep1",
    )
    secondary = prepared.map(lambda row: row, num_cpus=1, num_gpus=0.5, name="ep2")
    primary.data.write_parquet("primary-output")
    secondary.data.write_parquet("secondary-output")
    context.execute("resource-plan-fixture").wait()


if __name__ == "__main__":
    build_pipeline()
