# SPDX-License-Identifier: Apache-2.0
"""Streaming record partitioning strategies."""

from typing import Any

from ray.klein._internal.lazy_exports import resolve_lazy_export

_EXPORTS = {
    "AdaptivePartitioner": ("ray.klein.runtime.partitioning.adaptive_partitioner", "AdaptivePartitioner"),
    "BroadcastPartitioner": ("ray.klein.runtime.partitioning.broadcast_partitioner", "BroadcastPartitioner"),
    "ChannelPattern": ("ray.klein.runtime.partitioning.channel_topology", "ChannelPattern"),
    "ChannelTopology": ("ray.klein.runtime.partitioning.channel_topology", "ChannelTopology"),
    "ForwardPartitioner": ("ray.klein.runtime.partitioning.forward_partitioner", "ForwardPartitioner"),
    "KeyPartitioner": ("ray.klein.runtime.partitioning.key_partitioner", "KeyPartitioner"),
    "Partitioner": ("ray.klein.runtime.partitioning.partitioner", "Partitioner"),
    "PartitionerSpec": ("ray.klein.runtime.partitioning.partitioner_spec", "PartitionerSpec"),
    "RescalePartitioner": ("ray.klein.runtime.partitioning.rescale_partitioner", "RescalePartitioner"),
    "RoundRobinPartitioner": ("ray.klein.runtime.partitioning.round_robin_partitioner", "RoundRobinPartitioner"),
    "SimplePartitioner": ("ray.klein.runtime.partitioning.simple_partitioner", "SimplePartitioner"),
    "WorkerPoolDispatcher": ("ray.klein.runtime.partitioning.worker_pool_dispatcher", "WorkerPoolDispatcher"),
}

__all__ = sorted(_EXPORTS)


def __getattr__(name: str) -> Any:
    return resolve_lazy_export(name, _EXPORTS, globals(), __name__)
