# SPDX-License-Identifier: Apache-2.0
"""Adaptive worker-pool partitioning."""

from ray.klein.runtime.partitioning.round_robin_partitioner import RoundRobinPartitioner


class AdaptivePartitioner(RoundRobinPartitioner):
    """Round-robin initial routing with a full eligible-worker retry ring.

    Backpressure adaptation belongs to the unified delivery engine: a full inbox
    advances through the immutable ring embedded in the routing decision.
    """

    def __str__(self) -> str:
        return "ADAPTIVE"
