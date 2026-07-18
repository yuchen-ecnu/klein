# SPDX-License-Identifier: Apache-2.0

from ray.klein.runtime.partitioning.adaptive_partitioner import AdaptivePartitioner
from ray.klein.runtime.partitioning.forward_partitioner import ForwardPartitioner
from ray.klein.runtime.partitioning.partitioner import Partitioner
from ray.klein.runtime.partitioning.rescale_partitioner import RescalePartitioner


def default_partitioner(
    source_parallelism: int | tuple[int, int],
    target_parallelism: int | tuple[int, int],
) -> Partitioner:
    if isinstance(source_parallelism, tuple) or isinstance(target_parallelism, tuple):
        return AdaptivePartitioner()
    if source_parallelism == target_parallelism:
        return ForwardPartitioner()
    if source_parallelism % target_parallelism == 0 or target_parallelism % source_parallelism == 0:
        return RescalePartitioner()
    return AdaptivePartitioner()
