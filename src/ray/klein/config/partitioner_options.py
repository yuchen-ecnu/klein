# SPDX-License-Identifier: Apache-2.0
from ray.klein.config.config_option import ConfigOption


class PartitionerOptions:
    BUFFER_BUSY_THRESHOLD = ConfigOption(
        "partitioner.adaptive.buffer-busy-threshold",
        0.5,
        float,
        description="The busy threshold for adaptive partitioner. The target task will be regarded as busy "
        "when target task's input buffer size is larger than this threshold * INPUT_BUFFER_SIZE.",
    )
    BUSY_RATIO = ConfigOption(
        "partitioner.adaptive.busy-ratio",
        0.5,
        float,
        description="The busy ratio for adaptive partitioner. Partitioner will require to update buffer statistics"
        " when busy_task_count / total_target_tasks is larger than this ratio.",
    )
    UPDATE_INTERVAL = ConfigOption(
        "partitioner.adaptive.update-interval",
        3.0,
        float,
        description="The update interval for adaptive partitioner. Partitioner will update statisticsaccording to the interval.",
    )
