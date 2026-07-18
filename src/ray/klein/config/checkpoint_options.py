# SPDX-License-Identifier: Apache-2.0
import tempfile
from pathlib import Path

from ray.klein.config.config_option import ConfigOption


class CheckpointOptions:
    PERSISTENCE_INTERVAL = ConfigOption(
        "execution.checkpointing.persistence-interval",
        600,
        int,
        description="Checkpoint metadata persistence interval in seconds; 0 disables periodic persistence.",
    )

    MAX_CONCURRENT = ConfigOption(
        "execution.checkpointing.max-concurrent-checkpoints",
        100,
        int,
        description="The maximum number of checkpoint attempts that may be in progress at the same time. "
        "If this value is n, then no checkpoints will be triggered while n checkpoint attempts are currently "
        "in flight. For the next checkpoint to be triggered, one checkpoint attempt would need to finish or expire.",
    )

    TIMEOUT = ConfigOption(
        "execution.checkpointing.timeout",
        600,
        int,
        description="The maximum time that a checkpoint may take before being discarded.",
    )

    HISTORY_SIZE = ConfigOption(
        "execution.checkpointing.max-history-size",
        100,
        int,
        description="The maximum number of checkpoint histories maintained in checkpoint coordinator.",
    )

    # Committer-side notify dispatch. When True, a committer fires the
    # checkpoint-complete RPC WITHOUT blocking on the ack
    # (fire-and-reap): the call returns immediately and the result is reaped on
    # a later barrier, with carry-forward retry of any that haven't landed. This
    # removes the per-barrier coordinator round-trip from the alignment hot path
    # (a single-actor fan-in point under high checkpoint frequency). Safe because
    # the coordinator's per-committer ack is idempotent, so a retried notify
    # can't double-count. False performs a synchronous acknowledgement.
    ASYNC_NOTIFY = ConfigOption(
        "execution.checkpointing.async-notify",
        False,
        bool,
        description="Fire committer checkpoint-complete notifications without blocking "
        "on the coordinator ack (reaped + retried on later barriers). "
        "False = synchronous per-barrier notify.",
    )

    RESTORE_PATH = ConfigOption(
        "execution.savepoint.path", None, str, description="Path to a savepoint to restore the job from."
    )

    DIRECTORY = ConfigOption(
        "execution.checkpointing.dir",
        str(Path(tempfile.gettempdir()) / "klein" / "checkpoint"),
        str,
        description="Durable checkpoint root URI. Supports local/file:// and PyArrow "
        "object-store schemes such as s3:// and gs://.",
    )

    STORAGE_OPTIONS = ConfigOption(
        "execution.checkpointing.storage-options",
        None,
        dict,
        description="Keyword arguments for the PyArrow S3FileSystem or GcsFileSystem used by the checkpoint URI.",
    )

    RETAINED_COUNT = ConfigOption(
        "execution.checkpointing.num-retained",
        1,
        int,
        description="Number of completed chk-N directories retained per job.",
    )
