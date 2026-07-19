# SPDX-License-Identifier: Apache-2.0
import tempfile
from pathlib import Path

from ray.klein.config.config_option import ConfigOption


class StateOptions:
    """Configuration for managed operator state."""

    BACKEND = ConfigOption(
        "state.backend.type", "memory", str, description="Managed keyed-state backend: memory or rocksdb."
    )

    LOCAL_DIRECTORY = ConfigOption(
        "state.backend.local-dir",
        str(Path(tempfile.gettempdir()) / "klein" / "state"),
        str,
        description="Node-local working directory for managed state.",
    )

    OBJECT_STORE_CACHE_ENABLED = ConfigOption(
        "state.checkpoint.object-store-cache.enabled",
        True,
        bool,
        description="Cache sufficiently large immutable state snapshots in Ray's Object Store.",
    )

    OBJECT_STORE_CACHE_MIN_BYTES = ConfigOption(
        "state.checkpoint.object-store-cache.min-bytes",
        1024 * 1024,
        int,
        description="Minimum snapshot size cached in the Object Store instead of coordinator heap.",
    )

    TTL_CLEANUP_BATCH_SIZE = ConfigOption(
        "state.ttl.cleanup.batch-size",
        1000,
        int,
        description="Maximum expired state entries removed after one operator input.",
    )

    MAX_PARALLELISM = ConfigOption(
        "state.keyed.max-parallelism",
        32768,
        int,
        description="Stable key-group count. It must stay unchanged when restoring a rescaled keyed operator.",
    )
