# SPDX-License-Identifier: Apache-2.0
from datetime import timedelta

from ray.klein.config.config_option import ConfigOption


class PipelineOptions:
    INPUT_BUFFER_SIZE = ConfigOption(
        "pipeline.input-buffer.size",
        200,
        int,
        description="Maximum logical rows retained in each task input buffer; an oversized columnar block is exclusive.",
    )

    INPUT_BUFFER_MAX_BYTES = ConfigOption(
        "pipeline.input-buffer.max-bytes",
        64 * 1024 * 1024,
        int,
        description="Maximum estimated bytes retained in each task input buffer; an oversized block is exclusive.",
    )

    PLACEMENT_GROUP_ENABLED = ConfigOption(
        "pipeline.placement-group.enabled",
        True,
        bool,
        description="Reserve one independently releasable single-bundle Ray PlacementGroup "
        "per streaming actor, enabling incremental scale-out reservation and scale-in "
        "release without moving retained actors. Falls back to round-robin then native "
        "placement if the groups can't be reserved in time.",
    )

    PLACEMENT_GROUP_STRATEGY = ConfigOption(
        "pipeline.placement-group.strategy",
        "PACK",
        str,
        description="PlacementGroup strategy passed to each elastic actor group. Only PACK "
        "and SPREAD are supported and are currently equivalent because each group contains "
        "one bundle; STRICT_* cannot preserve cross-actor semantics in elastic mode.",
    )

    PLACEMENT_GROUP_READY_TIMEOUT = ConfigOption(
        "pipeline.placement-group.ready-timeout",
        timedelta(seconds=120),
        timedelta,
        description="Max time to wait for all elastic actor PlacementGroups to be reserved "
        "before falling back to round-robin/native placement.",
    )

    INPUT_BUFFER_PUT_TIMEOUT = ConfigOption(
        "pipeline.input-buffer.put-timeout",
        timedelta(seconds=1),
        timedelta,
        description="maximum waiting time for each put request of input buffer.",
    )

    OUTPUT_BUFFER_MAX_ROWS = ConfigOption(
        "pipeline.output-buffer.max-rows",
        1000,
        int,
        description="Maximum logical rows an operator invocation may buffer before handing output to the emit queue.",
    )

    OUTPUT_BUFFER_MAX_BYTES = ConfigOption(
        "pipeline.output-buffer.max-bytes",
        64 * 1024 * 1024,
        int,
        description="Maximum estimated bytes an operator invocation may retain before handing output to the emit queue.",
    )

    EMIT_QUEUE_MAX_BATCHES = ConfigOption(
        "pipeline.emit-queue.max-batches",
        2,
        int,
        description="Maximum detached output batches waiting in a task's FIFO emit queue.",
    )

    OPERATOR_CHAINING = ConfigOption(
        "pipeline.operator-chaining.enabled",
        True,
        bool,
        description="Operator chaining allows non-shuffle operations to be co-located in the same thread "
        "fully avoiding serialization and de-serialization. Enabled by default.",
    )

    INTERNAL_BATCH_SIZE = ConfigOption(
        "pipeline.internal.batch-size",
        10,
        int,
        description="Number of emitted records grouped into one downstream put request per target.",
    )

    INTERNAL_BATCH_MAX_ROWS = ConfigOption(
        "pipeline.internal.batch-max-rows",
        1000,
        int,
        description="Flush a downstream micro-batch after it reaches this many logical rows.",
    )

    INTERNAL_BATCH_MAX_BYTES = ConfigOption(
        "pipeline.internal.batch-max-bytes",
        4 * 1024 * 1024,
        int,
        description="Flush a downstream micro-batch after it reaches this estimated payload size.",
    )

    TRANSPORT_OBJECT_STORE_THRESHOLD_BYTES = ConfigOption(
        "pipeline.transport.object-store-threshold-bytes",
        128 * 1024,
        int,
        description="Minimum duplicated broadcast payload size shared through one Ray Object Store reference.",
    )

    # Columnar passthrough: keep a batched operator's output column-oriented all
    # the way to the next operator instead of exploding it into per-row records
    # (operator.collect) and re-columnarising downstream (InputBatchAccumulator). Removes
    # the column->row->column copy at every batched hop. Key/custom-partitioned
    # edges stay correct by slicing the columnar batch per target by row key.
    # Enabled by default: the transport, routing, replay and input accumulator
    # all preserve Record.num_rows and have row-slicing coverage.
    COLUMNAR_PASSTHROUGH_ENABLED = ConfigOption(
        "pipeline.columnar-passthrough.enabled",
        True,
        bool,
        description="Pass batched operator output downstream column-oriented (no per-row "
        "explode + re-columnarise). Disable for a legacy row-oriented wire format.",
    )

    # --- Single-fault at-least-once replay buffer (data-plane watermark) ---
    # Each EdgeOutput retains records it has already put() downstream until
    # the downstream confirms (via the put() return watermark) that it has in
    # turn forwarded their derived output onward — i.e. the records exist on two
    # nodes. On a downstream single-task crash the upstream replays its buffer to
    # the rebuilt task, giving at-least-once without a full-job restart. The
    # buffer is drained by per-sender durability watermarks and guarded by a hard
    # retained-memory limit before a stalled watermark can exhaust the process.
    REPLAY_BUFFER_ENABLED = ConfigOption(
        "pipeline.replay-buffer.enabled",
        True,
        bool,
        description="Retain emitted records for replay to a rebuilt downstream task "
        "(single-fault at-least-once). Disable to fall back to full-job "
        "restart recovery only.",
    )

    REPLAY_WATERMARK_FLUSH_BATCHES = ConfigOption(
        "pipeline.replay-buffer.watermark-flush-batches",
        32,
        int,
        description="Force a full flush + watermark advance every N processed input "
        "batches. Lower = tighter replay-buffer memory bound but smaller "
        "downstream micro-batches; higher = the opposite.",
    )

    # Hard retained-memory guard for the replay buffer. Once an acknowledgement
    # has trimmed old entries, admitting a new batch must still fit under this
    # bound or the task fails into normal recovery before the process reaches OOM.
    REPLAY_BUFFER_MAX_BYTES = ConfigOption(
        "pipeline.replay-buffer.max-bytes",
        256 * 1024 * 1024,
        int,
        description="Hard estimated-memory bound for retained replay records. Must be positive when replay is enabled.",
    )
