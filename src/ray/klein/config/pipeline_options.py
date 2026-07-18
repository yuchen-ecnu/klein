# SPDX-License-Identifier: Apache-2.0
from datetime import timedelta

from ray.klein.config.config_option import ConfigOption


class PipelineOptions:
    INPUT_BUFFER_SIZE = ConfigOption(
        "pipeline.input-buffer.size", 200, int, description="maximum input buffer size for each task."
    )

    PLACEMENT_GROUP_ENABLED = ConfigOption(
        "pipeline.placement-group.enabled",
        True,
        bool,
        description="Use a Ray PlacementGroup as the default actor placement: the whole "
        "job's subtasks are gang-scheduled (all-or-nothing) with FORWARD-affinity "
        "bundle grouping, removing per-actor fragmentation deadlock. Falls back to "
        "round-robin then native placement if the group can't be reserved in time.",
    )

    PLACEMENT_GROUP_STRATEGY = ConfigOption(
        "pipeline.placement-group.strategy",
        "PACK",
        str,
        description="PlacementGroup strategy: PACK (default, minimize cross-node fragmentation "
        "and keep FORWARD chains local) or SPREAD (avoid single-node hot spots).",
    )

    PLACEMENT_GROUP_READY_TIMEOUT = ConfigOption(
        "pipeline.placement-group.ready-timeout",
        timedelta(seconds=120),
        timedelta,
        description="Max time to wait for the PlacementGroup to be reserved before falling "
        "back to round-robin/native placement (a fragmented cluster may never "
        "satisfy the all-or-nothing reservation).",
    )

    INPUT_BUFFER_PUT_TIMEOUT = ConfigOption(
        "pipeline.input-buffer.put-timeout",
        timedelta(seconds=1),
        timedelta,
        description="maximum waiting time for each put request of input buffer.",
    )

    OUTPUT_BUFFER_SIZE = ConfigOption(
        "pipeline.output-buffer.size", 1000, int, description="maximum output buffer size for each task."
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
        description="The number of batches in upstream operator to reduce the number of drop requests.",
    )

    # Columnar passthrough: keep a batched operator's output column-oriented all
    # the way to the next operator instead of exploding it into per-row records
    # (operator.collect) and re-columnarising downstream (InputBatcher). Removes
    # the column->row->column copy at every batched hop. Key/custom-partitioned
    # edges stay correct by slicing the columnar batch per target by row key.
    # Default OFF: the explode/re-accumulate path is the verified failover
    # behaviour; this opt-in changes the on-wire record shape (Record.num_rows).
    COLUMNAR_PASSTHROUGH_ENABLED = ConfigOption(
        "pipeline.columnar-passthrough.enabled",
        False,
        bool,
        description="Pass batched operator output downstream column-oriented (no per-row "
        "explode + re-columnarise). Off = row-oriented wire format.",
    )

    # --- Single-fault at-least-once replay buffer (data-plane watermark) ---
    # Each OutputCollector retains records it has already put() downstream until
    # the downstream confirms (via the put() return watermark) that it has in
    # turn forwarded their derived output onward — i.e. the records exist on two
    # nodes. On a downstream single-task crash the upstream replays its buffer to
    # the rebuilt task, giving at-least-once without a full-job restart. The
    # buffer is bounded by backpressure (a full downstream inbox blocks put, which
    # propagates upstream to the source) plus the watermark-flush cadence — NOT by
    # a record cap: a slow downstream must never trigger failover, and the only
    # way the buffer grows unbounded is a watermark that never advances (a bug),
    # which surfaces as OOM -> Ray rebuild, the same recovery path as any crash.
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

    # Soft byte cap for the replay buffer. The buffer's current/high-water byte
    # footprint is always exported as a gauge; this option additionally PACES
    # emission once the retained bytes exceed the cap. Pacing only ever slows the
    # producer (a bounded sleep AFTER a batch has landed), never blocks the put
    # itself — the put carries back the forwarded-watermark that trims the buffer,
    # so blocking puts would starve the very signal that drains it. The pacing
    # propagates upstream through the bounded emit-queue -> inbox -> source, the
    # same backpressure path a full downstream inbox uses. ``0`` keeps the
    # unbounded behaviour (gauge only, no pacing).
    REPLAY_BUFFER_MAX_BYTES = ConfigOption(
        "pipeline.replay-buffer.max-bytes",
        0,
        int,
        description="Soft byte cap on retained replay-buffer records. Above this the "
        "producer is paced (never blocked) so backpressure flows to the "
        "source. 0 = unbounded (gauge exported, no pacing).",
    )
