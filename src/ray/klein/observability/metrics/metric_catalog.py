# SPDX-License-Identifier: Apache-2.0
"""Canonical catalogue of Klein runtime metrics.

Keep definitions here and instrumentation at the component boundary. This makes
names, units and histogram buckets reviewable without searching the data path.
"""

from ray.klein.observability.metrics.metric_spec import MetricKind, MetricSpec

_LATENCY_MS = (1, 2, 5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 30_000, 60_000)
_BATCH_RECORDS = (1, 2, 5, 10, 20, 50, 100, 200, 500, 1_000, 5_000, 10_000)
_BATCH_BYTES = (1_024, 4_096, 16_384, 65_536, 262_144, 1_048_576, 4_194_304, 16_777_216, 67_108_864)


class KleinMetrics:
    """Built-in metric contracts, grouped by runtime concern."""

    # Operator throughput and execution.
    RECORDS_IN = MetricSpec("records_in", MetricKind.COUNTER, "Rows received by the operator.")
    RECORDS_OUT = MetricSpec("records_out", MetricKind.COUNTER, "Rows emitted by the operator.")
    BYTES_IN = MetricSpec("bytes_in", MetricKind.COUNTER, "Estimated logical payload bytes received by the operator.")
    BYTES_OUT = MetricSpec("bytes_out", MetricKind.COUNTER, "Estimated logical payload bytes emitted by the operator.")
    PROCESSING_DURATION_MS = MetricSpec(
        "processing_duration_ms",
        MetricKind.HISTOGRAM,
        "Operator invocation duration in milliseconds.",
        _LATENCY_MS,
    )
    UDF_EXCEPTIONS = MetricSpec(
        "udf_exceptions", MetricKind.COUNTER, "UDF exceptions ignored by the configured error policy."
    )
    FILTER_RECORDS_IN = MetricSpec("filter_records_in", MetricKind.COUNTER, "Rows evaluated by a filter operator.")
    FILTER_RECORDS_DROPPED = MetricSpec(
        "filter_records_dropped", MetricKind.COUNTER, "Rows rejected by a filter operator."
    )
    LATE_RECORDS_DROPPED = MetricSpec(
        "late_records_dropped", MetricKind.COUNTER, "Late rows dropped after the event-time cleanup boundary."
    )
    TIMERS_FIRED = MetricSpec("timers_fired", MetricKind.COUNTER, "Managed state timers fired by the operator.")
    TTL_ENTRIES_CLEANED = MetricSpec(
        "ttl_entries_cleaned", MetricKind.COUNTER, "Expired managed-state entries removed by TTL cleanup."
    )
    MANAGED_STATE_SIZE_BYTES = MetricSpec(
        "managed_state_size_bytes", MetricKind.GAUGE, "Serialized managed-state snapshot size in bytes."
    )
    STATE_SNAPSHOT_DURATION_MS = MetricSpec(
        "state_snapshot_duration_ms",
        MetricKind.HISTOGRAM,
        "Managed-state serialization duration in milliseconds.",
        _LATENCY_MS,
    )
    STATE_RESTORE_DURATION_MS = MetricSpec(
        "state_restore_duration_ms",
        MetricKind.HISTOGRAM,
        "Managed-state restore duration in milliseconds.",
        _LATENCY_MS,
    )

    # Task data plane, barriers and event time.
    INPUT_BUFFER_RECORDS = MetricSpec(
        "input_buffer_records", MetricKind.GAUGE, "Logical rows currently queued in the task input buffer."
    )
    INPUT_BUFFER_CAPACITY_RECORDS = MetricSpec(
        "input_buffer_capacity_records", MetricKind.GAUGE, "Configured task input-buffer capacity."
    )
    INPUT_BUFFER_UTILIZATION = MetricSpec(
        "input_buffer_utilization", MetricKind.GAUGE, "Task input-buffer utilization as a ratio from 0 to 1."
    )
    INPUT_BUFFER_BYTES = MetricSpec(
        "input_buffer_bytes", MetricKind.GAUGE, "Estimated payload bytes currently queued in the task input buffer."
    )
    INPUT_BUFFER_CAPACITY_BYTES = MetricSpec(
        "input_buffer_capacity_bytes", MetricKind.GAUGE, "Configured task input-buffer byte capacity."
    )
    INPUT_BUFFER_BYTE_UTILIZATION = MetricSpec(
        "input_buffer_byte_utilization",
        MetricKind.GAUGE,
        "Task input-buffer byte utilization as a ratio from 0 to 1.",
    )
    EMIT_QUEUE_BATCHES = MetricSpec(
        "emit_queue_batches", MetricKind.GAUGE, "Detached output command batches waiting for transport."
    )
    EMIT_QUEUE_CAPACITY_BATCHES = MetricSpec(
        "emit_queue_capacity_batches", MetricKind.GAUGE, "Configured emit-queue batch capacity."
    )
    TRANSPORT_REQUESTS = MetricSpec("transport_requests", MetricKind.COUNTER, "Downstream data admission RPC attempts.")
    TRANSPORT_BATCH_ROWS = MetricSpec(
        "transport_batch_rows", MetricKind.HISTOGRAM, "Logical rows per accepted transport batch.", _BATCH_RECORDS
    )
    TRANSPORT_BATCH_BYTES = MetricSpec(
        "transport_batch_bytes", MetricKind.HISTOGRAM, "Estimated bytes per accepted transport batch.", _BATCH_BYTES
    )
    TRANSPORT_SEND_DURATION_MS = MetricSpec(
        "transport_send_duration_ms",
        MetricKind.HISTOGRAM,
        "Downstream data admission RPC duration in milliseconds.",
        _LATENCY_MS,
    )
    TRANSPORT_INFLIGHT_REQUESTS = MetricSpec(
        "transport_inflight_requests", MetricKind.GAUGE, "Downstream data admission RPCs currently in flight."
    )
    BARRIERS_IN = MetricSpec("barriers_in", MetricKind.COUNTER, "Checkpoint barriers received by the task.")
    BARRIERS_OUT = MetricSpec("barriers_out", MetricKind.COUNTER, "Aligned checkpoint barriers emitted by the task.")
    CHECKPOINT_ALIGNMENT_DURATION_MS = MetricSpec(
        "checkpoint_alignment_duration_ms",
        MetricKind.HISTOGRAM,
        "Time from the first barrier copy to full input alignment in milliseconds.",
        _LATENCY_MS,
    )
    CHECKPOINT_BARRIER_LATENCY_MS = MetricSpec(
        "checkpoint_barrier_latency_ms",
        MetricKind.HISTOGRAM,
        "End-to-end checkpoint barrier latency from source emission in milliseconds.",
        _LATENCY_MS,
    )
    CURRENT_INPUT_WATERMARK_MS = MetricSpec(
        "current_input_watermark_ms",
        MetricKind.GAUGE,
        "Current minimum event-time input watermark in epoch milliseconds.",
    )
    CURRENT_OUTPUT_WATERMARK_MS = MetricSpec(
        "current_output_watermark_ms", MetricKind.GAUGE, "Current emitted event-time watermark in epoch milliseconds."
    )
    WATERMARK_LAG_MS = MetricSpec(
        "watermark_lag_ms", MetricKind.GAUGE, "Wall-clock lag behind the emitted event-time watermark in milliseconds."
    )
    IDLE_INPUTS = MetricSpec("idle_inputs", MetricKind.GAUGE, "Physical task inputs currently marked idle.")
    BACKPRESSURE_EVENTS = MetricSpec(
        "backpressure_events", MetricKind.COUNTER, "Downstream sends that encountered backpressure."
    )
    BACKPRESSURE_DURATION_MS = MetricSpec(
        "backpressure_duration_ms",
        MetricKind.HISTOGRAM,
        "Time blocked by downstream backpressure in milliseconds.",
        _LATENCY_MS,
    )
    REPLAY_BUFFER_RECORDS = MetricSpec(
        "replay_buffer_records", MetricKind.GAUGE, "Unacknowledged records retained for replay."
    )
    REPLAY_BUFFER_BYTES = MetricSpec(
        "replay_buffer_bytes", MetricKind.GAUGE, "Estimated bytes retained for downstream replay."
    )
    STATE_OBJECT_STORE_WRITES = MetricSpec(
        "state_object_store_writes", MetricKind.COUNTER, "Managed-state snapshots placed in the Ray Object Store."
    )
    STATE_OBJECT_STORE_RESTORES = MetricSpec(
        "state_object_store_restores",
        MetricKind.COUNTER,
        "Managed-state restores served by hot Object Store references.",
    )
    STATE_DURABLE_RESTORE_FALLBACKS = MetricSpec(
        "state_durable_restore_fallbacks",
        MetricKind.COUNTER,
        "Managed-state restores that fell back from Object Store to durable checkpoint storage.",
    )
    STATE_OBJECT_STORE_BYTES = MetricSpec(
        "state_object_store_bytes", MetricKind.GAUGE, "Latest managed-state snapshot bytes placed in the Object Store."
    )

    # Job/checkpoint control plane.
    JOB_RESTARTS = MetricSpec("restarts", MetricKind.COUNTER, "Job failover or restart attempts.")
    CHECKPOINTS_TRIGGERED = MetricSpec(
        "checkpoints_triggered", MetricKind.COUNTER, "Checkpoints successfully allocated by the coordinator."
    )
    CHECKPOINTS_COMPLETED = MetricSpec(
        "checkpoints_completed", MetricKind.COUNTER, "Checkpoints completed by all committers."
    )
    CHECKPOINTS_FAILED = MetricSpec(
        "checkpoints_failed", MetricKind.COUNTER, "Checkpoints failed or expired before completion."
    )
    CHECKPOINT_PERSIST_FAILURES = MetricSpec(
        "checkpoint_persist_failures", MetricKind.COUNTER, "Checkpoint metadata persistence attempts that failed."
    )
    CHECKPOINTS_IN_PROGRESS = MetricSpec(
        "checkpoints_in_progress", MetricKind.GAUGE, "Checkpoints currently awaiting completion."
    )
    LAST_COMPLETED_CHECKPOINT_ID = MetricSpec(
        "last_completed_checkpoint_id", MetricKind.GAUGE, "Identifier of the latest completed checkpoint."
    )
    LAST_PERSISTED_CHECKPOINT_REVISION = MetricSpec(
        "last_persisted_checkpoint_revision",
        MetricKind.GAUGE,
        "Metadata revision of the latest durably persisted checkpoint.",
    )
    CHECKPOINT_DURATION_MS = MetricSpec(
        "checkpoint_duration_ms", MetricKind.HISTOGRAM, "Checkpoint completion duration in milliseconds.", _LATENCY_MS
    )
    CHECKPOINT_PERSIST_DURATION_MS = MetricSpec(
        "checkpoint_persist_duration_ms",
        MetricKind.HISTOGRAM,
        "Checkpoint persistence duration in milliseconds.",
        _LATENCY_MS,
    )
    CHECKPOINT_STATE_SIZE_BYTES = MetricSpec(
        "checkpoint_state_size_bytes", MetricKind.GAUGE, "Serialized size of the latest completed checkpoint state."
    )
    SINK_TRANSACTIONS_PENDING = MetricSpec(
        "sink_transactions_pending",
        MetricKind.GAUGE,
        "Prepared sink transactions awaiting durable commit or abort.",
    )
    SINK_TRANSACTIONS_COMMITTED = MetricSpec(
        "sink_transactions_committed",
        MetricKind.COUNTER,
        "Two-phase sink transactions committed after checkpoint durability.",
    )
    SINK_TRANSACTION_COMMIT_FAILURES = MetricSpec(
        "sink_transaction_commit_failures",
        MetricKind.COUNTER,
        "Two-phase sink transaction commit attempts that failed.",
    )
    SINK_TRANSACTION_COMMIT_DURATION_MS = MetricSpec(
        "sink_transaction_commit_duration_ms",
        MetricKind.HISTOGRAM,
        "Two-phase sink transaction commit duration in milliseconds.",
        _LATENCY_MS,
    )

    # Built-in integrations.
    SERVE_REQUEST_DURATION_MS = MetricSpec(
        "serve_request_duration_ms", MetricKind.HISTOGRAM, "Ray Serve request duration in milliseconds.", _LATENCY_MS
    )
    SERVE_REQUEST_FAILURES = MetricSpec("serve_request_failures", MetricKind.COUNTER, "Failed Ray Serve requests.")
    REDIS_FLUSH_DURATION_MS = MetricSpec(
        "redis_flush_duration_ms", MetricKind.HISTOGRAM, "Redis sink flush duration in milliseconds.", _LATENCY_MS
    )
    REDIS_FLUSH_BATCH_RECORDS = MetricSpec(
        "redis_flush_batch_records", MetricKind.HISTOGRAM, "Rows per Redis sink flush.", _BATCH_RECORDS
    )
    REDIS_LOOKUP_DURATION_MS = MetricSpec(
        "redis_lookup_duration_ms", MetricKind.HISTOGRAM, "Redis lookup duration in milliseconds.", _LATENCY_MS
    )
    REDIS_LOOKUP_BATCH_RECORDS = MetricSpec(
        "redis_lookup_batch_records", MetricKind.HISTOGRAM, "Rows per Redis lookup request.", _BATCH_RECORDS
    )
    REDIS_FAILURES = MetricSpec("redis_failures", MetricKind.COUNTER, "Failed Redis operations.")
    KAFKA_POLL_DURATION_MS = MetricSpec(
        "kafka_poll_duration_ms", MetricKind.HISTOGRAM, "Kafka consume call duration in milliseconds.", _LATENCY_MS
    )
    KAFKA_POLL_BATCH_RECORDS = MetricSpec(
        "kafka_poll_batch_records", MetricKind.HISTOGRAM, "Records returned by a non-empty Kafka poll.", _BATCH_RECORDS
    )
    KAFKA_ASSIGNED_PARTITIONS = MetricSpec(
        "kafka_assigned_partitions", MetricKind.GAUGE, "Kafka partitions currently owned by the source subtask."
    )
    KAFKA_CONSUMER_LAG_RECORDS = MetricSpec(
        "kafka_consumer_lag_records", MetricKind.GAUGE, "Aggregate Kafka high-watermark lag in records."
    )
    KAFKA_COMMITS = MetricSpec(
        "kafka_commits", MetricKind.COUNTER, "Kafka offset commits completed after durable Klein checkpoints."
    )
    KAFKA_COMMIT_DURATION_MS = MetricSpec(
        "kafka_commit_duration_ms", MetricKind.HISTOGRAM, "Kafka offset commit duration in milliseconds.", _LATENCY_MS
    )
    KAFKA_FLUSH_DURATION_MS = MetricSpec(
        "kafka_flush_duration_ms", MetricKind.HISTOGRAM, "Kafka producer flush duration in milliseconds.", _LATENCY_MS
    )
    KAFKA_ERRORS = MetricSpec(
        "kafka_errors", MetricKind.COUNTER, "Kafka discovery, consume, produce, or commit errors."
    )
    ROCKETMQ_RECEIVED_RECORDS = MetricSpec(
        "rocketmq_received_records", MetricKind.COUNTER, "RocketMQ records accepted by the source callback."
    )
    ROCKETMQ_ACKNOWLEDGED_RECORDS = MetricSpec(
        "rocketmq_acknowledged_records", MetricKind.COUNTER, "RocketMQ records acknowledged after downstream emit."
    )
    ROCKETMQ_PENDING_RECORDS = MetricSpec(
        "rocketmq_pending_records", MetricKind.GAUGE, "RocketMQ callback records waiting for downstream emit."
    )
    ROCKETMQ_ERRORS = MetricSpec(
        "rocketmq_errors", MetricKind.COUNTER, "RocketMQ callback, startup, or shutdown errors."
    )
