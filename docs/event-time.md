---
myst:
  html_meta:
    description: "Assign event timestamps and coordinate Klein for Ray watermarks across active and idle streaming inputs."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

# Event time and idle inputs

Klein carries event-time progress as ordered data-plane control messages. A
`Watermark(t)` means that one physical input does not expect another event at or
before `t`. `InputIdle` temporarily removes an input from minimum-watermark
calculation, and `InputActive` adds it back before resumed data is processed.
These messages flush preceding micro-batches and use the same physical edge
targets as checkpoint barriers, so they cannot overtake records.

For a task with multiple physical inputs:

- the output watermark is the minimum watermark across active inputs;
- an active input without its first watermark blocks progress;
- an idle input does not block progress;
- the task emits `InputIdle` only when every input is idle;
- the first resumed input emits `InputActive`, optionally with its last known
  watermark, before further progress;
- per-input and output watermarks are monotonic.

This is independent of Klein's replay-buffer acknowledgement watermark, which
tracks delivered batches rather than event time.

## Assign timestamps and watermarks

```python
from datetime import timedelta

import ray

strategy = (
    ray.klein.WatermarkStrategy.for_bounded_out_of_orderness(
        timedelta(seconds=5),
        lambda row: row["event_time_ms"],
    )
    .with_idleness(timedelta(seconds=30))
)

events = ray.klein.read_kafka(...).assign_timestamps_and_watermarks(strategy)
```

The strategy attaches the assigned timestamp to the record, emits monotonically
increasing watermarks from the largest observed timestamp minus the configured
out-of-orderness bound, marks the operator idle after the configured timeout,
and emits active before the next record. Idle strategies are checked according
to `event-time.idle-input.check-interval` (one second by default).

Stateful window and join timestamp selectors remain supported and can use the
same timestamp field. Their event-time timers fire only when the aggregate
watermark advances. A bounded source emits the maximum watermark before
`EndOfData`, closing all event-time windows in normal control-message order.

## Control event time from a source

Streaming sources emit records and event-time control through `SourceContext`:

```python
context.collect(row)
context.emit_watermark(event_time_ms)

# Mark an empty physical partition idle and check checkpoint cadence.
context.on_idle()

# Reactivate explicitly. collect() also reactivates an idle source.
context.mark_active(last_watermark)
```

Checkpoint state is source-owned and separate from records. A custom
`SourceFunction` implements `snapshot_state(checkpoint_id)`, `restore_state(state)`,
`cancel()`, and optionally `notify_checkpoint_complete(checkpoint_id)`. `cancel()`
only asks the source loop to return; release connections and other resources in
`close()`. Klein calls `close()` once after the loop has stopped. It stores the
opaque state per physical source subtask and never compares integration offsets.

`RAY_KLEIN_EVENT_TIME_IDLE_INPUT_CHECK_INTERVAL=500ms` configures the periodic
idleness check through the standard environment-variable configuration path.
