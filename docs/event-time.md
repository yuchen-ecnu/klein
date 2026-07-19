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

The timestamp assigner must return a non-negative integer in milliseconds.
Boolean, floating-point, negative, and second-based values are rejected or
produce the wrong window scale. With bounded out-of-orderness `d`, the candidate
watermark is `max_timestamp_seen - d`; Klein emits it only when it is
non-negative and greater than the previous watermark.

| Strategy | Use when | Watermark behavior |
|---|---|---|
| `for_monotonous_timestamps(assigner)` | Each physical input's timestamps never decrease. | Tracks the maximum observed timestamp directly. |
| `for_bounded_out_of_orderness(delay, assigner)` | Records may arrive late within a known bound. | Subtracts the bound from the maximum observed timestamp. |
| `.with_idleness(timeout)` | A physical input can stay empty while others continue. | Marks that input idle after the timeout and active before its next record. |

Apply one strategy after decoding the field that owns event time and before a
stateful window or join. A later transform preserves the attached record
timestamp, but an explicit timestamp selector on the stateful operator remains
the authoritative timestamp for that operator.

Stateful window and join timestamp selectors remain supported and can use the
same timestamp field. Their event-time timers fire only when the aggregate
watermark advances. A bounded source emits the maximum watermark before
`EndOfData`, closing all event-time windows in normal control-message order.

## Windows, allowed lateness, and late records

Klein windows use half-open intervals `[start, end)`:

- a tumbling window assigns each record to exactly one fixed-size interval;
- a sliding window assigns it to every overlapping `size` interval starting at
  the configured `slide` cadence;
- a session window creates `[timestamp, timestamp + gap)` and merges overlapping
  windows for the same key.

`allowed_lateness` delays cleanup and final emission beyond the window end. A
record is dropped when its window cleanup timestamp is already at or behind the
current watermark. Increasing allowed lateness retains state longer; it does
not rewind a watermark.

```python
from datetime import timedelta
from ray.klein import SlidingWindow

rolling = (
    events.key_by(lambda row: row["customer_id"])
    .window(
        SlidingWindow(
            size=timedelta(minutes=10),
            slide=timedelta(minutes=1),
        ),
        timestamp_selector=lambda row: row["event_time_ms"],
        allowed_lateness=timedelta(seconds=30),
        state_ttl=timedelta(hours=1),
    )
    .reduce(lambda left, right: {**right, "amount": left["amount"] + right["amount"]})
)
```

The window `state_ttl` is a processing-time safety bound. Set it longer than
the maximum expected window lifetime plus lateness; premature TTL expiry can
make an emitted result incomplete.

Interval joins apply the same lateness principle independently to their left
and right buffered records. A left record at time `t` can match right records
whose timestamp difference falls between `lower_bound` and `upper_bound`.
Watermarks remove records once no future on-time match is possible.

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
