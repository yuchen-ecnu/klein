---
myst:
  html_meta:
    description: "Compatibility, rehearsal, upgrade, validation, and rollback procedures for Klein for Ray jobs."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

# Upgrade Klein jobs

Upgrading a long-running Klein job means replacing its application artifact,
Klein/Ray environment, or graph while deciding whether its checkpointed source
positions, managed state, timers, and prepared sink transactions remain
compatible.

:::{danger}
Klein is currently `0.1.0.dev0` alpha software. Before 1.0, Semantic Versioning
compatibility is not promised, only the latest release is eligible for security
fixes, and checkpoint formats can change between releases. Klein currently
ships no automatic checkpoint migration tool and no user-triggered command for
creating an independently named savepoint.
:::

The supported maintenance path is a controlled stop and new submission from a
completed checkpoint. Replacing packages in live Ray actors, mixing Klein
versions within one job, or performing a rolling worker-by-worker framework
upgrade is not supported.

## Check the version boundary

The current tested compatibility target is:

- Python 3.10--3.12;
- `ray>=2.56.1,<2.57` with the `data` extra;
- `protobuf>=3.20.3,<7`;
- connector dependencies within the ranges in `pyproject.toml`.

Ray Data integration uses DeveloperAPI extension points. A newer Ray minor is
not supported merely because the package imports. Before widening the Ray
range, run the full unit, state, architecture, Ray integration, SQL, and
external-connector suites and review the private/public API inventory. See
[Compatibility](compatibility.md).

Pin exact resolved versions and the worker image for an upgrade. A dependency
upgrade can change pickle import paths, connector protocols, SQL planning, or
native-library behavior even when the Klein version does not change.

## Preserve the checkpoint contract

The current restore implementation accepts only its current versioned
checkpoint metadata and managed-state formats. It rejects an unknown format
instead of guessing a migration. Checkpoint metadata and application state use
pickle, so class/module availability is also part of compatibility.

Review every dimension below before attempting a stateful restore.

| Dimension | Compatible requirement | Unsafe or unsupported change |
| --- | --- | --- |
| Completed checkpoint | Restore from the full URI of one `chk-N` directory whose `_metadata` is readable. Preserve every referenced state object in the job prefix. | An incomplete directory, only a copied `_metadata` file, edited pickle metadata, or an object whose checksum/size no longer matches. |
| Graph identity | Rebuild streams in the same construction order and preserve sources, stateful operators, sinks, names, partition edges, and chaining-sensitive layout. After registering the terminals, save and compare `ray.klein.explain("<job-name>")` output. | Adding, removing, or reordering even a stateless stream can shift downstream numeric operator IDs. A same-numbered but different operator can receive the wrong checkpoint entry. |
| Managed state | Preserve `state.keyed.max-parallelism`, descriptor names, key type/serialization, value shape/serializer, namespaces, timer representation, and stateful operator meaning. | Changing max parallelism, renaming descriptors/classes/modules, incompatible value or key serialization, or assuming arbitrary Python objects will migrate automatically. |
| State backend | Keep `state.backend.type` and its dependency unchanged for the upgrade unless a separately tested migration is provided. Node-local state remains disposable; durable checkpoint data is authoritative. | Treating a memory-to-RocksDB or RocksDB-to-memory change as a documented migration path. Klein does not currently promise cross-backend restore. |
| Ordinary concurrency | Keyed operator concurrency may change only when it remains at or below the unchanged max parallelism and a rehearsal proves key-group redistribution. | Changing source or sink concurrency without connector-specific evidence, or changing concurrency together with unreviewed topology and state-schema changes. |
| Source state | Preserve source type, topic/table/path identity, consumer group, partition/split interpretation, start policy, source class, and checkpoint-state schema. | Switching connectors or identities and expecting old offsets to be translated. RocketMQ broker-managed progress cannot be rolled back by a Klein checkpoint. |
| Sink transactions | Preserve sink type, destination, transaction/idempotency scheme, credentials, serializer, and the code needed to deserialize and commit prepared committables. | Restoring a checkpoint into a rehearsal with access to production sinks, or changing sink semantics while a checkpoint contains prepared work. Restore may retry a durable committable. |
| Event time and SQL | Preserve timestamp units/types, watermark and idleness assumptions, state TTL meaning, changelog schema, join/window keys, and UDF semantics unless the change has an explicit data migration plan. | A change that makes existing timers/state semantically wrong even if deserialization succeeds. |
| Configuration | Use canonical keys and preserve recovery-critical values. Compare the effective configuration, not only source defaults. | Relying on an unknown or misspelled key: Klein retains unknown keys as metadata but does not apply them. |
| External effects | Preserve deduplication keys and identify effects produced between the rollback checkpoint and the cutover. | Assuming checkpoint restore reverses Kafka, SQL, Redis, RocketMQ, console, or other already-visible external writes. |

There is no general “stateless edit is safe” rule because source and sink
checkpoint keys also depend on graph identity. For a job whose old state can be
discarded, start a fresh deployment without `execution.savepoint.path` and
explicitly accept the new source/output semantics instead of pretending it is
a compatible restore.

## Prepare the upgrade

Keep the following rollback bundle before touching the running job:

- old wheel/application and immutable worker-image digest;
- complete dependency lock and Python/Ray versions;
- canonical configuration with secret references, not secret values;
- saved `ray.klein.explain("<job-name>")` output for the fully registered graph;
- job name, namespace, source identities, sink destinations, and checkpoint root;
- exact URI of a completed pre-upgrade `chk-N` directory;
- evidence that the checkpoint's referenced objects remain retained;
- duplicate reconciliation and external rollback procedures.

Inspect the running job from an operations client:

```bash
ray-klein status <namespace> --json > pre-upgrade-status.json
```

Confirm that the snapshot is not marked `dashboard_stale`, the job is running,
and a recent checkpoint completed within the RPO. Validate `_metadata` with the
object-store or filesystem tooling used by the platform. Do not select `_latest`
as the restore URI; record the resolved `chk-N` directory.

`ray-klein stop` requests cancellation. It does **not** first create a
checkpoint or savepoint. If the recorded checkpoint is too old, leave the job
running until a newer periodic checkpoint completes. At least one of
`execution.checkpointing.trigger.interval-duration` and
`execution.checkpointing.trigger.interval-records` must be nonzero for periodic
progress.

## Rehearse safely

Run the exact candidate artifact in an isolated Ray cluster before production:

1. Use the same Python/Ray versions, optional extras, graph, max parallelism,
   serializers, and state scale expected at cutover.
2. Replay representative production-shaped input into staging source
   identities and build a staging checkpoint with the current version.
3. Upgrade from that checkpoint into isolated sinks and a new checkpoint
   prefix. Do not give the rehearsal credentials for production destinations.
4. Inject worker, coordinator, driver, and sink failures during and after
   restore.
5. Compare state-derived output, source position, watermarks, late records,
   duplicate envelope, restore duration, and first checkpoint duration.
6. Stop the candidate and prove the old artifact can still restore the retained
   old-version staging checkpoint.

Do not restore an untrusted checkpoint. Also avoid using a production
checkpoint as a harmless test fixture: it contains pickle payloads and may
contain prepared sink committables that recovery can retry. Generate a
production-shaped staging checkpoint with isolated connector identities.

## Perform the cutover

### 1. Freeze the deployment definition

Prevent concurrent graph, image, configuration, schema, and connector changes.
Record the candidate digests and approved checkpoint URI.

### 2. Wait for the recovery point

Wait for a completed durable checkpoint and capture its exact `chk-N` URI.
Verify source lag, checkpoint duration, pending sink transactions, and output
health. A directory without `_metadata` is incomplete.

### 3. Stop the old job

```bash
ray-klein stop --force <old-namespace>
ray-klein list --all
```

Confirm terminal status and that old detached actors no longer serve control
requests. Stop external schedulers from resubmitting the old artifact. Do not
run old and new jobs concurrently against the same consumer identity or
non-idempotent destination.

### 4. Deploy one candidate environment

Roll out the immutable candidate to every eligible Ray node before submission.
Do not mix old and new Klein workers. If the Ray framework itself changes,
create a separately tested cluster rather than upgrading live actors in place.

### 5. Restore explicitly

Build the compatible graph and set the complete checkpoint URI:

```python
import ray
import ray.klein

ray.init(address="auto")
ray.klein.configure(
    {
        "execution.runtime.mode": "streaming",
        "execution.checkpointing.dir": "s3://platform/klein-checkpoints",
        "execution.savepoint.path": (
            "s3://platform/klein-checkpoints/"
            "orders-production/chk-42"
        ),
        "state.keyed.max-parallelism": 128,
        "job.namespace": "orders-production-v2",
    }
)

# Rebuild the reviewed graph in the same construction order.
build_pipeline()
handle = ray.klein.execute("orders")
print(handle.namespace)
handle.wait()
```

Use a fresh namespace when old actors may still exist or when a shadow cluster
must remain operationally distinct. After restore, new checkpoints are written
under the new namespace's job directory. Reusing a stable namespace can retain
the old operational identity, but only after the old actors are confirmed gone.
In both cases, `execution.savepoint.path` must identify the approved checkpoint
explicitly.

### 6. Validate before declaring success

Require all of the following:

- the job reaches and remains `RUNNING` without exhausting restart attempts;
- the graph/operator IDs and effective configuration match the reviewed plan;
- restored source positions are near the recorded checkpoint;
- keyed state, timers, SQL state, watermarks, and late-data behavior are correct;
- output has no loss outside the documented source boundary and duplicates are
  absorbed by the designed idempotency/reconciliation mechanism;
- prepared transactional output reaches the expected final state;
- restore duration is inside the RTO;
- the first post-upgrade checkpoint completes and is readable;
- checkpoint age, restart, lag, backpressure, state, replay, and sink-failure
  alerts remain healthy through a soak period.

Use `ray-klein status <new-namespace> --json`, retained logs, exported metrics,
and destination-level reconciliation. Successful submission alone is not an
upgrade success.

## Roll back

Roll back when restore fails, the job repeatedly recovers, state-derived output
is wrong, a connector cannot resume, or the candidate cannot complete a new
checkpoint.

1. Stop the candidate and disable its automatic resubmission.
2. Prevent further candidate writes or requests to external sinks.
3. Preserve its logs, status snapshot, failed checkpoint paths, and destination
   reconciliation evidence.
4. Redeploy the exact old application, worker image, dependencies, Ray/Python
   versions, graph, and configuration.
5. Restore the old artifact from the recorded **pre-upgrade** checkpoint, not a
   checkpoint written by the candidate.
6. Validate source positions, state, sink effects, and completion of a new
   old-version checkpoint.
7. Reconcile or deduplicate external effects produced since the rollback point.

A rollback rewinds Klein state; it does not undo output already visible in an
external system. Kafka, SQL, Redis, RocketMQ, console, and custom sinks can
replay or retain effects according to their documented guarantees. Filesystem,
Iceberg, and correct two-phase sinks have stronger checkpoint publication
boundaries, but destination-specific validation is still required.

Never assume an old release can read a checkpoint created by a newer alpha
release. Keep the pre-upgrade checkpoint, its referenced objects, and the old
artifact until the candidate has passed the agreed soak period and rollback
window.

## Upgrade decision matrix

| Proposed change | Default decision |
| --- | --- |
| Patch within the pinned application dependencies, no state, graph, or connector change | Rebuild and run the full relevant test tiers; still perform a normal stop/resubmit. |
| Klein release change with stateful restore | Rehearsal and explicit checkpoint compatibility proof required. No default compatibility promise before 1.0. |
| Ray 2.56 patch change inside the declared range | Test the resolved patch on every supported Python version and run Ray integration/connector suites. |
| Ray minor outside `>=2.56.1,<2.57` | Unsupported until the project widens the declared range after API review and CI. |
| Python minor change | Rebuild all native dependencies and prove pickle/state and connector compatibility in rehearsal. |
| Add/remove/reorder an operator | Treat as topology-incompatible unless an explicit migration and restore test proves all checkpoint identities and semantics. |
| Change keyed concurrency only | Permitted only at or below unchanged max parallelism, with connector constraints and restore/rescale testing. |
| Change max parallelism, state descriptor/serializer, checkpoint format, or backend type | No automatic migration. Start without old state or implement and validate a dedicated migration before cutover. |
| Change source/sink type, identity, or delivery semantics | Design a data migration and reconciliation procedure; checkpoint restore alone is insufficient. |

See [Checkpoint recovery](checkpoint-recovery.md) for restore mechanics,
[Delivery guarantees](delivery-semantics.md) for external effects,
[Security](security.md) for trusted checkpoint handling, and the
[Production readiness checklist](production-readiness.md) for the release gate.
