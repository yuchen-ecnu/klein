---
myst:
  html_meta:
    description: "Before-and-after evaluation of Klein's Arrow-first inter-task data plane for SQL, batch UDF, embedding, image, and PDF workloads."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

# Arrow-first data-plane evaluation

- **Evaluation date:** 2026-07-21
- **Historical baseline:** clean checkout of `cca02fe`
- **Candidate:** the source revision containing this report, with Arrow transport enabled by default

## Executive result

The Arrow-first candidate improved measured end-to-end streaming throughput by
5.5 to 6.3 percent over the historical revision. A same-revision transport
toggle showed the largest isolated gain, 17.4 percent, for a two-stage
embedding batch-UDF pipeline. Existing Ray Data batch SQL remained neutral, and
image/PDF workloads were neutral because native decoding, resizing, and
rendering dominated their runtime.

The result supports retaining Arrow at real inter-task data boundaries. It does
not support forcing Arrow through chained in-process operators or every
row-oriented UDF boundary.

## Scope and method

The historical comparison used a clean detached checkout for `cca02fe` and the
same Python environment as the candidate. The historical revision already had
Python mapping-based columnar passthrough; it was not a pure row-only engine.
The controlled comparison ran only the candidate and changed
`pipeline.columnar-passthrough.enabled`, which isolates Arrow transport but is
not a reconstruction of the historical revision.

| Item | Setting |
| --- | --- |
| Host | 64-vCPU AMD EPYC 9K84, 247 GiB RAM |
| Runtime | Python 3.10.16, Ray 2.56.1, PyArrow 23.0.1, NumPy 2.2.6 |
| Streaming cluster | Local Ray cluster with 8 logical CPUs |
| Complex batch-SQL cluster | Local Ray cluster with 16 logical CPUs |
| End-to-end samples | One warm-up followed by three measured jobs; median reported |
| Timed region | Job submission through terminal completion; Ray cluster startup excluded |
| Data-plane stress options | Operator chaining and replay disabled; transport batches capped at 1,000 rows for SQL/UDF and 16 rows for media |

Disabling chaining deliberately exposes inter-task transport. Default plans
that chain adjacent operators have fewer transport boundaries and can show a
smaller whole-job difference. Synthetic inputs were deterministic. The
benchmarked paths first passed their correctness tests, and every measured job
had to finish successfully before its timing was accepted.

## Historical revision versus candidate

| Workload | Scale | Historical | Candidate | Throughput change |
| --- | ---: | ---: | ---: | ---: |
| Streaming scalar SQL with a row consumer | 30,000 rows | 5.673 s; 5,288 rows/s | 5.366 s; 5,591 rows/s | **+5.7%** |
| Streaming scalar SQL with a batched consumer | 30,000 rows | 5.627 s; 5,332 rows/s | 5.293 s; 5,667 rows/s | **+6.3%** |
| Two 256-dimensional embedding batch UDFs | 20,000 rows | 3.452 s; 5,794 rows/s | 3.273 s; 6,110 rows/s | **+5.5%** |
| Batch CTE, filter, join, group, sort, and limit | 33,000 input rows | 20.323 s; 1,624 rows/s | 20.545 s; 1,606 rows/s | -1.1%; neutral |

The complex batch query used 30,000 orders and 3,000 customers. Ray Data was
already Arrow-native on both revisions, and the 1.1 percent difference was
smaller than observed run-to-run variation, so it is classified as neutral.

## Controlled transport toggle

This comparison used the candidate for both sides. "Legacy wire" disabled
`pipeline.columnar-passthrough.enabled`; "Arrow wire" enabled it. It measures
the transport policy in isolation and must not be presented as the historical
commit comparison.

| Workload | Legacy wire median | Arrow wire median | Throughput change |
| --- | ---: | ---: | ---: |
| Streaming scalar SQL with a row consumer | 5.674 s | 5.409 s | **+4.9%** |
| Streaming scalar SQL with a batched consumer | 5.621 s | 5.216 s | **+7.8%** |
| Two-stage embedding batch UDF | 3.876 s | 3.303 s | **+17.4%** |
| Resize 64 random 512x512 JPEG images to 224x224 WebP | 3.573 s | 3.617 s | -1.2%; neutral |
| Render 16 four-page PDFs at 72 DPI | 2.737 s | 2.721 s | +0.6%; neutral |

The media functions did not exist in the historical baseline, so only the
same-revision transport toggle is valid for them. Their results show that Arrow
does not materially change compute-bound native media operations at this
scale; they do not establish a media speedup.

## Data-plane microbenchmarks

The production micro-benchmark is available as
`python scripts/benchmark_data_plane.py`. Its Arrow batch-builder result is the
median of 25 timed repetitions after warm-up. On the same host, building one
1,000-row Arrow wire batch took 2.819 ms, compared with 25.504 ms for converting
rows individually and concatenating them: a **9.05x** improvement in the batch
builder itself.

| Payload | Historical envelopes and serialized size | Arrow envelopes and serialized size | Size change |
| --- | ---: | ---: | ---: |
| 1,000 scalar SQL rows | 1,000; 192,300 bytes | 1; 160,303 bytes | -16.6% |
| 1,000 rows with 768-dimensional embeddings | 1,000; 3,133,364 bytes | 1; 3,080,561 bytes | -1.7% |
| 64 rows with 64-KiB media values | 64; 4,197,837 bytes | 1; 4,196,689 bytes | -0.03% |

Serialized sizes use `ray.cloudpickle` as a consistent relative measurement;
they are not claimed to be exact Ray Object Store network-byte counts. Arrow
mostly removes envelope and Python-object overhead. It cannot compress an
already encoded JPEG, WebP, or PDF payload.

## Negative result: row materialization

A local pack, serialize, deserialize, and unbatched-consume microbenchmark
exposed the principal remaining risk:

| Payload | Historical | Arrow-first | Change |
| --- | ---: | ---: | ---: |
| 1,000 scalar rows | 2.651 ms | 11.721 ms | 4.4x slower |
| 1,000 rows with 768-dimensional embeddings | 7.857 ms | 165.856 ms | 21.1x slower |

This conversion is expensive because a columnar batch is materialized back
into Python dictionaries. Remote end-to-end SQL still improved because fewer
RPC envelopes and cheaper serialization outweighed the conversion, but a
small-payload, row-only downstream operator can be a counterexample.

## Decision and follow-up

- Keep Arrow-first transport at actual inter-task boundaries.
- Keep chained in-process operators in their native representation.
- Prefer batched SQL, vectorized UDFs, and AI tensor pipelines when practical.
- Treat media transport as neutral until larger multi-node tests demonstrate a benefit.
- Add a downstream-aware policy that can bypass Arrow for known small,
  unbatched row consumers, or provide a cheaper lazy row view.
- Extend future release evaluation with multi-node network throughput, p95/p99
  latency, peak worker/Object Store memory, skew, backpressure, and recovery.

These measurements justify the current architecture, but they are not a
general performance guarantee for arbitrary clusters or user UDFs.
