---
myst:
  html_meta:
    description: "Review reproducible Klein performance, scalability, reliability, SQL, AI UDF, and multimodal evaluations."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

# Evaluation

This section records measured Klein behavior against an explicit baseline.
It is separate from [Performance tuning](../performance-tuning.md): tuning
explains what users can change, while an evaluation report states what was
measured, what changed, and whether the evidence supports shipping the change.

```{toctree}
:maxdepth: 2

arrow-first
```

## Reporting contract

Every evaluation report must include:

- the baseline revision or configuration and the candidate under test;
- hardware, Python, Ray, PyArrow, and relevant native-library versions;
- workload shape, input size, topology, concurrency, and non-default options;
- warm-up policy, sample count, aggregation method, and measured units;
- correctness checks performed before accepting performance numbers;
- neutral and negative results alongside improvements;
- limits on what the experiment can establish; and
- the resulting engineering decision and unresolved follow-up work.

Changing the execution plan, operator topology, data shape, or resource budget
creates a different baseline. Reports must not combine those results into one
speedup number.

## Result interpretation

| Classification | Meaning |
| --- | --- |
| Improved | The candidate is consistently better for the stated metric and workload. |
| Neutral | The observed difference overlaps run-to-run variation or is too small to support a directional claim. |
| Regressed | The candidate is consistently worse for the stated metric and workload. |

A report with only a few repetitions is an engineering comparison, not a
statistical performance guarantee. Release gates should use longer runs,
tail-latency percentiles, peak memory, and representative cluster traffic.

## Planned coverage

Future reports belong in this section and should cover distinct concerns:

- data-plane serialization, routing, backpressure, and Object Store behavior;
- batch and continuous SQL at increasing scale and skew;
- scalar, vectorized, asynchronous, and AI UDF execution;
- image, PDF, audio, video, and mixed multimodal pipelines;
- checkpoint, replay, recovery, and rescaling overhead; and
- multi-node scalability, resource efficiency, and cost.
