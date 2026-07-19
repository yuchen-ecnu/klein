---
myst:
  html_meta:
    description: "Run a connected Klein for Ray transformation region behind Ray Serve through an embedded proxy client."
---
<!-- SPDX-License-Identifier: Apache-2.0 -->

(klein-ray-serve-integration)=
# Ray Serve execution integration

Ray Serve is an optional execution integration, not a data source or sink. It
moves one connected transform region behind a Serve deployment and replaces
that region in the Klein graph with an asynchronous HTTP proxy client. Use it
for independently deployed model or inference transforms in batch or streaming
graphs.

Install the optional dependencies:

```bash
python -m pip install "ray-klein[serve]"
```

## Mark a Serve region

Transform methods that accept `ray_serve_enabled` can be marked in the original
workflow:

```python
served = (
    stream
    .map_batches(preprocess, ray_serve_enabled=True)
    .map_batches(predict, ray_serve_enabled=True, batch_size=16)
)
served.write_parquet("s3://warehouse/predictions/")
ray.klein.execute("served-inference").wait()
```

All marked nodes in a job must form exactly one connected linear chain. A
branch or merge inside the marked region, or two disconnected marked regions,
is rejected during graph construction. External fan-in or fan-out is rewired
to the single proxy boundary. Functions, constructor arguments, and returned
values must remain serializable and compatible with the selected operator's
batch contract.

In a streaming graph the proxy is an ordinary transform operator, but the
remote HTTP service does not participate in Klein's checkpoint protocol.

## Deploy the operator chain

The Serve deployment imports Klein's bound application and receives the same
workflow path as user configuration:

```yaml
import_path: ray.klein.runtime.serve:app
deployments:
  - name: KleinServeDeployment
    user_config:
      workflow: /workspace/workflow.py
```

The deployment runs that workflow as `__main__` and intercepts its call to
`execute()`. It extracts the marked functions without submitting the workflow's
ordinary Klein job, then executes the extracted chain for incoming batches.
The workflow file must therefore build the graph and call `execute`; keep graph
construction deterministic and avoid unrelated process-wide side effects.

The job-side client must point at an already reachable proxy and deployment:

```yaml
serve:
  proxy-endpoints: "http://serve-proxy-a:8000,http://serve-proxy-b:8000"
  deployment-name: "KleinServeDeployment"
  route-prefix: "/"
```

## Configuration reference

| Key | Default | Meaning |
|---|---:|---|
| `serve.proxy-endpoints` | `None` | Required comma-separated HTTP proxy base URLs. |
| `serve.deployment-name` | `None` | Required Serve deployment name. |
| `serve.route-prefix` | `/` | Route prefix sent through the proxy API. |
| `serve.client.num-cpus` | `1.0` | CPU allocation for a multi-node region's embedded client. |
| `serve.client.concurrency` | `1` | Embedded client parallelism. |
| `serve.client.async-buffer-size` | `100` | Maximum asynchronous buffered work. |
| `serve.client.batch-timeout` | `5` | Proxy batch timeout in seconds. |
| `serve.client.batch-size` | `2` | Proxy request batch size. |
| `serve.client.max-attempts` | `30` | Maximum HTTP request attempts. |
| `serve.client.slow-request-warning` | `600` | Seconds before one slow-request warning. |
| `serve.client.http-timeout` | `300` | Total HTTP timeout in seconds. |
| `serve.client.http-connect-timeout` | `5` | HTTP connect timeout in seconds. |
| `serve.client.http-limit-per-host` | `1000` | Per-host connection limit. |
| `serve.client.http-connection-limit` | `1000` | Total connection limit. |
| `serve.client.retry-backoff-max` | `3.0` | Maximum randomized exponential backoff; runtime-capped at 10 seconds. |

For a single marked operator, the proxy inherits that operator's CPU,
concurrency, batch size, and timeout. A multi-operator region uses the
`serve.client.*` resource and batching values. All keys also appear in the
[complete configuration reference](../configuration-reference.md), including
accepted configuration sources and precedence.

## Requests, retries, and operations

The proxy JSON-encodes NumPy batches, chooses among configured endpoints, and
reuses one request ID across retries. Connection errors, timeouts, HTTP 429,
and HTTP 499 are retryable; most other 4xx responses stop retrying. Because a
remote deployment can finish a request before the client observes a failure,
functions should be deterministic and free of non-idempotent side effects.

The deployment can optionally validate the `rayservice` request header when
the `RAY_SERVICE_NAME` environment variable is set. Monitor Serve request
duration and failure metrics through [Observability](../observability.md), and
protect the proxy endpoint with the authentication and network policy expected
by your environment.
