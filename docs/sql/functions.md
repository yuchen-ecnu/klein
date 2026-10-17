<!-- SPDX-License-Identifier: Apache-2.0 -->

# SQL functions

Use built-in functions directly in a query, register Python scalar functions
for row-level logic, or register a batched backend for SQL AI functions. The
registration examples build on the `pipeline` from
[SQL getting started](getting-started.md). Define their input table first:

```python
inputs = pipeline.from_items([{"id": 1, "prompt": "  Hello Klein  "}])
```

## Use common SQL built-ins

Klein provides a portable set of common scalar functions through the shared
batch and streaming expression evaluator. Use them directly in SQL; no Python
registration is required.

| Category | Functions and expressions |
|---|---|
| Text | `LOWER`, `UPPER`, `LENGTH`, `TRIM`, `CONCAT`, `CONCAT_WS`, `SUBSTRING`, `REPLACE`, `CONTAINS`, `STARTSWITH`, `ENDSWITH`, `LIKE`, `ILIKE` |
| Conditional and null | `COALESCE`, `NULLIF`, `IF`, `CASE`, `GREATEST`, `LEAST` |
| Numeric | `ABS`, `ACOS`, `ASIN`, `ATAN`, `CEIL`, `COS`, `EXP`, `FLOOR`, `LN`, `LOG`, `POWER`, `ROUND`, `SIGN`, `SIN`, `SQRT`, `TAN`, `TRUNC` |
| Date | `TO_DATE`, `YEAR`, `MONTH`, `DAY`, `DATE_ADD`, `DATE_SUB`, `DATEDIFF` |
| JSON and collections | `PARSE_JSON`, `GET_JSON_OBJECT`, `TO_JSON`, `ARRAY`, `MAP`, `ARRAY_SIZE` |
| Hash and encoding | `MD5`, `SHA2`, `BASE64`, `UNBASE64` |

These functions follow SQL `NULL` propagation unless their purpose defines
otherwise; for example, `COALESCE` selects the first non-`NULL` value and
`CONCAT_WS` skips `NULL` inputs. Date parsing currently accepts ISO values,
date arithmetic uses day offsets, and `DATEDIFF` returns whole days. `SHA2`
accepts bit lengths `0`, `224`, `256`, `384`, and `512`, where `0` means
SHA-256.

## Process images and PDFs

Install `ray-klein[media]` on every worker, then use the built-in media
functions directly on `BINARY` columns. They are fused into one batched
operator per projection. A worker prefers native libvips processing, including
its SIMD and threaded codec paths, and falls back to Pillow when libvips cannot
load or save a format. Repeated functions over the same value in one row share
the decoded image or PDF handle. Native threads match the SQL operator's integer
CPU reservation (at least one); a preconfigured `VIPS_CONCURRENCY` environment
variable takes precedence.

| Function | Result |
|---|---|
| `IMAGE_WIDTH(data)`, `IMAGE_HEIGHT(data)` | Oriented image dimensions as integers |
| `IMAGE_FORMAT(data)` | Canonical input format name |
| `IMAGE_RESIZE(data, width, height [, fit [, format [, quality]]])` | Encoded image bytes; defaults are `contain`, `PNG`, and quality `85` |
| `PDF_PAGE_COUNT(data)` | Number of pages |
| `PDF_SPLIT(data [, start_page [, end_page]])` | One single-page PDF per page as `ARRAY<BINARY>` |
| `PDF_RENDER_PAGE(data, page [, dpi])` | One page as PNG bytes; DPI defaults to `144` |
| `PDF_TO_IMAGES(data [, dpi [, start_page [, end_page]]])` | An inclusive page range as `ARRAY<BINARY>` PNG images |

PDF page numbers and ranges are 1-based. `contain` preserves aspect ratio
inside the requested box, `cover` fills and center-crops it, and `stretch`
returns the exact dimensions. Supported resize outputs are PNG, JPEG, WebP,
TIFF, GIF, BMP, AVIF, HEIF, JPEG 2000, JPEG XL, ICO, and PPM; less common
codecs remain dependent on the libvips or Pillow build installed on the worker.
Inputs can use any image loader exposed by either backend, so formats such as
SVG and additional camera or scientific formats can be enabled by a custom
libvips build. Multi-frame inputs currently operate on the first frame.

`DOWNLOAD(uri_column)` can be the direct data argument of a media function, so
fetch, decode, resize, and AI consumption remain in the distributed plan:

```sql
SELECT id,
       IMAGE_RESIZE(DOWNLOAD(uri), 1024, 1024, 'contain', 'WEBP', 85) AS image,
       AI_GENERATE(PDF_RENDER_PAGE(DOWNLOAD(pdf_uri), 1, 144)) AS summary
FROM assets
```

SQL `NULL` inputs produce `NULL` without invoking a decoder. Invalid media
fails the task without including payload data in the error. Per-call limits
bound input and output bytes, decoded pixels, PDF pages, and render DPI; the
defaults are 256 MiB, 64 megapixels, 100 selected pages, and 600 DPI. One fused
media batch processes one row at a time and has a 512 MiB cumulative
binary-value budget.

## Register Python scalar functions

Klein SQL scalar functions use ordinary Python values and do not expose a Ray
Data expression contract. Register a function, then call it from SQL with an
unquoted, case-insensitive name:

```python
def normalize_prompt(value, prefix):
    if value is None:
        return None
    return prefix + value.strip().lower()


pipeline.sql_session.register_scalar_function(
    "normalize_prompt",
    normalize_prompt,
)
prepared = pipeline.sql(
    "SELECT id, NORMALIZE_PROMPT(prompt, 'query: ') AS prompt FROM inputs",
    tables={"inputs": inputs},
)
```

For a one-off query, pass query-local bindings. They override
same-named session functions without changing the session:

```python
prepared = pipeline.sql(
    "SELECT NORMALIZE_PROMPT(prompt, 'query: ') AS prompt FROM inputs",
    tables={"inputs": inputs},
    functions={"normalize_prompt": normalize_prompt},
)
```

Arguments are evaluated with SQL semantics and passed as Python scalars. SQL
`NULL` is passed as `None`; the function decides whether to propagate or replace
it. Functions work in projections, predicates, grouping expressions, and
built-in aggregate inputs. Registration validates the name and callable;
query planning validates call arity and rejects unknown functions before workers
run. Built-in SQL/Klein function names cannot be replaced.

The function and its closure are serialized with the query graph. Install its
dependencies on every worker and treat it as trusted application code. Scalar
functions are synchronous, share the enclosing SQL operator's resources, and
run row by row. For model initialization, GPU allocation, vectorized inference,
or external-service concurrency, use the batched SQL AI functions below (or
`map_batches()` before SQL). Aggregate UDFs are not supported. In streaming
changelog or recoverable queries, functions must also be deterministic and
side-effect-free: the same arguments must always produce the same result so
retractions remain correct, and retries may invoke a function more than once.
Materialize nondeterministic or external results before ordinary SQL operators.

## Register batched AI functions

`AI_GENERATE` and `AI_EMBED` are first-class SQL expressions with a
provider-neutral execution contract. Klein does not choose a model provider or
own its SDK and credentials. Register trusted application code as the backend;
a callable class is initialized once per worker, which is the appropriate place
to construct a local model or provider client. In the example below, replace
`load_model_or_client()` with your application's model or client loader.

```python
class GenerateBackend:
    def __init__(self, model_name):
        self.model = load_model_or_client(model_name)

    def __call__(self, calls):
        # Each item is the argument tuple from one non-NULL SQL call.
        prompts = [prompt for (prompt,) in calls]
        return self.model.generate(prompts)


pipeline.sql_session.register_ai_function(
    "AI_GENERATE",
    GenerateBackend,
    fn_constructor_kwargs={"model_name": "my-model"},
    batch_size=32,
    concurrency=4,
    num_gpus=1,
)

generated = pipeline.sql(
    "SELECT id, AI_GENERATE(prompt) AS answer FROM inputs",
    tables={"inputs": inputs},
)
```

Register `AI_EMBED` the same way; its result for each call can be a vector. A
backend receives `calls`, a sequence of one- or two-element argument tuples,
and must return a result sequence of exactly the same length and order. The
first argument is the input. An optional second scalar argument carries
provider-specific configuration without Klein interpreting it. If any argument
for a row is SQL `NULL`, Klein produces `NULL` for that row and does not include
the call in the backend batch.

Use `batch_size` and, for streaming, `batch_timeout` to control batching. Use
`concurrency`, `num_cpus`, and `num_gpus` to control worker concurrency and
resources. Keep secrets in the worker environment or its secret-management
facility rather than SQL text. The backend and its constructor arguments are
serialized with the query graph and must be available on every worker. A batch
may be retried, so backends must tolerate duplicate calls and account for
provider idempotency and cost.

Synchronous backends work for bounded batch and streaming queries. An
`async def` backend is supported only in streaming mode; `async_buffer_size`
controls its in-flight batch buffer. AI calls currently have these planner
boundaries:

- The call must be a top-level `SELECT` expression, optionally with an alias;
  it cannot be nested in another expression, used in a predicate, or used by an
  aggregate query.
- Arguments cannot contain `*`, a qualified wildcard such as `inputs.*`, or a
  wildcard nested inside another expression such as `MAP(*)`.
- Streaming input must be insert-only. Updating/retracting changelog streams
  are rejected before execution.
- `AI_CLASSIFY`, `AI_SIMILARITY`, `AI_AGG`, and `AI_SUMMARIZE_AGG` are reserved
  for later versions and currently raise a planning error.

Registrations are session-local. Register and query through the same explicit
pipeline. Compatibility code can instead use the module-level
`register_ai_function()` and `sql()` functions on the same current context.

For `DOWNLOAD` behavior and query restrictions, see
[SQL execution and support](execution.md).
