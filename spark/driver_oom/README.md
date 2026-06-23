# SPK-3 — Driver OOM (collecting unbounded data)

> **Break → Detect → Fix → Prove.** The driver is a **single JVM**. Pull a generated-large
> frame back to it with `.collect()` / `.toPandas()` (or broadcast something huge) and you blow
> its heap — more executors can't save you, because the bottleneck is the one process the result
> flows *through*. The lesson in one line: **never collect unbounded data to the driver.**

- **Notebook:** [`spk3_driver_oom.ipynb`](./spk3_driver_oom.ipynb)
- **Toolkit used:** `common.datagen` (`wide_rows` / `uniform_keys` / `key_dimension`), `common.profiles` (narrate the box), `common.metrics_diff` (prove it)
- **Run against:** the unified Spark server (`make up`) — open the Spark UI at http://localhost:4040 while the notebook runs. **No need for `make up-constrained`** here.
- **Time:** ~10 min. **Laptop-safe:** the failure is *contained*. `conf/spark-defaults.conf` sets `spark.driver.maxResultSize 1g`, so an oversized `.collect()` raises a **clean exception** — Spark refuses to ship a result bigger than the cap rather than letting the driver heap die and freezing the laptop. Data is generated lazily and nothing is written.

---

## 1. The scenario

A reporting job builds a wide per-event frame (dozens of columns, tens of millions of rows) and
the author finishes it the way they'd finish a 100-row sample: `df.collect()` (or
`df.toPandas()`) to "pull it into Python and look at it." On a small dev sample it worked. In
production the same line either **hangs the driver and then dies with `OutOfMemoryError`**, or —
because this repo caps result size — fails fast with:

```
Total size of serialized results of N tasks (X GB) is bigger than spark.driver.maxResultSize (1024.0 MiB)
```

Adding executors does nothing: the executors compute the data fine, but every partition's result
is shipped **back to the single driver JVM** to be assembled into one local Python object. That
assembly point is the bottleneck, and it has a fixed, small heap.

## 2. Break it

We generate a wide fact with `common.datagen.wide_rows(..., n_rows≈15M, n_cols=50)` — ~50 doubles
per row makes the *serialized* result genuinely large per row — then call `.collect()` inside a
`try/except`:

- The job runs on the cluster, but as task results stream back to the driver they exceed
  `spark.driver.maxResultSize` (1g) → Spark aborts with a clean
  `Total size of serialized results ... is bigger than spark.driver.maxResultSize` error.
- The `try/except` catches it, prints the message, and the notebook keeps running.

A **second** driver-pressure example: `df.join(F.broadcast(big_df), ...)` asks Spark to collect
the *whole* large side to the driver and ship it to every task — same root cause (big data → one
JVM), surfaced as a broadcast/`maxResultSize` failure.

> Why this is laptop-safe: the cap turns "driver heap dies, host swaps, fan screams" into a
> contained Python exception. We generate (don't store) the rows and never successfully
> materialize them locally, so memory and disk stay bounded. Runs fine on the default **tuned** box.

## 3. Detect it — read the Spark UI

Open http://localhost:4040 (see [`docs/spark-ui-guide.md`](../../docs/spark-ui-guide.md)):

| Where | What you see |
|-------|--------------|
| **Driver-side error message** | `Total size of serialized results ... is bigger than spark.driver.maxResultSize` — the definitive tell. (Without the cap this would instead be an `OutOfMemoryError` on the driver.) |
| **Jobs → Event Timeline** | the collect launches a job whose tasks complete on the cluster, then a **long driver-side gap / failure** as results are pulled back and assembled — not cluster compute. |
| **SQL / DataFrame** | the query for the `Collect`/`CollectLimit` action; the scan reads ~all rows even though "you only wanted to look at it." |
| **Executors** | look at the **driver** row's storage/heap — the result is buffered on the driver, the executors are idle by then. |
| **Environment** | confirm `spark.driver.maxResultSize` (1g) and `spark.driver.memory` — the size of the box the result must fit into. |

Per the [Spark-UI guide](../../docs/spark-ui-guide.md): *"Driver hangs or dies right after an
action; `OutOfMemoryError` on the driver → a `.collect()` / `.toPandas()` / oversized broadcast
pulling a generated-large frame to the driver."*

## 4. Diagnose

`.collect()` / `.toPandas()` send **every** partition's rows to the driver to build one local
object; `F.broadcast(df)` collects the whole side to the driver before shipping it out. The driver
is a single JVM with a fixed (here, small) heap, so the result size — not the cluster's compute
capacity — is the limit. **More/bigger executors don't help**; they can produce the rows faster but
the rows still funnel through the one driver. The fix is to *not move big data to the driver*.

## 5. Fix it — keep big work on the cluster, return only small results

| Fix | How | Why it works |
|-----|-----|--------------|
| **Aggregate on the cluster** | `df.groupBy(...).agg(...)` then `.collect()` / `.toPandas()` | the shuffle/aggregation runs distributed; only the **tiny** grouped result (a handful of rows) returns to the driver. |
| **`limit()` before collecting** | `df.limit(1000).toPandas()` | bounds the bytes shipped to the driver to a known small amount — the right way to "peek" at a sample. |
| **Write / stream instead of collect** | `df.write...` to a table (or a streaming sink) | the data never transits the driver at all; the cluster writes it in parallel. The default for any genuinely large result. |
| **Broadcast only small sides** | `F.broadcast(small_dim)` (e.g. `key_dimension`) | broadcasting a small dimension is correct and fast; broadcasting a large frame is the bug. |

## 6. Prove it

`common.metrics_diff.compare([...])` puts the failing approach next to the fixed ones. Expected shape:

| Metric | collect (huge, **fails**) | aggregate→collect | limit→pandas | broadcast small dim |
|--------|--------------------------:|------------------:|-------------:|--------------------:|
| Wall-clock runtime | (errors after shipping) | fast | fast | fast |
| Result returned to driver | **> 1 GB → blocked** | a few rows | ≤ limit rows | n/a (small side only) |
| Outcome | `maxResultSize` exception | tiny result | tiny result | tiny result |

The proof is qualitative *and* quantitative: the broken cell raises the `maxResultSize` error
(caught, printed), while each fix returns a small result quickly. The contrast — **unbounded
collect vs bounded result** — is the whole lesson.

## 7. Takeaways & "in real production…"

- **Never collect unbounded data to the driver.** `.collect()` / `.toPandas()` are for *small*,
  already-aggregated or `limit`ed results — treat them like `head()`, not like an export.
- **Aggregate or write on the cluster**; bring back only what a human/dashboard can read.
- **Broadcast only genuinely small sides.** A broadcast of a large frame is a driver-OOM in
  disguise (it collects that side to the driver first).
- **The cap is a guardrail, not a fix.** `spark.driver.maxResultSize` turns a driver heap death
  into a clean failure (and a smooth laptop) — but the real fix is to stop moving big data to the
  driver. In production, set the cap deliberately, alert on driver memory, and code review for
  stray `.collect()` / `.toPandas()` on unbounded frames.

## 8. Teardown

Nothing was written (the large frame was never successfully collected, and the fixes only return
small results), so there is nothing to delete. The notebook restores the `tuned` profile at the
end. If you experimented with writes, `make clean` removes everything under `.tmp/`.
