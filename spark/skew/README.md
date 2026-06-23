# SPK-1 ⭐ — Data / Partition Skew (flagship module)

> **Break → Detect → Fix → Prove.** The single most common Spark performance failure:
> one key holds most of the rows, so one task does most of the work while the rest sit idle.
> This is the reference module that proves the whole curriculum framework
> ([`common/`](../../common/) toolkit + [Spark-UI guide](../../docs/spark-ui-guide.md) + before/after metrics).

- **Notebook:** [`spk1_data_skew.ipynb`](./spk1_data_skew.ipynb)
- **Toolkit used:** `common.datagen` (skew knob), `common.profiles` (force/relieve the pathology), `common.metrics_diff` (prove it)
- **Run against:** the unified Spark server (`make up`) — open the Spark UI at http://localhost:4040 while the notebook runs.
- **Time:** ~10 min. **Laptop-safe:** data is generated lazily and only counted (never collected or written), so nothing fills disk; there is nothing to delete at the end.

---

## 1. The scenario

A nightly job aggregates revenue by joining a large `orders` fact onto a `customers`
dimension on `customer_id`. It *usually* finishes in a couple of minutes — but some nights it
crawls for 40 minutes and occasionally dies. The data volume barely changed. What's going on?

One customer — a marketplace "house account" (`customer_id = 0`) — accounts for **90% of all
orders**. When Spark shuffles `orders` by `customer_id` for the join, every row for that one
customer lands in the **same** reduce partition. One task gets 90% of the data; the other
tasks finish in milliseconds and wait. That single straggler *is* the job.

## 2. Break it

We generate a skewed fact with `common.datagen.skewed_keys(..., hot_key_fraction=0.9)` and join
it to a dimension, under the **`constrained` session profile** (`common.profiles.apply_profile`):

- **AQE off** — no runtime skew rescue, so we see the raw pathology.
- **Broadcast off** (`autoBroadcastJoinThreshold = -1`) — forces a **sort-merge join** that
  shuffles both sides by key (instead of broadcasting the small dimension and side-stepping skew).

The join + `count()` runs, and one task takes far longer than the rest.

> Why this is laptop-safe: 20M rows are *generated*, not stored; we only `count()`, so the
> driver never collects a large result. Skew is task-time imbalance, not a memory problem — so
> this module runs fine on the default **tuned** box; you do **not** need `make up-constrained`.

## 3. Detect it — read the Spark UI

Open http://localhost:4040 → **SQL / DataFrame** tab → click the running/last query → scroll to
the join's reduce **Stage** → **Tasks** → **Summary Metrics**. The tell (see
[`docs/spark-ui-guide.md`](../../docs/spark-ui-guide.md)):

| Signal | Skewed (broken) | Healthy |
|--------|-----------------|---------|
| **Task time: Max vs Median** (Duration / executorRunTime) | **Max ≫ 75th ≫ Median** (e.g. 40× the median) | clustered near the median |
| **Shuffle Read** per task | one task reads ~90% of the bytes | even across tasks |
| **Event Timeline** | one long bar; the rest finished long ago | bars roughly equal length |
| (often) **Spill** | the straggler spills memory→disk while sorting its giant partition | none |

`common.metrics_diff.measure()` captures this quantitatively as **`skew_ratio = max ÷ median`** —
the headline number this module drives down.

## 4. Diagnose

A hash shuffle sends all rows with the same key to the same partition. With one dominant key,
that partition is enormous and its task becomes a straggler. More executors/CPU won't help — the
work can't be split across tasks because it's pinned to one key's partition.

## 5. Fix it — three production remedies

| Fix | How | When to reach for it |
|-----|-----|----------------------|
| **Broadcast the small side** | `df.join(F.broadcast(dim), ...)` (or raise `autoBroadcastJoinThreshold`) | One side is small enough to replicate → no shuffle at all → skew becomes irrelevant. **Try this first.** |
| **AQE skew-join** | `adaptive.enabled=true` + `adaptive.skewJoin.enabled=true`; Spark splits the skewed partition at runtime | Two large tables that must shuffle-join. On at small scale you must lower `skewedPartitionThresholdInBytes` (defaults assume huge data). |
| **Salting** | add a random `salt` to the hot key on the fact side; replicate the dimension across all salts; join on `(key, salt)` | When you can't broadcast and need explicit control, or on engines/versions without AQE. Spreads the hot key across N partitions. |

## 6. Prove it

`common.metrics_diff.compare([...])` prints a before/after table. Expected shape:

| Metric | skewed (SMJ) | salted | AQE skew-join | broadcast |
|--------|-------------:|-------:|--------------:|----------:|
| Wall-clock runtime | high | ↓ | ↓ | ↓↓ |
| Task time — max | **huge** | ↓ | ↓ | ↓ |
| **Skew (max ÷ median)** | **large (e.g. 30–50×)** | ~1–3× | ~1–3× | ~1× |
| Spill | maybe | none | none | none |

The skew ratio collapsing from tens-of-× to ~1× is the proof the fix worked.

## 7. Takeaways & "in real production…"

- **Detect** skew by the task-time **max-vs-median** spread and a single fat **Shuffle Read** task —
  not by average duration (averages hide the straggler).
- **Prefer broadcast** when one side fits; otherwise **AQE skew-join** (on by default in Spark 3.2+/4.x);
  fall back to **salting** when you need explicit control.
- **Laptop caveat (the "shrink the box" trick):** because our data is tiny, AQE's default skew
  threshold (256 MB) never trips — so the notebook *lowers* `skewedPartitionThresholdInBytes` to
  reproduce the behavior. On real (large) data the defaults work as-is.
- **In production:** alert on per-stage task-time skew (max/median), keep AQE enabled, set
  `autoBroadcastJoinThreshold` deliberately, and watch for "one task running far longer than the
  rest" — the universal skew signature. Pre-aggregate or isolate known mega-keys when you can.

## 8. Teardown

Nothing to clean: the data was generated lazily and only counted, so no tables or files were
written. The notebook resets the session profile to `tuned` at the end. If you experimented with
writes, `make clean` removes everything under `.tmp/`.
