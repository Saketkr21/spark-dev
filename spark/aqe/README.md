# SPK-6 — Adaptive Query Execution (AQE) deep-dive

> **Break → Detect → Fix → Prove.** AQE re-plans a query *at runtime* using the real
> statistics produced by each shuffle, instead of trusting the optimizer's compile-time
> guesses. This module is the natural deep-dive companion to the skew flagship
> [`SPK-1`](../skew/README.md): SPK-1 fixes skew by hand (salting / broadcast) and uses AQE's
> skew-join as *one* of three remedies — here we open AQE up and watch **all** of what it does
> for free, demo by demo, **AQE-off vs AQE-on**.

- **Notebook:** [`spk6_aqe.ipynb`](./spk6_aqe.ipynb)
- **Toolkit used:** `common.datagen` (skewed / uniform / dimension generators), `common.profiles`
  (`constrained` = AQE off, `tuned` = AQE on), `common.metrics_diff` (prove it)
- **Run against:** the unified Spark server (`make up`) — open the Spark UI at
  http://localhost:4040 while the notebook runs.
- **Time:** ~12 min. **Laptop-safe:** data is generated lazily and only `count()`-ed (never
  collected or written), so nothing fills memory or disk; there is nothing to delete at the end.
  AQE behavior is about task/partition counts and plan shape, not memory, so the default **tuned**
  box is fine — you do **not** need `make up-constrained`.

---

## 1. The scenario

The Catalyst optimizer plans a query **before a single row moves**. It picks the join strategy,
the number of post-shuffle partitions (`spark.sql.shuffle.partitions`, default **200**), and the
data layout from compile-time estimates that are often wrong: it can't know that a filter will
throw away 99% of the rows, that one join key holds 90% of them, or that a side it planned to
sort-merge actually fits in memory.

**Adaptive Query Execution** closes that gap. After each shuffle (an `Exchange`) completes, AQE
looks at the **actual** map-output statistics and rewrites the *not-yet-run* part of the plan:

1. **Coalesce shuffle partitions** — collapse the 200 mostly-empty post-shuffle partitions into a
   handful sized to the real data, so you don't schedule 200 tiny tasks for a few KB.
2. **Skew-join split** — detect a partition far larger than its siblings and split it into several,
   so the skewed key stops being one straggler task (the runtime twin of SPK-1's manual fixes).
3. **Re-optimize joins** — once a side's real size is known, switch a planned sort-merge join to a
   broadcast join (no shuffle of the big side at all).

AQE has been **on by default since Spark 3.2** and remains on in **Spark 4.x**. This module shows
*what it buys you*, *how to read it in the plan and the SQL tab*, and *the two places it can cost
you*: extra planning overhead on trivial queries, and run-to-run non-determinism in partition counts.

## 2. Break it — AQE off (three demos)

We force the pre-adaptive behavior with the **`constrained` session profile**
([`common.profiles.apply_profile`](../../common/profiles.py) — it already sets
`spark.sql.adaptive.enabled=false`, skew-join off, coalesce off), then run three queries that each
trigger one AQE feature:

| Demo | What we run (AQE off) | The pathology you see |
|------|-----------------------|-----------------------|
| **(a) Coalesce** | aggregate a heavily-*filtered* fact with `spark.sql.shuffle.partitions = 200` | the shuffle produces **200 tiny partitions → 200 tasks** for a few rows of result |
| **(b) Skew-join** | sort-merge join a 90%-hot-key fact (`skewed_keys`) onto its dimension | one reduce partition gets ~90% of the rows → **one straggler task** (max ≫ median) |
| **(c) Re-optimize** | sort-merge join a big fact onto a *small* (broadcastable) dimension with broadcast disabled | both sides shuffled + sorted — an **`Exchange` and `SortMergeJoin`** that didn't need to happen |

> Why this is laptop-safe: each fact is *generated* (10–20M rows), not stored, and we only
> `count()`, so the driver never collects a large result. Nothing is written, so there's nothing
> to clean up.

## 3. Detect it — read the Spark UI

Open http://localhost:4040 → **SQL / DataFrame** → click the query → read the **final physical
plan** and per-node metrics. The tells (see [`docs/spark-ui-guide.md`](../../docs/spark-ui-guide.md),
"SQL / DataFrame" and the AQE-adjusted-plans note):

| Demo | AQE **off** (broken) — plan / metrics | AQE **on** (fixed) — plan / metrics |
|------|---------------------------------------|-------------------------------------|
| **(a) Coalesce** | `Exchange` feeds **~200 tasks**; Stages tab shows hundreds of millisecond tasks | plan shows **`AQEShuffleRead coalesced`**; far **fewer tasks** (single digits) |
| **(b) Skew-join** | `SortMergeJoin`; Tasks → Summary Metrics: **Duration Max ≫ Median** on the reduce stage | plan shows **`AQEShuffleRead ... skewed`** — the hot partition split into many; max-vs-median flattens |
| **(c) Re-optimize** | `SortMergeJoin` with **two `Exchange` nodes** | plan flips to **`BroadcastHashJoin`** — the big side's `Exchange` disappears |

The signature you confirm in the plan text, AQE-on, is **`AdaptiveSparkPlan isFinalPlan=true`** with
**`AQEShuffleRead`** nodes. In the notebook we read these from `df.explain()` (Connect-safe — no
`sparkContext` / RDD access) and quantify them with `metrics_diff` (task counts, runtime, skew ratio).

> **The "shrink the box" caveat (same trick as SPK-1):** AQE's skew threshold defaults to **256 MB**,
> which our tiny generated data never reaches. So demo (b) **lowers**
> `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` to ~16 MB to reproduce the split on a
> laptop. On real (large) data the defaults trip on their own — see
> [`SPK-1` §7](../skew/README.md).

## 4. Diagnose

The optimizer commits to a plan from **compile-time estimates**. Three ways those estimates go stale:

- **Too many partitions:** `shuffle.partitions=200` is a fixed default. After a selective filter,
  the post-shuffle data is tiny, but you still pay to schedule, launch, and track 200 tasks —
  scheduling overhead dominates the actual work.
- **Skew:** a hash shuffle pins all rows of one key to one partition. The planner sizes all
  partitions equally; reality is one giant partition and 199 empty ones.
- **Wrong join strategy:** the planner sized a side as "too big to broadcast" (or broadcast was
  disabled), so it shuffles both sides. Only *after* the build side materializes does its true size
  become known.

AQE fixes all three **after the shuffle**, using map-output statistics — information that simply
doesn't exist at compile time.

## 5. Fix it — turn AQE on (and where to still intervene)

In the notebook we flip each demo with `apply_profile(spark, "tuned")` (AQE + skew-join +
coalesce on) plus targeted `**overrides`:

| Demo | The AQE knob that fixes it | Note |
|------|----------------------------|------|
| **(a) Coalesce** | `spark.sql.adaptive.enabled=true` + `spark.sql.adaptive.coalescePartitions.enabled=true` | collapses 200 → a few partitions sized to the real post-filter data |
| **(b) Skew-join** | `…adaptive.skewJoin.enabled=true`; lower `…skewJoin.skewedPartitionThresholdInBytes` to `16m` and `…skewedPartitionFactor` to `2` for laptop scale (broadcast kept **off** so it stays a sort-merge join) | the runtime version of SPK-1's salting — no code change needed |
| **(c) Re-optimize** | `spark.sql.adaptive.enabled=true` with broadcast re-enabled (default 10 MB threshold) | AQE switches SMJ → broadcast once the small side's real size is known |

**Where AQE still costs you (demo (c)'s second half):**

- **Planning overhead on tiny queries.** Re-planning after every shuffle is near-free on a big job
  but is pure overhead on a trivial one. The notebook measures AQE off vs on for a tiny query — the
  runtime difference is the overhead. (Spark only skips AQE entirely for queries with **no**
  `Exchange`/subquery.)
- **Run-to-run non-determinism.** The **coalesced partition count depends on the runtime data
  size**, so it can vary between runs and between environments — surprising if a downstream step or
  test assumes a fixed partition/file count. (Ties to the streaming small-files concern in `STR-3`.)

## 6. Prove it

`common.metrics_diff.compare([...])` prints a before/after table per demo. Expected shape:

**(a) Coalesce** — the headline is the **task count** dropping:

| Metric | AQE off (200 parts) | AQE on (coalesced) |
|--------|--------------------:|-------------------:|
| Tasks | **~200** | **single digits** |
| Wall-clock runtime | higher (scheduling overhead) | ↓ |

**(b) Skew-join** — the headline is the **skew ratio** collapsing (the same number SPK-1 drives down):

| Metric | AQE off (SMJ) | AQE on (skew-join) |
|--------|--------------:|-------------------:|
| Task time — max | **huge** | ↓ |
| **Skew (max ÷ median)** | **large (tens of ×)** | **~1–3×** |

**(c) Re-optimize** — the headline is **shuffle bytes** going to ~0 as SMJ → broadcast:

| Metric | AQE off (SMJ) | AQE on (broadcast) |
|--------|--------------:|-------------------:|
| Shuffle read/write | large | **~0** |
| Wall-clock runtime | higher | ↓↓ |

The numbers moving — tasks, skew ratio, shuffle bytes — are the proof AQE re-planned at runtime.

## 7. Takeaways & "in real production…"

- **AQE is on by default in Spark 3.2+ / 4.x.** Three things it fixes *for free*: too-many-tiny
  partitions (**coalesce**), skewed joins (**skew-join split**), and a mis-sized join strategy
  (**SMJ → broadcast re-optimize**).
- **Read it in the plan:** AQE-on plans say **`AdaptiveSparkPlan isFinalPlan=true`** and carry
  **`AQEShuffleRead`** nodes (annotated `coalesced` / `skewed`); the displayed plan is the *final*
  one. The **SQL / DataFrame** tab is where you confirm the join operator and the coalesced
  partition count ([`docs/spark-ui-guide.md`](../../docs/spark-ui-guide.md)).
- **Where to still intervene:** AQE adds a little planning overhead on trivial queries, and its
  coalesced partition counts are **non-deterministic** (data-size-dependent) — don't hard-code an
  expected partition/file count downstream. For known mega-keys, AQE's defaults assume large data,
  so on small data you lower `skewedPartitionThresholdInBytes` (the SPK-1 caveat).
- **In production:** keep AQE enabled; set `spark.sql.shuffle.partitions` sanely and let coalesce
  trim it; for big⋈big skew lean on AQE skew-join before reaching for salting (`SPK-1`); set
  `autoBroadcastJoinThreshold` deliberately so the SMJ→broadcast re-optimization can fire.

## 8. Teardown

Nothing was written (we only counted generated data), so there is nothing to delete. The notebook
resets the session profile to `tuned` at the end, restoring the production-tuned safety nets.
`make clean` removes everything under `.tmp/` if you experimented with writes.
