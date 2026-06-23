# SPK-9 — Shuffle internals & stages

> **Break → Detect → Fix → Prove.** Every Spark job is a chain of **stages** separated by
> **shuffles**. This module makes the invisible visible: it shows *where* a shuffle boundary
> comes from (narrow vs wide dependencies in `df.explain()`), then sweeps the post-shuffle
> **partition count** across three values to prove that too few partitions spill and too many
> drown in scheduling overhead — with a sweet spot in between.

- **Notebook:** [`spk9_shuffle.ipynb`](./spk9_shuffle.ipynb)
- **Toolkit used:** `common.datagen` (`uniform_keys` — a clean, balanced fact so partition count is the *only* variable), `common.profiles` (apply a baseline, then override `spark.sql.shuffle.partitions`), `common.metrics_diff` (the before/after sweep table)
- **Run against:** the unified Spark server (`make up`) — open the Spark UI at http://localhost:4040 while the notebook runs.
- **Time:** ~10 min. **Laptop-safe:** ~20M rows are generated *lazily* and only `count()`-ed (never collected or written), so nothing fills memory or disk. The default **tuned** box is fine. Nothing to delete at the end.

---

## 1. The scenario

A teammate's aggregation job "feels slow," so they copy a `spark.sql.shuffle.partitions = 2000`
they saw on a Stack Overflow answer for a 5 TB job. On our data it gets *slower*. Someone else
sets it to `8` to "reduce overhead" and the stage starts **spilling to disk**. Neither knows
*why* — because neither can see that the `groupBy` they wrote inserts a **shuffle**, that the
shuffle creates a **new stage**, and that `shuffle.partitions` sets how many **tasks** that stage
runs and therefore **how big each task's slice of data is**.

This module steps back and teaches the mechanic itself: what a shuffle *is*, why it draws a stage
boundary, and how the partition count trades **task size** against **task count**.

## 2. Break it — wide dependencies create shuffles, partition count sets the slice

Two parts, both on a **balanced** `uniform_keys` fact (no skew — see [`SPK-1`](../skew/README.md)
for that) so the partition count is the only thing changing:

1. **Narrow vs wide.** We `explain()` a chain of `select` / `filter` / `withColumn` (all **narrow**:
   each output partition reads exactly one input partition → **no `Exchange`**, one stage) and
   contrast it with a `groupBy` / `distinct` / `join` (all **wide**: output partitions pull from
   *all* input partitions → an **`Exchange`** node → a stage boundary).
2. **The sweep.** We run the **same ~20M-row aggregation** three times at
   `spark.sql.shuffle.partitions ∈ {8, 200, 2000}` and `measure()` each. We force the broken ends
   on purpose:
   - **8** → the shuffle output is packed into 8 huge partitions → 8 fat tasks that **spill** memory→disk.
   - **2000** → ~20M rows split into 2000 tiny partitions → 2000 tasks whose **scheduling overhead** dwarfs their compute.
   - **200** → Spark's default; here it's the **sweet spot** (task size and task count both reasonable).

> Why this is laptop-safe: 20M rows are *generated*, not stored; we only `count()`, so the driver
> never collects a large result. This is a partition-sizing demo, not a memory bomb — it runs fine
> on the default **tuned** box; you do **not** need `make up-constrained`.

## 3. Detect it — read the Spark UI

Open http://localhost:4040 (see [`docs/spark-ui-guide.md`](../../docs/spark-ui-guide.md)):

- **SQL / DataFrame** tab → click the query → the physical-plan DAG. **Every `Exchange` node is a
  shuffle and a stage boundary.** The narrow chain has **none**; the aggregation has **one**. This
  is the visual proof that "wide dependency ⇒ shuffle ⇒ new stage."
- **Stages** tab → the post-shuffle (reduce) stage. Its **number of tasks** *equals*
  `spark.sql.shuffle.partitions`. Compare the three runs:

| `shuffle.partitions` | What you see in Stages → Tasks | The signal |
|---------------------:|--------------------------------|------------|
| **8** | 8 tasks; large per-task input; **non-zero Spill (disk)** | too few → giant partitions → spill (the [`SPK-4`](../spill/README.md) tell) |
| **200** | ~200 modest tasks; little/no spill | balanced — the sweet spot |
| **2000** | 2000 tiny tasks; large **Scheduler Delay** fraction in the Event Timeline; runtime dominated by overhead | too many → tiny tasks → scheduling overhead ([`SPK-9`](./README.md) "thousands of tiny tasks") |

`common.metrics_diff.measure()` captures this quantitatively: **Tasks** (= partition count),
**Spill (disk)**, and wall-clock **runtime** are the three numbers that tell the story.

> **Connect-safe note:** notebooks talk to Spark over Connect, so we never call
> `rdd.getNumPartitions()`. We reason about partition count from the conf we set
> (`spark.sql.shuffle.partitions`) and confirm it via the **task count** in the metrics table —
> the reduce stage runs exactly that many tasks.

## 4. Diagnose

A **shuffle** redistributes rows across the network so that all rows with the same key land
together (required by `groupBy`, `join`, `distinct`). It is a hard **stage boundary**: the
upstream "map" stage must finish writing shuffle files before the downstream "reduce" stage can
read them. **Narrow** transformations (`select`, `filter`, `withColumn`) need no redistribution,
so they fuse into one stage with no `Exchange`.

`spark.sql.shuffle.partitions` is the number of **reduce partitions** that shuffle produces — and
therefore the number of **tasks** in the reduce stage. It directly sets the **task size ↔ task
count** tradeoff:

- **too few** → each partition is huge → it doesn't fit in execution memory → **spill to disk** (slow I/O).
- **too many** → each partition is tiny → per-task **scheduling / serialization overhead** dominates the actual work.

## 5. Fix it — right-size the partition count

| Fix | How | When |
|-----|-----|------|
| **Right-size `shuffle.partitions`** | A common rule of thumb: **~2–3× total executor cores** (here, the local box's cores). On our laptop the default `200` already lands near the sweet spot. | When you control the conf and the data volume is roughly known. |
| **Let AQE coalesce** | `spark.sql.adaptive.enabled=true` + `adaptive.coalescePartitions.enabled=true`: start high, and AQE **merges** small post-shuffle partitions at runtime based on actual sizes — so you don't have to guess. | The modern default ([`SPK-6`](../README.md) AQE deep-dive). Set a generous `shuffle.partitions` and let AQE coalesce down. |

This module's `constrained` baseline holds AQE **off** so the partition count we set is the count
that runs (no runtime coalescing masking the effect). In production you'd usually leave AQE on and
let it tune this for you.

## 6. Prove it

`common.metrics_diff.compare([p8, p200, p2000])` prints the sweep. Expected shape:

| Metric | parts=8 | parts=200 | parts=2000 |
|--------|--------:|----------:|-----------:|
| Wall-clock runtime | slow (spill I/O) | **fast** | slow (overhead) |
| Tasks | 8 | ~200 | 2000 |
| Spill (disk) | **non-zero** | ~none | none |
| Task time — max | large (fat task) | modest | tiny per task |

The **U-shape in runtime** (slow at 8, fast at 200, slow again at 2000) is the proof: partition
count is a real, measurable knob, and the middle wins.

## 7. Tie-back to the rest of Phase 1

This is the mechanic underneath several other modules:

- **Too few partitions → spill.** The `8`-partition run reproduces in miniature the disk-spill
  pathology of [`SPK-4`](../spill/README.md).
- **Skew → one fat partition.** Even with a *good* partition count, a single hot key sends all its
  rows to **one** reduce partition → a straggler. That's [`SPK-1`](../skew/README.md): skew is a
  per-partition imbalance the count alone can't fix.
- **AQE auto-tunes this.** [`SPK-6`](../README.md) (AQE deep-dive) coalesces tiny partitions and
  splits skewed ones at runtime — automating the right-sizing you do by hand here.

## 8. Takeaways & "in real production…"

- A **wide dependency** (`groupBy` / `join` / `distinct`) inserts an **`Exchange`** = a shuffle =
  a **new stage**; **narrow** ops (`select` / `filter` / `withColumn`) don't. Read `df.explain()`
  (or the SQL tab) and **count the `Exchange` nodes** — each one is a cost.
- `spark.sql.shuffle.partitions` = the reduce stage's **task count**, which sets **task size**.
  **Too few → spill; too many → scheduling overhead.** Aim for ~**2–3× total cores**, or let AQE
  coalesce.
- **In production:** keep **AQE enabled** (`coalescePartitions` does this sizing for you), don't
  copy a giant `shuffle.partitions` from someone else's TB-scale job, and when a stage is
  mysteriously slow check **task count** and **spill** before reaching for more hardware.

## 9. Teardown

Nothing was written (we only counted generated data), so there is nothing to delete. The notebook
resets the session profile to `tuned` at the end. If you experimented with writes, `make clean`
removes everything under `.tmp/`.
