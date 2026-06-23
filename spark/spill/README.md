# SPK-4 — Disk Spill (too few shuffle partitions)

> **Break → Detect → Fix → Prove.** A heavy shuffle (a wide aggregation / sort) split into
> **too few** partitions makes each post-shuffle partition larger than the memory a task has to
> sort it in — so the engine **spills to disk**. The job still finishes, but it crawls: extra
> disk I/O, serialization, and an external merge-sort the partitioning forced on it.

- **Notebook:** [`spk4_disk_spill.ipynb`](./spk4_disk_spill.ipynb)
- **Toolkit used:** `common.datagen` (`uniform_keys` / `wide_rows` — a big lazy frame), `common.profiles` (force/relieve the pathology), `common.metrics_diff` (already captures `spill_mem_bytes` / `spill_disk_bytes` — proves it)
- **Run against:** the unified Spark server (`make up`) — open the Spark UI at http://localhost:4040 while the notebook runs.
- **Time:** ~10 min. **Laptop-safe:** ~20M rows are generated *lazily* and only aggregated / `count()`-ed (never collected or written), so nothing fills disk beyond Spark's own short-lived spill files; there is nothing to delete at the end.

> **This is a partition-sizing module, not a container-memory one.** The pathology is per-task
> sort memory vs partition *size*, which you control with `spark.sql.shuffle.partitions`. It
> reproduces fine on the default **tuned** box — you do **not** need `make up-constrained`.
> (Container-memory failures are `SPK-2` executor OOM / `SPK-3` driver OOM.)

---

## 1. The scenario

A reporting job rolls a large event stream up to per-(customer, day) totals — a `groupBy` over
many keys plus a sort. It used to fit comfortably in memory. After someone "tidied up" the Spark
config and pinned `spark.sql.shuffle.partitions = 16` (to "avoid tiny files"), the same job now
spends most of its time doing disk I/O. Throughput collapsed even though the data volume didn't
change. What happened?

With only **16** post-shuffle partitions, every group-by/sort task is handed a huge slice of the
shuffled data — far more than it can sort in the execution memory available to one task. Spark's
external sort does the only thing it can: it **spills** sorted runs to local disk, then
merge-sorts them back. The work still completes, but the disk round-trips dominate the runtime.

## 2. Break it

We generate ~20M wide-ish rows with `common.datagen` and run a heavy `groupBy(many keys).agg(...)`
(plus an `orderBy`) under the **`constrained` session profile** (`common.profiles.apply_profile`):

- **`spark.sql.shuffle.partitions = 16`** — the whole shuffle output is forced into 16 oversized
  partitions, so each sort task gets a partition too big for its memory budget.
- **AQE off** — so AQE's `coalescePartitions` can't quietly resize the post-shuffle partitions and
  rescue us; we see the raw pathology.

The aggregation + `count()` runs, and `measure()` reports **non-zero Spill (memory)** and the
expensive **Spill (disk)**.

> Why this is laptop-safe: 20M rows are *generated*, not stored; we only aggregate and `count()`,
> so the driver never collects a large result. The only thing that touches disk is Spark's own
> spill scratch (short-lived, under `.tmp`), which `make clean` clears.

## 3. Detect it — read the Spark UI

Open http://localhost:4040 → **SQL / DataFrame** tab → click the running/last query → scroll to
the aggregation's reduce **Stage** → **Tasks** → **Summary Metrics**. The tell (see
[`docs/spark-ui-guide.md`](../../docs/spark-ui-guide.md), the **Spill** rows):

| Signal | Spilling (broken) | Healthy |
|--------|-------------------|---------|
| **Spill (memory)** per task | non-zero — the sort overflowed its in-heap buffer | 0 |
| **Spill (disk)** per task | **non-zero** — sorted runs written to local disk (the expensive one) | 0 |
| **Number of tasks** in the reduce stage | tiny (= `shuffle.partitions`, e.g. 16) | sized to the data (e.g. ~200, or AQE-coalesced) |
| **Shuffle Read Size** per task | large (the whole shuffle ÷ 16) | a comfortable slice |
| **Duration** | inflated by disk round-trips | dominated by compute |

The Spark-UI guide maps exactly this: *"Non-zero **Spill (memory)** and especially **Spill (disk)**;
often `shuffle.partitions` too low → the `SPK-4` signal."* `common.metrics_diff.measure()` captures
the same numbers as **`spill_mem_bytes`** / **`spill_disk_bytes`** — the headline figures this
module drives toward zero.

## 4. Diagnose

A shuffle partition must be **sorted/aggregated in memory by a single task**. The post-shuffle
partition *size* is `(total shuffle bytes) ÷ (shuffle.partitions)`. Pin the partition count too
low and each partition is too big to fit in the execution memory one task gets — so Spark's
**external sort spills** the overflow to disk and merge-sorts it back. The cause is **partition
size > available per-task execution memory**, not a shortage of total RAM: the fix is to make the
partitions *smaller* (more of them), not to add memory.

## 5. Fix it — two production remedies

| Fix | How | When to reach for it |
|-----|-----|----------------------|
| **Raise `shuffle.partitions`** | set `spark.sql.shuffle.partitions` higher (e.g. 200) so each post-shuffle partition is small enough to sort in memory | The direct lever when you know the shuffle is too coarse. Static, predictable. |
| **Let AQE coalesce** | `apply_profile(spark, "tuned")` → AQE picks a partition count at runtime from the actual shuffle size (`advisoryPartitionSizeInBytes`), so you don't hand-tune | The production default on Spark 3.2+/4.x — adapts to data volume without a magic number. |

We show **both**: first bump `shuffle.partitions` to 200 with AQE still off (isolates the
partition-count lever), then flip to `tuned` and let AQE size the partitions itself.

## 6. Prove it

`common.metrics_diff.compare([...])` prints a before/after table. Expected shape:

| Metric | 16 partitions (spilling) | 200 partitions | AQE (tuned) |
|--------|-------------------------:|---------------:|------------:|
| Wall-clock runtime | high | ↓ | ↓ |
| **Spill (disk)** | **large** | ~0 | ~0 |
| **Spill (memory)** | **large** | ~0 | ~0 |
| Tasks (reduce stage) | 16 | 200 | AQE-coalesced |

**Spill (disk) collapsing toward 0** while runtime drops is the proof the fix worked.

## 7. Takeaways & "in real production…"

- **Detect** spill by the per-task **Spill (memory)** / **Spill (disk)** columns in Stages → Tasks —
  disk spill is the costly one; non-zero is a red flag even if the job "succeeds".
- **Right-size the shuffle**: post-shuffle partition size ≈ total shuffle bytes ÷
  `shuffle.partitions`. Aim for partitions that fit in per-task memory (rule of thumb ~100–200 MB).
- **Prefer AQE** (`coalescePartitions`, `advisoryPartitionSizeInBytes`) so the partition count
  tracks the real data size instead of a hard-coded number that's wrong half the time.
- **The tradeoff (don't over-correct):** raising `shuffle.partitions` too far swings into the
  *opposite* pathology — thousands of tiny partitions where **scheduling overhead** dominates a
  small job. That's `SPK-9` (shuffle internals & stages). There's a sweet spot; AQE finds it for you.
- **In production:** alert on non-zero disk spill on hot stages, keep AQE enabled, and set a
  sensible `advisoryPartitionSizeInBytes` rather than pinning `shuffle.partitions` globally.

## 8. Teardown

Nothing durable was written — we only aggregated and counted generated data, so there are no
tables or files to delete. The notebook resets the session profile to `tuned` at the end and
clears any cache. Spark's transient spill scratch lives under `.tmp/`; `make clean` removes
everything there if you want a clean slate.
