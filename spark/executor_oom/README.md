# SPK-2 — Executor OOM (memory pressure → GC thrash → kill)

> **Break → Detect → Fix → Prove.** The second-most-common Spark failure after skew:
> a job pins too much memory in **cache**, then runs a heavy aggregation with **too few
> shuffle partitions**, so each partition is enormous. The single local-mode executor JVM
> runs out of headroom, spends most of its time in **garbage collection**, spills to disk,
> and — on a small enough box — gets **OOM-killed** (`exit 137`).

- **Notebook:** [`spk2_executor_oom.ipynb`](./spk2_executor_oom.ipynb)
- **Toolkit used:** `common.datagen` (`wide_rows` — fat rows that eat memory fast), `common.profiles` (force/relieve the pathology), `common.metrics_diff` (prove it)
- **Time:** ~12 min.

---

## ⚠️ This module REQUIRES the constrained box (`make up-constrained`)

Unlike SPK-1 (skew), this is a **memory** pathology, and **memory is fixed when the Spark
server boots**. A notebook talking to the server over Spark Connect can set SQL confs
(`shuffle.partitions`, AQE, …) at runtime, but it **cannot shrink the driver/executor heap** —
that's a JVM/container property decided at startup (see the two-layers note in
[`common/README.md`](../../common/README.md)).

So to make the memory pressure *real* and *contained*, start the small box first:

```bash
make up-constrained     # ~2 GB container, driver.memory ~1g, 2 cores
```

On the default **tuned** (~3 GB) box the same notebook will still show heavy GC and spill, but
the squeeze is gentler and an outright OOM is less likely. The constrained box is what makes the
failure honest — *real inside the container, harmless to the host*. `make clean` recovers
everything under `.tmp/` afterward.

> **Local-mode reminder:** the server runs `--master local[*]`, so the **driver JVM is also the
> executor**. There is one heap. "Executor OOM" and "driver heap exhaustion" are the same event
> here — exactly the small-scale stand-in for a killed executor on a real cluster.

---

## 1. The scenario

A daily revenue-rollup job reads a wide events table (lots of columns per row), and an engineer —
trying to "make the repeated aggregations faster" — slaps a `.cache()` on the full frame. To keep
the shuffle "simple" they also set `spark.sql.shuffle.partitions` very low. It worked on a sample.
In production the job now spends minutes in GC, the Spark UI shows the executor pinned at its
memory limit, and on the constrained box the container dies with `exit 137`. Nothing about the
*logic* is wrong — the **memory plan** is.

Two mistakes compound:

1. **Over-caching.** `.cache()` on a wide frame pins a large block of memory as **storage**. That
   memory is now unavailable to **execution** (the shuffle/aggregation).
2. **Too few partitions.** With `shuffle.partitions` tiny, each post-shuffle partition holds a
   huge slice of the data. A task must hold its whole partition in **execution** memory — which
   the cache just shrank. Per-partition memory balloons.

Storage and execution are fighting over one small heap. The loser is the JVM.

## 2. Break it

Under the **`constrained` session profile** (`common.profiles.apply_profile`) — which also sets
`spark.sql.shuffle.partitions = 16` — we:

- generate a **wide** frame with `common.datagen.wide_rows(..., n_cols=60)` (fat rows: ~60 doubles
  each, so the same row count costs far more bytes than a narrow table),
- **`.cache()` it and force it resident** with a `.count()` (cache is lazy — see the Storage-tab
  note below), pinning storage memory,
- then run a **heavy aggregation** (group many distinct keys, summing all the wide columns) that
  needs a large amount of *execution* memory at once.

GC time balloons, spill appears, and on the constrained box the executor may be **killed**.

> **Be honest about scale (the "shrink the box" caveat):** at laptop row counts you will reliably
> see **heavy GC + disk spill** and storage memory pinned near the limit; a *guaranteed hard OOM*
> (`exit 137`) depends on how tight the box is and your row count. The teaching point is the
> **memory pressure and its relief** — and that the constrained container makes a real failure
> *possible but contained*. If you want to push it over the edge, raise `N_ROWS`/`N_COLS` (the
> notebook says where) — but expect the container to die, which is the whole point.

## 3. Detect it — read the Spark UI

Open http://localhost:4040 (live) or http://localhost:18080 (history, if the job already died).
The tells, per [`docs/spark-ui-guide.md`](../../docs/spark-ui-guide.md):

| Where | Signal (broken) | Healthy |
|-------|-----------------|---------|
| **Executors** tab → **Task Time (GC Time)** | GC time is a **large fraction** of task time — the leading indicator of an OOM | GC a small slice of task time |
| **Executors** tab → **Storage Memory** | pinned **at / near the limit** — the cache is crowding out execution | comfortably below the limit |
| **Executors** tab → **stderr** (or the driver log) | `OutOfMemoryError` / **`GC overhead limit exceeded`**; an executor that **vanishes**; process **`exit 137`** | no errors, executor stays alive |
| **Stages → Tasks** Summary Metrics → **Spill (memory)** / **Spill (disk)** | **non-zero** — data didn't fit execution memory | none |
| **Storage** tab | the cached wide frame resident, **Fraction Cached** high, eating the heap | — |

`common.metrics_diff.measure()` captures the quantitative side: **spill bytes** and **peak
execution memory** are the headline numbers this module drives down (GC time itself is a
JVM-internal metric you read in the UI, not over the Connect REST surface).

## 4. Diagnose

A Spark task must hold its **whole partition** in execution memory to process it. With
`shuffle.partitions` tiny, each partition is huge → per-task memory is huge. Meanwhile the
`.cache()` has pinned a big chunk of the *same* heap as **storage**, so there's less execution
memory to go around. Spark's unified memory manager lets storage and execution share a pool
(governed by `spark.memory.fraction`), but when both want a lot at once on a small heap, the
engine spills to disk, GC churns trying to free space, and if it still can't keep up the JVM
throws `OutOfMemoryError` (or the OS/cgroup kills the container for exceeding `mem_limit` →
`exit 137`). More cores won't help — the limit is **bytes per partition**, not parallelism.

## 5. Fix it — three production remedies

| Fix | How | Why it works |
|-----|-----|--------------|
| **Raise `shuffle.partitions`** | bump from 16 → a few hundred (or let **AQE coalesce**) | More, smaller partitions → each task holds far less data at once → execution memory fits, spill drops. **The biggest lever — try first.** |
| **Don't over-cache; `unpersist()`** | only `.cache()` a frame you reuse **many** times and that **fits**; release it with `.unpersist()` before the heavy shuffle | Frees storage memory back to execution. Caching a frame you scan once is pure cost — let Spark **spill** the shuffle (cheap, transient) instead of **pinning** the input (expensive, resident). |
| **Tune `spark.memory.fraction`** *(note only)* | the storage+execution share of the heap | A real lever on a cluster, but it's set at **server startup**, so a Connect client can't change it here — we describe it and point at the constrained vs tuned box. |

The notebook applies the first two (raise partitions **and** drop the needless cache) and proves
the relief. It then notes the third as the JVM-level knob you'd reach for on a real cluster.

## 6. Prove it

`common.metrics_diff.compare([...])` prints a before/after table. Expected shape:

| Metric | over-cached + few partitions | unpersist + more partitions |
|--------|-----------------------------:|----------------------------:|
| Wall-clock runtime | high (GC-bound) | ↓ |
| Spill (memory) | **large** | ↓↓ / none |
| Spill (disk) | **large** | ↓↓ / none |
| Peak exec memory | **high** | ↓ |
| Tasks | few (e.g. 16) | many (hundreds) |

Spill collapsing toward zero and peak execution memory dropping is the proof the fix worked —
backed by the **Executors** tab showing GC time and Storage Memory fall back to a healthy range.

## 7. Takeaways & "in real production…"

- **Detect** executor memory trouble by **GC time as a fraction of task time** climbing, **Storage
  Memory pinned at the limit**, and **non-zero spill** — *before* the executor dies. The kill
  itself (`exit 137` / `container killed`) is the late symptom; the GC/spill pressure is the early
  warning.
- **Too few partitions is the usual culprit:** per-partition memory, not core count, is the
  constraint. Raise `shuffle.partitions` / keep **AQE coalesce** on so partitions fit the heap.
- **Cache deliberately.** `.cache()` only what you reuse many times *and* that fits; always
  `.unpersist()` when done. Prefer letting Spark **spill** a shuffle over **pinning** an input you
  scan once. A forgotten cache is a classic slow-burn OOM.
- **The "shrink the box" trick:** memory is fixed at server boot, so this module needs the
  constrained container — that's what turns a gentle laptop squeeze into a real (but contained)
  failure. On real clusters you'd size `executor.memory` / `spark.memory.fraction` instead.
- **In production:** alert on executor GC-time ratio and OOM kills / `FetchFailedException`
  (a vanished executor's shuffle blocks go missing downstream); set `shuffle.partitions` (or AQE)
  to keep partitions in the tens-to-hundreds-of-MB range; review every `.cache()` in code review.

## 8. Teardown

The data is generated lazily and only aggregated (never collected or written), so there are no
tables or files to delete. The notebook **`unpersist()`s** the cache, **clears all cached data**,
and resets the session profile to **tuned**. If the constrained container was OOM-killed mid-run,
`make up-constrained` (or `make up`) restarts it; `make clean` clears anything left under `.tmp/`.
