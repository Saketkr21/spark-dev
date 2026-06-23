# How to Read the Spark UI — symptom → tab → metric

> **Module F-4** of Phase 0. This is the **reference every challenge module's "Detect it" step points to.**
> When a module says *"open the Spark UI and look at the Stages tab"*, this guide is what it means.
>
> Read the pedagogy first: [`CURRICULUM_BRIEF.md`](./CURRICULUM_BRIEF.md) (the **Break → Detect → Fix → Prove** pattern and the "break it safely & measure it" trick) and the module roadmap in [`CURRICULUM_PLAN.md`](./CURRICULUM_PLAN.md). The shared toolkit lives in [`../common/README.md`](../common/README.md); the Phase 1 Spark track index is [`../spark/README.md`](../spark/README.md).

This guide powers the **Detect** step of the loop. The companion **Prove** step is a quantitative
before/after table from [`common/metrics_diff.py`](../common/README.md) — the UI shows you *where* the
pathology lives; `metrics_diff` proves the fix *moved the number*.

Accurate to **Spark 4.0.2** (Scala 2.13, Java 17) tab and metric names.

> 📸 **Screenshots will be added here as modules generate them.** Each challenge module
> (starting with the flagship `SPK-1`) drops annotated screenshots of the exact tab/metric it
> relies on into this guide as it is built, so the symptom→evidence mapping below gradually gets
> a visual companion. For now the descriptions are text-only.

---

## 1. Opening the UI

| What | Where | Notes |
|------|-------|-------|
| **Live Spark UI** | `http://localhost:4040` | The unified **Thrift + Connect** server in Docker. Shows a job **only while it runs and briefly after**. |
| **History Server** | `http://localhost:18080` | **Completed** applications, replayed from event logs in `.tmp/spark-events/`. Use this for any job that has already finished. |

Key facts about the live UI at `:4040`:

- It is attached to **one long-lived SparkContext** shared by notebooks (Spark Connect, gRPC `:15002`)
  **and** dbt (Thrift JDBC `:10000`). All of their jobs appear in the **same** UI.
- If the server is busy a second UI may bind to `:4041`, `:4042`, … — check the console if `:4040` looks empty.
- The live UI **forgets** old jobs once the in-memory retention limit is hit. For a job you ran an hour
  ago, or to compare two completed runs side by side, go to the **History Server** (`:18080`).
- **Rule of thumb:** *running or just-finished → `:4040`; finished and gone → `:18080`.* Both expose the
  identical tab set described below.

---

## 2. Tab-by-tab tour

Each tab below lists **what it's for** and **what to look for / red flags**.

### Jobs

**What it's for:** the top-level timeline. One **action** (`.count()`, `.write`, `.collect()`, a dbt model)
becomes one or more **jobs**; each job is a DAG of **stages**.

**Look for / red flags:**

- **Failed** jobs (red) — click in to find the failed stage, then the failed task and its exception.
- A job whose **Duration** dwarfs the others — your hotspot. Drill into its stages.
- The **Event Timeline** (expandable) shows jobs laid out over wall-clock time. Long **gaps** between
  jobs = driver-side work (planning, `.collect()` post-processing, Python round-trips), not cluster work.
- Many tiny jobs back-to-back can indicate a driver-side loop instead of one distributed job.

### Stages → Tasks  ⭐ *the most important tab for performance*

**What it's for:** a stage is a set of tasks separated by a **shuffle boundary**. The per-stage **Tasks**
table and **Summary Metrics** are where almost every Phase 1 performance pathology becomes visible.

**The skew tell (memorize this):** open a slow stage and read the **Summary Metrics** percentile table.
For **Duration** (task time):

```
Min     25th    Median(50th)    75th    Max
0.4s    0.5s    0.6s            0.7s    48s     ← Max ≫ 75th ≫ Median  → SKEW
```

> When **Max ≫ 75th percentile ≫ Median**, a few tasks are doing far more work than the rest — the
> classic **data-skew straggler**. This is the primary detect signal for **`SPK-1`** (see the quick-reference
> table). A balanced stage has Max within a small multiple of the Median.

**Summary Metrics percentile rows to scan** (each shown as Min / 25th / Median / 75th / Max):

| Metric | What a bad spread means |
|--------|-------------------------|
| **Duration** | Max ≫ Median → straggler / skew. |
| **Shuffle Read Size / Records** | One task reading far more than the median → skewed key landed on it (the `SPK-1` corroborating signal). |
| **Shuffle Write Size / Records** | Lopsided write → upstream skew being *produced*. |
| **Spill (memory)** / **Spill (disk)** | **Non-zero spill** = data didn't fit in execution memory and was written to disk → the `SPK-4` signal. Disk spill is the expensive one. |
| **Input Size / Records**, **Output Size / Records** | Sanity-check how much each task actually read/wrote. |
| **GC Time** | High GC relative to Duration → memory pressure; precursor to `SPK-2` executor OOM. |

**The Tasks table** (one row per task) lets you find the **single straggler**: sort by **Duration** or
**Shuffle Read Size** descending and the skewed task floats to the top with a value far above its peers.
Columns to watch: **Status**, **Duration**, **GC Time**, **Shuffle Read Size / Records**,
**Shuffle Write Size**, **Spill (memory)**, **Spill (disk)**, **Errors**.

**Other red flags here:**

- **Number of tasks** absurdly high for the data (hundreds/thousands of tiny tasks) → the
  too-many-tiny-partitions problem (`SPK-9`); often paired with `spark.sql.shuffle.partitions` left at 200.
- **Stage retries** / failed tasks with `FetchFailedException` → lost shuffle blocks, often downstream of an OOM.
- A flat **DAG Visualization** with a giant single stage → no parallelism.

### SQL / DataFrame

**What it's for:** every DataFrame/SQL action gets a **query** entry whose detail page shows the **physical
plan DAG** with **per-node runtime metrics**. This is where you read *what the optimizer actually did* —
crucial for join, pruning, and AQE modules.

**Look for / red flags:**

- **`Exchange` nodes** = shuffles. Each one is a stage boundary and a cost. Count them; an unexpected
  `Exchange` often means a join or aggregation forced a repartition you didn't intend.
- **Join operator name** tells you the strategy Spark chose:
  - **`BroadcastHashJoin`** — one side was small enough to broadcast (no shuffle of the big side). Usually what you want for big⋈small.
  - **`SortMergeJoin`** — both sides shuffled + sorted. The default for big⋈big; check it isn't being used where a broadcast was intended.
  - **`ShuffleHashJoin`** — both sides shuffled, one built into a hash map. Appears in specific size/config cases.
  - This is the core read for **`SPK-5`** (join strategies) — the wrong operator here *is* the bug.
- **Per-node "number of output rows" and "data size"** — follow rows down the plan. A node emitting far
  more rows than expected localizes an exploding join or missing filter.
- **Scan node rows** — compare **"number of output rows"** at the scan against the table total. A **full
  scan** (reads everything) vs a **pruned scan** (reads a fraction) is how you detect a **partition-pruning
  failure** (`SPK-7`); look also for **`PartitionFilters`** / **`PushedFilters`** in the scan node details
  (empty `PartitionFilters` where you expected pruning = a `CAST`/UDF killed it).
- **AQE-adjusted plans** — with Adaptive Query Execution on, the displayed plan is the **final** one and is
  annotated (e.g. `AQEShuffleRead`, coalesced partition counts, a skew-join split into more partitions).
  Comparing the plan with AQE on vs off is the heart of **`SPK-6`**.

### Executors

**What it's for:** one row per executor (plus the driver) showing resource use and health across the whole app.

**Look for / red flags:**

- **Task Time (GC Time)** column — the parenthesized GC figure is the tell. **GC time as a large fraction
  of task time** = memory pressure / churn; the leading indicator before an **executor OOM** (`SPK-2`).
- **Failed** / **killed** executors, or an executor that vanishes mid-job. In the executor's **stderr**
  log (linked from the row) look for **`container killed`**, **`Container killed by YARN/OS`**, or process
  **`exit 137`** — that's an **OOM kill** (the kernel/cgroup killed the JVM for exceeding the container's
  memory). In this repo OOM is *real inside the ~2 GB constrained container* but the host stays smooth — the
  detect surface for `SPK-2`.
- **Storage Memory** near its limit → cache is crowding out execution memory (feeds caching modules `SPK-8`
  and OOM `SPK-2`).
- **Shuffle Read / Shuffle Write** totals per executor — large imbalance hints at skewed placement.
- **Cores** and **Active Tasks** — confirm you actually have the parallelism you think you do.

### Storage

**What it's for:** every **cached / persisted** DataFrame or RDD, with its storage level and how much is
actually resident.

**Look for / red flags:**

- **Fraction Cached < 100%** — the dataset didn't fully fit; partitions are being **evicted** and recomputed
  on every access → cache **thrash**. The primary signal for **`SPK-8`** (caching tradeoffs).
- **Storage Level** (e.g. `Memory Deserialized 1x Replicated`, `Memory and Disk`) — confirm it's what you
  asked for; an unexpected level changes the memory/CPU tradeoff.
- **Size in Memory vs Size on Disk** — a large on-disk portion means memory overflowed to disk.
- **Empty Storage tab when you expected a cache** — `.cache()` is **lazy**; nothing appears until an action
  materializes it. A common "why is it still slow?" gotcha.

### Environment

**What it's for:** the **effective** configuration — the source of truth for *what confs are actually in
effect* (after defaults, `spark-defaults.conf`, and session-level `apply_profile(...)` overrides merge).

**Look for / red flags (confirm before you trust a fix):**

- **`spark.sql.adaptive.enabled`** — is AQE actually on/off? (`SPK-6`, and the constrained vs tuned profile.)
- **`spark.sql.autoBroadcastJoinThreshold`** — the broadcast cutoff that decides `BroadcastHashJoin` vs
  `SortMergeJoin` (`SPK-5`). `-1` disables broadcast entirely.
- **`spark.sql.shuffle.partitions`** — the post-shuffle partition count behind spill (`SPK-4`) and
  tiny-partition (`SPK-9`) problems.
- **`spark.sql.adaptive.skewJoin.enabled`**, **`spark.sql.adaptive.coalescePartitions.enabled`** — the
  AQE sub-features for `SPK-1`/`SPK-6`.
- **`spark.driver.memory`**, **`spark.executor.memory`**, **`spark.memory.fraction`** — the memory envelope
  behind OOM/spill. (Remember: the server's memory is fixed at container boot — see
  [`../common/README.md`](../common/README.md) on the two layers of constrained-vs-tuned.)
- Use this tab to catch the embarrassing case where your "fix" never took effect because the conf was set
  in the wrong layer.

### Event Timeline (Jobs / Stages)

**What it's for:** a wall-clock visualization of jobs (Jobs tab) and of every task within a stage (Stage
detail page) — each task drawn as a colored bar across executors.

**Look for / red flags:**

- **One bar far longer than the rest** in the stage timeline = the **straggler** drawn graphically (the
  visual twin of "Max ≫ Median"). Reinforces `SPK-1`.
- **Scheduler Delay** (a colored segment of each task bar) that's large → tasks waiting to be scheduled
  rather than computing; can mask as "slowness" that isn't compute-bound.
- **Gaps** between tasks/stages → driver-side stalls, GC pauses, or dependency waits, not useful work.
- Bars clustered on **one executor** while others idle → poor task locality / placement imbalance.

---

## 3. Symptom → tab/metric quick-reference

The lookup the **Detect** step of each module uses. Find your symptom on the left; the right column names
the **module** that breaks → detects → fixes → proves it. `SPK-1` is the **flagship (in progress)**; the
rest are **planned** ([`../spark/README.md`](../spark/README.md)).

| Symptom you observe | Where to look | What confirms it | Module |
|---------------------|---------------|------------------|--------|
| One task runs for minutes while the rest finish in seconds; stage "almost done" forever | **Stages → Tasks** Summary Metrics (Duration) + **Event Timeline** | **Task-time Max ≫ Median** in the percentile table, **plus** one task's **Shuffle Read Size** far above the median | **`SPK-1`** ⭐ *(in progress)* |
| Executor disappears mid-job; `FetchFailedException` downstream; job retries | **Executors** tab + executor **stderr** log | High **GC Time / Task Time** ratio, then **`container killed`** / **exit 137** (OOM) | `SPK-2` *(planned)* |
| Driver hangs or dies right after an action; `OutOfMemoryError` on the driver | **Jobs** Event Timeline (long driver gap) + driver log | OOM on a `.collect()` / `.toPandas()` / oversized broadcast pulling a generated-large frame to the driver | `SPK-3` *(planned)* |
| Stage crawls; lots of disk I/O though data "should fit" | **Stages → Tasks** Summary Metrics | Non-zero **Spill (memory)** and especially **Spill (disk)**; often `shuffle.partitions` too low | `SPK-4` *(planned)* |
| Join is far slower than expected / unexpected big shuffle | **SQL / DataFrame** plan DAG | Operator is **`SortMergeJoin`** (two `Exchange` nodes) where a **`BroadcastHashJoin`** was intended; check `autoBroadcastJoinThreshold` in **Environment** | `SPK-5` *(planned)* |
| Plan looks different run-to-run; partition counts change after shuffle | **SQL / DataFrame** plan (AQE-adjusted) + **Environment** | `AQEShuffleRead` / coalesced partitions / skew-join split present; toggled by `spark.sql.adaptive.enabled` | `SPK-6` *(planned)* |
| Query reads the whole table despite a filter on the partition column | **SQL / DataFrame** scan node | Scan **output rows ≈ table total** (full scan); empty **`PartitionFilters`** (a `CAST`/UDF on the partition col killed pruning) | `SPK-7` *(planned)* |
| Repeated use of a cached DataFrame stays slow; memory keeps churning | **Storage** tab (+ **Executors** Storage Memory) | **Fraction Cached < 100%** → eviction/recompute thrash; storage memory pinned at limit | `SPK-8` *(planned)* |
| Thousands of tiny tasks; scheduling overhead dominates a small job | **Stages** (task count) + **SQL** plan | Huge **number of tasks** / `Exchange` producing far more partitions than the data warrants (e.g. `shuffle.partitions=200` on tiny data) | `SPK-9` *(planned)* |
| "My fix didn't change anything" | **Environment** tab | The conf you set isn't in effect (wrong layer / wrong session) — see container-vs-session note above | *(all modules)* |

---

## 4. How this ties back to Break → Detect → Fix → Prove

| Step | Tool | This guide's role |
|------|------|-------------------|
| **Break** | constrained profile + `common/datagen.py` (see [`../common/README.md`](../common/README.md)) | — |
| **Detect** | **the Spark UI** (`:4040` live / `:18080` history) | **This guide** — symptom → exact tab/metric/red-flag. |
| **Diagnose** | the UI evidence above | Name the root cause from what the metric shows. |
| **Fix** | the module's production remedy (salting, AQE, partition tuning, broadcast, …) | — |
| **Prove** | [`common/metrics_diff.py`](../common/README.md) | Re-run, capture metrics, print a **before/after** table — the same numbers (task-time max-vs-median, shuffle, spill, runtime) you eyeballed here, now quantified. |

> The UI tells you **where** and **why**; `metrics_diff` proves the fix **moved the number**. Detection is
> qualitative (read the tab), proof is quantitative (read the table) — and every Phase 1 module links back
> to this file for the Detect step.
