# Troubleshooting — symptom → cause → fix cheat-sheet

> **Module F-5** of Phase 0. The **cross-cutting reference you scan when something breaks** —
> a learner debugging an exercise, or an on-call engineer staring at a slow job. Each row maps a
> *symptom you observe* to *where you see it*, the *likely cause*, the *fix*, and the *module* that
> teaches it end-to-end (Break → Detect → Fix → Prove).
>
> Read the pedagogy first: [`CURRICULUM_BRIEF.md`](./CURRICULUM_BRIEF.md) (the loop + the "break it
> safely & measure it" trick) and the module roadmap in [`CURRICULUM_PLAN.md`](./CURRICULUM_PLAN.md).

## How to use this sheet

1. **Find the symptom** in the left column of the table for the tool you're using.
2. **Open the "Where you see it" surface.** For Spark that's almost always a specific Spark UI tab/metric —
   [`spark-ui-guide.md`](./spark-ui-guide.md) is the companion that tells you *exactly* which tab, which
   percentile row, and what a bad value looks like. This sheet names the surface; that guide shows you how
   to read it.
3. **Confirm the cause, apply the fix,** then **prove it moved the number** with
   [`common/metrics_diff.py`](../common/README.md) (the before/after table every module ends on).
4. If your symptom isn't here, it's probably an **environment/setup** issue — jump to
   [§ Environment & setup gotchas](#environment--setup-gotchas) at the bottom.

> **This is a living index.** It grows as modules are built. Rows are marked **in progress** ⭐,
> **planned**, or environment-only. Today only `SPK-1` (the flagship skew module) is being built; every
> other module row below is the *intended* diagnosis pre-seeded so the structure is honest and complete.
> When a module ships it drops annotated screenshots into [`spark-ui-guide.md`](./spark-ui-guide.md) and
> its row here graduates from *planned* to *in progress / done*.

**Status legend:** ⭐ flagship · **[~]** in progress · **[ ]** planned · **[env]** environment/setup (live today).

---

## Spark — performance pathologies (Phase 1, the flagship track)

The bread-and-butter failures. Track index: [`../spark/README.md`](../spark/README.md). Almost every
"Where you see it" below is detailed in [`spark-ui-guide.md`](./spark-ui-guide.md) §2 (tab tour) and §3
(quick-reference). `SPK-1` is **in progress**; the rest are **planned**.

| Symptom (what you observe) | Where you see it (UI tab / metric / log) | Likely cause | Fix | Module |
|---|---|---|---|---|
| One task runs for minutes while the rest finish in seconds; a stage sits at "almost done" forever | **Stages → Tasks** Summary Metrics → **Duration** percentiles; **Event Timeline** one long bar | **Data skew** — one key holds ~90% of rows → one straggler task does most of the work | Salt the hot key; enable **AQE skew-join** (`spark.sql.adaptive.skewJoin.enabled`); repartition; broadcast the small side | `SPK-1` ⭐ **[~]** |
| One task's **Shuffle Read** is far above the median (corroborates the straggler) | **Stages → Tasks** Summary Metrics → **Shuffle Read Size / Records** | Skewed key landed all its rows on one partition after the shuffle | Same as above — salting / AQE skew-join split the fat partition | `SPK-1` ⭐ **[~]** |
| Executor vanishes mid-job; `FetchFailedException` downstream; job retries the stage | **Executors** tab; executor **stderr** → `container killed` / **exit 137** | **Executor OOM** — too few partitions + skew + oversized cache exceeds the (~2 GB) container | More/right-sized partitions; tune `spark.memory.fraction`; stop over-caching; raise executor memory (within the box) | `SPK-2` **[ ]** |
| Rising **GC Time** as a large fraction of task time, *before* the executor dies | **Executors** tab → **Task Time (GC Time)**; **Stages** Summary → **GC Time** | Memory pressure / churn — leading indicator of the OOM above | Reduce per-task data (more partitions); cut cache footprint; tune GC / memory fraction | `SPK-2` **[ ]** |
| Driver hangs or dies right after an action; `OutOfMemoryError` on the **driver** | **Jobs** Event Timeline (long driver-side gap) + **driver log** | `.collect()` / `.toPandas()` / oversized **broadcast** pulls a generated-large frame to the 1 GB driver | Don't collect — aggregate/`limit` first, or **write** the result; lower `autoBroadcastJoinThreshold`; stream instead of materialize | `SPK-3` **[ ]** |
| Stage crawls with heavy disk I/O though the data "should fit in memory" | **Stages → Tasks** Summary → **Spill (memory)** / **Spill (disk)** non-zero | **Disk spill** — wrong (too-low) `spark.sql.shuffle.partitions` makes each partition too big for execution memory | Raise `shuffle.partitions` so partitions fit; let AQE coalesce; add memory headroom | `SPK-4` **[ ]** |
| A join is far slower than expected / drags an unexpected big shuffle | **SQL / DataFrame** plan → join operator is **`SortMergeJoin`** (two `Exchange`) where a broadcast was intended | Wrong **join strategy** — small side wasn't broadcast (threshold too low / stale stats / `-1`) | Raise/restore `spark.sql.autoBroadcastJoinThreshold`; `broadcast()` hint; refresh stats so Spark picks **`BroadcastHashJoin`** | `SPK-5` **[ ]** |
| Physical plan differs run-to-run; post-shuffle partition counts change unexpectedly | **SQL / DataFrame** (AQE-adjusted plan: `AQEShuffleRead`, coalesced counts) + **Environment** | **AQE** runtime re-optimization — usually a win, but adds non-determinism / overhead in some shapes | Understand it's expected; pin behavior by toggling `spark.sql.adaptive.enabled` / sub-features when you need determinism | `SPK-6` **[ ]** |
| Query reads the **whole table** despite a filter on the partition column | **SQL / DataFrame** scan node → output rows ≈ table total; empty **`PartitionFilters`** | **Lost partition pruning** — a `CAST`/function/UDF on the partition column defeats predicate pushdown | Filter on the raw partition column (no cast/UDF); push the predicate so `PartitionFilters` / `PushedFilters` populate | `SPK-7` **[ ]** |
| A repeatedly-used cached DataFrame stays slow; memory keeps churning | **Storage** tab → **Fraction Cached < 100%**; **Executors** Storage Memory pinned | **Cache eviction / GC thrash** — dataset doesn't fit, partitions evict & recompute; or a forgotten `.unpersist()` | Right-size what you cache; pick a fitting storage level (e.g. `MEMORY_AND_DISK`); **`.unpersist()`** when done | `SPK-8` **[ ]** |
| `.cache()` "did nothing" — Storage tab is empty | **Storage** tab (empty when you expected an entry) | `.cache()` is **lazy** — nothing materializes until an action runs | Trigger an action (`.count()`) to materialize, then re-check Storage | `SPK-8` **[ ]** |
| Thousands of tiny tasks; scheduling overhead dominates a small job | **Stages** (huge task count) + **SQL** plan (`Exchange` over-partitioning) | **Too many tiny shuffle partitions** — `spark.sql.shuffle.partitions=200` on tiny data | Lower `shuffle.partitions` to fit the data; let **AQE coalesce** post-shuffle partitions | `SPK-9` **[ ]** |
| Mysterious slow serialization / large shuffle payloads; or duplicate task execution | **Stages** Summary (Serialization / shuffle sizes); **Environment** (serializer); Tasks (speculative dupes) | **Java serialization** instead of Kryo; or **speculative execution** firing on a skewed (not slow-node) stage | Switch to **Kryo** (`spark.serializer`); register classes; understand speculation masks skew rather than fixing it | `SPK-10` **[ ]** |
| "My fix didn't change anything" | **Environment** tab — the conf isn't actually in effect | Conf set in the wrong layer / wrong session (container memory is fixed at boot) | Verify the value in **Environment**; set session confs via `apply_profile(...)`, container memory via the profile/compose layer | *(all Spark modules)* |

---

## Lakehouse — Iceberg / Delta / Parquet (Phase 2)

Table-format maintenance debt. Track folder: [`../iceberg/README.md`](../iceberg/README.md). All **planned**.
"Where you see it" leans on table **metadata** (`.files`, `.snapshots`, `.manifests`) and query planning time
more than the Spark UI.

| Symptom (what you observe) | Where you see it (metadata / metric / log) | Likely cause | Fix | Module |
|---|---|---|---|---|
| Queries on a frequently-written table get steadily slower; scans touch hundreds of files | Iceberg `table.files` / Delta file listing; **SQL** scan node file count | **Small-files problem** — streaming/many small writes emit tiny files | Iceberg **`rewrite_data_files`** / Delta **`OPTIMIZE`**; set a target file size; compact on a schedule | `LAK-2` **[ ]** |
| Table metadata/listing grows huge; query *planning* slows even on small data | `table.snapshots` count keeps climbing; metadata dir bloats | **Snapshot growth** — every write creates a new snapshot, none expired | **`expire_snapshots`** + retention props (`history.expire.*`); schedule expiry | `LAK-3` **[ ]** |
| Storage keeps growing even after deletes/compaction; files not referenced by any snapshot | Storage size ≫ live data; orphan-file scan reports danglers | **Orphan files** — failed/partial writes leave files no snapshot points to | **`remove_orphan_files`** (mind the age threshold); tune `gc.*` props | `LAK-4` **[ ]** |
| Planning time balloons; thousands of manifests scanned per query | `table.manifests` count very high | **Manifest explosion** — many small manifests accumulate | **`rewrite_manifests`**; set target manifest size; pair with data compaction | `LAK-5` **[ ]** |
| A write/read fails or silently drops columns after an upstream schema change | Job error or column mismatch vs expected schema | **Schema-evolution breakage** — add/rename/drop not tolerated by the format/reader path | Use format-native evolution (Iceberg add/rename/drop by field-id); align reader expectations; see also `DBT-5` | `LAK-6` **[ ]** |
| A **1-row** `MERGE`/update rewrites an entire partition's worth of files | Output file size ≫ rows changed; long write for a tiny change | **Copy-on-write** MERGE rewrites whole data files containing matched rows | Batch updates; consider **merge-on-read** (MoR) tradeoffs; partition so changes localize | `LAK-8` **[ ]** |

---

## Kafka & Structured Streaming (Phase 3)

Messaging + streaming correctness. Track folder: [`../kafka/README.md`](../kafka/README.md). All **planned**.
"Where you see it" is **kafka-ui** (`:8080`) for broker/consumer state and the **Spark UI / Streaming query**
for the consumer side.

| Symptom (what you observe) | Where you see it (kafka-ui / metric / log) | Likely cause | Fix | Module |
|---|---|---|---|---|
| One partition's lag grows while others stay flat; throughput capped on one consumer | **kafka-ui** per-partition lag; uneven partition sizes | **Hot partition** — bad key choice routes most messages to one partition (ordering is per-partition only) | Redesign the partition key; add salting/more keys; size partition count to parallelism | `KAF-1` **[ ]** |
| Consumer falls behind; on crash it reprocesses or skips messages | **kafka-ui** consumer-group **lag**; offset reset behavior | **Offset semantics** — auto-commit vs manual; committing before/after processing | Commit **after** successful processing; choose `earliest`/`latest` deliberately; make the sink idempotent | `KAF-2` **[ ]** |
| Killing/adding a consumer triggers a pause + duplicate or lost work across the group | **kafka-ui** group state flapping between rebalances | **Rebalance storm** — short session timeout / eager rebalancing | Tune `session.timeout.ms` / `heartbeat.interval.ms`; **static membership**; cooperative rebalancing | `KAF-3` **[ ]** |
| Consumer offline a while, then on restart throws `OffsetOutOfRange` | Consumer log `OffsetOutOfRangeException`; **kafka-ui** retention vs committed offset | Offline **past `retention.ms`** — the committed offset aged out of the log | Increase retention for the topic; use **log compaction** for state topics; handle reset policy explicitly | `KAF-4` **[ ]** |
| A single bad message stalls a whole partition; consumer loops on it | Consumer log repeating parse/deser error on one offset | **Poison pill** — uncaught deserialization/processing error blocks commit | try/catch → route to a **dead-letter topic** → commit & continue | `KAF-6` **[ ]** |
| Late events silently disappear from windowed aggregates | **Spark UI** Streaming query stats; output missing late rows | **Watermark** dropped events older than `(max event time − watermark delay)` | Tune the watermark delay to your real lateness; accept the freshness/correctness tradeoff | `STR-1` **[ ]** |
| After a stream restart, rows are duplicated or a window recomputes oddly | Streaming query checkpoint dir; duplicate output rows | **Checkpoint/restart** semantics — at-least-once without an idempotent sink | Keep the checkpoint; dedup on a key/LSN; write **exactly-once into Iceberg** | `STR-2` **[ ]** |
| Streaming sink produces hundreds of tiny files (ties to `LAK-2`) | Output table file count climbs each micro-batch | **Micro-batch sizing** — small triggers / unbounded input per batch | `maxOffsetsPerTrigger` / `maxFilesPerTrigger`; longer trigger interval; compact downstream | `STR-3` **[ ]** |

---

## Debezium / CDC (Phase 4)

Postgres → Debezium (Kafka Connect) → Kafka → Spark → Iceberg. Track folder:
[`../debezium/README.md`](../debezium/README.md). All **planned**. ⚠️ The replication-slot row is the
laptop-disk hazard of this track.

| Symptom (what you observe) | Where you see it (Postgres / Connect / log) | Likely cause | Fix | Module |
|---|---|---|---|---|
| Postgres disk grows steadily while a connector is stopped but the DB keeps taking writes ⚠️ | `SELECT * FROM pg_replication_slots` — slot **inactive**, `restart_lsn` frozen, WAL retained | **Replication-slot / WAL growth** — an unconsumed slot pins WAL so it can't be recycled | Restart/heal the consumer so the slot advances; drop unused slots; cap with **`max_slot_wal_keep_size`**; monitor slot age | `CDC-5` ⚠️ **[ ]** |
| Interrupting the connector mid-snapshot restarts the snapshot from scratch | Connect log restarts READ phase; snapshot progress lost | **Snapshot restart** — default snapshot isn't resumable | Let the initial snapshot finish; choose an appropriate **`snapshot.mode`**; consider incremental snapshots (signals) | `CDC-3` **[ ]** |
| Downstream never sees DELETEs, or deletes carry no old values | Kafka event has `op=d` but null `before`; or no event at all | **Missing `REPLICA IDENTITY FULL`** — Postgres doesn't emit old row image | `ALTER TABLE … REPLICA IDENTITY FULL`; handle tombstones; MERGE the delete downstream | `CDC-6` **[ ]** |
| An upstream `ALTER TABLE` breaks the downstream consumer / Iceberg schema | Consumer schema mismatch; new/missing columns after DDL | **No DDL in logical decoding** — Debezium streams data changes, not DDL | Trigger an ad-hoc snapshot; **evolve the Iceberg schema** to match; version the contract | `CDC-8` **[ ]** |

---

## dbt (Phase 5)

Expanded dbt project + data-quality labs. Track folder: [`../dbt/`](../dbt/). All **planned**.
Some rows reuse the live Thrift/Iceberg gotchas below.

| Symptom (what you observe) | Where you see it (dbt log / table / Spark UI) | Likely cause | Fix | Module |
|---|---|---|---|---|
| An incremental model silently drops late-arriving rows | Row counts vs source; missing recent-but-late records | **Tight incremental window** — `WHERE event_date >= max(...)` excludes late data | Add a configurable **lookback window**; accept the cost/freshness tradeoff | `DBT-3` **[ ]** |
| Adding/changing a column makes the incremental run fail or ignore the column | dbt run error, or column missing in target; Thrift classloader error | **`on_schema_change`** default + the **Thrift + Iceberg classloader** gotcha (see below) | Set `on_schema_change: sync_all_columns` (or `append_new_columns`); build Iceberg in notebooks, Hive/Delta via dbt's `spark_catalog` | `DBT-5` **[ ]** |
| A failing test aborts the whole `dbt build`, blocking good models | `dbt build` stops at the failed test | **Test severity** — a data issue treated as a hard build failure | **Quarantine pattern**: post-hook routes bad rows aside; `severity: warn` for non-blocking checks | `DBT-7` **[ ]** |

---

## Airflow (Phase 6)

Generic local teaching DAGs orchestrating the repo's own jobs. Track folder:
[`../airflow/README.md`](../airflow/README.md). All **planned**.

| Symptom (what you observe) | Where you see it (Airflow UI / log) | Likely cause | Fix | Module |
|---|---|---|---|---|
| Re-running or backfilling a task double-writes / corrupts data | Duplicated rows after a retry/backfill; Grid shows re-runs | **Non-idempotent task** — appends instead of overwriting/upserting the interval | Make tasks idempotent: partition-overwrite or upsert keyed on **`data_interval`** | `AF-1` **[ ]** |
| A backfill processes "today" instead of the historical date for each run | Task log shows wall-clock `now()` regardless of run date | **`now()` antipattern** — using wall-clock instead of the run's data interval | Use **`data_interval_start` / `data_interval_end`**; keep tasks deterministic across retries/backfills | `AF-2` **[ ]** |
| The scheduler is slow; DAGs take long to appear or update | Scheduler / DAG-processor parse times high | **Heavy top-level DAG code** — expensive imports/queries run on every parse | Move heavy work **inside tasks**; keep top-level code light; defer connections to execution time | `AF-10` **[ ]** |

---

## Environment & setup gotchas

These are **live today** (folded in from `CLAUDE.md` → "Common Issues & Fixes"). They aren't a teaching
module's pathology — they're the friction you hit standing the stack up. See `CLAUDE.md` for the full
architecture rationale.

| Symptom (what you observe) | Where you see it | Likely cause | Fix |
|---|---|---|---|
| **`SCHEMA_NOT_FOUND`** when a Thrift client (dbt / JDBC) connects | dbt / beeline connection error on connect | Iceberg's Hadoop catalog doesn't auto-create `default` — namespaces are filesystem directories that must exist | `scripts/docker-entrypoint.sh` pre-creates `default,analytics,staging,marts,seeds` under `.tmp/local_iceberg_warehouse/`. If missing, recreate or `make clean` and restart |
| **`NoClassDefFoundError`** / class-not-found for Iceberg or Delta | Executor/Thrift stderr; query fails at runtime | Thrift Server's classloader can't see JARs loaded via `spark.jars.packages` (HiveServer2 isolation bug) | JARs must be on the **system classpath** — baked into `$SPARK_HOME/jars/` in the multi-stage Docker build, **not** `spark.jars.packages` |
| dbt: **"thrift connection method requires additional dependencies"** | `dbt debug` / `dbt run` startup error | The PyHive extra for the thrift method isn't installed | Install **`dbt-spark[PyHive]`** (already pinned in `pyproject.toml`); `uv sync` |
| dbt can't find Iceberg tables / creates in the "wrong" catalog | Tables appear under `spark_catalog`, not `iceberg_catalog` | **By design** — dbt's default catalog is `spark_catalog` (Delta/Hive) to dodge the Iceberg+Thrift classloader issue | Notebooks address Iceberg explicitly (`iceberg_catalog.my_database.xxx`); don't rely on the default catalog for Iceberg from dbt |
| Very slow first container start (Ivy resolving JARs at boot) | Long startup; Ivy download logs on boot | JARs being resolved at **runtime** instead of from the image | Should not happen — JARs are baked in. If it does, `spark.jars.packages` leaked into `spark-defaults.conf`; remove it and rebuild |
| Host laptop becomes sluggish / a container is OOM-killed during a heavy module | Host monitor; container `exit 137` | Running an OOM/spill module on the default (tuned, ~3 GB) profile, or too many services up | Use **`make up-constrained`** (~2 GB cap) for OOM/spill modules; stop optional services (history server, kafka-ui) on 8 GB hosts |
| Disk filling up from `.tmp/` (warehouses, checkpoints, event logs) | `.tmp/` size grows across runs | Generated data, streaming checkpoints, and event logs accumulate | **`make clean`** (`rm -rf .tmp`) to recover; streams **auto-stop** so they don't run unbounded |

> **Laptop-safety reminders** (non-negotiable, per the [brief](./CURRICULUM_BRIEF.md)): every "break it"
> exercise is **bounded and reversible** — capped container memory (`make up-constrained`), **auto-stopping
> streams**, and **`make clean`** recovery. Failure is *contained inside the container*, not real.

---

## See also

- [`spark-ui-guide.md`](./spark-ui-guide.md) — **the "where do I see it" companion.** Every Spark row's
  surface (tab, percentile row, red flag) is detailed there.
- [`CURRICULUM_BRIEF.md`](./CURRICULUM_BRIEF.md) — mission, the Break → Detect → Fix → Prove loop, laptop-safety rules.
- [`CURRICULUM_PLAN.md`](./CURRICULUM_PLAN.md) — full phase/module roadmap and per-tool deep-topic inventory.
- `CLAUDE.md` (repo root) — architecture rationale behind the environment gotchas above.
