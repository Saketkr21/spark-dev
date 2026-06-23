# spark-dev — Curriculum Plan (Phased Roadmap)

> The **how / when / module-by-module** companion to [`CURRICULUM_BRIEF.md`](./CURRICULUM_BRIEF.md).
> Read the brief first (mission, the core "small-scale simulation" trick, the
> Break→Detect→Fix→Prove pattern, guardrails). This document is the execution roadmap plus a
> per-tool deep-topic inventory.

---

## How to use this plan

- Work is organized into **Phases** (0–7). Each phase is a self-contained track folder.
- Inside a phase are **Modules**, each addressable by ID (e.g. `SPK-1`, `CDC-3`).
- You can say **"do Phase 1"**, **"do module SPK-1"**, or **"do SPK-1 through SPK-3"** and I'll build
  exactly that, then stop for review.
- **Recommended order:** Phase 0 → Phase 1 (skew flagship first, then verify) → expand outward.
  Tracks are otherwise independent, so reorder to taste.
- Every module ships as: a runnable notebook (or DAG / dbt model / connector config) in its track
  folder, a short README following Break→Detect→Fix→Prove, and reuse of the shared `common/` toolkit.

### Module status legend
`[ ]` not started · `[~]` in progress · `[x]` done

---

# PHASE 0 — Foundation, toolkit & hygiene  *(do first)*

Builds the shared machinery every later module depends on, plus repo cleanup. **No teaching content
yet** — this is the scaffolding that makes the "break it safely & measure it" loop possible.

| ID | Module | What it delivers |
|----|--------|------------------|
| `F-0` | **Modular restructure (incremental)** | Introduce per-track folders (`common/`, `spark/`, `iceberg/`, `kafka/`, `debezium/`, `quality/`, expand `dbt/`, `airflow/`). Migrate gradually; repo stays runnable. |
| `F-1` | **Resource-profile switcher** | `constrained` (tiny `driver/executor.memory`, few cores, AQE off, low broadcast threshold) vs `tuned` profiles, plus a **memory-capped Docker service (~2–3 GB)** so OOM is real but the host stays usable. A `make` flag / env var flips them. |
| `F-2` | **Synthetic data generator** (`common/datagen.py`) | `spark.range()`-based helpers to generate uniform / **skewed** / wide / high-cardinality data on the fly at any "logical size" without storing it. Skew knob (e.g. 90% of rows on one key). |
| `F-3` | **`metrics_diff` helper** (`common/metrics_diff.py`) | Captures stage/query metrics (runtime, shuffle r/w, spill, max-vs-median task time, peak memory) and prints a **before/after comparison table**. Used by every module. |
| `F-4` | **"How to read the Spark UI" guide** (`docs/spark-ui-guide.md`) | Annotated map: symptom → which tab/metric (Stages, Tasks, SQL, Executors, Storage, Timeline). Screenshots added as modules generate them. |
| `F-5` | **Symptom → cause → fix cheat-sheet** (`docs/troubleshooting.md`) | Living index linking every module's failure to its diagnosis and remedy. |
| `F-6` | **Repo hygiene pass** | Fix notebook bugs (`04` ordering + missing `os` import; `03` stale filename), remove cruft (`Untitled.ipynb`, checkpoints, committed `target/` & `.airflow_home/`), fix `.gitignore` (stop ignoring teaching DAGs), update README + CLAUDE.md. |
| `F-7` | **Sanitize / mask inherited infra** | Strip or mask all internal infra from old Airflow code (account IDs, API keys, S3, K8s conn IDs, Snowflake roles, internal domains). |

**Phase 0 exit check:** a learner can flip to the constrained profile, generate skewed data,
run a job, and read a before/after metrics table — all without freezing their laptop.

---

# PHASE 1 — Spark performance pathologies  *(flagship track)*

The bread-and-butter failures every data engineer hits. **`SPK-1` (skew) is the flagship** —
build it first, verify the whole framework, checkpoint with the owner, then continue.

| ID | Module | Scenario / what the learner breaks & fixes |
|----|--------|--------------------------------------------|
| `SPK-1` ⭐ | **Data / partition skew** | One key holds 90% of rows → one straggler task. Detect via task-time max≫median (Stages/Tasks tab). Fix: salting, AQE skew-join, repartition, broadcast. |
| `SPK-2` | **Executor OOM** | Too few partitions + skew + oversized cache → container killed. Detect: Executors tab, GC time, "container killed". Fix: partitioning, memory fractions, don't over-cache. |
| `SPK-3` | **Driver OOM** | `.collect()` / `.toPandas()` / oversized broadcast on a generated-large frame. Fix: limits, aggregation pushdown, stream/write instead of collect. |
| `SPK-4` | **Disk spill** | Wrong `shuffle.partitions` → spill. Detect spill metrics. Fix: partition tuning, memory. |
| `SPK-5` | **Join strategies** | Broadcast vs sort-merge vs shuffle-hash; what triggers each; `autoBroadcastJoinThreshold`; cost of getting it wrong. Read it in the SQL/Explain tab. |
| `SPK-6` | **AQE deep-dive** | Coalescing, skew-join split, runtime re-optimization; when AQE helps vs adds overhead/non-determinism. |
| `SPK-7` | **Partition pruning & predicate pushdown** | Why a `CAST`/UDF in the filter kills pruning; full scan vs pruned scan in the plan. |
| `SPK-8` | **Caching & persistence tradeoffs** | Storage levels, lazy `.cache()`, eviction/GC thrash, forgetting `.unpersist()`. Read the Storage tab. |
| `SPK-9` | **Shuffle internals & stages** | Stage boundaries, narrow vs wide deps, shuffle read/write, sort-shuffle, why too many tiny partitions hurt. |
| `SPK-10` | **(Deep) Spark internals sampler** | Unified/Tungsten memory, WholeStageCodegen, Catalyst rules, serialization (Kryo vs Java), speculative execution, broadcast-variable misuse. Short, demo-driven. |

---

# PHASE 2 — Lakehouse & table-format correctness (Iceberg / Delta / Parquet)

Open-table-format internals and the maintenance debt that bites in production.

| ID | Module | Scenario / what the learner breaks & fixes |
|----|--------|--------------------------------------------|
| `LAK-1` | **Format comparison** | Iceberg vs Delta vs Parquet (vs optional Hudi) on ACID, time-travel, schema evolution, upserts — extends current `01_setup_tables`. |
| `LAK-2` | **Small files & compaction** | Streaming writes → hundreds of tiny files; query slows. Detect via `.files` metadata. Fix: Iceberg `rewrite_data_files`, Delta `OPTIMIZE`, target file size. |
| `LAK-3` | **Snapshot growth & expiration** | Every write makes a snapshot → metadata bloat. Fix: `expire_snapshots`, retention props. |
| `LAK-4` | **Orphan files & GC** | Failed/partial writes leave dangling files; storage grows. Fix: `remove_orphan_files`, `gc.*` props. |
| `LAK-5` | **Manifest explosion & rewrite** | Thousands of manifests slow planning. Fix: `rewrite_manifests`, target manifest size. |
| `LAK-6` | **Schema evolution** | Add/rename/drop columns across formats; what each tolerates vs breaks. |
| `LAK-7` | **Partition & hidden partitioning + evolution** | Iceberg transforms (`year/bucket/truncate`), hidden-partition pruning, evolution only affecting new files (+ rewrite to fix old). |
| `LAK-8` | **MERGE / upsert: CoW vs MoR** | Why a 1-row MERGE rewrites a whole partition (copy-on-write); MoR tradeoffs; batching strategy. |
| `LAK-9` | **Time travel & rollback** | Recover from a bad write via snapshot/timestamp; the cost of querying old snapshots; expired-snapshot gotcha. |
| `LAK-10` | **(Deep) format internals** | Metadata pointer/version-hint, manifest column stats & pruning, format v1 vs v2 (delete files), catalog types (Hadoop vs Hive vs REST/Nessie), streaming-into-Iceberg checkpointing & exactly-once. |

---

# PHASE 3 — Kafka & Structured Streaming robustness

Messaging fundamentals + streaming correctness. Builds on existing `sales_producer` / notebook 04.

| ID | Module | Scenario / what the learner breaks & fixes |
|----|--------|--------------------------------------------|
| `KAF-1` | **Partitioning & hot partitions** | Bad key choice → one hot partition; per-partition ordering only. Detect per-partition lag in kafka-ui. Fix: key design, salting. |
| `KAF-2` | **Consumer lag & offset semantics** | Auto-commit vs manual; reprocessing/duplicates on crash. Detect via consumer-group lag. |
| `KAF-3` | **Consumer groups & rebalancing** | Kill a consumer → rebalance storm, pause, duplicate/lost work. Fix: session/heartbeat tuning, static membership. |
| `KAF-4` | **Retention & compaction** | Offline consumer past `retention.ms` → `OffsetOutOfRange`; log-compaction for state topics. |
| `KAF-5` | **Delivery semantics** | At-least-once vs exactly-once; idempotent producer, `read_committed`, idempotent sinks. |
| `KAF-6` | **Poison pill / dead-letter** | A corrupt message stalls a partition. Fix: try/catch → dead-letter topic → commit & continue. |
| `STR-1` | **Watermarking & late data** | Event-time vs processing-time; watermark tuning; observe dropped late events. |
| `STR-2` | **Idempotency, checkpoints & restart** | Kill/restart a stream safely; checkpoint recovery; dedup on restart; exactly-once into Iceberg. |
| `STR-3` | **Backpressure & micro-batch sizing** | `maxOffsetsPerTrigger` / `maxFilesPerTrigger`; trigger intervals; the streaming small-files problem (ties to `LAK-2`). |

---

# PHASE 4 — Debezium CDC track  *(new)*

Self-contained: **Postgres → Debezium (Kafka Connect) → Kafka → Spark Structured Streaming → Iceberg MERGE.**
Lives in `debezium/` with its own compose additions (Postgres + Kafka Connect), connector configs, and notebooks.

| ID | Module | Scenario / what the learner builds & breaks |
|----|--------|---------------------------------------------|
| `CDC-1` | **Local Postgres + logical replication** | Postgres with `wal_level=logical`, publication, replication slot; a source table + seed data. |
| `CDC-2` | **Debezium connector bring-up** | Kafka Connect + Debezium Postgres connector; register via Connect API; watch READ (snapshot) events land in Kafka (inspect in kafka-ui). |
| `CDC-3` | **Snapshot vs streaming phases** | Initial consistent snapshot then streaming; interrupt mid-snapshot to show restart-from-scratch; `snapshot.mode`. |
| `CDC-4` | **The CDC event envelope** | `before` / `after` / `op` (c/u/d) / `ts`; how to flatten (`ExtractNewRecordState`) and route topics. |
| `CDC-5` | **Replication slot & WAL growth** ⚠️ | Stop the connector, keep writing to Postgres → unconsumed slot retains WAL → disk grows. Detect via `pg_replication_slots`. Fix: monitor slot age, `max_slot_wal_keep_size`, keep consumers healthy. |
| `CDC-6` | **Tombstones, deletes & replica identity** | Deletes → tombstones; `REPLICA IDENTITY FULL` to capture old values; handle deletes downstream. |
| `CDC-7` | **CDC → Spark → Iceberg upsert pipeline** | Consume CDC stream, `MERGE` into an Iceberg mirror of the source; handle c/u/d; **idempotent sink** (dedup by LSN); at-least-once resilience. |
| `CDC-8` | **CDC schema evolution** | `ALTER TABLE` upstream; logical decoding doesn't emit DDL; downstream mismatch; trigger ad-hoc snapshot + evolve Iceberg schema. |
| `CDC-9` | **(Deep) failure-mode tour** | Connector restart/offset recovery, Connect vs Debezium Server, out-of-order delivery, ordering guarantees, end-to-end exactly-once reasoning. |

---

# PHASE 5 — dbt advanced & data quality (dbt tests + Great Expectations)

Expands the existing dbt project well beyond the two demo models. Data-quality labs live in `quality/`.

| ID | Module | Scenario / what the learner builds & breaks |
|----|--------|---------------------------------------------|
| `DBT-1` | **Materializations & cost** | view/table/ephemeral/incremental tradeoffs; view-chain bloat on Spark; when full-refresh is unavoidable. |
| `DBT-2` | **Incremental strategies on Spark/Iceberg** | `merge` vs `insert_overwrite` vs `append`; `unique_key`; why merge rewrites a whole Iceberg partition (ties to `LAK-8`). |
| `DBT-3` | **Late-arriving data & lookback windows** | Tight incremental window drops late rows; add a configurable lookback; cost vs freshness tradeoff. |
| `DBT-4` | **Snapshots / SCD Type 2** | `dbt_valid_from/to`; snapshot frequency vs missed intraday changes; querying current vs historical. |
| `DBT-5` | **Schema-change handling** | `on_schema_change` (fail/ignore/sync_all_columns); adding a non-nullable column; the Thrift+Iceberg classloader gotcha. |
| `DBT-6` | **Testing strategy & layering** | generic vs singular vs custom tests; staging (structural) vs marts (business-logic); `severity: warn`. |
| `DBT-7` | **Quarantine pattern** | A test finds bad rows → route them to a quarantine table via post-hook instead of failing the build. |
| `DBT-8` | **dbt-expectations + Great Expectations** | Statistical/range/distribution tests with `dbt-expectations`; standalone **GE checkpoints** for profiling/drift; when to use which; running GE against the Spark/Iceberg tables. |
| `DBT-9` | **Sources, freshness, contracts, exposures** | source freshness SLAs, model contracts/constraints, exposures + lineage/docs DAG. |
| `DBT-10` | **(Deep) macros, state & slim CI** | Jinja/macro patterns, `generate_surrogate_key`, idempotent macros, `--state` / `--select state:modified+` slim CI, deferral. |

---

# PHASE 6 — Airflow orchestration challenges  *(generic, local-runnable DAGs)*

Replace the internal `prodrat_main` DAG with teaching DAGs in `airflow/dags/` that orchestrate the
repo's own Spark/dbt jobs. Each DAG demonstrates one production concept and runs fully locally.

| ID | Module (teaching DAG) | Concept it teaches |
|----|------------------------|--------------------|
| `AF-1` | `dag_idempotency_demo` | Idempotency & deterministic tasks; re-run/backfill safely (partition overwrite / upsert keyed on `data_interval`). |
| `AF-2` | `dag_execution_model_demo` | Data-interval model; why `now()` is an antipattern; stable `data_interval_start` across retries/backfills. |
| `AF-3` | `dag_catchup_backfill_demo` | `catchup`, replaying history, `airflow dags backfill` over a date range without collisions. |
| `AF-4` | `dag_retries_sla_demo` | Retries, `retry_delay`, exponential backoff, SLA miss callbacks/alerting. |
| `AF-5` | `dag_sensors_modes_demo` | Sensor poke vs reschedule vs **deferrable/async**; freeing worker slots. |
| `AF-6` | `dag_trigger_rules_branching` | Trigger rules (all_success/all_done/none_failed), branching, short-circuit. |
| `AF-7` | `dag_dynamic_mapping` | Dynamic task mapping over a list of tables/Spark configs; TaskGroups for structure. |
| `AF-8` | `dag_xcom_limits` | XCom for small metadata vs passing URIs for large data; what NOT to push. |
| `AF-9` | `dag_assets_data_aware` | Airflow 3 Assets/Datasets — DAG B runs when DAG A produces an asset (data-aware scheduling). |
| `AF-10` | `dag_dbt_spark_e2e` (+ Cosmos) | Orchestrate the real repo: Spark extract → dbt run/test → quality gate → cleanup; Cosmos vs BashOperator; antipatterns (top-level code). |

---

# PHASE 7 — Capstone: end-to-end pipeline, incident simulator & observability

| ID | Module | What it delivers |
|----|--------|------------------|
| `CAP-1` | **End-to-end pipeline** | Postgres → Debezium → Kafka → Spark → Iceberg → dbt marts → quality gates → Airflow-orchestrated, with the constrained profile. The whole stack working together. |
| `CAP-2` | **Production Incident Simulator** ⭐ | "On-call" scenario cards: the learner is handed a broken/slow job + Spark UI + logs and must diagnose & fix like an SRE. Reuses faults from every earlier phase. The grand finale. |
| `CAP-3` | **Observability (optional)** | Reading Spark/job metrics, lineage (OpenLineage via Cosmos), a local metrics/alerting approach. Optional, masked New Relic appendix — never required to be online. |
| `CAP-4` | **Learning-path index** | README learning path: ordering, time estimates, prerequisites, "what you can diagnose after each module". |

---

# Per-tool deep-topic inventory (knowledge map)

Condensed from research, organized must-know / good-to-have / niche-deep. This is the *content
backlog* the modules above draw from — useful when fleshing out or extending any module.

## Spark
- **Must:** data/partition skew; executor OOM; driver OOM; disk spill; join strategy selection; GC pauses.
- **Good:** shuffle internals & stage boundaries; AQE (coalesce/skew-join/reoptimize); partition pruning & predicate pushdown; caching/persistence levels & eviction; broadcast-variable misuse; serialization (Kryo vs Java); Catalyst optimizer basics; dynamic-partition-overwrite pitfalls.
- **Niche/Deep:** unified (Tungsten) memory model & `memory.fraction`; WholeStageCodegen; columnar/Arrow execution; Exchange operator internals; bloom-filter joins; speculative execution; external sort/spill protocol; task locality; RDD lineage & checkpointing.

## Iceberg / Delta / Parquet
- **Must:** small files & compaction (`rewrite_data_files` / `OPTIMIZE`); snapshot growth & `expire_snapshots`; orphan files & GC; manifest explosion & `rewrite_manifests`; streaming-write metadata thrash.
- **Good:** CoW vs MoR MERGE semantics; partition evolution; hidden partitioning; time travel & rollback; metadata caching/staleness; data-file lifecycle/GC; complex-predicate pruning failures.
- **Niche/Deep:** manifest column stats; format v1 vs v2 delete files; partition-spec versioning; metadata pointer/version-hint; catalog implementations (Hadoop/Hive/REST/Nessie); branch-based commits; Z-order/clustering; streaming checkpoint↔snapshot exactly-once.

## Kafka
- **Must:** partitioning & key choice / hot partitions; consumer groups & rebalancing; consumer lag & offset commit semantics; retention & log compaction; replication factor & ISR; exactly-once (idempotent producer + transactions + idempotent sink); schema evolution/compatibility; poison-pill/dead-letter.
- **Good:** fetch tuning (throughput vs latency); monitoring metrics & alerts; per-partition vs global ordering.
- **Niche/Deep:** transactional guarantees in Kafka Connect sinks; static membership; cooperative rebalancing.

## Debezium / CDC
- **Must:** snapshot vs streaming phases; logical replication slots & **WAL/disk growth**; CDC event envelope (before/after/op/ts); tombstones & delete handling; logical decoding & slot LSN; schema/DDL evolution; replica identity (capturing old values); at-least-once + idempotent sinks; the full Postgres→Debezium→Kafka→Spark→Iceberg pipeline & its failure modes.
- **Good:** connector restart & offset recovery; updates-as-upserts (MERGE) into Iceberg.
- **Niche/Deep:** Debezium Server vs Kafka Connect; WAL-retention tuning (`max_slot_wal_keep_size`); out-of-order delivery handling; ad-hoc/incremental snapshots (signals).

## dbt
- **Must:** materializations & full-refresh cost; incremental strategies (merge/insert_overwrite/append) + `unique_key`; late-arriving data & lookback windows; snapshots/SCD2; testing strategy & layering; `on_schema_change`; quarantine (`severity: warn`).
- **Good:** sources & freshness; model contracts/constraints; dbt-utils & dbt-expectations; Great Expectations integration & when to use which; exposures, docs/lineage; environments/targets & env-var config; packages.
- **Niche/Deep:** macros/Jinja patterns & `execute` phase; surrogate keys; slim CI (`--state`/`state:modified+`) & deferral; hooks (pre/post/on-run-end); Spark/Iceberg-specific gotchas (Thrift classloader on schema change, catalog split, partition-scoped MERGE).

## Airflow
- **Must:** idempotency & deterministic tasks; data-interval model (vs `now()`); catchup & backfills; retries/backoff/SLA; sensor modes (poke/reschedule/deferrable); trigger rules; TaskGroups; dynamic task mapping; XCom & size limits; connections/variables/secrets; branching/short-circuit; scheduling/timezones; Assets/Datasets (AF3); AF3 changes & Task SDK.
- **Good:** deferrable operators & triggers; testing DAGs; antipatterns (top-level code/heavy parsing); custom hooks; partitioned idempotent loads; pools/resource limits; monitoring & callbacks.
- **Niche/Deep:** custom triggers; HA triggerer; custom executors/queues/priority; Astronomer Cosmos for dbt; event-driven scheduling/REST API; custom XCom backends (object storage); advanced backfill strategies.

---

## Suggested build sequence (default)

1. **Phase 0** (`F-0`→`F-7`) — scaffolding + cleanup.
2. **`SPK-1`** — flagship skew module; **stop & review with owner**.
3. Rest of **Phase 1** (Spark) → **Phase 2** (Iceberg).
4. **Phase 3** (Kafka/Streaming) → **Phase 4** (Debezium CDC).
5. **Phase 5** (dbt + quality) → **Phase 6** (Airflow).
6. **Phase 7** capstone (incident simulator + e2e + observability).

> Tell me which phase or module IDs to build next, and I'll implement them one batch at a time,
> keeping the repo runnable and checking in for review at each checkpoint.
