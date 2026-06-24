# Learning Path — the production-challenges curriculum (CAP-4)

The master route through the whole curriculum: **58 hands-on modules across 6 tracks + a capstone**,
each following **Break → Detect → Fix → Prove**. You don't just run happy-path code — you break a
real system at small scale, watch it fail in the Spark UI / a dashboard, diagnose the root cause,
fix it, and **measure** the improvement.

How it stays laptop-safe: **generate, don't store** (synthesize billions of rows lazily with
`spark.range()` + `rand()`/`hash()`), **shrink the box, not the data** (a memory-capped container so
OOM/spill are real but the host stays usable), and **toggle the safety nets** (AQE / broadcast /
checkpoints on and off). See [`CURRICULUM_BRIEF.md`](CURRICULUM_BRIEF.md) for the philosophy and
[`CURRICULUM_PLAN.md`](CURRICULUM_PLAN.md) for the full module catalogue.

---

## Setup (once)

```bash
uv sync                 # Python deps (Spark Connect client, dbt, GE, kafka-python, psycopg2)
make up                 # Spark + Kafka + history + kafka-ui   (tuned ~3 GB profile)
make jupyter            # JupyterLab at :8888 — the notebook tracks (Spark, Iceberg, Kafka, CDC)
# opt-in, per track:
make cdc-up             # + Postgres + Kafka Connect (Phase 4 CDC; ~1.3 GB)
make airflow-up         # local Airflow 3 at :5000 (Phase 6 orchestration)
cd dbt && source .env && dbt deps   # Phase 5 dbt packages
```

Dashboards: Spark UI http://localhost:4040 · History http://localhost:18080 · kafka-ui
http://localhost:8080 · Kafka Connect http://localhost:8083 · Airflow http://localhost:5000.
Recover anytime with `make clean` (wipes `.tmp/`). For the OOM/spill modules use
`make up-constrained` (~2 GB) so failure is real but the laptop stays responsive.

Two reading guides sit alongside this path: [`spark-ui-guide.md`](spark-ui-guide.md) (symptom → which
UI tab/metric) and [`troubleshooting.md`](troubleshooting.md) (symptom → cause → fix cheat-sheet).

---

## Recommended order & what you can diagnose after each module

Times are rough hands-on estimates. Each module is self-contained; the **prereq** column is the
minimum concept you want first, not a hard gate.

### Phase 1 · `spark/` — performance pathologies  (start here)
Prereq: none. Run on `make up`; flip to `make up-constrained` for SPK-2/3/4.

| Module | min | After it you can diagnose… |
|--------|----:|----------------------------|
| [SPK-1 data skew](../spark/skew/) ⭐ | 25 | a straggler task doing 40× the median work; fix via AQE skew-join / salting |
| [SPK-2 executor OOM](../spark/executor_oom/) | 15 | container/executor OOM vs spill; partition sizing |
| [SPK-3 driver OOM](../spark/driver_oom/) | 15 | a `.collect()`/`toPandas()` that kills the driver (and the Connect session) |
| [SPK-4 disk spill](../spark/spill/) | 15 | shuffle/aggregation spill in the Stages metrics |
| [SPK-5 join strategies](../spark/joins/) | 20 | broadcast vs sort-merge vs shuffle-hash; why the planner chose wrong |
| [SPK-6 AQE deep-dive](../spark/aqe/) | 20 | what AQE rewrites at runtime (coalesce, skew, join switch) |
| [SPK-7 partition pruning & pushdown](../spark/pruning/) | 15 | a full scan that should have pruned; predicate/projection pushdown |
| [SPK-8 caching & persistence](../spark/caching/) | 15 | recomputation vs cache; storage levels; when cache hurts |
| [SPK-9 shuffle internals & stages](../spark/shuffle/) | 20 | stage boundaries, shuffle read/write, partition counts |
| [SPK-10 deep internals](../spark/internals/) | 20 | Catalyst/AQE plan reading; the physical plan |

### Phase 2 · `iceberg/` — lakehouse / table-format correctness
Prereq: SPK-1 (reading the Spark UI). Run on `make up`.

| Module | min | After it you can diagnose… |
|--------|----:|----------------------------|
| [LAK-1 format comparison](../iceberg/format_comparison/) | 20 | Iceberg vs Delta vs Parquet (ACID, time travel, schema evo, MERGE) |
| [LAK-2 small files](../iceberg/small_files/) ⭐ | 20 | a table slow from thousands of tiny files; fix via `rewrite_data_files` |
| [LAK-3 snapshot growth](../iceberg/snapshots/) | 15 | unbounded snapshots; `expire_snapshots` |
| [LAK-4 orphan files & GC](../iceberg/orphan_files/) | 15 | unreferenced files; `remove_orphan_files` (24h guard) |
| [LAK-5 manifest explosion](../iceberg/manifests/) | 15 | slow planning from too many manifests; `rewrite_manifests` |
| [LAK-6 schema evolution](../iceberg/schema_evolution/) | 20 | add/rename/drop/widen by field-id vs positional Parquet |
| [LAK-7 partitioning & evolution](../iceberg/partitioning/) | 20 | hidden partitioning, pruning, partition-spec evolution |
| [LAK-8 MERGE: CoW vs MoR](../iceberg/merge_cow_mor/) | 20 | why a 1-row MERGE rewrites a whole partition |
| [LAK-9 time travel & rollback](../iceberg/time_travel/) | 15 | recover a bad write; the expired-snapshot gotcha |
| [LAK-10 deep internals](../iceberg/internals/) | 20 | metadata pointer, manifest stats, v1/v2 deletes |

### Phase 3 · `kafka/` — Kafka & Structured Streaming robustness
Prereq: SPK-1. Run on `make up` (Kafka is part of the base stack).

| Module | min | After it you can diagnose… |
|--------|----:|----------------------------|
| [KAF-1 partitioning & hot partitions](../kafka/partitioning/) ⭐ | 15 | one partition flooded by a dominant key; rekey/salt |
| [KAF-2 consumer lag & offsets](../kafka/consumer_lag/) | 15 | lag = end − committed; auto vs manual commit; reprocess/loss |
| [KAF-3 rebalancing](../kafka/consumer_groups/) | 15 | rebalance storms; static membership / cooperative-sticky |
| [KAF-4 retention & compaction](../kafka/retention/) | 15 | `OffsetOutOfRange` on a stale consumer; log compaction |
| [KAF-5 delivery semantics](../kafka/delivery_semantics/) | 20 | at-least-once vs EOS; idempotent producer, `read_committed` |
| [KAF-6 poison pill / dead-letter](../kafka/poison_pill/) | 15 | a corrupt record stalling a partition; dead-letter routing |
| [STR-1 watermarking & late data](../kafka/watermarking/) | 20 | event vs processing time; a dropped-late-event |
| [STR-2 checkpoints & restart](../kafka/checkpoints/) | 20 | resume-from-checkpoint; dedup on restart; exactly-once into Iceberg |
| [STR-3 backpressure](../kafka/backpressure/) | 15 | `maxOffsetsPerTrigger`; the streaming small-files problem |

### Phase 4 · `debezium/` — Change Data Capture
Prereq: Phase 3 (Kafka), LAK-8 (MERGE). Run `make cdc-up` first.

| Module | min | After it you can diagnose… |
|--------|----:|----------------------------|
| [CDC-1 logical replication](../debezium/postgres_setup/) | 20 | `wal_level`, publications, replication slots |
| [CDC-2 connector bring-up](../debezium/connector_bringup/) | 20 | registering Debezium via the Connect REST API; snapshot→stream |
| [CDC-3 snapshot modes](../debezium/snapshot_modes/) | 15 | `snapshot.mode`; restart-from-scratch |
| [CDC-4 event envelope](../debezium/event_envelope/) | 15 | before/after/op/ts; `ExtractNewRecordState` |
| [CDC-5 WAL/slot growth](../debezium/wal_growth/) ⭐⚠️ | 20 | a slot pinning WAL → disk fills; `max_slot_wal_keep_size` |
| [CDC-6 deletes & replica identity](../debezium/deletes_tombstones/) | 15 | tombstones; `REPLICA IDENTITY FULL` |
| [CDC-7 Spark→Iceberg MERGE](../debezium/cdc_to_iceberg/) ⭐ | 25 | building an idempotent (LSN-deduped) upsert mirror |
| [CDC-8 schema evolution](../debezium/schema_evolution/) | 15 | DDL not in the stream; evolving the sink |
| [CDC-9 failure-mode tour](../debezium/failure_modes/) | 20 | offset recovery, ordering, effectively-once reasoning |

### Phase 5 · `quality/` — dbt advanced & data quality
Prereq: SQL/dbt basics. Run `cd dbt && source .env && dbt deps` first.

| Module | min | After it you can diagnose… |
|--------|----:|----------------------------|
| [DBT-1 materializations](../quality/dbt1_materializations/) | 15 | view/table/ephemeral/incremental cost tradeoffs |
| [DBT-2 incremental strategies](../quality/dbt2_incremental/) ⭐ | 20 | merge vs insert_overwrite vs append; `unique_key` idempotency |
| [DBT-3 late-arriving & lookback](../quality/dbt3_late_arriving/) ⭐ | 20 | rows silently dropped by a tight incremental window |
| [DBT-4 SCD2 snapshots](../quality/dbt4_snapshots_scd2/) | 15 | `dbt_valid_from/to`; missed intraday changes |
| [DBT-5 schema-change](../quality/dbt5_schema_change/) | 15 | `on_schema_change`; a column added/removed across runs |
| [DBT-6 testing & layering](../quality/dbt6_testing_strategy/) | 15 | generic/singular/custom tests; `severity: warn` |
| [DBT-7 quarantine](../quality/dbt7_quarantine/) | 15 | routing bad rows out instead of failing the build |
| [DBT-8 dbt-expectations + GE](../quality/dbt8_expectations_ge/) | 20 | statistical/distribution checks; when dbt-tests vs GE |
| [DBT-9 sources/freshness/contracts](../quality/dbt9_sources_contracts/) | 20 | a freshness SLA breach; an enforced contract |
| [DBT-10 macros & slim CI](../quality/dbt10_macros_slim_ci/) | 20 | surrogate-key macros; `state:modified+` slim CI |

### Phase 6 · `airflow/` — orchestration
Prereq: AF-1→AF-3 give the data-interval foundation. Run `make airflow-up` (or `airflow dags test`).

| Module | min | After it you can diagnose… |
|--------|----:|----------------------------|
| [AF-1 idempotency](../airflow/dags/af1_idempotency.py) ⭐ | 15 | a re-run/backfill that double-writes |
| [AF-2 execution model](../airflow/dags/af2_execution_model.py) | 15 | `now()` antipattern vs the stable data interval |
| [AF-3 catchup/backfill](../airflow/dags/af3_catchup_backfill.py) | 15 | replaying history without collisions |
| [AF-4 retries/SLA](../airflow/dags/af4_retries_sla.py) | 15 | retry/backoff policy; deadline alerting |
| [AF-5 sensor modes](../airflow/dags/af5_sensor_modes.py) | 15 | poke vs reschedule vs deferrable; slot starvation |
| [AF-6 trigger rules/branching](../airflow/dags/af6_trigger_rules_branching.py) | 15 | a join skipped by the wrong trigger rule |
| [AF-7 dynamic mapping](../airflow/dags/af7_dynamic_mapping.py) | 15 | mapping over a runtime list; TaskGroups |
| [AF-8 XCom limits](../airflow/dags/af8_xcom_limits.py) | 10 | XCom bloat; pass URIs not payloads |
| [AF-9 assets/data-aware](../airflow/dags/af9_assets_data_aware.py) | 15 | producer→asset→consumer data-aware scheduling |
| [AF-10 dbt+Spark e2e](../airflow/dags/af10_dbt_spark_e2e.py) ⭐ | 20 | orchestrating real dbt/Spark/GE; Cosmos vs Bash; top-level-code |

### Phase 7 · `capstone/` — put it all together
Prereq: everything above (or at least the ⭐ flagships). `make up` + `make cdc-up`.

| Module | min | What it is |
|--------|----:|------------|
| [CAP-1 end-to-end pipeline](../capstone/) | 30 | one Airflow DAG: Postgres→Debezium→Kafka→Spark→Iceberg + dbt marts + quality gates + cleanup |
| [CAP-2 incident simulator](../capstone/incident_simulator/) ⭐ | open-ended | 8 on-call scenario cards — diagnose & fix like an SRE; the grand finale |
| CAP-3 observability *(optional)* | — | local metrics/lineage options for the stack (opt-in appendix) |
| CAP-4 learning path | — | this document |

---

## Routes through the curriculum

- **Full path (recommended):** Phase 1 → 2 → 3 → 4 → 5 → 6 → 7, in order. ~20–25 hours hands-on.
- **"I work on batch Spark":** Phase 1 (all) → Phase 2 → LAK-8/CDC-7 for MERGE → CAP-2 cards INC-1/2/3.
- **"I work on streaming":** SPK-1 → Phase 3 (all) → Phase 4 (CDC) → CAP-2 cards INC-4/5/8.
- **"I work on the warehouse / analytics engineering":** Phase 2 (LAK-1/2/6/8) → Phase 5 (all) →
  Phase 6 (AF-1/2/3/10) → CAP-2 cards INC-6/7.
- **"On-call prep / interview drill":** skim each track's README, then go straight to
  [CAP-2 the incident simulator](../capstone/incident_simulator/) and diagnose cold.

Every flagship (⭐) is a good standalone session if you only have an hour.
