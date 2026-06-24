# CAP-2 — Production Incident Simulator ⭐

The grand finale: an **on-call drill**. Each card hands you a *symptom* — an alert that just fired,
a job that's slow or broken, and where to look — and asks you to **diagnose and fix it like an SRE**.
No root cause is named up front. Form a hypothesis, find the metric that confirms it, apply the fix,
and prove it with a number. Only then open the collapsed **🔧 Diagnosis & fix** to check yourself.

Every incident is a real pathology you already built and fixed in an earlier phase — here it's
re-framed the way you'd actually meet it in production: **symptom first, cause unknown.** The hidden
solution links back to the module that teaches it, so you can reproduce the break and the fix.

> **How to use it.** Pick a card (cold, ideally). Read only the **Page** + **Symptom**. Work the
> three questions. Write down your hypothesis and the *one measurement* that would confirm it. Then
> expand the solution. To actually reproduce a scenario, run the linked module against the live
> stack (`make up`, plus `make cdc-up` for the CDC ones).

## The scenario cards

| # | On-call page (symptom) | Exercises | Reproduce |
|---|------------------------|-----------|-----------|
| [INC-1](inc1_slow_skew.md) | A nightly job that took 2 min now runs 20+; one task won't finish | Spark **data skew** | [SPK-1](../../spark/skew/) |
| [INC-2](inc2_driver_oom.md) | Job dies mid-run; the Spark session drops with `[NO_ACTIVE_SESSION]` | Spark **driver OOM** | [SPK-3](../../spark/driver_oom/) |
| [INC-3](inc3_small_files.md) | Dashboard query crept from <1s to many seconds; row count barely moved | Iceberg **small files** | [LAK-2](../../iceberg/small_files/) |
| [INC-4](inc4_kafka_hot_partition.md) | Consumer-group lag climbing; one partition way behind, peers idle | Kafka **hot partition** | [KAF-1](../../kafka/partitioning/) · [KAF-2](../../kafka/consumer_lag/) |
| [INC-5](inc5_cdc_wal_growth.md) | Postgres disk filling fast; WAL won't recycle | CDC **replication-slot / WAL growth** | [CDC-5](../../debezium/wal_growth/) |
| [INC-6](inc6_late_data.md) | Yesterday's revenue is short; some source orders never reached the mart | dbt **late-arriving data** | [DBT-3](../../quality/dbt3_late_arriving/) |
| [INC-7](inc7_backfill_double_count.md) | A backfilled day is exactly ~2× the truth | Airflow/dbt **idempotency** | [AF-1](../../airflow/dags/af1_idempotency.py) · [DBT-2](../../quality/dbt2_incremental/) |
| [INC-8](inc8_stream_restart_dupes.md) | After a restart the streaming job re-emitted data; duplicates downstream | Streaming **checkpoints / delivery** | [STR-2](../../kafka/checkpoints/) · [KAF-6](../../kafka/poison_pill/) |

The eight span every track: **Spark** (1,2), **Iceberg** (3), **Kafka/streaming** (4,8), **CDC** (5),
**dbt/quality** (6), **orchestration** (7). Diagnose all eight cold and you can read the Spark UI, a
Kafka dashboard, `pg_replication_slots`, and a dbt/Airflow run like an on-call data engineer.

## The SRE loop (the muscle this builds)

1. **Read the signal** — what actually alerted? (latency, lag, disk, a count that's wrong)
2. **Localize** — which tab/metric/log narrows it? (Spark UI Stages, kafka-ui partitions, slot bytes)
3. **Confirm** — the *one* measurement that distinguishes the real cause from look-alikes.
4. **Fix** — the production-grade remedy (not just a restart).
5. **Prove** — the before→after number. If you can't measure the win, you haven't fixed it.

This is the same **Break → Detect → Fix → Prove** loop as every module — just run backwards, from
symptom to cause, the way production makes you do it.
