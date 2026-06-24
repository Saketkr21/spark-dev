# `capstone/` — Phase 7: end-to-end pipeline, incident simulator & observability ✅

The finale that ties all six tracks together. After the per-tool tracks (Spark, Iceberg, Kafka,
CDC, dbt/quality, Airflow), the capstone runs the **whole stack as one pipeline**, drills you on
**diagnosing incidents like an SRE**, points to a master **learning path**, and offers an optional
**observability** appendix.

| ID | Module | What it is |
|----|--------|------------|
| **CAP-1** | [End-to-end pipeline](cap1_pipeline.py) + [DAG](../airflow/dags/cap1_e2e_pipeline.py) | One Airflow DAG: Postgres→Debezium→Kafka→Spark→Iceberg MERGE + dbt marts + quality gates + cleanup — the whole stack, orchestrated |
| **CAP-2** ⭐ | [Production Incident Simulator](incident_simulator/) | 8 on-call scenario cards — symptom first, diagnose & fix like an SRE; the grand finale |
| CAP-3 | Observability *(optional)* | Local metrics / lineage options for the stack — opt-in appendix, never required to be online (see [`docs/OBSERVABILITY.md`](../docs/OBSERVABILITY.md) when present) |
| **CAP-4** | [Learning path](../docs/LEARNING_PATH.md) | The master route: all 58 modules across 6 tracks + capstone — ordering, time estimates, prerequisites, "what you can diagnose after each module" |

## CAP-1 — the end-to-end pipeline

`cap1_pipeline.py` is a staged script (`ingest` → `transform` → `quality` → `cleanup`) that reuses a
verified building block from each phase, and `airflow/dags/cap1_e2e_pipeline.py` orchestrates it
alongside the dbt marts + tests:

```
operational lineage (CDC):  cdc_ingest → spark_transform → ge_gate ─┐
analytics  lineage (dbt):   dbt_marts → dbt_test ──────────────────┤→ cleanup (always runs)
```

- **cdc_ingest** — Postgres→Debezium→Kafka→Spark→Iceberg, LSN-deduped `MERGE` (CDC-7 / Phase 4)
- **spark_transform** — a Spark aggregate mart over the Iceberg mirror (Phase 2)
- **ge_gate** — Great Expectations on the mirror, Connect-safe via `toPandas` (DBT-8 / Phase 5)
- **dbt_marts / dbt_test** — the Phase-5 dbt models + their tests (Delta side)
- **cleanup** — drop the Iceberg objects + tear down CDC (`trigger_rule=all_done`, always runs)

Run it (needs `make up` **and** `make cdc-up`):

```bash
# whole DAG, end to end (~1-2 min), verified green:
cd airflow && AIRFLOW_HOME=$PWD/.airflow_home AIRFLOW__CORE__DAGS_FOLDER=$PWD/dags \
  uv run airflow dags test cap1_e2e_pipeline 2025-03-01

# or a single stage:
PYTHONPATH=$(pwd) uv run python capstone/cap1_pipeline.py ingest   # transform | quality | cleanup
```

The two lineages (Iceberg via Spark, Delta via Thrift) reflect this stack's documented catalog split
(see [`CLAUDE.md`](../CLAUDE.md)); the **orchestration** of all the repo's capabilities under one DAG
is the lesson. The quality gate exits non-zero on a breach, so bad data never reaches promotion.

## Layout

```
capstone/
├── README.md                  # this file (Phase 7 index)
├── cap1_pipeline.py           # CAP-1 staged pipeline (ingest/transform/quality/cleanup)
└── incident_simulator/        # CAP-2 — 8 on-call scenario cards + index
```

(CAP-1's DAG lives in [`airflow/dags/cap1_e2e_pipeline.py`](../airflow/dags/cap1_e2e_pipeline.py);
CAP-4 lives in [`docs/LEARNING_PATH.md`](../docs/LEARNING_PATH.md).)
