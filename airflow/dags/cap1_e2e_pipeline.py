"""CAP-1 — End-to-end capstone pipeline, Airflow-orchestrated (the whole stack together).

The grand integration: one DAG drives every layer the curriculum built, with real task
boundaries, a quality gate that can stop promotion, and a teardown that always runs.

    ┌─ operational lineage (CDC) ───────────────────────────────────────────┐
    │  cdc_ingest:  Postgres → Debezium → Kafka → Spark → Iceberg MERGE       │  (CDC-7 / Phase 4)
    │  spark_transform: Spark aggregate mart over the Iceberg mirror          │  (Phase 2 lakehouse)
    │  ge_gate:     Great Expectations on the mirror (Connect-safe toPandas)  │  (DBT-8 / Phase 5)
    └────────────────────────────────────────────────────────────────────────┘
    ┌─ analytics lineage (dbt on Delta) ────────────────────────────────────┐
    │  dbt_marts:   dbt build fct_orders + the clean/quarantine marts         │  (DBT-2/7 / Phase 5)
    │  dbt_test:    dbt test the marts (structural + business assertions)      │  (DBT-6 / Phase 5)
    └────────────────────────────────────────────────────────────────────────┘
                              ↓ (both branches)
    cleanup:  drop the Iceberg objects + tear down CDC  (trigger_rule=all_done — always runs)

Both lineages run under one DAG because that's how a real platform looks: an operational
CDC replica (Iceberg) *and* analytics transforms (dbt/Delta) on the same orchestrator. The two
catalogs are the documented reality of this stack (Iceberg via Spark, Delta via Thrift — see
CLAUDE.md); the orchestration is the lesson. The capstone CDC/Iceberg/GE stages live in
`capstone/cap1_pipeline.py`; the dbt stages reuse the Phase-5 project.

**Antipatterns it avoids:** all heavy work is inside tasks (no top-level DB/Spark calls — AF-10);
the CDC ingest is idempotent/LSN-deduped so a retry can't double-apply (AF-1 / CDC-7); the quality
gate fails the task (non-zero exit) so bad data never reaches `cleanup`-and-promote.

Prerequisites: `make up` AND `make cdc-up` (Postgres + Kafka Connect). The brief's "constrained
profile" note: run the heavy version with `make up-constrained` to feel back-pressure; the data
here is tiny so it completes on either profile.

Run it (end to end, ~1-2 min):
    cd airflow && AIRFLOW_HOME=$PWD/.airflow_home AIRFLOW__CORE__DAGS_FOLDER=$PWD/dags \\
      uv run airflow dags test cap1_e2e_pipeline 2025-03-01
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
PY = f"cd {REPO} && PYTHONPATH={REPO} uv run python capstone/cap1_pipeline.py"
DBT = f"cd {REPO}/dbt && set -a && . ./.env && set +a && uv run dbt"

with DAG(
    dag_id="cap1_e2e_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["airflow-curriculum", "CAP-1", "capstone", "e2e"],
    doc_md=__doc__,
):
    # Operational lineage: CDC → Iceberg → transform → quality gate
    cdc_ingest = BashOperator(task_id="cdc_ingest", bash_command=f"{PY} ingest")
    spark_transform = BashOperator(task_id="spark_transform", bash_command=f"{PY} transform")
    ge_gate = BashOperator(task_id="ge_gate", bash_command=f"{PY} quality")

    # Analytics lineage: dbt marts + tests (Phase 5, on Delta)
    dbt_marts = BashOperator(
        task_id="dbt_marts",
        bash_command=f"{DBT} build -s fct_orders orders_clean orders_quarantine",
    )
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT} test -s orders_clean orders_quarantine",
    )

    # Teardown always runs (even if a gate fails) so the next run starts clean.
    cleanup = BashOperator(task_id="cleanup", bash_command=f"{PY} cleanup", trigger_rule="all_done")

    cdc_ingest >> spark_transform >> ge_gate >> cleanup
    dbt_marts >> dbt_test >> cleanup
