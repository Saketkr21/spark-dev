# Claude Code — Project Context

## What This Repo Is

A Docker-based local environment for learning Apache Spark 4.0.2, Iceberg, Delta Lake,
Kafka Structured Streaming, dbt-core, and Airflow. It is being grown into a hands-on
**Data Engineering production-challenges curriculum**: learners break real systems at small
scale, watch them fail in the Spark UI / tool dashboards, diagnose the root cause, fix it,
and measure the improvement — all on an ordinary laptop without making it unusable.

- The curriculum spec lives in `docs/CURRICULUM_BRIEF.md` (mission / rules / the "shrink the
  box, generate the data" trick) and `docs/CURRICULUM_PLAN.md` (phased roadmap + module IDs).
- Every challenge module follows **Break → Detect → Fix → Prove** and reuses the shared
  `common/` toolkit. Status is tracked per-module in each track's README.

Everything runs locally via Docker Compose (Spark + Kafka) plus a local JupyterLab and a
local Airflow.

## Architecture (Critical to Understand)

One Docker container (`spark-connect`) runs a **Spark Thrift Server** (HiveServer2) with the **Spark Connect plugin** enabled in the same JVM. This means:

- **Notebooks** connect via Spark Connect gRPC at `localhost:15002`
- **dbt** connects via Thrift JDBC at `localhost:10000`
- **Both share the same SparkContext** → single Spark UI at `localhost:4040`

This is NOT a standard Spark Connect-only setup. The entrypoint runs `start-thriftserver.sh` with `spark.plugins=org.apache.spark.sql.connect.SparkConnectPlugin`.

The server runs in **local mode** (`--master local[*]`), so the driver JVM is also the
executor — `spark.driver.memory` is effectively the whole heap.

## Curriculum Framework (the "break it safely & measure it" machinery)

### Shared toolkit — `common/`
Importable from notebooks (host `PYTHONPATH` includes the repo root):
- `common/spark_session.py` — Spark Connect session factory + `display_df()`; `reconnect()`/`get_spark()` rebuild a dead session after a driver OOM (a stale Connect handle raises `[NO_ACTIVE_SESSION]`).
- `common/profiles.py` — `apply_profile(spark, "constrained"|"tuned")`: the **session-level** safety-net switcher (AQE, skew-join, broadcast threshold, shuffle partitions).
- `common/datagen.py` — `spark.range()`-based generators (uniform / **skewed** / wide / high-cardinality). Generate huge *logical* datasets without storing them; skew is deterministic & reproducible.
- `common/metrics_diff.py` — `measure()` + `compare()`: capture stage metrics via the Spark UI REST API (Connect-safe) and print a **before/after** table; `measure()` also tags each step's jobs (`spark.addTag`) so the UI **Jobs tab** is filterable (the SQL Description can't be set over Connect). The "Prove it" for perf modules.
- `common/iceberg_meta.py` — `table_health()` + `compare_health()`: Iceberg data-file / snapshot / manifest counts. The "Prove it" for the lakehouse track.

### Resource profiles — two layers
The Spark Connect server's memory is fixed when the container boots; a Connect client can't
change the driver heap at runtime. So "constrained vs tuned" has two layers:
1. **Container / box size** (flip at startup, requires restart):
   - `make up` → **tuned** (`mem_limit` 3 GB, `driver.memory` 2g, all cores).
   - `make up-constrained` → **constrained** (`mem_limit` 2 GB, `driver.memory` 1g, 2 cores) — for OOM/spill modules; failure is real inside the container but the host stays usable.
   - Driven by env vars `SPARK_MEM_LIMIT` / `SPARK_DRIVER_MEMORY` / `SPARK_CORES` (compose `mem_limit` + entrypoint `--master`/`--conf`).
2. **Session safety-nets** (flip at runtime from a notebook): `common.profiles.apply_profile()`.
   Most Spark pathology modules force the broken behavior with `constrained`, then relieve it with `tuned`.

### Per-track layout (curriculum)
Each track is a self-contained top-level folder with its own README (Break→Detect→Fix→Prove):
- `common/` — shared toolkit.
- `spark/` — **Phase 1 ✅ complete**: `SPK-1…SPK-10` perf pathologies (skew flagship in `spark/skew/`).
- `iceberg/` — **Phase 2 ✅ complete**: `LAK-1…LAK-10` lakehouse / table-format correctness.
- `kafka/` — **Phase 3 ✅ complete**: `KAF-1…KAF-6` (partitioning, consumer lag, rebalancing, retention/compaction, delivery semantics, poison-pill/dead-letter) + `STR-1…STR-3` (watermarking, checkpoints/restart, backpressure). Reuses `common/kafka_helpers.py`; producers/admin on host `localhost:29092`, Spark reads `kafka:9092`, bounded `trigger(availableNow=True)` streams.
- `debezium/` — **Phase 4 ✅ complete**: `CDC-1…CDC-9` (logical replication, connector bring-up, snapshot modes, event envelope, WAL/slot growth, deletes/replica identity, Spark→Iceberg MERGE, schema evolution, failure-mode tour). Adds **opt-in** Postgres + Kafka Connect (`make cdc-up`; compose profile `cdc`). Reuses `common/cdc_helpers.py` (Postgres DML, Debezium connector lifecycle over the Connect REST API, slot inspection, offset-resetting teardown).
- `capstone/` — **Phase 7 ✅ complete**: `CAP-1` end-to-end pipeline (`capstone/cap1_pipeline.py` staged ingest/transform/quality/cleanup + `airflow/dags/cap1_e2e_pipeline.py` orchestrating CDC→Iceberg + dbt marts + GE gate; verified green via `airflow dags test`), `CAP-2` incident simulator (`capstone/incident_simulator/` — 8 symptom-first on-call cards linking back to each fault's module), `CAP-3` observability (optional design appendix `docs/OBSERVABILITY.md` — Prometheus+Grafana + OpenLineage/Marquez; NOT wired into `make up`; web research was unavailable so it's knowledge-based), `CAP-4` learning path (`docs/LEARNING_PATH.md`). **All 7 phases ✅ — 58 modules.**
- `quality/` — **Phase 5 ✅ complete**: `DBT-1…DBT-10` (materializations, incremental strategies, late-arriving/lookback, SCD2 snapshots, schema-change, testing/layering, quarantine, dbt-expectations + Great Expectations, sources/freshness/contracts/exposures, macros/slim-CI). Expands the `dbt/` project (verified by one `dbt build`: PASS=50/WARN=1/ERROR=0); standalone GE lab in `quality/great_expectations/` (Connect-safe via `toPandas` — GE's Spark engine doesn't work over Connect). dbt-expectations via `metaplane/dbt_expectations` (`dbt deps` needs the corp CA → set `REQUESTS_CA_BUNDLE`/`SSL_CERT_FILE`). Contracts enforce name/type on Spark+Delta; column constraints (not_null) unsupported.

All built modules are verified end-to-end via headless `nbconvert` against the running server before commit. Modules are Connect-safe (DataFrame/SQL + `df.explain()`; no `sparkContext`/RDD) and laptop-safe (lazy/tiny data in `.tmp/`, teardown, `make clean`).

### Curriculum docs — `docs/`
`CURRICULUM_BRIEF.md`, `CURRICULUM_PLAN.md`, `spark-ui-guide.md` (symptom → which UI tab/metric),
`troubleshooting.md` (living symptom → cause → fix cheat-sheet).

## Key Technical Decisions

### JARs are pre-installed in Docker (not resolved at runtime)
- Iceberg, Delta, Kafka JARs are resolved via Ivy in a **multi-stage Docker build** and copied to `$SPARK_HOME/jars/`
- Reason: The Thrift Server has a classloader isolation bug where `spark.jars.packages` JARs aren't accessible from HiveServer2 execution threads
- `spark.jars.packages` is NOT used in spark-defaults.conf — JARs on the system classpath work

### Default catalog is `spark_catalog` (not `iceberg_catalog`)
- dbt creates tables in `spark_catalog` (Delta/Hive managed tables)
- Notebooks use `iceberg_catalog.my_database.xxx` explicitly — they never rely on the default catalog
- This avoids the Iceberg classloader issue with Thrift while keeping Iceberg fully usable from notebooks
- Notebook `01_setup_tables` also writes the same data as **Parquet** (plain files) and **Delta** — the repo demonstrates all three table formats side by side.

### Iceberg namespaces are pre-created as directories
- The Hadoop-based Iceberg catalog stores namespaces as filesystem directories
- `docker-entrypoint.sh` creates `.tmp/local_iceberg_warehouse/{default,analytics,staging,marts,seeds}` on startup
- Without this, Thrift clients get `SCHEMA_NOT_FOUND` on connect (Iceberg doesn't auto-create `default`)

### All runtime data lives in `.tmp/`
- Warehouses: `.tmp/local_iceberg_warehouse/`, `.tmp/local_delta_warehouse/`
- Metastore: `.tmp/metastore/` (Derby, via `derby.system.home` JVM prop)
- Spark warehouse: `.tmp/spark-warehouse/` (via `spark.sql.warehouse.dir`)
- Event logs: `.tmp/spark-events/`
- Streaming checkpoints: `.tmp/checkpoint_*`
- `make clean` = `rm -rf .tmp`

## dbt Setup

### How users run dbt
```bash
cd dbt
source .env        # sets DBT_PROFILES_DIR=. and connection vars
dbt run -s model   # just works
dbt build          # seed + run + test
```

### Connection
- Method: `thrift` (PyHive)
- Host/port from env vars: `DBT_SPARK_HOST`, `DBT_SPARK_PORT`
- Schema: `analytics`
- No authentication (SASL default, no password)

### Models
- `models/staging/stg_customers` (view) — cleaned/typed customers.
- `models/marts/dim_customers` (table) — enriched customer dimension (region, tier, tenure).
- `models/marts/agg_customers` (table) — aggregated customer metrics. (Phase 5 expands this project.)

### dbt-spark-qualify
- Local package at `./dbt-spark-qualify/`
- Monkeypatches `SparkConnectionManager.add_query()` to transpile QUALIFY clauses via sqlglot
- Installed via `[tool.uv.sources]` in pyproject.toml
- Uses a `.pth` file for automatic loading before dbt imports

### Schema naming
- `macros/generate_schema_name.sql` overrides dbt's default behavior
- Custom schemas are used directly (e.g., `staging`, `marts`) without prepending the target schema

## Airflow

Airflow 3 runs **locally** via `uv` (separate venv in `airflow/`), independent of Docker:
`make airflow-up` (UI at :5000, login airflow/airflow), `make airflow-down`, `make airflow-clean`.
- DAGs live in `airflow/dags/` (now **tracked** — `.gitignore` no longer excludes it).
- The inherited internal `prodrat_main` DAG was **removed** (it carried real S3 buckets, K8s
  namespaces, internal cell domains, Snowflake roles, and an NR account id — none of it teaching material).
- **Phase 6 ✅ complete**: `AF-1…AF-10` generic local teaching DAGs (`airflow/dags/af1_idempotency.py`
  … `af10_dbt_spark_e2e.py`) — idempotency, data-interval execution model, catchup/backfill,
  retries/SLA, sensor modes, trigger rules/branching, dynamic task mapping, XCom limits, Assets/
  data-aware scheduling, and a dbt+Spark+GE end-to-end (`AF-10` shells into the repo's `uv` project
  via BashOperator; Cosmos described). See [`airflow/README.md`](airflow/README.md).
- Verify a DAG headlessly (how Phase 6 was tested — synchronous, no scheduler): from `airflow/`,
  `AIRFLOW_HOME=$PWD/.airflow_home AIRFLOW__CORE__DAGS_FOLDER=$PWD/dags uv run airflow dags test <dag_id> 2025-03-01`.

## File Layout

```
├── Dockerfile              Multi-stage: deps (Ivy JAR resolution) → final image
├── docker-compose.yml      spark-connect (mem_limit profile), spark-history, kafka, kafka-ui
├── Makefile                make up (tuned) / up-constrained / jupyter / dbt-* / airflow-* / clean
├── conf/spark-defaults.conf  Catalogs, extensions, memory (driver 2g baseline), Thrift+Connect
├── scripts/docker-entrypoint.sh  Thrift+Connect (profile-aware) or History Server
├── common/                 Shared toolkit: spark_session, profiles, datagen, metrics_diff, iceberg_meta
├── spark/                  Phase 1 ✅ SPK-1..SPK-10 perf pathologies (skew flagship in spark/skew/)
├── iceberg/                Phase 2 ✅ LAK-1..LAK-10 lakehouse / table-format correctness
├── kafka/ debezium/ quality/   Phase 3–5 track signposts (built gradually)
├── docs/                   CURRICULUM_BRIEF, CURRICULUM_PLAN, spark-ui-guide, troubleshooting
├── app/
│   ├── utils/
│   │   ├── producer.py        File-based event producer (JSON → streaming_input/)
│   │   └── sales_producer.py  Kafka event producer (→ sales-events topic)
│   ├── data/source/           Static CSVs (customers.csv, orders.csv)
│   └── notebooks/             Jupyter notebooks 01-04 (import from common.spark_session)
├── dbt/
│   ├── dbt_project.yml        staging=view, marts=table
│   ├── models/staging/        stg_customers (view)
│   ├── models/marts/          dim_customers + agg_customers (tables)
│   └── macros/                generate_schema_name override
├── airflow/                Local Airflow (separate uv venv); dags/ tracked (example_dag.py)
├── dbt-spark-qualify/      Local package: QUALIFY → CTE transpilation
├── pyproject.toml          uv-managed, Python >=3.13
└── .tmp/                   ALL generated data (gitignored)
```

## Docker Services

| Service | Image | Ports | Purpose |
|---------|-------|-------|---------|
| spark-connect | spark-dev:latest | 10000, 15002, 4040 | Unified Thrift+Connect server (memory-capped via `mem_limit`) |
| spark-history | spark-dev:latest | 18080 | History Server (reads .tmp/spark-events) |
| kafka | apache/kafka:latest | 29092 | KRaft broker (no ZooKeeper); txn-state-log RF/min-ISR pinned to 1 for single-broker idempotent producers |
| kafka-ui | provectuslabs/kafka-ui | 8080 | Topic browser |
| postgres | postgres:16 | 5432 | **opt-in** (`make cdc-up`, profile `cdc`) CDC source, `wal_level=logical` |
| kafka-connect | debezium/connect:3.0.0.Final | 8083 | **opt-in** (`make cdc-up`, profile `cdc`) Debezium Postgres connector + REST API |

(JupyterLab :8888 and Airflow :5000 run locally on the host, not in Docker. Postgres + Kafka
Connect are **opt-in** — `make up` does not start them; `make cdc-up` does.)

### CDC re-runnability gotchas (Phase 4 — baked into `common/cdc_helpers.py`)
- **Unique `publication.name` per connector.** Debezium's default is the shared `dbz_publication`; two
  connectors with different `table.include.list` fight over it and silently stop emitting. `debezium_pg_config` sets a per-connector name.
- **Re-registering a connector with the same name skips the snapshot** (Connect persists offsets in `connect_offsets`; deleting the connector doesn't clear them). `teardown()` calls `reset_offsets()` (STOP → DELETE /offsets) so the next run snapshots; snapshot-dependent demos (CDC-7) use `snapshot.mode="always"` to be bulletproof.
- **`decimal.handling.mode=double`** so NUMERIC is readable (not base64); **`teardown` deletes the data topic** so stale events don't accumulate across runs.

## Common Issues & Fixes

- **"SCHEMA_NOT_FOUND" on Thrift connect**: Iceberg namespaces not created. Check `docker-entrypoint.sh` creates dirs in `.tmp/local_iceberg_warehouse/`
- **NoClassDefFoundError with Iceberg/Delta**: JARs not on system classpath. Must be in `$SPARK_HOME/jars/`, not loaded via `spark.jars.packages`
- **dbt "thrift connection method requires additional dependencies"**: `dbt-spark[PyHive]` extra is missing from pyproject.toml
- **Slow first start**: Should no longer happen — JARs are baked into the Docker image. If Ivy runs at startup, something is wrong with spark-defaults.conf
- **OOM/spill module won't fail (or freezes the laptop)**: use `make up-constrained` for the small box; don't run heavy modules on the tuned profile expecting an OOM. `make clean` recovers generated data.

## Dependency Versions (as of 2026-05-06)

- Spark: 4.0.2 (Scala 2.13, Java 17)
- Iceberg: 1.10.1
- Delta Lake: 4.0.0
- dbt-core: 1.11.0
- dbt-spark: 1.10.1
- Python: 3.13
- Package manager: uv
