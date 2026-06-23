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
- `common/spark_session.py` — Spark Connect session factory + `display_df()` (moved here from `app/utils/`).
- `common/profiles.py` — `apply_profile(spark, "constrained"|"tuned")`: the **session-level** safety-net switcher (AQE, skew-join, broadcast threshold, shuffle partitions).
- `common/datagen.py` — `spark.range()`-based generators (uniform / **skewed** / wide / high-cardinality). Generate huge *logical* datasets without storing them; skew is deterministic & reproducible.
- `common/metrics_diff.py` — `measure()` + `compare()`: capture stage metrics via the Spark UI REST API (Connect-safe) and print a **before/after** table. The "Prove it" step.

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
`common/` (toolkit), `spark/` (Phase 1 perf pathologies — `SPK-1` skew flagship lives in
`spark/skew/`), `iceberg/` (Phase 2), `kafka/` (Phase 3), `debezium/` (Phase 4 CDC),
`quality/` (Phase 5 dbt-tests + Great Expectations). `iceberg/`/`kafka/`/`quality/`/`debezium/`
are currently README **signposts** — built gradually, repo stays runnable at each step.

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
- Currently only `example_dag.py` (a trivial working DAG). The inherited internal `prodrat_main`
  DAG was **removed** (it carried real S3 buckets, K8s namespaces, internal cell domains,
  Snowflake roles, and an NR account id — none of it teaching material).
- Generic, local-runnable teaching DAGs (`AF-1`…`AF-10`) that orchestrate this repo's own
  Spark/dbt jobs come in **Phase 6** of the curriculum.

## File Layout

```
├── Dockerfile              Multi-stage: deps (Ivy JAR resolution) → final image
├── docker-compose.yml      spark-connect (mem_limit profile), spark-history, kafka, kafka-ui
├── Makefile                make up (tuned) / up-constrained / jupyter / dbt-* / airflow-* / clean
├── conf/spark-defaults.conf  Catalogs, extensions, memory (driver 2g baseline), Thrift+Connect
├── scripts/docker-entrypoint.sh  Thrift+Connect (profile-aware) or History Server
├── common/                 SHARED TOOLKIT: spark_session, profiles, datagen, metrics_diff
├── spark/                  Phase 1 perf pathologies (SPK-1 skew flagship in spark/skew/)
├── iceberg/ kafka/ quality/ debezium/   Phase 2–5 track signposts (built gradually)
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
| kafka | apache/kafka:latest | 29092 | KRaft broker (no ZooKeeper) |
| kafka-ui | provectuslabs/kafka-ui | 8080 | Topic browser |

(JupyterLab :8888 and Airflow :5000 run locally on the host, not in Docker.)

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
