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
- `capstone/` — **Phase 7 ✅ complete**: `CAP-1` end-to-end pipeline (`capstone/cap1_pipeline.py` staged ingest/transform/quality/cleanup + `airflow/dags/cap1_e2e_pipeline.py` orchestrating CDC→Iceberg + dbt marts + GE gate; verified green via `airflow dags test`), `CAP-2` incident simulator (`capstone/incident_simulator/` — 8 symptom-first on-call cards linking back to each fault's module), `CAP-3` observability (`docs/OBSERVABILITY.md`) — **built & verified opt-in profile** `make monitoring-up` (Prometheus + Grafana + `kafka-exporter` + `postgres-exporter` + Spark `PrometheusServlet`; all 5 Prometheus targets UP; CDC-5 slot + KAF-1/2 lag live; NOT in `make up`); Connect-JMX / Airflow-OTel / dbt-Elementary / OpenLineage-Marquez are documented next-steps, `CAP-4` learning path (`docs/LEARNING_PATH.md`). **All 7 phases ✅ — 58 modules.**
- `dbt/quality/` — **Phase 5 ✅ complete**: `DBT-1…DBT-10` (materializations, incremental strategies, late-arriving/lookback, SCD2 snapshots, schema-change, testing/layering, quarantine, dbt-expectations + Great Expectations, sources/freshness/contracts/exposures, macros/slim-CI). Lives **inside** the `dbt/` project it teaches — 10 flat `dbtN_*.md` Break→Detect→Fix→Prove writeups + the `great_expectations/` lab (the module folders are markdown-only, so they're files, not folders; dbt only compiles `models/seeds/tests/macros`, so `dbt/quality/` is ignored by `dbt build`). Expands the `dbt/` project (verified by one `dbt build`: PASS=50/WARN=1/ERROR=0); standalone GE lab in `dbt/quality/great_expectations/` (Connect-safe via `toPandas` — GE's Spark engine doesn't work over Connect). dbt-expectations via `metaplane/dbt_expectations` (`dbt deps` needs the corp CA → set `REQUESTS_CA_BUNDLE`/`SSL_CERT_FILE`). Contracts enforce name/type on Spark+Delta; column constraints (not_null) unsupported.

All built modules are verified end-to-end via headless `nbconvert` against the running server before commit. Modules are Connect-safe (DataFrame/SQL + `df.explain()`; no `sparkContext`/RDD) and laptop-safe (lazy/tiny data in `.tmp/`, teardown, `make clean`).

### Curriculum docs — `docs/`
`CURRICULUM_BRIEF.md`, `CURRICULUM_PLAN.md`, `spark-ui-guide.md` (symptom → which UI tab/metric),
`troubleshooting.md` (living symptom → cause → fix cheat-sheet).

## Key Technical Decisions

### JARs are pre-installed in Docker (not resolved at runtime)
- Iceberg, Delta, Kafka JARs are resolved via Ivy in a **multi-stage Docker build** and copied to `$SPARK_HOME/jars/`
- Reason: The Thrift Server has a classloader isolation bug where `spark.jars.packages` JARs aren't accessible from HiveServer2 execution threads
- `spark.jars.packages` is NOT used in spark-defaults.conf — JARs on the system classpath work

### Default catalog is `spark_catalog`; dbt can target any catalog explicitly
- `spark_catalog` (Delta/Hive) is the **default** — dbt models land there unless they say otherwise.
- Notebooks use `iceberg_catalog.my_database.xxx` explicitly.
- **Iceberg writes over Thrift work now** (verified: `CREATE TABLE iceberg_catalog.ns.t USING iceberg AS …`
  via :10000 succeeds; metadata ops too). The old "Iceberg classloader bug on Thrift" is resolved by the
  baked-in system-classpath JARs — that caveat no longer applies.
- **dbt → explicit catalog: the schema-string trick (no code).** dbt-spark forbids the `database`
  field (`SparkRelation` raises `"Cannot set database in spark!"`) but renders `schema.identifier`, so
  put the catalog *in the schema*: `{{ config(schema='iceberg_catalog.marts', file_format='iceberg') }}`
  → renders `iceberg_catalog.marts.<t>`. The repo's `generate_schema_name` override already passes a
  custom schema through unchanged, so **no package/macro is needed**. Verified on stock dbt-spark: a
  delta-schema model → `spark_catalog.marts.<t>` (provider delta), an iceberg-schema model →
  `iceberg_catalog.marts.<t>` (provider iceberg). (A `dbt-spark-catalog` monkeypatch was prototyped and
  **removed as over-engineered** — the trick does the same in one config line.) Only `delta` + `iceberg`
  catalogs exist today; `hudi_catalog` is designed (needs a `hudi-spark4.0-bundle_2.13` JAR + image
  rebuild) — see `docs/CATALOG_ROUTING_DESIGN.md`.

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

### dbt-polyglot (formerly dbt-spark-transpile)
- Local package at `./dbt/dbt-polyglot/` — PyPI-ready src-layout (`src/dbt_polyglot/`).
- Write a model in another SQL dialect (e.g. Snowflake); it is transpiled to Spark via `sqlglot` at
  **compile phase** — monkeypatches `dbt.compilation.Compiler._compile_code`, so the rewrite happens
  on the model **body before** dbt's materialization wrapper. `target/compiled/` and the executed SQL
  are both the Spark form (no mixed-dialect string; no separate output folder). This replaced the old
  `add_query` (submit-phase) patch, which couldn't handle the Spark-DDL-wrapped string.
- Opt in via config (no per-project code): project-level `models: +transpile_from: snowflake` and/or
  per-model `{{ config(transpile_from='snowflake') }}` (model overrides project). Optional
  `transpile_to` (default `spark`).
- **No-op** when `transpile_from` is unset or equals the target dialect → sqlglot is never called.
  Otherwise **every opted-in model is transpiled** (full sqlglot breadth — IFF→IF, NVL→COALESCE, ::→CAST,
  DATEADD, QUALIFY, …); scope it the dbt-native way (set `+transpile_from` on a folder/model subtree, not
  project-wide) rather than a token throttle. (An earlier `TRANSPILE_MODE=guarded` QUALIFY-token throttle
  was **removed as POC residue** — for a real Snowflake repo it would silently skip the many non-QUALIFY
  models that still need IFF/NVL/:: conversion.) **Fail-soft:** any transpile error / empty / multi-statement
  output logs an `AdapterLogger` WARNING visible in the dbt run and passes the original SQL through unchanged
  (never crashes a compile) — e.g. with project-wide `+transpile_from: snowflake`, `stg_customers`' Spark-style
  `datediff(end, start)` warns and runs unchanged. Output is **pretty-printed** (`pretty=True`).
- **`NULLS LAST` in transpiled SQL is intentional**, not cosmetic: Snowflake (`nulls_are_large`) and Spark
  (`nulls_are_small`) have opposite default null ordering, so sqlglot makes the ordering explicit to
  preserve Snowflake semantics (e.g. a `QUALIFY ROW_NUMBER()=1` top-N pick). No clean sqlglot knob
  suppresses only the cosmetic case — don't strip it. (The `spark_catalog.` table qualification is from
  the `generate_schema_name` routing macro, not sqlglot.)
- **Fix-up layer (`SPARK_FIXUPS`) — makes it trustable for a real Snowflake repo.** sqlglot's Spark output
  is sometimes rejected by Spark 4.0.2's *real* parser — notably `x NOT IN (subquery)`, which sqlglot's
  Snowflake reader canonicalizes to the **unsupported** `x <> ALL (subquery)`. So the transpile is now
  `parse(read=src) → apply fix-up transforms → generate(spark)`; the first fix-up rewrites quantified-subquery
  comparisons (`<> ALL`/`= ANY (subq)`) back to `NOT x IN`/`x IN (subq)`. Extensible registry, each
  EXPLAIN-verified. A model is converted to **verified-valid Spark or fails LOUD — never silently wrong**.
- **Trust check — delegated to native dbt, not custom code:** `make transpile-check` runs
  `dbt build --empty` (build every model with zero input rows, in DAG order, against the
  `profiles.yml` adapter — moves no data, fails loud naming any model whose transpiled SQL is
  invalid). `dbt show --limit 0 -s <model>` is the read-only variant. The earlier custom `dbt verify`
  command + `transpile_check.py` + PyHive connection were **removed (2026-06-26) as a reinvention of
  what dbt-core 1.8+ already does** (`--empty`); this also dropped the pyhive/pyyaml deps and made
  validation warehouse-agnostic. The full **"run a Snowflake dbt repo on Spark, config-only"** story is
  `docs/SNOWFLAKE_ON_SPARK.md`.
- Installed via `[tool.uv.sources]` in pyproject.toml; the `.pth` is placed into site-packages by a
  `build_py` override in `setup.py` (the `data_files` `.pth` trick lands in the venv root under uv and
  never loads — see the package README). Spark 4.0.2 has no native `QUALIFY` (`[PARSE_SYNTAX_ERROR]`),
  which is why the transpile is genuinely needed. The model SQL→Spark catalog/format routing
  (delta/iceberg/hudi) is a **separate** concern — the schema-string trick, below.

### Multi-catalog targeting (format-driven, via `generate_schema_name`)
- **The user sets only `file_format`; the table is auto-routed to the matching catalog.**
  `macros/generate_schema_name.sql` maps `delta→spark_catalog`, `iceberg→iceberg_catalog`,
  `hudi→hudi_catalog` and prepends the catalog onto the schema (the "schema-string trick": dbt-spark
  renders `schema.identifier`, so `catalog.schema.identifier` targets that catalog). So
  `{{ config(materialized='table', file_format='iceberg') }}` in `marts/` →
  `iceberg_catalog.marts.<t>`; `file_format='delta'` → `spark_catalog.marts.<t>`. No `database` field, no
  manual `schema=` (you *can* still pass a dotted `schema='cat.ns'` — the macro leaves an already-dotted
  schema untouched). Models with no `file_format` (views, seeds) are unaffected (no prefix).
- **Why the schema, not `database`:** dbt-spark forbids the `database` field (`SparkRelation` raises
  `"Cannot set database in spark!"`). **Verified on stock dbt-spark:** a `delta` model → provider delta in
  `spark_catalog`, an `iceberg` model → provider iceberg in `iceberg_catalog` (incl. incremental-merge).
- A `dbt-spark-catalog` `.pth` monkeypatch (relaxing the `SparkRelation` guards to honor `database`) was
  prototyped and **removed as over-engineered** — the schema-string trick + this macro achieve the same
  with no package (the user's real-world Glue approach: `+schema: silver.schema`). See
  `docs/CATALOG_ROUTING_DESIGN.md`.
- Only `delta` + `iceberg` catalogs exist today. Hudi is **designed, not installed**: needs
  `org.apache.hudi:hudi-spark4.0-bundle_2.13:1.2.0` in the Dockerfile + a `hudi_catalog` (`HoodieCatalog`)
  + `HoodieSparkSessionExtension`/`KryoSerializer` in `conf/spark-defaults.conf` → image rebuild + restart.
  Once added, the same schema trick routes to it.

### Schema naming
- `macros/generate_schema_name.sql` overrides dbt's default behavior. Two jobs:
  (1) custom schemas are used directly (e.g., `staging`, `marts`) without prepending the target schema;
  (2) **format-driven catalog routing** — it prepends the catalog matching the model's `file_format`
  (delta→`spark_catalog`, iceberg→`iceberg_catalog`, hudi→`hudi_catalog`). See *Multi-catalog targeting* above.

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
├── kafka/ debezium/        Phase 3–4 track signposts (built gradually)
├── docs/                   CURRICULUM_BRIEF, CURRICULUM_PLAN, spark-ui-guide, troubleshooting
├── dbt/
│   ├── dbt_project.yml        staging=view, marts=table
│   ├── models/staging/        stg_customers (view)
│   ├── models/marts/          dim_customers + agg_customers (tables)
│   ├── macros/                generate_schema_name override
│   ├── quality/               Phase 5 ✅ DBT-1..10 writeups + great_expectations/ (GE lab)
│   └── dbt-polyglot/          Local pkg (PyPI: dbt-polyglot): compile-time transpile (.pth, src-layout)
├── airflow/                Local Airflow (separate uv venv); dags/ tracked (example_dag.py)
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
| prometheus / grafana | prom/prometheus, grafana/grafana | 9090 / 3000 | **opt-in** (`make monitoring-up`, profile `monitoring`, CAP-3) metrics + dashboards |
| kafka-exporter / postgres-exporter | danielqsj/kafka-exporter, prometheuscommunity/postgres-exporter | 9308 / 9187 | **opt-in** (profile `monitoring`) Kafka lag + Postgres slot/WAL metrics |

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
