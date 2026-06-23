# Spark Dev — Learning Repo

A Docker-based environment for **Apache Spark**, **Iceberg**, **Delta Lake**, **Kafka Structured Streaming**, **dbt**, and **Airflow** — with a unified Spark server so notebooks and dbt jobs share one Spark UI.

---

## Architecture

```
┌───────────────────────────────────────────────────────────────┐
│  docker compose                                               │
│                                                               │
│  ┌──────────────────┐   ┌──────────────────────────────────┐  │
│  │  kafka            │   │  spark-connect                   │  │
│  │  apache/kafka     │◄──│  Spark 4.0.2 Unified Server     │  │
│  │  :9092 (internal) │   │  :10000 (Thrift JDBC — dbt)     │  │
│  │  :29092 (external)│   │  :15002 (Connect gRPC — notebooks)│ │
│  └────────┬──────────┘   │  :4040  (Spark UI)              │  │
│           │              └──────────────────────────────────┘  │
│  ┌────────▼──────────┐   ┌──────────────────────────────────┐  │
│  │  kafka-ui          │   │  spark-history                   │  │
│  │  :8080             │   │  :18080                          │  │
│  └───────────────────┘   └──────────────────────────────────┘  │
└───────────────────────────────────────────────────────────────┘
         ▲                          ▲                ▲
    localhost:29092         sc://localhost:15002   jdbc:hive2://localhost:10000
    (producers)            (notebooks)            (dbt)
```

**Key design:**
- Notebooks connect via **Spark Connect** (`sc://localhost:15002`)
- dbt connects via **Thrift Server** (`jdbc:hive2://localhost:10000`)
- Both share the **same SparkContext** — all jobs appear in one Spark UI at `http://localhost:4040`

---

## Curriculum (production challenges)

This repo doubles as a self-paced **Data Engineering production-challenges curriculum** — break
real systems at small scale, watch them fail in the Spark UI, fix them, and measure the gain.
Start with [`docs/CURRICULUM_BRIEF.md`](docs/CURRICULUM_BRIEF.md) and
[`docs/CURRICULUM_PLAN.md`](docs/CURRICULUM_PLAN.md).

- **Shared toolkit** in `common/`: `datagen` (synthesize skewed/wide data without storing it),
  `metrics_diff` (before/after metric tables), `profiles` (constrained vs tuned), `spark_session`.
- **Resource profiles** (laptop-safe): `make up` runs a tuned ~3 GB Spark box; `make up-constrained`
  runs a ~2 GB box so OOM/spill are real but the host stays usable. Session-level safety nets
  (AQE, broadcast, shuffle partitions) flip per-notebook via `common.profiles.apply_profile()`.
- **Tracks**: `spark/` (perf), `iceberg/`, `kafka/`, `debezium/`, `quality/` — each a self-contained
  module folder. The flagship `SPK-1` (data skew) lives in `spark/skew/`.
- **Guides**: [`docs/spark-ui-guide.md`](docs/spark-ui-guide.md) (symptom → which UI tab) and
  [`docs/troubleshooting.md`](docs/troubleshooting.md) (symptom → cause → fix).

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- [uv](https://docs.astral.sh/uv/) (Python package manager)

---

## Quick Start

```bash
# 1. Clone and enter
git clone <repo-url> spark-dev && cd spark-dev

# 2. Install Python dependencies locally
uv sync

# 3. Start Docker services (Spark + Kafka + History Server)
make up

# 4. Start JupyterLab locally
make jupyter

# 5. Open http://localhost:8888 and run notebooks
```

---

## Services & Ports

| Service | URL | Description |
|---------|-----|-------------|
| Spark Connect | `sc://localhost:15002` | gRPC endpoint for notebooks |
| Spark Thrift | `jdbc:hive2://localhost:10000` | JDBC endpoint for dbt |
| Spark UI | http://localhost:4040 | Unified DAG view for all jobs |
| History Server | http://localhost:18080 | Completed Spark applications |
| Kafka UI | http://localhost:8080 | Topic browser, message inspector |
| Kafka broker | http://localhost:29092 | Bootstrap server for producers |
| JupyterLab | http://localhost:8888 | Local notebook server |
| Airflow | http://localhost:5000 | Local DAG scheduler & web UI (airflow/airflow) |

---

## dbt

dbt-core is integrated via the Spark Thrift Server. All dbt jobs appear in the same Spark UI alongside notebook jobs.

### Usage

```bash
cd dbt
source .env
dbt run -s stg_customers       # run a single model
dbt build                      # seed + run + test (full pipeline)
dbt test -s dim_customers      # test a specific model
```

### Project Structure

```
dbt/
├── dbt_project.yml            # Project config
├── profiles.yml               # Connection config (Thrift → localhost:10000)
├── .env                       # Source this for direct dbt usage
├── seeds/
│   └── customers.csv          # Raw customer data
├── models/
│   ├── staging/
│   │   ├── stg_customers.sql  # Cleaned + typed customer data
│   │   └── _staging__models.yml
│   └── marts/
│       ├── dim_customers.sql  # Customer dimension (regions, tiers, tenure)
│       ├── agg_customers.sql  # Aggregated customer metrics
│       └── _marts__models.yml
└── macros/
    └── generate_schema_name.sql
```

### Models

| Model | Layer | Materialized | Description |
|-------|-------|--------------|-------------|
| `stg_customers` | staging | view | Cleaned customer data with typed dates and tenure |
| `dim_customers` | marts | table | Enriched with region, tier rank, tenure segment |
| `agg_customers` | marts | table | Aggregated customer metrics |

---

## Notebooks

Run notebooks in order: **01 → 02 → 04 → 03**.

| File | Producer | Description |
|------|----------|-------------|
| `01_setup_tables` | — | Load CSV into Iceberg, Delta Lake, Parquet |
| `02_streaming_to_iceberg` | `make producer` | File-based Structured Streaming → Iceberg |
| `03_query_iceberg` | — | Time travel, snapshots, cross-table analysis |
| `04_sales_streaming_to_iceberg` | `make sales-producer` | Kafka stream + customer enrichment → Iceberg |

---

## Airflow

Airflow 3.1.7 runs locally via `uv` (separate venv in `airflow/`). It is independent of Docker services.

### Usage

```bash
make airflow-up       # Start in background (webserver + scheduler + triggerer)
make airflow-down     # Stop all Airflow processes
make airflow-logs     # Tail standalone log
make airflow-clean    # Wipe DB + logs for a fresh start
```

- **Web UI:** http://localhost:5000
- **Login:** `airflow` / `airflow`
- **DAGs folder:** `airflow/dags/`
- **Logs:** `airflow/.airflow_home/logs/`
- **Dependencies:** `airflow/pyproject.toml` (isolated from the main project)

### First-time setup

```bash
cd airflow && uv sync    # Install Airflow + providers into airflow/.venv
make airflow-up          # Initializes DB and starts all components
```

---

## Make Targets

```bash
make help             # Show all commands
make up               # Start Docker services — tuned profile (~3 GB Spark)
make up-constrained   # Start Docker services — constrained profile (~2 GB Spark; OOM/spill modules)
make down             # Stop Docker services
make restart          # Restart everything (tuned)
make restart-constrained # Restart everything (constrained profile)
make logs             # Tail service logs
make status           # Show service status
make jupyter          # Start local JupyterLab
make producer         # Start file-based event producer
make sales-producer   # Start Kafka sales event producer
make airflow-up       # Start Airflow locally (UI at :5000, airflow/airflow)
make airflow-down     # Stop Airflow
make airflow-logs     # Tail Airflow logs
make airflow-clean    # Stop + wipe Airflow state (fresh start)
make dbt-build        # Run full dbt pipeline (seed + run + test)
make dbt-debug        # Verify dbt connection
make clean            # Remove generated data
make clean-all        # Remove data + Docker volumes
```

---

## Configuration

| File | Purpose |
|------|---------|
| `.env` | Ports, Spark remote URL, Kafka address, dbt vars, resource-profile vars (`SPARK_MEM_LIMIT` / `SPARK_DRIVER_MEMORY` / `SPARK_CORES`) |
| `conf/spark-defaults.conf` | Spark server config (catalogs, memory, extensions) |
| `conf/log4j2.properties` | Logging levels |
| `dbt/profiles.yml` | dbt connection config (uses env vars from `dbt/.env`) |
| `airflow/pyproject.toml` | Airflow dependencies (separate uv project) |
| `airflow/passwords.json` | Airflow local auth credentials |

### Spark Catalogs

| Catalog | Format | Default | Warehouse path |
|---------|--------|---------|----------------|
| `spark_catalog` | Delta Lake / Hive | Yes (dbt) | `.tmp/local_delta_warehouse` |
| `iceberg_catalog` | Apache Iceberg | Notebooks use explicitly | `.tmp/local_iceberg_warehouse` |

---

## Project Structure

```
spark-dev/
├── docker-compose.yml          # Docker services (Spark, Kafka)
├── Dockerfile                  # Spark Unified Server image
├── Makefile                    # Dev workflow commands
├── .env                        # Environment variables
├── .env.example                # Template for .env
├── conf/
│   ├── spark-defaults.conf     # Spark config (catalogs, extensions, memory)
│   └── log4j2.properties       # Logging config
├── scripts/
│   └── docker-entrypoint.sh    # Container entrypoint (Thrift+Connect / history)
├── common/                     # Shared curriculum toolkit
│   ├── spark_session.py        # Spark session helper (Connect/local)
│   ├── profiles.py             # constrained vs tuned session profiles
│   ├── datagen.py              # synthetic data generators (skew knob)
│   └── metrics_diff.py         # before/after metrics tables
├── spark/                      # Phase 1: Spark performance pathologies (SPK-1 skew flagship)
├── iceberg/ kafka/ quality/ debezium/   # Phase 2–5 track signposts (built gradually)
├── docs/                       # curriculum brief/plan, spark-ui-guide, troubleshooting
├── app/
│   ├── utils/
│   │   ├── producer.py         # File-based event producer
│   │   └── sales_producer.py   # Kafka sales event producer
│   ├── data/
│   │   └── source/             # Static reference data (CSV)
│   └── notebooks/              # Jupyter notebooks (01–04; import from common.spark_session)
├── airflow/                    # Airflow project (separate uv env)
│   ├── pyproject.toml          # Airflow + provider dependencies
│   ├── passwords.json          # Local auth (airflow/airflow, role: admin)
│   └── dags/                   # DAG definitions
├── dbt/                        # dbt project (models, seeds, tests)
├── pyproject.toml              # Python dependencies
└── .tmp/                       # Generated data (gitignored)
```

---

## How the Unified Server Works

The Docker container runs **one Spark application** that exposes two interfaces:

```
dbt ─────────── Thrift (JDBC :10000) ──┐
                                        ├── Same SparkContext → Spark UI :4040
Notebooks ───── Connect (gRPC :15002) ─┘
```

This is achieved by starting the Spark **Thrift Server** (`HiveServer2`) with the **Spark Connect plugin** enabled in the same JVM. JARs (Iceberg, Delta, Kafka) are pre-installed in the Docker image for fast startup and classloader compatibility.

---

## Troubleshooting

**Spark not starting?**
```bash
docker compose logs spark-connect    # Check logs
make restart                         # Restart everything
```

**dbt can't connect?**
```bash
# Verify the Thrift port is open
nc -z localhost 10000 && echo OK

# Check dbt config
cd dbt && source .env && dbt debug
```

**Port conflict?**
Edit `.env` to change any port, then `make restart`.
