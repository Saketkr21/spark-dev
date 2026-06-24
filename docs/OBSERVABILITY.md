# CAP-3 — Observability (optional appendix)

> **Status: design appendix, opt-in, offline-first.** This proposes tried-and-tested **open-source**
> observability you *can* add to the repo for each service. Nothing here is wired into the default
> `make up` stack — observability is heavy (another ~1–2 GB) and the curriculum's core promise is a
> responsive laptop, so it's an explicit add-on. The recipes below are compiled from established OSS
> practice (not live-verified against each project's latest docs in this environment); treat exact
> versions/flags as a starting point to iterate on — it *is* somewhat trial-and-error per stack.

There is **no single framework** that covers Spark + Kafka + Debezium + Airflow + dbt. The proven,
vendor-neutral approach is two OSS pillars, each of which every one of these tools already integrates
with:

1. **Metrics & dashboards → Prometheus + Grafana** (the de-facto OSS metrics stack).
2. **Data lineage → OpenLineage + Marquez** (the LF AI & Data lineage standard + its reference UI).

Both run locally in Docker and are the closest thing to an "already tried-and-tested framework for
all these services."

---

## Pillar 1 — Metrics: Prometheus + Grafana

Each service exposes metrics; Prometheus scrapes them; Grafana dashboards visualize them. Per-service
exporters (all OSS, all standard):

| Service | How it exposes metrics (OSS) | Notable signals (tie-in) |
|---------|------------------------------|--------------------------|
| **Spark** | Built-in **`PrometheusServlet`** (Spark 3.0+): set `spark.ui.prometheus.enabled=true` and a `metrics.properties` `*.sink.prometheusServlet` — scrape `:4040/metrics/prometheus` + `/metrics/executors/prometheus`. (Older: Graphite/JMX sink.) For streaming, a `StreamingQueryListener`. | task time, shuffle, GC, executor memory, spill — the SPK-* signals |
| **Kafka broker** | **`jmx_exporter`** (Prometheus JMX exporter agent jar) on the broker JVM → `/metrics`. Mature Grafana dashboards exist (Strimzi/Confluent community). | under-replicated partitions, request latency, bytes in/out |
| **Kafka Connect / Debezium** | Connect JMX → `jmx_exporter`. Debezium publishes connector MBeans (`debezium.postgres:type=connector-metrics`): `MilliSecondsBehindSource`, `SnapshotCompleted`, `NumberOfEventsSeen`, queue sizes. | CDC lag, snapshot progress — the CDC-* signals |
| **Postgres** | **`postgres_exporter`** (prometheus-community). | **replication-slot retained WAL / lag** — exactly the CDC-5 pathology; connections; tx age |
| **Airflow** | Native **StatsD** metrics → **`statsd_exporter`** → Prometheus; or Airflow 3's **OpenTelemetry** metrics (`[metrics] otel_on=True`) → an OTel Collector → Prometheus. | DAG/task duration, failures, scheduler health, pool slots — AF-* signals |
| **dbt** | dbt has no live metrics — it emits **artifacts** (`run_results.json`, `manifest.json`) per run. Use **Elementary** (`elementary-data`, dbt-native: test results, freshness, anomaly detection + a report/dashboard) or **re_data**; or a tiny exporter that pushes `run_results` timings to Prometheus. | model run times, test pass/fail, freshness — DBT-* signals |

**How you'd add it (sketch — a `monitoring` compose profile, opt-in like `cdc`):**

```yaml
# docker-compose.yml — services behind  profiles: ["monitoring"]
prometheus:   { image: prom/prometheus,            ports: ["9090:9090"], volumes: ["./conf/prometheus.yml:/etc/prometheus/prometheus.yml"] }
grafana:      { image: grafana/grafana,            ports: ["3000:3000"] }
postgres-exporter: { image: quay.io/prometheuscommunity/postgres-exporter, profiles: ["monitoring","cdc"] }
# kafka / kafka-connect: add the jmx_exporter agent jar via KAFKA_OPTS=-javaagent:...
# spark: set spark.ui.prometheus.enabled=true + a prometheusServlet sink in conf/metrics.properties
# airflow: AIRFLOW__METRICS__STATSD_ON=true + a statsd_exporter container
```
Then `make monitoring-up` (a `--profile monitoring` target) and import community Grafana dashboards.
The single highest-value, lowest-effort piece for *this* repo is **`postgres_exporter`** — it turns
the CDC-5 "slot retains WAL → disk fills" lab into a live Grafana gauge with almost no wiring.

---

## Pillar 2 — Lineage: OpenLineage + Marquez

**OpenLineage** is an open spec for emitting run/dataset/job lineage events; **Marquez** is its
reference metadata server + web UI. Every tool in this repo has a first-class integration, so you get
**one lineage graph across Spark, Airflow, and dbt**:

| Tool | OpenLineage integration |
|------|-------------------------|
| **Airflow** | the native **`apache-airflow-providers-openlineage`** provider — emits events per task automatically |
| **Spark** | the **`openlineage-spark`** listener jar: `spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener` + a transport pointed at Marquez — dataset-level lineage for reads/writes/MERGE |
| **dbt** | **`dbt-ol`** (OpenLineage's dbt wrapper) or via **astronomer-cosmos** (already a dependency) — model/test lineage |

Wiring (sketch): add `marquez` + `marquez-web` + a small Postgres to a `lineage` compose profile, set
the OpenLineage transport (`OPENLINEAGE_URL=http://marquez:5000`) for each tool, and browse the graph
at the Marquez UI. This is the cleanest way to *see* CAP-1's two lineages (CDC→Iceberg and dbt/Delta)
as one DAG of datasets.

---

## What I'd actually recommend for this repo

- **Start tiny, prove value:** add **`postgres_exporter` + Prometheus + Grafana** behind a
  `monitoring` profile and graph the **CDC-5 replication-slot lag** live. It's the smallest change
  with the most curriculum payoff.
- **Then metrics breadth:** Spark `PrometheusServlet` (config-only, no new container) and the Kafka/
  Connect `jmx_exporter`.
- **Then lineage:** OpenLineage → Marquez, starting with the Airflow provider (one pip package + an
  env var), since AF-10/CAP-1 already orchestrate the real jobs.
- Keep everything **opt-in and offline**; document the extra memory; never required to run a module.

## Optional, masked: New Relic (commercial alternative)

The inherited code referenced New Relic; the curriculum is **100% offline and never requires it**.
If a learner *wants* a hosted backend, NR (and any OTel vendor) ingests the **same** OpenTelemetry
metrics + OpenLineage events described above — point the OTel Collector / OpenLineage transport at the
vendor endpoint instead of Prometheus/Marquez. **No account IDs, license keys, or internal endpoints
are stored in this repo** (they were removed in F-7); a learner would supply their own via env vars.

---

### Verifying this appendix

These recipes are from established OSS practice but were **not live-verified** against each project's
current docs in this environment (web access is disabled here). Before implementing, confirm the
exact image tags, the Spark 4 `metrics.properties` sink class names, the Debezium MBean names for your
connector version, and the Airflow 3 OTel vs StatsD choice. If web access is enabled, the exact
configs and current Grafana dashboard IDs can be pinned down and a `monitoring` profile built + verified.
