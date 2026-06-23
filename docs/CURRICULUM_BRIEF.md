# spark-dev — Curriculum Brief (Master Spec)

> This is the **why / what / rules** document for evolving `spark-dev` into a complete,
> hands-on Data Engineering **Production-Challenges** curriculum.
> The **how / when / module-by-module roadmap** lives in [`CURRICULUM_PLAN.md`](./CURRICULUM_PLAN.md).
> Read both before starting any phase.

---

## MISSION

Turn `spark-dev` from a working batch + streaming + dbt + Airflow skeleton into a
self-paced curriculum where the learner doesn't just run happy-path code — they
**break real systems, watch them fail in the Spark UI / tool dashboards, diagnose the
root cause, fix it, and measure the improvement** — all on an ordinary laptop without
making the machine unusable.

The curriculum spans every tool in the repo: **Spark, Iceberg/Delta, Kafka, Debezium (CDC),
dbt, Airflow**, plus a data-quality layer (**dbt tests + Great Expectations**) and an
optional observability layer.

---

## LEARNER & ENVIRONMENT (hard constraints — never violate)

- **Audience:** students / early-career engineers on ordinary laptops (8–16 GB RAM, no GPU).
- **The laptop must stay responsive** the whole time. No exercise may freeze the machine,
  fill the disk, or require downloading large datasets.
- Every "break it" exercise must be **bounded and reversible**: capped container memory,
  auto-stopping streams, `make clean` recovery, clear teardown. Failure is *contained*, not real.
- **Build on the existing architecture** (unified Spark Thrift+Connect server, Iceberg/Delta/
  Parquet catalogs, Kafka in KRaft, dbt-via-thrift, Airflow). Extend; don't re-platform.

---

## THE CORE TRICK (the heart of the project)

We can't store TBs. So we **reproduce big-data failure modes at small scale by constraining
resources and generating data on the fly**, not by using large data:

1. **Generate, don't store.** Use `spark.range(...)` + `rand()`/`hash()` to synthesize
   billions of rows lazily. Nothing hits disk, but the engine does the real shuffle/spill/OOM work.
2. **Shrink the box, not the data.** Run constrained exercises against a Spark profile with tiny
   memory (e.g. `driver.memory=1g`, low `executor.memory`, few cores) inside a **memory-capped
   Docker container (~2–3 GB)** so OOM is *real inside the container* but the host stays smooth.
3. **Toggle the safety nets.** Turn AQE off / broadcast threshold down to force the broken
   behavior, then turn them on to show the fix. Ship a "constrained" vs "tuned" profile the
   learner flips between.
4. **Engineer the pathology.** Skew = one key holding 90% of rows. Small files = a trigger that
   emits hundreds of tiny files. Driver OOM = `.collect()` on a generated-large frame. WAL growth
   = an unconsumed Postgres replication slot. Each is a deliberate, documented recipe.

---

## PEDAGOGICAL PATTERN (every challenge module follows this)

1. **The scenario** — a realistic production story.
2. **Break it** — run the pathological version; it fails or crawls.
3. **Detect it** — a guided **Spark UI / tool-dashboard walkthrough**: exactly which tab/metric
   reveals the problem (task-time max-vs-median for skew, spill metrics, GC time, shuffle size,
   consumer lag, replication-slot size, connector status). Annotated "what you should see".
4. **Diagnose** — name the root cause and *why* it happens.
5. **Fix it** — the production-grade remedy.
6. **Prove it** — a **before/after metrics table** from a reusable helper. Learning is *quantitative*.
7. **Takeaways + "in real production…"** — how you'd detect/prevent this at scale (alerts, guardrails,
   config defaults).

---

## CONFIRMED DECISIONS (from the project owner)

1. **Data quality: teach BOTH** dbt native tests **and** Great Expectations — show where each fits
   (dbt tests for structural/in-pipeline assertions; GE for statistical/profiling/drift and
   standalone validation), and how they complement each other.
2. **New Relic / observability: optional and masked.** The core curriculum is **100% offline**.
   Any New Relic references inherited from the old Airflow code must be **removed or masked**
   (no real account IDs, API keys, internal domains, S3 buckets, Snowflake roles). An optional
   observability module may use a generic/local approach; NR specifics are an opt-in appendix.
3. **Build incrementally.** Start with the **Spark data-skew module as the flagship reference**,
   verify the framework (data generator, Spark UI walkthrough, metrics_diff, resource profiles)
   works end-to-end, checkpoint with the owner, *then* expand to the next modules.
4. **Airflow DAGs: rewrite as generic, local-runnable teaching DAGs.** The real `prodrat_main`
   DAG (Snowflake/S3/K8s-dependent, full of internal infra) is **not** teaching material — replace
   it. Optionally keep one **sanitized, read-only** copy as a "this is what production looks like"
   reference, with all secrets/infra masked.

---

## ADDED SCOPE

### A. Debezium CDC track (new)
Add a self-contained CDC track: spin up a **local Postgres**, enable logical replication, run
**Debezium** (via Kafka Connect) to stream changes into Kafka, then consume with **Spark
Structured Streaming** and **MERGE** into an **Iceberg** table that mirrors the source. Teach the
full pipeline *and* its production failure modes (snapshot vs streaming, replication-slot/WAL
growth, tombstones/deletes, replica identity, schema evolution, idempotent upsert sink).

### B. Modular folder structure (per tool / per topic)
Just as `airflow/` and `dbt/` are self-contained today, **every track is its own top-level,
self-contained module** with its own notebooks/configs/README so learners can start with whatever
they want to learn. Shared utilities live in one common toolkit. Target layout:

```
spark-dev/
├── docs/            # briefs, plan, learning path, Spark-UI guide, cheat-sheets
├── common/          # shared toolkit: datagen, metrics_diff, resource profiles, spark session
├── spark/           # Spark performance-challenge modules (notebooks + README)
├── iceberg/         # Lakehouse / table-format modules
├── kafka/           # Kafka + Structured Streaming modules + producers
├── debezium/        # CDC track: Postgres + Debezium compose, connector configs, notebooks
├── dbt/             # dbt project (expanded)
├── quality/         # Great Expectations + dbt-test labs
├── airflow/         # generic local teaching DAGs (+ optional sanitized prod reference)
├── conf/ scripts/ Dockerfile docker-compose.yml
```
(Exact final layout to be confirmed in Phase 0 — keep the migration incremental so the repo
stays runnable at every step.)

### C. Depth, not just breadth, for every tool
The curriculum should not be Spark-skewed. For **each** tool (Spark internals, Iceberg, Kafka,
Debezium, dbt, Airflow) include must-know, good-to-have, and niche/deep topics — the deep internals
that separate senior engineers. The full per-tool topic inventory is catalogued in
[`CURRICULUM_PLAN.md`](./CURRICULUM_PLAN.md).

---

## DELIVERABLE STANDARDS

- Each module: self-contained, runnable top-to-bottom, with markdown narration in the
  Break → Detect → Fix → Prove structure, and a teardown step.
- Reusable utilities (data generator, metrics_diff, resource profiles) live in `common/` with docstrings.
- Each track folder has its own README; the repo README carries the master learning path.
- Keep dependencies minimal; prefer what's already in the stack.
- Keep CLAUDE.md updated as the architecture grows (it currently omits Airflow, Parquet, and `agg_customers`).

---

## REPO HYGIENE / FIXES TO FOLD IN

- **Sanitize internal infra.** `airflow/dags/prodrat_main/` + `config.py` contain real internal
  infrastructure (S3 buckets, K8s conn IDs, internal cell domains, Snowflake roles, an NR account
  ID). Remove or mask before this is shared.
- **Fix `.gitignore`.** It currently ignores `airflow/dags/`, so the orchestration teaching layer
  isn't tracked. Track the new sanitized teaching DAGs.
- **Fix notebook bugs.** `04_sales_streaming` uses `customers_df` before definition and uses `os`
  without import; `03_query_iceberg` header references the old `01_csv_to_iceberg` name.
- **Resolve doc drift.** README/CLAUDE.md vs. reality (Airflow, Parquet, `agg_customers`, Hudi
  shown as live but commented out).
- **Remove cruft.** Committed `Untitled.ipynb` / `Spark DataFrame.ipynb`, stray `.ipynb_checkpoints/`,
  committed `dbt/target/` and `airflow/.airflow_home/` artifacts.

---

## GUARDRAILS

- **Do not over-engineer.** Favor the simplest thing that teaches the concept. No frameworks for
  one-off helpers.
- **Laptop safety is non-negotiable** — every heavy/destructive exercise is capped and reversible.
- **Work incrementally**, one module/phase at a time; keep the repo runnable at every step.
- **Consult the owner** when a decision is genuinely ambiguous (scope, tool choice, anything that
  changes the learner experience) rather than guessing.

---

## RESOLVED DECISIONS (Phase 0 ground rules)

1. **Folder migration — gradual, but rename freely.** The repo must stay runnable so each phase can
   be tested as it's built, so migrate to the per-track layout incrementally. **However**, this does
   not block renaming folders/files to proper, clear names — when you rename, update **every**
   reference to the old name (compose, Makefile, entrypoint, configs, notebooks, imports, docs) in
   the same change so the repo keeps working.
2. **Debezium deployment — Kafka Connect + Debezium Postgres connector** (mirrors real production),
   not Debezium Server.
3. **Memory budget — target an 8 GB / 256 GB laptop; use only what's needed, leave headroom.**
   - Leave ~3 GB for the host OS; the whole Docker stack should stay within ~5 GB.
   - **Constrained Spark profile:** container `mem_limit` ≈ **2 GB**, `driver.memory` ≈ 1 GB — small
     enough that OOM/spill trigger easily while the laptop stays usable.
   - **Default (tuned) Spark service:** `mem_limit` ≈ **3 GB**.
   - The Debezium track adds Postgres + Kafka Connect; document that learners may need to stop other
     optional services (e.g. history server) while running the heavier CDC pipeline on 8 GB.
   - Keep disk in check: all generated data stays in `.tmp/`, streams auto-stop, `make clean` recovers.
