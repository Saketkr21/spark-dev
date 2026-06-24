# INC-7 — Backfill doubled yesterday's revenue ⛑️

> **Page:** `[warn] daily_orders_rollup: 2025-03-01 revenue = $204,118 — +98.7% vs 7-day median ($102,740). Anomaly detector tripped.`
> **Handed to you:** the Airflow UI for `daily_orders_rollup` and one row-count query against the `dt=2025-03-01` partition of the rollup table. An on-call from last night cleared and re-ran that date after a transient upstream failure. Diagnose before acting — do **not** "just re-run it again."

## Symptom

The metric for a single date is almost exactly **2×** the truth — not noisy, not +12%, a clean double. Every other date looks right. The doubled date is the one someone **re-ran / backfilled**. Spot-checking the partition shows each `order_id` for that day appears **twice**, and `sum(amount)` is exactly double the distinct-key sum. Nothing upstream changed; the source for that date is unchanged.

## Your job (think like an SRE)

1. **Where do you look first?** The metric jumped only for a re-run date — what's special about a re-run vs a first run? Pull the task that writes that partition and read how it writes.
2. **What confirms the root cause?** Take the affected date, run it once into a clean partition, count the rows. Run the **same logical date again**. Does the count stay put, or grow?
3. **What's the fix — and what proves it?** A correct daily write must be safe to replay. What write semantics give you that, and how do you demonstrate a re-run is now a no-op on the row count?

<details>
<summary>🔧 Diagnosis &amp; fix — open only after a hypothesis</summary>

- **Root cause:** the rollup task is **non-idempotent**. It writes its output in **append** mode keyed on nothing stable — every invocation for a date *adds* a fresh copy of that day's rows instead of *replacing* them. A first run looks fine; a retry / manual clear-and-rerun / backfill stacks a second copy, so the partition double-counts. The data-interval re-execution model (Airflow retries and backfills re-run the *same* `data_interval`) guarantees this eventually fires.
- **Detect:** count rows for the affected date across **one run vs two runs of the same `--logical-date`** — a non-idempotent task grows, an idempotent one is stable. In the code, look for a blind `append` / bare `INSERT` with **no `data_interval` key and no `unique_key`** (e.g. `open(path, "a")`, or `df.write.mode("append")` into the date partition). The smoking gun: `count(*)` for the partition is an exact integer multiple of `count(distinct order_id)`.
- **Fix:** make the write a **pure function of the interval** so replays converge. Either **OVERWRITE the partition** keyed on `data_interval_start` (`open(path, "w")` / `df.write.mode("overwrite")` / `INSERT OVERWRITE … PARTITION (dt=…)`), or **`MERGE` / upsert on a `unique_key`** (`order_id`). Derive the partition value **only** from the data interval — **never** from `now()` / wall-clock, or re-runs land in the wrong partition.
- **Prove:** re-run the same date back-to-back; the partition's `count(*)` is **identical** both times and `count(*) == count(distinct order_id)`. Backfilling a range is now safe to replay — each date converges to one copy.
- **Reproduce &amp; learn it:** [AF-1](../../airflow/dags/af1_idempotency.py) — the `append_partition` (broken) vs `overwrite_partition` (fixed) tasks demonstrate exactly this double-count and the interval-keyed overwrite cure; run the DAG twice for the same date. Then [DBT-2](../../quality/dbt2_incremental/) for the warehouse equivalent: `incremental_strategy='merge'` + `unique_key='order_id'` upserts by key so overlapping/replayed batches don't duplicate (`count(order_id) == count(distinct order_id)` stays equal).

</details>
