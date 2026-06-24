# INC-6 — Yesterday's revenue is short ⛑️

> **Page:** Finance — yesterday's revenue in `analytics.fct_orders` is short of the source-of-truth total; a handful of orders are missing from the mart even though they exist in the upstream orders table.
> **Handed to you:** the dbt run logs for the nightly incremental build, plus query access to both the source (`stg_orders`) and the mart (`fct_orders`). Diagnose before acting.

## Symptom
The daily incremental mart is **missing rows**. The missing orders are present in the source — but they have an unusual shape: their **event time** (`ordered_at`) falls *before* the previous run's high-water mark, while their **load time** (`loaded_at`) is *after* it. They are **late arrivals** — landed today, but stamped with an earlier business timestamp. Each nightly run only adds rows strictly newer than the previous max event-time, so once a late row's event time is below the watermark, it never gets picked up. No error, no failed test — the rows are just silently absent and the revenue total quietly under-counts.

## Your job (think like an SRE)
1. Where do you look first? (Is this an upstream data gap, a failed load, or a filter in the model?)
2. What confirms the root cause — what does the incremental `WHERE` clause actually select, and why would a row that *is* in the source never reach the `MERGE`?
3. What's the fix — and what count proves the missing rows are back without double-counting the rest?

<details>
<summary>🔧 Diagnosis &amp; fix — open only after a hypothesis</summary>

- **Root cause:** The incremental filter watermarks on **event time** with **zero lookback**:
  ```sql
  where ordered_at > (select max(ordered_at) from {{ this }})
  ```
  That is correct only if data always arrives in event-time order. It doesn't. A row can land **today** (`loaded_at` = today) carrying **yesterday's** `ordered_at`. Its event time is already *below* the high-water mark, so the `WHERE` excludes it **before the `MERGE` ever sees it**. The watermark only moves forward, so re-running never recovers it — the late row is lost permanently. This is the freshness-vs-completeness trade: tight watermark = cheap runs but lost stragglers.

- **Detect:** Reconcile source against mart for the window, then hunt the exact shape:
  ```sql
  -- counts disagree
  SELECT COUNT(*) FROM analytics.fct_orders;              -- short
  -- find rows that loaded recently but predate the mart's watermark
  SELECT * FROM analytics.stg_orders s
  WHERE s.ordered_at < (SELECT max(ordered_at) FROM analytics.fct_orders)
    AND s.loaded_at  > (SELECT max(loaded_at)  FROM analytics.fct_orders)
    AND s.order_id NOT IN (SELECT order_id FROM analytics.fct_orders);
  ```
  Any row returned is a dropped late arrival: event-time below the watermark, load-time after it, absent from the fact. On a real-sized table you catch this with a reconciliation count or a relationship/recency test, not by eyeballing rows.

- **Fix:** Add an explicit **lookback window** to the incremental filter so each run re-scans a bounded slice of recent history:
  ```sql
  where ordered_at > (select max(ordered_at) from {{ this }})
                     - interval '{{ var("lookback_hours", 48) }}' hour
  ```
  Re-scanning is **safe** because `fct_orders` is an incremental `merge` keyed on `unique_key = order_id`: rows already present update in place, the genuinely missing late row inserts — no duplicates, no double-counting. Size the lookback to your **worst realistic lateness** (p99 arrival lag): large enough to catch stragglers, small enough that re-reading stays cheap. The `unique_key` MERGE is what makes any lookback idempotent.

- **Prove:** Re-run the same batch (`load_through` unchanged) with the lookback set, and count. The missing late rows reappear and the mart count matches the source for the window — e.g. naive run = 13 rows (late order dropped), fixed run with `lookback_hours: 48` = 14 rows (late order **recaptured**), and the 13 already-present rows are not duplicated.

- **Reproduce &amp; learn it:** [DBT-3](../../quality/dbt3_late_arriving/) — a 14-row seed with separate `ordered_at` (event) and `loaded_at` (load) clocks and exactly one genuinely-late order; replay batch 1 → naive batch 2 (lookback 0, row dropped) → fixed batch 2 (lookback 48 h, row recaptured) and watch the count go 12 → 13 → 14.

</details>
