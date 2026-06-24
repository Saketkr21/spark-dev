# INC-1 — Nightly aggregation blew its SLA ⛑️

> **Page:** P2: nightly `agg_orders` revenue rollup missed its 02:00 SLA. It normally finishes in ~2 min; it's still running at 06:00 and on-call got paged when the downstream dashboard refresh failed.
> **Handed to you:** the running job + the Spark UI (http://localhost:4040) + the driver logs. No code changes yet — diagnose first.

## Symptom
- The job is the same code that ran fine last night. Input volume is flat (`orders` grew <2% day-over-day).
- Spark UI → **Stages**: the join's reduce stage shows **199/200 tasks complete** and **1 task still running** — and has been for ~3.5 hours.
- That one task's **Shuffle Read** is ~40× the median task; everything else finished in milliseconds and the executors are otherwise idle.
- No errors in the log. Nothing is crashing. It's just… stuck on one task.

## Your job (think like an SRE)
1. **Where do you look first?** Don't trust averages — they hide a straggler. Which Spark UI tab and which row in the task table tells you *one* task is doing all the work?
2. **What measurement confirms the root cause vs. the alternatives?** Is this a slow executor (hardware), a GC pause, or genuine data imbalance? What single number distinguishes "one fat partition" from "everything is uniformly slow"?
3. **What's the fix — and what number proves it worked?**

<details>
<summary>🔧 Diagnosis &amp; fix — open only after you've formed a hypothesis</summary>

- **Root cause:** **Data skew.** One key holds most of the rows (here a marketplace "house account", `customer_id = 0`, ≈90% of all orders). The join hash-shuffles `orders` by `customer_id`, so every row for that one key lands in the **same** reduce partition. One task gets ~90% of the data; the other 199 finish instantly and wait. That single straggler *is* the job — and adding executors won't help, because the work is pinned to one key's partition and can't be split across tasks.

- **Detect:** Spark UI → **SQL / DataFrame** → the running query → the join's reduce **Stage** → **Tasks** → **Summary Metrics**. The tell: **Task Duration / Shuffle Read — Max ≫ 75th ≫ Median** (the max is ~40× the median; a healthy stage clusters near the median). The **Event Timeline** shows one long bar with all others finished long ago. Often the straggler also **spills** memory→disk while sorting its giant partition. A uniformly slow stage (bad node / GC) would push *all* percentiles up together — skew blows out only the max.

- **Fix (in priority order):**
  1. **Broadcast the small side** — `df.join(F.broadcast(dim), ...)` (or raise `spark.sql.autoBroadcastJoinThreshold`). The dimension is replicated, the shuffle disappears, and skew becomes irrelevant. *Try this first.*
  2. **AQE skew-join** — `spark.sql.adaptive.enabled=true` + `spark.sql.adaptive.skewJoin.enabled=true`; Spark splits the skewed partition at runtime. (At small scale you must lower `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` — the 256 MB default never trips on tiny data — and carry real payload, not just a count.)
  3. **Salt the hot key** — add a random `salt` to the fact's hot key, replicate the dimension across all salts, join on `(key, salt)`. Use when you can't broadcast and need explicit control.

- **Prove:** capture the **skew ratio = max task time ÷ median task time** before and after. Broken: large (≈30–50×). Fixed (broadcast / AQE / salt): collapses toward **~1×** (broadcast gets nearest to 1×). The ratio dropping from tens-of-× to ~1× — not "it feels faster" — is the proof. `common.metrics_diff.compare([...])` prints the before/after table.

- **Reproduce &amp; learn it:** [SPK-1](../../spark/skew/) — run that module to watch the straggler appear in the UI and the skew ratio collapse after each fix.
</details>
