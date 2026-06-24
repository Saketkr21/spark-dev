# INC-2 — Reporting job dies mid-run, session vanishes ⛑️

> **Page:** P2: the `wide_report` notebook/job died at ~03:10. The Spark Connect session dropped and any retry from the same client immediately errors with `[NO_ACTIVE_SESSION]`. The on-call owner says "it worked on my dev sample yesterday."
> **Handed to you:** the dead session + the Spark UI (http://localhost:4040) + the driver logs. No code changes yet — diagnose first.

## Symptom
- The job builds a wide per-event frame (dozens of columns, tens of millions of rows), then dies — always **right after the "pull results to the client" step**, never during the upstream transforms.
- Driver logs show either `java.lang.OutOfMemoryError` / `GC overhead limit exceeded`, **or** (with this repo's guardrail on) a clean abort: `Total size of serialized results of N tasks (X GB) is bigger than spark.driver.maxResultSize (1024.0 MiB)`.
- The Spark UI **Executors** tab shows executors idle by the time it dies; the **driver** row is where the memory pressure sits.
- Someone already tried "just add more executors." It didn't help — same death, same step.

## Your job (think like an SRE)
1. **Where do you look first?** The session is gone — what's the *last* thing the job did before it died, and which UI tab (or log line) names the action that triggered it?
2. **What measurement confirms the root cause vs. the alternatives?** Is this a slow query, a bad executor, or something funneling through one process? What config tells you the size of the box the result has to fit into?
3. **What's the fix — and what number proves it worked?**

<details>
<summary>🔧 Diagnosis &amp; fix — open only after you've formed a hypothesis</summary>

- **Root cause:** **Driver OOM from collecting unbounded data.** The job ends with `.collect()` / `.toPandas()` on a generated-large frame (or an oversized `F.broadcast(big_df)`), which ships **every** partition's rows back to the **single driver JVM** to assemble one local object. The driver heap is fixed and small, so the *result size* — not cluster compute — is the limit. That's why more/bigger executors do nothing: the rows still funnel through the one driver. Over Spark Connect a real driver OOM **hard-kills the session** (it's not a catchable Python exception) — which is exactly why the next client call sees `[NO_ACTIVE_SESSION]`.

- **Detect:** Driver logs + Spark UI. **Jobs → Event Timeline:** tasks complete on the cluster, then a long **driver-side gap / failure** as results are pulled back and assembled (not cluster compute). **SQL / DataFrame:** the query is a `Collect` / `CollectLimit` action whose scan reads ~all rows even though "you only wanted to look at it." **Executors:** pressure is on the **driver** row, executors idle. **Environment:** check `spark.driver.maxResultSize` (1g here) and `spark.driver.memory` — the size of the box the result must fit into. The definitive tell when the cap is set is the `maxResultSize` message; without the cap it's an `OutOfMemoryError` on the driver.

- **Fix (don't move big data to the driver):**
  - **Aggregate on the cluster first** — `df.groupBy(...).agg(...)` then collect; only the tiny grouped result returns.
  - **`limit()` before collecting** — `df.limit(1000).toPandas()` to *peek* at a bounded sample.
  - **Write / stream instead of collect** — `df.write...` to a table; the data never transits the driver. The default for any genuinely large result.
  - **Broadcast only small sides** — `F.broadcast(small_dim)` is correct; broadcasting a large frame is the bug.
  - Raise `spark.driver.memory` only as a **last resort** — it buys headroom, not a fix.
  - **Recover the dead session:** call `common.spark_session.reconnect()` (a stale Connect handle keeps raising `[NO_ACTIVE_SESSION]` until you rebuild it).

- **Prove:** before → the collect step **errors after shipping** (>1 GB blocked by `maxResultSize`, or driver OOM) and the session dies. After → the job **completes by writing to a table** (or returns ≤ `limit` rows / a handful of aggregated rows), and the **session stays alive**. The contrast — unbounded collect vs. bounded result, dead session vs. live session — is the proof, not wall-clock alone.

- **Reproduce &amp; learn it:** [SPK-3](../../spark/driver_oom/) — run that module to watch the collect get blocked, then watch each fix return a small result while the session survives.
</details>
