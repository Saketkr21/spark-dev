# INC-3 — Tiny dashboard table got slow overnight ⛑️

> **Page:** P3: the `events` dashboard tile that reads an Iceberg table has crept from <1 s to **8–12 s** over the last week. The table holds only a few thousand rows and barely grew. Users think the dashboard is "broken."
> **Handed to you:** the slow read + the table's storage and Iceberg metadata tables. Spark UI at http://localhost:4040; `common.iceberg_meta.table_health`. Diagnose before changing code.

## Symptom
- Query latency is climbing steadily — ~1 s → ~10 s over a week — but `COUNT(*)` is **~flat** (a few thousand rows the whole time). The cost isn't the data volume.
- The result is correct. Time travel works. Nothing errors. It's just slow, and getting slower every day.
- Object storage / the warehouse dir for this table shows **thousands of files**, almost all of them a few **KB** each.
- A streaming/micro-batch job upserts into this table every few seconds and has been running all week.

## Your job (think like an SRE)
1. **Where do you look first?** Row count says "trivial table," yet it's slow — so don't profile the *data*, profile the *layout*. Which Iceberg metadata table tells you how the rows are physically stored?
2. **What measurement confirms the root cause vs. the alternatives?** Is this a slow planner, a cold cache, network, or genuine fragmentation? What single pair of numbers separates "one big healthy file" from "ten thousand tiny ones"?
3. **What's the fix — and what number proves it worked?**

<details>
<summary>🔧 Diagnosis &amp; fix — open only after you've formed a hypothesis</summary>

- **Root cause:** **Small-files problem.** The streaming job commits a tiny micro-batch every few seconds, and **every Iceberg commit writes at least one data file per partition** (often several, at the default write parallelism) — by design, so writers never coordinate. After a week that's thousands of tiny files holding a few thousand rows. Query time is dominated not by scanning rows but by the planner **opening and reading the footer of every file** (schema + column stats) and paying a fixed per-file open cost on the scan. More CPU doesn't help: the cost is **per file, not per row**, so adding executors leaves it just as slow.

- **Detect:** read the table's own metadata, not the Spark UI Stages tab — this is a *metadata* pathology:
  ```sql
  SELECT COUNT(*) AS files, AVG(file_size_in_bytes) AS avg_bytes
  FROM iceberg_catalog.default.<table>.files;
  ```
  The tell: a **huge `.files` count** with a **tiny avg `file_size_in_bytes`** (KB, not MB) — while the row count stays small. `common.iceberg_meta.table_health(spark, T, "before")` prints `data_files` and `avg_file_bytes` directly. A cold-cache or network theory would *not* show thousands of KB-sized files; a slow-planner theory still bottoms out in "too many footers to open." In the Spark UI the same problem appears at query time as a **scan node reading thousands of files**.

- **Fix:** **bin-pack compaction** — rewrite the many small files into a few large ones in one new snapshot:
  ```sql
  CALL iceberg_catalog.system.rewrite_data_files(
    table => 'default.<table>',                 -- namespace.table, NO catalog prefix
    options => map('min-input-files','2')
  );
  ```
  Then **prevent re-fragmentation**: set a target file size (`ALTER TABLE ... SET TBLPROPERTIES ('write.target-file-size-bytes'='134217728')`, 128 MB); **write fewer, larger batches** upstream (the streaming knob in STR-3 — `maxOffsetsPerTrigger` / longer trigger intervals); and **schedule compaction** as nightly maintenance. (Delta equivalent: `OPTIMIZE <table>`.) Note compaction is *not* cleanup — it leaves the old snapshots and their files in place; pair it with `expire_snapshots` (LAK-3) and `remove_orphan_files` (LAK-4) to actually reclaim storage.

- **Prove:** `common.iceberg_meta.compare_health([before, after])` prints the before/after. Data-file count collapses from **hundreds/thousands → a handful** (e.g. 1–4) while **avg file size rises** sharply (KB → MB). That pair moving in opposite directions — not "it feels faster" — is the proof; the query latency falls back toward <1 s because the planner now opens a handful of footers instead of thousands.

- **Reproduce &amp; learn it:** [LAK-2](../../iceberg/small_files/) — run that module to watch the `.files` count balloon under tiny appends and then collapse after `rewrite_data_files`, with `avg_file_bytes` rising as the proof.
</details>
