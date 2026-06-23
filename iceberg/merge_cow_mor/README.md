# LAK-8 — MERGE / upsert: Copy-on-Write vs Merge-on-Read

> **Break → Detect → Fix → Prove.** A tiny upsert — one CDC row, one late correction — into a
> partitioned table. Under **copy-on-write** (Iceberg's default) that 1-row `MERGE` **rewrites every
> data file in the touched partition**: write amplification far larger than the change. The
> **merge-on-read** alternative writes a small **delete file** instead — cheap writes, but reads must
> now reconcile deletes and you must compact periodically.

- **Notebook:** [`lak8_merge.ipynb`](./lak8_merge.ipynb)
- **Toolkit used:** `common.iceberg_meta` (`table_health` / `compare_health` — the "Prove it"),
  `common.metrics_diff` (`measure` — the MERGE's write cost), `common.spark_session`
- **Run against:** the unified Spark server (`make up`) — Spark UI at http://localhost:4040.
- **Time:** ~10 min. **Laptop-safe:** a few hundred rows across a handful of partitions, all under
  `.tmp/`; the notebook drops both tables at the end (`make clean` clears the rest).

---

## 1. The scenario

A pipeline upserts changes into a partitioned fact table: a CDC stream applies a handful of updated
rows, or an analyst issues a one-row correction ("order #12345's amount was wrong"). The change is
*tiny* — one row, sometimes a few. The table is partitioned (here, bucketed on `customer_id`) so the
matched row lives in exactly one partition.

You'd expect a 1-row update to write ~1 row's worth of data. Under **copy-on-write** it doesn't.
Iceberg data files are immutable, so to "change" one row CoW reads the **whole data file(s)** that
contain matched rows, applies the change in memory, and writes a **brand-new file** for each — then
commits a snapshot that swaps old files for new. The touched partition is effectively rewritten. For
a 1-row change in a partition holding hundreds of rows that's **hundreds of rows rewritten per row
changed** — pure write amplification. At production scale (partitions of millions of rows, hundreds
of MB each) a single-row CDC apply can rewrite hundreds of MB.

## 2. Break it — copy-on-write MERGE

We create a partitioned `iceberg_catalog.default.lak8_cow` (CoW is the default write mode), load a
few hundred orders bucketed across a small number of partitions, then `MERGE` a **single updated
row**:

```python
spark.range(1, 401)...writeTo(COW).using("iceberg")
    .partitionedBy(F.bucket(8, "customer_id")).create()   # CoW by default

spark.sql(f"""
  MERGE INTO {COW} t USING one_update u ON t.order_id = u.order_id
  WHEN MATCHED THEN UPDATE SET t.amount = u.amount
""")                                                       # rewrites the whole partition's file(s)
```

`table_health(spark, COW, ...)` before vs after the MERGE shows the **data-file count churn** and the
snapshot's own summary (`added-data-files` / `deleted-data-files`) shows the rewrite: the MERGE
**adds** new files and **deletes** the old ones for the touched partition — far more bytes than the
1-row change. `common.metrics_diff.measure` around the MERGE captures the write as a cost number.

> Why this is laptop-safe: the table is intentionally small (a few hundred rows). The point isn't
> volume — it's **amplification ratio**. Even at this scale the 1-row MERGE rewrites an entire
> partition's file, which is exactly the fingerprint we want to see and then avoid.

## 3. Detect it — read the snapshot + `.files` metadata

This is a **write-amplification** pathology. The tells live in Iceberg's metadata, not the Spark UI
Stages tab:

```sql
-- what the MERGE actually wrote (CoW): whole data files rewritten
SELECT operation, summary['added-data-files']   AS added_data,
                  summary['deleted-data-files'] AS deleted_data,
                  summary['added-delete-files'] AS added_deletes
FROM   iceberg_catalog.default.lak8_cow.snapshots
ORDER BY committed_at DESC LIMIT 1;

-- file content types: 0 = data, 1 = position-delete, 2 = equality-delete
SELECT content, COUNT(*) AS files, SUM(file_size_in_bytes) AS bytes
FROM   iceberg_catalog.default.lak8_cow.files GROUP BY content;
```

| Signal | Copy-on-write (broken-by-default) | Merge-on-read (the fix) |
|--------|-----------------------------------|-------------------------|
| Snapshot `added-data-files` | ≥1 (a full rewritten data file) | 0 |
| Snapshot `deleted-data-files` | ≥1 (the old file it replaced) | 0 |
| Snapshot `added-delete-files` | 0 | **≥1** (a tiny delete file) |
| `.files` `content` types present | only `0` (data) | `0` **and** `1`/`2` (delete) |
| Bytes written for a 1-row change | ≫ 1 row (whole partition file) | tiny (delete vector only) |

The companion entry is the **LAK-8 row** in [`docs/troubleshooting.md`](../../docs/troubleshooting.md):
*"A 1-row MERGE/update rewrites an entire partition's worth of files."*

## 4. Diagnose

Iceberg (and Delta, and Parquet) data files are **immutable** — you cannot edit a row in place. So a
MERGE/UPDATE/DELETE has two possible strategies for expressing a change:

- **Copy-on-write (CoW, the default):** rewrite each data file that contains a matched row into a new
  file with the change applied, and commit a snapshot that swaps old → new. **Reads stay trivial**
  (just read current data files), but **every write rewrites whole files** even for one changed row.
- **Merge-on-read (MoR):** leave the data file untouched and write a small **delete file** marking the
  old row as removed (plus a tiny data file for any new/updated values). **Writes are cheap**, but
  **every read must merge the delete files** against the data files to produce the correct rows.

CoW pays the cost at **write** time and amortizes nothing across reads; MoR pays it at **read** time
and defers it. The whole-partition rewrite you saw is CoW working exactly as designed — immutability
plus "keep reads cheap" *forces* the rewrite.

## 5. Fix / tradeoff — merge-on-read MERGE

Create the same table with **merge-on-read** write modes (format-version 2 is required for delete
files):

```python
spark.range(1, 401)...writeTo(MOR).using("iceberg")
    .partitionedBy(F.bucket(8, "customer_id"))
    .tableProperty("format-version", "2")
    .tableProperty("write.merge.mode",  "merge-on-read")
    .tableProperty("write.update.mode", "merge-on-read")
    .tableProperty("write.delete.mode", "merge-on-read")
    .create()
```

(Or flip an existing table: `ALTER TABLE ... SET TBLPROPERTIES ('format-version'='2',
'write.merge.mode'='merge-on-read', ...)`.) The **same 1-row MERGE** now writes a small delete file
(`content` = 1, a position delete) instead of rewriting the partition — `added-delete-files` ≥ 1,
`added-data-files` ≈ 0, bytes written tiny.

**The MoR catch — and the compaction that fixes it.** MoR makes writes cheap but pushes work to
reads (every scan reconciles delete files) and lets delete files **accumulate**. Periodic compaction
applies the deletes and removes them, restoring read speed:

```python
spark.sql("""
  CALL iceberg_catalog.system.rewrite_data_files(
    table => 'default.lak8_mor',
    options => map('delete-file-threshold','1')   -- rewrite files carrying deletes
  )
""")
```

After compaction the `.files` content types collapse back to data-only (`content` = 0) — the same
`rewrite_data_files` procedure from **LAK-2**, here clearing **delete** files rather than bin-packing
small ones. MoR without scheduled compaction degrades reads over time; that maintenance is the price
of cheap writes.

## 6. Prove it

`common.iceberg_meta.compare_health([cow_before, cow_after, mor_before, mor_after])` plus the
per-snapshot `added/deleted` summary make the contrast concrete for the **same 1-row change**:

| | CoW MERGE | MoR MERGE |
|---|---|---|
| Data files added | ≥1 (full rewrite) | ~0 |
| Data files deleted | ≥1 (old file) | 0 |
| Delete files added | 0 | **≥1 (tiny)** |
| Bytes written | **whole partition** | **delete vector only** |
| Read cost afterward | unchanged (cheap) | higher until compaction |

CoW writes far more than one row's worth; MoR writes almost nothing — that's the proof. The mirror
cost (MoR's slower reads + the compaction debt) is why this is a **tradeoff**, not a free win.

## 7. Takeaways & "in real production…"

- **CoW = read-optimized, write-amplified.** Great when reads dominate and writes are infrequent /
  batched. A 1-row CoW update rewrites whole files — never apply CDC row-by-row to a CoW table.
- **MoR = write-optimized, needs compaction.** Great for high-frequency upserts (CDC, streaming),
  but reads pay to reconcile deletes and delete files pile up — **schedule `rewrite_data_files`** to
  apply and clear them (ties to **LAK-2**).
- **Choose by your read/write ratio.** Read-heavy + rare writes → CoW. Write-heavy / frequent small
  upserts → MoR + compaction. This is the same decision dbt makes for incremental models (**DBT-2**).
- **Batch your upserts.** Whichever mode, **amortize the cost**: under CoW, batch many changes into
  one MERGE so you rewrite each partition once, not once per row; under MoR, compact on a cadence so
  reads don't drift.
- **Partition so changes localize.** Good partitioning means a small upsert touches one partition,
  not all of them — the difference between rewriting one file (CoW) and rewriting the whole table.

## 8. Teardown

The notebook ends with a **Teardown** cell that drops `iceberg_catalog.default.lak8_cow` and
`iceberg_catalog.default.lak8_mor`. `make clean` removes everything under `.tmp/` if you want a
fully fresh warehouse.
