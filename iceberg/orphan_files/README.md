# LAK-4 — Orphan files & GC

> **Break → Detect → Fix → Prove.** Failed/aborted writes, cancelled compactions, and direct
> path writes leave **data files on disk that no snapshot references** — "orphans". They consume
> storage forever and are **not** reclaimed by snapshot expiry. The fix is Iceberg's
> `remove_orphan_files` GC procedure.

- **Notebook:** [`lak4_orphan_files.ipynb`](./lak4_orphan_files.ipynb)
- **Toolkit used:** `common.iceberg_meta` (`table_health` — the metadata file count = the "Prove it")
- **Run against:** the unified Spark server (`make up`) — connects via Spark Connect.
- **Time:** ~5 min. **Laptop-safe:** tiny generated data, everything under `.tmp/`; teardown drops
  the table and `make clean` clears `.tmp/`.

See the [`iceberg/` track index](../README.md), the sibling [`LAK-1` format comparison](../format_comparison/),
and the [troubleshooting sheet](../../docs/troubleshooting.md).

---

## 1. The scenario

A streaming job writes into an Iceberg table all day. Occasionally a write task is **killed**
mid-flight (executor OOM, spot reclaim, a cancelled compaction), or someone runs a one-off Spark
job that writes Parquet **straight into the table's directory** instead of going through the
catalog. In every case the *physical* `.parquet` files land on disk — but the **commit never
happens**, so no manifest, no snapshot, nothing in the table metadata ever points at them.

Months later the storage bill is climbing and nobody knows why: the table only has a few thousand
rows, yet its directory holds gigabytes. Those unreferenced files are **orphans**, and ordinary
`expire_snapshots` will never touch them — expiry only removes files that *were* referenced by an
old, now-expired snapshot. A file that was **never** referenced is invisible to snapshot logic.

## 2. Break it — create orphans deterministically

We create `iceberg_catalog.default.lak4_t`, append a little data through the catalog (so the
table has a real, referenced set of files), then **write extra Parquet files directly into the
table's data directory** so they bypass the commit path entirely:

```python
spark.range(5000).withColumn("v", F.rand()).write.mode("append") \
     .parquet(".tmp/local_iceberg_warehouse/default/lak4_t/data")
```

Those files are physically present under `…/lak4_t/data/` but appear in **no manifest** — the
table can't see them. (Iceberg's Hadoop catalog stores an unpartitioned table's files flat under
`<table>/data/*.parquet`, which is exactly where we drop the orphans.)

> Why this is laptop-safe: a few thousand generated rows in a handful of small files, all under
> `.tmp/`. Nothing fills memory or disk; the teardown removes the table.

## 3. Detect it — disk files > metadata files

Two counts that *should* match but won't:

| Count | How we get it | What it measures |
|-------|---------------|------------------|
| **Metadata data-files** | `table_health(spark, ICE)["data_files"]` (reads `<table>.files`) | files the table **references** |
| **On-disk Parquet files** | Python `os`/`glob` over `…/lak4_t/data/**/*.parquet` | files **physically present** |

After the direct write, **on-disk > metadata** — and `SELECT COUNT(*)` on the table is unchanged
(the orphan rows are invisible). The gap *is* the orphan count, and it's pure wasted storage.

The production signal is the same: storage for a table grows with **no matching growth in row
count or referenced files**, and snapshot expiry doesn't bring it back down.

## 4. Diagnose

Orphans are **unreferenced by any metadata**, so the snapshot machinery has no handle on them:

- `expire_snapshots` removes data files that belonged to snapshots it expires. An orphan was
  never in *any* snapshot, so it is never a candidate — running expiry does nothing for it.
- Only a procedure that **lists the directory and diffs it against the metadata** can find them.
  That's exactly what `remove_orphan_files` does.

## 5. Fix it — `remove_orphan_files`

```python
spark.sql("CALL iceberg_catalog.system.remove_orphan_files("
          "table => 'default.lak4_t', older_than => now())")
```

It lists every file under the table location, subtracts the files the current metadata references,
and **deletes the difference** — returning the list of removed paths. The on-disk count drops back
down to the metadata count.

> ⚠️ **`older_than => now()` is a teaching shortcut — never use it in production.**
> `remove_orphan_files` has a built-in **3-day age guard** by default, precisely so it can't delete
> files from an **in-flight write** that hasn't committed yet. Passing `older_than => now()`
> disables that guard and removes our freshly-created orphans immediately — fine in a lab where we
> know nothing else is writing, **dangerous** on a live table where a concurrent write's
> not-yet-committed files would be deleted out from under it. In prod, leave the default (or set a
> conservative threshold well past your longest write).

## 6. Prove it

The headline number is the **on-disk Parquet file count, before vs after**:

| | Before write | After orphan write (broken) | After GC (fixed) |
|---|---|---|---|
| **On-disk files** | = metadata | **> metadata** (orphans) | **= metadata** again |
| **Metadata `data_files`** | n | n (unchanged) | n (unchanged) |
| **Row count** | r | r (orphans invisible) | r |

After `remove_orphan_files`, on-disk count **== metadata `data_files`** — every physical file is
once again referenced. That equality is the proof the orphans are gone.

## 7. Takeaways & "in real production…"

- **Orphans ≠ expired snapshots.** Snapshot expiry reclaims *de-referenced* files; orphans were
  *never* referenced. You need **both** maintenance jobs.
- **Schedule `remove_orphan_files` periodically** (e.g. weekly) with a **safe age threshold** — the
  default 3-day guard, or longer than your longest-running write — **not** `now()`.
- **Tune `gc.*` table properties** for retention policy, and prefer committing through the catalog
  so writes are atomic and never leave dangling files in the first place.
- **Detect at scale:** alert when a table's storage size diverges from its referenced-file size /
  row count; periodically diff directory listings against metadata.
