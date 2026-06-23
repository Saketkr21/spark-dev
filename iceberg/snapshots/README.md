# LAK-3 — Snapshot growth & expiration

> **Break → Detect → Fix → Prove.** Every write to an Iceberg table — `append`, `overwrite`,
> `MERGE` — creates a **new snapshot**. Snapshots power time-travel and rollback, but Iceberg keeps
> them *forever* unless you expire them. On a busy table the snapshot history (and the metadata-file
> log) grows **unbounded**: metadata bloats, planning slows, and old snapshots **pin old data files**
> so storage never shrinks.

- **Notebook:** [`lak3_snapshots.ipynb`](./lak3_snapshots.ipynb)
- **Toolkit used:** [`common.iceberg_meta`](../../common/iceberg_meta.py) (`table_health` / `compare_health` — the **snapshots** field is the headline metric)
- **Run against:** the unified Spark server (`make up`) — Spark UI at http://localhost:4040.
- **Time:** ~10 min. **Laptop-safe:** one tiny table, ~20 small writes, all under `.tmp/`; nothing to
  download, nothing heavy. The notebook drops the table at the end.

---

## 1. The scenario

A streaming/CDC sink continuously upserts into an Iceberg `orders` table — a steady trickle of
small `append`s plus the occasional `MERGE` (a correction) and an `overwrite` (a reclassification).
The live table stays small (a few hundred rows), yet over weeks **planning gets slower, commits get
heavier, and storage keeps growing** even though the row count barely moves. Why?

Every commit writes a **snapshot** and a new `vN.metadata.json`. Nothing prunes them by default, so
the snapshot history and metadata log grow without bound — and each retained snapshot keeps alive the
data files it referenced, so the bytes that `overwrite`/`MERGE` superseded are never reclaimed.

## 2. Break it

The notebook creates `iceberg_catalog.default.lak3_orders` and does **~20 writes** (15 appends + 3
MERGEs + 1 overwrite). The row count stays tiny, but the snapshot count climbs to ~20.

> Why this is laptop-safe: each write touches at most a few rows, so the table never grows large.
> The pathology is **metadata** growth (snapshots + metadata files), not data volume — the "shrink
> the box" trick applied to the lakehouse: small data, real production failure mode.

## 3. Detect it

Two Iceberg metadata tables (both Connect-safe — plain `spark.sql`):

| Signal | Query | What you see (broken) |
|--------|-------|------------------------|
| **Snapshot count** (headline) | `SELECT COUNT(*) FROM <t>.snapshots` | ~20 — one per commit, climbing forever |
| **Metadata-file log** | `SELECT COUNT(*) FROM <t>.metadata_log_entries` | grows alongside: one `vN.metadata.json` per commit |
| **`table_health` → `snapshots`** | `compare_health([table_health(spark, t)])` | the **Snapshots** row ≈ 20 |

`SELECT ... operation FROM <t>.snapshots ORDER BY committed_at` shows the per-commit operation
(`append` / `overwrite` — MERGE commits as overwrite/delete+append), making the growth concrete.

## 4. Diagnose

Iceberg retains **every** snapshot so you can time-travel (`VERSION AS OF` / `TIMESTAMP AS OF`) or
roll back a bad write — that's the feature. But there is **no background expiry**; without explicit
maintenance they accumulate. Two costs:

1. **Metadata bloat & slower planning** — each commit appends a snapshot to the table metadata and
   writes a new metadata JSON; planning and commits get heavier.
2. **Storage that never shrinks** — an old snapshot **pins the data files it referenced**, so the
   files `overwrite`/`MERGE` superseded can't be deleted while any snapshot still points at them.

## 5. Fix it — `expire_snapshots`

```sql
CALL iceberg_catalog.system.expire_snapshots(
    table => 'default.lak3_orders',
    older_than => now(),     -- lift the default 5-day age guard so FRESH snapshots are eligible
    retain_last => 3         -- but always keep the newest 3 (a time-travel window)
);
```

**Gotcha (the one that bites at small scale):** `expire_snapshots` has a built-in age guard —
by default it will not expire snapshots younger than `history.expire.max-snapshot-age-ms`
(**5 days**). All our snapshots were just created, so a *bare* call expires **nothing**. To expire
fresh snapshots you **must** pass `older_than => now()` (lift the age guard) and/or `retain_last => N`.
The procedure returns counts of the data / manifest / metadata files it deleted.

**Make it automatic — table properties.** Calling the procedure by hand doesn't scale. Set retention
**policy** on the table so a scheduled maintenance job (or engine auto-maintenance) behaves predictably:

```sql
ALTER TABLE iceberg_catalog.default.lak3_orders SET TBLPROPERTIES (
    'history.expire.max-snapshot-age-ms'   = '604800000',  -- 7 days: how long snapshots live
    'history.expire.min-snapshots-to-keep' = '3'           -- floor: never drop below 3
);
```

These properties don't expire on their own (Iceberg has no background thread) — they're the policy a
**scheduled** `expire_snapshots` reads.

## 6. Prove it

`compare_health([before, after])` prints the before/after table. Expected shape:

| Metric | before expire | after expire |
|--------|--------------:|-------------:|
| Data files | many (CoW/MERGE leftovers) | ↓ |
| **Snapshots** | **~20** | **3** (`retain_last`) |

The **Snapshots** row collapsing to `retain_last` is the proof; data files drop too, because
expiring the old snapshots unpinned the files that `overwrite`/`MERGE` had superseded — and the table
stays fully queryable.

## 7. Takeaways & "in real production…"

- **Detect** snapshot bloat with `SELECT COUNT(*) FROM <t>.snapshots`; watch
  `<t>.metadata_log_entries` for metadata-file growth. Rising counts on a high-write table are the tell.
- **The age-guard gotcha:** `expire_snapshots` won't drop snapshots younger than the max-snapshot-age
  (5-day default) — pass `older_than => now()` and/or `retain_last => N` to expire fresh ones.
- **Expiry reclaims storage**, not just metadata: it deletes data files only the expired snapshots
  referenced. Truly *orphaned* files (failed/partial writes) are a separate job — **LAK-4**
  (`remove_orphan_files`).
- **In production:** **schedule** `expire_snapshots` (nightly/weekly) and set
  `history.expire.max-snapshot-age-ms` + `history.expire.min-snapshots-to-keep` so the policy is
  explicit. **Balance retention against the time-travel window you actually need** — expiring too
  aggressively destroys the snapshots that rollback/audit depend on (**LAK-9** time-travel & rollback).

## 8. Teardown

The notebook's last cell drops `lak3_orders`. `make clean` removes everything under `.tmp/`
(warehouses, metastore, event logs) if you want a fully fresh slate.
