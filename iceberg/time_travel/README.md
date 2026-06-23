# LAK-9 — Time travel & rollback

> **Break → Detect → Fix → Prove.** A bad write — an accidental `INSERT OVERWRITE`, a wrong
> `MERGE`, a fat-fingered `UPDATE` — silently corrupts the *current* table. Because every Iceberg
> write is a **snapshot**, the good data isn't gone. Recovery is a **metadata operation**: read the
> last-good snapshot with `VERSION AS OF` / `TIMESTAMP AS OF`, then `rollback_to_snapshot` to make
> it current again — no restore-from-backup, no pipeline re-run.

- **Notebook:** [`lak9_time_travel.ipynb`](./lak9_time_travel.ipynb)
- **Toolkit used:** `common.iceberg_meta` (`table_health` / `compare_health` — the "Prove it"), `common.spark_session`
- **Run against:** the unified Spark server (`make up`) — Spark UI at http://localhost:4040.
- **Time:** ~10 min. **Laptop-safe:** one tiny `orders` table + a throwaway clone, a handful of
  small writes, all under `.tmp/`; the notebook drops both tables at the end (`make clean` clears
  the rest).

---

## 1. The scenario

A nightly job loads orders into an Iceberg table. One night a code change drops a partition filter,
so a job that should have replaced *today's* rows instead runs an unqualified `INSERT OVERWRITE`
against the **whole** table — with the wrong (or empty) source. The 150 good rows are atomically
replaced by 5 garbage rows.

Nothing errors. Iceberg's overwrite is atomic and instant, so the job "succeeds" and the pipeline
goes green. The damage only surfaces when a downstream query returns almost no rows. In a
file-based world (plain Parquet) you'd be reaching for last night's backup. In a lakehouse the good
data is still there — pinned by the snapshot taken just before the bad write — and recovery is a
one-line metadata commit.

## 2. Break it

The notebook builds the correct state in two commits, then corrupts it in a third:

```python
good_a.writeTo(T).using("iceberg").create()   # snapshot A — 100 good rows
good_b.writeTo(T).append()                     # snapshot B — +50 good rows  (last known good)
...
spark.sql(f"INSERT OVERWRITE {T} SELECT * FROM bad")   # snapshot C — 5 corrupt rows (the accident)
```

We record **B's `snapshot_id`** and its **`committed_at`** timestamp *before* breaking anything —
that's the state we'll recover to.

> Why this is laptop-safe: every write touches at most a few rows, so the table stays tiny. The
> point isn't volume — it's that an atomic overwrite replaces the whole table in one commit, which
> is exactly the production accident we want to reverse.

## 3. Detect it — read the snapshot log

This is a **metadata** recovery, so the evidence is in Iceberg's metadata tables, not the Spark UI:

```sql
SELECT committed_at, snapshot_id, operation
FROM iceberg_catalog.default.lak9_t.snapshots ORDER BY committed_at;
```

| Signal | What you see |
|--------|--------------|
| **`.snapshots`** | three rows — A (create), B (`append`), C (`overwrite`): a full audit trail of what happened when |
| **`COUNT(*)` current** | 5 (the regression) |
| **`COUNT(*) ... VERSION AS OF <B>`** | 150 — the good data is intact; only the *current pointer* is wrong |

Time travel lets you *read* the good state to confirm before acting — by snapshot id
(`VERSION AS OF <id>`) or by wall-clock time (`TIMESTAMP AS OF '<committed_at>'`); both resolve to
the same B. The companion entry is in [`docs/troubleshooting.md`](../../docs/troubleshooting.md).

## 4. Diagnose

`INSERT OVERWRITE` (and `MERGE` / `UPDATE`) is **copy-on-write**: it writes new data files and
commits a new snapshot whose *current* pointer no longer references the old files — but the old
files (and snapshot B that points at them) are still there. The table isn't "broken"; it's pointing
at the wrong snapshot. So the fix isn't to rewrite data — it's to **move the pointer back**.

## 5. Fix it — `rollback_to_snapshot`

Reset the current snapshot to B:

```python
spark.sql("""
  CALL iceberg_catalog.system.rollback_to_snapshot(
    table => 'default.lak9_t',
    snapshot_id => <B>
  )
""")
```

(The `table` arg is `namespace.table` — **no** catalog prefix.) Key properties:

- **It's a metadata-only commit** — no data is rewritten, so it's instant regardless of table size.
- **It creates a *new* snapshot** (call it D) whose data is identical to B's. History is preserved
  and **auditable** — you can see a rollback happened, and even roll the rollback back.
- **Ancestor rule:** `rollback_to_snapshot` requires the target to be an *ancestor* of the current
  snapshot (B is an ancestor of C, so it works). To jump to a *non-ancestor* snapshot (forward
  again, or a sibling branch) use **`set_current_snapshot(snapshot_id => ...)`** instead.

## 6. Prove it

Two-part proof in the notebook:

1. **Row-level diff** of *current-after-rollback* vs *`VERSION AS OF B`* is **0** — byte-for-byte
   identical, and the `corrupt` rows are gone (`COUNT(*)` back to 150).
2. **`compare_health`** + the snapshot log show the rollback **added** a snapshot rather than
   deleting C — the incident stays in the audit trail.

| Check | Broken (snapshot C) | After rollback (snapshot D) |
|-------|--------------------:|----------------------------:|
| Live row count | 5 | **150** |
| `corrupt` rows | 5 | **0** |
| Diff vs snapshot B | (n/a) | **0 (identical)** |
| Snapshots in history | 3 | **4** (rollback adds one; nothing destroyed) |

## 7. Gotcha — expiry can make a snapshot unrecoverable (ties to LAK-3)

Rollback and time-travel only work while the target snapshot's **data files still exist**.
`expire_snapshots` ([LAK-3](../snapshots/README.md)) deletes old snapshots *and the data files only
they referenced*. If a nightly `expire_snapshots(older_than => now())` had run between the good load
and your recovery, **B's files would be gone, and rollback / `VERSION AS OF B` would FAIL**.

The notebook demonstrates this safely on a **throwaway clone** (`lak9_gotcha`): build a 2-snapshot
table, `expire_snapshots(older_than => now(), retain_last => 1)` to drop the older snapshot, then
try to time-travel to it and catch the failure. The lesson: **retention is your recovery window.**
`history.expire.max-snapshot-age-ms` (and `min-snapshots-to-keep`) must be at least as long as the
window in which you'd realistically need to undo a bad write. Expire too aggressively and you delete
the very snapshot rollback depends on.

## 8. Takeaways & "in real production…"

- **Recovery is a metadata op.** Every write is a snapshot, so a bad overwrite/MERGE/UPDATE is
  reversible *instantly* with `rollback_to_snapshot` (ancestor) or `set_current_snapshot` (any
  snapshot) — no backup restore, no pipeline re-run, regardless of table size.
- **Diagnose with the snapshot log + time travel.** `<t>.snapshots` is the audit trail;
  `VERSION AS OF <id>` / `TIMESTAMP AS OF '<ts>'` let you read the good state to confirm before you
  roll back.
- **Rollback is auditable** — it adds a snapshot pointing at the old data rather than deleting
  history, so the incident *and* the fix stay in the log.
- **Size retention to your recovery needs (the LAK-3 link).** Expiry reclaims storage but shrinks
  your recovery window; balance storage/planning cost against how far back you must be able to undo.
- **In production:** alert on unexpected row-count drops / `overwrite` ops on critical tables; keep
  a retention window comfortably longer than your detection-to-recovery time; rehearse rollback so
  it's muscle memory during an incident.

## 9. Teardown

The notebook ends with a **Teardown** cell that drops `iceberg_catalog.default.lak9_t` (and the
`lak9_gotcha` clone). `make clean` removes everything under `.tmp/` for a fully fresh warehouse.
