# LAK-5 — Manifest explosion & rewrite

> **Break → Detect → Fix → Prove.** The metadata-layer cousin of LAK-2: many small commits don't
> just scatter *data* files — they also scatter *manifest* files. A table with thousands of tiny
> manifests is slow to **plan** (the planner reads manifests to prune) even when the data itself is
> perfectly healthy. The fix is **`rewrite_manifests`** — coalesce many small manifests into a few.

- **Notebook:** [`lak5_manifests.ipynb`](./lak5_manifests.ipynb)
- **Toolkit used:** `common.iceberg_meta` (`table_health` / `compare_health` — the **manifests** field is the headline), `common.metrics_diff` (optional planning-time before/after), `common.spark_session`
- **Run against:** the unified Spark server (`make up`) — Spark UI at http://localhost:4040.
- **Time:** ~10 min. **Laptop-safe:** a few rows per write across ~25 appends, all under `.tmp/`;
  the notebook drops the table at the end (`make clean` clears the rest).

---

## 1. The scenario

A pipeline appends to an Iceberg table on a tight cadence — every few seconds a micro-batch lands.
Each append is its own **commit**, and every commit writes a new **manifest file** (a metadata
file that lists data files plus their partition/column stats) and a new **manifest list** for the
snapshot. After a day of this the table holds a trivial number of rows but **thousands of tiny
manifests**.

Counts are correct, time travel works, the *data* files may even be fine. But every query gets
slower — and the slowness is in **query planning**, before a single data byte is read. To prune,
Iceberg opens and reads **every manifest** in the current snapshot. Planning cost grows with the
**manifest count**, independent of how much data the table holds. This is the **manifest
explosion** problem, and it's distinct from (though often co-occurring with) the small-*data*-files
problem of LAK-2.

## 2. Break it

We create `iceberg_catalog.default.lak5_t` and append ~25 tiny batches in a loop. Each append is a
separate snapshot and writes (roughly) one new manifest:

```python
batch.writeTo(T).append()          # one commit → ~one new manifest
```

After the loop, `common.iceberg_meta.table_health(spark, T, "before")` reports a **high
`manifests` count** — one (or a few) per append. Listing `<t>.manifests` shows many entries, each
tiny and referencing only a file or two.

> Why this is laptop-safe: the data is intentionally minuscule (a few rows per append). The point
> isn't volume — it's **metadata fragmentation**. ~25 appends is enough to accumulate ~25 manifests
> and make the planning-cost-grows-with-manifest-count story concrete, while the table stays a few
> KB on disk.

## 3. Detect it — read the table metadata

This is a **metadata** pathology, so the tell is in Iceberg's own metadata tables, not the Spark UI
Stages tab:

```sql
SELECT COUNT(*) FROM iceberg_catalog.default.lak5_t.manifests;   -- how many manifests planning must read
SELECT COUNT(*) FROM iceberg_catalog.default.lak5_t.snapshots;   -- one per append (≈ manifest count)
```

| Signal | Exploded (broken) | Healthy (after rewrite) |
|--------|-------------------|-------------------------|
| **`.manifests` row count** | high (≈ one+ per commit) | a handful |
| **per-manifest content** | each lists 1–2 data files | few manifests, each listing many files |
| **`.snapshots` count** | one per append | unchanged by rewrite (rewriting manifests ≠ expiring snapshots) |
| **planning time** | grows with manifest count | flat / fast |

`common.iceberg_meta.table_health` surfaces the count directly as its **`manifests`** field — the
number this module drives down. The companion entry is in
[`docs/troubleshooting.md`](../../docs/troubleshooting.md) (LAK-5 row).

## 4. Diagnose

Every commit is atomic and self-contained: a writer never rewrites a previous commit's metadata, so
each append adds a **new manifest** (and a new manifest list for its snapshot). Many small commits
therefore mean many small manifests. At plan time Iceberg reads the current snapshot's manifest
list, then opens **each manifest** to evaluate partition/column stats and prune files. That cost is
**per-manifest**, so it scales with the manifest count — not with the row count or data size. A
table with 10 rows and 3,000 manifests plans slowly; more CPU won't help, because the work is
fixed metadata I/O.

## 5. Fix it — rewrite (coalesce) the manifests

Iceberg's `rewrite_manifests` procedure rewrites many small manifests into a few larger ones in a
single new snapshot (it rewrites only **metadata** — the data files are untouched):

```python
spark.sql("CALL iceberg_catalog.system.rewrite_manifests(table => 'default.lak5_t')")
# returns: rewritten_manifests_count / added_manifests_count
```

(The `table` arg is `namespace.table` — **no** catalog prefix. The result row tells you how many
manifests were rewritten and how many were written in their place — the second number is much
smaller.)

**Prevent it** going forward:

- **Keep manifest merging on** — `commit.manifest-merge.enabled` (default `true`) lets a commit
  merge new manifests into recent small ones instead of always adding a fresh one.
- **Tune the manifest target size** — `commit.manifest.target-size-bytes` (default ~8 MB) and
  `commit.manifest.min-count-to-merge` control when small manifests get merged on write.
- **Schedule `rewrite_manifests`** as routine maintenance (alongside `rewrite_data_files`).

## 6. Prove it

`common.iceberg_meta.compare_health([before, after])` prints the before/after table. Expected
shape:

| Metric | before | after |
|--------|-------:|------:|
| Data files | unchanged | unchanged (rewrite touches metadata, not data) |
| **Manifests** | **high (e.g. 25+)** | **few (e.g. 1–2)** |
| Snapshots | N | N + 1 (the rewrite adds one; it does **not** remove the old snapshots) |

**Manifests** collapsing while **data files stay flat** is the proof: this is a pure-metadata fix.
Optionally, time a metadata query (`SELECT COUNT(*) FROM <t>`) with `common.metrics_diff.measure`
before and after to show planning getting cheaper.

## 7. Takeaways & "in real production…"

- **Detect** manifest explosion from table **metadata** (`.manifests` count), not row counts — a
  tiny table can plan slowly if it has thousands of manifests.
- **Keep `commit.manifest-merge.enabled` on** and set a sensible `commit.manifest.target-size-bytes`
  so writers coalesce manifests as they go, instead of accumulating thousands of tiny ones.
- **Schedule `rewrite_manifests`** as routine maintenance — it's cheap (metadata only) and keeps
  planning fast.
- **LAK-2 vs LAK-5 — two halves of metadata upkeep.** LAK-2 (`rewrite_data_files`) fixes too many
  small *data* files (slow scans); LAK-5 (`rewrite_manifests`) fixes too many small *manifests*
  (slow planning). Many-small-commits drives **both**, so production tables need both on a schedule —
  plus `expire_snapshots` (LAK-3) and `remove_orphan_files` (LAK-4) to reclaim what they leave behind.

## 8. Teardown

The notebook ends with a **Teardown** cell that drops `iceberg_catalog.default.lak5_t`.
`make clean` removes everything under `.tmp/` if you want a fully fresh warehouse.
