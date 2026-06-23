# LAK-10 (Deep) — Iceberg internals

> **Deep dive — exploration, not Break → Fix.** Everything earlier in this track (small files,
> snapshots, manifests, MERGE) *uses* Iceberg's metadata; this module **opens it up** and shows
> the moving parts that make those behaviors possible. There is no pathology to break here — each
> step is a short demo of a real internal, and **the metadata-table (or on-disk) output _is_ the
> artifact**. This is the senior-engineer "how does it actually work underneath" tour.

- **Notebook:** [`lak10_internals.ipynb`](./lak10_internals.ipynb)
- **Toolkit used:** `common.spark_session` (Connect session + `display_df`); the rest is plain
  `spark.sql` against Iceberg **metadata tables** plus read-only `os` / `glob` on the warehouse dir.
- **Run against:** the unified Spark server (`make up`) — Spark UI at http://localhost:4040 (not
  central here; this module reads metadata, not Stages).
- **Time:** ~15 min. **Laptop-safe:** two tiny tables under
  `.tmp/local_iceberg_warehouse/default/`; the notebook drops them at the end (`make clean` clears
  the rest). Connect-safe: `spark.sql` + DataFrame + read-only `os`/`glob` only — no
  `sparkContext`/RDD.

---

## What you'll look at

Iceberg is a **table format**, i.e. a precise spec for a tree of metadata files that sits on top of
plain data files. A read or write is really a walk down that tree:

```
catalog pointer  ─►  vN.metadata.json   (current schema, partition specs, snapshot list, refs)
                          │
                          └─► manifest list (one per snapshot)   <t>.manifests + .../snap-*.avro
                                   │
                                   └─► manifest files (avro)      <t>.files / .all_data_files
                                            │
                                            └─► data files (parquet)  +  delete files (v2)
```

Almost every node in that tree is queryable as a SQL **metadata table** over Spark Connect, and the
files themselves are visible with `os`/`glob`. The six demos below each light up one layer.

## The six demos

| # | Internal | What you query / list | The "aha" |
|---|----------|------------------------|-----------|
| 1 | **Metadata pointer / version-hint** | `os.listdir` the `metadata/` dir; read `version-hint.text`; `<t>.metadata_log_entries` | The whole table is reached through **one tiny pointer**; every commit writes a new `vN.metadata.json` and bumps the hint. |
| 2 | **Manifest column stats & pruning** | `<t>.files` → `readable_metrics`, `lower_bounds` / `upper_bounds` | Iceberg stores **per-column min/max per file**, so a `WHERE` can skip whole files **without opening them**. |
| 3 | **Format v1 vs v2 (delete files)** | v2 table + `MERGE`/`DELETE`; `content` in `<t>.files` | v2 **merge-on-read** records a **delete file** (`content` = 1 position / 2 equality) instead of rewriting data; v1 has no delete files. |
| 4 | **Snapshots, refs & history** | `<t>.snapshots`, `<t>.history`, `<t>.refs`; `CREATE TAG` | History is a **DAG of snapshots**; **branches/tags** are named pointers into it (the basis for write-audit-publish & rollback). |
| 5 | **Catalog types** | `conf/spark-defaults.conf` (prose) | What the **catalog** does for you: Hadoop (filesystem, this repo) vs Hive vs REST/Nessie — atomic commits, multi-engine, branching. |
| 6 | **Manifest list & partition summaries** | `<t>.manifests` → `partition_summaries` | The manifest list keeps **partition value ranges per manifest**, so planning can skip an entire manifest before reading any file list. |

## How the metadata tree maps to a query

When you run `SELECT … WHERE order_date = '2026-01-01'`, Iceberg:

1. reads the catalog pointer → the current `vN.metadata.json` (demo 1),
2. picks the current snapshot's **manifest list** and uses each manifest's `partition_summaries`
   to drop manifests whose partition range can't match (demo 6),
3. reads the surviving manifests and uses each data file's `lower_bounds`/`upper_bounds` to drop
   files whose column range can't match (demo 2),
4. on a v2 table, applies any **delete files** to the data files it does read (demo 3).

That cascade — pointer → manifest list → manifest → data/delete file — is the entire read path, and
every level prunes. This module makes each level visible.

## Observable here vs. only in a real catalog

This repo uses the **Hadoop (filesystem) catalog**, which is perfect for *seeing* the file tree
(`version-hint.text`, `vN.metadata.json`, manifest lists) because everything is just files under
`.tmp/`. What it can **not** demonstrate:

- **Atomic multi-writer commits.** The Hadoop catalog swaps the pointer with a filesystem rename;
  on a real object store that isn't atomic, which is *why* production uses a real catalog.
- **Multi-engine concurrency.** Trino/Flink/Spark hitting the same table at once needs a shared
  catalog (Hive/REST/Nessie), not a directory.
- **Branch-based commits as a workflow.** `<t>.refs` shows the mechanism here, but Nessie/REST add
  Git-like branch/merge across many tables.

Demo 5 explains those tradeoffs from the actual config so you know what you'd reach for at scale.

## Pointers

- Spec / module map: [`docs/CURRICULUM_PLAN.md`](../docs/CURRICULUM_PLAN.md) (LAK-10 row + the Iceberg
  "Niche/Deep" inventory: manifest column stats, format v1 vs v2 delete files, partition-spec
  versioning, metadata pointer/version-hint, catalog implementations, branch-based commits).
- The table-health helper this track's other modules use: [`common/iceberg_meta.py`](../../common/iceberg_meta.py)
  (LAK-2/3/5 "Prove it") — LAK-10 reads the same metadata tables it does, just for *understanding*
  rather than measuring.
- Catalog config: [`conf/spark-defaults.conf`](../../conf/spark-defaults.conf)
  (`spark.sql.catalog.iceberg_catalog.type = hadoop`).

## Teardown

The notebook ends with a **Teardown** cell that drops both demo tables
(`iceberg_catalog.default.lak10_*`). `make clean` removes everything under `.tmp/` for a fully fresh
warehouse.
