# SPK-7 — Partition Pruning & Predicate Pushdown

> **Break → Detect → Fix → Prove.** A filter on the partition column *should* let Spark read only
> the matching partition and skip the rest. Wrap that column in a `CAST` or a UDF and the
> optimizer can no longer match the filter to the partitions — so it reads **every** partition
> (a full scan) to do work that one partition's worth of data could have done.

- **Notebook:** [`spk7_partition_pruning.ipynb`](./spk7_partition_pruning.ipynb)
- **Toolkit used:** `common.datagen` (`uniform_keys` — a small lazy frame to write out), `common.profiles` (default `tuned`), `common.metrics_diff` (input-rows / runtime before vs after). The headline evidence here is the **physical plan** (`df.explain()`), not a UI percentile.
- **Run against:** the unified Spark server (`make up`) — open the Spark UI at http://localhost:4040 while the notebook runs.
- **Time:** ~10 min. **Laptop-safe:** this module is the rare one that **writes a small table** — a few hundred thousand rows across ~12 partitions of plain Parquet under `.tmp/spk7_orders/`. It is tiny, and the **Teardown** step (plus `make clean`) removes it.

> **This is a query-planning module, not a memory one.** Pruning is decided by Catalyst at plan
> time from the *shape of your predicate*, so it reproduces fine on the default **tuned** box —
> you do **not** need `make up-constrained`.

---

## 1. The scenario

An `orders` table is partitioned on disk by day (`dt`, an integer 0–11 standing in for a date
bucket). A dashboard query only ever wants **one day** at a time, so reading one day should touch
**1/12** of the files. It used to be fast.

Then someone "tidied up" the query and wrote the day filter as `WHERE CAST(dt AS STRING) = '5'`
(the dashboard passes the day as a string). Overnight the query got ~12× slower and the storage
team noticed it now scans the whole table every run. The data didn't grow. What changed?

Wrapping the partition column in a function (a `CAST` here, but a UDF does the same) means
Catalyst no longer sees a clean `dt = <literal>` predicate it can match against the directory
layout. It can't prove which partitions are irrelevant, so — to stay correct — it reads **all**
of them and filters afterward. The pruning silently turned off; the query still returns the right
answer, just by doing 12× the I/O.

## 2. Break it

We generate a small fact with `common.datagen.uniform_keys`, add a partition column
`dt = pmod(row_id, 12)`, and write it as **Parquet partitioned by `dt`** under `.tmp/` (a real,
on-disk partitioned table — the simplest reliable way to make pruning observable over Spark
Connect). Then we read it back and filter the *broken* way:

```python
broken = orders.where(F.cast("string", F.col("dt")) == "5")   # CAST on the partition col
```

The `CAST(dt AS STRING)` defeats partition-filter matching, so Spark scans all 12 partitions.

> Why this is laptop-safe: the table is a few hundred thousand rows of two columns; the whole
> thing is a handful of MB under `.tmp/spk7_orders/`. We only `count()` the result, so the driver
> never collects a large frame. The default **tuned** box is fine.

## 3. Detect it — read the plan (and the Spark UI)

The headline detector for pruning is the **physical plan**, not a UI percentile. Call
`broken.explain()` and read the Parquet scan node (see
[`docs/spark-ui-guide.md`](../../docs/spark-ui-guide.md) → **SQL / DataFrame** → scan node):

| Signal in the scan node | Broken (`CAST(dt …)`) | Fixed (`dt = 5`) |
|--------------------------|------------------------|-------------------|
| **`PartitionFilters`** | `[]` (empty) — no pruning | `[isnotnull(dt), (dt = 5)]` — pruned |
| **`PushedFilters`** | the cast can't push to Parquet | data-col predicates push down |
| Partitions / files read | **all 12** (full scan) | **1** (≈ 1/12 the data) |

The same thing is visible live at http://localhost:4040 → **SQL / DataFrame** → click the query →
the **Scan parquet** node: compare its **"number of output rows"** / files read against the table
total. Broken ≈ table total (full scan); fixed ≈ 1/12. `common.metrics_diff.measure()` captures
this quantitatively as the scan's **input rows** and the wall-clock runtime — the numbers this
module drives down.

## 4. Diagnose

Partition pruning is a **plan-time** optimization: Catalyst matches predicates of the form
`<partition_col> <op> <literal>` against the partition directory layout and discards directories
that can't match, so those files are never opened. Wrapping the partition column in **any**
function — `CAST`, `substr`, `date_trunc`, a Python UDF — produces an expression Catalyst cannot
invert against the partition values, so it conservatively keeps **every** partition and applies
the predicate as an ordinary post-scan filter. The query is still *correct*; it just reads
everything. The identical trap hits **predicate pushdown to Parquet** on data columns: only
simple, pushdown-friendly comparisons reach the file reader's row-group filtering — a UDF or a
non-trivial expression stays in Spark and forfeits the skip.

## 5. Fix it

| Fix | How | Why it works |
|-----|-----|--------------|
| **Filter the partition column directly** | `WHERE dt = 5` — compare the *raw* column to a **matching-typed literal** (int literal for the int partition col) | Catalyst sees a clean `dt = 5` predicate, populates `PartitionFilters`, and opens only that one directory. |
| **Cast the literal, not the column** | if the input is a string, do `WHERE dt = CAST('5' AS INT)` (cast the *constant* side) | The constant folds at plan time; the partition column stays bare, so pruning still matches. **Never** wrap the column. |
| **Push-down-friendly predicates on data columns** | prefer simple `col <op> literal` / `IN` / `BETWEEN`; avoid UDFs and functions on the filtered column | Simple comparisons push into Parquet row-group filtering (`PushedFilters`); UDFs do not. |

The fix is almost embarrassingly small — drop the `CAST` from the column — but it changes the scan
from 12 directories to 1.

## 6. Prove it

`common.metrics_diff.compare([...])` prints a before/after table. Expected shape:

| Metric | broken (`CAST(dt) = '5'`) | fixed (`dt = 5`) |
|--------|--------------------------:|-----------------:|
| Wall-clock runtime | higher (full scan) | ↓ |
| Input rows scanned (`count()`) | **all ~N rows** | **≈ N/12** |
| `PartitionFilters` (from `explain()`) | `[]` | `[dt = 5]` |

Reading ≈ 1/12 of the rows for the same answer — and `PartitionFilters` going from empty to
populated — is the proof the fix worked.

## 7. Takeaways & "in real production…"

- **Detect** a pruning failure in the **plan**, not by feel: an empty `PartitionFilters` (or a scan
  whose output rows ≈ the table total) where you expected a single partition.
- **Never apply a function to the column you're pruning/filtering on.** Cast or transform the
  **literal** side instead, or store the partition column in the type you'll query it with.
- The same rule governs **predicate pushdown** to Parquet/ORC data columns: simple comparisons
  push down (`PushedFilters`), UDFs and wrapped columns do not.
- **In production:** make `explain()` (or the SQL tab's scan node) part of code review for any hot
  partitioned query; alert on jobs whose scanned-bytes ≈ full-table size despite a partition
  filter; keep partition columns in a query-friendly type so no cast is ever tempting.

## 8. Teardown

This module **wrote a small table**: `.tmp/spk7_orders/` (a few hundred thousand rows of Parquet
partitioned by `dt`). The final notebook cell deletes that directory and restores the `tuned`
profile. If anything is left behind, `make clean` removes everything under `.tmp/`.
