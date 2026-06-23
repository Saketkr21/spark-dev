# LAK-7 — Partitioning, hidden partitioning & evolution

> **Break → Detect → Fix → Prove.** Iceberg's two best-kept partitioning features: **hidden
> partitioning** (the engine prunes partitions from a predicate on the *raw* column — no extra
> partition column, no `CAST` in your `WHERE`) and **partition evolution** (change the partition
> layout as a metadata-only operation that touches only *new* data — no rewrite of old files).

- **Notebook:** [`lak7_partitioning.ipynb`](./lak7_partitioning.ipynb)
- **Toolkit used:** `common.iceberg_meta` (`table_health` — the "Prove it"), `common.spark_session`
- **Run against:** the unified Spark server (`make up`) — Spark UI at http://localhost:4040.
- **Time:** ~10 min. **Laptop-safe:** a few dozen rows spanning a handful of days, all under
  `.tmp/`; the notebook drops the table at the end (`make clean` clears the rest).

---

## 1. The scenario

An `events` table is queried almost exclusively by **day** (`WHERE ts >= '…' AND ts < '…'`).
On Hive-style partitioning you'd have to (a) add an explicit `event_date` column, (b) keep it in
sync with `ts` on every write, and (c) remember to filter on `event_date` — *not* `ts` — or the
engine scans the whole table. Worse, the moment a query does `WHERE date(ts) = …` or
`WHERE CAST(ts AS DATE) = …`, pruning silently breaks because the partition column and the
predicate column don't match.

Then the access pattern changes: now you also look up a **single customer's** events. A
day-partition doesn't help that query at all — you want to *add* a `bucket(customer_id)` layer.
On Hive that means a full table rewrite under a new partitioning scheme. The history is huge;
nobody wants to rewrite it. So partitioning is treated as a one-time, irreversible decision —
and that's exactly the trap Iceberg removes.

## 2. Break it (the contrast) — what Hive-style partitioning forces

We don't literally build a broken Hive table; we name the two pain points Iceberg fixes, then
show Iceberg *not* having them:

- **Predicate coupling:** Hive prunes only when you filter on the **partition column** itself.
  A function on the column (`date(ts)`, `CAST(ts AS DATE)`) defeats pruning (this is the
  partition-pruning failure of **SPK-7**). You also have to materialize and maintain that derived
  column.
- **Frozen layout:** changing the partition scheme rewrites the whole table.

In the notebook we create the Iceberg table **partitioned by `days(ts)`** and load rows across
several days:

```sql
CREATE TABLE iceberg_catalog.default.lak7_events (
  event_id BIGINT, customer_id BIGINT, amount DOUBLE, ts TIMESTAMP
) USING iceberg
PARTITIONED BY (days(ts));
```

`days(ts)` is a **partition transform**: Iceberg derives the partition value from `ts` and stores
that mapping in the table metadata. There is **no** `event_date` column in the schema.

## 3. Detect it / Diagnose — hidden pruning, read from the plan & metadata

This is a **planning** behaviour, not a memory/task one, so the tell is in the **query plan** and
Iceberg's **metadata tables**, not the Spark UI Stages tab.

**(a) Hidden partitioning prunes on the raw column.** Query `WHERE ts >= '…' AND ts < '…'` —
*no* `event_date`, *no* `CAST` — and inspect the plan:

```python
df = spark.sql("SELECT * FROM …lak7_events WHERE ts >= '…' AND ts < '…'")
df.explain()                      # look for the pushed partition/data filter on ts
```

| Signal | Full scan (Hive antipattern) | Hidden-partition pruned (Iceberg) |
|--------|------------------------------|-----------------------------------|
| **`WHERE` you must write** | on a separate `event_date` column | on the **raw `ts`** column |
| **Plan** (`df.explain()`) | filter applied after a full scan | **pushed filter** on `ts`; partitions skipped at planning |
| **Files / rows scanned** | all files / all rows | only the matching day's files / rows |
| **`CAST`/UDF in filter** | breaks pruning (SPK-7) | not needed — that's the whole point |

**(b) The partition layout is visible in metadata.** `…lak7_events.partitions` shows one row per
partition (the `days(ts)` value) with its `file_count` / `record_count` — proof the data is
physically grouped by day.

## 4. Fix it (evolve) — change partitioning without rewriting history

The access pattern now includes per-customer lookups. **Add** a partition field — a metadata-only
operation:

```sql
ALTER TABLE iceberg_catalog.default.lak7_events
  ADD PARTITION FIELD bucket(8, customer_id);
```

Insert more rows. Now inspect `…lak7_events.files`:

```sql
SELECT partition, spec_id, file_count FROM …lak7_events.files ORDER BY spec_id;
```

You'll see **two `spec_id` values**: the **old** files keep `spec_id = 0` (`days(ts)` only), the
**new** files use `spec_id = 1` (`days(ts)` + `bucket(8, customer_id)`). Evolution affected only
data written **after** the change — **no old file was rewritten**. The partition spec is
**versioned per file**.

- Available transforms: `years` / `months` / `days` / `hours` (time), `bucket(N, col)` (hash into
  N buckets — great for high-cardinality ids), `truncate(L, col)` (prefix — e.g. first `L` chars /
  numeric width), and `identity`. Pick the transform that matches how you filter.
- **Drop** a field with `ALTER TABLE … DROP PARTITION FIELD <transform>` (again metadata-only).
- **Want the old data in the new layout?** That's the only case that rewrites files — run
  `rewrite_data_files` (the LAK-2 compaction procedure) to re-bin the old `spec_id = 0` files under
  the current spec. It's *optional*: queries work across mixed specs without it.

## 5. Prove it

Two proofs, both in the notebook:

1. **Pruning** — the `df.explain()` plan shows a pushed filter on the raw `ts`, and a
   day-scoped query reports **far fewer input files / rows** than a full-table scan (printed
   side by side). Hidden partitioning works without touching the predicate.
2. **Evolution is per-file, metadata-only** — `…lak7_events.files` shows the **mixed `spec_id`**
   (old files `0`, new files `1`) and `table_health` shows the **data-file count rising only by
   the newly written files** — confirming zero rewrite of the old data.

## 6. Takeaways & "in real production…"

- **Choose transforms to match query patterns.** Filter by time → `days`/`hours`(`ts`); look up by
  a high-cardinality id → `bucket(N, id)`; range/prefix scans → `truncate`. The transform is the
  partitioning decision.
- **Hidden partitioning means you never add CAST/derived columns** to your queries (or your
  schema). Filter on the natural column; the engine prunes. This is the cure for the
  pruning-defeating `CAST`/UDF filter in **SPK-7**.
- **Partition evolution is metadata-only and per-file.** Change the layout as the workload
  changes — old data keeps its old spec and is read correctly; only new data uses the new spec.
  Rewriting old data to the new spec (`rewrite_data_files`) is optional, do it when the old
  partitions actually hurt a query.
- **Relate it forward:** pruning is the lakehouse-format twin of **SPK-7** (predicate pushdown /
  partition pruning); per-partition write cost is **LAK-8** (a 1-row MERGE rewriting a whole
  copy-on-write partition — partition *granularity* is what you're choosing here).
- **In production:** keep partitions reasonably sized (avoid thousands of tiny partitions *and*
  giant ones); prefer `bucket` over raw high-cardinality columns; evolve deliberately and document
  the spec change; schedule `rewrite_data_files` if old partitions need to adopt the new layout.

## 7. Teardown

The notebook ends with a **Teardown** cell that drops `iceberg_catalog.default.lak7_events`.
`make clean` removes everything under `.tmp/` if you want a fully fresh warehouse.
