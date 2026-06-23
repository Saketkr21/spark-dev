# LAK-2 — Small files & compaction

> **Break → Detect → Fix → Prove.** The classic lakehouse maintenance failure: frequent small
> writes (streaming / micro-batches) litter a table with hundreds of tiny data files. Query
> planning and scan I/O degrade even though the row count is trivial. The fix is **compaction**.

- **Notebook:** [`lak2_small_files.ipynb`](./lak2_small_files.ipynb)
- **Toolkit used:** `common.iceberg_meta` (`table_health` / `compare_health` — the "Prove it"), `common.spark_session`
- **Run against:** the unified Spark server (`make up`) — Spark UI at http://localhost:4040.
- **Time:** ~10 min. **Laptop-safe:** a few thousand rows per write across ~15 appends, all under
  `.tmp/`; the notebook drops the table at the end (`make clean` clears the rest).

---

## 1. The scenario

A streaming job upserts events into an Iceberg table every few seconds. Each micro-batch is tiny
— a few thousand rows — but it's a **separate commit**, and every commit writes **at least one
data file per partition** (often several, at the default write parallelism). After a day of this
the table holds a few thousand rows spread across **hundreds of tiny files**.

Nothing looks broken: counts are correct, time travel works. But queries get steadily slower.
The cost isn't the data volume — it's the **file count**. The query planner must open and read
the footer of every file, and the scan pays per-file open overhead instead of streaming through a
few large files. This is the **small-files problem**, and it's the single most common reason a
healthy-looking lakehouse table slows to a crawl.

## 2. Break it

We create `iceberg_catalog.default.lak2_events` and append ~15 tiny batches (you can also force it
in one shot with `.repartition(200)`). Each append is its own snapshot and emits its own files:

```python
batch.writeTo(T).append()          # one commit → ≥1 file per partition
```

After the loop, `common.iceberg_meta.table_health(spark, T, "before")` reports a **high
`data_files` count** and a **tiny `avg_file_bytes`** — the fingerprint of the pathology.

> Why this is laptop-safe: the data is intentionally small (a few thousand rows total). The point
> isn't volume — it's **fragmentation**. A handful of small appends is enough to scatter the rows
> across many files, which is exactly what we want to observe and then fix.

## 3. Detect it — read the table metadata

This is a **metadata** pathology, not a memory/task one — so the tell is in Iceberg's own
metadata tables rather than the Spark UI Stages tab:

```sql
SELECT COUNT(*) AS files, AVG(file_size_in_bytes) AS avg_bytes
FROM iceberg_catalog.default.lak2_events.files;
```

| Signal | Fragmented (broken) | Healthy (after compaction) |
|--------|---------------------|----------------------------|
| **`.files` row count** | high (≈ one+ per write per partition) | a handful |
| **avg `file_size_in_bytes`** | tiny (KB) | large (toward the target file size) |
| **`.snapshots` count** | one per append | unchanged by compaction (data rewrite ≠ snapshot expiry) |

In the Spark UI the same problem shows up at query time as a **scan node reading hundreds of
files** (see [`docs/spark-ui-guide.md`](../../docs/spark-ui-guide.md) on reading the scan node);
mostly, though, you diagnose this from metadata. The companion entry is in
[`docs/troubleshooting.md`](../../docs/troubleshooting.md) (LAK-2 row).

## 4. Diagnose

Every commit is atomic and writes **at least one file per partition** — by design, so writers
never coordinate. Many small commits therefore mean many small files. Each file carries footer
metadata (schema, column stats) the planner must read, and each scan pays a fixed open cost per
file. Hundreds of tiny files = planning overhead + poor scan efficiency, regardless of how few
rows they hold. More CPU doesn't help; the cost is per-file, not per-row.

## 5. Fix it — bin-pack compaction

Iceberg's `rewrite_data_files` procedure **bin-packs** many small files into fewer large ones in a
single new snapshot (the old files stay until snapshots expire — that's LAK-3):

```python
spark.sql("""
  CALL iceberg_catalog.system.rewrite_data_files(
    table => 'default.lak2_events',
    options => map('min-input-files','2')
  )
""")
```

(The `table` arg is `namespace.table` — **no** catalog prefix. `min-input-files=2` lets it compact
even our tiny groups; in production you'd lean on the size-based defaults.)

**Prevent it** going forward:

- Set a **target file size** — `ALTER TABLE ... SET TBLPROPERTIES ('write.target-file-size-bytes'='134217728')` (128 MB) — so writers aim for fewer, larger files.
- **Write less often / larger batches** — fewer, bigger commits beat many tiny ones (this is the
  streaming knob in **STR-3**: `maxOffsetsPerTrigger` / longer trigger intervals).
- **Schedule compaction** as routine maintenance (nightly `rewrite_data_files`).

The Delta equivalent is `OPTIMIZE <table>` (optionally `ZORDER BY`).

## 6. Prove it

`common.iceberg_meta.compare_health([before, after])` prints the before/after table. Expected
shape:

| Metric | before | after |
|--------|-------:|------:|
| Data files | **high (e.g. 30–60)** | **few (e.g. 1–4)** |
| Avg file size | tiny (KB) | **much larger** |
| Snapshots | N | N + 1 (compaction adds one; it does **not** remove the old snapshots) |

`data_files` collapsing while **avg file size rises** is the proof the compaction worked. Note
snapshots/manifests don't shrink here — reclaiming the old files and metadata is LAK-3 / LAK-5.

## 7. Takeaways & "in real production…"

- **Detect** small files from table **metadata** (`.files` count + avg size), not from row counts —
  a tiny table can still be badly fragmented.
- **Compact on a schedule** (`rewrite_data_files` / Delta `OPTIMIZE`) and **set a target file size**
  so writers don't re-fragment between runs.
- **Prefer fewer, larger writes.** Most small-file problems are born upstream in a streaming job
  writing every trigger — fix the batch sizing first (**STR-3**), compact as a safety net.
- **Compaction is not cleanup.** It rewrites data into new files but leaves the old snapshots
  (and their files) in place; pair it with **`expire_snapshots`** (LAK-3) and
  **`remove_orphan_files`** (LAK-4) to actually reclaim storage.

## 8. Teardown

The notebook ends with a **Teardown** cell that drops `iceberg_catalog.default.lak2_events`.
`make clean` removes everything under `.tmp/` if you want a fully fresh warehouse.
