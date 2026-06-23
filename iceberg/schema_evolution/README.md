# LAK-6 — Schema evolution

> **Break → Detect → Fix → Prove.** The source schema changes over time — a column is added,
> renamed, dropped, or widened. A good table format absorbs the change **in metadata, without
> rewriting history**. Plain Parquet has no table metadata, so the same change silently
> mismatches or breaks. This module shows the difference, column by column.

- **Notebook:** [`lak6_schema_evolution.ipynb`](./lak6_schema_evolution.ipynb)
- **Toolkit used:** `common.spark_session` (Connect session + `display_df`), `common.iceberg_meta`
  (`table_health` — shows evolution is metadata-only, data files unchanged)
- **Run against:** the unified Spark server (`make up`) — Spark UI at http://localhost:4040.
- **Time:** ~10 min. **Laptop-safe:** a few hundred rows total, all under `.tmp/`; the notebook
  drops the Iceberg table and removes the Parquet dir at the end (`make clean` clears the rest).

---

## 1. The scenario

An upstream service evolves. Last quarter `orders` had `(order_id, customer_id, amount)`. This
quarter product adds a loyalty `region`, renames the ambiguous `amount` to `total_amount`, retires
a legacy `status` column, and bumps `customer_id` from `INT` to `BIGINT` because the id space
overflowed. None of that is unusual — schemas drift constantly in production.

The question is what your table does when the schema changes **mid-stream**, with months of old
data already written. Do you rewrite the whole table on every change (expensive, risky), break
on read, or absorb it as a cheap metadata edit? Iceberg (and Delta) take the third path: every
column has a stable **field-id**, so the storage layer doesn't care about column names or
position — it tracks identity. Plain Parquet files have no such layer; readers match columns by
**position**, and a directory of files written with different schemas is a reconciliation problem
you own.

## 2. Break it — evolve an Iceberg table, then read across the change

We create `iceberg_catalog.default.lak6_orders` with the v1 schema and write a batch of "old"
rows. Then we apply each schema change with `ALTER TABLE` and read back — both the old rows and
fresh rows — to see exactly what each operation does:

| Change | DDL | What happens on read |
|--------|-----|----------------------|
| **ADD COLUMN** | `ALTER TABLE … ADD COLUMN region STRING` | old rows read back `NULL` for `region`; no data rewrite |
| **RENAME COLUMN** | `ALTER TABLE … RENAME COLUMN amount TO total_amount` | matched by **field-id** — every existing value is intact under the new name |
| **DROP COLUMN** | `ALTER TABLE … DROP COLUMN status` | column hidden from the schema; the bytes are **not** rewritten or removed |
| **WIDEN type** | `ALTER TABLE … ALTER COLUMN customer_id TYPE BIGINT` | allowed (lossless promotion); old INT values read back as BIGINT |
| **NARROW type** | `ALTER TABLE … ALTER COLUMN customer_id TYPE INT` | **rejected** — shown in a `try/except` (would lose data) |

The "break" here isn't a crash — it's that a naive mental model ("schema change = rewrite the
table" or "rename loses the data") is wrong for Iceberg, and we prove it.

> Why this is laptop-safe: a few hundred rows. The point isn't volume — it's **behavior across a
> schema change**, which a handful of rows demonstrates exactly.

## 3. Detect it — `printSchema()` / column lists + row reads, and the metadata

Schema evolution is a **metadata** operation, so the tells are in the schema and the table
metadata, not the Spark UI Stages tab:

- **`printSchema()` / `.columns` before vs after** each `ALTER` — the column appears, is renamed,
  or disappears immediately, with no job to rewrite data.
- **Row reads** — `SELECT region FROM … WHERE region IS NULL` returns the old rows (added column);
  the renamed column still holds its original values; the widened column reads old values back at
  the new type.
- **`common.iceberg_meta.table_health`** — capture `data_files` before and after the `ALTER`s and
  see the count **unchanged**: evolution touched only metadata, not data files.

The companion entry is in [`docs/troubleshooting.md`](../../docs/troubleshooting.md) (LAK-6 row);
for a symptom → tab/metric map see [`docs/spark-ui-guide.md`](../../docs/spark-ui-guide.md).

## 4. Diagnose

Iceberg assigns every column a **field-id** at creation and tracks data by that id, not by name or
ordinal position. So:

- **ADD** gives a new id; files written before it simply don't have that id → read as `NULL`.
- **RENAME** changes only the name attached to an existing id → all data stays addressable.
- **DROP** removes the id from the current schema → the bytes remain in old files but are never
  projected.
- **WIDEN** (`INT→BIGINT`, `FLOAT→DOUBLE`, `DECIMAL` precision up) is a lossless promotion the spec
  permits; **NARROW** could truncate data, so the spec **forbids** it and Spark raises.

Plain **Parquet** has none of this. Each file embeds its own schema, and a `spark.read.parquet(dir)`
over files with *different* schemas reconciles them **positionally** by default — so an added
column is dropped/mismatched unless you pass `mergeSchema`, and a rename looks like "drop old
column + add new column" because position, not identity, is all Parquet has to go on.

## 5. Fix it — use the format's `ALTER TABLE`; never rely on column position

- **On a lakehouse table, just `ALTER TABLE`.** ADD / RENAME / DROP / widen are all metadata-only
  and safe across history — that's the whole point of the format. (Delta: same DDL; for `mergeSchema`
  on writes it's `.option("mergeSchema","true")` / `spark.databricks.delta.schema.autoMerge`.)
- **For raw Parquet directories, you own reconciliation.** Read with
  `.option("mergeSchema","true")` so added columns surface as `NULL` on older files — but know that
  merge is **positional/by-name**, so a *rename* breaks it (old files keep the old name → two
  columns, each half-null). The robust answer is: don't store evolving data as bare Parquet; put a
  table format over it.
- **Avoid `SELECT *`-by-position assumptions** anywhere downstream; reference columns by name and
  let the format map names → field-ids.

## 6. Prove it

`printSchema()` / `.columns` and targeted row reads before and after each change:

| Check | Iceberg | Plain Parquet (naive read) |
|-------|---------|----------------------------|
| Add column → old rows | `region = NULL`, data intact | column **ignored** without `mergeSchema` |
| Add column → with `mergeSchema` | n/a (always works) | old files surface `NULL`, new files have the value |
| Rename column | all values intact under new name | **breaks** — old files keep old name (two half-null columns) |
| Drop column | hidden, files unchanged | only affects what you select |
| Widen INT→BIGINT | allowed, old values read at new type | — |
| Narrow BIGINT→INT | **rejected** (caught) | — |
| `data_files` after all `ALTER`s | **unchanged** (metadata-only) | — |

Optionally, reading an **older snapshot** (`VERSION AS OF`) after evolving shows the schema is
recorded **per snapshot** — time-travel reads see the schema as it was at that commit.

The proof is twofold: on Iceberg the renamed/widened data reads back intact while `data_files`
stays flat (no rewrite), and on Parquet the *same* changes mismatch until you intervene — and a
rename can't be saved by `mergeSchema` at all.

## 7. Takeaways & "in real production…"

- **Schema changes are metadata-only on Iceberg/Delta** — ADD / RENAME / DROP / widen don't rewrite
  history, so they're cheap and safe even on huge tables. Reach for `ALTER TABLE`, not a full
  rebuild.
- **Field-id tracking is why rename and reorder are safe** — the format tracks column *identity*,
  not name or position. Never write code that depends on column ordinal.
- **Widening is allowed, narrowing is not** — promote types freely (`INT→BIGINT`); a narrowing
  needs an explicit, lossy rewrite you opt into.
- **Plain Parquet is positional with no table metadata** — evolving a bare Parquet directory is a
  manual `mergeSchema` chore that still can't survive a rename. This is precisely the gap open
  table formats close.
- **Coordinate with downstream** — in dbt this is the `on_schema_change` setting
  (`fail` / `ignore` / `sync_all_columns`); a non-nullable add or a rename can still break an
  incremental model even when the table format is fine. That's **DBT-5**.

## 8. Teardown

The notebook ends with a **Teardown** cell that drops `iceberg_catalog.default.lak6_orders` and
`shutil.rmtree`s the Parquet path. `make clean` removes everything under `.tmp/` for a fully fresh
warehouse.
