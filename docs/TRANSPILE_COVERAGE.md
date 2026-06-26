# dbt-polyglot — coverage, advantages & limitations

A trust writeup for the Snowflake/other-dialect → Spark transpile drop-in. Backed by a cross-dialect
test matrix run against **real Spark 4.0.2** (each source-dialect snippet was transpiled, then validated
on Spark via a zero-row execution that forces parse + analysis).

## TL;DR
- **Advantage:** the vast majority of analytical SQL across **Snowflake, BigQuery, Databricks, Redshift,
  T-SQL, Postgres, DuckDB** transpiles to valid Spark with **zero model edits** — config only.
- **Trust:** a model is either **verified-valid Spark** or it **fails loudly** (native `dbt build --empty`
  names it). It never silently produces wrong-but-running SQL from an un-converted construct.
- **Limitation:** a small tail of genuinely engine-specific constructs (chiefly Snowflake `LATERAL
  FLATTEN` and some semi-structured features) has no faithful Spark form and surfaces as a loud failure.
- **Caveat:** validity ≠ guaranteed semantic identity. sqlglot is best-effort on semantics (it does
  handle the big ones, e.g. null-ordering); subtle behavioral differences are possible — see below.

## Advantages
1. **No code refactoring.** Models stay in their source dialect; only `profiles.yml` + one
   `dbt_project.yml` flag + a `pip install` change. Swap the warehouse, keep the repo.
2. **Dialect-parametric.** `transpile_from` is any sqlglot dialect — the same drop-in serves
   Snowflake→Spark today and BigQuery/Redshift/T-SQL→Spark tomorrow.
3. **Broad coverage** (see matrix) — window funcs incl. `QUALIFY`, subquery predicates, conditional/null
   functions, casts, date math, string funcs, aggregates, and even much semi-structured access.
4. **Verifiable.** `make transpile-check` (native `dbt build --empty`) certifies, upfront, exactly which
   models run on Spark — so you trust every green model and see every blocker.
5. **Safe to leave on.** No-op when not opted in or already Spark; fail-soft (never crashes a compile);
   pretty-printed output in `target/compiled/`.

## Cross-dialect test matrix (validated on Spark 4.0.2)
**PASS (transpiled to valid Spark)** — 39 of 40 functional constructs, across all 7 dialects:

| Category | Examples that passed | Dialects covered |
|---|---|---|
| Basics | select, join, `USING`, CTEs | snowflake, bigquery, databricks, redshift, tsql, postgres, duckdb |
| Window | `QUALIFY ROW_NUMBER()…=1` | snowflake, bigquery, databricks |
| Subquery | `x NOT IN (subquery)` *(fix-up: was `<> ALL`)* | snowflake, bigquery, redshift |
| Conditional/null | `IFF`, `NVL`, `ZEROIFNULL`, `IFNULL`, `ISNULL` | snowflake, bigquery, tsql |
| Casts | `::string`/`::int`, `SAFE_CAST` | snowflake, redshift, bigquery |
| Dates | `DATEADD`, `DATEDIFF`, `DATE_ADD`, `DATE_TRUNC` | snowflake, redshift, tsql, bigquery |
| Strings | `SUBSTR`, `ILIKE` | snowflake, bigquery |
| Aggregates | `LISTAGG`, `STRING_AGG`, `ARRAY_AGG` | snowflake, bigquery |
| Semi-structured | VARIANT path `js:f::int`→`GET_JSON_OBJECT`, `OBJECT_CONSTRUCT`→`STRUCT`, `UNNEST`, `STRUCT` | snowflake, bigquery |
| Dialect syntax | `TOP n`, `SAMPLE`, `SELECT * EXCEPT(col)` | tsql, snowflake, bigquery |
| Quoting | reserved/quoted identifiers | snowflake |

**FAIL on Spark (transpiled but rejected) — 1, genuine:**
- Snowflake **`LATERAL FLATTEN(input => arr)`** → sqlglot emits `LATERAL EXPLODE(input => arr) AS
  f(SEQ,KEY,PATH,INDEX,VALUE,THIS)`; Spark rejects the named arg (`UNRECOGNIZED_PARAMETER_NAME`) and the
  6-column FLATTEN shape has no direct `explode` equivalent. **Fails loud** (caught by the trust gate).

**Edge cases — fail-soft (original passed through, by design) — 3:**
- empty / comment-only input → nothing to transpile.
- multi-statement (`select 1; select 2`) → not supported; passed through. (dbt models are single-statement.)

> Note: a `name::int` cast on a *constant* test column reported a cast error — that was a **test
> artifact** (Spark ANSI constant-folds `CAST('a' AS INT)`); on a real column the cast is a valid plan
> with the same runtime semantics as the source engine. `::` casts are fully supported.

## Limitations (the honest list)
1. **Genuinely engine-specific constructs** have no faithful Spark form and fail loud:
   - Snowflake `LATERAL FLATTEN` and deeper semi-structured table functions; some proprietary functions
     (`GENERATOR`, certain `OBJECT_*`/`ARRAY_*`), and the full `VARIANT` type model.
   - These are inherent to dialect differences — **any** transpiler (incl. SQLMesh, which also uses
     sqlglot) hits them. They are surfaced, never silently wrong.
2. **Validity ≠ guaranteed semantic identity.** The check proves SQL *parses + analyzes* on Spark; it
   does not prove identical results. sqlglot handles the well-known semantic gaps (e.g. null ordering →
   explicit `NULLS LAST`), but subtle behavioral differences can remain (numeric precision/overflow under
   ANSI, implicit-cast rules, regex/string-collation, timezone/date edge cases). For business-critical
   models, spot-check results (e.g. row-count/aggregate diff vs the source warehouse).
3. **Multi-statement model bodies aren't transpiled** (passed through). dbt models are single-statement,
   so this rarely matters.
4. **Couples to a dbt-core private method** (`Compiler._compile_code`). Guarded to fail-open, but pin a
   supported dbt-core range; re-verify on dbt upgrades.
5. **The transpiled form must be something Spark actually supports** — the fix-up layer closes the
   sqlglot↔Spark gaps we find (e.g. `<> ALL`→`NOT IN`); new gaps are added as discovered (each
   EXPLAIN-verified). It is extensible, not exhaustive on day one.

## How to know for *your* repo
Don't trust this matrix blindly — certify your actual models with native dbt:
```bash
cd your_dbt_project
dbt build --empty                  # or: make transpile-check
```
`--empty` runs every model's transpiled SQL against Spark with zero rows, in DAG order, and **fails
loudly naming any model** that isn't valid Spark (exits non-zero — a CI gate). Whatever builds green is
proven to parse+analyze on Spark; whatever it flags is the finite, named set to handle (rewrite that
handful, or add a fix-up transform). Use `dbt show --limit 0 -s <model>` for a read-only check.

## Recommendation
Use it as the **bridge**: turn it on, run the check, and you'll typically find the large majority of
models verified green with a short, explicit list of genuinely-Snowflake-only constructs to address by
hand. That converts an all-or-nothing rewrite into a small, known, trustable remainder.
