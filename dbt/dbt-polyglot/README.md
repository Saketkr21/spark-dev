# dbt-polyglot

**Run a dbt project written in another SQL dialect (Snowflake, BigQuery, Redshift, …) on
Spark — unchanged.** Each model's SQL is transpiled to Spark with
[`sqlglot`](https://github.com/tobikodata/sqlglot) at dbt's **compile phase**, so the SQL
dbt actually executes (and what lands in `target/compiled/`) is already Spark.

The only changes are configuration — your model `.sql` files are never edited. Drop the
package into any existing dbt repo, point `profiles.yml` at Spark, declare the source
dialect in `dbt_project.yml`, and `dbt build`.

> Why this exists: Spark has no `QUALIFY` clause (`[PARSE_SYNTAX_ERROR] … near 'QUALIFY'`),
> plus dozens of smaller dialect gaps (`IFF`, `NVL`, `::` casts, `DATEADD`, null ordering, …).
> A portable/Snowflake-style model fails on Spark until its SQL is translated. This package
> does that translation transparently, in-place, at compile time.

---

## Install

It is a **normal Python package** — install it into the same virtualenv your `dbt` runs in.
Installation auto-activates the patch (via a `.pth` file that imports the module on
interpreter start-up; see [Installation: why pip, not `dbt deps`](#installation-why-pip-not-dbt-deps)).

```bash
pip install dbt-polyglot
```

From a git checkout (no PyPI release yet):

```bash
pip install "git+https://github.com/SaketKumar/dbt-polyglot.git"
```

Local / editable (developing the package):

```bash
pip install -e path/to/dbt-polyglot
```

You also need a Spark adapter for dbt (this package does not pull one in, so you can choose
your connection method):

```bash
pip install "dbt-spark[PyHive]"     # Thrift/HiveServer2, used in the examples below
```

---

## Configure (the only changes you make)

### 1. `profiles.yml` — point the output at Spark

```yaml
your_profile:
  target: dev
  outputs:
    dev:
      type: spark
      method: thrift
      host: "{{ env_var('DBT_SPARK_HOST', 'localhost') }}"
      port: "{{ env_var('DBT_SPARK_PORT', 10000) | int }}"
      schema: analytics
```

### 2. `dbt_project.yml` — declare your models' source dialect

```yaml
models:
  your_project:
    +transpile_from: snowflake     # the dialect your models are written in
    # +transpile_to: spark         # optional, default 'spark'
```

`transpile_from` accepts **any** dialect `sqlglot` understands — `snowflake`, `bigquery`,
`redshift`, `tsql`, `postgres`, `duckdb`, `presto`, `trino`, … `transpile_to` defaults to
`spark` and rarely needs changing.

You can scope it to a subtree (`models.your_project.staging.+transpile_from: …`) or override
it per model — a per-model `config` beats the project default:

```sql
-- models/marts/latest_order.sql  (written in Snowflake SQL, runs on Spark)
{{ config(materialized='table', transpile_from='snowflake') }}

select *
from {{ ref('orders') }}
qualify row_number() over (partition by customer_id order by ordered_at desc) = 1
```

That's it. `dbt build` now runs your existing models on Spark, no model edits.

---

## How it works

At dbt **compile**, the package wraps `dbt.compilation.Compiler._compile_code` and runs an
extra step on each opted-in model's compiled SQL body:

```
parse(read=transpile_from)  →  apply SPARK_FIXUPS  →  generate(transpile_to, pretty=True)
```

Because the rewrite happens on the model body **before** dbt wraps it in the materialization
DDL (`create table … as …`), both `target/compiled/` and the SQL sent to Spark are pure
Spark — there is no mixed-dialect string and no separate output directory.

### The fix-up layer (what makes it trustable)

`sqlglot`'s output is occasionally valid in *its* model of Spark but rejected by Spark's
**real** parser. The classic case: `x NOT IN (subquery)`, which `sqlglot`'s Snowflake reader
canonicalizes to the **unsupported** `x <> ALL (subquery)`. The `SPARK_FIXUPS` registry is a
list of small AST transforms applied to the parsed tree before Spark SQL is generated; the
first one rewrites quantified-subquery comparisons (`<> ALL` / `= ANY (subq)`) back to
`NOT x IN` / `x IN (subq)`. The registry is extensible — one EXPLAIN-verified transform per
gap discovered.

### Trust model — verified, or fails loud (never silently wrong)

A model is either converted to **valid Spark SQL** or it **fails loudly** with a clear
dbt/Spark error naming the model. It never silently emits a wrong result from an
un-converted construct:

- **Fail-soft + loud.** If `sqlglot` can't parse the SQL as the source dialect, or produces
  empty/multi-statement output, the patch logs a `WARNING` (visible in the dbt run) and
  passes the **original SQL through unchanged**. Spark then either runs it (it was already
  valid) or rejects it loudly — so the failure surfaces, it is never hidden.

To certify a whole repo **upfront** — before a heavy run — use dbt's own native validation.
No extra tooling: dbt already runs SQL through your `profiles.yml` adapter, against whatever
warehouse you target.

```bash
dbt build --empty              # build every model with 0 input rows (DAG-ordered)
dbt build --empty --select marts.*   # any dbt selector works
dbt show --limit 0 -s my_model # read-only: validate the SELECT without materializing
```

`--empty` limits every `ref`/`source` to zero rows, so dbt executes each model's real SQL
against the warehouse — moving no data — and **fails loudly, naming the model**, if the
transpiled SQL is invalid. Because it builds in dependency order, there is no "upstream not
built" ambiguity. That makes `dbt build --empty` a drop-in CI gate (it exits non-zero on the
first invalid model). `dbt show --limit 0` is the non-destructive variant when the target
role can't create objects.

### Scope

Every opted-in model is transpiled — the full `sqlglot` breadth (`IFF`→`IF`, `NVL`→`COALESCE`,
`::`→`CAST`, `DATEADD`→`DATE_ADD`, `QUALIFY`→windowed subquery, …). To transpile only part of a
project, scope `+transpile_from` to a folder/model subtree (or set it per model) — the dbt-native
way — rather than a global on/off.

### No-op guarantee

If `transpile_from` is unset, or equals `transpile_to` (you're already writing Spark SQL),
the model is **never touched** — `sqlglot` is not even called and nothing is reformatted.

### A note on `NULLS LAST` in the output (intentional)

Snowflake and Spark have **opposite** default null ordering (Snowflake sorts NULLs largest →
last; Spark sorts them smallest → first). When translating a Snowflake `ORDER BY x`,
`sqlglot` appends an explicit `… NULLS LAST` to **preserve Snowflake semantics** — without
it, a `QUALIFY ROW_NUMBER() … = 1` top-N pick could choose a different row. It is added only
on a true cross-dialect translation, and is semantically required — do not strip it.

---

## Installation: why `pip`, not `dbt deps`

**`dbt deps` cannot install this — you must `pip install` it.** They do different things:

- **`dbt deps`** installs **dbt packages**: bundles of dbt *macros, models, seeds, and
  tests* (the things listed in `packages.yml` / `dependencies.yml`). It pulls SQL/Jinja
  assets into `dbt_packages/` and **never installs or runs Python code**.
- **`dbt-polyglot`** is a **Python package**. It works by monkeypatching a dbt-core
  function at runtime, and it activates through a `.pth` file that Python executes on
  interpreter start-up. Both of those are Python-installer concerns — only `pip` (or `uv`,
  `poetry`, etc.) places a `.pth` into `site-packages` and registers the dependency.

So it is installed exactly like `dbt-core` or an adapter, into the same environment as your
dbt. It does not appear in `packages.yml`.

---

## Package contents

A standard src-layout package — `src/dbt_polyglot/` holds the import package, plus a `.pth`
that activates it on start-up:

| File | Role |
|------|------|
| `src/dbt_polyglot/__init__.py` | Import-time activation: patches the dbt Compiler. |
| `src/dbt_polyglot/transpile.py` | The compile-phase patch (`patch_compiler`) + core `spark_safe_transpile`. |
| `src/dbt_polyglot/fixups.py` | The `SPARK_FIXUPS` registry of AST transforms. |
| `dbt_polyglot.pth` | One line (`import dbt_polyglot`); auto-activates on start-up. Installed into `site-packages` by the `build_py` shim in `setup.py`. |
| `pyproject.toml` / `setup.py` | PEP 517 metadata; `setup.py` exists only to place the `.pth` into purelib. |
| `LICENSE` | Apache-2.0. |

This package is intentionally limited to **transpilation**. Validating the result is left to
dbt's native `dbt build --empty` (see [Trust model](#trust-model--verified-or-fails-loud-never-silently-wrong)
above); catalog routing (mapping `file_format` → a Spark catalog) and seed re-runnability are
**separate concerns** and are not bundled here.

---

## Compatibility & caveats

- **dbt-core private method.** The patch wraps `dbt.compilation.Compiler._compile_code`, a
  **private** dbt-core method. It forwards `*args/**kwargs` to tolerate signature drift and
  is fully import-guarded (if dbt-core or `sqlglot` aren't importable, or the seam moves, the
  patch does nothing rather than breaking the interpreter). Still, **pin a supported dbt-core
  range** when depending on this in production, and re-verify after major dbt upgrades.
- **`sqlglot` coverage.** `sqlglot` maps a large surface but not everything. Exotic dialect
  features — Snowflake `LATERAL FLATTEN`, `VARIANT`/`OBJECT`/`ARRAY` semantics, `:` path
  access, `LISTAGG`, and similar — may not translate cleanly. Those surface via the fail-soft
  WARNING and `dbt build --empty`, by design, rather than silently.
- **Self-contained.** The module imports nothing from any host project, so it can be lifted
  into its own repo unchanged.

## License

Apache-2.0 — see [LICENSE](LICENSE).
