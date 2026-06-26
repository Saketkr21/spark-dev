# Changelog

All notable changes to `dbt-polyglot` are documented here. Format loosely follows
[Keep a Changelog](https://keepachangelog.com/); this project uses [SemVer](https://semver.org/).

## [0.2.0] — Unreleased

### Changed
- **Renamed `dbt-spark-transpile` → `dbt-polyglot`** and restructured to a standard src-layout
  (`src/dbt_polyglot/`): split into `transpile` (the compile-phase patch) and `fixups` (the
  `SPARK_FIXUPS` registry), with import-time activation in `__init__`.

### Removed
- The custom trust-gate (the old `transpile_check` module / `[check]` extra). Validating the
  transpiled SQL is now delegated to dbt-core's **native** `dbt build --empty` (or `dbt show
  --limit 0`), which runs each model against the warehouse via your `profiles.yml` adapter with
  zero rows — warehouse-agnostic, no PyHive/PyYAML dependency, no reinvented connection layer.

## [0.1.0] — Unreleased
Initial release (as `dbt-spark-transpile`).

### Added
- Compile-phase transpile: wraps `dbt.compilation.Compiler._compile_code` to translate each opted-in
  model's SQL from a source dialect to Spark via `sqlglot` (`parse → fix-ups → generate`), before dbt
  wraps it in materialization DDL. Opt in with `+transpile_from: <dialect>` in dbt config; no model edits.
- **Spark-output fix-up layer** (`SPARK_FIXUPS`): repairs sqlglot output that Spark's real parser rejects.
  First transform rewrites quantified-subquery comparisons (`x <> ALL (subq)` / `x = ANY (subq)`) back to
  `NOT x IN (subq)` / `x IN (subq)`. Extensible registry.
- Fail-soft: any transpile error / empty / multi-statement output logs a WARNING and passes the original
  SQL through unchanged — never crashes a compile, never silently emits a wrong result.
- Pretty-printed output; no-op when `transpile_from` is unset or equals the target dialect.

### Notes
- Patches a dbt-core private method (`_compile_code`); import-guarded to fail open. Pin a supported
  dbt-core range and re-verify on major dbt upgrades.
