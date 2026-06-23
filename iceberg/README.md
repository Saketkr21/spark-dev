# `iceberg/` — Lakehouse & table-format correctness (Phase 2) ✅ complete

Open-table-format internals (Iceberg / Delta / Parquet) and the **maintenance debt** that bites in
production. Each module follows **Break → Detect → Fix → Prove** (see
[`docs/CURRICULUM_BRIEF.md`](../docs/CURRICULUM_BRIEF.md)), reuses the [`common/`](../common/) toolkit
— including [`common/iceberg_meta.py`](../common/iceberg_meta.py) (`table_health` / `compare_health`,
the data-file / snapshot / manifest counts that are the "Prove it" here) — and ends with teardown.

> **Laptop-safe:** tiny data, all under `.tmp/`; `make clean` recovers. **Connect-safe:** every
> notebook uses `spark.sql` + DataFrame APIs only (Iceberg maintenance runs via
> `CALL iceberg_catalog.system.<proc>(...)`, which works over Spark Connect).
>
> **Run any module:** `make up` → `make jupyter` → open its notebook.

## Modules

`[ ]` not started · `[~]` in progress · `[x]` built & live-tested (headless `nbconvert`)

| ID | Module | Status |
|----|--------|--------|
| `LAK-1` | [Format comparison](format_comparison/) — Iceberg vs Delta vs Parquet (ACID, time travel, schema evo, MERGE) | `[x]` |
| `LAK-2` | [Small files & compaction](small_files/) — tiny-file litter → `rewrite_data_files` | `[x]` |
| `LAK-3` | [Snapshot growth & expiration](snapshots/) — unbounded snapshots → `expire_snapshots` | `[x]` |
| `LAK-4` | [Orphan files & GC](orphan_files/) — unreferenced files → `remove_orphan_files` (24h guard) | `[x]` |
| `LAK-5` | [Manifest explosion & rewrite](manifests/) — many manifests slow planning → `rewrite_manifests` | `[x]` |
| `LAK-6` | [Schema evolution](schema_evolution/) — add/rename/drop/widen by field-id vs positional Parquet | `[x]` |
| `LAK-7` | [Partitioning & hidden partitioning + evolution](partitioning/) — `days()`/`bucket()`, prune, evolve | `[x]` |
| `LAK-8` | [MERGE: CoW vs MoR](merge_cow_mor/) — 1-row MERGE rewrites a partition vs delete files | `[x]` |
| `LAK-9` | [Time travel & rollback](time_travel/) — recover a bad write; the expired-snapshot gotcha | `[x]` |
| `LAK-10` | [Deep format internals](internals/) — metadata pointer, manifest stats, v1/v2 deletes, catalogs | `[x]` |

## Layout

```
iceberg/
├── README.md             # this file (Phase 2 track index)
├── format_comparison/    # LAK-1
├── small_files/          # LAK-2
├── snapshots/            # LAK-3
├── orphan_files/         # LAK-4
├── manifests/            # LAK-5
├── schema_evolution/     # LAK-6
├── partitioning/         # LAK-7
├── merge_cow_mor/        # LAK-8
├── time_travel/          # LAK-9
└── internals/            # LAK-10
```

Each `iceberg/<topic>/` holds a `README.md` (the Break→Detect→Fix→Prove writeup) and a runnable
`lak<N>_<topic>.ipynb`. All built and **live-verified** end-to-end against the Spark server.

## Suggested order

`LAK-1` (formats) → `LAK-2` (small files) → `LAK-3` (snapshots) → `LAK-5` (manifests) →
`LAK-4` (orphans) → `LAK-6` (schema) → `LAK-7` (partitioning) → `LAK-8` (MERGE) →
`LAK-9` (time travel) → `LAK-10` (internals). The first five are the everyday maintenance jobs;
the rest are correctness/semantics deep-dives.
