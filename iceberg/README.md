# `iceberg/` — Lakehouse & table-format correctness (Phase 2)

> **Signpost — not built yet.** This track is scaffolded so the learning path is visible.
> Content arrives when Phase 2 is built (after Phase 1 / SPK-1 is reviewed).

Open-table-format internals (Iceberg / Delta / Parquet) and the maintenance debt that bites
in production. Each module follows **Break → Detect → Fix → Prove** and reuses [`common/`](../common/).
The existing happy-path notebooks under [`app/notebooks/`](../app/notebooks/) (`01_setup_tables`,
`03_query_iceberg`) are the seeds of this track and will migrate here.

## Planned modules — see [`docs/CURRICULUM_PLAN.md`](../docs/CURRICULUM_PLAN.md) (Phase 2)

| ID | Module |
|----|--------|
| `LAK-1` | Format comparison (Iceberg vs Delta vs Parquet) |
| `LAK-2` | Small files & compaction (`rewrite_data_files` / `OPTIMIZE`) |
| `LAK-3` | Snapshot growth & expiration |
| `LAK-4` | Orphan files & GC |
| `LAK-5` | Manifest explosion & rewrite |
| `LAK-6` | Schema evolution |
| `LAK-7` | Partition & hidden partitioning + evolution |
| `LAK-8` | MERGE / upsert: CoW vs MoR |
| `LAK-9` | Time travel & rollback |
| `LAK-10` | (Deep) format internals |
