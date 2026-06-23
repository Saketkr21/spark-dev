# `quality/` — Data quality labs (Phase 5)

> **Signpost — not built yet.** This track is scaffolded so the learning path is visible.
> Content arrives when Phase 5 is built.

Teaches **both** data-quality approaches and where each fits:

- **dbt native tests** — structural / in-pipeline assertions (not-null, unique, relationships,
  accepted values), layered staging (structural) vs marts (business-logic).
- **Great Expectations** — statistical / profiling / drift checks and standalone validation,
  run against the Spark / Iceberg tables.

The dbt project itself lives in [`dbt/`](../dbt/); the GE checkpoints and `dbt-expectations`
labs live here.

## Planned modules — see [`docs/CURRICULUM_PLAN.md`](../docs/CURRICULUM_PLAN.md) (Phase 5)

| ID | Module |
|----|--------|
| `DBT-6` | Testing strategy & layering |
| `DBT-7` | Quarantine pattern (`severity: warn` → quarantine table) |
| `DBT-8` | dbt-expectations + Great Expectations (when to use which) |
| `DBT-9` | Sources, freshness, contracts, exposures |

(The dbt-modeling modules `DBT-1`…`DBT-5`, `DBT-10` extend the [`dbt/`](../dbt/) project directly.)
