# `debezium/` — Change Data Capture track (Phase 4)

> **Signpost — not built yet.** This track is scaffolded so the learning path is visible.
> Content arrives when Phase 4 is built.

A self-contained CDC pipeline:
**Postgres → Debezium (Kafka Connect) → Kafka → Spark Structured Streaming → Iceberg MERGE.**
Deployed as **Kafka Connect + the Debezium Postgres connector** (mirrors real production),
with its own Compose additions (Postgres + Kafka Connect) and connector configs living here.

> **Laptop note:** the CDC pipeline adds Postgres + Kafka Connect. On an 8 GB machine you may
> need to stop optional services (e.g. the history server) while running this track.

## Planned modules — see [`docs/CURRICULUM_PLAN.md`](../docs/CURRICULUM_PLAN.md) (Phase 4)

| ID | Module |
|----|--------|
| `CDC-1` | Local Postgres + logical replication |
| `CDC-2` | Debezium connector bring-up |
| `CDC-3` | Snapshot vs streaming phases |
| `CDC-4` | The CDC event envelope (before/after/op/ts) |
| `CDC-5` | Replication slot & WAL growth ⚠️ |
| `CDC-6` | Tombstones, deletes & replica identity |
| `CDC-7` | CDC → Spark → Iceberg upsert pipeline |
| `CDC-8` | CDC schema evolution |
| `CDC-9` | (Deep) failure-mode tour |
