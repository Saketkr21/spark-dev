# `kafka/` — Kafka & Structured Streaming robustness (Phase 3)

> **Signpost — not built yet.** This track is scaffolded so the learning path is visible.
> Content arrives when Phase 3 is built.

Messaging fundamentals + streaming correctness. Builds on the existing
[`app/utils/sales_producer.py`](../app/utils/sales_producer.py) /
[`app/notebooks/04_sales_streaming_to_iceberg`](../app/notebooks/) — the producers and the
sales-streaming notebook are the seeds of this track and will migrate here.

## Planned modules — see [`docs/CURRICULUM_PLAN.md`](../docs/CURRICULUM_PLAN.md) (Phase 3)

| ID | Module |
|----|--------|
| `KAF-1` | Partitioning & hot partitions |
| `KAF-2` | Consumer lag & offset semantics |
| `KAF-3` | Consumer groups & rebalancing |
| `KAF-4` | Retention & compaction |
| `KAF-5` | Delivery semantics (at-least-once vs exactly-once) |
| `KAF-6` | Poison pill / dead-letter |
| `STR-1` | Watermarking & late data |
| `STR-2` | Idempotency, checkpoints & restart |
| `STR-3` | Backpressure & micro-batch sizing |
