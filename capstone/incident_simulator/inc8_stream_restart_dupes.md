# INC-8 — Stream restart re-ingested the topic ⛑️

> **Page:** `[crit] orders_ingest sink row-count +312% in 10m after deploy; duplicate order_ids detected downstream.`
> **Handed to you:** the Structured Streaming job's `checkpointLocation` directory on disk, the sink table (`count(*)` vs `count(distinct id)`), and the driver logs around the last restart. A deploy went out 10 minutes ago. Diagnose before acting — do **not** wipe state.

## Symptom

Right after a restart (deploy / config change / operator stop-start), the sink **`count(*)` jumped far past what the producers actually sent**, and `count(distinct id)` is now well below `count(*)` — duplicate rows. The new events are all already-seen `id`s; the job appears to have **re-read the topic from the beginning** and re-emitted batches it had already written. (Variant on a sister job: instead of duplicates the consumer is **stuck** on one offset and **lag is climbing** — same family of failure.)

## Your job (think like an SRE)

1. **Where do you look first?** A stream that resumes correctly remembers which offsets it committed. Where does Structured Streaming keep that memory — and is it still there, and the **same path** as before the restart?
2. **What confirms the root cause?** Compare the sink's `count(*)` against `count(distinct id)`. Inspect `q.lastProgress` / the checkpoint's `offsets/` log: on a healthy resume the run's **start offset == the previous run's end offset**. What do you see instead?
3. **What's the fix — and what proves it?** What makes a restart re-ingest **only new** data? And given Kafka delivery is at-least-once, what second guarantee removes duplicates even if a batch replays?

<details>
<summary>🔧 Diagnosis &amp; fix — open only after a hypothesis</summary>

- **Root cause:** the **checkpoint was lost or not reused** — the deploy changed/wiped `checkpointLocation` (or pointed the query at a fresh dir). With no committed offsets and `startingOffsets=earliest`, the query has no memory of what it processed and **re-reads from offset 0, reprocessing every event**. Paired with a **non-idempotent sink** (plain append, no key), that replay lands as duplicate rows. (The stuck-consumer variant: a corrupt/poison record makes the parse fail on one offset; without committing past it the partition blocks and lag grows.)
- **Detect:** confirm the `checkpointLocation` **exists and is the SAME path across restarts** (diff the deploy config; check the dir wasn't recreated). Compare sink `count(*)` vs `count(distinct id)` — equal is exactly-once, `count(*)` jumping past what you produced is the duplicate signature. Read `q.lastProgress.sources[0].startOffset/endOffset`: on a wiped checkpoint the start offset resets to `0` instead of matching the prior run's end offset. For the stuck variant: `parsed.id IS NULL AND value IS NOT NULL` and a consumer pinned at one offset in kafka-ui.
- **Fix:** keep **ONE stable, durable `checkpointLocation` per query** — treat it as part of the job's state, never scratch, never deleted casually — so the stream resumes from its committed offsets. Make the sink **idempotent** (Iceberg atomic append + checkpoint = effectively-once; or `MERGE`/upsert by `id` so an at-least-once replay can't duplicate). For the poison-pill stall, **route the bad record to a dead-letter table and commit past it** so the partition keeps moving.
- **Prove:** restart with the restored checkpoint and the run **re-ingests only NEW data** — `count(*) == count(distinct id)`, no duplicates, and `lastProgress` shows the resuming run's start offset equal to the prior run's end offset. A no-new-data restart processes `0` rows (idempotent).
- **Reproduce &amp; learn it:** [STR-2](../../kafka/checkpoints/) — bounded `availableNow` runs sharing ONE checkpoint go `1000 → 1500 → 1500` with `count(*) == count(distinct id)`, and a contrast run against a fresh/empty checkpoint re-ingests all 1000 (duplicates made explicit). Then [KAF-6](../../kafka/poison_pill/) for the stuck-partition variant: the dead-letter pattern isolates the unparseable record so progress is decoupled from parse-success and the consumer commits past it.

</details>
