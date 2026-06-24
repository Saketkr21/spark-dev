# INC-5 — Postgres is eating its own disk ⛑️

> **Page:** PagerDuty — `postgres-primary` disk usage climbing fast toward full; WAL segments are not being recycled. Disk-full ⇒ the primary stops accepting writes.
> **Handed to you:** a psql shell on the primary (or `common.cdc_helpers.list_slots()`) and the Kafka Connect REST API at `:8083`. Diagnose before acting.

## Symptom
`pg_wal/` is growing without bound — segment count only ever goes up, never down. Free disk is dropping at a steady MB/min even though the write rate looks normal. There is a logical replication slot that reads `active = false`, yet the WAL it is retaining keeps rising. The timeline lines up: it started right after the Debezium connector (the slot's consumer) stopped/crashed/was paused — but application writes to the source tables kept flowing the whole time.

## Your job (think like an SRE)
1. Where do you look first? (Is this a runaway query, a checkpoint problem, or something holding WAL?)
2. What confirms the root cause — which single metric on which catalog view tells you a slot is the culprit, and that it is *inactive*?
3. What's the fix — and what proves WAL actually starts recycling again?

<details>
<summary>🔧 Diagnosis &amp; fix — open only after a hypothesis</summary>

- **Root cause:** A logical replication slot is a **named cursor into the WAL**. Postgres retains every WAL segment from the slot's `restart_lsn` forward **until a consumer reads past it and advances the slot**. A healthy Debezium task advances it continuously, so WAL recycles and disk stays flat. When the consumer stops while writes continue, the slot goes **inactive**, `restart_lsn` freezes, but `pg_current_wal_lsn()` keeps moving — so the gap Postgres must keep on disk only grows. An abandoned/stalled slot pins WAL **forever** → disk fills → the primary halts. This is silent until the disk is full.

- **Detect:** `pg_replication_slots` is the source of truth. Run:
  ```sql
  SELECT slot_name,
         active,
         pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS retained
  FROM pg_replication_slots
  ORDER BY retained DESC;
  ```
  (or `common.cdc_helpers.list_slots()`, which surfaces the same `active` / `retained_bytes`). The smoking gun is a slot with `active = false` **and** `retained` climbing on every measurement. Sample it twice a minute apart: a monotonically rising `retained` on an inactive slot *is* the incident.

- **Fix:** Two paths depending on whether the slot still has an owner you want.
  - **Slot you still need (connector merely paused/crashed):** resume/restart the connector so its task reattaches and consumes — `PUT /connectors/<name>/resume` on `:8083` (or restart the failed task). A running consumer advances the slot and Postgres recycles the pinned WAL.
  - **Orphaned slot (connector deleted but slot left behind, or down indefinitely):** drop it — `SELECT pg_drop_replication_slot('<slot_name>')` (or `cdc.drop_slot(name)`). Postgres frees the WAL it was pinning the moment the slot is gone.
  - **Cap the blast radius:** set `max_slot_wal_keep_size` (PG 13+). The repo's default is `-1` (unbounded — the dangerous default). A ceiling lets Postgres **invalidate** a runaway slot instead of running out of disk. Trade-off: an invalidated slot forces its consumer to re-snapshot, so size it generously and alert *before* the cap.

- **Prove:** Re-sample `pg_replication_slots`. After resume, `retained` falls back toward ~0 as the connector catches up and advances `restart_lsn`; after a drop, that slot's `retained` disappears entirely. Either way `pg_wal/` segments start getting recycled and free disk stops falling — the gauge flattens, then recovers.

- **Reproduce &amp; learn it:** [CDC-5](../../debezium/wal_growth/) — reproduces the *exact* mechanism at MB scale (a hand-made inactive slot whose `retained_bytes` climbs monotonically, plus a real Debezium connector driven active → paused → resumed so you watch `retained_bytes` go flat → climbing → recycled). Same failure mode as the GB-scale outage, laptop-safe.

</details>
