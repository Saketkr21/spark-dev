# SPK-5 — Join Strategies (broadcast vs sort-merge vs shuffle-hash)

> **Break → Detect → Fix → Prove.** The same big⋈small join can run three completely
> different ways. Spark picks the *physical* join operator for you — and the wrong choice
> is a needless full shuffle of a huge fact (slow) or a too-large broadcast that crushes
> the driver. This module reads the chosen operator straight out of `df.explain()` and the
> **SQL / DataFrame** tab, then drives the cost down by choosing the strategy deliberately.

- **Notebook:** [`spk5_join_strategies.ipynb`](./spk5_join_strategies.ipynb)
- **Toolkit used:** `common.datagen` (`uniform_keys` fact + `key_dimension`), `common.profiles` (flip the broadcast threshold via the `constrained`/`tuned` safety nets), `common.metrics_diff` (prove it)
- **Run against:** the unified Spark server (`make up`) — open the Spark UI at http://localhost:4040 while the notebook runs. Default **tuned** box is fine.
- **Time:** ~10 min. **Laptop-safe:** the fact (~15M rows) is generated lazily and only `count()`-ed — never collected or written — so nothing fills memory or disk. Nothing to delete at the end.

---

## 1. The scenario

A reporting job joins a large `events` fact (~15M rows) onto a small `dimension` table on
`key`. On a freshly-tuned cluster it's quick. Then someone, chasing an unrelated OOM, drops
`spark.sql.autoBroadcastJoinThreshold` to `-1` "to be safe" — and the *same* join suddenly
shuffles all 15M fact rows across the network every run, doubling its runtime. Nobody changed
the data. What changed is the **physical join operator** Spark chose, and the only place that's
visible is the query plan.

Three strategies are on the table for this join, and the threshold/config decides which one fires:

| Strategy | Operator in the plan | What it does | Cost shape |
|----------|----------------------|--------------|------------|
| **Broadcast hash join** | `BroadcastHashJoin` | Ship the *small* side to every executor; stream the big side through it. **No shuffle of the fact.** | Cheapest for big⋈small — until the broadcast side is too big for the driver. |
| **Sort-merge join** | `SortMergeJoin` | Shuffle **both** sides by key, sort each partition, merge. | The safe default for big⋈big; wasteful when one side was small enough to broadcast. |
| **Shuffle-hash join** | `ShuffleHashJoin` | Shuffle both sides, then build an in-memory hash map from the *smaller* side per partition (no sort). | Niche: smaller side fits in memory but is over the broadcast threshold, and you opt out of sort-merge. |

## 2. Break it — the needless shuffle

We generate a `uniform_keys` fact and a small `key_dimension`, then run the join under the
**`constrained` session profile** (`common.profiles.apply_profile`), which sets
`spark.sql.autoBroadcastJoinThreshold = -1`. With broadcasts disabled, Spark cannot ship the
tiny dimension, so it falls back to a **sort-merge join** that shuffles *both* sides — including
all 15M fact rows.

`df.explain()` prints the physical plan; the join node reads `SortMergeJoin` and there are **two
`Exchange` nodes** (one shuffle per side). That second exchange on the fact is the wasted work.

> Why this is laptop-safe: 15M rows are *generated*, not stored; we only `count()`, so the
> driver never collects a large result. This is a wall-clock + shuffle-bytes problem, not a
> memory problem, so the default **tuned** box is fine (no need for `make up-constrained`).

## 3. Detect it — read `df.explain()` and the Spark UI

`df.explain()` is the primary tool for this module (and it **is** Spark-Connect-safe — unlike
`spark.sparkContext`). Read the join operator name and count the `Exchange` nodes:

| You see in the plan | Strategy | Shuffle of the fact? |
|---------------------|----------|----------------------|
| `BroadcastHashJoin` + `BroadcastExchange` (small side only) | broadcast | **none** — fact streamed in place |
| `SortMergeJoin` with **two** `Exchange (hashpartitioning…)` + `Sort` | sort-merge | **yes** — both sides shuffled |
| `ShuffleHashJoin` with two `Exchange`, **no** `Sort` | shuffle-hash | yes — both sides shuffled, no sort |

Then corroborate in the UI — see [`docs/spark-ui-guide.md`](../../docs/spark-ui-guide.md):

- **SQL / DataFrame** tab → click the query → the physical-plan DAG shows the same join operator
  and `Exchange` nodes, now annotated with per-node row counts and **data size**. This is the
  guide's core read for SPK-5: *"the wrong operator here is the bug."*
- **Environment** tab → confirm `spark.sql.autoBroadcastJoinThreshold` is what you think it is.
  `-1` disables broadcast entirely; that single conf flips `BroadcastHashJoin` → `SortMergeJoin`.

## 4. Diagnose

Spark's planner chooses the join physically by size estimate and config:

1. If one side's estimated size ≤ `autoBroadcastJoinThreshold` (default 10 MB) → **broadcast** it.
2. Else, if `spark.sql.join.preferSortMergeJoin = false` **and** one side is small enough to build
   a hash map (≤ threshold × `shuffleHashJoinFactor`) → **shuffle-hash**.
3. Else → **sort-merge** (the universal fallback that always works for big⋈big).

So the pathology in step 2 has two failure directions:

- **Threshold too low** (e.g. `-1`): a dimension that easily fits gets a needless full shuffle of
  the fact. That's the Break above.
- **Broadcasting a too-large table**: forcing a broadcast (high threshold or a `BROADCAST` hint) on
  a side that *isn't* small collects it to the **driver** first, then ships it to every executor —
  driver memory pressure and an `OutOfMemoryError` / GC stall. (This is exactly the driver-OOM
  failure mode of **SPK-3** — *forward reference*; broadcast only what genuinely fits.)

## 5. Fix it — choose the strategy deliberately

| Fix | How | When to reach for it |
|-----|-----|----------------------|
| **Set the threshold deliberately** | `spark.conf.set("spark.sql.autoBroadcastJoinThreshold", <bytes>)` — or use the `tuned` profile (10 MB) | Default policy. Size your dimensions; keep the threshold high enough to broadcast them but low enough not to broadcast a fact. |
| **Force a broadcast** | `df.join(F.broadcast(dim), …)` or the SQL hint `/*+ BROADCAST(d) */` / `.hint("broadcast")` | The estimate is wrong (e.g. after filters the side is tiny but Spark over-estimates) and the side genuinely fits the driver. |
| **Force shuffle-hash** | `spark.conf.set("spark.sql.join.preferSortMergeJoin", "false")` (+ hint `.hint("shuffle_hash")`) | The smaller side is over the broadcast threshold but still fits in per-partition memory, and you want to skip the sort cost of sort-merge. |

Map each strategy to **when to use it**:

- **Broadcast** → big⋈small where the small side fits the driver/executors. *Try first.*
- **Sort-merge** → big⋈big (neither side broadcastable). The safe default; correct but shuffles both.
- **Shuffle-hash** → medium⋈big: smaller side too big to broadcast yet small enough to hash in memory; sort overhead avoided.

## 6. Prove it

`common.metrics_diff.compare([...])` prints a before/after table across the strategies. Expected
shape (the headline rows for this module are **runtime** and **shuffle bytes** — *not* the skew
ratio, which is the SPK-1 headline):

| Metric | sort-merge (broadcast off) | broadcast | shuffle-hash |
|--------|---------------------------:|----------:|-------------:|
| Wall-clock runtime | high | **↓↓** | ↓ |
| Shuffle read/write | **large** (both sides shuffled) | **~0 on the fact** | large (both shuffled, no sort) |
| Tasks | many (post-shuffle partitions) | few | many |

The fact-side shuffle collapsing to ~0 under broadcast is the proof you picked the right operator.

## 7. Takeaways & "in real production…"

- **Read the operator, don't guess.** `df.explain()` (Connect-safe) or the **SQL / DataFrame** tab
  names the strategy; `BroadcastHashJoin` vs `SortMergeJoin` vs `ShuffleHashJoin` *is* the decision.
- **`autoBroadcastJoinThreshold` is the master switch.** `-1` disables broadcast and forces
  sort-merge; the default 10 MB broadcasts small dimensions. Set it deliberately — don't cargo-cult
  `-1` to "fix" an unrelated OOM, or you'll shuffle every fact.
- **Broadcast only what fits the driver.** A too-large broadcast pressures driver memory → SPK-3
  territory. Size the side after filters; use `F.broadcast()` / `BROADCAST` hints when the estimate
  is wrong, not as a blanket policy.
- **In production:** keep AQE on (it can convert sort-merge → broadcast at runtime when the actual
  side turns out small), set the threshold to comfortably cover your dimensions, and watch the SQL
  tab for an unexpected `SortMergeJoin` (two `Exchange` nodes) where a `BroadcastHashJoin` was
  intended — the universal "wrong join strategy" signature.

## 8. Teardown

Nothing to clean: the data was generated lazily and only counted, so no tables or files were
written. The notebook resets the session profile to `tuned` at the end. If you experimented with
writes, `make clean` removes everything under `.tmp/`.
