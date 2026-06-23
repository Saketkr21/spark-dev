# SPK-8 — Caching & Persistence Tradeoffs

> **Break → Detect → Fix → Prove.** Caching is not free: `.cache()` trades **memory** for
> avoided **recompute**. Cache the wrong thing (a frame used once), at the wrong storage level
> (a `MEMORY_ONLY` frame too big to fit), or forget to `unpersist()` — and a cache that was
> supposed to *speed you up* either does nothing, thrashes (evict → recompute → evict), or
> pins memory that more useful data needed. This module shows when caching helps, how to pick a
> storage level, and why you always release a cache when you're done.

- **Notebook:** [`spk8_caching.ipynb`](./spk8_caching.ipynb)
- **Toolkit used:** `common.datagen` (`uniform_keys` / `wide_rows` — big lazy frames), `common.profiles` (the tuned safety nets), `common.metrics_diff` (`measure()` repeated-access runtime + `compare()` — proves it), plus `pyspark.StorageLevel`.
- **Run against:** the unified Spark server (`make up`) — open the Spark UI at http://localhost:4040 while the notebook runs, and watch the **Storage** tab.
- **Time:** ~12 min. **Laptop-safe:** ~10–20M rows are generated *lazily* and only `count()`-ed (never collected or written), so nothing fills disk; every cached frame is `unpersist()`-ed at the end and `spark.catalog.clearCache()` releases the rest. Runs fine on the default **tuned** box — you do **not** need `make up-constrained`.

> **This is a memory-vs-recompute tradeoff module, not a container-OOM one.** We never try to
> crash the box; we make the *cost* of caching visible. (Container-memory failures are `SPK-2`
> executor OOM / `SPK-3` driver OOM, where over-caching is one contributing cause.)

---

## 1. The scenario

A pipeline derives an expensive intermediate frame — a wide projection over a large generated
fact, plus a per-key aggregation — and then reuses it several times: a `count`, a couple of
filtered summaries, a join. Someone noticed it recomputes the whole lineage on *every* reuse and
sprinkled `.cache()` calls "to make it fast". Some helped. One was on a frame touched exactly
once (pure overhead). One cached a frame too big for memory at `MEMORY_ONLY` (it thrashed — GC
and recompute, no faster). And nothing ever got `unpersist()`-ed, so the caches sat pinning
memory long after the frames were needed, crowding out the caches that mattered.

The fix isn't "cache more" or "cache less" — it's **cache selectively, at the right storage
level, and release it when done.**

## 2. Break it — four caching pathologies

All on the default **tuned** profile (`common.profiles.apply_profile`); the lessons are about
DataFrame caching, not the session safety nets. We demonstrate four behaviors with
`df.cache()`, `df.persist(StorageLevel.X)`, and `df.unpersist()` — all **Spark Connect-safe**:

1. **Lazy cache.** `df.cache()` does *nothing* until an action. The **first** action still
   computes the full lineage *and* populates the cache; **subsequent** actions read the cache and
   are fast. We `measure()` first-access vs second-access runtime.
2. **Reuse speedup.** The expensive derived frame is reused several times. Uncached it recomputes
   the whole lineage each time; cached it computes once and replays. We `compare()` the **total**
   across reuses, uncached vs cached.
3. **Storage levels.** The same frame under `MEMORY_ONLY` vs `MEMORY_AND_DISK` vs `DISK_ONLY`
   (`persist(StorageLevel.X)`). When a `MEMORY_ONLY` frame doesn't fully fit, partitions are
   **evicted and recomputed on every access** — GC / recompute thrash. `MEMORY_AND_DISK` spills
   the overflow to disk instead of recomputing it.
4. **Forgetting `unpersist()`.** Cached frames pin memory whether or not you still need them, and
   can evict more useful caches. `unpersist()` releases that memory immediately.

## 3. Detect it — read the Spark UI

Open http://localhost:4040 → the **Storage** tab (see
[`docs/spark-ui-guide.md`](../../docs/spark-ui-guide.md) → *Storage*). The tells:

| Signal | Healthy cache | Broken cache |
|--------|---------------|--------------|
| **Fraction Cached** | **100%** — the whole frame is resident | **< 100%** → partitions are being **evicted and recomputed on every access** (cache thrash) — the primary `SPK-8` signal |
| **Storage Level** | the level you asked for (`Memory Deserialized 1×`, `Memory and Disk`, `Disk Serialized`) | not what you set → a different memory/CPU tradeoff than you intended |
| **Size in Memory vs Size on Disk** | fits in memory | a large **on-disk** portion → memory overflowed (expected for `MEMORY_AND_DISK`; a recompute cost for `MEMORY_ONLY`) |
| **Empty Storage tab when you expected a cache** | the frame appears after its first action | nothing listed → `.cache()` is **lazy**; no action has materialized it yet (the classic "why is it still slow?" gotcha) |

Cross-check the **Executors** tab: **Storage Memory** pinned near its limit means cache is
crowding out execution memory — the link to forgetting `unpersist()` (and to `SPK-2`).

The **Prove** step turns this into numbers: `common.metrics_diff.measure()` captures the
**repeated-access runtime** the Storage tab's eviction state is causing.

## 4. Diagnose

`.cache()` / `.persist()` mark a frame to be kept after its first materialization; the cache
manager stores its partitions at the chosen storage level and reuses them instead of replaying
the lineage. Two things make it a *cost*, not a free win:

- **It only pays off on reuse.** Caching a frame used once just adds the bookkeeping and the
  memory footprint with nothing to amortize it against — pure overhead.
- **Memory is finite.** `MEMORY_ONLY` keeps deserialized partitions in execution/storage memory;
  if they don't all fit, the surplus is **evicted and recomputed on the next access** (thrash) —
  it can be *slower* than not caching. `MEMORY_AND_DISK` trades that recompute for a disk read;
  `DISK_ONLY` always reads from disk (cheap memory, slower than RAM). And a cache that's never
  `unpersist()`-ed keeps pinning memory after it's useful, evicting caches that still matter.

## 5. Fix it — cache deliberately

| Fix | How | When |
|-----|-----|------|
| **Cache only reused frames** | `.cache()` a frame *before* the first of several actions; skip it for single-use frames | The frame is read **2+ times** and recomputing its lineage is expensive. |
| **Choose the storage level by size & reuse** | `persist(StorageLevel.MEMORY_AND_DISK)` for frames bigger than memory; `MEMORY_ONLY` only when it comfortably fits; `DISK_ONLY` when memory is scarce | Avoids `MEMORY_ONLY` thrash on a frame that doesn't fit. `MEMORY_AND_DISK` is the safe default for "big and reused". |
| **Always `unpersist()` when done** | `df.unpersist()` as soon as the reuse window closes; `spark.catalog.clearCache()` to drop everything | Releases memory so later caches (and execution) aren't evicted. |

## 6. Prove it

`common.metrics_diff.compare([...])` prints a before/after table.

- **Lazy cache:** first access ≈ uncached compute; second access **much faster** (cache hit).
- **Reuse speedup:** `compare([uncached_total, cached_total])` — the cached total is **much
  lower** because the lineage runs once instead of N times. The cost: the **memory** the cache
  holds (visible as Size in Memory in the Storage tab).
- **Storage levels:** `MEMORY_AND_DISK` / `DISK_ONLY` avoid the `MEMORY_ONLY` recompute when the
  frame doesn't fit — steadier repeated-access runtime, at the price of disk reads.

Repeated-access runtime collapsing once the frame is cached (and the Storage tab showing it
resident) is the proof — together with the reminder that the win was *bought* with memory.

## 7. Takeaways & "in real production…"

- **Cache is a tradeoff, not a speedup button:** it spends memory to avoid recompute, and only
  pays off when a frame is **reused multiple times**.
- **Pick the storage level by size and reuse:** `MEMORY_ONLY` only if it fits; `MEMORY_AND_DISK`
  for big reused frames (avoids thrash); `DISK_ONLY` when memory is tight.
- **Always `unpersist()`** when the reuse window closes — a forgotten cache evicts caches that
  still matter and feeds executor memory pressure (`SPK-2`).
- **Detect** via the **Storage** tab: Fraction Cached < 100% = eviction/recompute thrash; an
  empty tab after `.cache()` = it's lazy, no action has run yet.
- **In production:** cache only well-reused intermediates, default to `MEMORY_AND_DISK` for large
  ones, scope caches to the job that uses them and release them, and watch Storage Memory on the
  Executors tab so caching doesn't starve execution.

## 8. Teardown

The notebook `unpersist()`-es every frame it cached and calls `spark.catalog.clearCache()`, then
restores the `tuned` profile. Nothing was written (we only counted generated data), so there is
nothing else to delete; `make clean` clears `.tmp/` if you experimented with writes.
