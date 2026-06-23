# SPK-10 — (Deep) Spark internals sampler

> **The senior-engineer internals tour.** Not a single pathology like the rest of Phase 1 —
> a **short, demo-driven sampler** of the machinery *underneath* the failures you've already
> broken and fixed: how Catalyst rewrites your query, how Tungsten turns a stage into JVM
> bytecode, how memory is split between execution and storage, and three cluster-launch knobs
> (serialization, speculation, broadcast) that you tune but rarely *see*.
>
> Where SPK-1…SPK-9 follow **Break → Detect → Fix → Prove**, this module follows
> **Show → Read → Note**: each item is 1–2 cells, an `explain()` (or `spark.conf`) read, and a
> short *"what you just saw"* — plus an **honest line on whether it's observable over Spark
> Connect at all.**

- **Notebook:** [`spk10_internals.ipynb`](./spk10_internals.ipynb)
- **Toolkit used:** `common.profiles` (flip codegen-relevant knobs), `common.datagen`
  (a small fact + dimension to plan over), `common.metrics_diff` (the one place a `compare()`
  earns its keep: codegen on vs off).
- **Run against:** the unified Spark server (`make up`) — the default **tuned** box is fine.
  Open the Spark UI at http://localhost:4040 if you want to see the **SQL / DataFrame** plan
  rendered, but the workhorse here is `df.explain(...)` printed inline.
- **Time:** ~12 min. **Laptop-safe:** this module is about **plans, not volume** — data is
  generated lazily and only `count()`-ed (never collected or written). Nothing to delete.

---

## The big idea: Connect shows you the *plan*, the JVM owns the *internals*

This is the most important framing in the module, so it leads. Notebooks here talk to Spark over
**Spark Connect** (gRPC, `:15002`). Over Connect you have a **logical/physical-plan client**, not a
JVM handle:

- ✅ **Observable over Connect** — anything that lives in the **query plan**:
  `df.explain(extended=True)` (the four Catalyst plans), `df.explain(mode="formatted")` and
  `df.explain("codegen")` (Tungsten codegen + `WholeStageCodegen`), and any SQL conf via
  `spark.conf.get(...)`. These `explain()` calls **are the artifact** of this module.
- ❌ **NOT observable over Connect** — anything that needs the JVM driver:
  `spark.sparkContext`, **RDDs**, **accumulators**, and low-level **broadcast *variables***
  (`sc.broadcast(...)`). These raise on Connect. Where a concept only lives there, this module
  **describes it and shows the DataFrame-level analogue** instead of faking a JVM poke.

So the deliverable for half these items is a printed plan; for the other half it's a config value
plus prose. That split is itself a senior-level lesson: *you tune many internals you will never
directly observe from a Connect client.*

---

## The sampler — six internals

| # | Internal | What the demo shows | Observable over Connect? |
|---|----------|--------------------|--------------------------|
| 1 | **WholeStageCodegen** (Tungsten) | `explain("codegen")` / `mode="formatted"` — fused operators under a `WholeStageCodegen` node, `*` markers, generated JVM source | ✅ via the plan |
| 2 | **Catalyst optimizer** | `explain(extended=True)` — Parsed → Analyzed → Optimized → Physical; **constant folding** (`lit(2)*lit(3)` → `6`) and **column pruning** visible between Analyzed and Optimized | ✅ via the plan |
| 3 | **Tungsten / unified memory** | `spark.memory.fraction` + `storageFraction` read via `spark.conf`; execution vs storage tug-of-war; off-heap conceptually | ⚠️ confs yes; live memory split no (JVM) |
| 4 | **Serialization: Kryo vs Java** | `spark.conf.get("spark.serializer")`; why Kryo is smaller/faster for shuffle & cache | ⚠️ conf yes; it's a **launch** setting |
| 5 | **Speculative execution** | `spark.conf.get("spark.speculation")`; relaunching stragglers — and why it **can't fix SPK-1 skew** | ⚠️ conf yes; relaunch happens cluster-side |
| 6 | **Broadcast: variable vs join** | `sc.broadcast` is JVM-only (not in Connect) → describe it; show the DataFrame-native analogue `F.broadcast(df)` in a plan (ties to SPK-5) | ✅ the join analogue via the plan; ❌ the variable |

Each maps to a tab in the [Spark-UI guide](../../docs/spark-ui-guide.md): items 1–2 and 6 are read
in **SQL / DataFrame** (the physical-plan DAG); item 3 in **Environment**
(`spark.memory.fraction`); items 4–5 in **Environment** as well (`spark.serializer`,
`spark.speculation`).

---

## Show → Read → Note (what each item teaches)

**(1) WholeStageCodegen.** A simple chained query (`filter → withColumn → groupBy → agg`) is shown
with `explain("codegen")`. **Read:** operators collapse into one `WholeStageCodegen (N)` node and
carry a `*` prefix in the tree; the *Generated code* section is real Java that Tungsten compiles to
bytecode at runtime. **Note:** instead of an interpreter calling `next()` per row per operator
(the "Volcano" model), Tungsten fuses a whole stage into one tight loop over the data — fewer
virtual calls, better CPU/cache behavior. The `*` is your at-a-glance "this stage is codegen'd"
marker in any plan.

**(2) Catalyst optimizer.** `explain(extended=True)` prints all four plans. **Read:** `2 * 3` in a
projection is already **folded to `6`** by the Optimized plan (constant folding); selecting two
columns from a five-column frame shows the other three **pruned away** before the scan
(column pruning / pushed projection). **Note:** Catalyst is a **rule-based + cost-based** rewriter —
your DataFrame/SQL is a *declaration*; the Optimized plan is what Spark actually intends to run. The
gap between Analyzed and Optimized is the optimizer's work made visible.

**(3) Tungsten / unified memory model.** No JVM poking — confs + prose. **Read:**
`spark.memory.fraction` (default ~0.6 — the unified region for execution **and** storage) and
`spark.memory.storageFraction` (default ~0.5 — the slice of that region storage is *guaranteed*
before it must yield to execution). **Note:** execution memory (shuffle/sort/agg buffers) and
storage memory (cache) share **one** pool and borrow from each other; execution can evict cache,
not vice-versa. This is the model behind **SPK-2** (executor OOM) and **SPK-8** (cache thrash).
Off-heap (`spark.memory.offHeap.*`) moves buffers outside the JVM heap to dodge GC — same idea,
different address space. The *live* split is a JVM/Executors-tab thing, not a Connect read.

**(4) Serialization: Kryo vs Java.** **Read:** `spark.conf.get("spark.serializer")` — what's
actually in effect. **Note:** Java's default serializer is portable but **bulky and slow**; Kryo
(`org.apache.spark.serializer.KryoSerializer`) produces **smaller, faster** payloads — which matters
exactly when bytes move or rest: **shuffle** and **cached** (`MEMORY_*_SER`) data. Catch: it's a
**cluster-launch** setting (`--conf spark.serializer=...` at submit), not a reliable mid-session
flip from a Connect client — so we **read** it, we don't toggle it. (DataFrame columnar/Tungsten
encoding sidesteps this for built-in types; it bites most with RDDs / arbitrary objects.)

**(5) Speculative execution.** **Read:** `spark.conf.get("spark.speculation")` (off by default).
**Note:** when on, Spark relaunches **straggler** tasks (much slower than their stage peers) on
another executor and takes whichever finishes first — great for **heterogeneous/flaky nodes** (a
slow disk, a noisy neighbor). The senior catch: **it cannot fix data skew (SPK-1).** A speculative
copy of the hot-key task gets the *same* giant partition and is equally slow — you just did the
work twice. Speculation fixes *unlucky placement*, not *unbalanced data*.

**(6) Broadcast — variable misuse vs broadcast join.** Two different things share the word
"broadcast". **Read/Note (the variable):** a low-level **broadcast *variable*** (`sc.broadcast(x)`)
ships a read-only value to every executor once instead of per-task — a classic optimization for a
lookup map used inside an RDD closure. It lives on `sparkContext`, so it's **JVM-only and raises on
Spark Connect**; we describe it, we don't call it. **Show (the analogue):** the DataFrame-native
equivalent for the common "small lookup table" case is a **broadcast join**, `df.join(F.broadcast(dim), …)`,
which appears as **`BroadcastHashJoin`** in the plan — no shuffle of the big side. That's the same
mechanism SPK-5 (join strategies) lives in, and one of SPK-1's skew fixes.

---

## Prove it (the one measurable item)

Most of this module's "proof" is the printed plan — the artifact *is* the evidence. The one place a
quantitative **before/after** helps is **WholeStageCodegen on vs off**: toggle
`spark.sql.codegen.wholeStage` and `compare()` two runs of the same chained aggregation with
`common.metrics_diff`. On tiny laptop data the wall-clock delta is small and noisy (codegen's win
grows with row count and CPU-bound work) — so treat the number as *directional*, and let the
**plan difference** (one fused `WholeStageCodegen` node vs separate interpreted operators) be the
real takeaway. This honesty is the point: not every internal moves a laptop-scale number.

---

## Takeaways & "in real production…"

- **Two of these you can *see* from a Connect notebook** (Catalyst plans, Tungsten codegen, and the
  broadcast-*join* plan) — they live in the **query plan**, so `df.explain(...)` is your window.
- **The rest you *tune* but don't directly observe from Connect** (unified-memory split,
  Kryo, speculation, broadcast *variables*) — they're JVM-/launch-/cluster-side. Knowing *which
  bucket* a knob is in is itself the senior skill: don't waste time trying to flip a launch setting
  at runtime, and don't expect a Connect client to expose a live executor-memory number.
- **`explain()` is the cheapest, most underused debugging tool in Spark.** `extended=True` to see
  what the optimizer did, `"codegen"`/`mode="formatted"` to see what Tungsten will run, and the
  join-operator name to confirm a broadcast actually happened (SPK-5).
- **Internals connect the dots across Phase 1:** unified memory → executor OOM (SPK-2) & cache
  thrash (SPK-8); codegen/Catalyst → why a `CAST`/UDF kills pruning (SPK-7); speculation vs skew →
  why more retries don't save SPK-1; broadcast → the first skew/join fix (SPK-1, SPK-5).
- **In production:** keep AQE on, set serialization at submit, enable speculation only where node
  heterogeneity (not skew) is the problem, and reach for `explain()` *before* you reach for more
  executors.

## Teardown

Nothing was written — every demo generated data lazily and only `count()`-ed or `explain()`-ed it,
so there are no tables or files to remove. The notebook restores the production-tuned safety nets at
the end. If you experimented with writes, `make clean` clears everything under `.tmp/`.
