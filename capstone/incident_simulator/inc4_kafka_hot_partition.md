# INC-4 — One Kafka partition keeps falling behind ⛑️

> **Page:** P2: consumer-group lag alarm on the `orders` topic. Average throughput looks fine, but the lag graph for the group climbs without bound — and it's all coming from **one partition** while the rest sit at zero.
> **Handed to you:** the consumer group + the topic. kafka-ui at http://localhost:8080 (topic → Partitions / Consumers); `common.kafka_helpers.consumer_group_lag` / `topic_end_offsets`. Diagnose before changing code.

## Symptom
- **Per-partition lag is wildly uneven:** partition 1 lag grows unbounded; partitions 0 and 2 sit near zero.
- One consumer instance is **pinned at ~100% CPU** and steadily behind; its peers in the group are nearly **idle**.
- The on-call's instinct — **add more consumers** — did nothing. The new instances pick up the already-drained partitions; the hot one still has exactly one owner.
- Producer rate is steady and the broker is healthy. No errors, no rebalance storm. Just one lane backed up.

## Your job (think like an SRE)
1. **Where do you look first?** Aggregate throughput hides the imbalance — look **per partition**, not at the group total. Which kafka-ui view (or which helper) shows you that one partition's end offset is racing ahead of the others?
2. **What measurement confirms the root cause vs. the alternatives?** Is this a slow consumer instance (bad node), an under-provisioned group, or a genuinely lopsided workload? What single ratio distinguishes "one partition gets all the traffic" from "all consumers are uniformly slow"?
3. **What's the fix — and what number proves it worked?**

<details>
<summary>🔧 Diagnosis &amp; fix — open only after you've formed a hypothesis</summary>

- **Root cause:** **Hot partition from a lopsided key.** The producer picks a partition by hashing the message key: `partition = hash(key) % num_partitions`. A **low-cardinality / dominant / `null`** key (e.g. 90% of orders share one `country` or `tenant`, or the key is constant/null) collapses most of the traffic onto **one partition**. A partition has exactly **one consumer in a group**, so that single consumer carries ~90% of the load while its peers starve. Crucially, you **cannot fix this by adding partitions** — the hot key still hashes to a single partition — nor by adding consumers, because the work is pinned to one partition that only one instance can own. This is the streaming cousin of SPK-1 data skew.

- **Detect:** look at the **per-partition** offsets and lag, not the average:
  - **kafka-ui** → topic `orders` → **Partitions**: one partition's end offset is far ahead of the others. → **Consumers** → the group: that partition's lag is the only one climbing.
  - **`common.kafka_helpers.topic_end_offsets(topic)`** / **`consumer_group_lag(group, topic)`**: reduce to **skew ratio = max ÷ min** across partitions. A hot partition shows **skew ≫ 1** (tens-of-×); the lagging partition's consumer does ~all the work. A *uniformly* slow group (bad node / under-provisioned) would push **all** partitions' lag up together and skew would stay near **1×** — skew blows out only when the key is lopsided.

- **Fix:** spread the key, don't add capacity:
  - **Rekey to a high-cardinality key** aligned to how you consume — e.g. a per-customer id (`cust-{i % 300}`) instead of `country`. Recreate the topic so the hot run's offsets don't carry over.
  - If one key is **legitimately** hot, **salt** it (`key + "-" + rand(0..N)`) and **merge downstream** — the same trick as SPK-1 salting.
  - Remember the trade-off: partition key = parallelism **and** ordering, and ordering holds only **within** a partition — design for that.

- **Prove:** `topic_end_offsets` before vs after the rekey, reduced to the **skew ratio**. Broken (dominant key `"HOT"`): one fat partition, two tiny → skew **~28×**. Fixed (high-cardinality key): ~even across all three → skew **~1×**. The ratio collapsing from tens-of-× toward ~1× — even partitions → even consumer load → no single laggard — is the proof, and the consumer-group lag then **drains** because every instance finally has work.

- **Reproduce &amp; learn it:** [KAF-1](../../kafka/partitioning/) — produce a dominant-key topic and watch one partition's offset race ahead, then rekey and watch the skew ratio collapse. Pair it with [KAF-2](../../kafka/consumer_lag/) for reading per-partition lag (`consumer_group_lag`) as the health signal.
</details>
