"""
kafka_helpers (Phase 3) — producers + topic admin for the Kafka / Structured Streaming track.

Topology reminder (see docker-compose.yml): the broker advertises two listeners.
    • Notebooks/producers run on the HOST   → use the EXTERNAL listener `localhost:29092`.
    • Spark runs INSIDE the Docker network   → reads via the INTERNAL listener `kafka:9092`.
So: produce / inspect offsets from the notebook with ``BOOTSTRAP`` (host); point Spark's
``readStream`` at ``SPARK_BOOTSTRAP`` (``kafka:9092``).

Laptop-safe streaming pattern these modules use:
    produce a BOUNDED batch, then read with ``.trigger(availableNow=True)`` — Spark consumes all
    available data and **stops on its own** (no infinite stream pinning the laptop). Checkpoints
    go under ``.tmp/`` so ``make clean`` recovers.
"""

from __future__ import annotations

import json
import os
import time

from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import NewTopic

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")  # host / external (producers, admin)
SPARK_BOOTSTRAP = "kafka:9092"                                           # internal (Spark readStream/writeStream)


def ensure_topic(topic: str, num_partitions: int = 1, configs: dict | None = None,
                 recreate: bool = True) -> None:
    """Create ``topic`` with ``num_partitions`` (replication 1). If ``recreate``, delete first
    so partition count / configs are deterministic. ``configs`` sets topic props, e.g.
    ``{"retention.ms": "60000"}`` or ``{"cleanup.policy": "compact"}``."""
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    try:
        if recreate:
            try:
                admin.delete_topics([topic])
                time.sleep(1.0)  # deletion is async
            except Exception:  # noqa: BLE001 — topic may not exist
                pass
        for attempt in range(5):
            try:
                admin.create_topics([NewTopic(topic, num_partitions=num_partitions,
                                              replication_factor=1, topic_configs=configs or {})])
                break
            except Exception:  # noqa: BLE001 — pending deletion / already exists; retry
                time.sleep(1.0)
    finally:
        admin.close()


def produce_events(topic: str, n: int, value_fn=None, key_fn=None, flush: bool = True) -> int:
    """Publish ``n`` JSON events to ``topic``. ``value_fn(i)->dict`` builds each value (default
    ``{"id": i, "v": i*1.0}``); ``key_fn(i)`` sets the partition key (drives partitioning — e.g.
    a constant key creates a hot partition). Returns ``n``."""
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: None if k is None else str(k).encode("utf-8"),
    )
    for i in range(n):
        value = value_fn(i) if value_fn else {"id": i, "v": i * 1.0}
        key = key_fn(i) if key_fn else None
        producer.send(topic, key=key, value=value)
    if flush:
        producer.flush()
    producer.close()
    return n


def topic_end_offsets(topic: str) -> dict[int, int]:
    """Latest offset per partition (sum ≈ messages produced). Useful for lag / hot-partition demos."""
    consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP, consumer_timeout_ms=3000)
    try:
        parts = consumer.partitions_for_topic(topic) or set()
        tps = [TopicPartition(topic, p) for p in parts]
        consumer.assign(tps)
        end = consumer.end_offsets(tps)
        return {tp.partition: int(off) for tp, off in end.items()}
    finally:
        consumer.close()


def consumer_group_lag(group_id: str, topic: str) -> dict[int, dict]:
    """Per-partition committed offset, end offset, and lag for a consumer group.
    The headline metric for KAF-2 (consumer lag). Returns ``{partition: {committed, end, lag}}``."""
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP, consumer_timeout_ms=3000)
    try:
        parts = consumer.partitions_for_topic(topic) or set()
        tps = [TopicPartition(topic, p) for p in parts]
        consumer.assign(tps)
        end = consumer.end_offsets(tps)
        committed = admin.list_consumer_group_offsets(group_id)
        out = {}
        for tp in tps:
            com = committed.get(tp).offset if tp in committed and committed.get(tp) else 0
            e = int(end[tp])
            out[tp.partition] = {"committed": int(com), "end": e, "lag": max(0, e - int(com))}
        return out
    finally:
        consumer.close()
        admin.close()


def delete_topic(topic: str) -> None:
    """Teardown: delete a topic (ignore if absent)."""
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    try:
        admin.delete_topics([topic])
    except Exception:  # noqa: BLE001
        pass
    finally:
        admin.close()
