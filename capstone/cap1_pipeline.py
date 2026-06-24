"""CAP-1 — End-to-end capstone pipeline (the whole stack working together).

    Postgres → Debezium → Kafka → Spark → Iceberg MERGE → Spark transform → Great Expectations gate

This is the **operational + analytics** half of the capstone, driven as discrete *stages* so an
Airflow DAG (`airflow/dags/cap1_e2e_pipeline.py`) can orchestrate them with real task boundaries,
retries, and a quality gate. Every stage reuses a verified building block from an earlier phase:
the CDC bring-up + envelope parse + LSN-deduped `MERGE` is CDC-7 (Phase 4); the Iceberg transform is
the lakehouse track (Phase 2); the Great Expectations gate is DBT-8 (Phase 5, Connect-safe via
`toPandas`). The DAG adds the dbt-marts + dbt-test stages (Phase 5) on top.

Run a single stage (what the DAG does, one BashOperator per stage):
    PYTHONPATH=<repo> uv run python capstone/cap1_pipeline.py ingest
    PYTHONPATH=<repo> uv run python capstone/cap1_pipeline.py transform
    PYTHONPATH=<repo> uv run python capstone/cap1_pipeline.py quality
    PYTHONPATH=<repo> uv run python capstone/cap1_pipeline.py cleanup

Prerequisites: `make up` (Spark + Kafka) and `make cdc-up` (Postgres + Kafka Connect). Laptop-safe:
a ~12-row source, bounded reads, every object dropped in `cleanup`. Exit code is non-zero on a
failed quality gate so Airflow marks the task (and the DAG) failed.
"""

from __future__ import annotations

import sys
import time

from pyspark.sql import functions as F, Window
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

from common import cdc_helpers as cdc
from common.kafka_helpers import SPARK_BOOTSTRAP, topic_end_offsets
from common.spark_session import get_spark

NAME, TABLE = "cap1-orders", "cap1_orders"
TOPIC = cdc.topic_name(TABLE)
MIRROR = "iceberg_catalog.default.cap1_orders_mirror"
MART = "iceberg_catalog.default.cap1_orders_by_status"

# Debezium envelope (decimal.handling.mode=double, schemas off) — same shape verified in CDC-7.
_AFTER = StructType([
    StructField("id", LongType()), StructField("customer", StringType()),
    StructField("amount", DoubleType()), StructField("status", StringType()),
    StructField("updated", LongType()),
])
_ENV = StructType([
    StructField("before", _AFTER), StructField("after", _AFTER),
    StructField("op", StringType()), StructField("ts_ms", LongType()),
    StructField("source", StructType([StructField("lsn", LongType()), StructField("ts_ms", LongType())])),
])


def ingest() -> None:
    """Stage 1 — CDC the source into an Iceberg mirror (Postgres→Debezium→Kafka→Spark→Iceberg MERGE)."""
    spark = get_spark()
    cdc.teardown(NAME, TABLE)
    cdc.seed_orders(TABLE, n=12)
    cdc.register_connector(NAME, cdc.debezium_pg_config(NAME, TABLE, snapshot_mode="always"))
    print("connector:", cdc.wait_for_connector(NAME, timeout=60))
    for _ in range(20):
        try:
            off = topic_end_offsets(TOPIC)
        except Exception:  # noqa: BLE001
            off = {}
        if isinstance(off, dict) and sum(off.values()) >= 12:
            break
        time.sleep(2)
    # a few live changes so the pipeline exercises update + delete + insert, not just the snapshot
    cdc.pg_exec(f"UPDATE public.{TABLE} SET status='PAID', amount=777.0 WHERE id=1")
    cdc.pg_exec(f"DELETE FROM public.{TABLE} WHERE id=2")
    cdc.pg_exec(f"INSERT INTO public.{TABLE}(id,customer,amount,status) VALUES (500,'capstone',55.5,'NEW')")
    time.sleep(6)

    raw = (spark.read.format("kafka").option("kafka.bootstrap.servers", SPARK_BOOTSTRAP)
           .option("subscribe", TOPIC).option("startingOffsets", "earliest").load())
    evt = raw.select(F.from_json(F.col("value").cast("string"), _ENV).alias("e")).select("e.*")
    changes = (evt.filter(F.col("op").isNotNull())
               .select("op", F.col("source.lsn").alias("lsn"),
                       F.coalesce("after.id", "before.id").alias("id"),
                       F.col("after.customer").alias("customer"), F.col("after.amount").alias("amount"),
                       F.col("after.status").alias("status"), F.col("after.updated").alias("updated")))
    w = Window.partitionBy("id").orderBy(F.col("lsn").desc_nulls_last())
    latest = changes.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")

    spark.sql(f"DROP TABLE IF EXISTS {MIRROR}")
    spark.sql(f"CREATE TABLE {MIRROR} (id BIGINT, customer STRING, amount DOUBLE, status STRING, updated BIGINT) USING iceberg")
    latest.createOrReplaceTempView("cap1_changes")
    spark.sql(f"""MERGE INTO {MIRROR} t USING cap1_changes s ON t.id = s.id
        WHEN MATCHED AND s.op = 'd' THEN DELETE
        WHEN MATCHED THEN UPDATE SET t.customer=s.customer, t.amount=s.amount, t.status=s.status, t.updated=s.updated
        WHEN NOT MATCHED AND s.op <> 'd' THEN INSERT (id, customer, amount, status, updated)
                                          VALUES (s.id, s.customer, s.amount, s.status, s.updated)""")
    n = spark.sql(f"SELECT count(*) c FROM {MIRROR}").first()["c"]
    print(f"[ingest] Iceberg mirror {MIRROR} now holds {n} rows (CDC snapshot + live c/u/d, LSN-deduped)")


def transform() -> None:
    """Stage 2 — Spark transform: a small aggregate mart over the Iceberg mirror."""
    spark = get_spark()
    spark.sql(f"DROP TABLE IF EXISTS {MART}")
    spark.sql(f"""CREATE TABLE {MART} USING iceberg AS
        SELECT status, count(*) AS orders, round(sum(amount), 2) AS total_amount
        FROM {MIRROR} GROUP BY status""")
    rows = spark.sql(f"SELECT * FROM {MART} ORDER BY status").collect()
    print(f"[transform] built {MART}:")
    for r in rows:
        print(f"    {r['status']:<10} orders={r['orders']:<3} total_amount={r['total_amount']}")


def quality() -> None:
    """Stage 3 — Great Expectations gate on the mirror (Connect-safe via toPandas). Exit 1 on breach."""
    import great_expectations as gx
    spark = get_spark()
    pdf = spark.table(MIRROR).toPandas()
    print(f"[quality] validating {MIRROR}: {len(pdf)} rows")
    batch = (gx.get_context().data_sources.add_pandas("cap1")
             .add_dataframe_asset(name="mirror").add_batch_definition_whole_dataframe("b")
             .get_batch(batch_parameters={"dataframe": pdf}))
    suite = gx.ExpectationSuite(name="cap1_orders_quality")
    for exp in [
        gx.expectations.ExpectColumnValuesToNotBeNull(column="id"),
        gx.expectations.ExpectColumnValuesToBeUnique(column="id"),
        gx.expectations.ExpectColumnValuesToBeBetween(column="amount", min_value=0, max_value=1_000_000),
        gx.expectations.ExpectColumnValuesToBeInSet(column="status",
                                                    value_set=["NEW", "PAID", "completed", "refunded", "pending"]),
    ]:
        suite.add_expectation(exp)
    result = batch.validate(suite)
    print(f"[quality] GE success: {result.success}")
    for r in result.results:
        print(f"    {'PASS' if r.success else 'FAIL'}  {r.expectation_config.type}")
    if not result.success:
        sys.exit(1)  # fail the Airflow task → the DAG stops before promoting bad data


def cleanup() -> None:
    """Stage 4 — drop the Iceberg objects and tear down CDC (connector, slot, table, topic)."""
    spark = get_spark()
    spark.sql(f"DROP TABLE IF EXISTS {MART}")
    spark.sql(f"DROP TABLE IF EXISTS {MIRROR}")
    cdc.teardown(NAME, TABLE)
    print("[cleanup] dropped Iceberg mart + mirror; CDC connector/slot/table/topic torn down")


_STAGES = {"ingest": ingest, "transform": transform, "quality": quality, "cleanup": cleanup}

if __name__ == "__main__":
    stage = sys.argv[1] if len(sys.argv) > 1 else ""
    if stage not in _STAGES:
        print(f"usage: cap1_pipeline.py [{'|'.join(_STAGES)}]")
        sys.exit(2)
    _STAGES[stage]()
