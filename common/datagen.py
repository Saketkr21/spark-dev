"""
Synthetic data generator (F-2) — the core "generate, don't store" trick.

We can't keep terabytes on a laptop, so we synthesize huge *logical* datasets on
the fly with ``spark.range()`` + deterministic hashing. Nothing hits disk until an
action runs, but the engine still does the real shuffle / spill / skew work.

Every generator returns a **lazy** DataFrame — calling it is free; cost is paid
only on an action (``.count()``, a join, a write). Skew is reproducible across runs
(deterministic ``hash()`` on the row id), so a module's before/after numbers are stable.

Generators
----------
uniform_keys          fact rows whose key is spread evenly over ``n_keys`` (the baseline)
skewed_keys           fact rows where ``hot_key_fraction`` of rows share ONE key (the SPK-1 pathology)
key_dimension         a dimension covering keys ``0..n_keys-1`` to join a fact against
wide_rows             many-column rows (stresses serialization / memory)
high_cardinality_keys keys that are ~unique (stresses aggregation / shuffle planning)

Laptop safety: these describe billions of rows cheaply, but YOU choose the row count
per module. Prefer aggregations / counts over ``.collect()`` so results stay small.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def uniform_keys(
    spark: SparkSession,
    n_rows: int,
    n_keys: int = 1_000,
    key_col: str = "key",
    num_partitions: int | None = None,
) -> DataFrame:
    """Fact rows whose join/group key is spread **evenly** over ``n_keys`` distinct keys.

    This is the well-behaved baseline to contrast against :func:`skewed_keys`:
    every key gets ~``n_rows / n_keys`` rows, so a shuffle by ``key`` produces
    balanced partitions (task times cluster around the median).

    Columns: ``row_id`` (long), ``<key_col>`` (long, 0..n_keys-1), ``amount`` (double).
    """
    base = spark.range(0, n_rows, numPartitions=num_partitions).withColumnRenamed("id", "row_id")
    return (
        base
        .withColumn(key_col, F.pmod(F.col("row_id"), F.lit(n_keys)))
        .withColumn("amount", (F.rand(seed=17) * 100.0))
    )


def skewed_keys(
    spark: SparkSession,
    n_rows: int,
    hot_key_fraction: float = 0.9,
    n_cold_keys: int = 1_000,
    hot_key: int = 0,
    key_col: str = "key",
    num_partitions: int | None = None,
) -> DataFrame:
    """Fact rows where ``hot_key_fraction`` of all rows carry a single ``hot_key`` —
    the engineered pathology behind **SPK-1 (data/partition skew)**.

    A shuffle by ``key`` (group-by or sort-merge join) sends every hot-key row to the
    same reduce partition, so one task does ~90% of the work → a straggler you can see
    in the Spark UI as **task-time max ≫ median**. The remaining rows spread evenly over
    cold keys ``1..n_cold_keys``.

    Which rows are hot is decided by a deterministic hash of ``row_id`` (not the first
    90% sequentially), so the skew is realistic *and* identical on every run.

    Args:
        hot_key_fraction: share of rows assigned to ``hot_key`` (0.0–1.0, e.g. 0.9 = 90%).
        n_cold_keys: number of distinct non-hot keys the rest spread across.
        hot_key: the value of the dominant key (kept inside the dimension's key range).

    Columns: ``row_id`` (long), ``<key_col>`` (long), ``amount`` (double).
    """
    if not 0.0 < hot_key_fraction < 1.0:
        raise ValueError("hot_key_fraction must be strictly between 0 and 1 (e.g. 0.9).")

    base = spark.range(0, n_rows, numPartitions=num_partitions).withColumnRenamed("id", "row_id")

    hot_pct = int(round(hot_key_fraction * 100))
    # bucket in 0..99 from a hash of row_id; buckets < hot_pct → hot key.
    bucket = F.pmod(F.hash(F.col("row_id")), F.lit(100))
    # cold key in 1..n_cold_keys, uniform over row_id (row_id is sequential → even spread).
    cold_key = (F.pmod(F.col("row_id"), F.lit(n_cold_keys)) + F.lit(1))
    key = F.when(bucket < F.lit(hot_pct), F.lit(hot_key)).otherwise(cold_key)

    return (
        base
        .withColumn(key_col, key.cast("long"))
        .withColumn("amount", (F.rand(seed=23) * 100.0))
    )


def key_dimension(
    spark: SparkSession,
    n_keys: int,
    key_col: str = "key",
) -> DataFrame:
    """A dimension covering keys ``0..n_keys-1`` to join a fact table against.

    Sized to span both the hot key (``0``) and the cold keys produced by
    :func:`skewed_keys`. It's small, so Spark would normally *broadcast* it — which is
    itself one valid skew fix. To force the sort-merge join that exposes skew (and that
    AQE skew-join / salting then repair), disable broadcasts with the ``constrained``
    profile (``spark.sql.autoBroadcastJoinThreshold=-1``).

    Columns: ``<key_col>`` (long), ``key_label`` (string), ``tier`` (string).
    """
    return (
        spark.range(0, n_keys)
        .withColumnRenamed("id", key_col)
        .withColumn("key_label", F.concat(F.lit("key_"), F.col(key_col).cast("string")))
        .withColumn("tier", F.element_at(
            F.array(F.lit("bronze"), F.lit("silver"), F.lit("gold")),
            (F.pmod(F.col(key_col), F.lit(3)) + F.lit(1)).cast("int"),
        ))
    )


def wide_rows(
    spark: SparkSession,
    n_rows: int,
    n_cols: int = 50,
    num_partitions: int | None = None,
) -> DataFrame:
    """Rows with many columns — stresses serialization, shuffle payload size, and memory.

    Useful for spill / OOM / serialization modules: the same row count costs far more
    bytes per row than a narrow table. Columns: ``row_id`` plus ``c0..c{n_cols-1}`` doubles.
    """
    df = spark.range(0, n_rows, numPartitions=num_partitions).withColumnRenamed("id", "row_id")
    for i in range(n_cols):
        df = df.withColumn(f"c{i}", F.rand(seed=1000 + i) * 100.0)
    return df


def high_cardinality_keys(
    spark: SparkSession,
    n_rows: int,
    n_distinct: int | None = None,
    key_col: str = "key",
    num_partitions: int | None = None,
) -> DataFrame:
    """Fact rows whose key is ~unique (``n_distinct`` defaults to ``n_rows``).

    The opposite failure mode from skew: a group-by/distinct produces a huge number of
    tiny groups, stressing shuffle planning and aggregation memory rather than a single
    partition. Columns: ``row_id`` (long), ``<key_col>`` (long), ``amount`` (double).
    """
    n_distinct = n_distinct or n_rows
    base = spark.range(0, n_rows, numPartitions=num_partitions).withColumnRenamed("id", "row_id")
    return (
        base
        .withColumn(key_col, F.pmod(F.hash(F.col("row_id")), F.lit(n_distinct)).cast("long"))
        .withColumn("amount", (F.rand(seed=29) * 100.0))
    )
