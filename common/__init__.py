"""Shared toolkit for the spark-dev production-challenges curriculum.

Every challenge module reuses these helpers so the "break it safely &
measure it" loop is consistent across tracks.

Modules
-------
spark_session
    Spark Connect session factory + scrollable display helper for notebooks.
profiles
    ``constrained`` vs ``tuned`` *session* profiles (the safety-net switcher:
    AQE, skew-join, broadcast threshold, shuffle partitions). The *container*
    memory profile is flipped separately via ``make up`` / ``make up-constrained``.
datagen
    ``spark.range()``-based synthetic data generators (uniform / skewed / wide /
    high-cardinality) — generate billions of logical rows without storing them.
metrics_diff
    Capture stage/query metrics (runtime, shuffle, spill, task-time skew) and
    print a before/after comparison table. Makes every fix *quantitative*.
"""
