"""
Resource-profile switcher — the *session* layer (F-1).

The spark-dev curriculum reproduces big-data failure modes at small scale by
(1) shrinking the box and (2) toggling Spark's safety nets. There are two layers:

  • Container / box size  — `spark.driver.memory` + container `mem_limit`. Fixed when
    the Docker server boots, so it's flipped at startup, NOT here:
        make up              # tuned       (~3 GB box, driver 2g, all cores)
        make up-constrained  # constrained (~2 GB box, driver 1g, 2 cores)  ← OOM/spill modules
    A Spark Connect client cannot change the server's heap at runtime, so memory-bound
    pathologies (executor/driver OOM, heavy spill) need the constrained *box*.

  • Session safety nets   — AQE, skew-join, broadcast threshold, shuffle partitions.
    These ARE runtime SQL confs, settable per session over Spark Connect — that's THIS
    module. Most pathology modules (e.g. SPK-1 skew) force the broken behavior with the
    `constrained` profile, then relieve it with `tuned`, all without restarting the server.

Usage in a notebook:
    from common.spark_session import spark
    from common.profiles import apply_profile, profile_summary

    apply_profile(spark, "constrained")   # AQE off, broadcast off → force the pathology
    # ... run the broken query, read the Spark UI ...

    apply_profile(spark, "tuned")          # AQE on, skew-join on → watch it heal
    # ... or apply a manual fix (salting) and compare with metrics_diff ...

    profile_summary(spark)                 # print the knobs currently in effect
"""

from __future__ import annotations

# The four knobs that flip a Spark job between "pathological" and "production-tuned".
# Values are strings because Spark Connect's RuntimeConf.set expects strings.
PROFILES: dict[str, dict[str, str]] = {
    # Force the broken behavior: no adaptive rescue, no broadcast shortcut, coarse
    # shuffle. A skewed join here becomes a sort-merge join with one straggler task.
    "constrained": {
        "spark.sql.adaptive.enabled": "false",
        "spark.sql.adaptive.skewJoin.enabled": "false",
        "spark.sql.adaptive.coalescePartitions.enabled": "false",
        "spark.sql.autoBroadcastJoinThreshold": "-1",   # -1 disables broadcast joins entirely
        "spark.sql.shuffle.partitions": "16",
    },
    # Production-tuned: let Adaptive Query Execution coalesce partitions and split the
    # skewed one at runtime; allow broadcast joins for small sides.
    "tuned": {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.autoBroadcastJoinThreshold": str(10 * 1024 * 1024),  # 10 MB (Spark default)
        "spark.sql.shuffle.partitions": "200",
    },
}

# Keys worth printing when narrating a module (superset of the profile keys).
_SUMMARY_KEYS = [
    "spark.sql.adaptive.enabled",
    "spark.sql.adaptive.skewJoin.enabled",
    "spark.sql.adaptive.coalescePartitions.enabled",
    "spark.sql.autoBroadcastJoinThreshold",
    "spark.sql.shuffle.partitions",
]


def apply_profile(spark, name: str, **overrides: object) -> dict[str, str]:
    """Apply a named session profile, returning the settings that were applied.

    Args:
        spark: an active SparkSession (Connect or local).
        name: ``"constrained"`` or ``"tuned"``.
        **overrides: any extra ``spark.*`` confs to set on top of the profile, e.g.
            ``apply_profile(spark, "constrained", **{"spark.sql.shuffle.partitions": "8"})``.

    Returns:
        The dict of conf keys → values that were actually set.
    """
    if name not in PROFILES:
        raise ValueError(
            f"Unknown profile {name!r}. Choose one of {sorted(PROFILES)} "
            f"(or pass overrides for ad-hoc tweaks)."
        )

    settings = {**PROFILES[name], **{k: str(v) for k, v in overrides.items()}}
    for key, value in settings.items():
        spark.conf.set(key, value)

    print(f"Applied '{name}' session profile:")
    for key, value in settings.items():
        print(f"  {key:<48} = {value}")
    return settings


def profile_summary(spark, keys: list[str] | None = None) -> dict[str, str]:
    """Print and return the session knobs currently in effect (for module narration)."""
    keys = keys or _SUMMARY_KEYS
    current = {k: spark.conf.get(k, "<unset>") for k in keys}
    print("Current session knobs:")
    for key, value in current.items():
        print(f"  {key:<48} = {value}")
    return current
