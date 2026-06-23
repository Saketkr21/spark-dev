"""
metrics_diff (F-3) — make every fix *quantitative*.

The "Prove it" step of Break → Detect → Fix → Prove. Wrap an action, capture the
stage metrics it produced, then print a **before/after** table so a learner sees the
improvement in numbers (runtime, shuffle, spill, and the skew tell: task-time max-vs-median).

Why the Spark UI REST API (not a SparkListener)?
    Notebooks talk to Spark over **Spark Connect**, where there is no client-side
    ``SparkContext`` to attach a listener to. The live Spark UI REST API at
    ``http://localhost:4040/api/v1`` exposes the same stage/task metrics and is reachable
    from the host, so it works for both Connect and local mode. If the UI isn't reachable
    we still report wall-clock runtime and say stage metrics were unavailable.

Usage:
    from common.metrics_diff import measure, compare

    before = measure(spark, "skewed (sort-merge)", lambda: broken.count())
    after  = measure(spark, "salted",              lambda: fixed.count())
    compare([before, after])
"""

from __future__ import annotations

import json
import os
import time
import urllib.request
from typing import Callable

_TIMEOUT_S = 5
_SETTLE_RETRIES = 6          # poll the REST API briefly until the new stages show COMPLETE
_SETTLE_SLEEP_S = 0.5


# ── REST helpers ──────────────────────────────────────────────────────────────

def _ui_base() -> str:
    """Base URL of the live Spark UI REST API (override with SPARK_UI_BASE)."""
    base = os.environ.get("SPARK_UI_BASE")
    if base:
        return base.rstrip("/")
    port = os.environ.get("SPARK_UI_PORT", "4040")
    return f"http://localhost:{port}"


def _get_json(url: str):
    with urllib.request.urlopen(url, timeout=_TIMEOUT_S) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _app_id(base: str) -> str:
    apps = _get_json(f"{base}/api/v1/applications")
    if not apps:
        raise RuntimeError("no Spark application found at the UI REST API")
    return apps[0]["id"]


def _completed_stages(base: str, app_id: str) -> list[dict]:
    return _get_json(f"{base}/api/v1/applications/{app_id}/stages?status=COMPLETE")


def _task_summary(base: str, app_id: str, stage_id: int, attempt_id: int) -> dict:
    url = (
        f"{base}/api/v1/applications/{app_id}/stages/{stage_id}/{attempt_id}"
        f"/taskSummary?quantiles=0.5,0.75,1.0"
    )
    return _get_json(url)


# ── measurement ────────────────────────────────────────────────────────────────

def measure(spark, label: str, fn: Callable[[], object]) -> dict:
    """Run ``fn`` (an action), capturing the stage metrics it produced.

    Args:
        spark: active SparkSession (used only to confirm a session exists).
        label: name for this run, shown as a column in :func:`compare`.
        fn: a zero-arg callable that triggers a Spark action (``lambda: df.count()``).

    Returns:
        A metrics dict (runtime, shuffle, spill, task-time median/max, skew ratio, …).
        Fields are ``None`` when the UI REST API couldn't be reached.
    """
    base = _ui_base()
    app_id = None
    before_ids: set[int] = set()
    try:
        app_id = _app_id(base)
        before_ids = {s["stageId"] for s in _completed_stages(base, app_id)}
    except Exception as exc:  # noqa: BLE001 — degrade gracefully, never block the lesson
        print(f"[metrics_diff] UI REST API unavailable ({exc}); reporting wall-clock only.")

    t0 = time.perf_counter()
    fn()
    runtime_s = time.perf_counter() - t0

    metrics = {
        "label": label,
        "runtime_s": round(runtime_s, 2),
        "num_tasks": None,
        "shuffle_read_bytes": None,
        "shuffle_write_bytes": None,
        "spill_mem_bytes": None,
        "spill_disk_bytes": None,
        "task_time_median_ms": None,
        "task_time_max_ms": None,
        "skew_ratio": None,
        "peak_mem_max_bytes": None,
    }
    if app_id is None:
        return metrics

    try:
        new_stages = []
        for _ in range(_SETTLE_RETRIES):
            new_stages = [s for s in _completed_stages(base, app_id) if s["stageId"] not in before_ids]
            if new_stages:
                break
            time.sleep(_SETTLE_SLEEP_S)

        if not new_stages:
            return metrics

        metrics["num_tasks"] = sum(s.get("numTasks", 0) for s in new_stages)
        metrics["shuffle_read_bytes"] = sum(s.get("shuffleReadBytes", 0) for s in new_stages)
        metrics["shuffle_write_bytes"] = sum(s.get("shuffleWriteBytes", 0) for s in new_stages)
        metrics["spill_mem_bytes"] = sum(s.get("memoryBytesSpilled", 0) for s in new_stages)
        metrics["spill_disk_bytes"] = sum(s.get("diskBytesSpilled", 0) for s in new_stages)

        # The "heavy" stage — where skew shows up — is the one moving the most shuffle
        # data (fallback: the most tasks). Read its per-task time distribution.
        heavy = max(
            new_stages,
            key=lambda s: (s.get("shuffleReadBytes", 0), s.get("numTasks", 0)),
        )
        try:
            ts = _task_summary(base, app_id, heavy["stageId"], heavy.get("attemptId", 0))
            run_times = ts.get("executorRunTime")  # [median, p75, max]
            if run_times and len(run_times) >= 3:
                median, _p75, mx = run_times[0], run_times[1], run_times[2]
                metrics["task_time_median_ms"] = round(median, 1)
                metrics["task_time_max_ms"] = round(mx, 1)
                metrics["skew_ratio"] = round(mx / median, 1) if median else None
            peak = ts.get("peakExecutionMemory")
            if peak:
                metrics["peak_mem_max_bytes"] = peak[-1]
        except Exception:  # noqa: BLE001 — task summary is best-effort
            pass
    except Exception as exc:  # noqa: BLE001
        print(f"[metrics_diff] could not read stage metrics ({exc}); reporting wall-clock only.")

    return metrics


# ── reporting ────────────────────────────────────────────────────────────────

def _fmt_bytes(n) -> str:
    if n is None:
        return "—"
    if n == 0:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(n)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{n} B"


def _fmt(v, kind: str = "num") -> str:
    if v is None:
        return "—"
    if kind == "bytes":
        return _fmt_bytes(v)
    if kind == "secs":
        return f"{v:.2f} s"
    if kind == "ms":
        return f"{v:,.0f} ms"
    if kind == "ratio":
        return f"{v:.1f}×"
    return f"{v:,}"


# (row label, metric key, formatter kind)
_ROWS = [
    ("Wall-clock runtime", "runtime_s", "secs"),
    ("Tasks", "num_tasks", "num"),
    ("Shuffle read", "shuffle_read_bytes", "bytes"),
    ("Shuffle write", "shuffle_write_bytes", "bytes"),
    ("Spill (memory)", "spill_mem_bytes", "bytes"),
    ("Spill (disk)", "spill_disk_bytes", "bytes"),
    ("Task time — median", "task_time_median_ms", "ms"),
    ("Task time — max", "task_time_max_ms", "ms"),
    ("Skew (max ÷ median)", "skew_ratio", "ratio"),
    ("Peak exec memory", "peak_mem_max_bytes", "bytes"),
]


def compare(results: list[dict]) -> None:
    """Print a before/after metrics table. ``results`` is a list of :func:`measure` dicts.

    Metrics are rows; each measured run is a column. The **Skew (max ÷ median)** row is the
    headline for skew modules — a value near 1× means balanced tasks; a large value means a
    straggler. Renders as an aligned Markdown table (copy-paste friendly into module READMEs).
    """
    if not results:
        print("compare(): nothing to show — pass a list of measure() results.")
        return

    labels = [r.get("label", f"run {i}") for i, r in enumerate(results)]
    metric_col_w = max(len(name) for name, _, _ in _ROWS)
    col_w = [max(len(lbl), 12) for lbl in labels]

    def _row(cells: list[str]) -> str:
        head = cells[0].ljust(metric_col_w)
        rest = " | ".join(c.rjust(col_w[i]) for i, c in enumerate(cells[1:]))
        return f"| {head} | {rest} |"

    print(_row(["Metric", *labels]))
    sep_rest = " | ".join("-" * col_w[i] for i in range(len(labels)))
    print(f"| {'-' * metric_col_w} | {sep_rest} |")
    for name, key, kind in _ROWS:
        cells = [name] + [_fmt(r.get(key), kind) for r in results]
        print(_row(cells))


def diff(before: dict, after: dict) -> None:
    """Convenience: a two-column before/after table (sugar for ``compare([before, after])``)."""
    compare([before, after])
