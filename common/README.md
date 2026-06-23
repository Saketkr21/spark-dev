# `common/` — shared curriculum toolkit

The reusable machinery every challenge module depends on. Import it from notebooks
(host `PYTHONPATH` includes the repo root, so `from common.xxx import ...` just works).

| Module | Purpose | Built in |
|--------|---------|----------|
| `spark_session.py` | Spark Connect session factory + `display_df()` scrollable table | F-0 ✅ |
| `profiles.py` | `apply_profile(spark, "constrained"\|"tuned")` — toggle AQE / skew-join / broadcast threshold / shuffle partitions at the **session** level | F-1 |
| `datagen.py` | `spark.range()`-based generators: uniform / **skewed** / wide / high-cardinality — synthesize huge logical datasets without storing them | F-2 |
| `metrics_diff.py` | Capture stage/query metrics (runtime, shuffle, spill, task-time max-vs-median) and print a **before/after** table | F-3 |

## The two layers of "constrained vs tuned"

The Spark Connect server's **memory** (`spark.driver.memory`, container `mem_limit`) is fixed
when the container boots — notebooks connecting over gRPC can't change it at runtime. So the
resource profile has two layers:

1. **Container / box size** — flipped at startup: `make up` (tuned, ~3 GB) vs
   `make up-constrained` (~2 GB). Use the constrained box for OOM / spill modules.
2. **Session safety-nets** — flipped at runtime from the notebook via
   `common.profiles.apply_profile(spark, "constrained")`. This is what most Spark
   pathology modules (e.g. skew) use to force the broken behavior, then relieve it.

## Usage

```python
from common.spark_session import spark, display_df
from common.profiles import apply_profile
from common.datagen import skewed_keys
from common.metrics_diff import measure, diff

apply_profile(spark, "constrained")        # AQE off, broadcast off, force the pathology
df = skewed_keys(spark, n_rows=50_000_000, hot_key_fraction=0.9)
```
