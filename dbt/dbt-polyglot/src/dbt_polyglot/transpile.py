"""Core transpile logic — parse source dialect, apply fix-ups, generate Spark SQL.

Called at dbt compile time via the Compiler._compile_code monkeypatch.
"""
import sqlglot
from dbt_polyglot.fixups import SPARK_FIXUPS

_DEFAULT_TARGET = "spark"


def spark_safe_transpile(code, src, dst=None):
    """Parse as `src`, apply fix-up registry (when targeting spark), generate `dst` SQL.

    Raises on multi-statement / empty so the caller's fail-soft kicks in.
    """
    dst = dst or _DEFAULT_TARGET
    statements = sqlglot.parse(code, read=src)
    if len(statements) != 1 or statements[0] is None:
        raise ValueError(f"expected exactly one statement, got {len(statements)}")
    tree = statements[0]
    if dst == _DEFAULT_TARGET:
        for fixup in SPARK_FIXUPS:
            tree = tree.transform(fixup)
    out = tree.sql(dialect=dst, pretty=True)
    if not (out or "").strip():
        raise ValueError("transpile produced empty SQL")
    return out


def patch_compiler():
    """Monkeypatch dbt's Compiler._compile_code to transpile opted-in models."""
    from dbt.compilation import Compiler
    from dbt.adapters.events.logging import AdapterLogger

    logger = AdapterLogger("dbt-polyglot")
    orig = Compiler._compile_code

    def _patched(self, node, manifest, extra_context=None, *args, **kwargs):
        node = orig(self, node, manifest, extra_context, *args, **kwargs)
        src = dst = None
        try:
            src = node.config.get("transpile_from")
            dst = node.config.get("transpile_to") or _DEFAULT_TARGET
            if not src or src == dst:
                return node
            node.compiled_code = spark_safe_transpile(node.compiled_code or "", src, dst)
        except Exception as e:
            uid = getattr(node, "unique_id", "<unknown>")
            logger.warning(
                f"[ dbt-polyglot ] could not transpile {uid} from '{src}' -> "
                f"'{dst or _DEFAULT_TARGET}' ({type(e).__name__}: {e}); "
                f"passing model SQL through UNCHANGED."
            )
        return node

    Compiler._compile_code = _patched
