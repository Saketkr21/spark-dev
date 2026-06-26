"""Spark-output fix-up registry.

Each entry is an (exp.Expression -> exp.Expression) transform applied (via .transform,
bottom-up) to the parsed tree BEFORE generating Spark SQL. They repair cases where
sqlglot's output is rejected by Spark's real parser.

Extensible: append a transform function per gap found, EXPLAIN-verify on Spark.
"""
from sqlglot import exp


def _as_subquery(node):
    return node if isinstance(node, exp.Subquery) else exp.Subquery(this=node)


def fixup_quantified_subquery(node):
    """Spark has no quantified subquery comparison.

    sqlglot's Snowflake parser canonicalizes:
      x NOT IN (subq) -> x <> ALL (subq)
      x IN (subq)     -> x = ANY (subq)
    Spark rejects both. Rewrite back to NOT x IN (subq) / x IN (subq).
    """
    if isinstance(node, exp.NEQ) and isinstance(node.expression, exp.All):
        return exp.Not(this=exp.In(this=node.this, query=_as_subquery(node.expression.this)))
    if isinstance(node, exp.EQ) and isinstance(node.expression, exp.Any):
        return exp.In(this=node.this, query=_as_subquery(node.expression.this))
    return node


SPARK_FIXUPS = [fixup_quantified_subquery]
