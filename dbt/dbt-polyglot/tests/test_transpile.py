"""Unit tests for the transpile + fix-up layer. No Spark required (pure sqlglot string checks).

Run:  pip install -e ".[test]" && pytest
"""
import pytest
from dbt_polyglot.transpile import spark_safe_transpile as transpile


def test_not_in_subquery_is_not_emitted_as_unsupported_all():
    out = transpile("select 1 from x where a not in (select a from y)", "snowflake", "spark")
    assert "ALL" not in out.upper()
    assert "NOT" in out.upper() and "IN (" in out.replace("\n", " ").upper().replace("IN(", "IN (")


def test_eq_any_subquery_becomes_in():
    out = transpile("select 1 from x where a = any (select a from y)", "snowflake", "spark")
    assert "ANY" not in out.upper()
    assert "IN" in out.upper()


def test_qualify_is_rewritten_to_subquery():
    out = transpile("select a from x qualify row_number() over (order by a) = 1", "snowflake", "spark")
    assert "QUALIFY" not in out.upper()


def test_common_snowflake_functions_translate():
    out = transpile("select iff(a > 0, 1, 0) c, nvl(b, 'x') d, a::string e from x", "snowflake", "spark")
    up = out.upper()
    assert "IFF(" not in up
    assert "::" not in out
    assert "CAST(" in up


def test_plain_spark_passthrough_is_valid():
    out = transpile("select a, b from x where a = 1", "snowflake", "spark")
    assert "SELECT" in out.upper() and "FROM X" in out.upper()


@pytest.mark.parametrize("bad", ["", "/* only a comment */", "select 1; select 2"])
def test_empty_or_multistatement_raises_so_failsoft_engages(bad):
    with pytest.raises(Exception):
        transpile(bad, "snowflake", "spark")
