"""
Spark session factory for spark-dev notebooks.

Usage in notebooks:
    from common.spark_session import spark, display_df

    # spark is ready — Connect, eager eval, sparksql-magic all configured
    spark.sql("SELECT * FROM iceberg_catalog.my_database.orders_iceberg")

    # Scrollable display
    display_df(spark.table("iceberg_catalog.my_database.orders_iceberg"))
"""

import os
from pyspark.sql import SparkSession
from IPython.display import display, HTML


def _create_spark() -> SparkSession:
    """Create and configure a SparkSession for notebooks."""
    remote = os.environ.get("SPARK_REMOTE", "sc://localhost:15002")

    if remote:
        session = SparkSession.builder.remote(remote).getOrCreate()
    else:
        session = (
            SparkSession.builder
            .appName("Spark-Developer")
            .master("local[*]")
            .getOrCreate()
        )

    # Enable rich DataFrame display in Jupyter (must be set client-side for Spark Connect)
    session.conf.set("spark.sql.repl.eagerEval.enabled", True)
    session.conf.set("spark.sql.repl.eagerEval.maxNumRows", 20)
    session.conf.set("spark.sql.repl.eagerEval.truncate", 50)

    # Spark Connect doesn't set _instantiatedSession; some tools expect it
    SparkSession._instantiatedSession = session

    return session


def display_df(df, limit=100, height="400px"):
    """Scrollable HTML table display for Spark DataFrames."""
    html = df.limit(limit).toPandas().to_html(index=False)
    display(HTML(f"<div style='max-height:{height}; overflow:auto'>{html}</div>"))


# Pre-built session — just import and use
spark = _create_spark()
