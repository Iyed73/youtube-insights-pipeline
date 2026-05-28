"""
Thin wrappers around Spark's JDBC reader/writer for PostgreSQL.

All jobs import from here rather than spelling out JDBC options inline —
one place to update if the driver class, URL format, or auth ever changes.
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession

from config import cfg

logger = logging.getLogger(__name__)

_DRIVER = "org.postgresql.Driver"


def read_table(spark: SparkSession, table: str) -> DataFrame:
    """Read an entire table as a Spark DataFrame (single JDBC partition)."""
    logger.info("[postgres] Reading table '%s'.", table)
    return (
        spark.read.format("jdbc")
        .option("url",      cfg.postgres.jdbc_url)
        .option("dbtable",  table)
        .option("user",     cfg.postgres.user)
        .option("password", cfg.postgres.password)
        .option("driver",   _DRIVER)
        .load()
    )


def read_query(spark: SparkSession, query: str, alias: str = "sq") -> DataFrame:
    """
    Execute an arbitrary SQL query and return the result as a DataFrame.
    Spark JDBC requires the query wrapped in a subquery alias.
    """
    logger.info("[postgres] Executing JDBC query (alias=%s).", alias)
    return (
        spark.read.format("jdbc")
        .option("url",      cfg.postgres.jdbc_url)
        .option("dbtable",  f"({query}) AS {alias}")
        .option("user",     cfg.postgres.user)
        .option("password", cfg.postgres.password)
        .option("driver",   _DRIVER)
        .load()
    )


def write_table(df: DataFrame, table: str, mode: str = "append") -> None:
    """
    Write a DataFrame to a Postgres table via JDBC.

    mode  'append'    — add rows (default; safe for incremental writes)
          'overwrite'  — DROP + CREATE; avoid on production tables
          'ignore'     — no-op if table already has data
          'error'      — raise if table has data
    """
    count = df.count()
    logger.info("[postgres] Writing %d rows → '%s' (mode=%s).", count, table, mode)
    (
        df.write.format("jdbc")
        .option("url",      cfg.postgres.jdbc_url)
        .option("dbtable",  table)
        .option("user",     cfg.postgres.user)
        .option("password", cfg.postgres.password)
        .option("driver",   _DRIVER)
        .mode(mode)
        .save()
    )
    logger.info("[postgres] Write to '%s' complete.", table)