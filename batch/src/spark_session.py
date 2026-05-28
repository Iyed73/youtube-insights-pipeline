"""
SparkSession factory for the batch layer.

Called once per job entry-point.  Spark deduplicates sessions within a JVM,
so calling get_spark_session() multiple times in the same process is safe.

S3A → MinIO wiring:
  • Uses SimpleAWSCredentialsProvider — disables the full AWS credential chain
    (no EC2 metadata calls, no STS, no ~/.aws lookups).
  • Path-style access is mandatory for MinIO.
  • SSL disabled (MinIO runs plain HTTP in this setup).

JDBC / PostgreSQL:
  • The postgresql-*.jar is passed via --jars in submit.sh.
  • driver.extraClassPath also points at batch/jars/ as a fallback for
    IDE / pytest runs where spark-submit is not used.
"""

from __future__ import annotations

import logging
from pathlib import Path

from pyspark.sql import SparkSession

from config import cfg   # src/ is on PYTHONPATH; relative import works

logger = logging.getLogger(__name__)


def get_spark_session(app_name: str = "YouTubeBatchPipeline") -> SparkSession:
    """Return a fully-configured SparkSession."""

    jar_dir     = Path(__file__).resolve().parent.parent / "jars"
    jar_glob    = str(jar_dir / "*.jar")

    logger.info("[spark] Building SparkSession '%s' (master=%s)", app_name, cfg.spark_master)

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(cfg.spark_master)

        # ── Memory ────────────────────────────────────────────────────────────
        .config("spark.driver.memory",   cfg.spark_driver_memory)
        .config("spark.executor.memory", cfg.spark_executor_memory)

        # ── Serialization ─────────────────────────────────────────────────────
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

        # ── S3A → MinIO ───────────────────────────────────────────────────────
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config("spark.hadoop.fs.s3a.endpoint",    cfg.minio.endpoint_url)
        .config("spark.hadoop.fs.s3a.access.key",  cfg.minio.access_key)
        .config("spark.hadoop.fs.s3a.secret.key",  cfg.minio.secret_key)
        # Path-style: required for MinIO (no virtual-hosted-style DNS)
        .config("spark.hadoop.fs.s3a.path.style.access",      "true")
        # Disable AWS STS / assume-role calls that MinIO doesn't support
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled",  "false")

        # ── JDBC ──────────────────────────────────────────────────────────────
        # Fallback class-path for IDE / pytest (submit.sh uses --jars instead)
        .config("spark.driver.extraClassPath", jar_glob)

        # ── SQL / misc ────────────────────────────────────────────────────────
        .config("spark.sql.adaptive.enabled",                        "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled",     "true")
        # Suppress noisy _SUCCESS / .crc marker files on MinIO writes
        .config(
            "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    logger.info("[spark] SparkSession ready (appId=%s)", spark.sparkContext.applicationId)
    return spark