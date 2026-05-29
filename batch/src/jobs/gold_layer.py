"""
gold_layer.py — Spark Batch Job (Gold Layer)

Reads the Silver scene metrics (editing pace, brightness) from MinIO Parquet,
joins them against ClickHouse comment sentiment aggregates, and produces one
Gold row per video that correlates visual editing style with viewer reaction.

Gold Metrics produced per video:
  - cuts                    : total scene cuts (from Silver)
  - duration_sec            : video length in seconds (from Silver)
  - asl_sec                 : Average Shot Length — duration / cuts (from Silver)
  - cut_density             : cuts-per-minute array (from Silver)
  - avg_brightness          : mean pixel intensity 0–255 (from Silver)
  - total_comments          : total comment count (from ClickHouse)
  - positive/neutral/negative_count  : raw sentiment counts (from ClickHouse)
  - positive/neutral/negative_ratio  : sentiment share 0.0–1.0 (derived)
  - sentiment_score         : (positive - negative) / total, range -1 to +1 (derived)
  - editing_pace            : 'fast' <3 s | 'medium' 3–8 s | 'slow' >8 s (derived)
  - pace_sentiment_label    : human-readable correlation signal (derived)

Architecture (standalone Spark — no Hadoop, same as scene_detection.py):
  1. Driver downloads Silver Parquet files from MinIO to a local /tmp staging dir.
  2. Driver reads them into a Spark DataFrame (silver_df).
  3. Driver queries ClickHouse via clickhouse-connect for per-video sentiment
     aggregates and materialises them as a second Spark DataFrame (sentiment_df).
     Aggregation is pushed down to ClickHouse so Spark only receives one summary
     row per video, not millions of raw comment rows.
  4. Spark joins silver_df ⟕ sentiment_df on video_id (left join — keeps videos
     that have scene data but no comments yet).
  5. Derived columns are computed with Spark SQL functions.
  6. Results are written back to MinIO as Parquet under analytics/gold/ and
     inserted into ClickHouse gold.video_insights via clickhouse-connect.

Python environment note:
  Same split as scene_detection.py — driver uses system python3, workers use the
  unpacked venv.  clickhouse-connect and minio must be installed in both.
  No worker-side processing is needed here (all logic is driver-side Spark SQL),
  so no imports are deferred inside partition functions.
"""

from __future__ import annotations

import glob
import io
import os
import shutil
import tempfile
from dataclasses import dataclass

import clickhouse_connect
from minio import Minio
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


# ── Schema ─────────────────────────────────────────────────────────────────────
# Explicit schema prevents CANNOT_DETERMINE_TYPE on nullable fields.

SILVER_SCHEMA = StructType([
    StructField("video_id",       StringType(),             nullable=False),
    StructField("channel_id",     StringType(),             nullable=False),
    StructField("cuts",           IntegerType(),            nullable=True),
    StructField("duration_sec",   DoubleType(),             nullable=True),
    StructField("asl_sec",        DoubleType(),             nullable=True),
    StructField("cut_density",    ArrayType(IntegerType()), nullable=True),
    StructField("avg_brightness", DoubleType(),             nullable=True),
    StructField("status",         StringType(),             nullable=False),
    StructField("error",          StringType(),             nullable=True),
])

# Sentiment summary pulled from ClickHouse — one row per video.
SENTIMENT_SCHEMA = StructType([
    StructField("video_id",        StringType(), nullable=False),
    StructField("total_comments",  LongType(),   nullable=False),
    StructField("positive_count",  LongType(),   nullable=False),
    StructField("neutral_count",   LongType(),   nullable=False),
    StructField("negative_count",  LongType(),   nullable=False),
])

GOLD_SCHEMA = StructType([
    StructField("video_id",             StringType(),             nullable=False),
    StructField("channel_id",           StringType(),             nullable=False),
    StructField("cuts",                 IntegerType(),            nullable=True),
    StructField("duration_sec",         DoubleType(),             nullable=True),
    StructField("asl_sec",              DoubleType(),             nullable=True),
    StructField("cut_density",          ArrayType(IntegerType()), nullable=True),
    StructField("avg_brightness",       DoubleType(),             nullable=True),
    StructField("total_comments",       LongType(),               nullable=True),
    StructField("positive_count",       LongType(),               nullable=True),
    StructField("neutral_count",        LongType(),               nullable=True),
    StructField("negative_count",       LongType(),               nullable=True),
    StructField("positive_ratio",       DoubleType(),             nullable=True),
    StructField("neutral_ratio",        DoubleType(),             nullable=True),
    StructField("negative_ratio",       DoubleType(),             nullable=True),
    StructField("sentiment_score",      DoubleType(),             nullable=True),
    StructField("editing_pace",         StringType(),             nullable=True),
    StructField("pace_sentiment_label", StringType(),             nullable=True),
])


# ── Config ─────────────────────────────────────────────────────────────────────

@dataclass
class Config:
    # MinIO
    minio_endpoint:    str   # host:port — no scheme
    minio_access_key:  str
    minio_secret_key:  str
    results_bucket:    str   # same bucket as scene_detection writes to
    # ClickHouse
    clickhouse_host:   str
    clickhouse_port:   int
    clickhouse_user:   str
    clickhouse_password: str
    # Spark
    num_workers:       int
    cores_per_worker:  int


def _cfg() -> Config:
    endpoint = os.environ.get("MINIO_ENDPOINT", "minio:9000")
    endpoint = endpoint.replace("http://", "").replace("https://", "")
    return Config(
        minio_endpoint=endpoint,
        minio_access_key=os.environ.get("MINIO_ACCESS_KEY",    "minioadmin"),
        minio_secret_key=os.environ.get("MINIO_SECRET_KEY",    "minioadmin"),
        results_bucket=os.environ.get("MINIO_RESULTS_BUCKET",  "analytics"),
        clickhouse_host=os.environ.get("CLICKHOUSE_HOST",      "clickhouse"),
        clickhouse_port=int(os.environ.get("CLICKHOUSE_PORT",  "8123")),
        clickhouse_user=os.environ.get("CLICKHOUSE_USER",      "default"),
        clickhouse_password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
        num_workers=int(os.environ.get("NUM_WORKERS",           "2")),
        cores_per_worker=int(os.environ.get("CORES_PER_WORKER", "2")),
    )


# ── Helpers ────────────────────────────────────────────────────────────────────

def _minio_client(cfg: Config) -> Minio:
    return Minio(
        cfg.minio_endpoint,
        access_key=cfg.minio_access_key,
        secret_key=cfg.minio_secret_key,
        secure=False,
    )


def _clickhouse_client(cfg: Config):
    """Return a clickhouse-connect client (HTTP, no JDBC driver needed)."""
    return clickhouse_connect.get_client(
        host=cfg.clickhouse_host,
        port=cfg.clickhouse_port,
        username=cfg.clickhouse_user,
        password=cfg.clickhouse_password,
    )


# ── Step 1 helpers: download Silver Parquet from MinIO ────────────────────────

def _download_silver(cfg: Config, minio: Minio, local_dir: str) -> list[str]:
    """
    Download every Parquet file under analytics/scene_metrics/ to local_dir.
    Returns a list of local file paths.
    """
    local_paths = []
    for obj in minio.list_objects(cfg.results_bucket, prefix="scene_metrics/", recursive=True):
        if not obj.object_name.endswith(".parquet"):
            continue
        local_path = os.path.join(local_dir, os.path.basename(obj.object_name))
        minio.fget_object(cfg.results_bucket, obj.object_name, local_path)
        local_paths.append(local_path)
        print(f"  [Silver] Downloaded {obj.object_name} → {local_path}")
    return local_paths


# ── Step 2 helpers: pull sentiment aggregates from ClickHouse ─────────────────

def _fetch_sentiment(cfg: Config, ch) -> list[tuple]:
    """
    Push the GROUP BY down to ClickHouse — only one summary row per video
    comes back to Spark, not the raw millions of comment rows.

    Returns a list of (video_id, total, positive, neutral, negative) tuples.
    """
    query = """
        SELECT
            video_id,
            count(*)                       AS total_comments,
            countIf(sentiment = 'Positive') AS positive_count,
            countIf(sentiment = 'Neutral')  AS neutral_count,
            countIf(sentiment = 'Negative') AS negative_count
        FROM analytics.comments
        GROUP BY video_id
    """
    result = ch.query(query)
    # result.result_rows is a list of tuples in column order
    return result.result_rows


# ── Gold ClickHouse DDL ────────────────────────────────────────────────────────

_GOLD_DDL = """
CREATE DATABASE IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.video_insights
(
    video_id                String,
    channel_id              LowCardinality(String),
    cuts                    Nullable(Int32),
    duration_sec            Nullable(Float64),
    asl_sec                 Nullable(Float64),
    cut_density             Array(Int32),
    avg_brightness          Nullable(Float64),
    total_comments          Nullable(Int64),
    positive_count          Nullable(Int64),
    neutral_count           Nullable(Int64),
    negative_count          Nullable(Int64),
    positive_ratio          Nullable(Float64),
    neutral_ratio           Nullable(Float64),
    negative_ratio          Nullable(Float64),
    sentiment_score         Nullable(Float64),
    editing_pace            LowCardinality(Nullable(String)),
    pace_sentiment_label    LowCardinality(Nullable(String)),
    batch_ts                DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(batch_ts)
ORDER BY (channel_id, video_id)
SETTINGS index_granularity = 8192;
"""


# ── Driver ─────────────────────────────────────────────────────────────────────

def main() -> None:
    cfg = _cfg()

    spark = SparkSession.builder \
        .appName("GoldLayer") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    minio  = _minio_client(cfg)
    ch     = _clickhouse_client(cfg)

    # ── Phase 1: Read Silver layer from MinIO ─────────────────────────────────
    print("\n[Silver] Downloading scene metrics from MinIO...")
    # Use the shared bind-mount so Spark executors (on worker containers) can
    # access the downloaded Parquet files.  The driver's /tmp/ is private to
    # spark-master and invisible to spark-worker* — only /opt/spark/batch/
    # is bind-mounted across all three containers.
    silver_staging = "/opt/spark/batch/tmp/silver"
    if os.path.exists(silver_staging):
        shutil.rmtree(silver_staging)   # clear stale files from previous runs
    os.makedirs(silver_staging, exist_ok=True)
    silver_files   = _download_silver(cfg, minio, silver_staging)

    if not silver_files:
        print("[Silver] No scene_metrics Parquet files found — nothing to process.")
        spark.stop()
        return

    print(f"[Silver] Loaded {len(silver_files)} Parquet file(s).")
    silver_df = spark.read \
        .schema(SILVER_SCHEMA) \
        .parquet(silver_staging) \
        .filter(F.col("status") == "ok")   # drop error rows from scene_detection

    num_partitions = cfg.num_workers * cfg.cores_per_worker
    # Cache to prevent re-scanning Parquet files on every downstream action
    # (count, show, join each trigger a separate read without caching).
    silver_df = silver_df.repartition(num_partitions).cache()

    print(f"\n[Silver] Scene metrics ({silver_df.count()} video(s)):")
    silver_df.show(truncate=False)

    # ── Phase 2: Pull sentiment aggregates from ClickHouse ───────────────────
    print("\n[ClickHouse] Fetching comment sentiment aggregates...")
    sentiment_rows = _fetch_sentiment(cfg, ch)

    if not sentiment_rows:
        print("[ClickHouse] No comment data found — Gold will have NULL sentiment columns.")

    sentiment_df = spark.createDataFrame(sentiment_rows, schema=SENTIMENT_SCHEMA)

    print(f"[ClickHouse] Sentiment summary ({sentiment_df.count()} video(s)):")
    sentiment_df.show(truncate=False)

    # ── Phase 3: Join Silver ⟕ Sentiment on video_id ─────────────────────────
    # Left join — Silver is the authoritative left side.
    # Videos with no comments yet keep their scene metrics with NULL sentiment.
    joined_df = silver_df.join(sentiment_df, on="video_id", how="left")

    # ── Phase 4: Derive Gold metrics ──────────────────────────────────────────
    # 4.1  Sentiment ratios — guard divide-by-zero when total_comments is NULL or 0
    has_comments = (F.col("total_comments").isNotNull()) & (F.col("total_comments") > 0)

    gold_df = joined_df.select(
        "video_id",
        "channel_id",
        "cuts",
        "duration_sec",
        "asl_sec",
        "cut_density",
        "avg_brightness",
        "total_comments",
        "positive_count",
        "neutral_count",
        "negative_count",

        # ── Ratios ────────────────────────────────────────────────────────────
        F.when(has_comments,
            F.round(F.col("positive_count") / F.col("total_comments"), 4)
        ).otherwise(F.lit(None)).alias("positive_ratio"),

        F.when(has_comments,
            F.round(F.col("neutral_count") / F.col("total_comments"), 4)
        ).otherwise(F.lit(None)).alias("neutral_ratio"),

        F.when(has_comments,
            F.round(F.col("negative_count") / F.col("total_comments"), 4)
        ).otherwise(F.lit(None)).alias("negative_ratio"),

        # ── Sentiment score: +1 = all positive, -1 = all negative ─────────────
        # Formula: (positive − negative) / total
        F.when(has_comments,
            F.round(
                (F.col("positive_count") - F.col("negative_count"))
                / F.col("total_comments"),
                4,
            )
        ).otherwise(F.lit(None)).alias("sentiment_score"),

        # ── Editing pace derived from ASL ─────────────────────────────────────
        # fast   < 3 s  → rapid cuts, high-energy (action, TikTok-style)
        # medium 3–8 s  → standard vlog / tutorial pace
        # slow   > 8 s  → documentary, lecture, long-form
        F.when(F.col("asl_sec") < 3,   "fast")
         .when(F.col("asl_sec") <= 8,  "medium")
         .when(F.col("asl_sec") >  8,  "slow")
         .otherwise(F.lit(None))
         .alias("editing_pace"),
    )

    # 4.2  pace_sentiment_label — human-readable correlation signal
    # Combines editing_pace + sign of sentiment_score into one interpretable label
    # e.g. "fast_positive" means the audience reacts well to rapid editing.
    gold_df = gold_df.withColumn(
        "pace_sentiment_label",
        F.when(
            F.col("editing_pace").isNotNull() & F.col("sentiment_score").isNotNull(),
            F.concat(
                F.col("editing_pace"),
                F.lit("_"),
                F.when(F.col("sentiment_score") >  0.1, "positive")
                 .when(F.col("sentiment_score") < -0.1, "negative")
                 .otherwise("neutral"),
            )
        ).otherwise(F.lit(None))
    )

    print("\n── Gold Layer Results ──")
    gold_df.show(truncate=False)

    # ── Phase 5: Write Gold Parquet back to MinIO ─────────────────────────────
    print("\n[Gold] Writing Parquet to MinIO analytics/gold/...")
    # Use the shared bind-mount so the driver's glob() can see executor-written files.
    gold_local = "/opt/spark/batch/tmp/gold_metrics"
    os.makedirs(gold_local, exist_ok=True)
    gold_df.write.mode("overwrite").parquet(gold_local)

    for parquet_file in glob.glob(f"{gold_local}/*.parquet"):
        object_name = f"gold/{os.path.basename(parquet_file)}"
        minio.fput_object(cfg.results_bucket, object_name, parquet_file)
        print(f"  -> Uploaded {object_name} to bucket '{cfg.results_bucket}'")

    # ── Phase 6: Write Gold rows into ClickHouse gold.video_insights ──────────
    print("\n[Gold] Ensuring gold.video_insights table exists in ClickHouse...")
    for statement in _GOLD_DDL.strip().split(";"):
        statement = statement.strip()
        if statement:
            ch.command(statement)

    print("[Gold] Inserting rows into gold.video_insights...")
    gold_rows = gold_df.collect()

    ch.insert(
        "gold.video_insights",
        [list(row) for row in gold_rows],
        column_names=GOLD_SCHEMA.fieldNames(),
    )
    print(f"  -> Inserted {len(gold_rows)} row(s) into gold.video_insights")

    print("\n[Done] Gold layer batch job complete.")
    spark.stop()


if __name__ == "__main__":
    main()