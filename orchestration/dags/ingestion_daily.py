"""
Ingestion Daily DAG
===================
Schedule: daily at midnight UTC

Tasks (run in parallel — independent of each other)
----------------------------------------------------
discover_channels
    Fetches the latest top-N videos for every channel in config/channels.yaml,
    publishes newly discovered videos to the channel-discovery Kafka topic,
    and upserts them into the Postgres tracked_videos table so comment_poll
    can start polling them.

download_videos
    Queries ClickHouse for the top videos ranked by positive-sentiment
    percentage, downloads them with yt-dlp, and stores the MP4 in MinIO.
    Already-downloaded videos are skipped (idempotent).

Configuration
-------------
All secrets come from the Airflow container's environment (docker-compose
injects them from the host .env file). Required env vars:

  YOUTUBE_API_KEY
  POSTGRES_USER, POSTGRES_PASSWORD
  KAFKA_BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL
  CLICKHOUSE_USER, CLICKHOUSE_PASSWORD
  MINIO_ACCESS_KEY, MINIO_SECRET_KEY
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

# ── Image & network ───────────────────────────────────────────────────────────
# Image is built by: make build-ingestion
INGESTION_IMAGE = "youtube-insights/ingestion:latest"
INFRA_NETWORK = os.environ.get("INFRA_DOCKER_NETWORK", "infra_default")


# ── Shared env for both ingestion tasks ──────────────────────────────────────
def _ingestion_env() -> dict[str, str]:
    return {
        # YouTube API
        "YOUTUBE_API_KEY": os.environ.get("YOUTUBE_API_KEY", ""),
        # PostgreSQL
        "POSTGRES_HOST": "postgres",
        "POSTGRES_PORT": "5432",
        "POSTGRES_USER": os.environ.get("POSTGRES_USER", ""),
        "POSTGRES_PASSWORD": os.environ.get("POSTGRES_PASSWORD", ""),
        "INGESTION_DB": os.environ.get("INGESTION_DB", "ingestion"),
        # Kafka + Schema Registry (used by discover/poll Avro producers)
        "KAFKA_BOOTSTRAP_SERVERS": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        "SCHEMA_REGISTRY_URL": os.environ.get(
            "SCHEMA_REGISTRY_URL",
            "http://schema-registry:8080/apis/ccompat/v7",
        ),
        # ClickHouse (used by download_videos to rank by sentiment)
        "CLICKHOUSE_HOST": "clickhouse",
        "CLICKHOUSE_HTTP_PORT": "8123",
        "CLICKHOUSE_USER": os.environ.get("CLICKHOUSE_USER", ""),
        "CLICKHOUSE_PASSWORD": os.environ.get("CLICKHOUSE_PASSWORD", ""),
        # MinIO (used by download_videos to store MP4s)
        "MINIO_ENDPOINT": "minio:9000",
        "MINIO_ACCESS_KEY": os.environ.get("MINIO_ACCESS_KEY", ""),
        "MINIO_SECRET_KEY": os.environ.get("MINIO_SECRET_KEY", ""),
        "MINIO_VIDEOS_BUCKET": os.environ.get("MINIO_VIDEOS_BUCKET", "videos"),
        # Downloader knobs (optional — have sensible defaults)
        "DOWNLOAD_LOOKBACK_DAYS": os.environ.get("DOWNLOAD_LOOKBACK_DAYS", "1"),
        "TOP_VIDEOS_TO_DOWNLOAD": os.environ.get("TOP_VIDEOS_TO_DOWNLOAD", "5"),
        "MIN_COMMENTS_FOR_DOWNLOAD": os.environ.get("MIN_COMMENTS_FOR_DOWNLOAD", "50"),
    }


# ── DAG ───────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="ingestion_daily",
    description="Daily: discover new videos + download top-satisfaction videos",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "owner": "data-platform",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["ingestion", "discovery", "download"],
) as dag:

    # Refresh the tracked-video list for each channel.
    # Publishes to channel-discovery Kafka topic + upserts Postgres.
    discover_channels = DockerOperator(
        task_id="discover_channels",
        image=INGESTION_IMAGE,
        api_version="auto",
        auto_remove=True,
        command=["python3", "-m", "channel_discovery.main"],
        environment=_ingestion_env(),
        network_mode=INFRA_NETWORK,
        mount_tmp_dir=False,
        execution_timeout=timedelta(minutes=30),
    )

    # Download top-satisfaction videos to MinIO via yt-dlp.
    # Already-downloaded videos are skipped automatically.
    download_videos = DockerOperator(
        task_id="download_videos",
        image=INGESTION_IMAGE,
        api_version="auto",
        auto_remove=True,
        command=["python3", "-m", "video_downloader.main"],
        environment=_ingestion_env(),
        network_mode=INFRA_NETWORK,
        mount_tmp_dir=False,
        # Video downloads can be slow depending on count and file sizes.
        execution_timeout=timedelta(hours=2),
    )

    # Both tasks run in parallel — they read from different sources and
    # write to different sinks, so there is no dependency between them.
