"""
Download Videos Daily DAG
=========================
Schedule: daily at midnight UTC

Tasks
-----
download_videos
    Queries ClickHouse for the top videos ranked by positive-sentiment
    percentage, downloads them with yt-dlp, and stores the MP4 in MinIO.
    Already-downloaded videos are skipped (idempotent).

Configuration
-------------
All secrets come from the Airflow container's environment (docker-compose
injects them from the host .env file). Required env vars:

  CLICKHOUSE_USER, CLICKHOUSE_PASSWORD
  MINIO_ACCESS_KEY, MINIO_SECRET_KEY
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

INGESTION_IMAGE = "youtube-insights/ingestion:latest"
INFRA_NETWORK = os.environ.get("INFRA_DOCKER_NETWORK", "infra_default")


def _download_env() -> dict[str, str]:
    return {
        "CLICKHOUSE_HOST": "clickhouse",
        "CLICKHOUSE_HTTP_PORT": "8123",
        "CLICKHOUSE_USER": os.environ.get("CLICKHOUSE_USER", ""),
        "CLICKHOUSE_PASSWORD": os.environ.get("CLICKHOUSE_PASSWORD", ""),
        "MINIO_ENDPOINT": "minio:9000",
        "MINIO_ACCESS_KEY": os.environ.get("MINIO_ACCESS_KEY", ""),
        "MINIO_SECRET_KEY": os.environ.get("MINIO_SECRET_KEY", ""),
        "MINIO_VIDEOS_BUCKET": os.environ.get("MINIO_VIDEOS_BUCKET", "videos"),
        "DOWNLOAD_LOOKBACK_DAYS": os.environ.get("DOWNLOAD_LOOKBACK_DAYS", "1"),
        "TOP_VIDEOS_TO_DOWNLOAD": os.environ.get("TOP_VIDEOS_TO_DOWNLOAD", "5"),
        "MIN_COMMENTS_FOR_DOWNLOAD": os.environ.get("MIN_COMMENTS_FOR_DOWNLOAD", "50"),
    }


with DAG(
    dag_id="download_videos_daily",
    description="Daily: download top-satisfaction videos to MinIO",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "owner": "data-platform",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["ingestion", "download"],
) as dag:

    download_videos = DockerOperator(
        task_id="download_videos",
        image=INGESTION_IMAGE,
        api_version="auto",
        auto_remove=True,
        command=["python3", "-m", "video_downloader.main"],
        environment=_download_env(),
        network_mode=INFRA_NETWORK,
        mount_tmp_dir=False,
        # Video downloads can be slow depending on count and file sizes.
        execution_timeout=timedelta(hours=2),
    )
