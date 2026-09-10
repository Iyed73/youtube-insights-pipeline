from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG

from common.containers import (
    INGESTION_IMAGE,
    clickhouse_env,
    docker_task,
    minio_env,
    optional_env,
)

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

    download_videos = docker_task(
        task_id="download_videos",
        image=INGESTION_IMAGE,
        command=["python3", "-m", "video_downloader.main"],
        environment=optional_env(
            "DOWNLOAD_LOOKBACK_DAYS", "TOP_VIDEOS_TO_DOWNLOAD", "MIN_COMMENTS_FOR_DOWNLOAD"
        ),
        private_environment={**clickhouse_env(), **minio_env()},
        execution_timeout=timedelta(hours=2),
    )
