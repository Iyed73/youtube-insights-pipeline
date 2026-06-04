from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG

from common.containers import INGESTION_IMAGE, docker_task, kafka_env, postgres_env, youtube_env

with DAG(
    dag_id="discover_channels_daily",
    description="Daily: discover new videos for tracked channels",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "owner": "data-platform",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["ingestion", "discovery"],
) as dag:

    discover_channels = docker_task(
        task_id="discover_channels",
        image=INGESTION_IMAGE,
        command=["python3", "-m", "channel_discovery.main"],
        environment=kafka_env(),
        private_environment={**youtube_env(), **postgres_env()},
        execution_timeout=timedelta(minutes=30),
    )
