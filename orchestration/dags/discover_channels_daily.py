"""
Discover Channels Daily DAG
===========================
Schedule: daily at midnight UTC

Tasks
-----
discover_channels
    Fetches the latest top-N videos for every channel in config/channels.yaml,
    publishes newly discovered videos to the channel-discovery Kafka topic,
    and upserts them into the Postgres tracked_videos table so comment_poll
    can start polling them.

Configuration
-------------
All secrets come from the Airflow container's environment (docker-compose
injects them from the host .env file). Required env vars:

  YOUTUBE_API_KEY
  POSTGRES_USER, POSTGRES_PASSWORD
  KAFKA_BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

INGESTION_IMAGE = "youtube-insights/ingestion:latest"
INFRA_NETWORK = os.environ.get("INFRA_DOCKER_NETWORK", "infra_default")


def _discover_env() -> dict[str, str]:
    return {
        "YOUTUBE_API_KEY": os.environ.get("YOUTUBE_API_KEY", ""),
        "POSTGRES_HOST": "postgres",
        "POSTGRES_PORT": "5432",
        "POSTGRES_USER": os.environ.get("POSTGRES_USER", ""),
        "POSTGRES_PASSWORD": os.environ.get("POSTGRES_PASSWORD", ""),
        "INGESTION_DB": os.environ.get("INGESTION_DB", "ingestion"),
        "KAFKA_BOOTSTRAP_SERVERS": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        "SCHEMA_REGISTRY_URL": os.environ.get(
            "SCHEMA_REGISTRY_URL",
            "http://schema-registry:8080/apis/ccompat/v7",
        ),
    }


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

    discover_channels = DockerOperator(
        task_id="discover_channels",
        image=INGESTION_IMAGE,
        api_version="auto",
        auto_remove=True,
        command=["python3", "-m", "channel_discovery.main"],
        environment=_discover_env(),
        network_mode=INFRA_NETWORK,
        mount_tmp_dir=False,
        execution_timeout=timedelta(minutes=30),
    )
