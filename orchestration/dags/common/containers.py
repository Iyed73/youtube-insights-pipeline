from __future__ import annotations

import os

from airflow.providers.docker.operators.docker import DockerOperator

# Network name pinned in infra/docker-compose.yml.
DOCKER_NETWORK = "youtube-insights"
INGESTION_IMAGE = "youtube-insights/ingestion:latest"
SPARK_IMAGE = "youtube-insights/spark:latest"


def docker_task(
    *,
    task_id: str,
    image: str,
    command: list[str],
    private_environment: dict[str, str],
    environment: dict[str, str] | None = None,
    **kwargs,
) -> DockerOperator:
    return DockerOperator(
        task_id=task_id,
        image=image,
        command=command,
        environment=environment or {},
        private_environment=private_environment,
        network_mode=DOCKER_NETWORK,
        api_version="auto",
        auto_remove="success",
        mount_tmp_dir=False,
        **kwargs,
    )


def optional_env(*names: str) -> dict[str, str]:
    return {name: os.environ[name] for name in names if os.environ.get(name)}


def postgres_env() -> dict[str, str]:
    return {
        "POSTGRES_HOST": "postgres",
        "POSTGRES_PORT": "5432",
        "POSTGRES_USER": os.environ["POSTGRES_USER"],
        "POSTGRES_PASSWORD": os.environ["POSTGRES_PASSWORD"],
        "INGESTION_DB": os.environ["INGESTION_DB"],
    }


def minio_env() -> dict[str, str]:
    return {
        "MINIO_ENDPOINT": "minio:9000",
        "MINIO_ACCESS_KEY": os.environ["MINIO_ACCESS_KEY"],
        "MINIO_SECRET_KEY": os.environ["MINIO_SECRET_KEY"],
        "MINIO_VIDEOS_BUCKET": os.environ["MINIO_VIDEOS_BUCKET"],
    }


def clickhouse_env() -> dict[str, str]:
    return {
        "CLICKHOUSE_HOST": "clickhouse",
        "CLICKHOUSE_HTTP_PORT": "8123",
        "CLICKHOUSE_USER": os.environ["CLICKHOUSE_USER"],
        "CLICKHOUSE_PASSWORD": os.environ["CLICKHOUSE_PASSWORD"],
    }


def kafka_env() -> dict[str, str]:
    return {
        "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
        "SCHEMA_REGISTRY_URL": "http://schema-registry:8080/apis/ccompat/v7",
    }


def youtube_env() -> dict[str, str]:
    return {"YOUTUBE_API_KEY": os.environ["YOUTUBE_API_KEY"]}


def anthropic_env() -> dict[str, str]:
    return {"ANTHROPIC_API_KEY": os.environ["ANTHROPIC_API_KEY"]}
