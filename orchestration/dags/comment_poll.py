"""
Comment Poll DAG
================
Schedule: every 30 seconds (timedelta(seconds=30))

IMPORTANT — practical scheduling floor
---------------------------------------
Airflow uses DockerOperator for this task. Each run incurs:
  - Container spin-up:            ~2–5 s
  - YouTube API calls per video:  variable (5–30+ s for many active videos)
  - Container teardown:           ~1–2 s

``max_active_runs=1`` ensures only one poll runs at a time. If a run takes
longer than 30 s to complete, Airflow queues the next run immediately after
the current one finishes rather than dropping it. The effective throughput
ends up as "as fast as possible, never concurrent" — which is exactly what
you want for a comment poller.

In practice, with a moderate number of active videos, you can realistically
expect a new poll every 15–60 seconds. If you need strict 30-second
guarantees consider running comment_poller as a long-lived Docker service
with its own sleep loop instead.

Task
----
poll_comments
    For each active tracked video in Postgres:
      1. Fetches comments published after last_polled_at (paginated).
      2. Publishes each new comment to the raw-comments Kafka topic.
      3. Updates last_polled_at (and evicts silent videos) in Postgres.

Configuration
-------------
All secrets come from the Airflow container's environment (injected by
docker-compose from the host .env). Required env vars:

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


def _poll_env() -> dict[str, str]:
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
        "MAX_COMMENTS_PER_POLL": os.environ.get("MAX_COMMENTS_PER_POLL", "2000"),
        "COMMENT_INACTIVITY_HOURS": os.environ.get("COMMENT_INACTIVITY_HOURS", "24"),
    }


with DAG(
    dag_id="comment_poll",
    description="Poll YouTube comments for all active tracked videos",
    schedule=timedelta(seconds=30),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    # Never run two polls concurrently — the poller already processes all
    # active videos in one pass; overlapping runs would double-publish comments.
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": 0,  # Don't retry polls — a failed poll is just a missed window.
    },
    tags=["ingestion", "polling", "kafka"],
) as dag:

    poll_comments = DockerOperator(
        task_id="poll_comments",
        image=INGESTION_IMAGE,
        api_version="auto",
        auto_remove=True,
        command=["python3", "-m", "comment_poller.main"],
        environment=_poll_env(),
        network_mode=INFRA_NETWORK,
        mount_tmp_dir=False,
        # A single poll pass across all active videos should complete well
        # within 5 minutes; if it hangs, fail fast and let the next run start.
        execution_timeout=timedelta(minutes=5),
    )
