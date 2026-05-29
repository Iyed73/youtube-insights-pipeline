"""
Batch Topic Modeling DAG
========================
Schedule: weekly (Sunday at midnight UTC)

Task graph
----------

  transcribe  ──►  run_lda  ──►  label_and_write

transcribe  (spark-submit transcribe_job.py)
    Ensures every downloaded video has a .txt transcript cached in MinIO.
    Runs Whisper only on uncached videos. Idempotent — safe to re-run.

run_lda  (spark-submit lda_job.py)
    Reads transcripts from MinIO cache (no Whisper).
    Runs Spark ML pipeline: tokenize → stop words → lemmatize → LDA.
    Saves result rows + topic-words map to MinIO as JSON.
    NO Claude call, NO ClickHouse write.

label_and_write  (python3 label_and_write_job.py — no Spark)
    Loads the JSON written by run_lda.
    Calls Claude API to label each topic.
    Patches labels into result rows and writes to ClickHouse.

Why three tasks?
    - Re-run label_and_write alone if Claude times out or ClickHouse fails
      — no Spark restart, no re-transcription.
    - Re-run run_lda alone if you change LDA params (different k, max_iter)
      without touching transcripts.
    - Each task has its own Airflow log, retry count, and duration.

Prerequisites (run manually, or add to this DAG)
-------------------------------------------------
  make discover         — refresh tracked video list from YouTube API
  make download-videos  — pull top-satisfaction videos into MinIO

Configuration
-------------
Secrets come from the Airflow container's environment, injected by
docker-compose from the host .env file. Required:

  POSTGRES_USER, POSTGRES_PASSWORD
  MINIO_ACCESS_KEY, MINIO_SECRET_KEY
  CLICKHOUSE_USER, CLICKHOUSE_PASSWORD
  ANTHROPIC_API_KEY
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

# ── Image & network ───────────────────────────────────────────────────────────
SPARK_IMAGE = "youtube-insights/spark:latest"
INFRA_NETWORK = os.environ.get("INFRA_DOCKER_NETWORK", "infra_default")
SPARK_MASTER_URL = "spark://spark-master:7077"

_SPARK_SUBMIT = [
    "/opt/spark/bin/spark-submit",
    "--master", SPARK_MASTER_URL,
    "--conf", "spark.pyspark.python=python3",
    "--conf", "spark.executorEnv.PYTHONPATH=/opt/spark/work/batch",
]


def _batch_env() -> dict[str, str]:
    return {
        "POSTGRES_HOST": "postgres",
        "POSTGRES_PORT": "5432",
        "POSTGRES_USER": os.environ.get("POSTGRES_USER", ""),
        "POSTGRES_PASSWORD": os.environ.get("POSTGRES_PASSWORD", ""),
        "INGESTION_DB": os.environ.get("INGESTION_DB", "ingestion"),
        "MINIO_ENDPOINT": "minio:9000",
        "MINIO_ACCESS_KEY": os.environ.get("MINIO_ACCESS_KEY", ""),
        "MINIO_SECRET_KEY": os.environ.get("MINIO_SECRET_KEY", ""),
        "MINIO_VIDEOS_BUCKET": os.environ.get("MINIO_VIDEOS_BUCKET", "videos"),
        "CLICKHOUSE_HOST": "clickhouse",
        "CLICKHOUSE_HTTP_PORT": "8123",
        "CLICKHOUSE_USER": os.environ.get("CLICKHOUSE_USER", ""),
        "CLICKHOUSE_PASSWORD": os.environ.get("CLICKHOUSE_PASSWORD", ""),
        "SPARK_MASTER": SPARK_MASTER_URL,
        "PYTHONPATH": "/opt/spark/work/batch",
        "WHISPER_MODEL": os.environ.get("WHISPER_MODEL", "base"),
        "LDA_MAX_TOPICS": os.environ.get("LDA_MAX_TOPICS", "10"),
        "LDA_MAX_ITER": os.environ.get("LDA_MAX_ITER", "20"),
        "ANTHROPIC_API_KEY": os.environ.get("ANTHROPIC_API_KEY", ""),
    }


with DAG(
    dag_id="batch_topic_modeling",
    description="Weekly: transcribe → LDA → Claude labels → ClickHouse",
    schedule="@weekly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "owner": "data-platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    },
    tags=["batch", "spark", "nlp"],
) as dag:

    # ── Task 1 ────────────────────────────────────────────────────────────────
    # Transcribes any video that doesn't yet have a .txt in MinIO.
    # Already-cached videos are read and skipped — idempotent, fast on re-runs.
    transcribe = DockerOperator(
        task_id="transcribe",
        image=SPARK_IMAGE,
        api_version="auto",
        auto_remove=True,
        command=_SPARK_SUBMIT + ["/opt/spark/work/batch/jobs/transcribe_job.py"],
        environment=_batch_env(),
        network_mode=INFRA_NETWORK,
        mount_tmp_dir=False,
        # Cold run with many videos can be slow — Whisper on CPU.
        execution_timeout=timedelta(hours=4),
    )

    # ── Task 2 ────────────────────────────────────────────────────────────────
    # Reads transcripts from MinIO cache (no Whisper), runs LDA, saves
    # topic_words_map + unlabelled result rows to MinIO as JSON.
    # Retry this task alone to re-run LDA with different params.
    run_lda = DockerOperator(
        task_id="run_lda",
        image=SPARK_IMAGE,
        api_version="auto",
        auto_remove=True,
        command=_SPARK_SUBMIT + ["/opt/spark/work/batch/jobs/lda_job.py"],
        environment=_batch_env(),
        network_mode=INFRA_NETWORK,
        mount_tmp_dir=False,
        execution_timeout=timedelta(hours=2),
    )

    # ── Task 3 ────────────────────────────────────────────────────────────────
    # No Spark — plain Python only (Claude API + ClickHouse write).
    # Loads the JSON from MinIO, labels topics via Claude, writes ClickHouse.
    # Retry this task alone if Claude times out or the ClickHouse write fails.
    label_and_write = DockerOperator(
        task_id="label_and_write",
        image=SPARK_IMAGE,
        api_version="auto",
        auto_remove=True,
        # Plain python3 — no spark-submit overhead since there is no Spark work.
        command=[
            "python3", "/opt/spark/work/batch/jobs/label_and_write_job.py",
        ],
        environment=_batch_env(),
        network_mode=INFRA_NETWORK,
        mount_tmp_dir=False,
        execution_timeout=timedelta(minutes=30),
    )

    transcribe >> run_lda >> label_and_write
