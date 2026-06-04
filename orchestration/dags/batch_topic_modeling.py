from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG

from common.containers import (
    SPARK_IMAGE,
    anthropic_env,
    clickhouse_env,
    docker_task,
    minio_env,
    optional_env,
    postgres_env,
)

SPARK_SUBMIT = ["/opt/spark/bin/spark-submit", "--master", "spark://spark-master:7077"]
JOBS_DIR = "/opt/spark/work/batch/jobs"
# Must match EXIT_NOTHING_TO_DO in batch/src/utils/exit_codes.py.
EXIT_NOTHING_TO_DO = 99
# Scopes the LDA result in MinIO to this DAG run, so tasks never read another run's output.
RUN_ID = "{{ ts_nodash }}"

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

    transcribe = docker_task(
        task_id="transcribe",
        image=SPARK_IMAGE,
        command=[*SPARK_SUBMIT, f"{JOBS_DIR}/transcribe_job.py"],
        private_environment={**postgres_env(), **minio_env()},
        skip_on_exit_code=EXIT_NOTHING_TO_DO,
        execution_timeout=timedelta(hours=4),
    )

    run_lda = docker_task(
        task_id="run_lda",
        image=SPARK_IMAGE,
        command=[*SPARK_SUBMIT, f"{JOBS_DIR}/lda_job.py", "--run-id", RUN_ID],
        environment=optional_env("LDA_MAX_TOPICS", "LDA_MAX_ITER"),
        private_environment={**postgres_env(), **minio_env()},
        skip_on_exit_code=EXIT_NOTHING_TO_DO,
        execution_timeout=timedelta(hours=2),
    )

    label_and_write = docker_task(
        task_id="label_and_write",
        image=SPARK_IMAGE,
        command=["python3", f"{JOBS_DIR}/label_and_write_job.py", "--run-id", RUN_ID],
        environment=optional_env("ANTHROPIC_MODEL"),
        private_environment={**minio_env(), **clickhouse_env(), **anthropic_env()},
        execution_timeout=timedelta(minutes=30),
    )

    transcribe >> run_lda >> label_and_write
