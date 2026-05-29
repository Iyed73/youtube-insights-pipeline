from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass
class BatchConfig:
    # PostgreSQL (ingestion DB)
    postgres_host: str = field(default_factory=lambda: os.environ["POSTGRES_HOST"])
    postgres_port: int = field(default_factory=lambda: int(os.environ.get("POSTGRES_PORT", "5432")))
    postgres_user: str = field(default_factory=lambda: os.environ["POSTGRES_USER"])
    postgres_password: str = field(default_factory=lambda: os.environ["POSTGRES_PASSWORD"])
    ingestion_db: str = field(default_factory=lambda: os.environ.get("INGESTION_DB", "ingestion"))

    # MinIO
    minio_endpoint: str = field(default_factory=lambda: os.environ["MINIO_ENDPOINT"])
    minio_access_key: str = field(default_factory=lambda: os.environ["MINIO_ACCESS_KEY"])
    minio_secret_key: str = field(default_factory=lambda: os.environ["MINIO_SECRET_KEY"])
    minio_bucket: str = field(default_factory=lambda: os.environ.get("MINIO_VIDEOS_BUCKET", "videos"))

    # ClickHouse
    clickhouse_host: str = field(default_factory=lambda: os.environ["CLICKHOUSE_HOST"])
    clickhouse_http_port: int = field(
        default_factory=lambda: int(os.environ.get("CLICKHOUSE_HTTP_PORT", "8123"))
    )
    clickhouse_user: str = field(default_factory=lambda: os.environ["CLICKHOUSE_USER"])
    clickhouse_password: str = field(default_factory=lambda: os.environ["CLICKHOUSE_PASSWORD"])

    # Spark
    spark_master: str = field(
        default_factory=lambda: os.environ.get("SPARK_MASTER", "spark://spark-master:7077")
    )

    # Whisper
    whisper_model: str = field(default_factory=lambda: os.environ.get("WHISPER_MODEL", "base"))

    # LDA
    lda_max_topics: int = field(
        default_factory=lambda: int(os.environ.get("LDA_MAX_TOPICS", "10"))
    )
    lda_max_iter: int = field(
        default_factory=lambda: int(os.environ.get("LDA_MAX_ITER", "20"))
    )

    # Claude API (topic labeling)
    anthropic_api_key: str = field(
        default_factory=lambda: os.environ.get("ANTHROPIC_API_KEY", "")
    )

    @property
    def postgres_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.ingestion_db}"
        )
