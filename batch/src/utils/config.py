from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class PostgresConfig:
    host: str
    port: int
    user: str
    password: str
    database: str

    @classmethod
    def from_env(cls) -> PostgresConfig:
        return cls(
            host=os.environ["POSTGRES_HOST"],
            port=int(os.environ.get("POSTGRES_PORT", "5432")),
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
            database=os.environ.get("INGESTION_DB", "ingestion"),
        )


@dataclass(frozen=True)
class MinioConfig:
    endpoint: str
    access_key: str
    secret_key: str
    bucket: str

    @classmethod
    def from_env(cls) -> MinioConfig:
        return cls(
            endpoint=os.environ["MINIO_ENDPOINT"],
            access_key=os.environ["MINIO_ACCESS_KEY"],
            secret_key=os.environ["MINIO_SECRET_KEY"],
            bucket=os.environ.get("MINIO_VIDEOS_BUCKET", "videos"),
        )


@dataclass(frozen=True)
class ClickHouseConfig:
    host: str
    port: int
    user: str
    password: str

    @classmethod
    def from_env(cls) -> ClickHouseConfig:
        return cls(
            host=os.environ["CLICKHOUSE_HOST"],
            port=int(os.environ.get("CLICKHOUSE_HTTP_PORT", "8123")),
            user=os.environ["CLICKHOUSE_USER"],
            password=os.environ["CLICKHOUSE_PASSWORD"],
        )


@dataclass(frozen=True)
class WhisperConfig:
    # Set by infra/spark/Dockerfile to the model baked into the image.
    model_size: str

    @classmethod
    def from_env(cls) -> WhisperConfig:
        return cls(model_size=os.environ["WHISPER_MODEL"])


@dataclass(frozen=True)
class LdaConfig:
    max_topics: int
    max_iter: int

    @classmethod
    def from_env(cls) -> LdaConfig:
        return cls(
            max_topics=int(os.environ.get("LDA_MAX_TOPICS", "20")),
            max_iter=int(os.environ.get("LDA_MAX_ITER", "30")),
        )


@dataclass(frozen=True)
class LabelingConfig:
    api_key: str
    model: str

    @classmethod
    def from_env(cls) -> LabelingConfig:
        return cls(
            api_key=os.environ.get("ANTHROPIC_API_KEY", ""),
            model=os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-4-6"),
        )
