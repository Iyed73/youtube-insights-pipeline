"""
Single source of truth for all batch-layer configuration.

Resolution order for the .env file:
  1. Walk up from this file's location (batch/src/) → batch/ → project root
  2. The root .env sits two levels above batch/src/config.py:
       project_root/.env
         └── batch/
               └── src/
                     └── config.py   ← here
  3. python-dotenv loads it once at import time; subsequent imports reuse
     the module-level singleton `cfg`.

All jobs do:
    from config import cfg
and get typed, validated settings with no further boilerplate.
"""

from __future__ import annotations

import os
import warnings
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

# ── Locate project root and .env ─────────────────────────────────────────────
# batch/src/config.py → parent = batch/src → parent = batch/ → parent = root
_ROOT    = Path(__file__).resolve().parent.parent.parent
_ENV_FILE = _ROOT / ".env"

if _ENV_FILE.exists():
    load_dotenv(dotenv_path=_ENV_FILE, override=False)
else:
    warnings.warn(
        f".env not found at {_ENV_FILE}. "
        "Falling back to environment variables already set in the shell.",
        stacklevel=2,
    )


# ── Helper ────────────────────────────────────────────────────────────────────
def _require(key: str) -> str:
    val = os.environ.get(key, "").strip()
    if not val:
        raise EnvironmentError(
            f"Required environment variable '{key}' is not set. "
            f"Add it to {_ENV_FILE}."
        )
    return val


# ── Typed config dataclasses ─────────────────────────────────────────────────

@dataclass(frozen=True)
class PostgresConfig:
    host:     str
    port:     int
    user:     str
    password: str
    database: str

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"

    @property
    def jdbc_properties(self) -> dict[str, str]:
        return {
            "user":     self.user,
            "password": self.password,
            "driver":   "org.postgresql.Driver",
        }

    @property
    def sqlalchemy_url(self) -> str:
        """Used by Alembic and direct psycopg2/SQLAlchemy calls (outside Spark)."""
        return (
            f"postgresql+psycopg2://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )


@dataclass(frozen=True)
class MinIOConfig:
    endpoint:   str    # e.g. "localhost:9002"  (no scheme)
    access_key: str
    secret_key: str
    bucket:     str    # MINIO_VIDEOS_BUCKET

    @property
    def endpoint_url(self) -> str:
        """Full http:// URL used by boto3 and the s3a config."""
        return f"http://{self.endpoint}"

    def s3a_path(self, object_key: str) -> str:
        """Build a fully-qualified  s3a://bucket/key  URI."""
        key = object_key.lstrip("/")
        # Strip bucket prefix if the stored path already includes it
        # (e.g. minio_path="videos/UCxxx/vid.mp4" but bucket="videos")
        if key.startswith(f"{self.bucket}/"):
            key = key[len(self.bucket) + 1:]
        return f"s3a://{self.bucket}/{key}"


@dataclass(frozen=True)
class WhisperConfig:
    model_size: str       = "base"    # tiny | base | small | medium | large
    device:     str       = "cpu"     # cpu | cuda
    batch_size: int       = 8         # videos per driver-side processing loop
    language:   str | None = None     # None → Whisper auto-detects


@dataclass(frozen=True)
class LDAConfig:
    num_topics:   int   = 20
    max_iter:     int   = 50
    vocab_size:   int   = 10_000
    min_doc_freq: int   = 5           # ignore terms appearing in < N docs
    max_doc_freq: float = 0.90        # ignore terms appearing in > 90% of docs
    top_terms:    int   = 10          # top terms to store per topic


@dataclass(frozen=True)
class BatchConfig:
    postgres: PostgresConfig
    minio:    MinIOConfig
    whisper:  WhisperConfig
    lda:      LDAConfig

    # Spark cluster settings
    spark_master:           str = "local[*]"
    spark_driver_memory:    str = "4g"
    spark_executor_memory:  str = "4g"

    # ── Canonical job names (match rows in batch_watermarks) ─────────────────
    # Defined as plain class attributes (not dataclass fields) so they are
    # accessible as BatchConfig.JOB_WHISPER without an instance.
    JOB_WHISPER: str = field(default="whisper_transcript_extraction", init=False, repr=False)
    JOB_LDA:     str = field(default="lda_topic_modeling",            init=False, repr=False)

    def __post_init__(self) -> None:
        # frozen=True requires object.__setattr__ for post-init assignment
        object.__setattr__(self, "JOB_WHISPER", "whisper_transcript_extraction")
        object.__setattr__(self, "JOB_LDA",     "lda_topic_modeling")


# ── Build the module-level singleton ─────────────────────────────────────────
cfg = BatchConfig(
    postgres=PostgresConfig(
        host     = os.environ.get("POSTGRES_HOST",     "localhost"),
        port     = int(os.environ.get("POSTGRES_PORT", "5432")),
        user     = _require("POSTGRES_USER"),
        password = _require("POSTGRES_PASSWORD"),
        database = _require("INGESTION_DB"),          # ← INGESTION_DB, not POSTGRES_DB
    ),
    minio=MinIOConfig(
        endpoint   = os.environ.get("MINIO_ENDPOINT",       "localhost:9002"),
        access_key = _require("MINIO_ACCESS_KEY"),
        secret_key = _require("MINIO_SECRET_KEY"),
        bucket     = os.environ.get("MINIO_VIDEOS_BUCKET",  "videos"),
    ),
    whisper=WhisperConfig(
        model_size = os.environ.get("WHISPER_MODEL",        "base"),
        device     = os.environ.get("WHISPER_DEVICE",       "cpu"),
        batch_size = int(os.environ.get("WHISPER_BATCH_SIZE", "8")),
        language   = os.environ.get("WHISPER_LANGUAGE")  or None,
    ),
    lda=LDAConfig(
        num_topics   = int(os.environ.get("LDA_NUM_TOPICS",     "20")),
        max_iter     = int(os.environ.get("LDA_MAX_ITERATIONS", "50")),
        vocab_size   = int(os.environ.get("LDA_VOCAB_SIZE",     "10000")),
        min_doc_freq = int(os.environ.get("LDA_MIN_DOC_FREQ",   "5")),
        top_terms    = int(os.environ.get("LDA_TOP_TERMS",      "10")),
    ),
    spark_master          = os.environ.get("SPARK_MASTER",           "local[*]"),
    spark_driver_memory   = os.environ.get("SPARK_DRIVER_MEMORY",    "4g"),
    spark_executor_memory = os.environ.get("SPARK_EXECUTOR_MEMORY",  "4g"),
)