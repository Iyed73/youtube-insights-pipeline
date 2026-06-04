from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import URL, create_engine, text

from utils.config import PostgresConfig


@dataclass(frozen=True)
class DownloadedVideo:
    video_id: str
    channel_id: str
    minio_path: str
    satisfaction_pct: float


def fetch_downloaded_videos(cfg: PostgresConfig) -> list[DownloadedVideo]:
    engine = create_engine(
        URL.create(
            "postgresql+psycopg2",
            username=cfg.user,
            password=cfg.password,
            host=cfg.host,
            port=cfg.port,
            database=cfg.database,
        )
    )
    sql = text("""
        SELECT video_id, channel_id, minio_path, satisfaction_pct
        FROM downloaded_videos
        WHERE status = 'completed'
    """)
    try:
        with engine.connect() as conn:
            return [DownloadedVideo(**row._mapping) for row in conn.execute(sql)]
    finally:
        engine.dispose()
