from __future__ import annotations

from sqlalchemy import create_engine, text

from utils.config import BatchConfig


class VideoRepository:
    """Read-only access to downloaded video records in PostgreSQL."""

    def __init__(self, cfg: BatchConfig) -> None:
        self._engine = create_engine(cfg.postgres_url)

    def get_downloaded_videos(self) -> list[dict]:
        """Return all successfully downloaded videos with their channel names."""
        sql = text("""
            SELECT dv.video_id, dv.channel_id, dv.title, dv.minio_path, dv.satisfaction_pct,
                   tv.channel_name
            FROM downloaded_videos dv
            LEFT JOIN tracked_videos tv ON dv.video_id = tv.video_id
            WHERE dv.status = 'completed'
        """)
        with self._engine.connect() as conn:
            rows = conn.execute(sql)
            return [dict(r._mapping) for r in rows]
