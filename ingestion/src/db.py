from __future__ import annotations

import os
from datetime import datetime

from sqlalchemy import create_engine, nullsfirst, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from models import DownloadedVideo, TrackedVideo

_engine: Engine | None = None


def _get_engine() -> Engine:
    global _engine
    if _engine is None:
        host = os.environ["POSTGRES_HOST"]
        port = os.environ.get("POSTGRES_PORT", "5432")
        user = os.environ["POSTGRES_USER"]
        password = os.environ["POSTGRES_PASSWORD"]
        dbname = os.environ["INGESTION_DB"]
        _engine = create_engine(
            f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
        )
    return _engine


def get_session() -> Session:
    return Session(_get_engine())


def get_active_videos_for_channel(session: Session, channel_id: str) -> list[TrackedVideo]:
    return list(
        session.execute(
            select(TrackedVideo)
            .where(
                TrackedVideo.channel_id == channel_id,
                TrackedVideo.status == "active",
            )
            .order_by(TrackedVideo.published_at)
        ).scalars()
    )


def mark_videos_removed(session: Session, video_ids: list[str]) -> None:
    if not video_ids:
        return
    session.execute(
        update(TrackedVideo)
        .where(TrackedVideo.video_id.in_(video_ids))
        .values(status="removed")
    )
    session.commit()


def insert_tracked_video(
    session: Session,
    *,
    video_id: str,
    channel_id: str,
    channel_name: str,
    title: str,
    view_count: int,
    published_at: datetime,
) -> None:
    stmt = (
        pg_insert(TrackedVideo)
        .values(
            video_id=video_id,
            channel_id=channel_id,
            channel_name=channel_name,
            title=title,
            view_count=view_count,
            published_at=published_at,
        )
        .on_conflict_do_update(
            index_elements=["video_id"],
            set_={"title": title, "view_count": view_count, "status": "active"},
        )
    )
    session.execute(stmt)
    session.commit()


def get_active_videos(session: Session) -> list[TrackedVideo]:
    return list(
        session.execute(
            select(TrackedVideo)
            .where(TrackedVideo.status == "active")
            .order_by(nullsfirst(TrackedVideo.last_polled_at))
        ).scalars()
    )


def update_video_poll(
    session: Session,
    video_id: str,
    last_polled_at: datetime,
    last_comment_at: datetime | None = None,
    comment_cursor: datetime | None = None,
) -> None:
    values: dict = {"last_polled_at": last_polled_at}
    if last_comment_at is not None:
        values["last_comment_at"] = last_comment_at
    if comment_cursor is not None:
        values["comment_cursor"] = comment_cursor
    session.execute(
        update(TrackedVideo).where(TrackedVideo.video_id == video_id).values(**values)
    )
    session.commit()


def mark_video_removed(session: Session, video_id: str) -> None:
    session.execute(
        update(TrackedVideo)
        .where(TrackedVideo.video_id == video_id)
        .values(status="removed")
    )
    session.commit()


def is_video_downloaded(session: Session, video_id: str) -> bool:
    row = session.execute(
        select(DownloadedVideo)
        .where(DownloadedVideo.video_id == video_id, DownloadedVideo.status == "completed")
    ).scalar_one_or_none()
    return row is not None


def upsert_downloaded_video(
    session: Session,
    *,
    video_id: str,
    channel_id: str,
    title: str,
    minio_path: str,
    satisfaction_pct: float,
    status: str,
    error: str | None = None,
) -> None:
    stmt = (
        pg_insert(DownloadedVideo)
        .values(
            video_id=video_id,
            channel_id=channel_id,
            title=title,
            minio_path=minio_path,
            satisfaction_pct=satisfaction_pct,
            status=status,
            error=error,
        )
        .on_conflict_do_update(
            index_elements=["video_id"],
            set_={
                "minio_path": minio_path,
                "satisfaction_pct": satisfaction_pct,
                "status": status,
                "error": error,
            },
        )
    )
    session.execute(stmt)
    session.commit()
