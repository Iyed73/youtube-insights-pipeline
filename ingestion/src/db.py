"""PostgreSQL helpers for ingestion state management.

All functions accept an open SQLAlchemy ``Session`` and are intentionally
kept as thin wrappers around ORM operations so callers stay in full control
of transaction boundaries.
"""

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
    """Open and return a new SQLAlchemy session to the ingestion database."""
    return Session(_get_engine())


# ---------------------------------------------------------------------------
# channel_discovery helpers
# ---------------------------------------------------------------------------


def get_active_videos_for_channel(session: Session, channel_id: str) -> list[TrackedVideo]:
    """Return all active tracked videos for *channel_id*, ordered oldest first."""
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
    """Mark multiple videos as removed in one statement."""
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
    """Insert a new tracked video, or re-activate it if it was previously removed."""
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


# ---------------------------------------------------------------------------
# comment_poller helpers
# ---------------------------------------------------------------------------


def get_active_videos(session: Session) -> list[TrackedVideo]:
    """Return all active tracked videos, ordered so the longest-unpolled come first."""
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
    """Advance the poll cursor for *video_id*.

    *last_comment_at* — wall-clock time when new comments were last found
                        (used by the eviction check).
    *comment_cursor*  — published_at of the newest comment seen so far
                        (used as the cutoff on the next poll).
    """
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
    """Mark *video_id* as removed so comment_poller stops polling it."""
    session.execute(
        update(TrackedVideo)
        .where(TrackedVideo.video_id == video_id)
        .values(status="removed")
    )
    session.commit()


# ---------------------------------------------------------------------------
# video_downloader helpers
# ---------------------------------------------------------------------------


def is_video_downloaded(session: Session, video_id: str) -> bool:
    """Return True if *video_id* has already been successfully downloaded."""
    row = session.execute(
        select(DownloadedVideo)
        .where(DownloadedVideo.video_id == video_id, DownloadedVideo.status == "completed")
    ).scalar_one_or_none()
    return row is not None


def get_unprocessed_downloaded_videos(session: Session) -> list[DownloadedVideo]:
    """Return all successfully downloaded videos that have not been processed in the silver layer."""
    return list(
        session.execute(
            select(DownloadedVideo)
            .where(
                DownloadedVideo.status == "completed",
                DownloadedVideo.processed_for_cuts == False,
            )
        ).scalars()
    )


def mark_video_processed_for_cuts(session: Session, video_id: str) -> None:
    """Mark *video_id* as processed for cuts."""
    session.execute(
        update(DownloadedVideo)
        .where(DownloadedVideo.video_id == video_id)
        .values(processed_for_cuts=True)
    )
    session.commit()


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
    """Insert or update a row in downloaded_videos."""
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
