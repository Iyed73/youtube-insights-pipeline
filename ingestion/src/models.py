"""Shared data-transfer objects and ORM models for the ingestion package."""

from __future__ import annotations

import datetime
from dataclasses import dataclass

from sqlalchemy import BigInteger, CheckConstraint, Index, String, Text, func, text
from sqlalchemy.dialects.postgresql import TIMESTAMP as PgTIMESTAMP
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


# ---------------------------------------------------------------------------
# YouTube API data-transfer objects (not persisted directly)
# ---------------------------------------------------------------------------


@dataclass
class Video:
    """A YouTube video returned by the YouTube Data API."""

    video_id: str
    channel_id: str
    title: str
    view_count: int
    published_at: datetime.datetime


@dataclass
class Comment:
    """A top-level comment returned by the YouTube Data API."""

    comment_id: str
    video_id: str
    channel_id: str
    author_channel_id: str | None
    author_display_name: str
    text: str
    like_count: int
    published_at: datetime.datetime
    updated_at: datetime.datetime


# ---------------------------------------------------------------------------
# SQLAlchemy ORM models
# ---------------------------------------------------------------------------


class Base(DeclarativeBase):
    pass


class TrackedVideo(Base):
    """ORM model for the tracked_videos Postgres table."""

    __tablename__ = "tracked_videos"
    __table_args__ = (
        CheckConstraint("status IN ('active', 'removed')", name="tracked_videos_status_check"),
        Index("idx_tracked_videos_channel_status", "channel_id", "status"),
        Index(
            "idx_tracked_videos_active",
            "status",
            postgresql_where=text("status = 'active'"),
        ),
    )

    video_id: Mapped[str] = mapped_column(String(20), primary_key=True)
    channel_id: Mapped[str] = mapped_column(String(30), nullable=False)
    channel_name: Mapped[str] = mapped_column(Text, nullable=False)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    view_count: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    published_at: Mapped[datetime.datetime] = mapped_column(
        PgTIMESTAMP(timezone=True), nullable=False
    )
    added_at: Mapped[datetime.datetime] = mapped_column(
        PgTIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )
    last_polled_at: Mapped[datetime.datetime | None] = mapped_column(PgTIMESTAMP(timezone=True))
    last_comment_at: Mapped[datetime.datetime | None] = mapped_column(PgTIMESTAMP(timezone=True))
    # Newest comment published_at seen so far — used as the cutoff on the next
    # poll so we only fetch comments published after this timestamp.
    comment_cursor: Mapped[datetime.datetime | None] = mapped_column(PgTIMESTAMP(timezone=True))
    status: Mapped[str] = mapped_column(String(10), nullable=False, default="active")
