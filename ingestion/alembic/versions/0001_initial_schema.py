"""Initial schema: tracked_videos table."""
from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import TIMESTAMP as PgTIMESTAMP

revision: str = "0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "tracked_videos",
        sa.Column("video_id", sa.String(20), primary_key=True),
        sa.Column("channel_id", sa.String(30), nullable=False),
        sa.Column("channel_name", sa.Text, nullable=False),
        sa.Column("title", sa.Text, nullable=False),
        sa.Column("view_count", sa.BigInteger, nullable=False, server_default="0"),
        sa.Column("published_at", PgTIMESTAMP(timezone=True), nullable=False),
        sa.Column(
            "added_at",
            PgTIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("last_polled_at", PgTIMESTAMP(timezone=True), nullable=True),
        sa.Column("last_comment_at", PgTIMESTAMP(timezone=True), nullable=True),
        sa.Column("status", sa.String(10), nullable=False, server_default="active"),
        sa.CheckConstraint(
            "status IN ('active', 'removed')", name="tracked_videos_status_check"
        ),
    )
    op.create_index(
        "idx_tracked_videos_channel_status",
        "tracked_videos",
        ["channel_id", "status"],
    )
    op.create_index(
        "idx_tracked_videos_active",
        "tracked_videos",
        ["status"],
        postgresql_where=sa.text("status = 'active'"),
    )


def downgrade() -> None:
    op.drop_index("idx_tracked_videos_active", table_name="tracked_videos")
    op.drop_index("idx_tracked_videos_channel_status", table_name="tracked_videos")
    op.drop_table("tracked_videos")
