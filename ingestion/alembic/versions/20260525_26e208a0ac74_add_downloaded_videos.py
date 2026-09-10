"""add_downloaded_videos"""
from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = '26e208a0ac74'
down_revision: Union[str, None] = '75ba6fdf9712'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table('downloaded_videos',
    sa.Column('video_id', sa.String(length=20), nullable=False),
    sa.Column('channel_id', sa.String(length=30), nullable=False),
    sa.Column('title', sa.Text(), nullable=False),
    sa.Column('minio_path', sa.Text(), nullable=False),
    sa.Column('satisfaction_pct', sa.Float(), nullable=False),
    sa.Column('downloaded_at', postgresql.TIMESTAMP(timezone=True), server_default=sa.text('now()'), nullable=False),
    sa.Column('status', sa.String(length=10), nullable=False),
    sa.Column('error', sa.Text(), nullable=True),
    sa.CheckConstraint("status IN ('completed', 'failed')", name='downloaded_videos_status_check'),
    sa.PrimaryKeyConstraint('video_id')
    )


def downgrade() -> None:
    op.drop_table('downloaded_videos')
