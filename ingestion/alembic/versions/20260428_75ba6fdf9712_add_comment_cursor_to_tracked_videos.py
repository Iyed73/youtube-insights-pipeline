"""add comment_cursor to tracked_videos"""
from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = '75ba6fdf9712'
down_revision: Union[str, None] = '0001'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('tracked_videos', sa.Column('comment_cursor', postgresql.TIMESTAMP(timezone=True), nullable=True))


def downgrade() -> None:
    op.drop_column('tracked_videos', 'comment_cursor')
