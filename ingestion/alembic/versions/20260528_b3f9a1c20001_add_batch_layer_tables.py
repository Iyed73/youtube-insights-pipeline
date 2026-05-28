"""create batch layer tables: batch_watermarks, transcripts, video_topics

Revision ID: 20260528_b3f9a1c20001_add_batch_layer_tables
Revises: 26e208a0ac74
Create Date: 2026-05-28

NOTE ON down_revision
─────────────────────
This revision chains directly onto 26e208a0ac74 — the
`add_downloaded_videos` migration (your current Alembic head).

Before running `alembic upgrade head`, confirm the chain is intact:
    cd ingestion && alembic heads
Expected output (single head):
    26e208a0ac74 (head)

If your head hash differs, update `down_revision` below accordingly.
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

# ── Alembic revision identifiers ─────────────────────────────────────────────
revision      = "b3f9a1c20001"
down_revision = "26e208a0ac74"   # ← add_downloaded_videos is current head
branch_labels = None
depends_on    = None


def upgrade() -> None:

    # ── 1. batch_watermarks ──────────────────────────────────────────────────
    # One row per logical batch job.  Tracks the high-water timestamp so
    # incremental jobs know exactly where to resume after each run.
    #
    # Key columns:
    #   job_name           — e.g. 'whisper_transcript_extraction'
    #   last_processed_at  — MAX(downloaded_at) of the last processed batch.
    #                        Next run filters WHERE downloaded_at > this value.
    #   run_status         — 'pending' | 'running' | 'success' | 'failed'
    #                        'running' rows older than ~2 h indicate a crash.
    op.create_table(
        "batch_watermarks",
        sa.Column("id",       sa.Integer(), nullable=False),
        sa.Column(
            "job_name",
            sa.String(length=128),
            nullable=False,
            comment="Logical job identifier, e.g. 'whisper_transcript_extraction'",
        ),
        sa.Column(
            "last_processed_at",
            sa.DateTime(timezone=True),
            nullable=True,
            comment=(
                "Watermark: videos with downloaded_at <= this were already processed. "
                "NULL on the very first run (triggers a full table scan)."
            ),
        ),
        sa.Column(
            "last_run_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
            comment="Wall-clock timestamp of the last write to this row",
        ),
        sa.Column(
            "run_status",
            sa.String(length=32),
            nullable=False,
            server_default="pending",
            comment="pending | running | success | failed",
        ),
        sa.Column(
            "rows_processed",
            sa.Integer(),
            nullable=True,
            comment="Number of records handled in the last run",
        ),
        sa.Column(
            "meta",
            JSONB,
            nullable=True,
            comment="Arbitrary run metadata: Spark app-id, error messages, model info, etc.",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("job_name", name="uq_batch_watermarks_job_name"),
    )
    op.create_index(
        "ix_batch_watermarks_job_name",
        "batch_watermarks",
        ["job_name"],
        unique=True,
    )

    # ── 2. transcripts ───────────────────────────────────────────────────────
    # One row per video.  Raw Whisper output plus derived quality fields.
    # video_id is UNIQUE — re-running Phase 1 after a crash uses a left-anti
    # join to skip videos that are already present here.
    op.create_table(
        "transcripts",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column(
            "video_id",
            sa.String(length=64),
            nullable=False,
            comment="YouTube video ID — references downloaded_videos.video_id",
        ),
        sa.Column(
            "minio_path",
            sa.Text(),
            nullable=False,
            comment="Full MinIO object key used to fetch the .mp4 (for audit / re-run)",
        ),
        sa.Column(
            "transcript_text",
            sa.Text(),
            nullable=True,
            comment="Full verbatim transcript produced by Whisper",
        ),
        sa.Column(
            "language",
            sa.String(length=16),
            nullable=True,
            comment="ISO 639-1 language code auto-detected by Whisper",
        ),
        sa.Column(
            "whisper_model",
            sa.String(length=32),
            nullable=True,
            comment="Whisper model variant used: tiny | base | small | medium | large",
        ),
        sa.Column(
            "duration_seconds",
            sa.Float(),
            nullable=True,
            comment="Audio duration derived from Whisper segment end-times",
        ),
        sa.Column(
            "word_count",
            sa.Integer(),
            nullable=True,
        ),
        sa.Column(
            "extraction_status",
            sa.String(length=32),
            nullable=False,
            server_default="success",
            comment="success | failed | skipped",
        ),
        sa.Column(
            "error_message",
            sa.Text(),
            nullable=True,
            comment="Populated only when extraction_status = 'failed'",
        ),
        sa.Column(
            "extracted_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("video_id", name="uq_transcripts_video_id"),
    )
    op.create_index("ix_transcripts_video_id",     "transcripts", ["video_id"])
    op.create_index("ix_transcripts_extracted_at", "transcripts", ["extracted_at"])
    op.create_index("ix_transcripts_language",     "transcripts", ["language"])
    op.create_index(
        "ix_transcripts_status_extracted",
        "transcripts",
        ["extraction_status", "extracted_at"],
    )

    # ── 3. video_topics ──────────────────────────────────────────────────────
    # One row per video per clustering run.
    # run_id is the ISO-timestamp string set at Phase 2 job start so you can
    # query a specific model generation:
    #   SELECT * FROM video_topics WHERE run_id = '20260528T120000Z'
    op.create_table(
        "video_topics",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column(
            "video_id",
            sa.String(length=64),
            nullable=False,
            comment="YouTube video ID",
        ),
        sa.Column(
            "run_id",
            sa.String(length=64),
            nullable=False,
            comment="Unique identifier for the LDA clustering run (ISO timestamp)",
        ),
        sa.Column(
            "dominant_topic_id",
            sa.Integer(),
            nullable=True,
            comment="Index of the highest-probability LDA topic (0-based)",
        ),
        sa.Column(
            "topic_distribution",
            sa.Text(),
            nullable=True,
            comment=(
                "Full topic probability vector serialised as JSON array: "
                "[{topic_id: int, probability: float}, ...]"
            ),
        ),
        sa.Column(
            "top_terms",
            sa.Text(),
            nullable=True,
            comment="Top-N vocabulary terms for the dominant topic at run time (JSON array of strings)",
        ),
        sa.Column(
            "num_topics",
            sa.Integer(),
            nullable=True,
            comment="K value used in the LDA model for this run",
        ),
        sa.Column(
            "model_version",
            sa.String(length=256),
            nullable=True,
            comment="s3a:// path of the serialised LDA model for this run",
        ),
        sa.Column(
            "assigned_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "video_id", "run_id",
            name="uq_video_topics_video_run",
        ),
    )
    op.create_index("ix_video_topics_video_id",  "video_topics", ["video_id"])
    op.create_index("ix_video_topics_run_id",    "video_topics", ["run_id"])
    op.create_index(
        "ix_video_topics_dominant_topic",
        "video_topics",
        ["dominant_topic_id"],
    )


def downgrade() -> None:
    op.drop_table("video_topics")
    op.drop_table("transcripts")
    op.drop_table("batch_watermarks")