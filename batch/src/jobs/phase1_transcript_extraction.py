"""
Phase 1 — Incremental Whisper Transcript Extraction
=====================================================

Execution flow
──────────────
  1. Read the batch_watermarks table to get the last successful run timestamp.
  2. Query downloaded_videos WHERE downloaded_at > watermark
       AND video_id NOT IN (SELECT video_id FROM transcripts)
     → only net-new videos that haven't been transcribed yet.
  3. Collect (video_id, minio_path, downloaded_at) rows to the DRIVER.
  4. Load the Whisper model once on the driver.
  5. Process videos in batches of cfg.whisper.batch_size:
       a. Download .mp4 from MinIO via boto3 → local temp file.
       b. Run model.transcribe() → text, language, segments.
       c. Accumulate result dicts in memory.
  6. Convert result list → Spark DataFrame → JDBC append to `transcripts`.
  7. Advance the watermark to MAX(downloaded_at) of the processed batch.

Why Whisper runs on the driver (not in Spark UDFs)
───────────────────────────────────────────────────
Whisper loads a ~145 MB model into memory at startup.  Shipping this inside
a UDF would reload the model per partition per executor — enormous overhead
on a Spark Standalone cluster.  Driver-side batching loads it exactly once
and streams all videos through the same model instance.
"""

from __future__ import annotations

import logging
import traceback
from datetime import datetime, timezone

import whisper
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from config import cfg
from spark_session import get_spark_session
from utils.minio_client import download_video
from utils.postgres import read_query, write_table
from utils.watermark import get_watermark, mark_running, set_watermark

logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# ── Schema ────────────────────────────────────────────────────────────────────
TRANSCRIPT_SCHEMA = StructType([
    StructField("video_id",          StringType(),    nullable=False),
    StructField("minio_path",        StringType(),    nullable=False),
    StructField("transcript_text",   StringType(),    nullable=True),
    StructField("language",          StringType(),    nullable=True),
    StructField("whisper_model",     StringType(),    nullable=True),
    StructField("duration_seconds",  FloatType(),     nullable=True),
    StructField("word_count",        IntegerType(),   nullable=True),
    StructField("extraction_status", StringType(),    nullable=False),
    StructField("error_message",     StringType(),    nullable=True),
    StructField("extracted_at",      TimestampType(), nullable=False),
])


# ── Helpers ───────────────────────────────────────────────────────────────────

def _fetch_pending_videos(spark: SparkSession, watermark: datetime | None) -> list[Row]:
    """
    Return downloaded_videos rows that are new and not yet transcribed.
    The double-safety guard (watermark + NOT IN subquery) makes the job
    idempotent: rerunning after a crash will skip already-processed videos.
    """
    wm_clause = (
        f"AND dv.downloaded_at > '{watermark.isoformat()}'"
        if watermark else ""
    )
    query = f"""
        SELECT dv.video_id,
               dv.minio_path,
               dv.downloaded_at
        FROM   downloaded_videos dv
        WHERE  dv.status = 'downloaded'
          {wm_clause}
          AND  dv.video_id NOT IN (
                   SELECT video_id FROM transcripts
               )
        ORDER BY dv.downloaded_at ASC
    """
    logger.info("[phase1] Fetching pending videos.")
    rows = read_query(spark, query, alias="pending").collect()
    logger.info("[phase1] %d videos pending transcription.", len(rows))
    return rows


def _transcribe_batch(
    rows:       list[Row],
    model:      "whisper.Whisper",
    model_name: str,
) -> list[dict]:
    """
    Download each .mp4, run Whisper, return result dicts.
    Per-video errors are caught and recorded so one bad file doesn't abort
    the entire batch.
    """
    results = []
    now     = datetime.now(tz=timezone.utc)

    for row in rows:
        video_id   = row["video_id"]
        minio_path = row["minio_path"]
        logger.info("[phase1] Transcribing  video_id=%-20s  path=%s", video_id, minio_path)

        try:
            with download_video(minio_path) as local_path:
                result = model.transcribe(
                    local_path,
                    language = cfg.whisper.language,
                    verbose  = False,
                    fp16     = False,   # fp16=False is required for CPU inference
                )

            text     = (result.get("text") or "").strip()
            language = result.get("language")
            segments = result.get("segments", [])
            duration = float(segments[-1]["end"]) if segments else None
            words    = len(text.split()) if text else 0

            results.append({
                "video_id":          video_id,
                "minio_path":        minio_path,
                "transcript_text":   text or None,
                "language":          language,
                "whisper_model":     model_name,
                "duration_seconds":  duration,
                "word_count":        words,
                "extraction_status": "success",
                "error_message":     None,
                "extracted_at":      now,
            })
            logger.info(
                "[phase1] ✓  video_id=%s  lang=%s  words=%d  dur=%.1fs",
                video_id, language, words, duration or 0,
            )

        except Exception as exc:
            logger.error("[phase1] ✗  video_id=%s  error=%s", video_id, exc)
            results.append({
                "video_id":          video_id,
                "minio_path":        minio_path,
                "transcript_text":   None,
                "language":          None,
                "whisper_model":     model_name,
                "duration_seconds":  None,
                "word_count":        None,
                "extraction_status": "failed",
                "error_message":     traceback.format_exc(limit=5),
                "extracted_at":      now,
            })

    return results


# ── Entry point ───────────────────────────────────────────────────────────────

def run(spark: SparkSession | None = None) -> None:
    """
    Run Phase 1.  Accepts an optional SparkSession so the pipeline
    orchestrator can share a single session across both phases.
    """
    owns_spark = spark is None
    if owns_spark:
        spark = get_spark_session("Phase1_WhisperTranscriptExtraction")

    watermark: datetime | None = None   # scoped for except block

    try:
        mark_running(cfg.JOB_WHISPER)
        watermark = get_watermark(cfg.JOB_WHISPER)

        pending = _fetch_pending_videos(spark, watermark)
        if not pending:
            logger.info("[phase1] Nothing to do — no new videos since last run.")
            set_watermark(cfg.JOB_WHISPER, datetime.now(tz=timezone.utc), 0)
            return

        # ── Load Whisper model once on the driver ─────────────────────────────
        logger.info(
            "[phase1] Loading Whisper '%s' on %s …",
            cfg.whisper.model_size, cfg.whisper.device,
        )
        model = whisper.load_model(cfg.whisper.model_size, device=cfg.whisper.device)
        logger.info("[phase1] Model ready.")

        # ── Process in batches ────────────────────────────────────────────────
        bs      = cfg.whisper.batch_size
        chunks  = [pending[i : i + bs] for i in range(0, len(pending), bs)]
        logger.info("[phase1] %d videos → %d batches (batch_size=%d).", len(pending), len(chunks), bs)

        all_results: list[dict] = []
        for idx, chunk in enumerate(chunks, 1):
            logger.info("[phase1] Batch %d/%d …", idx, len(chunks))
            all_results.extend(_transcribe_batch(chunk, model, cfg.whisper.model_size))

        # ── Write to Postgres ─────────────────────────────────────────────────
        # Left-anti join against already-existing video_ids (crash-safety)
        result_df = spark.createDataFrame(
            [Row(**r) for r in all_results],
            schema=TRANSCRIPT_SCHEMA,
        )
        existing_df = read_query(
            spark,
            "SELECT video_id FROM transcripts",
            alias="existing",
        ).select(F.col("video_id").alias("_existing_vid"))

        new_df = result_df.join(
            existing_df,
            result_df.video_id == F.col("_existing_vid"),
            how="left_anti",
        )

        rows_written = new_df.count()
        if rows_written > 0:
            write_table(new_df, "transcripts", mode="append")
        else:
            logger.info("[phase1] All results already present — nothing written.")

        # ── Advance watermark ─────────────────────────────────────────────────
        max_dl_at = max(r["downloaded_at"] for r in pending)
        if isinstance(max_dl_at, datetime) and max_dl_at.tzinfo is None:
            max_dl_at = max_dl_at.replace(tzinfo=timezone.utc)

        set_watermark(
            cfg.JOB_WHISPER,
            new_watermark   = max_dl_at,
            rows_processed  = rows_written,
            status          = "success",
            meta            = {
                "total":   len(pending),
                "success": sum(1 for r in all_results if r["extraction_status"] == "success"),
                "failed":  sum(1 for r in all_results if r["extraction_status"] == "failed"),
                "model":   cfg.whisper.model_size,
            },
        )
        logger.info("[phase1] ✓ Phase 1 complete — %d transcripts written.", rows_written)

    except Exception as exc:
        logger.exception("[phase1] Unhandled error: %s", exc)
        set_watermark(
            cfg.JOB_WHISPER,
            new_watermark   = watermark or datetime(1970, 1, 1, tzinfo=timezone.utc),
            rows_processed  = 0,
            status          = "failed",
            meta            = {"error": str(exc)},
        )
        raise

    finally:
        if owns_spark:
            spark.stop()


if __name__ == "__main__":
    run()