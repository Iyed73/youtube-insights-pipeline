"""
Manages the batch_watermarks table in PostgreSQL using psycopg2 directly
(not Spark JDBC) for three reasons:

  1. Single-row ops — no value in spinning up a distributed read.
  2. Must be available BEFORE SparkSession touches any data, so we can build
     the incremental WHERE clause before Spark's query planner sees it.
  3. Must be fully ACID — a failed or partial run must NOT advance the mark.

Public API
──────────
  get_watermark(job_name)           → datetime | None
  mark_running(job_name)            → None
  set_watermark(job_name, ...)      → None
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

import psycopg2
import psycopg2.extras

from config import cfg   # src/ on PYTHONPATH

logger = logging.getLogger(__name__)


# ── Connection helper ────────────────────────────────────────────────────────

def _connect() -> "psycopg2.connection":
    pg = cfg.postgres
    return psycopg2.connect(
        host     = pg.host,
        port     = pg.port,
        dbname   = pg.database,
        user     = pg.user,
        password = pg.password,
    )


# ── Public API ────────────────────────────────────────────────────────────────

def get_watermark(job_name: str) -> datetime | None:
    """
    Return the last_processed_at for job_name.
    Returns None on the very first run (no row exists yet), which triggers
    a full table scan in the calling job.
    """
    sql = """
        SELECT last_processed_at
        FROM   batch_watermarks
        WHERE  job_name = %s
    """
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (job_name,))
            row = cur.fetchone()

    if row is None or row[0] is None:
        logger.info("[watermark] No prior watermark for '%s' — full scan will run.", job_name)
        return None

    ts: datetime = row[0]
    # Ensure the timestamp is tz-aware so comparisons with now() are safe
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)

    logger.info("[watermark] '%s' last processed at %s.", job_name, ts.isoformat())
    return ts


def mark_running(job_name: str) -> None:
    """
    Upsert a run_status='running' row at job start.
    Lets an operator detect stuck/crashed jobs by querying
    WHERE run_status = 'running' AND last_run_at < now() - interval '2 hours'.
    """
    sql = """
        INSERT INTO batch_watermarks (job_name, run_status, last_run_at)
        VALUES (%s, 'running', now())
        ON CONFLICT (job_name)
        DO UPDATE SET
            run_status  = 'running',
            last_run_at = now()
    """
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (job_name,))
        conn.commit()
    logger.info("[watermark] '%s' marked as running.", job_name)


def set_watermark(
    job_name:       str,
    new_watermark:  datetime,
    rows_processed: int = 0,
    status:         str = "success",
    meta:           dict[str, Any] | None = None,
) -> None:
    """
    Advance the watermark after a successful (or failed) run.

    Parameters
    ----------
    job_name        : matches the job_name used in mark_running()
    new_watermark   : MAX(downloaded_at) of the batch just processed.
                      The next run will filter WHERE downloaded_at > this.
    rows_processed  : record count for observability
    status          : 'success' | 'failed'
    meta            : arbitrary JSON blob (Spark app-id, error message, etc.)
    """
    sql = """
        INSERT INTO batch_watermarks
               (job_name, last_processed_at, last_run_at, run_status, rows_processed, meta)
        VALUES (%s,        %s,               now(),       %s,         %s,             %s)
        ON CONFLICT (job_name)
        DO UPDATE SET
            last_processed_at = EXCLUDED.last_processed_at,
            last_run_at       = now(),
            run_status        = EXCLUDED.run_status,
            rows_processed    = EXCLUDED.rows_processed,
            meta              = EXCLUDED.meta
    """
    meta_json = json.dumps(meta) if meta else None

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (job_name, new_watermark, status, rows_processed, meta_json))
        conn.commit()

    logger.info(
        "[watermark] '%s' → %s  (%d rows, status=%s)",
        job_name, new_watermark.isoformat(), rows_processed, status,
    )