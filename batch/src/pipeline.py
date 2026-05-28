"""
Batch pipeline orchestrator — the spark-submit entry point.

  Phase 1  Incremental Whisper transcript extraction
  Phase 2  Global LDA topic modeling

Both phases share a single SparkSession (one JVM start-up per pipeline run).
Phase 2 is skipped if Phase 1 produced no new transcripts, unless
--force-phase2 is passed or --phase 2 is specified explicitly.

Usage (via submit.sh — the recommended launcher):
  bash batch/submit.sh                           # full pipeline
  bash batch/submit.sh --phase 1                 # Phase 1 only
  bash batch/submit.sh --phase 2 --force-phase2  # Phase 2 only
"""
from __future__ import annotations

import argparse
import logging
import sys
import time

from spark_session import get_spark_session
from jobs.phase1_transcript_extraction import run as run_phase1
from jobs.phase2_topic_modeling import run as run_phase2

logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("batch.pipeline")


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="YouTube Trend Analysis — Batch Pipeline"
    )
    p.add_argument(
        "--phase",
        choices=["1", "2", "all"],
        default="all",
        help="Which phase(s) to run (default: all)",
    )
    p.add_argument(
        "--force-phase2",
        action="store_true",
        default=False,
        help="Run Phase 2 even when Phase 1 produced no new transcripts",
    )
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    t0   = time.perf_counter()

    logger.info("━" * 62)
    logger.info("  YouTube Batch Pipeline — starting")
    logger.info("  --phase=%s   --force-phase2=%s", args.phase, args.force_phase2)
    logger.info("━" * 62)

    spark  = get_spark_session("YouTubeBatchPipeline")
    p1_ran = False
    p2_ran = False

    try:
        if args.phase in ("1", "all"):
            ts = time.perf_counter()
            logger.info("── Phase 1 : Transcript Extraction ─────────────────")
            run_phase1(spark=spark)
            p1_ran = True
            logger.info("── Phase 1 done  (%.1f s) ──────────────────────────", time.perf_counter() - ts)

        run_p2 = args.phase == "2" or (
            args.phase == "all" and (p1_ran or args.force_phase2)
        )
        if run_p2:
            ts = time.perf_counter()
            logger.info("── Phase 2 : Topic Modeling ─────────────────────────")
            run_phase2(spark=spark)
            p2_ran = True
            logger.info("── Phase 2 done  (%.1f s) ──────────────────────────", time.perf_counter() - ts)
        else:
            logger.info("── Phase 2 skipped (p1_ran=%s, force=%s)", p1_ran, args.force_phase2)

        elapsed = time.perf_counter() - t0
        logger.info("━" * 62)
        logger.info("  Pipeline finished in %.1f s  (p1=%s  p2=%s)", elapsed, p1_ran, p2_ran)
        logger.info("━" * 62)

    except Exception as exc:
        logger.exception("Pipeline failed: %s", exc)
        sys.exit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()