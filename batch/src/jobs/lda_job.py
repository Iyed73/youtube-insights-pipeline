"""
LDA Job
=======
Step 2 of the batch pipeline (DAG task: run_lda).

Reads transcript .txt files from MinIO cache (written by transcribe_job),
runs the full Spark ML pipeline:

  tokenize → remove stop words → lemmatize → CountVectorize → LDA

Then extracts per-video topic distributions and per-topic word weights
(without labels — labels are added by label_and_write_job in the next
task) and saves everything to MinIO as a JSON handoff file.

No Claude API call, no ClickHouse write — this job is purely Spark ML.
"""

from __future__ import annotations

import time
import uuid
from datetime import datetime, timezone

from pyspark.sql import Row, SparkSession

from utils.config import BatchConfig
from utils.lda_store import LdaStore
from utils.topic_model import TopicModelingStage
from utils.transcribe import MinioCfg, TranscriptionStage
from utils.video_repository import VideoRepository


class LdaJob:
    def __init__(self, cfg: BatchConfig) -> None:
        self._cfg = cfg
        self._repo = VideoRepository(cfg)

    def run(self) -> None:
        run_id = str(uuid.uuid4())
        run_date = datetime.now(timezone.utc)

        print("=== LDA Job ===")
        print(f"run_id:   {run_id}")
        print(f"run_date: {run_date}")

        videos = self._repo.get_downloaded_videos()
        print(f"\nFound {len(videos)} downloaded video(s).")
        if not videos:
            print("Nothing to process. Exiting.")
            return

        minio_cfg = MinioCfg(
            endpoint=self._cfg.minio_endpoint,
            access_key=self._cfg.minio_access_key,
            secret_key=self._cfg.minio_secret_key,
            bucket=self._cfg.minio_bucket,
        )
        video_rows = [
            Row(
                video_id=v["video_id"],
                channel_id=v["channel_id"],
                channel_name=v.get("channel_name") or v["channel_id"],
                minio_path=v["minio_path"],
                satisfaction_pct=float(v["satisfaction_pct"]),
            )
            for v in videos
        ]

        spark = self._build_spark_session()
        try:
            # ── Step 1: Load transcripts from MinIO cache ──────────────────
            print("\n[Step 1/2] Loading transcripts from MinIO cache...")
            t0 = time.time()
            stage = TranscriptionStage(minio_cfg, self._cfg.whisper_model)
            transcript_df = stage.load_from_cache(spark, video_rows)
            transcript_df.cache()
            total = transcript_df.count()
            print(f"  {total} transcript(s) loaded in {time.time() - t0:.1f}s.")

            if total < 6:
                print(f"Need at least 6 transcripts for meaningful LDA (got {total}). Exiting.")
                return

            # ── Step 2: Run Spark ML → LDA ─────────────────────────────────
            print("\n[Step 2/2] Running Spark ML pipeline (tokenize → vectorize → LDA)...")
            t0 = time.time()
            lda_stage = TopicModelingStage(self._cfg.lda_max_iter, self._cfg.lda_max_topics)
            lda_model, cv_model, transformed_df = lda_stage.run(transcript_df)

            ll = lda_model.logLikelihood(transformed_df)
            lp = lda_model.logPerplexity(transformed_df)
            print(f"  log-likelihood: {ll:.2f}  log-perplexity: {lp:.4f}")
            lda_stage.describe_topics(lda_model, cv_model)
            print(f"  LDA done in {time.time() - t0:.1f}s.")

            # Build the topic-words map for Claude labeling in the next step.
            topic_words_map = lda_stage.get_topic_words_map(lda_model, cv_model)

            # Extract results with no labels yet (topic_label fields left empty).
            # extract_results collects everything from Spark to Python memory,
            # so it is safe to call spark.stop() immediately after.
            results = lda_stage.extract_results(
                lda_model, transformed_df, cv_model, run_id, run_date, topic_labels=None
            )

        finally:
            spark.stop()

        # ── Save to MinIO for label_and_write_job ──────────────────────────
        store = LdaStore(minio_cfg)
        store.save({
            "run_id": run_id,
            "run_date": run_date.isoformat(),
            "topic_words_map": topic_words_map,
            "video_topics": results.video_topics,
            "topic_words": results.topic_words,
        })
        print("\n=== LDA job complete — results saved to MinIO ===")

    def _build_spark_session(self) -> SparkSession:
        spark = (
            SparkSession.builder
            .appName("youtube-lda")
            .master(self._cfg.spark_master)
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        return spark


def main() -> None:
    LdaJob(BatchConfig()).run()


if __name__ == "__main__":
    main()
