from __future__ import annotations

import time
import uuid
from datetime import datetime, timezone

from pyspark.sql import Row, SparkSession

from utils.clickhouse_writer import ClickHouseWriter
from utils.config import BatchConfig
from utils.topic_labeler import label_topics
from utils.topic_model import TopicModelingStage
from utils.transcribe import MinioCfg, TranscriptionStage
from utils.video_repository import VideoRepository


class TopicModelingJob:
    """Orchestrates the full batch pipeline: transcribe → LDA → write to ClickHouse."""

    def __init__(self, cfg: BatchConfig) -> None:
        self._cfg = cfg
        self._repo = VideoRepository(cfg)
        self._writer = ClickHouseWriter(cfg)

    def run(self) -> None:
        run_id = str(uuid.uuid4())
        run_date = datetime.now(timezone.utc)

        print("=== Batch Topic Modeling ===")
        print(f"run_id:   {run_id}")
        print(f"run_date: {run_date}")

        # ── Step 1: Fetch videos from Postgres ────────────────────────────
        videos = self._repo.get_downloaded_videos()
        print(f"\nFound {len(videos)} downloaded video(s) in Postgres.")
        if not videos:
            print("Nothing to process. Exiting.")
            return

        spark = self._build_spark_session()
        try:
            self._run_pipeline(spark, videos, run_id, run_date)
        finally:
            spark.stop()

        print("\n=== Batch job complete ===")

    def _run_pipeline(
        self, spark: SparkSession, videos: list[dict], run_id: str, run_date
    ) -> None:
        cfg = self._cfg

        # ── Step 2: Load transcripts from MinIO cache ─────────────────────
        # Expects the transcribe_job to have run first (Airflow task dependency).
        # load_from_cache() reads .txt files from MinIO and never calls Whisper,
        # so this job is fast and has no ML dependency.
        print("\n[Step 2/5] Loading transcripts from MinIO cache...")
        t0 = time.time()
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
        for v in videos:
            print(f"  queued: {v['video_id']}  ({v.get('title', v['video_id'])})")
        minio_cfg = MinioCfg(
            endpoint=cfg.minio_endpoint,
            access_key=cfg.minio_access_key,
            secret_key=cfg.minio_secret_key,
            bucket=cfg.minio_bucket,
        )
        transcription = TranscriptionStage(minio_cfg, cfg.whisper_model)
        transcript_df = transcription.load_from_cache(spark, video_rows)
        transcript_df.cache()

        total = transcript_df.count()
        print(f"[Step 2/5] Loaded {total} cached transcript(s) in {time.time() - t0:.1f}s.")

        if total < 6:
            print("Need at least 6 transcripts for meaningful LDA. Skipping topic modeling.")
            return

        # ── Step 3: LDA topic modeling ────────────────────────────────────
        print("\n[Step 3/5] Running Spark ML pipeline (tokenize → stop words → lemmatize → vectorize → LDA)...")
        t0 = time.time()
        lda_stage = TopicModelingStage(cfg.lda_max_iter, cfg.lda_max_topics)
        lda_model, cv_model, transformed_df = lda_stage.run(transcript_df)

        ll = lda_model.logLikelihood(transformed_df)
        lp = lda_model.logPerplexity(transformed_df)
        print(f"  LDA log-likelihood: {ll:.2f}, log-perplexity: {lp:.4f}")

        num_topics = lda_model.describeTopics().count()
        print(f"  Discovered {num_topics} topics:")
        lda_stage.describe_topics(lda_model, cv_model)
        print(f"[Step 3/5] Topic modeling done in {time.time() - t0:.1f}s.")

        # ── Step 4: Label topics via Claude ─────────────────────────────
        print("\n[Step 4/5] Labeling topics via Claude API...")
        t0 = time.time()
        topic_words_map = lda_stage.get_topic_words_map(lda_model, cv_model)
        topic_labels = label_topics(cfg.anthropic_api_key, topic_words_map)
        for tid, label in sorted(topic_labels.items()):
            print(f"  Topic {tid}: {label}")
        print(f"[Step 4/5] Labeling done in {time.time() - t0:.1f}s.")

        results = lda_stage.extract_results(
            lda_model, transformed_df, cv_model, run_id, run_date, topic_labels
        )

        # ── Step 5: Write to ClickHouse ───────────────────────────────────
        print("\n[Step 5/5] Writing results to ClickHouse...")
        t0 = time.time()
        self._writer.ensure_tables()
        self._writer.delete_previous_runs()
        self._writer.write_topic_summary(results.topic_summary)
        self._writer.write_topic_words(results.topic_words)
        print(
            f"[Step 5/5] Wrote {len(results.topic_summary)} topic_summary rows "
            f"and {len(results.topic_words)} topic_words rows in {time.time() - t0:.1f}s."
        )

    def _build_spark_session(self) -> SparkSession:
        spark = (
            SparkSession.builder
            .appName("youtube-topic-modeling")
            .master(self._cfg.spark_master)
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        return spark


def main() -> None:
    job = TopicModelingJob(BatchConfig())
    job.run()


if __name__ == "__main__":
    main()
