"""
Transcription Job
=================
Ensures every downloaded video has a cached transcript in MinIO.

For each video in the ``downloaded_videos`` table:
  - If ``{channel_id}/{video_id}.txt`` already exists in MinIO  → reads it (fast).
  - If not                                                       → transcribes with
    Whisper on a Spark worker and writes the result to MinIO.

Idempotent: safe to re-run. Only videos without a cached transcript consume
GPU/CPU time. Run this before ``topic_modeling_job`` in the Airflow DAG.
"""

from __future__ import annotations

import time
from pyspark.sql import Row, SparkSession

from utils.config import BatchConfig
from utils.transcribe import MinioCfg, TranscriptionStage
from utils.video_repository import VideoRepository


class TranscribeJob:
    def __init__(self, cfg: BatchConfig) -> None:
        self._cfg = cfg
        self._repo = VideoRepository(cfg)

    def run(self) -> None:
        print("=== Transcription Job ===")

        videos = self._repo.get_downloaded_videos()
        print(f"Found {len(videos)} downloaded video(s) in Postgres.")
        if not videos:
            print("Nothing to transcribe. Exiting.")
            return

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

        minio_cfg = MinioCfg(
            endpoint=self._cfg.minio_endpoint,
            access_key=self._cfg.minio_access_key,
            secret_key=self._cfg.minio_secret_key,
            bucket=self._cfg.minio_bucket,
        )

        spark = self._build_spark_session()
        try:
            print(f"\nTranscribing via Whisper model '{self._cfg.whisper_model}'...")
            t0 = time.time()

            stage = TranscriptionStage(minio_cfg, self._cfg.whisper_model)
            # run() transparently reads from MinIO cache for already-transcribed
            # videos and only calls Whisper for the remainder.
            df = stage.run(spark, video_rows)
            count = df.count()

            print(f"\n=== Transcription complete: {count} transcript(s) ready in {time.time() - t0:.1f}s ===")
        finally:
            spark.stop()

    def _build_spark_session(self) -> SparkSession:
        spark = (
            SparkSession.builder
            .appName("youtube-transcribe")
            .master(self._cfg.spark_master)
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        return spark


def main() -> None:
    job = TranscribeJob(BatchConfig())
    job.run()


if __name__ == "__main__":
    main()
