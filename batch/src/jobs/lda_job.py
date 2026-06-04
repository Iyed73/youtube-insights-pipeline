from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timezone

from utils.config import LdaConfig, MinioConfig, PostgresConfig
from utils.exit_codes import EXIT_NOTHING_TO_DO
from utils.lda_store import LdaResultStore
from utils.spark import build_spark_session
from utils.topic_model import MIN_DOCUMENTS, TopicModelingStage
from utils.topics import LdaResult
from utils.transcripts import load_cached_transcripts
from utils.video_repository import fetch_downloaded_videos


class LdaJob:
    def __init__(self, run_id: str) -> None:
        self._run_id = run_id
        self._postgres_cfg = PostgresConfig.from_env()
        self._minio_cfg = MinioConfig.from_env()
        self._lda_cfg = LdaConfig.from_env()

    def run(self) -> int:
        run_date = datetime.now(timezone.utc)

        print("=== LDA Job ===")
        print(f"run_id:   {self._run_id}")
        print(f"run_date: {run_date}")

        videos = fetch_downloaded_videos(self._postgres_cfg)
        print(f"\nFound {len(videos)} downloaded video(s).")
        if len(videos) < MIN_DOCUMENTS:
            print(f"Need at least {MIN_DOCUMENTS} videos for meaningful LDA. Exiting.")
            return EXIT_NOTHING_TO_DO

        spark = build_spark_session("youtube-lda")
        try:
            print("\n[Step 1/2] Loading transcripts from MinIO cache...")
            t0 = time.time()
            transcripts = load_cached_transcripts(spark, self._minio_cfg, videos).cache()
            total = transcripts.count()
            print(f"  {total} transcript(s) loaded in {time.time() - t0:.1f}s.")

            if total < MIN_DOCUMENTS:
                print(f"Need at least {MIN_DOCUMENTS} transcripts for meaningful LDA. Exiting.")
                return EXIT_NOTHING_TO_DO

            print("\n[Step 2/2] Running Spark ML pipeline (tokenize → vectorize → LDA)...")
            t0 = time.time()
            topics = TopicModelingStage(self._lda_cfg).run(transcripts)
            print(f"  LDA done in {time.time() - t0:.1f}s.")
        finally:
            spark.stop()

        for topic in topics:
            print(f"  Topic {topic.topic_id}: {', '.join(w.word for w in topic.words[:5])}")

        LdaResultStore(self._minio_cfg).save(LdaResult(self._run_id, run_date, topics))
        print("\n=== LDA job complete — result saved to MinIO ===")
        return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Fit LDA topics on the cached transcripts.")
    parser.add_argument(
        "--run-id", required=True, help="Pipeline run id; names the result file in MinIO."
    )
    return LdaJob(parser.parse_args().run_id).run()


if __name__ == "__main__":
    sys.exit(main())
