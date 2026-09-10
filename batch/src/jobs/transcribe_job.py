from __future__ import annotations

import sys
import time

from utils.config import MinioConfig, PostgresConfig, WhisperConfig
from utils.exit_codes import EXIT_NOTHING_TO_DO
from utils.spark import build_spark_session
from utils.transcribe import TranscriptionStage
from utils.video_repository import fetch_downloaded_videos


class TranscribeJob:
    def __init__(self) -> None:
        self._postgres_cfg = PostgresConfig.from_env()
        self._minio_cfg = MinioConfig.from_env()
        self._whisper_cfg = WhisperConfig.from_env()

    def run(self) -> int:
        print("=== Transcription Job ===")

        videos = fetch_downloaded_videos(self._postgres_cfg)
        print(f"Found {len(videos)} downloaded video(s) in Postgres.")
        if not videos:
            print("Nothing to transcribe.")
            return EXIT_NOTHING_TO_DO

        spark = build_spark_session("youtube-transcribe")
        try:
            print(f"\nTranscribing via Whisper model '{self._whisper_cfg.model_size}'...")
            t0 = time.time()
            count = TranscriptionStage(self._minio_cfg, self._whisper_cfg).run(spark, videos)
        finally:
            spark.stop()

        print(
            f"\n=== Transcription complete: {count} transcript(s) ready "
            f"in {time.time() - t0:.1f}s ==="
        )
        return 0


def main() -> int:
    return TranscribeJob().run()


if __name__ == "__main__":
    sys.exit(main())
