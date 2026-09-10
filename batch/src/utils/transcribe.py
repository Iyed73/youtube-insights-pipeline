from __future__ import annotations

import os
import tempfile
from collections.abc import Iterable, Iterator

from faster_whisper import WhisperModel
from pyspark.sql import SparkSession

from utils.config import MinioConfig, WhisperConfig
from utils.storage import ObjectStore
from utils.transcripts import transcript_key
from utils.video_repository import DownloadedVideo


class TranscriptionStage:
    # Instance state must stay picklable: Spark ships _transcribe_partition to executors.
    def __init__(self, minio_cfg: MinioConfig, whisper_cfg: WhisperConfig) -> None:
        self._minio_cfg = minio_cfg
        self._whisper_cfg = whisper_cfg

    def run(self, spark: SparkSession, videos: list[DownloadedVideo]) -> int:
        rdd = spark.sparkContext.parallelize(videos)
        return rdd.mapPartitions(self._transcribe_partition).count()

    def _transcribe_partition(self, partition: Iterable[DownloadedVideo]) -> Iterator[str]:
        store = ObjectStore(self._minio_cfg)
        model: WhisperModel | None = None

        for video in partition:
            key = transcript_key(video)
            if store.exists(key):
                print(f"  [cached] {video.video_id}")
                yield video.video_id
                continue

            if model is None:
                print(f"  [whisper] loading model '{self._whisper_cfg.model_size}'...")
                model = WhisperModel(
                    self._whisper_cfg.model_size,
                    device="cpu",
                    compute_type="int8",
                    local_files_only=True,  # baked into the image
                )

            print(f"  [transcribe] {video.video_id} — downloading from MinIO...")
            transcript = self._transcribe(store, model, video.minio_path)
            print(f"  [transcribe] {video.video_id} — done ({len(transcript.split())} words)")
            store.write(key, transcript.encode("utf-8"), content_type="text/plain")
            yield video.video_id

    @staticmethod
    def _transcribe(store: ObjectStore, model: WhisperModel, minio_path: str) -> str:
        with tempfile.TemporaryDirectory() as tmp_dir:
            video_path = os.path.join(tmp_dir, "video.mp4")
            store.download(minio_path, video_path)
            segments, _ = model.transcribe(video_path, language="en")
            return " ".join(segment.text.strip() for segment in segments)
