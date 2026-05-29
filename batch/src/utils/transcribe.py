from __future__ import annotations

import io
import tempfile
from collections.abc import Iterator
from dataclasses import dataclass

from faster_whisper import WhisperModel
from minio import Minio
from minio.error import S3Error
from pyspark.sql import DataFrame, Row, SparkSession


@dataclass
class MinioCfg:
    """Serialisable MinIO config — safe to pass to Spark workers via closure."""

    endpoint: str
    access_key: str
    secret_key: str
    bucket: str


class TranscriptionStage:
    """Spark stage that transcribes videos using Whisper.

    Designed as a picklable callable for use with RDD.mapPartitions.
    Instance state is limited to plain config values — no open connections
    or model objects — so Spark can safely serialise and ship it to workers.
    The Whisper model is loaded lazily on the first uncached video in each
    partition to avoid redundant initialisation.
    """

    def __init__(self, minio_cfg: MinioCfg, whisper_model_size: str) -> None:
        self._minio_cfg = minio_cfg
        self._whisper_model_size = whisper_model_size

    # ── Spark entry point ─────────────────────────────────────────────────

    def run(self, spark: SparkSession, video_rows: list[Row]) -> DataFrame:
        """Parallelise video metadata and return a DataFrame with transcripts.

        For each video: reads the cached .txt from MinIO if it exists, otherwise
        transcribes with Whisper and writes the result back to cache.
        """
        rdd = spark.sparkContext.parallelize(video_rows)
        transcript_rdd = rdd.mapPartitions(self)
        return spark.createDataFrame(transcript_rdd)

    def load_from_cache(self, spark: SparkSession, video_rows: list[Row]) -> DataFrame:
        """Return a transcript DataFrame built exclusively from MinIO cache.

        Videos whose .txt cache file is absent are skipped with a warning.
        Whisper is never loaded. Raises RuntimeError if no transcripts are found.
        """
        rdd = spark.sparkContext.parallelize(video_rows)
        transcript_rdd = rdd.mapPartitions(self._yield_cached_only)
        df = spark.createDataFrame(transcript_rdd)
        if df.rdd.isEmpty():
            raise RuntimeError(
                "No cached transcripts found in MinIO. "
                "Run the transcribe job before topic modeling."
            )
        return df

    def _yield_cached_only(self, rows: Iterator[Row]) -> Iterator[Row]:
        """Spark worker function: emit cached transcripts, warn-and-skip missing ones."""
        client = self._make_minio_client()
        for row in rows:
            key = f"{row.channel_id}/{row.video_id}.txt"
            transcript = self._get_cached(client, key)
            if transcript is None:
                print(f"  [WARN] No cached transcript for {row.video_id} — skipping.")
                continue
            yield self._make_row(row, transcript)

    def __call__(self, rows: Iterator[Row]) -> Iterator[Row]:
        """Called by each Spark worker on its partition of video rows."""
        model: WhisperModel | None = None
        client = self._make_minio_client()

        for row in rows:
            key = f"{row.channel_id}/{row.video_id}.txt"

            transcript = self._get_cached(client, key)
            if transcript is not None:
                print(f"  [cached] {row.video_id}")
                yield self._make_row(row, transcript)
                continue

            if model is None:
                print(f"  [whisper] loading model '{self._whisper_model_size}'...")
                model = WhisperModel(
                    self._whisper_model_size, device="cpu", compute_type="int8"
                )
                print(f"  [whisper] model loaded")

            print(f"  [transcribe] {row.video_id} — downloading from MinIO...")
            transcript = self._transcribe(client, model, row.minio_path)
            print(f"  [transcribe] {row.video_id} — done ({len(transcript.split())} words)")
            self._cache(client, key, transcript)
            yield self._make_row(row, transcript)

    # ── Internal helpers ──────────────────────────────────────────────────

    def _make_minio_client(self) -> Minio:
        cfg = self._minio_cfg
        return Minio(cfg.endpoint, access_key=cfg.access_key, secret_key=cfg.secret_key, secure=False)

    def _get_cached(self, client: Minio, key: str) -> str | None:
        try:
            resp = client.get_object(self._minio_cfg.bucket, key)
            text = resp.read().decode("utf-8")
            resp.close()
            resp.release_conn()
            return text
        except S3Error as exc:
            if exc.code == "NoSuchKey":
                return None
            raise

    def _transcribe(self, client: Minio, model: WhisperModel, minio_path: str) -> str:
        with tempfile.NamedTemporaryFile(suffix=".mp4") as tmp:
            resp = client.get_object(self._minio_cfg.bucket, minio_path)
            for chunk in resp.stream(1024 * 1024):
                tmp.write(chunk)
            resp.close()
            resp.release_conn()
            tmp.flush()
            segments, _ = model.transcribe(tmp.name, language="en")
            return " ".join(seg.text.strip() for seg in segments)

    def _cache(self, client: Minio, key: str, text: str) -> None:
        data = text.encode("utf-8")
        client.put_object(
            self._minio_cfg.bucket, key, io.BytesIO(data),
            length=len(data), content_type="text/plain",
        )

    @staticmethod
    def _make_row(row: Row, transcript: str) -> Row:
        return Row(
            video_id=row.video_id,
            channel_id=row.channel_id,
            channel_name=row.channel_name,
            satisfaction_pct=row.satisfaction_pct,
            transcript=transcript,
        )
