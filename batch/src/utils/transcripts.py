from __future__ import annotations

from collections.abc import Iterable, Iterator

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from utils.config import MinioConfig
from utils.storage import ObjectStore
from utils.video_repository import DownloadedVideo

TRANSCRIPT_SCHEMA = StructType([
    StructField("video_id", StringType(), nullable=False),
    StructField("satisfaction_pct", DoubleType(), nullable=False),
    StructField("transcript", StringType(), nullable=False),
])


def transcript_key(video: DownloadedVideo) -> str:
    return f"{video.channel_id}/{video.video_id}.txt"


def load_cached_transcripts(
    spark: SparkSession, minio_cfg: MinioConfig, videos: list[DownloadedVideo]
) -> DataFrame:
    def read_partition(partition: Iterable[DownloadedVideo]) -> Iterator[tuple[str, float, str]]:
        store = ObjectStore(minio_cfg)
        for video in partition:
            transcript = store.read(transcript_key(video))
            if transcript is None:
                print(f"  [WARN] No cached transcript for {video.video_id} — skipping.")
                continue
            yield video.video_id, video.satisfaction_pct, transcript.decode("utf-8")

    rdd = spark.sparkContext.parallelize(videos).mapPartitions(read_partition)
    return spark.createDataFrame(rdd, TRANSCRIPT_SCHEMA)
