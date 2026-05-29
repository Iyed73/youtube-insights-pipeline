"""
scene_detection.py — Spark Batch Job (Phase 2 + 3)

Uses PySceneDetect to analyze "cut rate" (editing pace) for every video stored
in MinIO, then emits per-video metrics that can be joined against ClickHouse
comment-sentiment data to correlate editing style with viewer satisfaction.

Big Data Metrics produced per video:
  - cuts              : total number of scene cuts detected
  - duration_sec      : video duration in seconds
  - asl_sec           : Average Shot Length (duration / cuts)
  - cut_density       : array of cut counts per 60-second window
  - avg_brightness    : mean pixel brightness sampled at 1 fps (0–255 scale)

Architecture (standalone Spark — no Hadoop):
  1. Driver lists video objects from MinIO using the Python minio SDK.
  2. Driver distributes (channel_id, video_id, object_path) via sc.parallelize().
  3. Each worker downloads its video to a local tempfile using its own minio client,
     runs PySceneDetect + OpenCV, and yields a result dict.
  4. Driver collects results into a Spark DataFrame, prints a summary, and uploads
     the result as Parquet back to MinIO at analytics/scene_metrics/.

Python environment note:
  The driver runs with the container's system python3 (set via
  --conf spark.pyspark.driver.python=python3 in spark-submit).
  Workers run with the unpacked venv python set via
  --conf spark.pyspark.python=./environment/bin/python.
  Libraries are imported inside worker functions (analyse_partition) so they
  are only resolved in the worker's unpacked environment.
"""

from __future__ import annotations

import glob
import os
import tempfile
from dataclasses import dataclass
from typing import Iterator

from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# ── Phase 9.1: Silver Schema ──────────────────────────────────────────────────
# Explicit schema — avoids CANNOT_DETERMINE_TYPE when result fields are None.
# ArrayType(IntegerType()) carries the cut_density matrix (cuts per 60-sec window).
RESULT_SCHEMA = StructType([
    StructField("video_id",       StringType(),              nullable=False),
    StructField("channel_id",     StringType(),              nullable=False),
    StructField("cuts",           IntegerType(),             nullable=True),
    StructField("duration_sec",   DoubleType(),              nullable=True),
    StructField("asl_sec",        DoubleType(),              nullable=True),
    StructField("cut_density",    ArrayType(IntegerType()),  nullable=True),
    StructField("avg_brightness", DoubleType(),              nullable=True),
    StructField("status",         StringType(),              nullable=False),
    StructField("error",          StringType(),              nullable=True),
])


# ── Config ────────────────────────────────────────────────────────────────────

@dataclass
class Config:
    minio_endpoint: str      # host:port only — minio SDK does not want a scheme
    minio_access_key: str
    minio_secret_key: str
    videos_bucket: str
    results_bucket: str
    scene_threshold: float   # AdaptiveDetector threshold (0–100), default 3.0
    downscale_factor: int    # Phase 6.2: frame downscale passed to SceneManager
    postgres_host: str
    postgres_port: int
    postgres_user: str
    postgres_password: str
    ingestion_db: str


def _cfg() -> Config:
    endpoint = os.environ.get("MINIO_ENDPOINT", "minio:9000")
    # Strip scheme if accidentally provided — minio SDK wants host:port only
    endpoint = endpoint.replace("http://", "").replace("https://", "")
    return Config(
        minio_endpoint=endpoint,
        minio_access_key=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        minio_secret_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
        videos_bucket=os.environ.get("MINIO_VIDEOS_BUCKET", "videos"),
        results_bucket=os.environ.get("MINIO_RESULTS_BUCKET", "analytics"),
        scene_threshold=float(os.environ.get("SCENE_THRESHOLD", "3.0")),
        # Phase 6.2: default factor of 4 shrinks a 1280×720 frame to 320×180,
        # keeping each OpenCV matrix ~16× smaller in RAM.
        downscale_factor=int(os.environ.get("DOWNSCALE_FACTOR", "4")),
        postgres_host=os.environ.get("POSTGRES_HOST", "postgres"),
        postgres_port=int(os.environ.get("POSTGRES_PORT", "5432")),
        postgres_user=os.environ.get("POSTGRES_USER", "airflow"),
        postgres_password=os.environ.get("POSTGRES_PASSWORD", "airflow"),
        ingestion_db=os.environ.get("INGESTION_DB", "ingestion"),
    )


def fetch_unprocessed_videos_from_db(cfg: Config) -> list[tuple[str, str, str]]:
    """
    Query Postgres downloaded_videos table for completed downloads that
    have not yet been processed for cuts.
    Returns a list of (channel_id, video_id, minio_path) tuples.
    """
    import psycopg2
    conn = psycopg2.connect(
        host=cfg.postgres_host,
        port=cfg.postgres_port,
        user=cfg.postgres_user,
        password=cfg.postgres_password,
        database=cfg.ingestion_db,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT channel_id, video_id, minio_path FROM downloaded_videos "
                "WHERE status = 'completed' AND processed_for_cuts = FALSE"
            )
            return cur.fetchall()
    finally:
        conn.close()


def mark_videos_processed_in_db(cfg: Config, video_ids: list[str], results_path: str) -> None:
    """
    Update Postgres downloaded_videos table to set processed_for_cuts = TRUE
    and save the cuts_processing_results path for the successfully processed video IDs.
    """
    if not video_ids:
        return
    import psycopg2
    conn = psycopg2.connect(
        host=cfg.postgres_host,
        port=cfg.postgres_port,
        user=cfg.postgres_user,
        password=cfg.postgres_password,
        database=cfg.ingestion_db,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE downloaded_videos SET processed_for_cuts = TRUE, "
                "cuts_processing_results = %s "
                "WHERE video_id = ANY(%s)",
                (results_path, list(video_ids))
            )
        conn.commit()
        print(f"[Bronze] Marked {len(video_ids)} video(s) as processed_for_cuts and saved results path '{results_path}' in Postgres.")
    finally:
        conn.close()


# ── Worker logic (runs on each Spark executor) ────────────────────────────────

def analyse_partition(
    rows: Iterator[Row],
    minio_endpoint: str,
    minio_access_key: str,
    minio_secret_key: str,
    videos_bucket: str,
    scene_threshold: float,
    downscale_factor: int,
) -> Iterator[dict]:
    from minio import Minio
    from scenedetect import open_video, SceneManager
    from scenedetect.detectors import AdaptiveDetector
    import av                    # ← PyAV replaces cv2 for frame decoding
    import numpy as np

    minio_client = Minio(
        minio_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=False,
    )

    for row in rows:
        channel_id  = row["channel_id"]
        video_id    = row["video_id"]
        object_path = row["object_path"]
        tmp_path    = None

        try:
            with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as tmp:
                tmp_path = tmp.name

            # ── Phase 5.3: Download from MinIO ────────────────────────────────
            import time
            for attempt in range(3):
                try:
                    minio_client.fget_object(videos_bucket, object_path, tmp_path)
                    break
                except Exception:
                    if attempt == 2:
                        raise
                    time.sleep(2 ** attempt)

            # ── Phase 6: PySceneDetect via PyAV backend ───────────────────────
            # backend='pyav' bypasses cv2 entirely for frame decoding.
            video         = open_video(tmp_path, backend='pyav')
            scene_manager = SceneManager()
            scene_manager.add_detector(
                AdaptiveDetector(adaptive_threshold=scene_threshold)
            )
            scene_manager.downscale = downscale_factor
            scene_manager.detect_scenes(video, show_progress=False)
            scene_list = scene_manager.get_scene_list()

            # ── Phase 7.1: Cuts + Duration via PyAV ──────────────────────────
            cuts = max(0, len(scene_list) - 1)

            container    = av.open(tmp_path)
            av_stream    = container.streams.video[0]
            fps          = float(av_stream.average_rate) or 25.0
            # av duration is in microseconds
            duration_sec = (container.duration / 1_000_000) if container.duration else 0.0

            # ── Phase 7.2: ASL ────────────────────────────────────────────────
            asl_sec = round(duration_sec / cuts, 3) if cuts > 0 else None

            # ── Phase 7.3: Cut Density ────────────────────────────────────────
            if cuts > 0:
                total_minutes = max(1, int(duration_sec // 60) + 1)
                cut_density   = [0] * total_minutes
                for scene_start, _ in scene_list[1:]:
                    cut_sec    = scene_start.get_seconds()
                    bucket_idx = min(int(cut_sec // 60), total_minutes - 1)
                    cut_density[bucket_idx] += 1
            else:
                cut_density = [0]

            # ── Phase 7.4: Avg Brightness via PyAV ───────────────────────────
            # Decode every Nth frame (1 per second) directly via PyAV —
            # no cv2 codec dependency at all.
            sample_interval    = max(1, int(fps))
            brightness_samples = []
            frame_idx          = 0

            for av_frame in container.decode(video=0):
                if frame_idx % sample_interval == 0:
                    # to_ndarray('gray') gives a uint8 HxW array (0=black, 255=white)
                    gray = av_frame.to_ndarray(format='gray')
                    brightness_samples.append(float(np.mean(gray)))
                frame_idx += 1

            container.close()

            avg_brightness = (
                round(float(np.mean(brightness_samples)), 4)
                if brightness_samples else None
            )

            yield {
                "video_id":       video_id,
                "channel_id":     channel_id,
                "cuts":           cuts,
                "duration_sec":   round(duration_sec, 2),
                "asl_sec":        asl_sec,
                "cut_density":    cut_density,
                "avg_brightness": avg_brightness,
                "status":         "ok",
                "error":          None,
            }

        except Exception as exc:
            yield {
                "video_id":       video_id,
                "channel_id":     channel_id,
                "cuts":           None,
                "duration_sec":   None,
                "asl_sec":        None,
                "cut_density":    None,
                "avg_brightness": None,
                "status":         "error",
                "error":          str(exc),
            }

        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)


# ── Driver ────────────────────────────────────────────────────────────────────

NUM_WORKERS   = os.environ.get("NUM_WORKERS", 2)
CORES_PER_WORKER = os.environ.get("CORES_PER_WORKER", 2)

def main() -> None:
    cfg = _cfg()

    spark = SparkSession.builder \
        .appName("SceneDetection") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # ── Phase 3.1: Read the Bronze Layer — query PostgreSQL for unprocessed videos ────────────
    print(f"[Bronze] Querying PostgreSQL for unprocessed videos...")
    try:
        db_records = fetch_unprocessed_videos_from_db(cfg)
        print(f"[Bronze] Found {len(db_records)} unprocessed video(s) in Postgres.")
    except Exception as e:
        print(f"[Bronze] Error: Failed to query PostgreSQL: {e}")
        spark.stop()
        return

    from minio import Minio
    driver_minio = Minio(
        cfg.minio_endpoint,
        access_key=cfg.minio_access_key,
        secret_key=cfg.minio_secret_key,
        secure=False,
    )

    if not driver_minio.bucket_exists(cfg.results_bucket):
        driver_minio.make_bucket(cfg.results_bucket)
        print(f"[Bronze] Created results bucket '{cfg.results_bucket}'")

    raw_records = []
    for channel_id, video_id, minio_path in db_records:
        raw_records.append((channel_id, video_id, minio_path))

    if not raw_records:
        print("[Bronze] No new/unprocessed videos found in database — nothing to process.")
        spark.stop()
        return

    # ── Phase 3.2: Materialise as a Spark DataFrame ───────────────────────────
    CATALOG_SCHEMA = StructType([
        StructField("channel_id",  StringType(), nullable=False),
        StructField("video_id",    StringType(), nullable=False),
        StructField("object_path", StringType(), nullable=False),
    ])
    catalog_df = spark.createDataFrame(raw_records, schema=CATALOG_SCHEMA)
    target_df  = catalog_df.select("video_id", "channel_id", "object_path")

    print(f"\n[Bronze] Target video list ({target_df.count()} video(s)):")
    target_df.show(truncate=False)

    # ── Phase 4: Distribute to workers for parallel scene analysis ────────────
    # Step 4.1: .rdd drops below the DataFrame layer to a raw distributed
    # collection of Row objects.
    # Step 4.2: mapPartitions ships analyse_partition + the config closure to
    # every executor; each executor receives an iterator of its assigned rows
    # and constructs exactly one MinIO client for the entire batch.
    num_partitions = NUM_WORKERS * CORES_PER_WORKER

    results_rdd = target_df.repartition(num_partitions).rdd.mapPartitions(
        lambda rows: analyse_partition(
            rows,
            minio_endpoint=cfg.minio_endpoint,
            minio_access_key=cfg.minio_access_key,
            minio_secret_key=cfg.minio_secret_key,
            videos_bucket=cfg.videos_bucket,
            scene_threshold=cfg.scene_threshold,
            downscale_factor=cfg.downscale_factor,
        )
    )

    # ── Phase 9.2: Rebuild the DataFrame ─────────────────────────────────────
    # RESULT_SCHEMA now includes ArrayType(IntegerType()) for cut_density.
    # Passing the schema explicitly prevents CANNOT_DETERMINE_TYPE on None fields.
    results_df = spark.createDataFrame(results_rdd, schema=RESULT_SCHEMA)

    # Cache BEFORE the first action: show() and write() must share the same
    # computed snapshot.  Without cache(), every Spark action re-triggers the
    # full mapPartitions pipeline (re-download + re-process every video).
    # That is why the job appeared "stuck" after printing results — it was
    # silently running scene detection a second time for the write step.
    results_df.cache()

    print("\n── Scene Detection Results ──")
    results_df.show(truncate=False)

    # ── Phase 9.3: Write to the Silver Bucket ────────────────────────────────
    # Use /opt/spark/batch/tmp/ — this directory is a shared bind-mount
    # (../batch → /opt/spark/batch) present on spark-master AND both workers.
    # Writing to /tmp/ would land on the executor's private local filesystem
    # and would be invisible to the driver's glob() upload loop.
    local_out = "/opt/spark/batch/tmp/scene_metrics"
    os.makedirs(local_out, exist_ok=True)
    results_df.write.mode("overwrite").parquet(local_out)

    for parquet_file in glob.glob(f"{local_out}/*.parquet"):
        object_name = f"scene_metrics/{os.path.basename(parquet_file)}"
        driver_minio.fput_object(cfg.results_bucket, object_name, parquet_file)
        print(f"  -> Uploaded {object_name} to bucket '{cfg.results_bucket}'")

    # ── Phase 9.4: Update PostgreSQL state ───────────────────────────────────
    # Identify successfully processed video IDs (status == "ok")
    success_rows = results_df.filter(results_df.status == "ok").select("video_id").collect()
    success_video_ids = [row.video_id for row in success_rows]

    if success_video_ids:
        print(f"[Bronze] Marking {len(success_video_ids)} video(s) as processed for cuts in Postgres...")
        try:
            mark_videos_processed_in_db(cfg, success_video_ids, "scene_metrics/")
        except Exception as e:
            print(f"[Bronze] Warning: Failed to update database state: {e}")

    results_df.unpersist()  # release executor memory once written
    print("\n[Done] Scene detection batch job complete.")
    spark.stop()


if __name__ == "__main__":
    main()