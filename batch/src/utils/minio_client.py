"""
boto3 wrapper that downloads a MinIO .mp4 object to a local temp file
so Whisper (running on the driver) can process it.

We use boto3 here — not the s3a Hadoop connector — because Whisper needs
a plain filesystem path, not a distributed Spark datasource.

The module-level boto3 client is created once (lazy singleton) and reused
across the entire driver process lifetime.
"""

from __future__ import annotations

import logging
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

import boto3
from botocore.client import Config

from config import cfg

logger = logging.getLogger(__name__)

_s3_client = None   # lazy singleton


def _get_client():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client(
            "s3",
            endpoint_url          = cfg.minio.endpoint_url,
            aws_access_key_id     = cfg.minio.access_key,
            aws_secret_access_key = cfg.minio.secret_key,
            config                = Config(signature_version="s3v4"),
            # MinIO ignores region but boto3 requires a non-empty value
            region_name           = "us-east-1",
        )
    return _s3_client


@contextmanager
def download_video(minio_path: str) -> Generator[str, None, None]:
    """
    Context manager: download a MinIO object, yield the local temp path,
    then delete the temp file on exit.

    Parameters
    ----------
    minio_path : str
        The value stored in downloaded_videos.minio_path.
        Accepted formats:
          • "videos/UCsBjURrPoezykLs9EqgamOA/9OQ5vaYbGV0.mp4"   (with bucket prefix)
          • "UCsBjURrPoezykLs9EqgamOA/9OQ5vaYbGV0.mp4"          (without bucket prefix)

    Example
    -------
    with download_video("videos/UCxxx/9OQ5vaYbGV0.mp4") as path:
        result = model.transcribe(path)
    """
    bucket = cfg.minio.bucket
    key    = minio_path.strip("/")

    # Strip the bucket name prefix if it's baked into the stored path
    # e.g.  "videos/UCxxx/vid.mp4"  →  "UCxxx/vid.mp4"
    bucket_prefix = f"{bucket}/"
    if key.startswith(bucket_prefix):
        key = key[len(bucket_prefix):]

    suffix = Path(key).suffix or ".mp4"

    with tempfile.NamedTemporaryFile(suffix=suffix, delete=True) as tmp:
        local_path = tmp.name
        logger.info("[minio] s3://%s/%s → %s", bucket, key, local_path)

        try:
            _get_client().download_file(bucket, key, local_path)
            logger.info("[minio] Download complete.")
            yield local_path

        except Exception as exc:
            logger.error("[minio] Download failed for '%s/%s': %s", bucket, key, exc)
            raise