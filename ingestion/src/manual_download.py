"""
Manual Video Downloader
=======================
Download specific YouTube videos by URL, upload to MinIO, and register
them in Postgres with a random satisfaction score (50–70%).

Usage:
    python manual_download.py <url1> [url2 ...]

Example:
    python manual_download.py https://www.youtube.com/watch?v=dQw4w9WgXcQ
"""

from __future__ import annotations

import os
import random
import re
import sys
import tempfile
from pathlib import Path

import yt_dlp
from minio import Minio

import db


# ── Helpers ───────────────────────────────────────────────────────────────────

def _extract_video_id(url: str) -> str:
    """Extract the video ID from a YouTube URL."""
    patterns = [
        r"(?:v=|youtu\.be/|shorts/)([A-Za-z0-9_-]{11})",
    ]
    for pat in patterns:
        m = re.search(pat, url)
        if m:
            return m.group(1)
    raise ValueError(f"Could not extract video ID from URL: {url}")


def _fetch_video_title(url: str) -> str:
    """Use yt-dlp to fetch the video title without downloading."""
    with yt_dlp.YoutubeDL({"quiet": True, "skip_download": True}) as ydl:
        info = ydl.extract_info(url, download=False)
        return info.get("title") or url


class _QuietLogger:
    def debug(self, msg): pass
    def info(self, msg): pass
    def warning(self, msg): pass
    def error(self, msg): print(f"[yt-dlp error] {msg}")


def _progress_hook(d: dict) -> None:
    if d["status"] == "downloading":
        pct = d.get("_percent_str", "?%").strip()
        speed = d.get("_speed_str", "?B/s").strip()
        print(f"\r  downloading {pct}  {speed}   ", end="", flush=True)
    elif d["status"] == "finished":
        total = d.get("_total_bytes_str") or d.get("_total_bytes_estimate_str") or "?"
        print(f"\r  download done ({total.strip()})                  ")


def _download_and_upload(video_id: str, channel_id: str, url: str, minio_client: Minio, bucket: str) -> str:
    """Download video via yt-dlp and upload to MinIO. Returns the MinIO path."""
    minio_path = f"{channel_id}/{video_id}.mp4"

    with tempfile.TemporaryDirectory() as tmpdir:
        out_path = str(Path(tmpdir) / f"{video_id}.mp4")
        ydl_opts = {
            "format": "bestvideo[ext=mp4][height<=720]+bestaudio[ext=m4a]/best[ext=mp4][height<=720]/best",
            "outtmpl": out_path,
            "merge_output_format": "mp4",
            "logger": _QuietLogger(),
            "noprogress": True,
            "progress_hooks": [_progress_hook],
        }
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])

        candidates = list(Path(tmpdir).glob(f"{video_id}*.mp4"))
        if not candidates:
            raise FileNotFoundError(f"yt-dlp produced no mp4 file for {video_id}")
        actual_path = str(candidates[0])

        size_mb = Path(actual_path).stat().st_size / (1024 * 1024)
        print(f"  uploading to MinIO ({size_mb:.1f} MB)...", end="", flush=True)
        minio_client.fput_object(bucket, minio_path, actual_path, content_type="video/mp4")
        print(" done")

    return minio_path


# ── Main ──────────────────────────────────────────────────────────────────────

def main(urls: list[str]) -> None:
    minio_endpoint  = os.environ["MINIO_ENDPOINT"]
    minio_access    = os.environ["MINIO_ACCESS_KEY"]
    minio_secret    = os.environ["MINIO_SECRET_KEY"]
    bucket          = os.environ.get("MINIO_VIDEOS_BUCKET", "videos")

    minio_client = Minio(minio_endpoint, access_key=minio_access, secret_key=minio_secret, secure=False)
    if not minio_client.bucket_exists(bucket):
        minio_client.make_bucket(bucket)

    session = db.get_session()

    for url in urls:
        print(f"\n── {url}")
        try:
            video_id    = _extract_video_id(url)
            channel_id  = f"UCmanual{random.randint(10000, 99999)}"
            satisfaction = round(random.uniform(50.0, 70.0), 2)

            if db.is_video_downloaded(session, video_id):
                print(f"  [skip] {video_id} already downloaded")
                continue

            title = _fetch_video_title(url)
            print(f"  video_id={video_id}  title={title!r}")
            print(f"  channel_id={channel_id}  satisfaction={satisfaction}%")

            minio_path = _download_and_upload(video_id, channel_id, url, minio_client, bucket)

            db.upsert_downloaded_video(
                session,
                video_id=video_id,
                channel_id=channel_id,
                title=title,
                minio_path=minio_path,
                satisfaction_pct=satisfaction,
                status="completed",
            )
            print(f"  registered in Postgres → {bucket}/{minio_path}")

        except Exception as exc:
            print(f"  [fail] {exc}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python manual_download.py <url1> [url2 ...]")
        sys.exit(1)
    main(sys.argv[1:])
