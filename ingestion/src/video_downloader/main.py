from __future__ import annotations

import json
import logging
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

import requests
import yt_dlp
from minio import Minio


class _QuietLogger:
    def debug(self, msg): pass
    def info(self, msg): pass
    def warning(self, msg): pass
    def error(self, msg): logging.error(msg)

import db
from models import TrackedVideo


@dataclass
class Config:
    clickhouse_host: str
    clickhouse_http_port: int
    clickhouse_user: str
    clickhouse_password: str
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    minio_bucket: str
    lookback_days: int
    top_n: int
    min_comments: int
    max_height: int
    max_duration_sec: int


def _cfg() -> Config:
    return Config(
        clickhouse_host=os.environ["CLICKHOUSE_HOST"],
        clickhouse_http_port=int(os.environ.get("CLICKHOUSE_HTTP_PORT", "8123")),
        clickhouse_user=os.environ["CLICKHOUSE_USER"],
        clickhouse_password=os.environ["CLICKHOUSE_PASSWORD"],
        minio_endpoint=os.environ["MINIO_ENDPOINT"],
        minio_access_key=os.environ["MINIO_ACCESS_KEY"],
        minio_secret_key=os.environ["MINIO_SECRET_KEY"],
        minio_bucket=os.environ.get("MINIO_VIDEOS_BUCKET", "videos"),
        lookback_days=int(os.environ.get("DOWNLOAD_LOOKBACK_DAYS", "1")),
        top_n=int(os.environ.get("TOP_VIDEOS_TO_DOWNLOAD", "5")),
        min_comments=int(os.environ.get("MIN_COMMENTS_FOR_DOWNLOAD", "50")),
        max_height=int(os.environ.get("VIDEO_MAX_HEIGHT", "720")),
        max_duration_sec=int(os.environ.get("VIDEO_MAX_DURATION_SEC", "600")),
    )


@dataclass
class VideoCandidate:
    video_id: str
    channel_id: str
    satisfaction_pct: float
    total_comments: int


def fetch_top_videos(cfg: Config) -> list[VideoCandidate]:
    query = f"""
        SELECT
            video_id,
            channel_id,
            countIf(sentiment = 'Positive') * 100.0 / count() AS satisfaction_pct,
            count() AS total_comments
        FROM analytics.comments
        WHERE published_at >= now() - INTERVAL {cfg.lookback_days} DAY
        GROUP BY video_id, channel_id
        HAVING total_comments >= {cfg.min_comments}
        ORDER BY satisfaction_pct DESC
        LIMIT {cfg.top_n}
        FORMAT JSONEachRow
    """
    url = f"http://{cfg.clickhouse_host}:{cfg.clickhouse_http_port}/"
    resp = requests.post(
        url,
        data=query.encode(),
        auth=(cfg.clickhouse_user, cfg.clickhouse_password),
        timeout=30,
    )
    resp.raise_for_status()

    candidates = []
    for line in resp.text.strip().splitlines():
        if not line:
            continue
        row = json.loads(line)
        candidates.append(
            VideoCandidate(
                video_id=row["video_id"],
                channel_id=row["channel_id"],
                satisfaction_pct=float(row["satisfaction_pct"]),
                total_comments=int(row["total_comments"]),
            )
        )
    return candidates


def get_video_title(session, video_id: str) -> str:
    from sqlalchemy import select

    row = session.execute(
        select(TrackedVideo.title).where(TrackedVideo.video_id == video_id)
    ).scalar_one_or_none()
    return row or video_id


def _ydl_progress_hook(d: dict) -> None:
    if d["status"] == "downloading":
        pct = d.get("_percent_str", "?%").strip()
        speed = d.get("_speed_str", "?B/s").strip()
        eta = d.get("_eta_str", "?s").strip()
        print(f"\r          downloading {pct}  {speed}  ETA {eta}   ", end="", flush=True)
    elif d["status"] == "finished":
        total = d.get("_total_bytes_str") or d.get("_total_bytes_estimate_str") or "?"
        print(f"\r          download done ({total.strip()})                      ")


def download_and_upload(
    video_id: str,
    channel_id: str,
    cfg: Config,
    minio_client: Minio,
) -> str:
    minio_path = f"{channel_id}/{video_id}.mp4"

    with tempfile.TemporaryDirectory() as tmpdir:
        out_path = str(Path(tmpdir) / f"{video_id}.mp4")
        ydl_opts = {
            "format": f"bestvideo[ext=mp4][height<={cfg.max_height}]+bestaudio[ext=m4a]/best[ext=mp4][height<={cfg.max_height}]/best",
            "outtmpl": out_path,
            "merge_output_format": "mp4",
            "logger": _QuietLogger(),
            "noprogress": True,
            "progress_hooks": [_ydl_progress_hook],
            "download_ranges": yt_dlp.utils.download_range_func(None, [(0, cfg.max_duration_sec)]),
            "force_keyframes_at_cuts": True,
        }
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([f"https://www.youtube.com/watch?v={video_id}"])

        # yt-dlp may append .mp4 when merging, so the output name can differ.
        candidates = list(Path(tmpdir).glob(f"{video_id}*.mp4"))
        if not candidates:
            raise FileNotFoundError(f"yt-dlp produced no mp4 file in {tmpdir}")
        actual_path = str(candidates[0])

        file_size_mb = Path(actual_path).stat().st_size / (1024 * 1024)
        print(f"          uploading to MinIO ({file_size_mb:.1f} MB)...", end="", flush=True)
        minio_client.fput_object(
            cfg.minio_bucket,
            minio_path,
            actual_path,
            content_type="video/mp4",
        )
        print(" done")

    return minio_path


def main() -> None:
    cfg = _cfg()

    minio_client = Minio(
        cfg.minio_endpoint,
        access_key=cfg.minio_access_key,
        secret_key=cfg.minio_secret_key,
        secure=False,
    )
    if not minio_client.bucket_exists(cfg.minio_bucket):
        minio_client.make_bucket(cfg.minio_bucket)

    print(
        f"Querying ClickHouse for top {cfg.top_n} videos "
        f"by satisfaction over the last {cfg.lookback_days} days "
        f"(min {cfg.min_comments} comments)..."
    )
    candidates = fetch_top_videos(cfg)

    if not candidates:
        print("No eligible videos found.")
        return

    print(f"Found {len(candidates)} candidate(s).")

    downloaded = skipped = failed = 0

    session = db.get_session()
    for candidate in candidates:
        vid = candidate.video_id

        if db.is_video_downloaded(session, vid):
            print(f"  [skip]  {vid}  (already downloaded)")
            skipped += 1
            continue

        title = get_video_title(session, vid)
        print(
            f"  [dl]    {vid}  satisfaction={candidate.satisfaction_pct:.1f}%  "
            f"comments={candidate.total_comments}  title={title!r}"
        )

        try:
            minio_path = download_and_upload(vid, candidate.channel_id, cfg, minio_client)
            db.upsert_downloaded_video(
                session,
                video_id=vid,
                channel_id=candidate.channel_id,
                title=title,
                minio_path=minio_path,
                satisfaction_pct=candidate.satisfaction_pct,
                status="completed",
            )
            print(f"          → stored at {cfg.minio_bucket}/{minio_path}")
            downloaded += 1
        except Exception as exc:
            err_msg = str(exc)
            print(f"  [fail]  {vid}  {err_msg}")
            db.upsert_downloaded_video(
                session,
                video_id=vid,
                channel_id=candidate.channel_id,
                title=title,
                minio_path=f"{candidate.channel_id}/{vid}.mp4",
                satisfaction_pct=candidate.satisfaction_pct,
                status="failed",
                error=err_msg,
            )
            failed += 1

    print(f"\nDone: {downloaded} downloaded, {skipped} skipped, {failed} failed.")


if __name__ == "__main__":
    main()
