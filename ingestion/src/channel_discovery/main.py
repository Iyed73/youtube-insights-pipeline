from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml
from dotenv import load_dotenv

from db import get_active_videos_for_channel, get_session, insert_tracked_video, mark_videos_removed
from models import Video
from producers.base import AvroKafkaProducer
from producers.topics import CHANNEL_DISCOVERY_TOPIC
from youtube_client import YouTubeClient

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)

_CONFIG_PATH = Path(__file__).parents[3] / "config" / "channels.yaml"


def _load_channels() -> list[dict]:
    with _CONFIG_PATH.open() as f:
        return yaml.safe_load(f)["channels"]


def _build_record(video: Video, channel_name: str, discovered_at: datetime) -> dict:
    def to_ms(dt: datetime) -> int:
        return int(dt.timestamp() * 1000)

    return {
        "video_id": video.video_id,
        "channel_id": video.channel_id,
        "channel_name": channel_name,
        "title": video.title,
        "view_count": video.view_count,
        "published_at": to_ms(video.published_at),
        "discovered_at": to_ms(discovered_at),
    }


def run() -> None:
    max_videos = int(os.environ.get("MAX_VIDEOS_PER_CHANNEL", 10))
    max_tracked = int(os.environ.get("MAX_TRACKED_PER_CHANNEL", 100))
    lookback_days = int(os.environ.get("VIDEO_LOOKBACK_DAYS", 30))
    published_after = datetime.now(timezone.utc) - timedelta(days=lookback_days)

    channels = _load_channels()
    yt = YouTubeClient(os.environ["YOUTUBE_API_KEY"])
    conn = get_session()
    producer = AvroKafkaProducer(CHANNEL_DISCOVERY_TOPIC, "channel-discovery.avsc")

    try:
        for ch in channels:
            channel_id: str = ch["id"]
            channel_name: str = ch["name"]
            logger.info("Discovering videos for channel: %s (%s)", channel_name, channel_id)

            active_videos = get_active_videos_for_channel(conn, channel_id)

            stale = [v for v in active_videos if v.published_at < published_after]
            if stale:
                mark_videos_removed(conn, [v.video_id for v in stale])
                logger.info(
                    "  - Evicted %d stale video(s) (older than %d days).",
                    len(stale),
                    lookback_days,
                )

            remaining = [v for v in active_videos if v not in stale]
            remaining_ids = {v.video_id for v in remaining}

            # Fetch more than max_videos so we can rank and pick the true top-N.
            candidates = yt.get_channel_videos(
                channel_id, published_after, max_results=max_videos * 3
            )
            top_videos = sorted(candidates, key=lambda v: v.view_count, reverse=True)[:max_videos]
            new_videos = [v for v in top_videos if v.video_id not in remaining_ids]

            logger.info(
                "Channel %s: %d candidate(s) in top-%d window, %d new",
                channel_name,
                len(top_videos),
                max_videos,
                len(new_videos),
            )

            if new_videos:
                discovered_at = datetime.now(timezone.utc)
                for video in new_videos:
                    producer.produce(
                        key=video.video_id,
                        value=_build_record(video, channel_name, discovered_at),
                    )
                    insert_tracked_video(
                        conn,
                        video_id=video.video_id,
                        channel_id=video.channel_id,
                        channel_name=channel_name,
                        title=video.title,
                        view_count=video.view_count,
                        published_at=video.published_at,
                    )
                    logger.info("  + %s  (%d views)", video.video_id, video.view_count)

            total_active = len(remaining) + len(new_videos)
            if total_active > max_tracked:
                excess = total_active - max_tracked
                # remaining is oldest-first, so this drops the oldest videos.
                to_cap = [v.video_id for v in remaining[:excess]]
                mark_videos_removed(conn, to_cap)
                logger.info(
                    "  - Cap enforced: removed %d oldest video(s) (limit=%d).",
                    len(to_cap),
                    max_tracked,
                )

        producer.flush()
        logger.info("Channel discovery complete.")
    finally:
        conn.close()


if __name__ == "__main__":
    run()
