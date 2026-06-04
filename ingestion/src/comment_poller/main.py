from __future__ import annotations

import argparse
import logging
import os
import signal
import threading
import time
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv

from db import get_active_videos, get_session, mark_video_removed, update_video_poll
from models import Comment, TrackedVideo
from producers.base import AvroKafkaProducer
from producers.topics import RAW_COMMENTS_TOPIC
from youtube_client import YouTubeClient

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)


def _build_record(comment: Comment, polled_at: datetime) -> dict:
    def to_ms(dt: datetime) -> int:
        return int(dt.timestamp() * 1000)

    return {
        "comment_id": comment.comment_id,
        "video_id": comment.video_id,
        "channel_id": comment.channel_id,
        "author_channel_id": comment.author_channel_id,
        "author_display_name": comment.author_display_name,
        "text": comment.text,
        "like_count": comment.like_count,
        "published_at": to_ms(comment.published_at),
        "updated_at": to_ms(comment.updated_at),
        "polled_at": to_ms(polled_at),
    }


def _poll_video(
    yt: YouTubeClient,
    video: TrackedVideo,
    producer: AvroKafkaProducer,
    cutoff: datetime,
    polled_at: datetime,
    max_comments: int,
) -> tuple[int, datetime | None]:
    collected: list[Comment] = []
    page_token: str | None = None

    while len(collected) < max_comments:
        comments, next_token = yt.get_comment_threads(video.video_id, page_token=page_token)

        new_comments = [c for c in comments if c.published_at > cutoff]
        collected.extend(new_comments)

        # Comments come newest-first, so a partially new page means we reached the cutoff.
        if len(new_comments) < len(comments) or not next_token:
            break

        page_token = next_token

    collected = collected[:max_comments]

    newest_comment_at: datetime | None = None
    for comment in collected:
        producer.produce(comment.comment_id, _build_record(comment, polled_at))
        if newest_comment_at is None or comment.published_at > newest_comment_at:
            newest_comment_at = comment.published_at

    return len(collected), newest_comment_at


def poll_once(yt: YouTubeClient, producer: AvroKafkaProducer) -> None:
    inactivity_hours = int(os.environ.get("COMMENT_INACTIVITY_HOURS", 24))
    inactivity_threshold = datetime.now(timezone.utc) - timedelta(hours=inactivity_hours)
    max_comments = int(os.environ.get("MAX_COMMENTS_PER_POLL", 2000))

    conn = get_session()
    try:
        active_videos = get_active_videos(conn)
        logger.info("Polling %d active video(s).", len(active_videos))

        for video in active_videos:
            polled_at = datetime.now(timezone.utc)
            cutoff = video.comment_cursor or video.published_at

            logger.info("Polling %s — %s", video.video_id, video.title[:70])
            new_count, newest_comment_at = _poll_video(yt, video, producer, cutoff, polled_at, max_comments)

            if new_count > 0:
                update_video_poll(
                    conn,
                    video.video_id,
                    polled_at,
                    last_comment_at=polled_at,
                    comment_cursor=newest_comment_at,
                )
                logger.info("  → %d new comment(s) published.", new_count)
            else:
                update_video_poll(conn, video.video_id, polled_at)
                last_active = video.last_comment_at or video.added_at
                if last_active < inactivity_threshold:
                    mark_video_removed(conn, video.video_id)
                    logger.info(
                        "  → Evicted (no new comments for >%d hour(s)).", inactivity_hours
                    )

        producer.flush()
        logger.info("Comment polling complete.")
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Poll YouTube comments for active tracked videos.")
    parser.add_argument(
        "--interval",
        type=float,
        metavar="SECONDS",
        help="Keep polling, starting a pass every SECONDS. Without it, run one pass and exit.",
    )
    args = parser.parse_args()

    yt = YouTubeClient(os.environ["YOUTUBE_API_KEY"])
    producer = AvroKafkaProducer(RAW_COMMENTS_TOPIC, "raw-comments.avsc")

    if args.interval is None:
        poll_once(yt, producer)
        return

    # On `docker stop`, finish the pass in progress (so its comments are flushed
    # to Kafka before the cursors it advanced are relied on) instead of dying mid-pass.
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    while not stop.is_set():
        started = time.monotonic()
        poll_once(yt, producer)
        stop.wait(max(0.0, args.interval - (time.monotonic() - started)))
    logger.info("Comment poller stopped.")


if __name__ == "__main__":
    main()
