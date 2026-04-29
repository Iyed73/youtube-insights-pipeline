"""Comment Poller — The Hot Loop.

Intended to run every few minutes (via Airflow).  For each active tracked
video in Postgres:

  1. Fetch all top-level comments published after ``last_polled_at`` (newest
     first, paginating until we reach already-seen comments).
  2. Publish each new comment to the ``raw-comments`` Kafka topic.
  3. Advance ``last_polled_at`` (and ``last_comment_at`` when new comments
     were found) in Postgres.
  4. Evict videos that have been silent for longer than
     ``COMMENT_INACTIVITY_HOURS`` — mark them ``removed`` so they are
     excluded from future polls.

This service only polls comments — it never decides which videos to track.
"""

from __future__ import annotations

import logging
import os
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
    """Fetch and publish comments newer than *cutoff* for one video.

    The YouTube API returns comments newest-first.  We collect all new
    comments across pages and publish them in that order.

    Returns (total comments published, newest comment's published_at or None).
    """
    collected: list[Comment] = []
    page_token: str | None = None

    while len(collected) < max_comments:
        comments, next_token = yt.get_comment_threads(video.video_id, page_token=page_token)

        new_comments = [c for c in comments if c.published_at > cutoff]
        collected.extend(new_comments)

        # Stop paginating once we've hit comments older than the cutoff, or
        # when the API has no more pages to offer.
        if len(new_comments) < len(comments) or not next_token:
            break

        page_token = next_token

    # Trim to cap, keeping the most recent ones.
    collected = collected[:max_comments]

    newest_comment_at: datetime | None = None
    for comment in collected:
        producer.produce(comment.comment_id, _build_record(comment, polled_at))
        if newest_comment_at is None or comment.published_at > newest_comment_at:
            newest_comment_at = comment.published_at

    return len(collected), newest_comment_at


def run() -> None:
    inactivity_hours = int(os.environ.get("COMMENT_INACTIVITY_HOURS", 24))
    inactivity_threshold = datetime.now(timezone.utc) - timedelta(hours=inactivity_hours)
    max_comments = int(os.environ.get("MAX_COMMENTS_PER_POLL", 2000))

    yt = YouTubeClient(os.environ["YOUTUBE_API_KEY"])
    conn = get_session()
    producer = AvroKafkaProducer(RAW_COMMENTS_TOPIC, "raw-comments.avsc")

    try:
        active_videos = get_active_videos(conn)
        logger.info("Polling %d active video(s).", len(active_videos))

        for video in active_videos:
            polled_at = datetime.now(timezone.utc)
            # Only ingest comments newer than the last successful poll.
            # On first poll (last_polled_at is None), fall back to published_at
            # so we backfill all comments since the video went live.
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
                # Evict the video if it has been silent for too long.
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


if __name__ == "__main__":
    run()
