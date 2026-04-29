"""Centralised YouTube Data API v3 client.

Uses the uploads-playlist approach for video discovery (quota cost ≈ 3 units
per channel) rather than ``search.list`` (100 units) to stay well within the
free daily quota of 10 000 units.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from models import Comment, Video

logger = logging.getLogger(__name__)

_ISO_FMT = "%Y-%m-%dT%H:%M:%SZ"


def _parse_dt(s: str) -> datetime:
    return datetime.strptime(s, _ISO_FMT).replace(tzinfo=timezone.utc)


def _to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


class YouTubeClient:
    """Thin, typed wrapper around the YouTube Data API v3."""

    def __init__(self, api_key: str) -> None:
        # cache_discovery=False avoids writing a local file in read-only envs.
        self._yt = build("youtube", "v3", developerKey=api_key, cache_discovery=False)

    # ------------------------------------------------------------------
    # Channel video discovery
    # ------------------------------------------------------------------

    def get_channel_videos(
        self,
        channel_id: str,
        published_after: datetime,
        max_results: int = 50,
    ) -> list[Video]:
        """Return videos from *channel_id* published after *published_after*.

        Pagination stops as soon as we reach the lookback boundary or
        accumulate *max_results* video IDs, whichever comes first.
        """
        uploads_id = self._get_uploads_playlist_id(channel_id)
        if uploads_id is None:
            return []

        video_ids = self._collect_video_ids(uploads_id, published_after, max_results)
        if not video_ids:
            return []

        return self._fetch_video_details(channel_id, video_ids)

    def _get_uploads_playlist_id(self, channel_id: str) -> str | None:
        resp = self._yt.channels().list(part="contentDetails", id=channel_id).execute()
        items = resp.get("items", [])
        if not items:
            logger.warning("Channel %s not found or has no uploads playlist.", channel_id)
            return None
        return items[0]["contentDetails"]["relatedPlaylists"]["uploads"]

    def _collect_video_ids(
        self,
        playlist_id: str,
        published_after: datetime,
        max_results: int,
    ) -> list[str]:
        """Page through the uploads playlist and return video IDs within the window."""
        cutoff_str = published_after.strftime(_ISO_FMT)
        video_ids: list[str] = []
        next_page_token: str | None = None

        while len(video_ids) < max_results:
            resp = (
                self._yt.playlistItems()
                .list(
                    part="snippet,contentDetails",
                    playlistId=playlist_id,
                    maxResults=50,
                    pageToken=next_page_token,
                )
                .execute()
            )

            past_window = False
            for item in resp.get("items", []):
                pub = item["snippet"]["publishedAt"]
                if pub < cutoff_str:
                    # Playlist is newest-first; once we're past the window we stop.
                    past_window = True
                    break
                video_ids.append(item["contentDetails"]["videoId"])

            next_page_token = resp.get("nextPageToken")
            if past_window or not next_page_token:
                break

        return video_ids

    def _fetch_video_details(self, channel_id: str, video_ids: list[str]) -> list[Video]:
        """Batch-fetch snippet + statistics for a list of video IDs."""
        videos: list[Video] = []
        for i in range(0, len(video_ids), 50):
            chunk = video_ids[i : i + 50]
            resp = (
                self._yt.videos()
                .list(part="snippet,statistics", id=",".join(chunk))
                .execute()
            )
            for item in resp.get("items", []):
                videos.append(
                    Video(
                        video_id=item["id"],
                        channel_id=channel_id,
                        title=item["snippet"]["title"],
                        view_count=int(item["statistics"].get("viewCount", 0)),
                        published_at=_parse_dt(item["snippet"]["publishedAt"]),
                    )
                )
        return videos

    # ------------------------------------------------------------------
    # Comment polling
    # ------------------------------------------------------------------

    def get_comment_threads(
        self,
        video_id: str,
        page_token: str | None = None,
        max_results: int = 100,
    ) -> tuple[list[Comment], str | None]:
        """Return one page of top-level comments for *video_id*.

        Comments are ordered by time (newest first).  Pass the returned
        ``next_page_token`` on subsequent calls to page forward through
        older comments.

        Returns ``([], None)`` if comments are disabled on the video.
        """
        try:
            resp = (
                self._yt.commentThreads()
                .list(
                    part="snippet",
                    videoId=video_id,
                    order="time",
                    maxResults=max_results,
                    pageToken=page_token,
                    textFormat="plainText",
                )
                .execute()
            )
        except HttpError as exc:
            if exc.status_code in (403, 404):
                logger.warning(
                    "Cannot fetch comments for video %s (HTTP %s) — skipping.",
                    video_id,
                    exc.status_code,
                )
                return [], None
            raise

        comments: list[Comment] = []
        for item in resp.get("items", []):
            top = item["snippet"]["topLevelComment"]
            s = top["snippet"]
            comments.append(
                Comment(
                    comment_id=top["id"],
                    video_id=video_id,
                    channel_id=item["snippet"]["channelId"],
                    author_channel_id=s.get("authorChannelId", {}).get("value"),
                    author_display_name=s["authorDisplayName"],
                    text=s["textDisplay"],
                    like_count=int(s.get("likeCount", 0)),
                    published_at=_parse_dt(s["publishedAt"]),
                    updated_at=_parse_dt(s["updatedAt"]),
                )
            )

        return comments, resp.get("nextPageToken")
