"""Kafka topic name constants shared across all producers."""

# Newly discovered videos selected for comment tracking.
# Schema: schemas/channel-discovery.avsc  Key: video_id
CHANNEL_DISCOVERY_TOPIC = "channel-discovery"

# Raw comments fetched from the YouTube Data API.
# Schema: schemas/raw-comments.avsc  Key: comment_id
RAW_COMMENTS_TOPIC = "raw-comments"
