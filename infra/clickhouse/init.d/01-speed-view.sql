CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.comments
(
    comment_id          String,
    video_id            String,
    channel_id          LowCardinality(String),
    channel_name        LowCardinality(String),
    author_display_name String,
    author_channel_id   Nullable(String),
    text                String,
    sentiment    LowCardinality(String),   -- 'Positive' | 'Neutral' | 'Negative'
    published_at DateTime64(3, 'UTC'),
    processed_at DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree(processed_at)
PARTITION BY toYYYYMM(published_at)
ORDER BY (channel_id, video_id, comment_id)
SETTINGS index_granularity = 8192;
