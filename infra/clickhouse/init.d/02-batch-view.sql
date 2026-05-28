CREATE DATABASE IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.video_insights
(
    -- Identity
    video_id                String,
    channel_id              LowCardinality(String),

    -- Silver: editing metrics
    cuts                    Nullable(Int32),
    duration_sec            Nullable(Float64),
    asl_sec                 Nullable(Float64),
    cut_density             Array(Int32),
    avg_brightness          Nullable(Float64),

    -- From ClickHouse analytics.comments
    total_comments          Nullable(Int32),
    positive_count          Nullable(Int32),
    neutral_count           Nullable(Int32),
    negative_count          Nullable(Int32),
    positive_ratio          Nullable(Float64),
    neutral_ratio           Nullable(Float64),
    negative_ratio          Nullable(Float64),

    -- Derived Gold metrics
    sentiment_score         Nullable(Float64),   -- -1 to +1
    editing_pace            LowCardinality(Nullable(String)),  -- fast/medium/slow

    -- Housekeeping
    batch_ts                DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(batch_ts)
ORDER BY (channel_id, video_id)
SETTINGS index_granularity = 8192;