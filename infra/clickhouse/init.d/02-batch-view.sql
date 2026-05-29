-- ============================================================
-- ClickHouse Batch View — Video Topic Modeling (LDA)
-- Populated by the PySpark batch job.
-- Each run truncates and re-inserts all rows.
-- ============================================================

CREATE TABLE IF NOT EXISTS analytics.video_topics
(
    run_id          String,
    run_date        DateTime,
    video_id        String,
    channel_id      LowCardinality(String),
    channel_name    LowCardinality(String),
    topic_id        UInt8,
    topic_label     String DEFAULT '',
    topic_weight    Float32,
    dominant_topic  UInt8,
    satisfaction_pct Float32
)
ENGINE = MergeTree()
ORDER BY (video_id, topic_id);

CREATE TABLE IF NOT EXISTS analytics.topic_words
(
    run_id      String,
    run_date    DateTime,
    topic_id    UInt8,
    topic_label String DEFAULT '',
    word        String,
    weight      Float32
)
ENGINE = MergeTree()
ORDER BY (topic_id, weight);
