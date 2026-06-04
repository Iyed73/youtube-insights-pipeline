CREATE TABLE IF NOT EXISTS analytics.topic_summary
(
    run_id           String,
    run_date         DateTime,
    topic_id         UInt8,
    topic_label      String DEFAULT '',
    avg_satisfaction Float32,
    video_count      UInt32
)
ENGINE = MergeTree()
ORDER BY (topic_id);

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
